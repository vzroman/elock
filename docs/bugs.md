# elock: bugs and design pitfalls found by the functional tests

Found on 2026-09-26 while building `test/functional` (branch deadlock_refactoring). Nothing is
fixed; the cases named below assert the intended behaviour and fail on purpose. Run everything with
`./rebar3 ct --spec test/functional/test.spec`, one suite with
`./rebar3 ct --dir test/functional --suite <suite>`.

Two lists: **A** - cosmetic bugs, local fixes of a few lines; **B** - design pitfalls, each needs a
decision about the protocol or the process model before it is fixed.

## A. Cosmetic bugs, easy to fix

### A1. Improper option list in `erlang:cancel_timer/2` - severe impact, one-character fix

* Where: `src/elock_manager.erl:443` (`cancel_postpone_timer/1`) and `:1342` (`stop_timer/1`).
  `[{async, true} | {info, false}]` is an improper list; the call raises `badarg`.
* Effect 1: `cancel_postpone_timer/1` has no catch, so the manager crashes whenever the postpone
  timer is armed and then cancelled: a ticket gap that closes within 100 ms, or a client taking a
  ticket right before the last holder unlocks (`try_unlock/1` arms the timer for the next round, the
  next request cancels it). The crash comes after `#locked{}` was sent: the new holder keeps a lock
  without a manager, the next client starts a fresh manager and grants the term again. Under
  contention mutual exclusion is lost, and the stale entry stays in the table until a later
  client's `'DOWN'` cleanup.
* Effect 2: in `stop_timer/1` the call is under `catch`, so request timeout timers are never
  cancelled; the manager ignores the late `{timeout, ...}` messages (harmless, a small leak until
  they fire).
* Fix: `[{async, true}, {info, false}]` in both places.
* Tests: `elock_manager_SUITE` `postponed_gap_closed_test`, `postponed_out_of_order_test`,
  `manager_new_round_test`, `stop_timer_test`; unpatched, 7 cases of `elock_concurrency_SUITE`
  fail with exclusive/mode violations and stale entries.

### A2. An unknown option key crashes instead of being rejected

* Where: `src/elock.erl:416` `validate_option/2` has no clause for other keys, so
  `elock:lock(S, T, N, #{foo => bar})` raises `error:function_clause` while every other invalid
  option is a thrown `{invalid_..., Value}`.
* Fix: a catch-all clause throwing `{invalid_option, Key}`.
* Test: `elock_SUITE` `validate_options_test` (documents the current behaviour).

### A3. Raw ets/pg behaviour leaks through the API

* A lock on a scope whose table is gone raises `error:badarg` from `ets:update_counter/4`;
  `ready_nodes/1` of an unknown or stopped scope answers `[]` (`pg:get_members/2` swallows the
  missing table), indistinguishable from a running scope with no members.
* Fix: check `ets:whereis(Scope)` and return `{error, no_scope}` (or raise a named error).
* Tests: `elock_SUITE` `lock_on_stopped_scope_test`, `ready_nodes_unknown_scope_test`.

### A4. Starting a scope twice kills the caller

* Where: `src/elock.erl:39` `start_link/1`. The second start crashes in `ets:new/2` inside the
  spawned process and the badarg reaches the caller over the link, instead of
  `{error, {already_started, Pid}}`.
* Fix: check `ets:whereis(Scope)` first. (Making the scope process a proper OTP process is B9.)
* Test: `elock_SUITE` `start_twice_test`.

### A5. A non-error exit reason is passed through

* Where: `src/elock.erl:317` `wait_verdict/1` returns the first non-ok worker result as is; a worker
  that dies with e.g. `killed` makes `lock/4` return the bare atom instead of `{error, _}`.
  Practically unreachable today.
* Fix: wrap anything but `{error, _}` as `{error, {exit, Reason}}`.
* Test: `elock_SUITE` `wait_verdict_failure_reasons_test` (documents it).

### A6. An empty update creates an empty graph

* Where: `src/elock_graph.erl:193` `add_held_locks/4` with an empty update on no graph yields
  `#graph{index = #{}}`, against the "a #graph{} never has an empty index" invariant of
  `remove_edges/2`. Unreachable from the manager (`elock:notify_queued/5` never sends an empty
  update), harmless.
* Fix: guard `map_size(Update) > 0`.

### A7. README describes the previous API

* `lock/4` with `IsShared, Timeout`, an unlock fun, `infinity` timeouts. Rewrite for
  `lock/3,4` with the options map, `unlock/1`, `ready_nodes/1`.

## B. Design pitfalls

### B1. Withdrawing a queued multi-node copy hangs the client; a dead client leaves orphaned workers

Blocking for multi-node use.

* Mechanism: `src/elock_manager.erl:687` `kill_proxy/1` kills the remote proxy - the process that
  `ecall_receive` spawned on the manager's node to run `elock_manager:lock/1` - when a waiting
  request is removed by `#unlock{}` (the failure branch of `elock:wait_verdict/1` withdraws the
  copies still queued on other nodes) or by the client's `'DOWN'`. ecall's receiver starts that
  process with a plain `spawn` and only the process itself reports its result (`try ... catch`);
  a `kill` exit signal is not an exception, so a killed proxy sends nothing. The caller's worker
  (the `spawn_monitor` of `elock:run_request/1`) then waits in `ecall_connection:call/4` forever:
  it monitors only the local ecall pool worker, never the remote process.
* Effect (a): a client whose multi-node request fails on one node while a copy is still queued on
  another - a cross-node deadlock, a timeout on one node with a longer wait elsewhere - never
  returns from `elock:lock/4`: it hangs in `elock:wait_unlock/2` (`src/elock.erl:346`) with an
  empty mailbox.
* Effect (b): when a waiting multi-node client dies, its workers never exit - one leaked process
  per node of the request.
* Under load, same-term multi-node requests deadlock across nodes all the time, so this hangs the
  multi-node concurrency scenarios within seconds even with A1 patched.
* Evidence: `elock_multi_node_SUITE` `deadlock_withdraws_queued_copy_test` (client stuck in
  `wait_unlock/2`), `dead_client_leaves_no_workers_test` (workers left in
  `ecall_connection:call/4`); `elock_multi_node_concurrency_SUITE` `distributed_*` cases
  (`clients_did_not_finish`, clients stuck in `wait_unlock/2` or `wait_verdict/1`).
* Direction: never kill the proxy. Send it a verdict instead (a `#withdrawn{}` next to
  `#retry{}`, answered by `elock_manager:lock/1` as `{error, withdrawn}`) so it returns normally and
  the worker exits; or let the worker monitor the remote process. The code change is small, but it
  is a contract between elock and ecall, hence listed here and not under A.

### B2. A dead manager is never noticed by its holders

* Holders keep a context pointing at a dead pid; the stale ETS entry stays until a later client's
  `'DOWN'` cleanup; the next client starts a new manager that grants the term again, so an old
  holder and a new one coexist. `get_manager/3` (`src/elock_manager.erl:124`) polls forever if a
  manager dies before it wrote its pid into the entry (`{Term, 0, N}` stays).
* A1 is currently the main way to reach this state, but the mechanism has no recovery path of its
  own (no supervision, no monitor of the manager by the holder, no incarnation check on unlock).
* Tests: `elock_locking_SUITE` `manager_killed_holder_unlocks_test`,
  `manager_killed_with_waiters_test` (document the current behaviour).

### B3. Late `#queued{}` messages leak into client mailboxes

* A manager sends `#queued{}` through the batched ecall path, while the grant travels a different
  path via the worker's `'DOWN'`. When the grant overtakes the notification, `wait_verdict/1` has
  already returned and nothing ever consumes the message. Long-lived processes doing many
  multi-node locks accumulate them (the stale ref never matches again, so it is a leak, not a
  misbehaviour). Do not assert empty client mailboxes in tests.

### B4. Implicit ordering assumption in barging

* `add_busy_request/2` (`src/elock_manager.erl:652`) treats any client known to the manager as a
  holder, including one that only has a queued request. If a client's withdrawal of a queued copy
  (`#unlock{}` after a multi-node failure) were to arrive after its next request for the same term,
  that request would be granted at once over an exclusive holder (`try_barging/2` -> `get_lock/2`).
  ecall's one proxy worker per sender keeps the order today; the manager itself does not check
  `has_lock`. Direction: barge only for clients that hold (`has_lock = true`), enqueue otherwise.

### B5. Weight semantics favour multi-node holders

* The weight of a waiting request is the number of `{Scope, Term, Node}` keys it held when it
  asked: a 3-node lock outweighs three single-node ones, and a request that held nothing when it
  asked joins the graph through `#add_held_locks{}` with weight 0 and loses every unequal contest.
  Defensible, but it should be a stated rule of the API.

### B6. Probe fan-out is unbounded

* Every gained hold floods a probe to the managers of all locks held by all waiters of the manager,
  forwarded transitively, with no rate limit or coalescing. Fine at test scale; a consideration for
  large wait-for graphs and busy clusters.

### B7. Managers run at high priority

* One manager per locked term at `{priority, high}`: thousands of them (e.g. `many_terms_test`)
  can starve normal-priority clients during storms.

### B8. `unlock/1` from a foreign process is a silent no-op

* The context lives in the locking process's dictionary, so `unlock/1` from any other process
  returns `ok` and does nothing, hiding misuse; a lock cannot be handed over between processes.

### B9. The scope process is not an OTP process

* `start_link/1` spawns a plain process sleeping forever: no `sys` support, no graceful shutdown of
  the managers of the scope, the ETS table and the pg scope die with it. See also A4.
