# Functional tests: alignment with the production code

Spec, 2026-10-14. Base: commit `c4f61ef` on `lazy_graph`. The functional
tests were last updated in `8479915`; since then the production code has
moved through `c2f5758` (lazy graph), `c3542d7` (init_graph into
elock_graph), `41feb6a` (remove edges optimization) and `f7e7e75` (single
node happy path). This document says what the tests must become. It
contains no test code.

## 0. How to read this

- The spec is a floor, not a ceiling. Every "must" is required. More
  coverage is welcome. An existing case is dropped only where §4 names it
  and says why.
- The tests are held to the principles of the code. Every assertion names
  an event that can occur under the documented contracts. A process is
  spoken to as its library speaks to it: the messages are built exactly as
  the client functions build them, under the tag the protocol gives them.
  Every wait has a deadline and fails naming what it waited for. Every case
  leaves its scope idle and its graph table empty.
- Where this text names a function, a record or a message shape, it names
  the one in the sources at `c4f61ef`. The implementer copies the record
  mirrors from the sources, never from this text, and re-reads the clause
  a test pins before writing the test.
- Module tests reach internals through `export_all` of the test profile
  (see `rebar.config`); this stays.

## 1. The production code as the tests must see it

The facts below are what changed or what the tests get wrong today. What is
not listed (the manager's queue, the context arithmetic, the walk, the
scope, the peers) is as the tests already assume.

### 1.1 Request paths

| Request nodes (after `lists:usort`) | Who waits for the verdict | Workers | `#node.holder` |
|---|---|---|---|
| `[node()]`, whatever the client holds | the client itself, in `elock_manager:wait_verdict/4` | none | `undefined` |
| anything else: a remote node alone, or several nodes | the client in `elock_context:wait_verdict/2`; one worker per node blocked in `ecall_connection:call/4`; the callee on each lock node in `elock_manager:wait_verdict/4` | one per node | local node: the worker's pid, dead once it has reported; remote node: the worker, alive in `elock_context:holding/3`, monitoring the client |

Before `f7e7e75` a client that held something took the worker path on its
own node. That is gone: holds no longer select the path.

`#request.notify_queued` is set by `elock_context:lock/4`: `true` when the
request names more than one distinct node or the context holds something,
`false` otherwise.

### 1.2 The queued notification

`elock_manager:notify_queued/1` sends `{Tag, #queued{ref, manager = self(),
node = node()}}` to the proxy iff `notify_queued =:= true` and the proxy is a
pid. The proxy may be the client itself. The old rule ("only a proxy other
than the client") is gone. It is sent from `start_waiting/1`, i.e. by
`enqueue/2` and `enqueue_barging/2`, so a postponed request notifies when it
is taken in and an immediate grant never notifies.

### 1.3 `elock_manager:lock/2` and `wait_verdict/4`

`lock(Request, OnQueued)`. `OnQueued(#queued{}, Tag)` runs in the proxy
process with the bare `#queued{}` and `Tag`, the monitor reference of the
attempt (also the tag of every reply). A retry calls it again under the new
tag. The production callbacks: the single-node path spawns the graph worker
(§1.5); the worker path does `ecall:send(Client, Queued)`.

The clauses of `wait_verdict/4`, all under the attempt's tag:

| Message | Result |
|---|---|
| `{Tag, #locked{}}` | `{ok, Manager}` |
| `{Tag, #queued{ref = Ref}}` | the callback, keep waiting |
| `{Tag, #deadlock{winner = W}}` | `{error, {deadlock, W}}` |
| `{Tag, #timeout{}}` | `{error, timeout}` |
| `{'DOWN', _, process, _, {Tag, #deadlock{winner = W}}}` | the graph worker of a direct request has lost: `Manager ! #unlock{ref = Ref}`, then exactly one `{Tag, _}` reply is consumed (`#withdrawn{}` for a waiter; `#locked{}` or `#timeout{}` when the manager sent it before taking the unlock), then `{error, {deadlock, W}}` |
| `{Tag, #retry{}}` | a new ticket under a new tag, same callback |
| `{'DOWN', Tag, process, Manager, _}` | the entry `{Term, Manager, '_'}` is deleted, a new ticket |

### 1.4 `elock_manager:release_proxy/1` (was `kill_proxy/1`)

Proxy = client: `{Tag, #withdrawn{ref = Ref}}` to it, the process lives on.
Proxy another pid: `exit(Proxy, kill)`. No proxy: nothing. A holder has no
proxy (`locked/2` clears proxy and tag). Called by both clauses of
`remove_request/2` and by the last-holder clause of `handle_unlock/2`: every
removal of a waiting request whose proxy is its client sends `#withdrawn{}`,
also on `handle_down/2` and `handle_nodedown/2` (to a dead pid, harmless).

### 1.5 The graph worker: `elock_graph:init_graph/2`

Spawned with `spawn_monitor` by the client on the first `#queued{}` of a
request, never before:

- single node: inside `OnQueued`, Tag = the attempt's monitor ref; the pid
  and monitor are kept in the client's dictionary under
  `{elock_context, graph}` and the worker is stopped when
  `elock_manager:lock/2` returns, whatever the result;
- several nodes: inside `elock_context:wait_verdict/2`, Tag = Ref.

Start holds = the context's holds ∪ `{Scope, Term, N} => Manager` for every
node granted so far. `init_graph(Queued, Graph)` monitors the client and
publishes on the queued node before entering `graph_loop/1`.

| Message to the worker | Effect |
|---|---|
| `#queued{ref = Ref, node = N}` | publish the holds on N (idempotent for a node already queued), N joins `nodes` |
| `{add, Ref, N, Manager}` | if N ∈ `nodes`: the row on N is removed and N leaves `nodes` (`41feb6a`: no removal for a node that never reported `#queued{}`); `{Scope, Term, N} => Manager` joins the holds and is published alone to the remaining queued nodes |
| `{remove, Ref}` | the rows of every queued node are removed; exit `normal` |
| `#deadlock{ref = Ref} = D` | the rows are removed; `exit({Tag, D})` |
| `{'DOWN', _, process, Client, _}` | the rows are removed; exit `normal` |

Publication is `ecall:call(N, elock_graph, handle_add_edges,
[#add_edges{lock = {Scope, Term, N}, ref, birth, age, client = self(),
holds}])` and happens only when the holds are not empty: a worker without
holds (a multi-node request whose client holds nothing, before its first
grant) publishes nothing until a grant arrives. Removal is
`ecall:cast(N, elock_graph, handle_remove_edges, [#remove_edges{lock, ref}])`.
The row names the graph worker as `client`; walkers send `#deadlock{ref,
winner}` to it.

`handle_add_edges/1` on an existing row of the same lock and ref merges the
new keys into the row and keeps the row's `client` as it was.

### 1.6 The context

`#context{ref2lock, locked, counts, birth}`, `#lock{scope, term, nodes}`,
`#node{manager, holder}` are unchanged; only the holder rule of §1.1 is new.

### 1.7 What the compiler reports about the receives

As `elock_test_utils:recv_opt_info/1` lists them, in source order:

| Module | Entries |
|---|---|
| `elock_context` | `{{lock,4}, reserved_receive_marker}`, `{{lock,4}, passed_marker}`, `{{wait_verdict,2}, {used_receive_marker, {parameter,1}}}`, `{{wait_unlock,2}, {used_receive_marker, {parameter,1}}}`, `{{hold,3}, passed_marker}`, `{{holding,3}, unoptimized_selective_receive}` |
| `elock_manager` | `{{lock,2}, reserved_receive_marker}`, `{{lock,2}, passed_marker}`, `{{wait_verdict,4}, {used_receive_marker, {parameter,1}}}` twice (the outer receive and the nested one of the `'DOWN'` clause), `{{loop,1}, unoptimized_selective_receive}` |
| `elock_graph` | `{{loop,0}, unoptimized_selective_receive}`, `{{graph_loop,1}, unoptimized_selective_receive}` |

### 1.8 Records

- `#graph{}` lives in `include/elock.hrl` now, with a `tag` field.
- `#request{}` has `notify_queued`.
- `#withdrawn{ref}` is private to `elock_manager`, next to `#locked{}`,
  `#timeout{}`, `#retry{}`.

## 2. What is broken today

Established by compiling the test modules against `c4f61ef` with
`export_all` and cross-referencing them, then by reading.

| Where | Symptom | Cause |
|---|---|---|
| `elock_SUITE` | does not compile | its `-record(graph, ...)` mirror clashes with the header's |
| `elock_SUITE` | `graph_worker/3` calls `elock_context:init_graph/1` | moved to `elock_graph:init_graph/2`, which takes the first `#queued{}` and a `#graph{}` with `tag` |
| `elock_SUITE` | every `wait_verdict_*` case would crash with `function_clause` | the `#waiting{}` mirror lacks `holds`, `birth`, `age`; the exit reason of the graph worker is `{Ref, #deadlock{}}`, not `#deadlock{}` |
| `elock_SUITE` | `request_fields_test`, `queued_publishes_holds_test` assert a worker proxy for a holding client | §1.1: direct path; `#request{}` has `notify_queued` |
| `elock_SUITE` | `receive_marker_test` | `graph_loop/1` left the module (§1.7) |
| `elock_manager_SUITE` | eight calls of `elock_manager:lock/1`, one of `kill_proxy/1` | `lock/2`, `release_proxy/1` |
| `elock_manager_SUITE` | every expectation of `#queued{}` at a proxy times out | `request/5` builds requests without `notify_queued` |
| `elock_manager_SUITE` | `handle_down_test` fails on `?NO_MESSAGE` | the dying client's pending upgrade now gets `#withdrawn{}` (§1.4) |
| `elock_manager_SUITE` | `receive_marker_test` | §1.7 |
| `elock_test_utils` | `elock_process/1` misses direct waiters and graph workers; `graph_walkers/0` counts graph workers as walkers; `lock_nodes/3` asserts a worker pid for a holding single-node client | stale MFAs `{elock_manager, wait_verdict, 3}` and `{elock_context, graph_loop, 1}`; the worker-path assumption |
| `elock_test_utils_SUITE` | `quiet_sees_graph_work_test` | `graph_walkers/0` above |
| `elock_locking_SUITE` | `holding_client_request_path_test`, `waiting_client_mailbox_test`, every `lock_nodes` caller | §1.1 |
| `elock_deadlock_SUITE` | `upgrade_queued/4` waits for `{elock_context, wait_verdict, 2}`; `lock_nodes` callers | §1.1 |
| `elock_multi_node_SUITE` | `proxies/1` looks for `wait_verdict/3`; `lock_nodes` callers; comments describe the old path | §1.1 |
| `elock_graph_SUITE`, `elock_probe_limit_SUITE`, both concurrency suites | compile; as far as reading goes they run | the graph worker cases of `elock_SUITE` belong next to `init_graph/2` now |

## 3. Shared test code

### 3.1 `test/functional/elock_test.hrl`

No change is required. The `#waiter{}` mirror matches. A mirror of
`#withdrawn{}` is added where the suites mirror the other manager records
(`elock_SUITE`, `elock_manager_SUITE`, `elock_locking_SUITE`,
`elock_multi_node_SUITE`), not here.

### 3.2 `test/functional/util/elock_test_utils.erl`

- `elock_process/1`: the census of the quiet check names
  `{elock_manager, wait_verdict, 4}` and `{elock_graph, graph_loop, 1}`
  instead of the two stale MFAs. It must include a direct waiter, a
  multi-node client, a draining client, a graph worker, the graph process
  and the managers.
- `graph_walkers/0`: a graph worker is not a walker. Walkers are the
  processes other than the table owner whose stack names `elock_graph` and
  whose current function is not `graph_loop/1`: the `launch/3` processes
  spawned by `handle_add_edges/1` and the callbacks executing
  `handle_probe/1` or `handle_add_edges/1`. `wait_graph_idle/1` keeps its
  meaning (empty table, no walker) and `finish_scope/1` keeps using it.
- `lock_nodes/3`: the mode argument (`held_nothing` | `held_something`)
  goes; the path no longer depends on holds. `lock_nodes(Client, Ref,
  Managers)` checks, per node of the lock: a local node of a lock on that
  node alone has `holder = undefined` and its manager monitors the client;
  a local node of a lock on several nodes has a dead worker pid of the
  client's node as holder and its manager monitors the client; a remote
  node has a live holder on the client's node that monitors the client. It
  returns the `#node{}` map for the expected context, as today.
- The single-node suites stop calling `lock_nodes/3`: on one local node the
  expected `nodes` map is `#{Node => #node{manager = Manager}}`, written
  literally, as the first lock of every test already does. `lock_nodes/3`
  stays for the multi-node suite and the cases of `elock_SUITE` that name
  remote nodes.
- `lock_queued/6`, `taken/5`, `published/4`, `settled/2`, `quiet/1` keep
  their mechanics: the ticket is taken, the manager monitors a local client,
  the row at the lock carries the complete held-key set and its `client`
  monitors the requesting client, every elock process is at rest. The
  header comment describes the chain as it is now: manager → proxy (the
  client itself on its own node, a worker's callee otherwise) → client →
  graph worker → the lock node's table → walker.
- Everything else (scopes, clients, collectors, waits, `recv_opt_info/1`,
  peers) stays.

### 3.3 `test/functional/util/distributed_tests_utils.erl`

No change.

## 4. The suites

Every suite keeps its structure (groups, per-case scope, idle teardown).
Comments that describe the old worker path of a holding client are
rewritten where they occur (headers of `elock_locking_SUITE`,
`elock_deadlock_SUITE`, `elock_multi_node_SUITE`; the comments of the cases
named below).

### 4.1 `elock_test_utils_SUITE`

- `quiet_sees_graph_work_test` keeps its scenario and now also pins §3.2:
  while a direct holding waiter is queued, its row names a graph worker
  that monitors the client, `graph_walkers/0` is `[]`, `quiet/1` is `true`;
  a suspended graph worker with a message in its mailbox makes `quiet/1`
  false; after resume the repeated `#queued{}` leaves one row.
- The other cases stay.

### 4.2 `elock_SUITE` (elock, elock_context, elock_scope)

Mirrors: drop `#graph{}` (it comes from the header); complete `#waiting{}`
(`ref, scope, term, pending, nodes, queued, holds, birth, age, graph,
graph_monitor`); add `#withdrawn{}`. The `waiting/3,4` helper fills
`holds = #{}`, an integer `birth`, `age = 1`.

Validation, compatibility, context and scope groups: unchanged, except that
`#request{}` expectations name `notify_queued` and the `lock_nodes` calls
of single-node contexts become literal maps (§3.2).

Request group:

- `request_fields_test` asserts the whole `#request{}` the manager
  receives through `elock:lock/3,4` for the three combinations of §1.1:
  nothing held on one node → `proxy = Client`, `notify_queued = false`;
  something held on one node (also `[Node, Node]`, which collapses) →
  `proxy = Client`, `notify_queued = true`; nothing held on two distinct
  nodes (the local fake manager plus an unreachable node) → the proxy is
  not the client, `notify_queued = true`; the test grants the local leg
  with `{Tag, #locked{}}`, the request fails with `{error, {badrpc, _}}`
  and the fake manager receives the `#unlock{}` of the released grant.
  Every attempt has a tag of its own, distinct from the ref. The unlocks
  reach the fake manager as today.
- `verdicts_test`: unchanged apart from `notify_queued = false` in the
  expected requests.
- `queued_publishes_holds_test` is rewritten for the direct path and must
  cover, with the test process as the manager of `t2` and a client holding
  `t0`:
  1. the tagged `#queued{}` sent to the client itself makes it publish: one
     row at `{Scope, t2, Node}` with the ref, the context birth, the held
     map of the context, and a `client` that is neither the client nor the
     test process and monitors the client;
  2. grant: `{Tag, #locked{}}` → `{ok, Ref}`, the row is gone, the graph
     worker is dead, the key is in the context;
  3. a manager verdict `{Tag, #deadlock{}}` → `{error, {deadlock, W}}`,
     the context is as before, the row is gone, no `#unlock{}` reaches the
     manager (the manager never registered the request);
  4. a graph verdict: `#deadlock{ref = Ref, winner = W}` sent to the row's
     `client` → the manager receives `#unlock{ref = Ref}` and answers
     `{Tag, #withdrawn{ref = Ref}}` → `{error, {deadlock, W}}`, the context
     is as before, the row is gone, the client's mailbox is empty;
  5. the same with `{Tag, #locked{ref = Ref}}` as the one reply after the
     unlock, and with `{Tag, #timeout{ref = Ref}}`: the same result, and
     nothing else is consumed from the client's mailbox.
- `fake_manager_dies_test`: unchanged.

Multi-node client group (hand-built `#waiting{}`, collectors as managers):

- All `wait_verdict_*` cases keep their expectations with the completed
  record. `wait_verdict_deadlock_verdict_test` makes the collector exit with
  `{Ref, #deadlock{winner = W}}`; an exit with `#deadlock{}` alone, with
  another tag, or `kill` is not a verdict and the request goes on.
- New, `wait_verdict_spawns_graph_test`: `graph = undefined`, the local node
  pending with the test process as its fake manager, `holds` naming a key
  of the context. The first `#queued{}` for a pending node spawns the
  graph worker: a row at `{Scope, t, node()}` with the context's holds ∪
  the grants received before it; a later grant of another node is added
  (the row's holds grow by that key); `{ok, Nodes}` stops it (row gone,
  worker dead, no stray message). A second run with the worker's
  `#deadlock{ref = Ref, winner = W}`: `{error, {deadlock, W}}`, the queued
  manager gets `#unlock{}`, the pending workers are drained as in
  `wait_verdict_failure_test`.
- New, `wait_verdict_queued_after_result_test`: a `#queued{}` of a node
  that has already returned is dropped: nothing reaches the graph, nothing
  reaches that manager.
- `receive_marker_test` expects the `elock_context` list of §1.7.
- `single_remote_node_unreachable_test`,
  `unreachable_node_releases_granted_node_test`: unchanged.

Graph worker group: removed from this suite; it moves to
`elock_graph_SUITE` (§4.4).

### 4.3 `elock_manager_SUITE`

Mirrors: add `#withdrawn{}`. The `request/5` helper gains the
`notify_queued` option (default `false`), and every case that expects
`#queued{}` at a proxy builds its request with it `true`
(`waiting_request_proxy_test`, `handle_unlock_waiter_test`,
`postponed_asked_when_taken_test`, `untagged_request_test`,
`barging_queued_test`, `manager_tagged_replies_test`,
`handle_nodedown_test`, `lock_queued_notice_test`,
`manager_dies_after_queued_test`).

Client side group:

- `lock_free_term_test`, `manager_dies_before_verdict_test`,
  `manager_lifecycle_test`, `manager_new_round_test`,
  `manager_ignores_unexpected_message_test`, `manager_tagged_replies_test`:
  `elock_manager:lock(Request, OnQueued)` with a callback that must not be
  called (it sends to the test process; `?NO_MESSAGE` covers it).
- `lock_busy_term_test`: unchanged in substance, through `lock/2`.
- `lock_queued_notice_test` becomes the callback test: the tagged
  `#queued{}` of the manager reaches the callback as the bare `#queued{}`
  with the tag of the attempt, in the proxy process, before the grant; a
  `#queued{}` under another tag or a bare one is left in the proxy's
  mailbox; the callback's return value is ignored.
- `manager_dies_after_queued_test`: the callback is called under the first
  tag, the manager dies, the retry sends a new request under a new tag and
  the grant under the new tag is accepted.
- New, `direct_waiter_deadlock_test`: the test process is the manager, the
  proxy is the client (a util client calling `lock/2`), the callback
  spawns a monitored stand-in that the test makes exit with
  `{Tag, #deadlock{winner = W}}` after the `#queued{}`. The client sends
  `#unlock{ref = Ref}` to the manager and then returns
  `{error, {deadlock, W}}` after exactly one tagged reply, in three
  variants: `#withdrawn{}`, `#locked{}`, `#timeout{}`. After the return the
  manager is not monitored by the client any more, the client's mailbox
  holds nothing of the attempt. A `'DOWN'` carrying `{OtherTag,
  #deadlock{}}` or another reason is not a verdict: the client keeps
  waiting and the message stays in its mailbox.
- `receive_marker_test` expects the `elock_manager` entries of §1.7
  filtered to `lock` and `wait_verdict`: four entries.
- `get_manager_test`: unchanged.

Requests and queue group:

- `kill_proxy_test` becomes `release_proxy_test` with the three cases of
  §1.4; the `#withdrawn{}` carries the request's tag and ref.
- `notify_queued_test`: sent iff the flag is true and the proxy is a pid,
  to the proxy whether or not it is the client, with the request's tag
  (`undefined` included); not sent with the flag false, not sent to
  `proxy = undefined`.
- `handle_down_test`: the dying client's pending upgrade (proxy = client)
  produces `{Tag3, #withdrawn{ref = Ref3}}` at that collector; the state
  expectations stay.
- `handle_nodedown_test`, `remote_client_monitor_test`,
  `reset_keeps_node_monitors_test`: unchanged apart from the flag; the
  remote proxies are workers and are killed.
- `waiting_request_proxy_test`, `handle_unlock_waiter_test`,
  `barging_queued_test`: unchanged apart from the flag (their proxies are
  collectors other than the client: killed).
- New, `withdraw_direct_waiter_test`: `handle_unlock/2` of a queued request
  whose proxy is its client sends `{Tag, #withdrawn{ref}}` to it, dequeues
  it and pushes the queue; the same for a pending upgrade withdrawn by
  `handle_unlock/2`; `handle_timeout/2` sends `#timeout{}` only, no
  `#withdrawn{}`.
- Everything else in the group is unchanged.

### 4.4 `elock_graph_SUITE`

Existing cases: unchanged. New group `graph_worker` (from `elock_SUITE`),
spawning `elock_graph:init_graph(#queued{ref, manager = self(), node =
node()}, #graph{client, scope, term = t, nodes = [], holds, birth = 1,
age = 1, ref, tag})` under `spawn_monitor`, with a closer row planted so
that a publication shows as a verdict at a collector:

- `graph_worker_publishes_on_start_test`: the row is at `{Scope, t,
  node()}` as soon as the worker is up, with the start holds and the
  worker as `client`; a repeated `#queued{}` for the same node leaves one
  row and launches nothing new; with empty holds no row ever appears and
  `quiet/1` holds.
- `graph_worker_grant_test`: `{add, Ref, OtherNode, M}` for a node that
  never reported `#queued{}` adds the key to the row's holds and removes
  nothing; `{add, Ref, node(), M}` removes the row of the local node; a
  later `#queued{ref, node = node()}` publishes the merged holds again.
- `graph_worker_grant_without_holds_test`: a worker started with empty
  holds and the local node queued publishes nothing; `{add, Ref, Other,
  M}` makes the first row, holding that single key.
- `graph_worker_remove_test`, `graph_worker_deadlock_test`,
  `graph_worker_client_down_test`: rows gone; exit `normal`, `{Tag,
  #deadlock{}}` (the verdict record intact), `normal`.
- `handle_add_edges_test` additionally asserts that a merge keeps the row's
  `client` (§1.5, last paragraph).
- Optional: a `receive_marker_test` for `elock_graph` with the two
  `unoptimized_selective_receive` entries of §1.7.

### 4.5 `elock_probe_limit_SUITE`

No change is required. `probe_limit_cancellation_test` now exercises the
direct path end to end (`#deadlock{winner = probe_limit}` at the graph
worker → `wait_verdict/4` → `#unlock{}` → `#withdrawn{}`); its assertions
stay. `fixed_request_age_test` still sees the publications through the
trace of `ecall:call/4` because the graph worker is spawned by the traced
client. The header comment mentions the graph worker of `elock_graph`.

### 4.6 `elock_locking_SUITE`

- `holding_client_request_path_test` becomes `request_path_test` and pins
  §1.1 on one node: a holding waiter and a fresh waiter both wait in
  `elock_manager:wait_verdict/4`, no process is in
  `ecall_connection:call/4`; the holding waiter has one graph worker in
  `elock_graph:graph_loop/1`, which monitors the client and is the
  `client` of the row at the waited lock; the fresh waiter has no graph
  worker and no row; after the grant the worker is dead, the row is gone,
  the context's holder for the node is `undefined`.
- `waiting_client_mailbox_test`: the client is observed in
  `elock_manager:wait_verdict/4`; the mailbox keeps the bare verdicts, the
  replies under a foreign tag, the bare `#queued{}`, a `'DOWN'` under a
  foreign tag, and additionally a `'DOWN'` whose reason is `{ForeignTag,
  #deadlock{}}`; after the timeout, the grant and the unlocks the mailbox
  is exactly as it was.
- `processes_in/1` is used with the MFAs of §1.1.
- The `lock_nodes` calls become literal maps (§3.2).
- Everything else is unchanged.

### 4.7 `elock_deadlock_SUITE`

- `upgrade_queued/4` waits for the client in
  `{elock_manager, wait_verdict, 4}`.
- The `lock_nodes` call of `loser_keeps_other_locks_test` becomes a literal
  map.
- Everything else is unchanged; the header describes the graph worker as
  spawned on `#queued{}`.

### 4.8 `elock_concurrency_SUITE`

No change.

### 4.9 `elock_multi_node_SUITE`

- `proxies/1` looks for `{elock_manager, wait_verdict, 4}`.
- `lock_nodes/3` is called with the new arity everywhere (`holds_both/5`
  included).
- `single_remote_node_worker_test`: the last part ("the node of the client
  itself: no worker") holds whatever the client holds; the comment says
  so, and the case adds a holding client asking its own node alone: no
  worker, no proxy other than itself.
- `waiting_client_mailbox_remote_test`: the comment "holding u selects
  workers" becomes "a request naming a remote node runs through workers".
- Everything else is unchanged.

### 4.10 `elock_multi_node_concurrency_SUITE`

No change.

### 4.11 `test.spec`, `functional.config`

No change. The graph worker cases run in stage 1 with `elock_graph_SUITE`.

## 5. Coverage that must exist after the alignment

Collected from §4, one line each, so that nothing is lost in the rewrite:

1. The three `#request{}` shapes of §1.1 (`elock_SUITE`).
2. The callback contract of `lock/2` (`elock_manager_SUITE`).
3. `notify_queued/1` by the flag, proxy = client included
   (`elock_manager_SUITE`).
4. `release_proxy/1`, three cases, and `#withdrawn{}` on every withdrawal
   of a direct waiter (`elock_manager_SUITE`).
5. The direct waiter's graph verdict through `wait_verdict/4`, three
   replies, foreign `'DOWN'`s untouched (`elock_manager_SUITE` with a
   stand-in; `elock_SUITE` end to end with the real graph worker).
6. The lazy spawn of the graph worker on the first `#queued{}` in both
   paths, with the holds and grants it starts with (`elock_SUITE`,
   `elock_locking_SUITE`).
7. The graph worker's five messages, the no-removal for a node that never
   queued, the no-publication without holds (`elock_graph_SUITE`).
8. A `#queued{}` of a returned node is dropped (`elock_SUITE`).
9. The receive-marker lists of §1.7 for `elock_context` and
   `elock_manager`.
10. The census and walker definitions of §3.2 (`elock_test_utils_SUITE`).
11. The holder rule of §1.1 in the context (`elock_locking_SUITE`,
    `elock_multi_node_SUITE`).

## 6. Verification

- Build the tests against the sources with the test profile; the compile
  must be clean under `warnings_as_errors`.
- Run stage 1, stage 2 without `elock_concurrency_SUITE`
  (`elock_test_utils_SUITE`, `elock_SUITE`, `elock_graph_SUITE`,
  `elock_probe_limit_SUITE`, `elock_manager_SUITE`, `elock_locking_SUITE`,
  `elock_deadlock_SUITE`) and `elock_multi_node_SUITE` locally. Every
  case passes; `end_per_testcase` reports no leak.
- The two concurrency suites are run by the implementer at smoke sizes
  only (a `--config` override of the `concurrency` map: a few clients, a
  few rounds); their full sizes of `functional.config` are run by Roman.
