# Manager pool

Replaces the ETS ticket scheme of `elock_manager` with a fixed pool of
permanent manager processes, one partition of the term space each.

Status: design, for review. No performance assessment has been made yet.

## 1. Goal

Remove the ETS table of a scope and everything that exists only because
the ticket in ETS and the message to the manager are not atomic:

- `ets:update_counter/4` to take a ticket, `ets:lookup/2` to find the
  manager, the spin while the manager has not registered its pid yet;
- a manager process spawned by the first request and exited by the last
  unlock, with the delete-then-lookup race of `try_unlock/1`;
- the ordering of requests by ticket: `postponed`, the postpone timer,
  `#retry{}`, and the stepped-over ticket clauses.

A lock request becomes: hash the term to a pool slot, send the request to
that manager, wait for the verdict. The manager's mailbox is the order.

### 1.1 Principles

The code of this change follows four rules. They settle several questions
below, and they are the standard a review of the implementation applies.

1. **No unreachable checks.** A clause exists only for a case that can
   happen. A structure that a neighbouring module, or this one, can not
   produce is not matched against, and a function that is only called
   with a valid argument does not verify it. An invalid structure is a bug
   of the module that built it, to be fixed before the release, not to be
   tolerated at run time. Where this document keeps a tolerant clause, it
   names the reachable case that needs it.
2. **No defence against foreign messages.** Only the client side of this
   library and the managers send to a manager, and they send well-formed
   records. A malformed message from any other process is a bug of the
   user application. The one defence is the `Unexpected` clause of the
   manager loop, which logs and goes on.
3. **A manager does not die on its own.** It has no bugs. It stops with
   the node, or with the scope when the supervisor stops the scope. The
   one defence is the monitor the client takes on the manager for the
   duration of its call: a `'DOWN'` means the scope is gone.
4. **Performance first, happy path first.** The fast path of a lock is
   written for the common case, with no work spent on cases that
   principles 1 to 3 rule out: one lookup, one hash, one monitor, one
   send, one receive. Code style follows `elock_manager.erl` as it is: a
   `-spec` per function, records matched in the function heads, the
   comment banners, and the names of the functions that survive.

## 2. The pool

### 2.1 Start

`elock_scope:start_link(Scope)` spawns the scope process, which:

1. registers itself as `Scope`. A second start of the scope on the node
   fails here;
2. spawns `Size` managers with `spawn_opt(elock_manager, init, [Scope],
   [link, {priority, high}, {message_queue_data, off_heap}])`;
3. publishes the pool as a tuple of pids:
   `persistent_term:put({elock_scope, Scope}, Pool)`;
4. starts and joins `pg` as today;
5. answers `{ready, self()}` to the caller and sleeps forever.

`start_link` waits for the ready message, so the pool is published before
the supervisor continues. On a registration failure the scope process
unlinks the caller, answers `{error, self(), {already_started, Pid}}` and
exits normally; `start_link` returns `{error, {already_started, Pid}}`.
This is the handshake of `ecall_connection:start_link/1`.

`Size` is `erlang:system_info(schedulers_online)`. It is always an integer,
while `logical_processors` can be the atom `unknown`, and it reflects the
schedulers the managers can actually run on.

The pool is a tuple, not a map: `element(phash2(Term, Size) + 1, Pool)`
reads a pid out of the literal area without a copy.

### 2.2 Lookup

```erlang
%% elock_scope
-spec manager(atom(), term()) -> pid().
manager(Scope, Term)->
  Pool = persistent_term:get({elock_scope, Scope}),  % error:badarg if the scope is not started
  element(erlang:phash2(Term, tuple_size(Pool)) + 1, Pool).
```

The hash runs on the node that owns the lock: a request for a remote node
is executed there by the ecall worker (`elock_manager:lock/1`), so every
node hashes with its own pool size and its own pool. Pool sizes may differ
between nodes.

`erlang:phash2/2` distinguishes `1` and `1.0`, and so does a map key, so
the documented `=:=` identity of terms holds whichever slot a term lands
in.

### 2.3 Lifecycle

The managers are linked to the scope process and the scope process does
not trap exits. The scope stops, its managers stop: no manager outlives
its scope. Every lock of the scope on the node is lost and the holders
are not notified, which is the failure the documentation already
describes for a scope stop. By principle 3 there is no other way for a
manager to end, so there is no manager-level restart, no supervision of
the pool, and no handling of a half-alive pool.

The persistent term is not erased when the scope stops. A stale pool of
dead pids behaves like a missing one: the client monitors the pid, gets
`'DOWN'` with `noproc` at once, and raises `badarg` (see 3). The next
start replaces the entry.

Each `persistent_term:put/2` on an existing key triggers a global garbage
collection pass. A scope is published once per start, so this is a cost
of restarts only. One key per scope keeps scopes from rewriting each
other's entries.

## 3. The client side

`elock_manager:lock/2` becomes:

```erlang
lock(#request{scope = Scope, term = Term} = Request, HeldLocks)->
  Manager = elock_scope:manager(Scope, Term),
  MonitorRef = erlang:monitor(process, Manager),
  Manager ! Request#request{proxy = self(), tag = MonitorRef},
  Verdict = wait_verdict(MonitorRef, Manager, Request, HeldLocks),
  erlang:demonitor(MonitorRef, [flush]),
  Verdict.
```

`wait_verdict/4` keeps its `#locked{}`, `#queued{}`, `#deadlock{}` and
`#timeout{}` clauses. `#retry{}` is gone. The `'DOWN'` clause raises
`erlang:error(badarg)`: by principle 3 a dead manager means the scope is
stopped or restarting on this node. Today the retry loop ends with the same `badarg`
from `ets:update_counter/4` on the missing table, so the error is the one
the API documents, raised locally and returned as `{error, {exit, badarg}}`
through ecall for a remote node.

There is no retry. A client that retried blindly would send to the dead
pid in the stale pool, get `noproc` at once and spin until the scope
restarts. A client that retried only after the pool changed would take a
lock in a scope that has just lost every other lock, silently. Raising
tells the caller that something happened.

The monitor stays for two reasons: it is the reply tag that lets the
receive skip older messages, and it is the only way to learn that the
manager is gone.

`lock/1`, the worker variant for a remote node, is unchanged.
`elock_context` is unchanged apart from the record rename in 6.

## 4. Manager state

```erlang
%% One per pool slot. Lives with the scope.
-record(state,{
  scope    :: atom(),
  locks    :: #{term() => #lock{}},         % the live terms of this partition
  requests :: #{reference() => #req{}},     % every holder and waiter, all terms
  clients  :: #{pid() => #client{}},        % one monitor per client per manager
  seq      :: non_neg_integer()             % the last arrival number, replaces the ticket
}).

%% One per live term. Dropped from #state.locks when holders, queue and
%% barging are all empty (see put_lock/1).
-record(lock,{
  term      :: term(),                      % the canonical copy, see #req.term
  holders   :: #{reference() => {boolean(), pid()}},
  queue     :: gb_sets:set({pos_integer(), reference()}), % {Seq, Ref}, head is the smallest
  can_share :: boolean(),
  barging   :: reference() | undefined,     % the pending upgrade, its #req{} is in #state.requests
  graph     :: elock_graph:graph() | undefined
}).

-record(req,{
  client     :: pid(),
  ref        :: reference(),
  term       :: term(),                     % routes #unlock{}, #deadlock{}, timeouts to the #lock{}
  seq        :: pos_integer(),              % the arrival number, the sort key in #lock.queue
  proxy      :: pid() | undefined,          % undefined once the lock is held
  tag        :: reference() | undefined,    % undefined once the lock is held
  shared     :: boolean(),
  held_count :: non_neg_integer() | undefined,
  has_lock   :: boolean(),
  timer      :: reference() | undefined     % the timeout timer, while waiting
}).

-record(client,{
  monitor_ref :: reference(),
  locks       :: #{term() => #{reference() => true}}  % Term => the client's requests on it
}).
```

What moved, and why:

- `last`, `postponed`, `postpone_timer` are gone with the tickets. The
  manager's `seq` counter is the arrival order.
- `requests` is manager-wide. Every ref-carrying message finds its
  `#req{}` in one lookup and `#req.term` finds the `#lock{}`. A per-term
  map plus a ref-to-term index would be two structures for one answer.
- `#client.requests` became `#client.locks`, grouped by term. The old
  `Shared` value was never read. `only_holder/3` and `client_holds_lock/2`
  need the client's refs on one term, which is the inner map;
  `handle_down/2` folds the outer map. The client is demonitored when the
  outer map is empty, so a client with locks in several terms costs one
  monitor per manager, fewer than today.
- `#lock.term` is the canonical copy of the term. Every `#request{}`
  message brings its own copy into the manager heap. `#req.term` and the
  keys of `#client.locks` are taken from `#lock.term`, never from the
  message, so a deep queue on a large term retains the term once.
- `#lock.barging` is a ref. `next/1` fetches the `#req{}` by ref anyway and
  `dequeue/2` only matches the ref.

Routing of every inbound message:

| Message | Lookup |
|---|---|
| `#request{term}` | `locks` by term, created if absent |
| `#unlock{ref}`, `#add_held_locks{ref}`, `#deadlock{ref}`, `{timeout, _, {timeout, Ref}}` | `requests` by ref, then `locks` by `#req.term` |
| `#deadlock_probe{target}` | `locks` by the term of `target`; absent means no waiters |
| `{'DOWN', _, process, Pid, _}` | `clients` by pid, then every term of `#client.locks` |

A ref missing from `requests` is reachable for four messages, and only
for them, so only their handlers keep the tolerant clause:

- the timer message: a timer cancelled on the grant may have fired
  already;
- `#deadlock{}`: the answer to a probe arrives after the request was
  granted or left;
- `#add_held_locks{}`: the client answers `#queued{}` after the request
  was granted or left;
- `#unlock{}`: a multi-node request that fails releases every manager it
  is queued at, including the one whose timeout or deadlock verdict ended
  it (`elock_context:wait_verdict/2`).

`#request{}` and `'DOWN'` do not look a ref up. A probe for a term that
has no `#lock{}` any more is the same case one level up, see 6.

## 5. Handler contract

The loop keeps its shape: one message in, one top-level handler, one
`#state{}` out. The postpone timer clause disappears.

```erlang
%% Layer 1: top-level handlers. Route, then write back.
-spec handle_request(#request{}, #state{}) -> #state{}.
-spec handle_unlock(reference(), #state{}) -> #state{}.
-spec handle_timeout(reference(), #state{}) -> #state{}.
-spec handle_deadlock(#deadlock{}, #state{}) -> #state{}.
-spec handle_add_held_locks(#add_held_locks{}, #state{}) -> #state{}.
-spec handle_deadlock_probe(#deadlock_probe{}, #state{}) -> #state{}.
-spec handle_down(pid(), #state{}) -> #state{}.

%% Layer 2: the per-term core. {#lock{}, #state{}} in and out.
-type ls() :: {#lock{}, #state{}}.
-spec add_request(#request{}, ls()) -> ls().
-spec remove_request(#req{}, ls()) -> ls().
-spec abort_waiter(reference(), lock_key(), ls()) -> ls().  % the body of today's handle_deadlock/2
-spec enqueue(#request{}, ls()) -> ls().
-spec dequeue(#req{}, ls()) -> ls().
-spec grant(#request{}, ls()) -> ls().                      % today's get_lock/2, renamed
-spec locked(#req{}, ls()) -> ls().
-spec unlocked(#req{}, ls()) -> ls().
-spec try_barging(#request{}, ls()) -> ls().
-spec next(ls()) -> ls().

%% Layer 3: the resolvers and the write-back.
-spec get_lock(term(), #state{}) -> #lock{}.                % existing, or a fresh empty one
-spec get_req(reference(), #state{}) -> {#req{}, #lock{}} | undefined.
-spec put_lock(ls()) -> #state{}.
```

The rules:

1. **A handler loads one `#lock{}`, threads it through the core and calls
   `put_lock/1` exactly once.** The core never reads or writes
   `#state.locks`. Two copies of a lock in flight, one in the handler and
   one in the map, is the only way the two-level structure can lose an
   update, and this rule makes it impossible.
2. **A top-level handler never calls another top-level handler.** Today
   `handle_deadlock_probe/2` aborts closers by calling `handle_deadlock/2`
   in a fold. Under rule 1 that would reload the lock from the map for the
   second closer and lose the first abort. Hence `abort_waiter/3` in the
   core, used by the probe handler and by `handle_deadlock/2`.
3. **The core updates `requests` and `clients`, the handlers only route.**
   The manager-wide maps change in `locked/2`, `unlocked/2`, `enqueue/2`
   and `dequeue/2`, exactly where the per-term maps changed before. The
   port of each core function is a split of one `#state{...}` match into a
   `#lock{...}` match (holders, queue, can_share, barging, graph) and a
   `#state{...}` match (requests, clients).
4. **`put_lock/1` is the only place a term is created or dropped.**

   ```erlang
   put_lock({#lock{term = Term, holders = Holders}, #state{locks = Locks} = State})
     when map_size(Holders) =:= 0->
     State#state{locks = maps:remove(Term, Locks)};
   put_lock({#lock{term = Term} = Lock, #state{locks = Locks} = State})->
     State#state{locks = Locks#{Term => Lock}}.
   ```

   "No holders" is the whole condition. Every path that removes a holder
   or a waiter ends in `next/1`, and `next/1` with no holders grants the
   head of the queue, so after any handler an empty `holders` implies an
   empty queue and no barging request. The graph is `undefined` by then
   as well: `remove_edges/2` runs for every leaving waiter. By principle 1
   the queue and the barging field are not checked. This one function
   replaces `try_unlock/1`, the demonitor and `kill_proxy` dance of the
   single-holder clause of `handle_unlock/2`, and the state reset.
5. **`seq` is stamped in `handle_request/2` and nowhere else.**

   ```erlang
   handle_request(#request{term = Term} = Request, #state{seq = Seq0} = State0)->
     Seq = Seq0 + 1,
     Lock = get_lock(Term, State0),
     put_lock(add_request(Request#request{seq = Seq}, {Lock, State0#state{seq = Seq}})).
   ```
6. **The canonical term.** `new_req/2` and `add_client_request/4` take the
   term from `#lock.term`, never from the message.

Sketches of the routing handlers:

```erlang
handle_unlock(Ref, State)->
  case get_req(Ref, State) of
    {Req, Lock} -> put_lock(remove_request(Req, {Lock, State}));
    undefined   -> State
  end.

handle_down(Pid, #state{clients = Clients} = State0)->
  case Clients of
    #{Pid := #client{locks = Locks}} ->
      maps:fold(
        fun(Term, Refs, StateAcc)->
          LS = maps:fold(
            fun(Ref, true, {LockAcc, SAcc})->
              Req = maps:get(Ref, SAcc#state.requests),
              remove_request(Req, {LockAcc, SAcc})
            end,
            {maps:get(Term, StateAcc#state.locks), StateAcc},
            Refs),
          put_lock(LS)
        end,
        State0, Locks);
    _ -> State0
  end.

get_lock(Term, #state{locks = Locks})->
  case Locks of
    #{Term := Lock} -> Lock;
    _ -> #lock{term = Term, holders = #{}, queue = gb_sets:empty(),
               can_share = true, barging = undefined, graph = undefined}
  end.

get_req(Ref, #state{requests = Requests, locks = Locks})->
  case Requests of
    #{Ref := #req{term = Term} = Req} -> {Req, maps:get(Term, Locks)};
    _ -> undefined
  end.
```

`handle_down/2` is the one handler that touches several terms. It does so
as a sequence of complete load, core, write-back cycles, one term at a
time, over a snapshot of the client's terms. `remove_client_request/3`
only shrinks the live entry and finally demonitors. A term in
`#client.locks` always has its `#lock{}`, so the lookup is a plain
`maps:get/2`; `get_lock/2` with its fresh-lock clause is for
`handle_request/2` alone, where a new term is the common case. The
`'DOWN'` of a pid that is not in `clients` is reachable: the monitor is
dropped with `erlang:demonitor/1` without `flush`, so a `'DOWN'` already
in the mailbox stays there.

A fresh `#lock{}` has `can_share = true`, and `locked/2` computes
`Shared andalso CanShare0`, so the first grant sets `can_share` to the
request's mode, as `init/1` did.

The core is a pure function of `{#lock{}, #state{}}`, so the unit tests
of `elock_manager_SUITE` keep their style: build the records, call the
core, assert on both, with no fixtures.

## 6. Deadlock probes

Today a manager owns one term, so "this manager has seen the probe" and
"this lock has been probed" are the same thing, `sent_to` is keyed by
pid, and `run_probe/4` and `forward/2` never send to `self()`. With a
pool, two terms of one cycle can share a manager, and that cycle would
never be probed. The identity of a probe destination becomes the lock key.

```erlang
-record(deadlock_probe,{
  ref     :: reference(),           % the origin request
  edge    :: lock_key(),            % the lock the origin waits for
  target  :: lock_key(),            % NEW: the lock this copy is addressed to
  manager :: pid(),                 % the origin manager, gets the #deadlock{} reply
  weight  :: non_neg_integer(),
  sent_to :: #{lock_key() => true}  % was #{pid() => true}
}).
```

`run_probe/4` seeds `sent_to` with `edge` instead of `self()`. The barging
case, where the origin holds the term it waits for, is covered the same
way as before: that key is already in `sent_to`, so no probe goes out for
it. Every other held key gets a probe addressed to its manager, and
`self()` is no longer excluded:

```erlang
run_probe(Ref, Edge, Held, Weight)->
  SentTo = maps:merge(#{Edge => true}, maps:from_keys(maps:keys(Held), true)),
  Probe = #deadlock_probe{ref = Ref, edge = Edge, manager = self(),
                          weight = Weight, sent_to = SentTo},
  maps:foreach(
    fun(Key, Manager) when Key =/= Edge ->
          ecall:send(Manager, Probe#deadlock_probe{target = Key});
       (_Edge, _Self) ->
          ok
    end,
    Held).
```

`forward/2` builds its targets as key-to-manager pairs, one message per
key, and merges all target keys into `sent_to`:

```erlang
Targets = #{ Key => Manager ||
             {_Weight, Held} <- maps:values(Index),
             Key := Manager <- Held,
             not is_map_key(Key, SentTo) },
```

`elock_graph:probe/3` loses its `LocalEdge` argument: it is `target`.

The probe handler loads the lock of the target term:

```erlang
handle_deadlock_probe(
    #deadlock_probe{target = {_Scope, Term, _Node}, edge = Winner} = Probe,
    #state{locks = Locks} = State
)->
  case Locks of
    #{Term := Lock}->
      LS =
        case elock_graph:probe(Probe, Lock#lock.graph) of
          {forward, Closers}->
            {Lock1, State1} = lists:foldl(
              fun(Ref, Acc)-> abort_waiter(Ref, Winner, Acc) end,
              {Lock, State}, Closers),
            elock_graph:forward(Probe, Lock1#lock.graph),
            {Lock1, State1};
          stop->
            {Lock, State}
        end,
      put_lock(LS);
    _->
      % The waiters have left meanwhile: nothing to close, nothing to forward
      State
  end.
```

A probe is sent for a hold that existed when the origin queued. By the
time it arrives, every waiter of the target term may have left and the
term with them. That is the one reachable reason for a missing `#lock{}`
here, and the second clause is for it. Building a fresh lock to run the
empty case through `probe/2` and `put_lock/1` would be work for nothing.

The `#deadlock{}` reply to the origin manager is unchanged. It carries
only the ref, and `get_req/2` routes it, whether the origin manager is
another process or the same one.

`ecall:send/2` to a local pid is a plain send, so a self-addressed probe
needs nothing from ecall.

Why self-send through the mailbox, not an inline check:

1. Rule 1 of the contract. An inline check of another term would hold two
   `#lock{}` records in one handler, and the probe handler mutates:
   aborting a closer runs `next/1`, which can grant.
2. Equivalence with the remote case. A self-sent probe is processed after
   whatever is already in the mailbox, the timing a probe from another
   node gets. The algorithm already tolerates that: every probe is checked
   against the graph as it is when the probe is handled, and
   `check_cycles/4` keeps its own-ref and stale-hold skips.
3. Cost. One local message into an off-heap mailbox. A manager bounces a
   probe between its own terms at most once per distinct key on the path,
   so the flood is finite by the same argument as before.

`check_cycles/4` matches a closer's held entry as `#{Edge := Manager}`
with `Manager` the origin manager pid. This still holds: the origin
manager is the manager of `Edge`, and it is stable. The "stale hold"
meaning of a different pid narrows to "the scope restarted on that node",
which is the only way a manager pid changes under principle 3. `new_held_locks/2` and the
stale checks of `elock_context` keep working for the same reason.

## 7. Ordering

The BEAM delivers the signals between a pair of processes in order. A
client sends its request, its `#add_held_locks{}` and its `#unlock{}` to
the one manager of the term, so they arrive in the order they were sent.
Requests from different clients arrive in some order, and that order is
the one the manager serves them in: `seq` is assigned on arrival and is
the key of `#lock.queue`. The documented rule, "served on each node in the
order they arrive", is unchanged; the ticket was an emulation of it.

Messages that already travel by independent paths today, such as a remote
`#unlock{}` through ecall against a request through an ecall worker, keep
their current tolerance: a ref that is unknown, already granted, or
already gone is ignored.

## 8. Records and the wire

- `#request.queue` becomes `#request.seq`. It is `undefined` on the wire
  and stamped by the manager on arrival, the way `proxy` and `tag` are
  filled by the proxy before the send. The "ticket must stay first" rule
  in `elock.hrl` goes: nothing sorts `#request{}` records any more.
  Alternative: drop the field and thread `Seq` through `add_request/2`,
  `enqueue/2`, `grant/2` and `new_req/2`. One field smaller on the wire,
  four signatures longer.
- `#deadlock_probe{}` gains `target`, `sent_to` is keyed by lock key.
- `#retry{}` is removed. It was manager-internal.
- `#request{}` and `#deadlock_probe{}` travel between nodes. A cluster
  that mixes this version with the previous one breaks on them. This is
  true of every record change in the project and is noted, not solved.

## 9. Removed from elock_manager

`start_manager/1`, `init/1` as the per-term constructor, `get_manager/3`,
`try_unlock/1`, `handle_postponed/1`, `handle_postpone_timeout/2`,
`arm_postpone_timer/1`, `cancel_postpone_timer/1`,
`postpone_timer_fired/1`, the stepped-over clause of `handle_request/2`,
the `retry` verdict and `#retry{}`, the `ets:match_delete/2` on `'DOWN'`,
`POSTPONE_TIMEOUT`, and the single-holder clause of `handle_unlock/2`.

`elock_scope` loses the ETS table and gains `manager/2`, the handshake,
the registration and the pool.

## 10. Behaviour visible to users

Documentation of `elock` to update:

- `start_link/1`: a second start on a node returns
  `{error, {already_started, Pid}}` instead of `{ok, Pid}` with a process
  that exits with `badarg`. `Scope` names a registered process instead of
  a named table.
- A request that is waiting when the scope stops gets `badarg` on the
  local node and `{error, {exit, badarg}}` through a remote node, at once.
  Today the managers are not linked to the scope and outlive it until they
  touch the table, so a waiter keeps waiting and ends with a late `badarg`
  or even a grant in a stopped scope. The wording of `lock/4` can say the
  new behaviour directly.

Nothing else in the public API changes.

## 11. Performance, to be measured, not assumed

- The fast path is one `persistent_term:get/1`, one `phash2/2`, one
  monitor, one send and one receive, with no process spawn and no ETS
  write. Today an uncontended lock spawns and exits a process.
- `priority high` on N permanent processes, one per scheduler, can delay
  every normal process on the node during a lock storm. The storm limits
  itself, since the clients are the producers, but latency for unrelated
  processes is a new effect. Keep the priority, measure it.
- Two hot terms in one slot share a mailbox, where today each had its own
  manager. A pool size override in the application environment is cheap
  insurance if that shows up; it is not part of this change.
- Permanent managers accumulate heap and do major garbage collections,
  which pause every term of the slot. Short-lived managers never did. The
  off-heap mailbox keeps bursts out of it; `fullsweep_after` is the knob
  if it matters.
- A probe of a cycle through k keys on one manager is k local messages
  instead of one, and `sent_to` carries keys instead of pids, so a probe
  message grows with the number of locks on the path rather than the
  number of managers.

## 12. Tests

An introspection hook replaces the table: `elock_manager:state/1` returns
the `#state{}` of a manager, implemented as a message handled by the loop.
It is exported for tests and debugging, like `export_all` already exposes
the internals under the test profile.

- `elock_test_utils`: `locks/1,2` is rebuilt on `elock_manager:state/1`
  across the pool. The ticket in `{Term, Manager, Ticket}` has no
  equivalent; the 162 assertions on it in the suites assert on what they
  mean, the holders and the waiters of a term. `manager/3` becomes
  `elock_scope:manager/2` on the node, and `wait_manager/2,3` becomes a
  plain lookup: the slot of a term is known before any request.
  `managers/0` keeps working: the pool managers sit in
  `elock_manager:loop/1`. `start_scope/2` waits for the registered name
  and `is_ready/2` instead of the table; `stop_scope/1` waits for the name
  to be gone.
- `elock_manager_SUITE`: the fixtures that insert `{Term, Self, Ticket}`
  into the table become a pool of one published under the test scope,
  `persistent_term:put({elock_scope, TestCase}, {self()})`, so the test
  process plays the manager exactly as before. The ticket and retry cases
  go; the state transformation cases are rewritten against
  `{#lock{}, #state{}}`. `end_per_testcase` erases the key.
- `elock_graph_SUITE`: `sent_to` by key, `probe/2`, one message per key
  in the forwarding assertions.
- `elock_concurrency_SUITE`: the manager churn case kills managers while
  clients lock and expects every request to succeed. By principle 3 a
  manager is not killed on its own, so the case becomes a scope stop
  under load: the scope is stopped the way `stop_scope/1` does it, the
  requests in flight get `badarg`, no manager is left behind, and after a
  restart the next requests succeed. The stale entries check goes with
  the table.
- The multi-node suites and `elock_deadlock_SUITE` change only through
  the helpers.

## 13. Alternatives considered

- **Per-slot restart on a manager crash**, with the pool republished.
  Moot under principle 3: a manager does not crash. It would also leave
  the holders of the slot believing they hold their locks while new
  requests are granted over them.
- **One merged graph per manager.** `elock_graph` keys edges by the held
  lock and assumes every waiter waits on the one term of the manager; a
  merged graph would mix waiters of different terms. Rejected: one graph
  per `#lock{}` leaves the module as it is.
- **Inline probing of a same-manager term.** See 6.
- **Carrying the term in `#unlock{}` and the timer message** instead of
  `#req.term`. Avoids one map but copies large terms into messages, some
  of them over the wire. Rejected.
- **`logical_processors` as the pool size.** Can be `unknown`. See 2.1.
- **Erasing the persistent term when the scope stops.** Needs
  `trap_exit`, is skipped by a kill, and a stale entry is harmless. Not
  done.

## 14. Open points for review

1. `#request.seq` on the wire versus a threaded argument (8).
2. `{error, {already_started, Pid}}` from `start_link/1` (10) is a change
   of documented behaviour, chosen over keeping the exit with `badarg`.
