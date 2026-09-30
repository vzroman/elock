# Lazy held locks

The optimistic lock path stops carrying the locks the client holds. A
manager asks for them only when the request has to wait, and it asks
the client, which has the map ready in its context.

## Motivation

Every `elock:lock/4` builds the held map (`maps:map/2` over
`#context.locked`) and ships it inside `#request{}`: into the manager's
mailbox or the spawn closure of a new manager on the local node, over
dist for a remote node, and once per node for a multi node request. The
manager reads it only when the request is queued
(`elock_graph:add_edges/2`). On the optimistic path - a free lock, a
shared join, a barging request - it is built, copied and dropped.

Measured per request, N = locks held by the client (OTP 27):

| N     | build (`maps:map`) | copy to a process | `term_to_binary` | bytes on the wire |
|-------|--------------------|-------------------|------------------|-------------------|
| 100   | 17 us              | 7.5 us            | 14.5 us          | 6.3 KB            |
| 1000  | 116 us             | 35 us             | 101 us           | 65 KB             |
| 10000 | 1.8 ms             | 0.5 ms            | 0.95 ms          | 659 KB            |

An uncontended local lock costs a few microseconds. With a hundred
held locks the map dominates it; remotely it is 6 KB per node per
request.

## Overview

1. The client keeps the held map ready to send. `#context.locked` is
   exactly `#{ {Scope, Term, Node} => Manager }`; the re-entry counts
   move to a field next to it.
2. `#request{}` carries only the number of the held locks -
   `held_count`. The map itself travels in `#add_held_locks{}`, and
   only to the managers where the request waits.
3. A manager that queues a request sends `#queued{}` when it needs the
   map, the client answers with `#add_held_locks{}`. A request that
   holds nothing and asks a single node is never asked.

The manager feeds the graph at one point only: `elock_graph:add_edges/5`
(the renamed `add_held_locks/4`). The old `add_edges/2` is gone.

The public API (`lock/3,4`, `unlock/1`, `ready_nodes/1`) does not
change.

## Protocol

### `#request.held_count`

Replaces `#request.held`. An integer: `map_size(Locked)` of the client
at the moment of the request. Two uses:

* the manager sends `#queued{}` to a single node request only when it
  is greater than zero (see below);
* it is the weight of the request in the wait-for graph. The weight is
  fixed for the whole life of the request, the grants a multi node
  request gains meanwhile do not change it (see *Weight* under
  *Correctness*).

The field is renamed, not retyped in place: code that still builds a
`held = #{...}` fails to compile instead of failing a guard at run
time.

### `#queued{ref, manager, node}`

Sent by the manager from `start_waiting/1`, i.e. when a request is
enqueued or starts barging, when

* `held_count > 0`, or
* the request names more than one node (the manager needs the grants
  the request gets elsewhere, as today).

Meaning: "your request waits here - send me what it holds and keep me
posted". Today it is sent for multi node requests only.

### `#add_held_locks{ref, held}`

Unchanged shape. Carries

* the client's answer to `#queued{}`: its held map plus, for a multi
  node request, the grants it has got so far;
* every later grant of a multi node request, to every queued manager
  (as today).

The record keeps its name: it names what the client does. Only the
graph function that consumes it is renamed.

### Who answers `#queued{}`

| Request                          | Path                                        | Proxy                                       | Answer                                                                        |
|----------------------------------|---------------------------------------------|---------------------------------------------|-------------------------------------------------------------------------------|
| single node, local               | `elock_manager:lock/2` in the client itself | the client                                  | `Manager ! #add_held_locks{}` from `elock_manager:wait_verdict/3`             |
| single node remote, multi node   | workers + `elock:wait_verdict/1`            | the process spawned by ecall/erpc on the node | proxy forwards with `ecall:send/2`, the client answers with `ecall:send/2` |

A single remote node request today calls `ecall:call/4`, which blocks
the client in a selective receive on ecall's own reference: a forwarded
`#queued{}` would never be seen (and would stay in the mailbox for
good). It takes the worker path from now on, the dedicated clause of
`run_request/1` is removed. The cost is one local spawn per remote
request, against a network round trip.

## Client: `elock`

### Context

```erlang
-record(context, {
  ref2lock,   % #{ Ref => #lock{} }
  locked,     % #{ {Scope, Term, Node} => Manager } - the held map, sent as is
  counts      % #{ {Scope, Term, Node} => N } for N >= 2 only: the keys held
              % by more than one request of the client. A key absent here
              % is held once
}).
```

`add_lock/2`, per node of the lock, `Key = {Scope, Term, Node}`:

| `locked`                     | action                                                                       |
|------------------------------|------------------------------------------------------------------------------|
| `Key => Manager` (the same)  | re-entry: `counts` gets `Key => N + 1` with `N = 1` when absent             |
| `Key => Other`               | stale (the old manager is gone): warn, `locked` gets `Key => Manager`, `counts` drops `Key` |
| absent                       | `locked` gets `Key => Manager`                                               |

`remove_lock/2`, per node, `Manager` taken from `#lock.nodes`:

| `locked`                     | `counts`   | action                                          |
|------------------------------|------------|-------------------------------------------------|
| `Key => Manager`             | `Key => 2` | `counts` drops `Key`                            |
| `Key => Manager`             | `Key => N`, N > 2 | `counts` gets `Key => N - 1`             |
| `Key => Manager`             | absent     | `locked` drops `Key`                            |
| `Key => Other`               | -          | stale unlock: warn, nothing changes (as today)  |
| absent                       | -          | nothing changes (as today)                      |

The common path - a key locked once - touches `locked` only.

```erlang
held_locks(#context{locked = Locked}) -> Locked;
held_locks(_NoContext) -> #{}.

held_count(Context) -> map_size(held_locks(Context)).
```

`held_locks/1` is a field read now, no `maps:map/2`.

### `lock/4`

```erlang
Request = #request{
  ...
  held_count = held_count(Context),
  ...
},
```

The map is not built and not put into the request.

### `run_request/2`

`run_request(Request, HeldLocks)`, `HeldLocks = held_locks(Context)`
from `lock/4` - a pointer to the map in the context, no copy. Two
clauses instead of three:

* `nodes = [Node]` with `Node =:= node()`:
  `elock_manager:lock(Request, HeldLocks)`, result mapped to
  `{ok, #{ Node => Manager }}` as today;
* everything else - a single remote node or several nodes: the worker
  path as today (`spawn_monitor` of `ecall_connection:call/4` per
  node, `wait_verdict/1`) with `HeldLocks` in `#waiting.held`. The
  single remote node clause is deleted.

### `wait_verdict/1`

`#waiting{}` gets a `held` field: the client's held map.

On `#queued{ref = Ref, manager = Manager, node = Node}`:

1. send `#add_held_locks{ref = Ref, held = Held}` to `Manager`, where
   `Held` is the client's map with the grants so far folded in:
   `maps:fold(fun(N, M, Acc) -> Acc#{ {Scope, Term, N} => M } end,
   ClientHeld, Nodes0)`. Nothing is sent when the result is empty.
   Folding a few grants into a persistent map is O(grants * log N),
   the map is not rebuilt;
2. record `Queued#{ Node => Manager }` as today.

On a grant (`'DOWN'` with `{ok, {ok, Manager}}`): as today, the grant
alone - `#{ {Scope, Term, Node} => Manager }` - goes to every queued
manager.

`notify_queued/5` becomes a plain "send this held map to these
managers" helper used by both; the two call sites build the map.

`wait_unlock/2` does not change: a withdrawn request answers `#queued{}`
with `#unlock{}`, the held locks are not needed.

### Optional: mark the mailbox position with the request ref

Both client waits are not receive-optimized today (`erlc +recv_opt_info`:
"all clauses do not match a suitable reference") because the `'DOWN'`
clauses carry the monitor references, not `Ref`. If the workers are
started with

```erlang
spawn_opt(Fun, [{monitor, [{tag, Ref}]}])
```

the down message is `{Ref, MonRef, process, Pid, Reason}`, every clause
of `wait_verdict/1` and `wait_unlock/2` matches `Ref`, and the compiler
optimizes the receive (verified on OTP 27 with `Ref` made two calls up
the stack: "all clauses match reference in function parameter"). The
loop then skips every message that was in the mailbox before the
request started. Independent of the rest, can be left out.

## Manager: `elock_manager`

### `lock/1`, `lock/2`

```erlang
lock(Request) -> lock(Request, undefined).      % proxy mode, the remote apply
lock(Request, HeldLocks) -> ...                 % client mode, HeldLocks is the map
```

`wait_verdict/3` gets the third argument. On `#queued{ref = Ref}`:

* `undefined` - the process is a proxy: forward to the client with
  `ecall:send/2`, as today;
* a map - the process is the client itself:
  `Manager ! #add_held_locks{ref = Ref, held = HeldLocks}`. It answers
  every `#queued{}` it gets; an empty map is harmless (see the graph).

Today the forward would send the message to the process itself when
the proxy is the client. It is unreachable today because `#queued{}` is
only sent to multi node requests, it becomes reachable with this
change, hence the explicit mode.

### `#req{}`

Gets `held_count`, copied from the request in `new_req/1`. It is the
weight the graph gets with every `#add_held_locks{}` of the request.

### `start_waiting/1`

```erlang
start_waiting(#request{timeout = Timeout} = Request) ->
  notify_queued(Request),
  start_timer(new_req(Request), Timeout).
```

No graph call, returns `Req` alone. `enqueue/2` and
`enqueue_barging/2` do not touch `#state.graph` any more.
`notify_queued/1` applies the rule of the *Protocol* section
(`held_count > 0` or more than one node).

`stop_waiting/2` does not change: `remove_edges/2` handles a request
that never reached the graph.

### `handle_add_held_locks/2`

```erlang
#{Ref := #req{has_lock = false, held_count = Weight}} ->
  Graph = elock_graph:add_edges(Ref, {Scope, Term, node()}, Update, Weight, Graph0),
```

Everything else as today: a request that has the lock or has left is
ignored.

## Graph: `elock_graph`

### `add_edges(Ref, Edge, Update, Weight, Graph)`

Replaces both `add_edges/2` and `add_held_locks/4`. `Edge` is the lock
of this manager, `Update` a held map, `Weight` the `held_count` of the
request.

1. `map_size(Update) =:= 0` - the graph is returned as it is, whether
   it exists or not. An empty update must not create a `#graph{}` with
   an empty index.
2. The graph exists: `{Weight0, Held0} = maps:get(Ref, Index, {Weight,
   #{}})` - a request already in the index keeps its weight, a new one
   joins with the given one. The entries of `Update` that `Held0` does
   not have (a new key, or a fresh PID for a stale one -
   `new_held_locks/2`, unchanged) are probed with `Weight0`
   (`run_probe/4`, unchanged), join the edges (`add_holder/4`,
   unchanged) and the index entry becomes `{Weight0, merged}`. No new
   entries - the graph is returned as it is.
3. No graph yet: start one and go to 2.

The module reads `#request{}` nowhere any more; it still includes
`elock.hrl` for `#deadlock_probe{}` and `#deadlock{}`. The exports are
`add_edges/5`, `remove_edges/2`, `probe/3`, `forward/2`. `probe/3`,
`forward/2`, `remove_edges/2`, `add_holder/4`, `run_probe/4`,
`check_cycles/4` and `drop_coin/2` do not change.

The header comment is rewritten: the held map no longer comes with the
request, every hold of a waiting request - the client's locks and the
grants gained meanwhile - comes in through `add_edges/5` and is probed
as it comes; the weight is the `held_count` of the request.

## Correctness

**Detection with late edges.** A request joins the graph when its held
map arrives rather than when it is enqueued. Until then it is
indistinguishable from a request that has not asked yet. Every edge is
still probed the moment it appears, and each manager adds an edge and
sends its probe in one step, so of two probes that cross - the two
requests of a cycle asking at about the same time - at least one is
evaluated after both edges exist. This is the argument the graph
already relies on for the grants of a multi node request.

**Weight.** Frozen at `held_count`, as today at `map_size(Held)`. A
deadlock is resolved by pairwise comparisons evaluated at different
managers at different moments: for a two-cycle A-B, A's probe compares
`(Wa in the probe, Wb stored at B's manager)` and B's probe
`(Wb in the probe, Wa stored at A's manager)`. They name the same
loser only if both see the same pair. A weight that grew with the
grants could be outdated in a probe already in flight, and the two
evaluations could each abort the other side - two victims for one
cycle. With the count from the request every probe and every stored
entry of a request carry the same number, on every manager it waits
at. It is also the honest cost measure: the grants of the request
itself are released by the client on abort, the locks held before it
are the transaction's investment.

**Late and repeated answers.** An answer that arrives after the request
got the lock, timed out or was aborted is ignored by
`handle_add_held_locks/2` (`has_lock` or no such request). After
`#retry{}` the request keeps its `Ref`, so an answer to the previous
attempt feeds the new one - correctly, the client's holds did not
change - and the answer to the new `#queued{}` is filtered by
`new_held_locks/2`. An answer to a manager that has exited is lost,
the manager of the next attempt asks again.

**Lost answer.** Two messages instead of one: a lost
`#add_held_locks{}` would leave a waiter out of the graph. Over ecall a
message is lost only with the dist connection, and then the manager
sees the client go down and drops the request - the existing node-down
handling.

**Latency.** Detection of a deadlock starts one client round trip
later than today (manager to proxy to client to manager). The request
is waiting anyway.

**Message order.** The initial answer and the grant updates from one
client to one manager stay ordered (`ecall:send/2` pins a sender to
one worker, a direct `!` is FIFO), but nothing depends on it any more:
the weight comes from the request, not from the first map.

## Compatibility

* Public API unchanged.
* No mixed versions in a cluster. An old manager given an integer in
  `held` spins forever in the fallback clause of `add_edges/2` (both
  `map_size/1` guards fail, the clause recurses into them again); a new
  manager given a map in the same slot takes it for a weight - a map
  compares greater than any integer, the verdicts become nonsense
  without a crash. Upgrade all nodes together.

## Out of scope

* Tests. They are updated after the review of the implementation:
  `elock_graph_SUITE`, `elock_manager_SUITE`, `elock_SUITE`,
  `elock_multi_node_SUITE` build `#request{held = #{...}}` and expect
  the probes at enqueue time.
* `#graph.edges` stores a weight per `{Lock, Ref}` entry that
  `check_cycles/4` could read from the index it looks up anyway. A
  possible later cleanup, not part of this change.
