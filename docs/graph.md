# Deadlock detection in a graph process — spec

Status: draft, 2026-10-06. Not implemented. Supersedes the sketch that was here and `src/elock_graph2.erl`.
Scope: production code only. The test changes are a separate step.

The rules of this document are binding, the code sketches are illustrations: a sketch that violates the principles below gives way to the principles.

The project principles, which every clause of the implementation answers to:

1. **No unreachable checks.** A clause exists only for a case that can happen. A structure that a neighbouring module, or this one, can not produce is not matched against, and a function that is only called with a valid argument does not verify it. Where this document keeps a tolerant clause, it names the reachable case that needs it.
2. **No defence against foreign messages.** Only this library's own processes send to a manager or to the graph process, and they send well-formed records. The one defence is the `Unexpected` clause of a loop, which logs and goes on.
3. **A manager does not die on its own.** Nor does the graph process. Each stops with the node, or with the scope when the supervisor stops the scope.
4. **Performance first, happy path first.** The fast path of a lock is written for the common case. Code style follows `elock_manager.erl` as it is: a `-spec` per function, records matched in the function heads, the comment banners, and the names of the functions that survive.

## 1. Purpose

Today every manager keeps the wait-for graph of its own waiters (`elock_graph.erl`) and finds a cycle by probing. A new hold of a waiting request sends one probe to the manager of every new held lock; every manager that gets the probe forwards it to the managers of every lock held by every one of its waiters. Across nodes that is one small packet per edge of the component on the distribution link, which is the link that caps the multi node throughput. In the run of 2026-10-06 07:17 (2 nodes, 100 clients per node, 10 locks per transaction, random order) elock sent 286 MB per node for 200 000 locks and did 1 268 transactions per second, against 13 MB and 5 757 for mnesia.

The change: one **graph process per node** keeps the wait-for graph of the node in ETS. The managers cast their edges to it. A cycle within the node is found by one walk inside the process, with no message between managers. A cycle across nodes is found by the same walk hopping between the graph processes: one message per node per round, carrying the locks to expand there, instead of one probe per edge.

What stays: the semantics of a deadlock as `elock.erl` documents them (the weight, the coin, at least one loser per cycle, the loser keeps its locks, the extra losers of a concurrent cycle), the verdict `#deadlock{ref, winner}` and the manager's `handle_deadlock/2`, the client side, the scopes.

## 2. The process

- Module `elock_graph`, rewritten. One process per node, registered as `elock_graph`. Normal priority, `{message_queue_data, off_heap}`.
- Started by elock's own supervisor before any scope can start: elock becomes a started application (§7). `start_link/0` goes through `proc_lib:start_link/3`; the process creates the table, registers the name and acknowledges with `proc_lib:init_ack/1`, then loops. It never stops on its own (principle 3). It stops with the node.
- Owns one private ETS table, a `bag` keyed by the waited lock (§3). Nothing else reads or writes it, so every operation of the process, a walk included, sees the table as a whole.
- Talks to the managers of its node and to the graph processes of the other nodes, and to nobody else (§4).

```erlang
-export([
  start_link/0,
  add_edges/4,
  remove_edges/2
]).
```

## 3. The table

One object per waiting request that holds something, keyed by the lock it waits for:

```erlang
-record(waiter,{
  lock :: lock_key(),           % the lock the request waits for, the key
  ref :: reference(),           % the request
  weight :: non_neg_integer(),  % #request.held_count, fixed for the life of the request
  manager :: pid(),             % the manager of lock, the sender of the edges
  held :: held_locks()          % #{lock_key() => pid()}, as the client sent it
}).

ets:new(?MODULE, [bag, private, {keypos, #waiter.lock}])
```

- The waiters at a key all belong to one manager: a lock has one manager, which runs on the lock's node. So the rows of the waiters at lock `L` are complete on `node(L)` and nowhere else. This is what makes the walk hop to the node of a lock to expand it (§5.3), and what makes any replication of edges between nodes unnecessary.
- `held` keeps the manager pid of every held lock. It is the stale-manager check of the closure (§5.3): only a hold at the origin manager's pid closes a cycle, another pid is a hold at a manager that has died and been replaced.
- `weight` is the held count of the request, fixed, so every verdict about one request compares the same number.

**The rows leave with their requests, the process keeps nothing clean.** A row is written by `#add_edges{}` and deleted by `#remove_edges{}`. The manager sends the remove in `stop_waiting/2` for every waiter that leaves, by a grant, a timeout, a verdict, a withdrawal, a dead client or a dead node. A manager stops only with its node, where the graph process stops too, or after its scope was stopped, in `try_unlock/1`, which it reaches only with no holders and an empty queue, that is with no row in the graph. Hence no monitor on a manager or a scope, no bookkeeping of scopes, and no cleanup clause: none of them has an event. The locks of a node that went down are named in the held maps of rows on the other nodes, and a hop to that node is dropped (§5.4), as a probe to a manager of that node is dropped today.

## 4. Protocol

Records in `elock.hrl`.

```erlang
% Manager -> graph of its node: the new holds of a waiting request
-record(add_edges,{
  lock :: lock_key(),           % the manager's lock
  ref :: reference(),
  weight :: non_neg_integer(),
  manager :: pid(),
  held :: held_locks()          % #add_held_locks.held as it came
}).

% Manager -> graph of its node: the request has stopped waiting
-record(remove_edges,{
  lock :: lock_key(),
  ref :: reference()
}).

% Graph -> graph of another node: expand these locks there
-record(deadlock_probe,{
  ref :: reference(),           % the origin request
  edge :: lock_key(),           % the lock the origin waits for
  manager :: pid(),             % the origin manager
  weight :: non_neg_integer(),  % the held count of the origin
  expand :: [lock_key()],       % the locks to expand on the receiving node
  visited :: #{lock_key() => true} % the locks this branch has expanded or scheduled
}).
```

- `#deadlock{ref, winner}` from the graph to a manager is the record of today, unchanged. The `lock` field of the draft is not needed: a manager has one lock.
- Gone: `id` and `sent_to` of today's probe, the `seen` set, `#edges{}`, `#conflict{}`, `#cycle{}` of the draft.
- The manager sends to the graph of its node with `!`; the graph sends a hop with `ecall:send({elock_graph, Node}, Probe)` and a verdict with `ecall:send(Manager, Deadlock)`, the manager being local or remote.
- The add and the remove of one request come from one process to one process, so the remove is never handled before the add.
- Only the managers of the node and the graph processes of the other nodes send to the graph process, and they send these records (principle 2). The one defence is the `Unexpected` clause of its loop, which logs and goes on.

Client side of the protocol, in `elock_graph`:

```erlang
-spec add_edges(lock_key(), reference(), non_neg_integer(), held_locks()) -> ok.
add_edges(Lock, Ref, Weight, Held)->
  ?MODULE ! #add_edges{lock = Lock, ref = Ref, weight = Weight, manager = self(), held = Held},
  ok.

-spec remove_edges(lock_key(), reference()) -> ok.
remove_edges(Lock, Ref)->
  ?MODULE ! #remove_edges{lock = Lock, ref = Ref},
  ok.
```

## 5. The graph process

```erlang
-spec loop() -> no_return().
loop()->
  receive
    #add_edges{} = Add->
      handle_add_edges(Add);
    #remove_edges{} = Remove->
      handle_remove_edges(Remove);
    #deadlock_probe{} = Probe->
      handle_probe(Probe);
    Unexpected->
      ?LOGWARNING("unexpected message received: ~p",[Unexpected])
  end,
  loop().
```

### 5.1 `#add_edges{}`

The waiter's row is looked up among the waiters at `lock` (`ets:lookup/2`, then the object with `ref`).

| Case | Why it happens | Action |
|---|---|---|
| No row | The first answer of the client to `#queued{}` | Insert the row. Launch a walk with every key of `held` (§5.3) |
| A row, and `held` brings a new key, or a new manager pid for a key the row has | A multi node request queued here is granted on another node and the client sends the grant (`elock_context:wait_verdict/2`); a key held at a manager that died and was replaced | Replace the row with the merged map (`delete_object` + `insert`). Launch a walk with the new keys only |
| A row, nothing new | A multi node request of a client that already holds the term on one of its nodes: the grant there repeats a key of the context, which the client sent with its answer to `#queued{}` | Nothing |

The new keys only: every edge is probed once, when it appears, so the walk of the edge that closes a cycle finds the rest of the cycle in place. That is today's rule of `new_held_locks/2`, moved into the process that owns the rows.

### 5.2 `#remove_edges{}`

`ets:match_delete/2` of the object with `lock` and `ref`. With the key bound this is a lookup, not a scan.

### 5.3 The walk

A walk serves one **launch**: the origin request `Ref`, waiting for `Edge` at `Manager` with `Weight`. It starts with a list of locks to expand and a set `visited`. A lock is in `visited` from the moment it is **scheduled**: the locks of the entry list before the walk, a discovered lock the first time a waiter is seen to hold it. So no lock is expanded twice in a branch and the walk ends, also through a cycle in the graph that does not pass through the origin (a cycle whose verdict is still on its way, or whose loser's remove is still in the mailbox).

- At a launch (§5.1), `visited` starts as `#{Edge => true}` and the new held keys are scheduled through it. `Edge` is never expanded: the waiters at the origin's own lock depend on its holders, not on the origin. A barging request holds the lock it waits for, so `Edge` is among its keys: dropped there. This is today's `Self` in `sent_to`.
- At a hop (§5.4), `visited` and the entry list come from the probe; the sender has scheduled the entry locks in it already.

**Expanding a lock** `Lock` reads its waiters, `ets:lookup(?MODULE, Lock)`. They depend on the origin: the origin holds `Lock`, or a waiter expanded before does. For every waiter:

- `held` has `Edge => Manager`: the waiter **closes a cycle**. It is compared with the origin, weight first, the coin of `drop_coin/2` on a tie, as today. A closer that beats the origin ends the walk: the origin loses, with `winner = Lock`, the lock the closer waits for. A closer that loses is collected with its `manager`. A closer is never expanded: its abort breaks every cycle through it, and so does the origin's.
- Otherwise the keys of `held` are **scheduled**: a key in `visited` is dropped, a local key joins the list to expand, a remote key is put aside under its node for a hop. Both are added to `visited` then.

When the list is empty the walk is over. Then, in this order:

1. The origin lost: `#deadlock{ref = Ref, winner = Winner}` to the origin manager and nothing else. No abort of the closers collected before, no hop: the origin's abort breaks every cycle through it. Today this rule holds per manager, here it holds for the whole hop, since the hop is one sequence.
2. Otherwise every collected closer gets `#deadlock{ref = CloserRef, winner = Edge}` at its manager, and the hops go out (§5.4).

The verdicts leave the process as messages. The table changes only when the managers' removes come back, so the walk reads a snapshot and the order of the sends within a walk does not matter.

There is no clause for a waiter with the origin's own `ref`. `Edge` is never expanded, so the origin's own row is never read. The copy of a multi node request waiting on another node has the same `ref`, but it holds what the origin holds, never the origin's wait lock, so it is an ordinary waiter: its keys were scheduled by the launch already or are the grants of other copies, and expanding them leads only to real dependents of the request.

Sketch of the walk. `Hops` is `#{node() => [lock_key()]}`, a loser is `{reference(), pid()}`. The two entry points: a launch schedules the new held keys from an empty graph of the branch, a hop takes what the sender scheduled.

```erlang
%%-----------------------------------------------------------------
%%  A launch: the new holds of a waiter of this node. The origin's
%%  own lock is never expanded
%%-----------------------------------------------------------------
-spec launch(#deadlock_probe{}, [lock_key()]) -> ok.
launch(#deadlock_probe{edge = Edge} = Probe, NewKeys)->
  {Entry, Visited, Hops} = schedule(NewKeys, #{Edge => true}, #{}),
  verdict(walk(Entry, Probe, Visited, [], Hops), Probe).

%%-----------------------------------------------------------------
%%  A hop: the sender has scheduled the entry locks in visited
%%-----------------------------------------------------------------
-spec handle_probe(#deadlock_probe{}) -> ok.
handle_probe(#deadlock_probe{expand = Entry, visited = Visited} = Probe)->
  verdict(walk(Entry, Probe, Visited, [], #{}), Probe).

%%-----------------------------------------------------------------
%%  Expands the scheduled locks until none is left. Returns the
%%  origin's verdict, or the losers, the visited set and the hops
%%-----------------------------------------------------------------
-spec walk([lock_key()], #deadlock_probe{}, visited(), [loser()], hops()) ->
  {origin, lock_key()} | {[loser()], visited(), hops()}.
walk([Lock | Rest], Probe, Visited0, Losers0, Hops0)->
  case check_cycles(ets:lookup(?MODULE, Lock), Probe, Losers0, []) of
    origin->
      {origin, Lock};
    {Losers, Found}->
      {Next, Visited, Hops} = schedule(Found, Visited0, Hops0),
      walk(Next ++ Rest, Probe, Visited, Losers, Hops)
  end;
walk([], _Probe, Visited, Losers, Hops)->
  {Losers, Visited, Hops}.

%%-----------------------------------------------------------------
%%  A waiter holding the origin's lock at the origin manager closes a
%%  cycle: compared, never expanded. The others are expanded through
%%  the locks they hold
%%-----------------------------------------------------------------
-spec check_cycles([#waiter{}], #deadlock_probe{}, [loser()], [lock_key()]) ->
  origin | {[loser()], [lock_key()]}.
check_cycles(
    [#waiter{ref = Ref, weight = Weight, manager = Manager, held = Held} | Rest],
    #deadlock_probe{edge = Edge, manager = OriginManager} = Probe,
    Losers,
    Found
)->
  case Held of
    #{Edge := OriginManager}->
      case beats(Weight, Ref, Probe) of
        true-> origin;
        false-> check_cycles(Rest, Probe, [{Ref, Manager} | Losers], Found)
      end;
    _->
      check_cycles(Rest, Probe, Losers, maps:keys(Held) ++ Found)
  end;
check_cycles([], _Probe, Losers, Found)->
  {Losers, Found}.

%%-----------------------------------------------------------------
%%  A lock not yet visited is scheduled once: local ones to expand
%%  here, remote ones under their node for a hop
%%-----------------------------------------------------------------
-spec schedule([lock_key()], visited(), hops()) -> {[lock_key()], visited(), hops()}.
```

`beats/3` is today's comparison: heavier wins, the coin on a tie, `drop_coin/2` unchanged. The verdict of the launch:

```erlang
verdict({origin, Winner}, #deadlock_probe{ref = Ref, manager = Manager})->
  ecall:send(Manager, #deadlock{ref = Ref, winner = Winner});
verdict({Losers, Visited, Hops}, #deadlock_probe{edge = Edge} = Probe)->
  [ ecall:send(Manager, #deadlock{ref = Ref, winner = Edge}) || {Ref, Manager} <- Losers ],
  maps:foreach(
    fun(Node, Locks)->
      ecall:send({?MODULE, Node}, Probe#deadlock_probe{expand = Locks, visited = Visited})
    end,
    Hops
  ).
```

Cost of a walk: one lookup per expanded lock, one map lookup per held key seen. Each lock of the component is expanded once per branch and each waiter examined once, since a waiter waits at one lock per node.

### 5.4 Hops

The remote locks scheduled by a walk go out after its verdicts, one `#deadlock_probe{}` per node, with `expand` the locks of that node and `visited` the final set of the walk. Every hop of a round carries the same set, which holds the locks scheduled for the sibling hops too, so two branches of one launch never expand the same lock: this is today's merge of the targets into `sent_to` before the forward.

The receiving graph process runs the same walk from `expand` with the carried `visited`. The locks it discovers on its own node it expands in place; those on other nodes, the sending node included, it schedules for the next round. A walk returns to a node whenever the node has locks the branch has not seen: which locks of a node matter is learned from held maps that live on other nodes, so the first visit cannot know them all. A 2-cycle across two nodes is one hop; a 3-cycle through two nodes is two, there and back, each carrying only locks the branch has not seen.

A hop to a node that is down is dropped by ecall, as today's probe to a manager of that node is: the locks there are gone. A hop whose `expand` locks have no waiters any more reads nothing and ends.

Each hop sends the verdicts for the closers it finds, as each manager does today. The origin may get its verdict from several hops; the second one is ignored by `handle_deadlock/2` as it is today, the request having left.

## 6. Manager changes (`elock_manager.erl`)

### 6.1 State

- `#state.graph` is removed, with its writers: `init/1`, the reset in `try_unlock/1`, both clauses of `dequeue/2`, `locked/2`, `handle_add_held_locks/2`.
- `#req{}` gets one field, with a default so `init/1` and `new_req/1` need no change:

```erlang
edges = false :: boolean() % the graph has the edges of this request
```

Set to `true` by `handle_add_held_locks/2` when it sends the edges. `stop_waiting/2` sends `#remove_edges{}` only when it is `true`. The case the flag tells apart: a waiter that holds nothing on a single node is never asked for its held map, and a waiter that is asked can be granted or leave before the answer arrives (today's "granted or left meanwhile"). Neither has a row, and the remove would be a message for nothing on the path every waiter takes.

### 6.2 Transitions

| Event | Condition | Action |
|---|---|---|
| `#add_held_locks{}` | `ref` is in `requests` with `has_lock = false` | `elock_graph:add_edges({Scope, Term, node()}, Ref, HeldCount, Held)`, `edges = true` |
| | Anything else: granted or left meanwhile | Nothing, as today |
| `#deadlock{}` | As today | `handle_deadlock/2` unchanged: still waiting, so the verdict to the proxy, dequeue, `next/1`; otherwise nothing |
| `#deadlock_probe{}` | | The clause and `handle_deadlock_probe/2` are removed. Nothing sends it to a manager any more |
| A waiter leaves: `locked/2`, `dequeue/2` | `#req.edges = true` | `stop_waiting/2` sends `elock_graph:remove_edges({Scope, Term, node()}, Ref)` |
| | `#req.edges = false` | `stop_waiting/2` only stops the timer |

`stop_waiting/2` becomes `stop_waiting(#req{}, #state{}) -> #req{}`: the state for the key `{Scope, Term, node()}` in place of the graph; `locked/2` and `dequeue/2` lose the graph tuple.

### 6.3 Code that needs no change, and why

- `try_barging/2` settles the second upgrade by itself with `#deadlock{}` to the proxy: the graph never sees a barging request as a closer at its own lock (§5.3).
- `handle_down/2`, `handle_nodedown/2`, `handle_timeout/2`, `handle_unlock/2` end a wait through `dequeue/2`, which does the remove.
- `notify_queued/1` keeps asking for the held map only when the request holds something or spans nodes: a request that holds nothing is on no cycle.
- `elock_context`, `elock_scope`, `elock` (the API): untouched. The client receives `#deadlock{}` as today.

### 6.4 Comments

The banner of `elock_manager.erl` and the section banner "Deadlock probes (see elock_graph)" describe the cast of the edges and the verdict from the graph process. The banner of `elock_graph.erl` is rewritten for the process, the table, the walk and the hops, in the wording of §3 to §5.

## 7. Application

- `src/elock_app.erl`: `application` behaviour, `start/2` starts `elock_sup`, `stop/1` returns `ok`.
- `src/elock_sup.erl`: `supervisor` behaviour, `one_for_one`, one permanent worker `elock_graph`.
- `src/elock.app.src`: `{mod, {elock_app, []}}` and `{registered, [elock_graph]}`.

A user application that lists `elock` in its `applications` gets the graph process started before its own supervisor starts the scopes, so a scope never runs without it.

## 8. What the move changes in the verdicts

Kept:

- **At least one loser per cycle.** The add that closes a cycle is handled after the adds that preceded it in the same mailbox, so its walk finds the rest of the cycle in the table. Across nodes, a hop arrives at a node after the adds that preceded it there, by the same argument that holds today for a probe reaching a manager.
- **The extra losers of a concurrent cycle.** The hops of one launch and the launches of different requests decide independently, as the managers do today.

New, both accepted:

- **The edges lag the managers.** A row is written and deleted when the graph process handles the cast, not when the manager changes its state. A request that has left but whose remove is still in the mailbox can close a cycle for an origin, which then loses while no cycle exists. The loser keeps its locks and the caller repeats the request, as after a real deadlock. The window is the backlog of the graph process; it is not measured yet (§9).
- **A verdict is applied by the manager, not by the walk.** The aborted closers of a hop are still in the table while the hop expands on. Today a manager aborts its closers before it forwards, which matters when an aborted exclusive closer lets the shared waiters behind it through. Here those waiters may still be expanded, with the same outcome as above: a verdict for a request that still waits, one restart.

The module documentation of `elock.erl` already states that one cycle may cost more than one request. It gains the sentence that a request can also lose while its cycle has just dissolved.

## 9. Performance

The path of a request that is granted at once is untouched: no message to the graph process. A waiter that holds nothing sends nothing either.

Per waiting request that holds something, today against this spec:

| | Today | This spec |
|---|---|---|
| Messages from its manager | one probe per held lock, to the manager of each | one `#add_edges{}` to the local graph process, one per later grant, one `#remove_edges{}` |
| Messages per probe received | one forward per held lock of every waiter of the manager | none: the walk expands in place |
| Across nodes per launch | one probe per edge of the component that crosses a node | one hop per node per round |
| Work | spread over the managers, in their heaps | one walk per launch in the graph process, ETS lookups, the held map of every visited waiter copied out |

The graph process is one per node. Its share of a core, the restarts of the deadlock configuration (the proxy for the verdicts without a cycle of §8) and the octets per lock on the distribution link are the numbers to take from the performance suite before and after. If the process turns out to be the limit, the walk can leave it for a walker per launch over a public table, with the process as the address of the hops only; that is not part of this spec.

## 10. Documentation

- `elock.erl`, "Setup": elock is a started application now, with one graph process per node; nothing changes for the user beyond listing it in `applications`, which the text already asks for.
- `elock.erl`, "Deadlocks": the sentence of §8.
