
%%=================================================================
%%  The public API. The logic is in elock_scope.erl (the scope) and
%%  elock_context.erl (the locks)
%%=================================================================
-module(elock).

-moduledoc """
Distributed locks on Erlang terms.

A lock is a term locked in a scope on a node. `lock/4` takes it exclusively
or shared, on one node or on several nodes at once, and returns a reference
to release it with `unlock/1`. Waiting requests are served in order, the
locks of a process are released when it exits, and deadlocks are detected,
also across nodes and scopes.

This module is the whole public API. The other modules of the application
are implementation details.

## Setup

`elock` is a library application: list it in the `applications` of your own
application. The nodes must be connected by Erlang distribution.

A scope is an independent set of locks named by an atom. Start it under a
supervisor of yours, on every node that is to keep locks of the scope:

```erlang
#{
  id => my_locks,
  start => {elock, start_link, [my_locks]}
}
```

## Usage

Take a lock, do the work and release the lock in the same process:

```erlang
{ok, Ref} = elock:lock(my_locks, {account, 42}, [node()]),
try
  withdraw(42, Amount)
after
  elock:unlock(Ref)
end.
```

A shared lock on every node of the scope, waiting for it at most 5 seconds:

```erlang
Nodes = elock:ready_nodes(my_locks),
Options = #{is_shared => true, timeout => 5000},
case elock:lock(my_locks, {account, 42}, Nodes, Options) of
  {ok, Ref} ->
    Balance = balance(42),
    elock:unlock(Ref),
    {ok, Balance};
  {error, Reason} ->
    {error, Reason}
end.
```

## Locks

- Any term can be locked. Terms are compared with `=:=`, so `1` and `1.0`
  are different locks.
- The same term in two scopes, or on two nodes, is two locks. Two requests
  compete only on the nodes that both of them name.
- An exclusive lock has one holder. A shared lock has any number of holders
  and keeps the exclusive requests waiting.
- The requests for a lock are served on each node in the order they arrive.
  A shared request joins the shared holders at once only while nobody waits,
  so a stream of shared requests does not starve an exclusive one.
- A lock belongs to the process that took it, and only that process can
  release it. The locks of a process are released and its waiting requests
  are withdrawn when it exits.
- A request for several nodes is all or nothing: it succeeds when every node
  has granted it, and when it fails the nodes that have granted it are
  released. While it waits for some nodes it holds the others.
- A failed request leaves the locks the process already holds untouched.

## Re-entrancy

A process can lock a term it already holds. Every successful call returns a
reference of its own, and the term is released when all of them are
unlocked.

- A holder asking for the same mode is granted at once.
- An exclusive holder asking for shared is granted at once as well, and the
  lock stays exclusive until the exclusive reference is unlocked.
- A shared holder asking for exclusive is an upgrade. It waits for the other
  shared holders to leave and goes ahead of the queue. If two holders
  upgrade the same lock, the second gets `{error, {deadlock, Lock}}` at once
  and keeps its shared lock.

## Deadlocks

Processes that wait for each other's locks in a cycle are detected, within
a scope, across scopes and across nodes. At least one request of the cycle
fails with `{error, {deadlock, {Scope, Term, Node}}}`, which names the lock
the winning request waits for.

- A request of a process that holds fewer locks loses to a request of a
  process that holds more, a lock on several nodes counting once per node.
  Equal numbers are settled by a coin.
- The loser keeps the locks it holds, and the others keep waiting for them.
  To let them through, release those locks and then repeat the request.
- Two processes that lock the same term on the same several nodes at the
  same time may get a part of the nodes each. That is a deadlock as well, so
  a request for several nodes can lose while its process holds nothing.
- One cycle may cost more than one request. When the requests of a cycle are
  issued at the same moment, several of them can get the deadlock error,
  more often as the cycle gets longer. This is a performance trade-off: the
  verdicts are made independently of each other, without the coordination
  it takes to agree on a single loser. Issued one after another, the
  requests of a cycle yield exactly one loser.

## Failures

- A lock lives on the node it was taken on. If that node goes down, or the
  scope stops there, the lock is lost and its holder is not notified. The
  locks the holder has on the other nodes stay until it unlocks them.
- A request that is waiting on a node when the scope stops there fails at
  once, see `lock/4`.
- A request fails as a whole when one of its nodes can not be reached or
  does not run the scope, see `lock/4`.
""".

%%=================================================================
%%	OTP API
%%=================================================================
-export([
  start_link/1
]).

%%=================================================================
%%	API
%%=================================================================
-export([
  lock/3, lock/4, lock/5,
  unlock/1,
  ready_nodes/1
]).

% lock/4 is not listed: only its boolean form is deprecated
-deprecated([{lock, 5, "Use lock/3 or lock/4 with nodes and options, then unlock/1"}]).

-export_type([lock_options/0, lock_result/0]).

-doc """
The options of `lock/4`.

- `is_shared`: `true` for a shared lock, `false` (the default) for an
  exclusive one.
- `timeout`: how long the request may wait for the lock on a node, in
  milliseconds, or `undefined` (the default) to wait without a limit.
""".
-type lock_options() :: #{
  is_shared => boolean(),
  timeout => pos_integer() | undefined
}.

-doc """
The result of `lock/3` and `lock/4`. The reference in `{ok, Ref}` is the
argument of `unlock/1`. The errors are listed in `lock/4`.
""".
-type lock_result() :: {ok, reference()} | {error, term()}.

%%=================================================================
%%	OTP API
%%=================================================================
-doc """
Starts the scope `Scope` on the local node and links it to the caller.

Meant to be a permanent worker of a supervisor, see the setup in the module
documentation. The locks of the scope can be taken on this node while the
returned process is alive. When it exits, the locks taken on this node are
lost.

`Scope` is also the registered name of the returned process, so it must not
be the name of another registered process. The second start of a scope on a
node returns `{error, {already_started, Pid}}` with the process that already
has the name.
""".
-spec start_link(atom()) -> {ok, pid()} | {error, {already_started, pid()}}.
start_link(Scope)->
  elock_scope:start_link(Scope).

%%=================================================================
%%	API
%%=================================================================
-doc #{equiv => lock(Scope, Term, Nodes, #{})}.
-doc """
Takes an exclusive lock on `Term` on every node of `Nodes` and waits for it
without a timeout. See `lock/4`.
""".
-spec lock(atom(), term(), nonempty_list(node())) -> lock_result().
lock(Scope, Term, Nodes)->
  elock_context:lock(Scope, Term, Nodes).

-doc """
Locks `Term` in `Scope` on every node of `Nodes` and returns the reference
to release it with `unlock/1`.

The call returns when the lock is held on all the nodes or the request has
failed. `Options` are described in `t:lock_options/0`. Duplicates in `Nodes`
are ignored.

- `{ok, Ref}`: the calling process holds the lock on every node of `Nodes`.
- `{error, timeout}`: the request has waited on a node for longer than the
  `timeout`. A request that does not have to wait never times out.
- `{error, {deadlock, {Scope, Term, Node}}}`: the request has lost a
  deadlock to a request that waits for the named lock. See the deadlocks in
  the module documentation.
- `{error, {badrpc, Reason}}`: a node of `Nodes` can not be reached.
- `{error, {exit, badarg}}`: the scope is not started on a node of `Nodes`,
  or it has stopped there while the request was waiting. If `Nodes` is the
  local node alone, the call raises `badarg` instead.

After an error the request holds nothing on any node.

Invalid arguments are thrown, not returned: `{invalid_nodes, Nodes}` unless
`Nodes` is a non-empty list, `{invalid_node, Node}` for a node that is not
an atom, `{invalid_options, Options}` unless `Options` is a map,
`{invalid_option, Key}` for an unknown key, `{invalid_is_shared, Value}` and
`{invalid_timeout, Value}`.

```erlang
{ok, Ref} = elock:lock(my_locks, Key, ['a@host', 'b@host'], #{}),
{error, timeout} = elock:lock(my_locks, Busy, [node()], #{timeout => 100}),
{error, {deadlock, {my_locks, _, _}}} = elock:lock(my_locks, Other, [node()]).
```

`lock(Scope, Term, IsShared, Timeout)`, with a boolean as the third
argument, is the deprecated form of the local lock. It is the same as
`lock(Scope, Term, IsShared, Timeout, [node()])`, see `lock/5`.
""".
-spec lock(atom(), term(), boolean(), timeout()) ->
    {ok, fun(() -> ok)} | {error, term()};
  (atom(), term(), nonempty_list(node()), lock_options()) -> lock_result().
lock(Scope, Term, NodesOrIsShared, OptionsOrTimeout)->
  elock_context:lock(Scope, Term, NodesOrIsShared, OptionsOrTimeout).

-doc """
Locks `Term` on `Nodes` and returns a function that releases it.

The API of the previous versions, kept for compatibility. It takes the same
locks as `lock/4`, with these differences:

- The lock is released by calling `Unlock()` in the process that took it.
- `Timeout` is in milliseconds or `infinity`. With `0` the call returns
  `{error, timeout}` without asking for the lock.
- A deadlock is reported as `{error, deadlock}`.
- An empty `Nodes` locks nothing and returns `{ok, Unlock}`.
""".
-spec lock(atom(), term(), boolean(), timeout(), [node()]) ->
  {ok, fun(() -> ok)} | {error, term()}.
lock(Scope, Term, IsShared, Timeout, Nodes)->
  elock_context:lock(Scope, Term, IsShared, Timeout, Nodes).

-doc """
Releases the lock taken as `Ref` on all its nodes and returns at once.

It must be called by the process that took the lock. A reference that is
unknown, already unlocked or taken by another process is ignored, so the
call always returns `ok`.
""".
-spec unlock(reference()) -> ok.
unlock(Ref)->
  elock_context:unlock(Ref).

-doc """
Returns the nodes where `Scope` is started, as the local node sees them.

The local node is in the list if the scope is started on it, and the list is
empty if it is not. The other nodes join the list as they start the scope
and leave it when they stop it or disconnect, so the list follows the
cluster with a delay. Use it to build the `Nodes` of `lock/4`.
""".
-spec ready_nodes(atom()) -> [node()].
ready_nodes(Scope)->
  elock_scope:ready_nodes(Scope).
