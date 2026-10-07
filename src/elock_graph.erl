
%%=================================================================
%%  The wait-for graph of the node and the deadlock walks.
%%
%%  One process per node, registered as elock_graph, started by
%%  elock_sup. It owns a private ETS bag of the waiting requests of
%%  the node that hold something, keyed by the lock they wait for:
%%
%%      #waiter{ lock, ref, position, manager, held }
%%
%%  The waiters at a key all belong to one manager, which runs on
%%  the lock's node: the rows of the waiters at lock L are complete
%%  on node(L) and nowhere else. A walk expands a lock on its node,
%%  and hops to the other nodes for the locks it has there.
%%
%%  The rows leave with their requests. A manager casts #add_edges{}
%%  when a waiting request sends its held map and #remove_edges{}
%%  when the waiter leaves, by a grant, a timeout, a verdict, a
%%  withdrawal, a dead client or a dead node. A manager stops only
%%  with the node or with no row in the graph, so the process keeps
%%  nothing clean and never stops on its own.
%%
%%  A walk serves one launch: the origin request, waiting for Edge
%%  at Manager with Weight: its queue position, the birth of its
%%  context on the system time of its manager, compared as it is
%%  with the positions of the closers: the older wins, the coin on
%%  a tie (see beats/3). Every new hold of a waiting request is probed
%%  once, when it appears, so the walk of the edge that closes a
%%  cycle finds the rest of the cycle in place. The waiters of an
%%  expanded lock depend on the origin: it holds the lock, or a
%%  waiter expanded before does.
%%    * a waiter holding Edge at Manager closes a cycle: compared,
%%      never expanded. A closer that beats the origin ends the walk,
%%      #deadlock{} goes to the origin manager and nothing else is
%%      sent. The losing closers are collected.
%%    * the other waiters are expanded through the locks they hold.
%%      A lock is visited from the moment it is scheduled, so no lock
%%      is expanded twice in a branch and the walk ends.
%%  When the list is empty every collected closer gets #deadlock{}
%%  at its manager, then the remote locks go out as one
%%  #deadlock_probe{} per node, with the visited set of the whole
%%  walk: two branches of one launch never expand the same lock.
%%  The receiving graph walks on from them.
%%
%%  The verdicts leave as messages, the table changes only when the
%%  managers' removes come back: a walk reads a snapshot.
%%=================================================================
-module(elock_graph).
-moduledoc false.

-include("elock.hrl").

%%=================================================================
%%	OTP API
%%=================================================================
-export([
  start_link/0,
  init/0 % spawned by proc_lib:start_link/3
]).

%%=================================================================
%%	API
%%=================================================================
-export([
  add_edges/4,
  remove_edges/2
]).

%%=================================================================
%%  Manager -> graph protocol is in elock.hrl
%%=================================================================
-record(waiter,{
  lock :: lock_key(),           % the lock the request waits for, the key
  ref :: reference(),           % the request
  position :: integer(),        % #req.position, fixed for the life of the request
  manager :: pid(),             % the manager of lock, the sender of the edges
  held :: held_locks()          % #{lock_key() => pid()}, as the client sent it
}).

-type visited() :: #{lock_key() => true}.
-type hops() :: #{node() => [lock_key()]}.
-type loser() :: {reference(), pid()}.

%%=================================================================
%%	OTP API
%%=================================================================
-spec start_link() -> {ok, pid()}.
start_link()->
  proc_lib:start_link(?MODULE, init, []).

% Off heap mailbox: the bursts of edges stay out of its garbage collection
-spec init() -> no_return().
init()->
  process_flag(message_queue_data, off_heap),
  ets:new(?MODULE, [named_table, bag, private, {keypos, #waiter.lock}]),
  register(?MODULE, self()),
  proc_lib:init_ack({ok, self()}),
  loop().

%%=================================================================
%%	API
%%=================================================================
%%-----------------------------------------------------------------
%%  The new holds of a waiting request. Lock is the manager's lock,
%%  Held is never empty (see elock_context:notify_queued/3 and
%%  elock_manager:wait_verdict/4)
%%-----------------------------------------------------------------
-spec add_edges(lock_key(), reference(), integer(), held_locks()) -> ok.
add_edges(Lock, Ref, Position, Held)->
  ?MODULE ! #add_edges{lock = Lock, ref = Ref, position = Position, manager = self(), held = Held},
  ok.

-spec remove_edges(lock_key(), reference()) -> ok.
remove_edges(Lock, Ref)->
  ?MODULE ! #remove_edges{lock = Lock, ref = Ref},
  ok.

%%=================================================================
%%  The process
%%=================================================================
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

%%=================================================================
%%  The edges
%%=================================================================
%%-----------------------------------------------------------------
%%  No row: the first answer of the client to #queued{}. A row: a
%%  later grant of a multi node request, or a key held at a manager
%%  that died and was replaced. The new keys only are launched: every
%%  edge is probed once, when it appears, weighed then (see launch/2)
%%-----------------------------------------------------------------
-spec handle_add_edges(#add_edges{}) -> ok.
handle_add_edges(#add_edges{
  lock = Lock,
  ref = Ref,
  position = Position,
  manager = Manager,
  held = Held
})->
  ?TRACE(g_add, Ref, {map_size(Held), elock_trace:mailbox()}),
  case ets:match_object(?MODULE, #waiter{lock = Lock, ref = Ref, _ = '_'}) of
    []->
      Row = #waiter{
        lock = Lock,
        ref = Ref,
        position = Position,
        manager = Manager,
        held = Held
      },
      ets:insert(?MODULE, Row),
      launch(Row, maps:keys(Held));
    [#waiter{held = Held0} = Row0]->
      case new_held_locks(Held, Held0) of
        New when map_size(New) =:= 0->
          % The grant repeats a key of the context the client has sent already
          ok;
        New->
          Row = Row0#waiter{
            held = maps:merge(Held0, New)
          },
          ets:delete_object(?MODULE, Row0),
          ets:insert(?MODULE, Row),
          launch(Row, maps:keys(New))
      end
  end.

%%-----------------------------------------------------------------
%%  A new key, or a new manager PID for a stale key
%%-----------------------------------------------------------------
-spec new_held_locks(held_locks(), held_locks()) -> held_locks().
new_held_locks(Update, Held)->
  maps:filter(
    fun(Key, Manager)->
      case Held of
        #{Key := Manager}->
          false;
        _->
          true
      end
    end,
    Update
  ).

%%-----------------------------------------------------------------
%%  The key is bound: a lookup, not a scan
%%-----------------------------------------------------------------
-spec handle_remove_edges(#remove_edges{}) -> ok.
handle_remove_edges(#remove_edges{
  lock = Lock,
  ref = Ref
})->
  ?TRACE(g_remove, Ref, []),
  ets:match_delete(?MODULE, #waiter{lock = Lock, ref = Ref, _ = '_'}),
  ok.

%%=================================================================
%%  The walk
%%=================================================================
%%-----------------------------------------------------------------
%%  A launch: the new holds of a waiter of this node. The origin's
%%  own lock is never expanded: its waiters depend on its holders,
%%  not on the origin. A barging request holds it, dropped there
%%-----------------------------------------------------------------
-spec launch(#waiter{}, [lock_key()]) -> ok.
launch(
    #waiter{
      lock = Edge,
      ref = Ref,
      position = Position,
      manager = Manager
    },
    NewKeys
)->
  Probe = #deadlock_probe{
    ref = Ref,
    edge = Edge,
    manager = Manager,
    weight = Position
  },
  {Entry, Visited, Hops} = schedule(NewKeys, [], #{Edge => true}, #{}),
  Result = walk(Entry, Probe, Visited, [], Hops),
  ?TRACE(g_walk, Ref, elock_trace:walk(Result)),
  verdict(Result, Probe).

%%-----------------------------------------------------------------
%%  A hop: the sender has scheduled the entry locks in visited
%%-----------------------------------------------------------------
-spec handle_probe(#deadlock_probe{}) -> ok.
handle_probe(#deadlock_probe{
  expand = Entry,
  visited = Visited
} = Probe)->
  ?TRACE(g_probe, Probe#deadlock_probe.ref, {length(Entry), map_size(Visited), erlang:external_size(Probe), elock_trace:mailbox()}),
  Result = walk(Entry, Probe, Visited, [], #{}),
  ?TRACE(g_walk, Probe#deadlock_probe.ref, elock_trace:walk(Result)),
  verdict(Result, Probe).

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
      {Next, Visited, Hops} = schedule(Found, Rest, Visited0, Hops0),
      walk(Next, Probe, Visited, Losers, Hops)
  end;
walk([], _Probe, Visited, Losers, Hops)->
  {Losers, Visited, Hops}.

%%-----------------------------------------------------------------
%%  The copy of a multi node origin waiting on another node: a
%%  barging one holds the origin's lock too, on a tie with itself the
%%  origin would lose. It holds what the origin holds, launched here
%%  already
%%-----------------------------------------------------------------
-spec check_cycles([#waiter{}], #deadlock_probe{}, [loser()], [lock_key()]) ->
  origin | {[loser()], [lock_key()]}.
check_cycles(
    [#waiter{
      ref = Ref
    } | Rest],
    #deadlock_probe{
      ref = Ref
    } = Probe,
    Losers,
    Found
)->
  check_cycles(Rest, Probe, Losers, Found);

%%-----------------------------------------------------------------
%%  A waiter holding the origin's lock at the origin manager closes a
%%  cycle: compared, never expanded. Its abort breaks every cycle
%%  through it, and so does the origin's. Another PID at that key is
%%  a stale hold: that manager has died and a new one took the term.
%%  The others are expanded through the locks they hold
%%-----------------------------------------------------------------
check_cycles(
    [#waiter{
      ref = Ref,
      position = Position,
      manager = Manager,
      held = Held
    } | Rest],
    #deadlock_probe{
      edge = Edge,
      manager = OriginManager
    } = Probe,
    Losers,
    Found
)->
  case Held of
    #{Edge := OriginManager}->
      case beats(Position, Ref, Probe) of
        true->
          origin;
        false->
          check_cycles(Rest, Probe, [{Ref, Manager} | Losers], Found)
      end;
    _->
      check_cycles(Rest, Probe, Losers, maps:keys(Held) ++ Found)
  end;
check_cycles([], _Probe, Losers, Found)->
  {Losers, Found}.

%%-----------------------------------------------------------------
%%  The smaller position wins, the older context; the coin on a tie.
%%  The positions are stamped on the system time of their managers,
%%  so they compare across nodes as they are
%%-----------------------------------------------------------------
-spec beats(integer(), reference(), #deadlock_probe{}) -> boolean().
beats(
    CloserPosition,
    CloserRef,
    #deadlock_probe{
      ref = Ref,
      weight = Position
    }
)->
  if
    CloserPosition < Position ->
      true;
    CloserPosition > Position ->
      false;
    true ->
      drop_coin(CloserRef, Ref) =:= CloserRef
  end.

%%-----------------------------------------------------------------
%%  The pair is sorted before hashing, so every walk of a cycle
%%  picks the same winner whatever the argument order
%%-----------------------------------------------------------------
-spec drop_coin(reference(), reference()) -> reference().
drop_coin(Ref1, Ref2)->
  Tie =
    if
      Ref1 < Ref2 ->
        {Ref1, Ref2};
      true ->
        {Ref2, Ref1}
    end,
  Winner = erlang:phash2(Tie, 2) + 1,
  element(Winner, Tie).

%%-----------------------------------------------------------------
%%  A lock not yet visited is scheduled once: local ones ahead of
%%  the locks to expand here, remote ones under their node for a hop
%%-----------------------------------------------------------------
-spec schedule([lock_key()], [lock_key()], visited(), hops()) ->
  {[lock_key()], visited(), hops()}.
schedule([Lock | Rest], Local, Visited, Hops) when is_map_key(Lock, Visited)->
  schedule(Rest, Local, Visited, Hops);
schedule([{_Scope, _Term, Node} = Lock | Rest], Local, Visited, Hops) when Node =:= node()->
  schedule(Rest, [Lock | Local], Visited#{Lock => true}, Hops);
schedule([{_Scope, _Term, Node} = Lock | Rest], Local, Visited, Hops)->
  % The first lock of the node starts its list
  Locks = maps:get(Node, Hops, []),
  schedule(Rest, Local, Visited#{Lock => true}, Hops#{Node => [Lock | Locks]});
schedule([], Local, Visited, Hops)->
  {Local, Visited, Hops}.

%%-----------------------------------------------------------------
%%  The origin lost: its abort breaks every cycle through it, the
%%  closers collected before stay and no hop goes out. Otherwise the
%%  losing closers are aborted at their managers and the remote locks
%%  go out, one probe per node with the visited set of the walk
%%-----------------------------------------------------------------
-spec verdict({origin, lock_key()} | {[loser()], visited(), hops()}, #deadlock_probe{}) -> ok.
verdict(
    {origin, Winner},
    #deadlock_probe{
      ref = Ref,
      manager = Manager
    }
)->
  ?TRACE(g_verdict, Ref, origin),
  ecall:send(Manager, #deadlock{ref = Ref, winner = Winner}),
  ok;
verdict(
    {Losers, Visited, Hops},
    #deadlock_probe{
      edge = Edge
    } = Probe
)->
  [ begin
      ?TRACE(g_verdict, Ref, {closer, Probe#deadlock_probe.ref}),
      ecall:send(Manager, #deadlock{ref = Ref, winner = Edge})
    end || {Ref, Manager} <- Losers ],
  maps:foreach(
    fun(Node, Locks)->
      ?TRACE(g_hop, Probe#deadlock_probe.ref, {Node, length(Locks), map_size(Visited)}),
      ecall:send({?MODULE, Node}, Probe#deadlock_probe{expand = Locks, visited = Visited})
    end,
    Hops
  ).
