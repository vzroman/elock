
%%=================================================================
%%  The wait-for graph of the node and the deadlock walks.
%%
%%  One process per node, registered as elock_graph, started by
%%  elock_sup. It owns a public ETS bag of the waiting requests of
%%  the node that hold something, keyed by the lock they wait for:
%%
%%      #waiter{ lock, ref, birth, client, holds }
%%
%%  The waiters at a key all belong to one manager, which runs on
%%  the lock's node: the rows of the waiters at lock L are complete
%%  on node(L) and nowhere else. A walk expands a lock on its node,
%%  and hops to the other nodes for the locks it has there.
%%
%%  The graph worker of elock_context calls handle_add_edges/1 after
%%  a manager reports #queued{}, and adds each later grant to the
%%  waiting nodes. New holds are published before an independent
%%  walker starts and the call returns. The worker casts
%%  handle_remove_edges/1 when a node grants or the request finishes,
%%  and when its client dies.
%%  The owner only receives node status and removes departed clients.
%%
%%  A walk serves one launch: the origin request, waiting for Edge
%%  with Birth, fixed for the life of the context. Every new hold
%%  of a waiting request starts a walk when it appears. During normal
%%  updates the last edge of a stable cycle starts a walk that finds
%%  the rest in place. The waiters of an expanded lock depend on the
%%  origin: it holds the lock, or a waiter expanded before does.
%%    * a waiter holding Edge closes a cycle: compared,
%%      never expanded. A closer that beats the origin ends the walk,
%%      #deadlock{} goes to the origin client and nothing else is
%%      sent. The losing closers are collected.
%%    * the other waiters are expanded through the locks they hold.
%%      A lock is visited from the moment it is scheduled, so no lock
%%      is expanded twice in a branch and the walk ends.
%%  When the list is empty every collected closer gets #deadlock{}
%%  at its client, then the remote locks go out as one
%%  #deadlock_probe{} cast per node, with this branch's visited set.
%%  Separate branches can discover the same previously unseen lock.
%%  Each cast execution walks on from its entry locks.
%%
%%  Walkers read the table while updates run. Replacement inserts
%%  before deleting the old object, so a walk may read either or both.
%%  Verdicts go to request clients and never change the table.
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
%%  Graph worker and walker API
%%=================================================================
-export([
  handle_add_edges/1,
  handle_remove_edges/1,
  handle_probe/1
]).

%%=================================================================
%%  Client graph worker and walker protocol is in elock.hrl
%%=================================================================
-record(waiter,{
  lock :: lock_key(),           % the lock the request waits for, the key
  ref :: reference(),           % the request
  birth :: non_neg_integer(),   % priority fixed for the life of the context
  client :: pid(),              % the request client receiving verdicts
  holds :: held_locks()         % #{lock_key() => pid()}, as the client sent it
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

-spec init() -> no_return().
init()->
  process_flag(message_queue_data, off_heap),
  process_flag(priority, high),
  ets:new(?MODULE, [named_table, bag, public, {keypos, #waiter.lock},
    {read_concurrency, true}, {write_concurrency, auto}]),
  register(?MODULE, self()),
  net_kernel:monitor_nodes(true, [{node_type, all}]),
  proc_lib:init_ack({ok, self()}),
  loop().

%%=================================================================
%%  The process
%%=================================================================
-spec loop() -> no_return().
loop()->
  receive
    {nodedown, Node, _Info}->
      handle_nodedown(Node);
    {nodeup, _Node, _Info}->
      ok
  end,
  loop().

%%-----------------------------------------------------------------
%%  Remove departed clients' rows, including both replacement versions.
%%  Updates still in flight can publish after this cleanup pass
%%-----------------------------------------------------------------
-spec handle_nodedown(node()) -> ok.
handle_nodedown(Node)->
  ets:select_delete(?MODULE, [{
    #waiter{client = '$1', _ = '_'},
    [{'=:=', {node, '$1'}, Node}],
    [true]
  }]),
  ok.

%%=================================================================
%%  The edges
%%=================================================================
%%-----------------------------------------------------------------
%%  No row: the first publication by the client graph worker. A row: a
%%  later grant of a multi node request, or a key held at a manager
%%  that died and was replaced. The new keys only are launched: every
%%  edge starts a walk when it appears. Birth stays with the row.
%%  Holds is never empty (see elock_context:add_edges/3)
%%-----------------------------------------------------------------
-spec handle_add_edges(#add_edges{}) -> ok.
handle_add_edges(#add_edges{
  lock = Lock,
  ref = Ref,
  birth = Birth,
  client = Client,
  holds = Holds
})->
  case ets:match_object(?MODULE, #waiter{lock = Lock, ref = Ref, _ = '_'}) of
    []->
      Row = #waiter{
        lock = Lock,
        ref = Ref,
        birth = Birth,
        client = Client,
        holds = Holds
      },
      ets:insert(?MODULE, Row),
      spawn(fun()-> launch(Row, maps:keys(Holds)) end),
      ok;
    [#waiter{holds = Holds0} = Row0]->
      case new_held_locks(Holds, Holds0) of
        New when map_size(New) =:= 0->
          % The grant repeats a key of the context the client has sent already
          ok;
        New->
          Row = Row0#waiter{
            holds = maps:merge(Holds0, New)
          },
          ets:insert(?MODULE, Row),
          ets:delete_object(?MODULE, Row0),
          spawn(fun()-> launch(Row, maps:keys(New)) end),
          ok
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
      birth = Birth,
      client = Client
    },
    NewKeys
)->
  Probe = #deadlock_probe{
    ref = Ref,
    edge = Edge,
    client = Client,
    birth = Birth
  },
  {Entry, Visited, Hops} = schedule(NewKeys, [], #{Edge => true}, #{}),
  Result = walk(Entry, Probe, Visited, [], Hops),
  verdict(Result, Probe).

%%-----------------------------------------------------------------
%%  A hop: the sender has scheduled the entry locks in visited.
%%  The cast execution continues the walk directly
%%-----------------------------------------------------------------
-spec handle_probe(#deadlock_probe{}) -> ok.
handle_probe(#deadlock_probe{
  expand = Entry,
  visited = Visited
} = Probe)->
  Result = walk(Entry, Probe, Visited, [], #{}),
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
%%  A waiter holding the origin's lock closes a cycle: compared,
%%  never expanded. Its abort breaks every cycle through it, and so
%%  does the origin's. The others expand through the locks they hold
%%-----------------------------------------------------------------
check_cycles(
    [#waiter{
      ref = Ref,
      birth = CloserBirth,
      client = Client,
      holds = Holds
    } | Rest],
    #deadlock_probe{
      edge = Edge
    } = Probe,
    Losers,
    Found
)->
  case Holds of
    #{Edge := _}->
      case beats(CloserBirth, Ref, Probe) of
        true->
          origin;
        false->
          check_cycles(Rest, Probe, [{Ref, Client} | Losers], Found)
      end;
    _->
      check_cycles(Rest, Probe, Losers, maps:keys(Holds) ++ Found)
  end;
check_cycles([], _Probe, Losers, Found)->
  {Losers, Found}.

%%-----------------------------------------------------------------
%%  Earlier birth wins, the coin on a tie
%%-----------------------------------------------------------------
-spec beats(non_neg_integer(), reference(), #deadlock_probe{}) -> boolean().
beats(
    CloserBirth,
    CloserRef,
    #deadlock_probe{
      ref = Ref,
      birth = Birth
    }
)->
  if
    CloserBirth > Birth ->
      false;
    CloserBirth < Birth ->
      true;
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
%%  A lock not yet visited is scheduled once per branch: local ones
%%  ahead of the locks to expand here, remote ones under their node
%%  for a hop
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
%%  losing closers receive verdicts at their clients and the remote
%%  locks go out, one probe cast per node with this branch's visited set
%%-----------------------------------------------------------------
-spec verdict({origin, lock_key()} | {[loser()], visited(), hops()}, #deadlock_probe{}) -> ok.
verdict(
    {origin, Winner},
    #deadlock_probe{
      ref = Ref,
      client = Client
    }
)->
  ecall:send(Client, #deadlock{ref = Ref, winner = Winner}),
  ok;
verdict(
    {Losers, Visited, Hops},
    #deadlock_probe{
      edge = Edge
    } = Probe
)->
  [ begin
      ecall:send(Client, #deadlock{ref = Ref, winner = Edge})
    end || {Ref, Client} <- Losers ],
  maps:foreach(
    fun(Node, Locks)->
      ecall:cast(Node, ?MODULE, handle_probe, [
        Probe#deadlock_probe{expand = Locks, visited = Visited}
      ])
    end,
    Hops
  ).
