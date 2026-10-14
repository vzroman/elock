
%%=================================================================
%%  The wait-for graph of the node and the deadlock walks.
%%
%%  One process per node, registered as elock_graph. It owns a public
%%  ETS bag of the waiting requests that hold something, keyed by the
%%  lock they wait for:
%%
%%      #waiter{ lock, ref, birth, client, holds }
%%
%%  The rows of a lock are complete on its node and nowhere else: a
%%  walk expands a lock on its node and hops for the locks held on
%%  the others.
%%
%%  The graph worker of a request (see elock_context) publishes its
%%  holds on every node it is queued on: handle_add_edges/1 is called
%%  after each #queued{} and each later grant, handle_remove_edges/1
%%  is cast when the node grants, the request finishes or its client
%%  dies. The process itself only drops the rows of a departed node.
%%
%%  A walk serves one launch: the origin request, waiting for Edge
%%  with its Birth. Every new hold of a waiter starts one, so the
%%  edge that closes a cycle finds the rest in place. A waiter of an
%%  expanded lock that holds Edge closes a cycle: compared, never
%%  expanded. One that beats the origin ends the walk with
%%  #deadlock{} to the origin, the losers get theirs when the branch
%%  is done. The other waiters are expanded through their holds, a
%%  lock once per branch. The remote locks go out as one
%%  #deadlock_probe{} cast per node with the visited set of the
%%  branch, and the cast walks on from them. A launch has a visited
%%  limit from the origin's holds and age, past it the origin loses
%%  with probe_limit.
%%
%%  Walkers read the table while updates run: a replacement inserts
%%  before deleting the old row, so a walk may read either or both.
%%  Verdicts go to graph workers and never change the table
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

-export([
  init_graph/2
]).
%%=================================================================
%%  The table row. The messages are in elock.hrl
%%=================================================================
-record(waiter,{
  lock :: lock_key(),           % the lock the request waits for, the key
  ref :: reference(),           % the request
  birth :: non_neg_integer(),   % priority fixed for the life of the context
  client :: pid(),              % the graph worker, receives the verdicts
  holds :: held_locks()         % as the graph worker sent them
}).

-type visited() :: #{lock_key() => true}.
-type hops() :: #{node() => [lock_key()]}.
-type loser() :: {reference(), pid()}.

-define(BASE_FACTOR, 10).
-define(MIN_VISITED, 100).
-define(MAX_VISITED, 10000).
-define(AGE_STEP, 100). % milliseconds

%%=================================================================
%%	OTP API
%%=================================================================
-spec start_link() -> {ok, pid()} | {error, term()}.
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
%%  The rows of the departed node's clients, both versions of a
%%  replacement. An update in flight can publish after this pass
%%-----------------------------------------------------------------
-spec handle_nodedown(node()) -> ok.
handle_nodedown(Node)->
  ets:select_delete(?MODULE, [{
    #waiter{client = '$1', _ = '_'},
    [{'=:=', {node, '$1'}, Node}],
    [true]
  }]),
  ok.

-spec init_graph(#queued{}, #graph{}) -> ok.
init_graph(
    #queued{
      node = Node
    },
    #graph{
      client = Client
    } = Graph
)->
  erlang:monitor(process, Client),
  graph_loop(queued(Node, Graph)).

%% The request waits on Node: the holds are published there
-spec queued(node(), #graph{}) -> #graph{}.
queued(
    Node,
    #graph{
      nodes = Nodes0,
      holds = Holds
    } = Graph
)->
  add_edges([Node], Holds, Graph),
  Graph#graph{
    nodes = ordsets:add_element(Node, Nodes0)
  }.

-spec graph_loop(#graph{}) -> ok.
graph_loop(#graph{
  ref = Ref,
  client = Client,
  scope = Scope,
  term = Term,
  nodes = Nodes0,
  holds = Holds0,
  tag = Tag
} = Graph)->
  receive
    #queued{ref = Ref, node = Node}->
      graph_loop(queued(Node, Graph));
    #deadlock{ ref = Ref } = DeadLock->
      remove_edges(Nodes0, Graph),
      exit({Tag,DeadLock});
    {remove, Ref}->
      remove_edges(Nodes0, Graph);
    {add, Ref, Node, Manager}->
      % Only a node that reported #queued{} has a row to remove
      Nodes =
        case ordsets:is_element(Node, Nodes0) of
          true->
            remove_edges([Node], Graph),
            ordsets:del_element(Node, Nodes0);
          false->
            Nodes0
        end,
      Holds = #{ {Scope, Term, Node} => Manager },
      add_edges(Nodes, Holds, Graph),
      graph_loop(Graph#graph{
        nodes = Nodes,
        holds = maps:merge(Holds0, Holds)
      });
    {'DOWN', _MonRef, process, Client, _Reason}->
      remove_edges(Nodes0, Graph)
  end.

%%-----------------------------------------------------------------
%%  Calls: the holds are in place before the next update or removal.
%%  An unreachable node fails the request through its worker
%%-----------------------------------------------------------------
-spec add_edges([node()], held_locks(), #graph{}) -> ok.
add_edges(
    Nodes,
    Holds,
    #graph{
      scope = Scope,
      term = Term,
      birth = Birth,
      age = Age,
      ref = Ref
    }
) when map_size(Holds) > 0->
  AddEdges = #add_edges{
    ref = Ref,
    birth = Birth,
    age = Age,
    client = self(),
    holds = Holds
  },
  lists:foreach(
    fun(Node)->
      ecall:call(Node, ?MODULE, handle_add_edges, [
        AddEdges#add_edges{ lock = {Scope, Term, Node} }
      ])
    end,
    Nodes
  );
add_edges(_Nodes, _Holds, _Graph)->
  ok.

%%-----------------------------------------------------------------
%%  Casts: the additions were calls, a removal never overtakes them
%%-----------------------------------------------------------------
-spec remove_edges([node()], #graph{}) -> ok.
remove_edges(Nodes, #graph{
  scope = Scope,
  term = Term,
  ref = Ref
})->
  RemoveEdges = #remove_edges{ref = Ref},
  lists:foreach(
    fun(Node)->
      ecall:cast(Node, ?MODULE, handle_remove_edges, [
        RemoveEdges#remove_edges{ lock = {Scope, Term, Node} }
      ])
    end,
    Nodes
  ).
%%=================================================================
%%  The edges
%%=================================================================
%%-----------------------------------------------------------------
%%  No row: the first publication. A row: a later grant of a multi
%%  node request, or a key held at a manager that died and was
%%  replaced. Only the new keys are launched. Holds is never empty
%%  (see add_edges/3)
%%-----------------------------------------------------------------
-spec handle_add_edges(#add_edges{}) -> ok.
handle_add_edges(#add_edges{
  lock = Lock,
  ref = Ref,
  birth = Birth,
  age = Age,
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
      spawn(fun()-> launch(Row, maps:keys(Holds), Age) end),
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
          spawn(fun()-> launch(Row, maps:keys(New), Age) end),
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
%%  A launch: the new holds of a waiter of this node. Edge is visited
%%  from the start: its waiters depend on its holders, not on the
%%  origin, and a barging origin holds it itself
%%-----------------------------------------------------------------
-spec launch(#waiter{}, [lock_key()], non_neg_integer()) -> ok.
launch(
    #waiter{
      lock = Edge,
      ref = Ref,
      birth = Birth,
      client = Client,
      holds = Holds
    },
    NewKeys,
    Age
)->
  Limit = min(?MAX_VISITED, max(?MIN_VISITED,
    (?BASE_FACTOR + Age div ?AGE_STEP) * map_size(Holds))),
  Probe = #deadlock_probe{
    ref = Ref,
    edge = Edge,
    client = Client,
    birth = Birth,
    limit = Limit
  },
  Result = case schedule(NewKeys, [], #{Edge => true}, #{}, Limit) of
    probe_limit->
      {origin, probe_limit};
    {Entry, Visited, Hops}->
      walk(Entry, Probe, Visited, [], Hops)
  end,
  verdict(Result, Probe).

%%-----------------------------------------------------------------
%%  A hop: the sender has visited the entry locks already
%%-----------------------------------------------------------------
-spec handle_probe(#deadlock_probe{expand :: [lock_key()], visited :: visited()}) -> ok.
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
  {origin, lock_key() | probe_limit} | {[loser()], visited(), hops()}.
walk([Lock | Rest], #deadlock_probe{limit = Limit} = Probe, Visited0, Losers0, Hops0)->
  case check_cycles(ets:lookup(?MODULE, Lock), Probe, Losers0, []) of
    origin->
      {origin, Lock};
    {Losers, Found}->
      case schedule(Found, Rest, Visited0, Hops0, Limit) of
        probe_limit->
          {origin, probe_limit};
        {Next, Visited, Hops}->
          walk(Next, Probe, Visited, Losers, Hops)
      end
  end;
walk([], _Probe, Visited, Losers, Hops)->
  {Losers, Visited, Hops}.

%%-----------------------------------------------------------------
%%  The origin's own row, waiting here as a multi node request: a
%%  barging one holds Edge and would close a cycle with itself. Its
%%  holds were launched from here already
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
%%  A holder of Edge closes a cycle: compared, never expanded. The
%%  others are expanded through their holds
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
%%  An unvisited lock is scheduled once per branch: a local one ahead
%%  of the locks to expand here, a remote one under its node for the
%%  hop. Edge counts towards the limit, an unseen key past it aborts
%%  the origin
%%-----------------------------------------------------------------
-spec schedule([lock_key()], [lock_key()], visited(), hops(), pos_integer()) ->
  {[lock_key()], visited(), hops()} | probe_limit.
schedule([Lock | Rest], Local, Visited, Hops, Limit) when is_map_key(Lock, Visited)->
  schedule(Rest, Local, Visited, Hops, Limit);
schedule([_Lock | _Rest], _Local, Visited, _Hops, Limit) when map_size(Visited) >= Limit->
  probe_limit;
schedule([{_Scope, _Term, Node} = Lock | Rest], Local, Visited, Hops, Limit) when Node =:= node()->
  schedule(Rest, [Lock | Local], Visited#{Lock => true}, Hops, Limit);
schedule([{_Scope, _Term, Node} = Lock | Rest], Local, Visited, Hops, Limit)->
  Locks = maps:get(Node, Hops, []),
  schedule(Rest, Local, Visited#{Lock => true}, Hops#{Node => [Lock | Locks]}, Limit);
schedule([], Local, Visited, Hops, _Limit)->
  {Local, Visited, Hops}.

%%-----------------------------------------------------------------
%%  The origin lost: its abort breaks every cycle through it, the
%%  losers collected before stay and no hop goes out. Otherwise the
%%  losers get their verdicts and the hops go out
%%-----------------------------------------------------------------
-spec verdict({origin, lock_key() | probe_limit} | {[loser()], visited(), hops()}, #deadlock_probe{}) -> ok.
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
  forward_probe(Probe#deadlock_probe{ visited = Visited }, Hops).

%%-----------------------------------------------------------------
%%  A big probe bypasses the batched ecall channel
%%-----------------------------------------------------------------
-spec forward_probe(#deadlock_probe{visited :: visited()}, hops()) -> ok.
forward_probe(
    #deadlock_probe{
      visited = Visited
    }=Probe,
    Hops
) when map_size(Visited) < 50->
  maps:foreach(
    fun(Node, Locks)->
      ecall:cast(Node, ?MODULE, handle_probe, [
        Probe#deadlock_probe{expand = Locks}
      ])
    end,
    Hops
  );
forward_probe(Probe, Hops)->
  maps:foreach(
    fun(Node, Locks)->
      erpc:cast(Node, ?MODULE, handle_probe, [Probe#deadlock_probe{expand = Locks}])
    end,
    Hops
  ).

