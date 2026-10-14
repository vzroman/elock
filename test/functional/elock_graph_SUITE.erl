%% The per-node graph table, its owner, publications and captured walks.
%% Fixtures intentionally inspect public ETS rows; verdicts reach collectors.
-module(elock_graph_SUITE).
-include("elock.hrl").
-include("elock_test.hrl").

%% Common Test callbacks
-export([all/0, groups/0, init_per_suite/1, end_per_suite/1,
  init_per_testcase/2, end_per_testcase/2]).

%% Test cases
-export([graph_process_test/1, handle_add_edges_test/1,
  handle_remove_edges_test/1, walk_closer_loses_test/1,
  walk_origin_loses_test/1, walk_tie_test/1, walk_skips_origin_test/1,
  walk_chain_test/1, walk_hop_test/1, handle_nodedown_test/1,
  drop_coin_test/1, graph_worker_publishes_on_start_test/1,
  graph_worker_grant_test/1, graph_worker_grant_without_holds_test/1,
  graph_worker_remove_test/1, graph_worker_deadlock_test/1,
  graph_worker_client_down_test/1]).

-define(WINNER, {winner_scope, winner_term, 'winner@node'}).

%% Every case uses distinct keys and removes its fixture rows afterwards.
all()->
  [graph_process_test, handle_add_edges_test, handle_remove_edges_test,
    walk_closer_loses_test, walk_origin_loses_test, walk_tie_test,
    walk_skips_origin_test, walk_chain_test, walk_hop_test,
    handle_nodedown_test, drop_coin_test, {group, graph_worker}].

groups()->
  [{graph_worker, [], [graph_worker_publishes_on_start_test,
    graph_worker_grant_test, graph_worker_grant_without_holds_test,
    graph_worker_remove_test, graph_worker_deadlock_test,
    graph_worker_client_down_test]}].

init_per_suite(Config)->
  {ok, _} = application:ensure_all_started(elock),
  [Peer] = distributed_tests_utils:start_nodes([#{name => graph_peer}], [?MODULE]),
  [{peer, Peer} | Config].

end_per_suite(Config)->
  distributed_tests_utils:stop_nodes([?config(peer, Config)]).

init_per_testcase(Case, Config)->
  [{scope, Case} | Config].

end_per_testcase(_Case, Config)->
  elock_test_utils:stop_clients(),
  Result = ?safe(begin
    elock_test_utils:wait_graph_idle(node()),
    elock_test_utils:wait_graph_idle(?config(peer, Config))
  end),
  elock_test_utils:stop_collectors(),
  case Result of
    ok-> ok;
    Error-> {fail, {graph_not_idle, Error}}
  end.

%% The application owns the high-priority graph and its concurrent ETS bag.
graph_process_test(_Config)->
  Graph = whereis(elock_graph),
  ?assert(is_pid(Graph)),
  ?assertEqual({priority, high}, process_info(Graph, priority)),
  ?assertEqual({message_queue_data, off_heap},
    process_info(Graph, message_queue_data)),
  {dictionary, Dictionary} = process_info(Graph, dictionary),
  [Supervisor | _] = proplists:get_value('$ancestors', Dictionary),
  ?assertEqual({supervisor, elock_sup, 1},
    proc_lib:translate_initial_call(Supervisor)),
  ?assertEqual({ok, elock}, application:get_application(Supervisor)),
  ?assert(lists:keymember(Graph, 2, supervisor:which_children(Supervisor))),
  ?assert(lists:keymember(elock, 1, application:which_applications())),
  [ ?assertEqual(Value, ets:info(elock_graph, Key)) || {Key, Value} <-
    [{owner, Graph}, {type, bag}, {named_table, true}, {protection, public},
      {keypos, #waiter.lock}, {read_concurrency, true},
      {write_concurrency, auto}] ],
  ok.

%% Duplicate holds launch nothing; additions merge and walk only changed keys.
handle_add_edges_test(_Config)->
  graph_fixture([node()], fun()->
    Edge = key(),
    Old = key(),
    New = key(),
    Ref = make_ref(),
    Manager = elock_test_utils:collector(),
    Closer = row(Old, 1, #{Edge => self()}),
    ets:insert(elock_graph, Closer),
    Add = #add_edges{lock = Edge, ref = Ref, birth = 2, age = 10,
      client = self(), holds = #{Old => self()}},
    ok = elock_graph:handle_add_edges(Add),
    verdict(Ref, Old),
    Expected = #waiter{lock = Edge, ref = Ref, birth = 2,
      client = self(), holds = #{Old => self()}},
    ?assertEqual([Expected], ets:lookup(elock_graph, Edge)),
    ok = elock_graph:handle_add_edges(Add),
    ?NO_MESSAGE,
    ?assertEqual([Expected], ets:lookup(elock_graph, Edge)),
    ets:insert(elock_graph, row(New, 1, #{Edge => self()})),
    ok = elock_graph:handle_add_edges(Add#add_edges{client = Manager,
      holds = #{Old => self(), New => self()}}),
    verdict(Ref, New),
    ?NO_MESSAGE,
    ?assertEqual([Expected#waiter{holds = #{Old => self(), New => self()}}],
      ets:lookup(elock_graph, Edge)),
    % Only the old key has a closer; adding an unrelated leaf must not walk it.
    Leaf = key(),
    ets:delete(elock_graph, New),
    ok = elock_graph:handle_add_edges(Add#add_edges{
      holds = #{Old => self(), Leaf => self()}}),
    ?NO_MESSAGE,
    ok = elock_graph:handle_add_edges(Add#add_edges{holds = #{Old => Manager}}),
    verdict(Ref, Old),
    ?assertEqual([Expected#waiter{holds = #{Old => Manager, New => self(), Leaf => self()}}],
      ets:lookup(elock_graph, Edge)),
    ?NO_MESSAGE
  end).

%% Removing one reference at one key preserves other references and copies.
handle_remove_edges_test(_Config)->
  graph_fixture([node()], fun()->
    Lock = key(),
    A = row(Lock, 1, #{}),
    B = row(Lock, 2, #{}),
    Copy = A#waiter{lock = key()},
    ets:insert(elock_graph, [A, B, Copy]),
    ok = elock_graph:handle_remove_edges(#remove_edges{lock = Lock, ref = A#waiter.ref}),
    ?assertEqual([B], ets:lookup(elock_graph, Lock)),
    ?assertEqual([Copy], ets:lookup(elock_graph, Copy#waiter.lock)),
    ok = elock_graph:handle_remove_edges(#remove_edges{lock = Lock, ref = make_ref()}),
    ?assertEqual([B], ets:lookup(elock_graph, Lock))
  end).

%% An older origin aborts every younger closer, and receives no verdict itself.
walk_closer_loses_test(_Config)->
  graph_fixture([node()], fun()->
    Edge = key(),
    Held = key(),
    Origin = row(Edge, 1, #{Held => self()}),
    lists:foreach(fun(Count)->
      Closers = [ row(Held, 2, #{Edge => self()}) || _ <- lists:seq(1, Count) ],
      ets:insert(elock_graph, Closers),
      ok = elock_graph:launch(Origin, [Held], 0),
      [ verdict(Closer#waiter.ref, Edge) || Closer <- Closers ],
      ?NO_MESSAGE,
      ets:delete(elock_graph, Held)
    end, [1, 3])
  end).

%% An older closer stops the walk before collected younger losers are told.
walk_origin_loses_test(_Config)->
  graph_fixture([node()], fun()->
    Edge = key(),
    Held = key(),
    Origin = row(Edge, 2, #{Held => self()}),
    Younger = row(Held, 3, #{Edge => self()}),
    Older = row(Held, 1, #{Edge => self()}),
    Probe = probe(Origin),
    ?assertEqual(origin, elock_graph:check_cycles([Younger, Older], Probe, [], [])),
    ets:insert(elock_graph, [Younger, Older]),
    ok = elock_graph:launch(Origin, [Held], 0),
    verdict(Origin#waiter.ref, Held),
    ?NO_MESSAGE
  end).

%% Equal births use the deterministic, symmetric reference coin.
walk_tie_test(_Config)->
  graph_fixture([node()], fun()->
    Edge = key(),
    Held = key(),
    Origin = row(Edge, 1, #{Held => self()}),
    Ref = Origin#waiter.ref,
    Win = ref_until(fun(R)-> elock_graph:drop_coin(R, Ref) =:= R end),
    Lose = ref_until(fun(R)-> elock_graph:drop_coin(R, Ref) =:= Ref end),
    lists:foreach(fun({CloserRef, LoserRef, Winner})->
      ets:insert(elock_graph, (row(Held, 1, #{Edge => self()}))#waiter{ref = CloserRef}),
      ok = elock_graph:launch(Origin, [Held], 0),
      verdict(LoserRef, Winner),
      ?NO_MESSAGE,
      ets:delete(elock_graph, Held)
    end, [{Win, Ref, Held}, {Lose, Lose, Edge}])
  end).

%% The origin's other copy is neither a closer nor an expansion source.
walk_skips_origin_test(_Config)->
  graph_fixture([node()], fun()->
    Origin = row(key(), 1, #{key() => self()}),
    Copy = Origin#waiter{lock = key(), holds = #{Origin#waiter.lock => self(),
      key() => self()}},
    ?assertEqual({[], []}, elock_graph:check_cycles([Copy], probe(Origin), [], [])),
    ets:insert(elock_graph, Copy),
    ok = elock_graph:launch(Origin, [Copy#waiter.lock], 0),
    ?NO_MESSAGE
  end).

%% Expansion through a middle waiter finds a three-cycle; a leaf is quiet.
walk_chain_test(_Config)->
  graph_fixture([node()], fun()->
    Edge = key(),
    First = key(),
    Last = key(),
    Origin = row(Edge, 3, #{First => self()}),
    Middle = row(First, 2, #{Last => self()}),
    Closer = row(Last, 1, #{Edge => self()}),
    ets:insert(elock_graph, [Middle, Closer]),
    ok = elock_graph:launch(Origin, [First], 0),
    verdict(Origin#waiter.ref, Last),
    ets:delete(elock_graph, Last),
    ok = elock_graph:launch(Origin, [First], 0),
    ?NO_MESSAGE
  end).

%% Both small ecall hops and >=50-key erpc hops reach the peer's closer.
walk_hop_test(Config)->
  graph_fixture([node(), ?config(peer, Config)], fun()->
    Peer = ?config(peer, Config),
    lists:foreach(fun(LeafCount)->
      Edge = key(),
      Remote = {?MODULE, make_ref(), Peer},
      Leaves = maps:from_list([ {key(), self()} || _ <- lists:seq(1, LeafCount) ]),
      Origin = row(Edge, 2, Leaves#{Remote => self()}),
      Closer = row(Remote, 1, #{Edge => self()}),
      true = rpc:call(Peer, ets, insert, [elock_graph, Closer]),
      ok = elock_graph:launch(Origin, maps:keys(Origin#waiter.holds), 0),
      verdict(Origin#waiter.ref, Remote),
      true = rpc:call(Peer, ets, delete_object, [elock_graph, Closer])
    end, [0, 49])
  end).

%% Node cleanup removes both replacement versions, preserving other clients.
handle_nodedown_test(_Config)->
  graph_fixture([node()], fun()->
    Node = 'graph_client@unreachable',
    Client = elock_test_utils:remote_pid(Node, 1),
    Row = (row(key(), 1, #{}))#waiter{client = Client},
    Replacement = Row#waiter{holds = #{key() => self()}},
    Other = row(key(), 2, #{}),
    ets:insert(elock_graph, [Row, Replacement, Other]),
    whereis(elock_graph) ! {nodedown, Node, []},
    ?WAIT(ets:lookup(elock_graph, Row#waiter.lock) =:= []),
    ?assertEqual([Other], ets:lookup(elock_graph, Other#waiter.lock))
  end).

drop_coin_test(_Config)->
  Pairs = [ {make_ref(), make_ref()} || _ <- lists:seq(1, 200) ],
  Outcomes =
    [ begin
        Winner = elock_graph:drop_coin(A, B),
        ?assert(Winner =:= A orelse Winner =:= B),
        ?assertEqual(Winner, elock_graph:drop_coin(B, A)),
        ?assertEqual(Winner, elock_graph:drop_coin(A, B)),
        Winner =:= min(A, B)
      end || {A, B} <- Pairs ],
  % both sides win now and then
  ?assert(lists:member(true, Outcomes)),
  ?assert(lists:member(false, Outcomes)).


%%=================================================================
%% Fixtures and verdicts
%%=================================================================
key()-> {?MODULE, make_ref(), node()}.

row(Lock, Birth, Holds)->
  #waiter{lock = Lock, ref = make_ref(), birth = Birth,
    client = self(), holds = Holds}.

probe(#waiter{lock = Edge, ref = Ref, birth = Birth, client = Client})->
  #deadlock_probe{ref = Ref, edge = Edge, birth = Birth, client = Client,
    limit = 100, expand = [], visited = #{Edge => true}}.

verdict(Ref, Winner)->
  ?assertEqual(#deadlock{ref = Ref, winner = Winner},
    ?RECEIVE(#deadlock{ref = Ref})).

ref_until(Pred)->
  ref_until(Pred, 10000).
ref_until(_Pred, 0)->
  erlang:error(no_such_ref);
ref_until(Pred, Attempts)->
  Ref = make_ref(),
  case Pred(Ref) of
    true-> Ref;
    false-> ref_until(Pred, Attempts - 1)
  end.

%% Synthetic rows are owned by the case; cleanup is guaranteed on failure.
%% Retire active walks first so a late publication cannot recreate a fixture.
graph_fixture(Nodes, Fun)->
  Before = [{Node, rpc:call(Node, ets, tab2list, [elock_graph])} || Node <- Nodes],
  try Fun()
  after
    try
      ?WAIT(elock_test_utils:quiet(Nodes))
    after
      [ begin
        Rows = rpc:call(Node, ets, tab2list, [elock_graph]),
        [ rpc:call(Node, ets, delete_object, [elock_graph, Row])
          || Row <- Rows -- Original ]
      end || {Node, Original} <- Before ]
    end
  end.

%%=================================================================
%%  Graph workers
%%=================================================================
%% One row with all holds, idempotent repetition, and no row for empty holds.
graph_worker_publishes_on_start_test(Config)->
  Scope = ?config(scope, Config),
  Key = {Scope, held, node()},
  CloserClient = elock_test_utils:collector(),
  Closer = #waiter{lock = Key, ref = make_ref(), birth = 2,
    client = CloserClient, holds = #{{Scope, t, node()} => self()}},
  ets:insert(elock_graph, Closer),
  try
    lists:foreach(fun(Holds)->
      {Worker, Mon, Ref, _Tag} = graph_worker(Scope, self(), Holds),
      Queued = #queued{ref = Ref, manager = self(), node = node()},
      ?WAIT(process_info(Worker, current_function) =:=
        {current_function, {elock_graph, graph_loop, 1}}),
      case map_size(Holds) of
        0->
          ?WAIT(elock_test_utils:quiet([node()])),
          ?assertEqual([], elock_test_utils:graph_rows(Scope, t));
        _->
          ?WAIT(elock_test_utils:graph_rows(Scope, t) =:=
            [#waiter{lock = {Scope, t, node()}, ref = Ref, birth = 1,
              client = Worker, holds = Holds}]),
          ?assertEqual([#deadlock{ref = Closer#waiter.ref,
            winner = {Scope, t, node()}}],
            elock_test_utils:collected(CloserClient, 1))
      end,
      Worker ! Queued,
      ?WAIT(elock_test_utils:quiet([node()])),
      ExpectedRows = case map_size(Holds) of
        0-> [];
        _-> [#waiter{lock = {Scope, t, node()}, ref = Ref, birth = 1,
          client = Worker, holds = Holds}]
      end,
      ?assertEqual(ExpectedRows, elock_test_utils:graph_rows(Scope, t)),
      ?NO_MESSAGE,
      Worker ! {remove, Ref},
      ?RECEIVE({'DOWN', Mon, process, Worker, normal}),
      ?WAIT(elock_test_utils:graph_rows(Scope, t) =:= [])
    end, [#{}, #{Key => self()}])
  after
    ets:delete_object(elock_graph, Closer)
  end.

%% Grants propagate into queued rows; a local grant removes its waiting copy.
graph_worker_grant_test(Config)->
  Scope = ?config(scope, Config),
  Other = ?config(peer, Config),
  graph_fixture([node(), Other], fun()->
    Holds = #{{Scope, held, node()} => self()},
    {Worker, Mon, Ref, _Tag} = graph_worker(Scope, self(), Holds),
    ?WAIT(length(elock_test_utils:graph_rows(Scope, t)) =:= 1),
    % This copy is a sentinel: Other never queued in this worker. An
    % unconditional removal cast would delete it, even though local holds grow.
    Sentinel = #waiter{lock = {Scope, t, Other}, ref = Ref, birth = 1,
      client = self(), holds = #{}},
    true = rpc:call(Other, ets, insert, [elock_graph, Sentinel]),
    Updated = Holds#{{Scope, t, Other} => self()},
    Session = trace:session_create(?FUNCTION_NAME, self(), []),
    try
      1 = trace:function(Session, {elock_graph, remove_edges, 2}, true, [local]),
      1 = trace:process(Session, Worker, true, [call]),
      Worker ! {add, Ref, Other, self()},
      ?WAIT(elock_test_utils:graph_rows(Scope, t) =:=
        [#waiter{lock = {Scope, t, node()}, ref = Ref, birth = 1,
          client = Worker, holds = Updated}]),
      ?WAIT(elock_test_utils:quiet([node(), Other])),
      % Observe dispatch locally: quiet cannot see a remote cast in flight.
      ?assertEqual([], graph_removals(Session, Worker)),
      ?assertEqual([Sentinel], rpc:call(Other, ets, lookup,
        [elock_graph, Sentinel#waiter.lock])),
      Worker ! {add, Ref, node(), self()},
      ?WAIT(elock_test_utils:graph_rows(Scope, t) =:= []),
      ?assertEqual([[node()]], graph_removals(Session, Worker))
    after
      trace:session_destroy(Session)
    end,
    Worker ! #queued{ref = Ref, manager = self(), node = node()},
    ?WAIT(elock_test_utils:graph_rows(Scope, t) =:=
      [#waiter{lock = {Scope, t, node()}, ref = Ref, birth = 1,
        client = Worker, holds = Updated#{{Scope, t, node()} => self()}}]),
    Worker ! {remove, Ref},
    ?RECEIVE({'DOWN', Mon, process, Worker, normal}),
    ?WAIT(elock_test_utils:graph_rows(Scope, t) =:= [])
  end).

%% The first grant starts publication when the context initially held nothing.
graph_worker_grant_without_holds_test(Config)->
  Scope = ?config(scope, Config),
  Other = ?config(peer, Config),
  {Worker, Mon, Ref, _Tag} = graph_worker(Scope, self(), #{}),
  ?WAIT(process_info(Worker, current_function) =:=
    {current_function, {elock_graph, graph_loop, 1}}),
  ?WAIT(elock_test_utils:quiet([node()])),
  ?assertEqual([], elock_test_utils:graph_rows(Scope, t)),
  Worker ! {add, Ref, Other, self()},
  ?WAIT(elock_test_utils:graph_rows(Scope, t) =:=
    [#waiter{lock = {Scope, t, node()}, ref = Ref, birth = 1,
      client = Worker, holds = #{{Scope, t, Other} => self()}}]),
  Worker ! {remove, Ref},
  ?RECEIVE({'DOWN', Mon, process, Worker, normal}),
  ?WAIT(elock_test_utils:graph_rows(Scope, t) =:= []).

graph_worker_remove_test(Config)-> graph_worker_stop(Config, remove).
%% A deadlock record is preserved as the graph worker's exit reason.
graph_worker_deadlock_test(Config)-> graph_worker_stop(Config, deadlock).
%% The client monitor retires the same publication on process death.
graph_worker_client_down_test(Config)-> graph_worker_stop(Config, client_down).

graph_worker_stop(Config, How)->
  Scope = ?config(scope, Config),
  Client = elock_test_utils:client(),
  {Worker, Mon, Ref, Tag} = graph_worker(Scope, Client,
    #{{Scope, held, node()} => self()}),
  ?WAIT(length(elock_test_utils:graph_rows(Scope, t)) =:= 1),
  Reason = case How of
    remove-> Worker ! {remove, Ref}, normal;
    deadlock->
      Verdict = #deadlock{ref = Ref, winner = ?WINNER},
      Worker ! Verdict,
      {Tag, Verdict};
    client_down-> elock_test_utils:stop(Client), normal
  end,
  ?RECEIVE({'DOWN', Mon, process, Worker, Reason}),
  ?WAIT(elock_test_utils:graph_rows(Scope, t) =:= []).

graph_worker(Scope, Client, Holds)->
  Ref = make_ref(),
  Tag = make_ref(),
  Manager = self(),
  {Worker, Mon} = spawn_monitor(fun()->
    elock_graph:init_graph(#queued{ref = Ref, manager = Manager, node = node()},
      #graph{client = Client, scope = Scope, term = t,
        nodes = [], holds = Holds, birth = 1, age = 1, ref = Ref, tag = Tag})
  end),
  {Worker, Mon, Ref, Tag}.

%% Finish the worker's current update before the trace delivery barrier, so
%% every dispatch caused by that update is in this testcase's mailbox.
graph_removals(Session, Worker)->
  ?WAIT(process_info(Worker, [current_function, message_queue_len]) =:=
    [{current_function, {elock_graph, graph_loop, 1}}, {message_queue_len, 0}]),
  Delivered = trace:delivered(Session, Worker),
  ?RECEIVE({trace_delivered, Worker, Delivered}),
  graph_removals(Worker).

graph_removals(Worker)->
  receive
    {trace, Worker, call, {elock_graph, remove_edges, [Nodes, _Graph]}}->
      [Nodes | graph_removals(Worker)]
  after 0->
    []
  end.
