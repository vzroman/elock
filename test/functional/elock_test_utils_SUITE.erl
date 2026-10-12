%% Regression tests for the shared test helpers and failure reporting.
-module(elock_test_utils_SUITE).

-include("elock.hrl").
-include("elock_test.hrl").

%% Common Test API
-export([all/0, init_per_suite/1, end_per_suite/1]).

%% Test cases
-export([
  quiet_sees_graph_work_test/1,
  safe_result_test/1,
  safe_exception_test/1,
  finish_scope_failure_test/1,
  peer_modules_loaded_test/1
]).

all()->
  [safe_result_test, safe_exception_test, finish_scope_failure_test,
    peer_modules_loaded_test, quiet_sees_graph_work_test].

%% Every locking suite starts the graph application on the controller.
init_per_suite(Config)->
  {ok, _} = application:ensure_all_started(elock),
  Config.

%% The application may be shared by the following suites.
end_per_suite(_Config)-> ok.

%% Evaluate once in the calling process and preserve its result.
safe_result_test(_Config)->
  Ref = make_ref(),
  ?assertEqual({ok, Ref}, ?safe(begin
    self() ! {safe_evaluated, Ref},
    {ok, Ref}
  end)),
  ?RECEIVE({safe_evaluated, Ref}),
  ?NO_MESSAGE.

%% Repeated and nested expansions preserve every exception class and stack.
safe_exception_test(_Config)->
  Reason = {failure, make_ref()},
  Stack = [{?MODULE, safe_exception_test, 1, [{line, ?LINE}]}],
  ?assertEqual(Reason, ?safe(throw(Reason))),
  ?assertEqual({'EXIT', Reason}, ?safe(exit(Reason))),
  ?assertMatch({'EXIT', {Reason, [_ | _]}}, ?safe(erlang:error(Reason))),
  ?assertEqual({'EXIT', {Reason, Stack}},
    ?safe(erlang:raise(error, Reason, Stack))),
  ?assertEqual(Reason, ?safe(?safe(throw(Reason)))),
  ?assertEqual({'EXIT', Reason}, ?safe(?safe(exit(Reason)))).

%% A leaked entry fails teardown with its original error and stack;
%% the scope is still stopped after the failure.
finish_scope_failure_test(_Config)->
  Scope = ?FUNCTION_NAME,
  Holder = elock_test_utils:start_scope(Scope),
  try
    true = ets:insert(Scope, {leaked_entry, self(), 1}),
    ?assertMatch(
      {fail, {scope_not_idle, {'EXIT', {{wait_until_timeout, _}, [_ | _]}}}},
      elock_test_utils:finish_scope(Holder)
    ),
    ?assertNot(is_process_alive(Holder)),
    ?assertEqual(undefined, ets:whereis(Scope))
  after
    elock_test_utils:stop_scope(Holder)
  end.

%% Suite closures must be ready before any client executes them, on
%% initial peers, peers added later and new incarnations of the same node.
peer_modules_loaded_test(_Config)->
  try
    [First] = distributed_tests_utils:start_nodes([#{name => first}], [?MODULE]),
    peer_closures(First),
    Added = distributed_tests_utils:start_node(#{name => added}),
    peer_closures(Added),
    ok = distributed_tests_utils:stop_node(First),
    [PeerName, _Host] = string:split(atom_to_list(First), "@"),
    First = distributed_tests_utils:start_node(#{name => first, peer_name => PeerName}),
    peer_closures(First)
  after
    elock_test_utils:stop_clients(),
    distributed_tests_utils:stop_nodes(distributed_tests_utils:nodes())
  end.

peer_closures(Node)->
  ?assertMatch({file, _}, rpc:call(Node, code, is_loaded, [?MODULE])),
  Clients = [ elock_test_utils:client(Node) || _ <- lists:seq(1, 50) ],
  Requests = [
    {elock_test_utils:cast(Client, fun()-> integer_to_list(Index) end), Index}
    || {Client, Index} <- lists:zip(Clients, lists:seq(1, length(Clients)))
  ],
  [ ?assertEqual({ok, integer_to_list(Index)}, elock_test_utils:result(Ref, ?DEADLINE))
    || {Ref, Index} <- Requests ],
  [ elock_test_utils:stop(Client) || Client <- Clients ],
  ok.

%% Observe the real proxy/client/graph chain and refuse a busy graph worker.
quiet_sees_graph_work_test(_Config)->
  Scope = ?FUNCTION_NAME,
  Holder = elock_test_utils:start_scope(Scope),
  C1 = elock_test_utils:client(),
  C2 = elock_test_utils:client(),
  Empty = elock_test_utils:client(),
  try
    {ok, Ref1} = elock_test_utils:lock(C1, Scope, t1, [node()]),
    {ok, Ref2} = elock_test_utils:lock(C2, Scope, t2, [node()]),
    Request = elock_test_utils:lock_queued(C1, Scope, t2, [node()], #{}),
    [#waiter{client = Graph, ref = Ref}] = elock_test_utils:graph_rows(Scope, t2),
    ?assert(elock_test_utils:published(node(), C1, Scope, t2)),
    ?assertNot(elock_test_utils:published(node(), Empty, Scope, t2)),
    ?assertEqual([], elock_test_utils:graph_walkers()),
    ?WAIT(elock_test_utils:quiet([node()])),
    true = erlang:suspend_process(Graph),
    try
      Graph ! #queued{ref = Ref, manager = self(), node = node()},
      ?assertNotEqual(true, elock_test_utils:quiet([node()]))
    after
      true = erlang:resume_process(Graph)
    end,
    ?WAIT(elock_test_utils:quiet([node()])),
    ?assertMatch([#waiter{}], elock_test_utils:graph_rows(Scope, t2)),
    ok = elock_test_utils:unlock(C2, Ref2),
    {ok, {ok, Ref3}} = elock_test_utils:result(Request, ?DEADLINE),
    ok = elock_test_utils:unlock(C1, Ref3),
    ok = elock_test_utils:unlock(C1, Ref1)
  after
    elock_test_utils:stop_clients(),
    ok = elock_test_utils:finish_scope(Holder)
  end.
