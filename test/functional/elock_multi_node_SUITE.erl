%%=================================================================
%%  Functional tests of elock across nodes. Three peer nodes n1, n2,
%%  n3 (util/distributed_tests_utils) run the scope of every test
%%  case, the clients live on the peers and the controller (the ct
%%  node) drives them through elock_test_utils. Asserted: locks on
%%  a remote node and on several nodes (the tables and the managers
%%  per node, the context of a client that spans nodes), contention
%%  and timeouts of multi node requests (the partial grants and the
%%  withdrawal of the queued copies), deadlocks through several
%%  nodes and scopes, the death of clients, of nodes and of scopes,
%%  and the membership of the scope (ready_nodes). The death tests
%%  start a fourth node inside the test case and kill it there: the
%%  cluster of three must be intact afterwards.
%%
%%  A request that has to wait is issued asynchronously (lock_async
%%  or lock_queued with the nodes to watch) and its verdict is
%%  observed with result/2: no verdict within the quiet window means
%%  the request is still waiting. A late #queued{} may stay in a
%%  client's mailbox after its request completed (the grant may
%%  overtake the notification), hence nothing asserts an empty
%%  client mailbox
%%=================================================================
-module(elock_multi_node_SUITE).

-include("elock.hrl").
-include("elock_test.hrl").

%% API
-export([
  all/0,
  groups/0,
  suite/0,
  init_per_testcase/2,
  end_per_testcase/2,
  init_per_group/2,
  end_per_group/2,
  init_per_suite/1,
  end_per_suite/1
]).

-export([
  ready_nodes_test/1,
  single_remote_node_lock_test/1,
  duplicate_nodes_collapse_test/1,
  multi_node_lock_test/1,
  context_spans_nodes_test/1,
  shared_multi_node_test/1,
  multi_node_contention_test/1,
  remote_fifo_order_test/1,
  multi_node_partial_grant_then_wait_test/1,
  multi_node_timeout_test/1,
  multi_node_timeout_two_queued_test/1,
  multi_node_deadlock_test/1,
  cross_scope_multi_node_deadlock_test/1,
  ring_across_nodes_test/1,
  same_term_opposite_order_deadlock_test/1,
  weight_across_nodes_test/1,
  deadlock_withdraws_queued_copy_test/1,
  client_node_dies_test/1,
  manager_node_dies_test/1,
  remote_client_process_dies_test/1,
  waiting_multi_node_client_dies_test/1,
  dead_client_leaves_no_workers_test/1,
  unreachable_node_test/1,
  node_without_scope_test/1,
  ready_nodes_after_restart_test/1
]).

%% Internal: run on the peers
-export([
  processes_in/1
]).

% mirrors elock.erl
-record(context,{
  ref2lock,
  locked
}).
-record(lock,{
  scope,
  term,
  nodes
}).

-define(SHARED, #{is_shared => true}).
-define(EXCLUSIVE, #{}).

% The size of the cluster the test cases run on
-define(CLUSTER, 3).

% The verdict of a cycle comes within a second
-define(VERDICT, 1000).

% The test cases that need a second scope
-define(TWO_SCOPE_TESTS, [
  cross_scope_multi_node_deadlock_test
]).

all()->
  [
    {group, basic},
    {group, contention},
    {group, deadlocks},
    {group, failures},
    {group, scope}
  ].

groups()->
  [
    {basic, [], [
      ready_nodes_test,
      single_remote_node_lock_test,
      duplicate_nodes_collapse_test,
      multi_node_lock_test,
      context_spans_nodes_test,
      shared_multi_node_test
    ]},
    {contention, [], [
      multi_node_contention_test,
      remote_fifo_order_test,
      multi_node_partial_grant_then_wait_test,
      multi_node_timeout_test,
      multi_node_timeout_two_queued_test
    ]},
    {deadlocks, [], [
      multi_node_deadlock_test,
      cross_scope_multi_node_deadlock_test,
      ring_across_nodes_test,
      same_term_opposite_order_deadlock_test,
      weight_across_nodes_test,
      deadlock_withdraws_queued_copy_test
    ]},
    {failures, [], [
      client_node_dies_test,
      manager_node_dies_test,
      remote_client_process_dies_test,
      waiting_multi_node_client_dies_test,
      dead_client_leaves_no_workers_test,
      unreachable_node_test,
      node_without_scope_test
    ]},
    {scope, [], [
      ready_nodes_after_restart_test
    ]}
  ].

suite()->
  [{timetrap, {minutes, 10}}].

%%-----------------------------------------------------------------
%%  The cluster: three peer nodes, connected, ecall running and
%%  connected both ways between every pair
%%-----------------------------------------------------------------
init_per_suite(Config)->
  Nodes = distributed_tests_utils:start_nodes([#{name => n1}, #{name => n2}, #{name => n3}]),
  ?assertEqual(?CLUSTER, length(Nodes)),
  Config.

end_per_suite(_Config)->
  distributed_tests_utils:stop_nodes(distributed_tests_utils:nodes()).

init_per_group(_Group, Config)->
  Config.

end_per_group(_Group, _Config)->
  ok.

%%-----------------------------------------------------------------
%%  Every test case gets its own scope named after it on every node
%%  of the cluster, ready on all of them; the cases of
%%  ?TWO_SCOPE_TESTS a second one as well. The nodes come from the
%%  state of the harness: a test case may replace one (see
%%  ready_nodes_after_restart_test)
%%-----------------------------------------------------------------
init_per_testcase(TestCase, Config)->
  Nodes = cluster(),
  Holders = start_scopes(Nodes, TestCase),
  Config1 = [{nodes, Nodes}, {scope, TestCase}, {holders, Holders} | Config],
  case lists:member(TestCase, ?TWO_SCOPE_TESTS) of
    true->
      Scope2 = list_to_atom(atom_to_list(TestCase) ++ "_second"),
      Holders2 = start_scopes(Nodes, Scope2),
      [{scope2, Scope2}, {holders2, Holders2} | Config1];
    false->
      Config1
  end.

%%-----------------------------------------------------------------
%%  Every client is stopped, every scope must be idle on every node
%%  (no entry, no manager) - the scopes of a node the test case has
%%  stopped went with it. A leak fails the test case. A fourth node
%%  left behind by a failed death test is stopped
%%-----------------------------------------------------------------
end_per_testcase(_TestCase, Config)->
  elock_test_utils:stop_clients(),
  Holders = ?config(holders, Config) ++ proplists:get_value(holders2, Config, []),
  Alive = distributed_tests_utils:nodes(),
  Results = [ elock_test_utils:finish_scope(Holder) || {Node, Holder} <- Holders, lists:member(Node, Alive) ],
  [ catch distributed_tests_utils:stop_node(Extra) || Extra <- extra_nodes() ],
  case [ Fail || {fail, _} = Fail <- Results ] of
    []-> ok;
    [Fail | _]-> Fail
  end.

%%=================================================================
%%  Basic
%%=================================================================
%%-----------------------------------------------------------------
%%  Every node sees all three nodes of the scope. Stopping the scope
%%  holder of n3 removes n3 from the view of the others (n3 itself
%%  has no scope any more: no ready nodes); restarting the scope on
%%  n3 adds it back; stopping it again removes it again
%%-----------------------------------------------------------------
ready_nodes_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2, N3] = Nodes = ?config(nodes, Config),
  [ ?assertEqual(lists:sort(Nodes), lists:sort(ready_nodes(Node, Scope))) || Node <- Nodes ],

  ?assertEqual(ok, elock_test_utils:stop_scope(holder(N3, Config))),
  elock_test_utils:wait_ready(Scope, [N1, N2]),
  ?assertEqual([], ready_nodes(N3, Scope)),

  Holder3 = elock_test_utils:start_scope(N3, Scope),
  elock_test_utils:wait_ready(Scope, Nodes),

  ?assertEqual(ok, elock_test_utils:finish_scope(Holder3)),
  elock_test_utils:wait_ready(Scope, [N1, N2]),
  ?assertEqual([], ready_nodes(N3, Scope)).

%%-----------------------------------------------------------------
%%  A client on n1 locks a term on n2 only: the entry and the
%%  manager are on n2 and nowhere else, the manager runs on n2, the
%%  context has the single key {Scope, t, n2}; idle everywhere after
%%  the unlock, the manager exits
%%-----------------------------------------------------------------
single_remote_node_lock_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2, N3] = Nodes = ?config(nodes, Config),
  C1 = elock_test_utils:client(N1),

  {ok, Ref} = elock_test_utils:lock(C1, Scope, t, [N2]),
  Manager = elock_test_utils:wait_manager(N2, Scope, t),
  ?assertEqual(N2, node(Manager)),
  ?assertEqual([{t, Manager, 1}], elock_test_utils:locks(N2, Scope)),
  ?assertEqual([], elock_test_utils:locks(N1, Scope)),
  ?assertEqual([], elock_test_utils:locks(N3, Scope)),
  ?assertEqual([Manager], elock_test_utils:managers(N2)),
  ?assertEqual([], elock_test_utils:managers(N1)),
  ?assertEqual([], elock_test_utils:managers(N3)),
  ?assertEqual(#context{
    ref2lock = #{ Ref => #lock{ scope = Scope, term = t, nodes = #{ N2 => Manager } } },
    locked = #{ {Scope, t, N2} => {Manager, 1} }
  }, elock_test_utils:context(C1)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  wait_idle(Nodes, Scope),
  elock_test_utils:wait_dead(Manager),
  stop([C1]).

%%-----------------------------------------------------------------
%%  Duplicates in the node list collapse: [n2, n2, n2] is the single
%%  node path - the entry is on n2 only and the context has one key
%%-----------------------------------------------------------------
duplicate_nodes_collapse_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2, N3] = Nodes = ?config(nodes, Config),
  C1 = elock_test_utils:client(N1),

  {ok, Ref} = elock_test_utils:lock(C1, Scope, t, [N2, N2, N2]),
  Manager = elock_test_utils:wait_manager(N2, Scope, t),
  ?assertEqual([{t, Manager, 1}], elock_test_utils:locks(N2, Scope)),
  ?assertEqual([], elock_test_utils:locks(N1, Scope)),
  ?assertEqual([], elock_test_utils:locks(N3, Scope)),
  ?assertEqual(#context{
    ref2lock = #{ Ref => #lock{ scope = Scope, term = t, nodes = #{ N2 => Manager } } },
    locked = #{ {Scope, t, N2} => {Manager, 1} }
  }, elock_test_utils:context(C1)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  wait_idle(Nodes, Scope),
  stop([C1]).

%%-----------------------------------------------------------------
%%  A lock on all three nodes: an entry with the ticket 1 and one
%%  manager on every node, each manager running on its node, three
%%  keys in the context with the three managers; idle everywhere
%%  after the unlock
%%-----------------------------------------------------------------
multi_node_lock_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2, N3] = Nodes = ?config(nodes, Config),
  C1 = elock_test_utils:client(N1),

  {ok, Ref} = elock_test_utils:lock(C1, Scope, t, Nodes),
  #{ N1 := M1, N2 := M2, N3 := M3 } = Managers = managers_of(Nodes, Scope, t),
  ?assertEqual(3, length(lists:usort([M1, M2, M3]))),
  [ begin
      ?assertEqual(Node, node(Manager)),
      ?assertEqual([{t, Manager, 1}], elock_test_utils:locks(Node, Scope)),
      ?assertEqual([Manager], elock_test_utils:managers(Node))
    end || {Node, Manager} <- maps:to_list(Managers) ],
  ?assertEqual(#context{
    ref2lock = #{ Ref => #lock{ scope = Scope, term = t, nodes = #{ N1 => M1, N2 => M2, N3 => M3 } } },
    locked = #{ {Scope, t, N1} => {M1, 1}, {Scope, t, N2} => {M2, 1}, {Scope, t, N3} => {M3, 1} }
  }, elock_test_utils:context(C1)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  wait_idle(Nodes, Scope),
  stop([C1]).

%%-----------------------------------------------------------------
%%  One client with t on [n1, n2] and u on [n3]: three keys in the
%%  context, held_locks/1 names the three managers by key; the
%%  unlock of t leaves the key of u alone, the unlock of u leaves
%%  nothing
%%-----------------------------------------------------------------
context_spans_nodes_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2, N3] = Nodes = ?config(nodes, Config),
  C1 = elock_test_utils:client(N1),

  {ok, RefT} = elock_test_utils:lock(C1, Scope, t, [N1, N2]),
  {ok, RefU} = elock_test_utils:lock(C1, Scope, u, [N3]),
  MT1 = elock_test_utils:wait_manager(N1, Scope, t),
  MT2 = elock_test_utils:wait_manager(N2, Scope, t),
  MU3 = elock_test_utils:wait_manager(N3, Scope, u),
  Context = elock_test_utils:context(C1),
  ?assertEqual(#context{
    ref2lock = #{
      RefT => #lock{ scope = Scope, term = t, nodes = #{ N1 => MT1, N2 => MT2 } },
      RefU => #lock{ scope = Scope, term = u, nodes = #{ N3 => MU3 } }
    },
    locked = #{
      {Scope, t, N1} => {MT1, 1},
      {Scope, t, N2} => {MT2, 1},
      {Scope, u, N3} => {MU3, 1}
    }
  }, Context),
  ?assertEqual(#{
    {Scope, t, N1} => MT1,
    {Scope, t, N2} => MT2,
    {Scope, u, N3} => MU3
  }, elock:held_locks(Context)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, RefT)),
  ?assertEqual(#context{
    ref2lock = #{ RefU => #lock{ scope = Scope, term = u, nodes = #{ N3 => MU3 } } },
    locked = #{ {Scope, u, N3} => {MU3, 1} }
  }, elock_test_utils:context(C1)),
  ?WAIT(elock_test_utils:locks(N1, Scope) =:= [] andalso elock_test_utils:locks(N2, Scope) =:= []),
  ?assertEqual([{u, MU3, 1}], elock_test_utils:locks(N3, Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, RefU)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  wait_idle(Nodes, Scope),
  stop([C1]).

%%-----------------------------------------------------------------
%%  Shared holders of one term on [n1, n2] from clients on three
%%  different nodes share it with one manager per node; an
%%  exclusive request on [n1, n2] from n1 queues on both nodes
%%  behind all three (the tables show the ticket 4) and is granted
%%  by the last unlock only; idle after its unlock
%%-----------------------------------------------------------------
shared_multi_node_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2, N3] = Nodes = ?config(nodes, Config),
  [C1, C2, C3] = [ elock_test_utils:client(Node) || Node <- Nodes ],
  C4 = elock_test_utils:client(N1),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [N1, N2], ?SHARED),
  M1 = elock_test_utils:wait_manager(N1, Scope, t),
  M2 = elock_test_utils:wait_manager(N2, Scope, t),
  {ok, Ref2} = elock_test_utils:lock(C2, Scope, t, [N1, N2], ?SHARED),
  {ok, Ref3} = elock_test_utils:lock(C3, Scope, t, [N1, N2], ?SHARED),
  ?assertEqual([{t, M1, 3}], elock_test_utils:locks(N1, Scope)),
  ?assertEqual([{t, M2, 3}], elock_test_utils:locks(N2, Scope)),
  ?assertEqual([], elock_test_utils:locks(N3, Scope)),
  Lock = #lock{ scope = Scope, term = t, nodes = #{ N1 => M1, N2 => M2 } },
  [ ?assertEqual(#context{
      ref2lock = #{ Ref => Lock },
      locked = #{ {Scope, t, N1} => {M1, 1}, {Scope, t, N2} => {M2, 1} }
    }, elock_test_utils:context(C)) || {C, Ref} <- [{C1, Ref1}, {C2, Ref2}, {C3, Ref3}] ],

  R4 = elock_test_utils:lock_async(C4, Scope, t, [N1, N2], ?EXCLUSIVE),
  ?WAIT(elock_test_utils:locks(N1, Scope) =:= [{t, M1, 4}]),
  ?WAIT(elock_test_utils:locks(N2, Scope) =:= [{t, M2, 4}]),
  still_waiting(R4),
  ?assertEqual(undefined, elock_test_utils:context(C4)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  still_waiting(R4),
  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  still_waiting(R4),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  Ref4 = granted(R4),
  ?assertEqual(#context{
    ref2lock = #{ Ref4 => Lock },
    locked = #{ {Scope, t, N1} => {M1, 1}, {Scope, t, N2} => {M2, 1} }
  }, elock_test_utils:context(C4)),
  ?assertEqual([{t, M1, 4}], elock_test_utils:locks(N1, Scope)),
  ?assertEqual([{t, M2, 4}], elock_test_utils:locks(N2, Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C4, Ref4)),
  wait_idle(Nodes, Scope),
  stop([C1, C2, C3, C4]).

%%=================================================================
%%  Contention
%%=================================================================
%%-----------------------------------------------------------------
%%  C1 (n1) holds t on all three nodes; C2 (n2) asks for all three:
%%  queued on every node (the tables show the ticket 2), pending
%%  without a context; C1's unlock grants C2 on every node with the
%%  same managers; idle everywhere after C2's unlock
%%-----------------------------------------------------------------
multi_node_contention_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2, N3] = Nodes = ?config(nodes, Config),
  C1 = elock_test_utils:client(N1),
  C2 = elock_test_utils:client(N2),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, Nodes),
  #{ N1 := M1, N2 := M2, N3 := M3 } = managers_of(Nodes, Scope, t),

  R2 = elock_test_utils:lock_async(C2, Scope, t, Nodes, ?EXCLUSIVE),
  ?WAIT(elock_test_utils:locks(N1, Scope) =:= [{t, M1, 2}]),
  ?WAIT(elock_test_utils:locks(N2, Scope) =:= [{t, M2, 2}]),
  ?WAIT(elock_test_utils:locks(N3, Scope) =:= [{t, M3, 2}]),
  still_waiting(R2),
  ?assertEqual(undefined, elock_test_utils:context(C2)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref2 = granted(R2),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  ?assertEqual(#context{
    ref2lock = #{ Ref2 => #lock{ scope = Scope, term = t, nodes = #{ N1 => M1, N2 => M2, N3 => M3 } } },
    locked = #{ {Scope, t, N1} => {M1, 1}, {Scope, t, N2} => {M2, 1}, {Scope, t, N3} => {M3, 1} }
  }, elock_test_utils:context(C2)),
  [ ?assertEqual([{t, M, 2}], elock_test_utils:locks(N, Scope)) || {N, M} <- [{N1, M1}, {N2, M2}, {N3, M3}] ],

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  ?assertEqual(undefined, elock_test_utils:context(C2)),
  wait_idle(Nodes, Scope),
  stop([C1, C2]).

%%-----------------------------------------------------------------
%%  Waiters from three nodes (one of them local to the term) for a
%%  term on n3 are granted in the order of their tickets, one per
%%  unlock, whichever node they come from
%%-----------------------------------------------------------------
remote_fifo_order_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2, N3] = Nodes = ?config(nodes, Config),
  C1 = elock_test_utils:client(N1),
  C2 = elock_test_utils:client(N2),
  C3 = elock_test_utils:client(N3),
  C4 = elock_test_utils:client(N1),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [N3]),
  Manager = elock_test_utils:wait_manager(N3, Scope, t),
  R2 = elock_test_utils:lock_queued(N3, C2, Scope, t, [N3], ?EXCLUSIVE),
  R3 = elock_test_utils:lock_queued(N3, C3, Scope, t, [N3], ?EXCLUSIVE),
  R4 = elock_test_utils:lock_queued(N3, C4, Scope, t, [N3], ?EXCLUSIVE),
  ?assertEqual([{t, Manager, 4}], elock_test_utils:locks(N3, Scope)),
  [ still_waiting(R) || R <- [R2, R3, R4] ],

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref2 = granted(R2),
  [ still_waiting(R) || R <- [R3, R4] ],

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  Ref3 = granted(R3),
  still_waiting(R4),

  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  Ref4 = granted(R4),
  ?assertEqual([{t, Manager, 4}], elock_test_utils:locks(N3, Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C4, Ref4)),
  wait_idle(Nodes, Scope),
  stop([C1, C2, C3, C4]).

%%-----------------------------------------------------------------
%%  C1 (n2) holds t on n2 only; C2 (n1) asks [n1, n2, n3]: granted
%%  on n1 and n3 at once (new managers with the ticket 1, both
%%  monitoring C2), queued on n2 with the ticket 2, pending without
%%  a context - a multi node request holds what it has got while it
%%  waits; C1's unlock grants it: three keys in the context; idle
%%  everywhere after
%%-----------------------------------------------------------------
multi_node_partial_grant_then_wait_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2, N3] = Nodes = ?config(nodes, Config),
  C1 = elock_test_utils:client(N2),
  C2 = elock_test_utils:client(N1),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [N2]),
  M2 = elock_test_utils:wait_manager(N2, Scope, t),

  R2 = elock_test_utils:lock_async(C2, Scope, t, Nodes, ?EXCLUSIVE),
  M1 = elock_test_utils:wait_manager(N1, Scope, t),
  M3 = elock_test_utils:wait_manager(N3, Scope, t),
  ?assertEqual([{t, M1, 1}], elock_test_utils:locks(N1, Scope)),
  ?assertEqual([{t, M3, 1}], elock_test_utils:locks(N3, Scope)),
  ?WAIT(elock_test_utils:locks(N2, Scope) =:= [{t, M2, 2}]),
  ?WAIT(monitors(M1, C2) andalso monitors(M2, C2) andalso monitors(M3, C2)),
  still_waiting(R2),
  ?assertEqual(undefined, elock_test_utils:context(C2)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref2 = granted(R2),
  ?assertEqual(#context{
    ref2lock = #{ Ref2 => #lock{ scope = Scope, term = t, nodes = #{ N1 => M1, N2 => M2, N3 => M3 } } },
    locked = #{ {Scope, t, N1} => {M1, 1}, {Scope, t, N2} => {M2, 1}, {Scope, t, N3} => {M3, 1} }
  }, elock_test_utils:context(C2)),
  ?assertEqual([{t, M2, 2}], elock_test_utils:locks(N2, Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  wait_idle(Nodes, Scope),
  stop([C1, C2]).

%%-----------------------------------------------------------------
%%  The same with a timeout: {error, timeout} within the window; the
%%  nodes granted meanwhile are released (n1 and n3 idle), the copy
%%  queued on n2 is withdrawn (n2 shows C1's manager only, C2 is not
%%  monitored there), no context; n2 idle after C1's unlock
%%-----------------------------------------------------------------
multi_node_timeout_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2, N3] = Nodes = ?config(nodes, Config),
  C1 = elock_test_utils:client(N2),
  C2 = elock_test_utils:client(N1),
  Timeout = 300,

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [N2]),
  M2 = elock_test_utils:wait_manager(N2, Scope, t),

  R2 = timed_lock(C2, Scope, t, Nodes, #{timeout => Timeout}),
  ?WAIT(elock_test_utils:locks(N2, Scope) =:= [{t, M2, 2}]),
  ?WAIT(monitors(M2, C2)),
  {{error, timeout}, Elapsed} = verdict(C2, R2),
  elapsed_within(Elapsed, Timeout),
  ?assertEqual(undefined, elock_test_utils:context(C2)),
  elock_test_utils:wait_idle(N1, Scope),
  elock_test_utils:wait_idle(N3, Scope),
  ?assertEqual([{t, M2, 2}], elock_test_utils:locks(N2, Scope)),
  ?WAIT(not monitors(M2, C2)),
  ?assert(monitors(M2, C1)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  wait_idle(Nodes, Scope),
  stop([C1, C2]).

%%-----------------------------------------------------------------
%%  A timeout with two queued copies: C1 (n2) holds t on n2 and n3;
%%  C2 (n1) asks all three with a timeout - granted on n1, queued on
%%  n2 and n3. {error, timeout} within the window, n1 released, both
%%  queued copies withdrawn (n2 and n3 show C1's managers only, C2
%%  is not monitored there), no context; idle after C1's unlock. The
%%  two copies time out at about the same moment: the verdict of
%%  the first withdraws the other whether it has timed out already
%%  or not
%%-----------------------------------------------------------------
multi_node_timeout_two_queued_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2, N3] = Nodes = ?config(nodes, Config),
  C1 = elock_test_utils:client(N2),
  C2 = elock_test_utils:client(N1),
  Timeout = 300,

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [N2, N3]),
  M2 = elock_test_utils:wait_manager(N2, Scope, t),
  M3 = elock_test_utils:wait_manager(N3, Scope, t),

  R2 = timed_lock(C2, Scope, t, Nodes, #{timeout => Timeout}),
  ?WAIT(elock_test_utils:locks(N2, Scope) =:= [{t, M2, 2}]),
  ?WAIT(elock_test_utils:locks(N3, Scope) =:= [{t, M3, 2}]),
  {{error, timeout}, Elapsed} = verdict(C2, R2),
  elapsed_within(Elapsed, Timeout),
  ?assertEqual(undefined, elock_test_utils:context(C2)),
  elock_test_utils:wait_idle(N1, Scope),
  ?assertEqual([{t, M2, 2}], elock_test_utils:locks(N2, Scope)),
  ?assertEqual([{t, M3, 2}], elock_test_utils:locks(N3, Scope)),
  ?WAIT(not monitors(M2, C2) andalso not monitors(M3, C2)),
  ?assert(monitors(M2, C1) andalso monitors(M3, C1)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  wait_idle(Nodes, Scope),
  stop([C1, C2]).

%%=================================================================
%%  Deadlocks
%%=================================================================
%%-----------------------------------------------------------------
%%  C1 (n1) holds t1 on n1, C2 (n2) holds t2 on n2; C1 asks t2 on
%%  n2, C2 asks t1 on n1 - with either request closing the cycle:
%%  exactly one {error, deadlock} within a second, the other keeps
%%  waiting until the loser unlocks its term, then it is granted;
%%  the loser is left with no context; idle after
%%-----------------------------------------------------------------
multi_node_deadlock_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2 | _] = Nodes = ?config(nodes, Config),
  lists:foreach(
    fun(Closing)->
      C1 = elock_test_utils:client(N1),
      C2 = elock_test_utils:client(N2),
      Part1 = {C1, [{Scope, t1, [N1]}], {Scope, t2, [N2]}},
      Part2 = {C2, [{Scope, t2, [N2]}], {Scope, t1, [N1]}},
      Requests =
        case Closing of
          c1_closes-> setup([Part2, Part1]);
          c2_closes-> setup([Part1, Part2])
        end,

      {LoserR, Verdict} = any_result([ R || {_, R, _} <- Requests ], ?VERDICT),
      ?assertEqual({error, deadlock}, Verdict),
      {value, {Loser, LoserR, [LoserHeld]}, [{Winner, WinnerR, [WinnerHeld]}]} = lists:keytake(LoserR, 2, Requests),
      still_waiting(WinnerR),
      ?assertMatch(#context{ ref2lock = Ref2Lock } when map_size(Ref2Lock) =:= 1, elock_test_utils:context(Loser)),

      ?assertEqual(ok, elock_test_utils:unlock(Loser, LoserHeld)),
      ?assertEqual(undefined, elock_test_utils:context(Loser)),
      WinnerRef = granted(WinnerR),
      ?assertEqual(ok, elock_test_utils:unlock(Winner, WinnerRef)),
      ?assertEqual(ok, elock_test_utils:unlock(Winner, WinnerHeld)),
      ?assertEqual(undefined, elock_test_utils:context(Winner)),
      wait_idle(Nodes, Scope),
      stop([C1, C2])
    end,
    [c1_closes, c2_closes]
  ).

%%-----------------------------------------------------------------
%%  The same cycle through two scopes: C1 holds t1 in the first
%%  scope on n1, C2 holds t2 in the second scope on n2, each asks
%%  for the other's lock - exactly one loser with either request
%%  closing the cycle, the winner is granted once the loser releases
%%-----------------------------------------------------------------
cross_scope_multi_node_deadlock_test(Config)->
  Scope1 = ?config(scope, Config),
  Scope2 = ?config(scope2, Config),
  [N1, N2 | _] = Nodes = ?config(nodes, Config),
  lists:foreach(
    fun(Closing)->
      C1 = elock_test_utils:client(N1),
      C2 = elock_test_utils:client(N2),
      Part1 = {C1, [{Scope1, t1, [N1]}], {Scope2, t2, [N2]}},
      Part2 = {C2, [{Scope2, t2, [N2]}], {Scope1, t1, [N1]}},
      Requests =
        case Closing of
          c1_closes-> setup([Part2, Part1]);
          c2_closes-> setup([Part1, Part2])
        end,
      Verdicts = resolve(Requests),
      ?assertEqual(1, length(losers(Verdicts))),
      ?assertEqual(1, length(winners(Verdicts))),
      wait_idle(Nodes, Scope1),
      wait_idle(Nodes, Scope2),
      stop([C1, C2])
    end,
    [c1_closes, c2_closes]
  ).

%%-----------------------------------------------------------------
%%  A ring through the three nodes: the client of every node holds
%%  its own term on its node and asks for the term of the next node
%%  on that node, the last request closes the cycle - the probe is
%%  forwarded from manager to manager across the nodes. Exactly one
%%  loser, the two others are granted as the ring drains
%%-----------------------------------------------------------------
ring_across_nodes_test(Config)->
  Scope = ?config(scope, Config),
  Nodes = ?config(nodes, Config),
  Clients = [ elock_test_utils:client(Node) || Node <- Nodes ],
  N = length(Nodes),
  Parts =
    [ {Client, [{Scope, {t, I}, [lists:nth(I, Nodes)]}], {Scope, {t, I rem N + 1}, [lists:nth(I rem N + 1, Nodes)]}}
      || {Client, I} <- lists:zip(Clients, lists:seq(1, N)) ],
  Requests = setup(Parts),
  {LoserR, Verdict} = any_result([ R || {_, R, _} <- Requests ], ?VERDICT),
  ?assertEqual({error, deadlock}, Verdict),
  {value, {Loser, LoserR, [LoserHeld]}, Rest} = lists:keytake(LoserR, 2, Requests),
  [ still_waiting(R) || {_, R, _} <- Rest ],

  ?assertEqual(ok, elock_test_utils:unlock(Loser, LoserHeld)),
  Verdicts = resolve(Rest),
  ?assertEqual([], losers(Verdicts)),
  ?assertEqual(N - 1, length(winners(Verdicts))),
  [ ?assertEqual(undefined, elock_test_utils:context(Client)) || Client <- Clients ],
  wait_idle(Nodes, Scope),
  stop(Clients).

%%-----------------------------------------------------------------
%%  Two clients (n1 and n2) ask for the same term on [n1, n2] at
%%  once, twenty rounds. Either one of them gets both nodes first
%%  and the other waits behind it on both (granted after the
%%  unlock), or each gets one node and waits for the other's - a
%%  cycle closed through the grants they gain while waiting: one of
%%  them loses, releases the node it got and the other is granted.
%%  Every request gets exactly one verdict, a granted client holds
%%  both nodes, nothing hangs, idle at the end of every round
%%-----------------------------------------------------------------
same_term_opposite_order_deadlock_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2 | _] = Nodes = ?config(nodes, Config),
  Rounds = 20,
  Deadlocks =
    lists:foldl(
      fun(_Round, Acc)->
        C1 = elock_test_utils:client(N1),
        C2 = elock_test_utils:client(N2),
        R1 = elock_test_utils:lock_async(C1, Scope, t, [N1, N2], ?EXCLUSIVE),
        R2 = elock_test_utils:lock_async(C2, Scope, t, [N1, N2], ?EXCLUSIVE),

        {FirstR, FirstVerdict} = any_result([R1, R2], ?DEADLINE),
        {First, Other, OtherR} =
          case FirstR of
            R1-> {C1, C2, R2};
            R2-> {C2, C1, R1}
          end,
        Lost =
          case FirstVerdict of
            {ok, FirstRef}->
              holds_both(First, FirstRef, Scope, N1, N2),
              case elock_test_utils:result(OtherR, ?QUIET) of
                timeout->
                  % the other waits behind the first on both nodes
                  ?assertEqual(ok, elock_test_utils:unlock(First, FirstRef)),
                  OtherRef = granted(OtherR),
                  holds_both(Other, OtherRef, Scope, N1, N2),
                  ?assertEqual(ok, elock_test_utils:unlock(Other, OtherRef)),
                  0;
                {ok, {error, deadlock}}->
                  % the other lost the cycle and released its node to the first
                  ?assertEqual(undefined, elock_test_utils:context(Other)),
                  ?assertEqual(ok, elock_test_utils:unlock(First, FirstRef)),
                  1;
                {ok, Unexpected}->
                  erlang:error({unexpected_verdict, Unexpected})
              end;
            {error, deadlock}->
              % the first lost the cycle: the other is granted with what the loser released
              ?assertEqual(undefined, elock_test_utils:context(First)),
              OtherRef = granted(OtherR),
              holds_both(Other, OtherRef, Scope, N1, N2),
              ?assertEqual(ok, elock_test_utils:unlock(Other, OtherRef)),
              1;
            Unexpected->
              erlang:error({unexpected_verdict, Unexpected})
          end,
        wait_idle(Nodes, Scope),
        % no request got a second verdict
        ?assert(elock_test_utils:pending(R1)),
        ?assert(elock_test_utils:pending(R2)),
        ?assertEqual(undefined, elock_test_utils:context(C1)),
        ?assertEqual(undefined, elock_test_utils:context(C2)),
        stop([C1, C2]),
        Acc + Lost
      end,
      0,
      lists:seq(1, Rounds)
    ),
  ct:pal("same term, opposite order: ~p of ~p rounds with a deadlock", [Deadlocks, Rounds]).

%%-----------------------------------------------------------------
%%  The weight counts the nodes: Heavy (n1) holds t on [n1, n2] -
%%  two locks - and asks u on n2; Light (n2) holds u on n2 - one
%%  lock - and asks t on n1. Light always loses, whichever request
%%  closes the cycle, ten rounds each; Heavy is granted once Light
%%  releases u
%%-----------------------------------------------------------------
weight_across_nodes_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2 | _] = Nodes = ?config(nodes, Config),
  lists:foreach(
    fun({Closing, _Round})->
      Heavy = elock_test_utils:client(N1),
      Light = elock_test_utils:client(N2),
      HeavyPart = {Heavy, [{Scope, t, [N1, N2]}], {Scope, u, [N2]}},
      LightPart = {Light, [{Scope, u, [N2]}], {Scope, t, [N1]}},
      Requests =
        case Closing of
          heavy_closes-> setup([LightPart, HeavyPart]);
          light_closes-> setup([HeavyPart, LightPart])
        end,
      Verdicts = resolve(Requests),
      ?assertEqual([Light], losers(Verdicts)),
      ?assertEqual([Heavy], winners(Verdicts)),
      wait_idle(Nodes, Scope),
      stop([Heavy, Light])
    end,
    [ {Closing, Round} || Round <- lists:seq(1, 10), Closing <- [heavy_closes, light_closes] ]
  ).

%%-----------------------------------------------------------------
%%  A multi node request that loses a cycle on one node while its
%%  copy is queued on another. C3 (n3) holds t on n3; C1 (n2) holds
%%  t and y on n2 and waits for x on n1, held by C2 (n1); C2 asks t
%%  on [n2, n3]: the copy on n2 closes the cycle with C1 - weight 2
%%  against C2's 1 - and C2 loses, the copy on n3 waits behind C3.
%%  {error, deadlock} comes back within the deadline, the copy
%%  queued on n3 is withdrawn (n3 shows C3's manager only, C2 is
%%  not monitored there, no proxy is left waiting on n3), n2 shows
%%  C1's manager only, C2 keeps x, C1 still waits for x and is
%%  granted by C2's unlock; idle after
%%-----------------------------------------------------------------
deadlock_withdraws_queued_copy_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2, N3] = Nodes = ?config(nodes, Config),
  C1 = elock_test_utils:client(N2),
  C2 = elock_test_utils:client(N1),
  C3 = elock_test_utils:client(N3),

  {ok, RefT3} = elock_test_utils:lock(C3, Scope, t, [N3]),
  MT3 = elock_test_utils:wait_manager(N3, Scope, t),
  {ok, RefT2} = elock_test_utils:lock(C1, Scope, t, [N2]),
  {ok, RefY2} = elock_test_utils:lock(C1, Scope, y, [N2]),
  MT2 = elock_test_utils:wait_manager(N2, Scope, t),
  {ok, RefX} = elock_test_utils:lock(C2, Scope, x, [N1]),
  MX = elock_test_utils:wait_manager(N1, Scope, x),
  R1 = elock_test_utils:lock_queued(N1, C1, Scope, x, [N1], ?EXCLUSIVE),
  still_waiting(R1),

  R2 = elock_test_utils:lock_async(C2, Scope, t, [N2, N3], ?EXCLUSIVE),
  ?assertEqual({error, deadlock}, verdict(C2, R2)),
  ?assertEqual([{t, MT3, 2}], elock_test_utils:locks(N3, Scope)),
  ?WAIT(not monitors(MT3, C2)),
  ?WAIT(proxies(N3) =:= []),
  ?assertEqual([{t, MT2, 2}], elock_test_utils:locks(N2, Scope)),
  ?WAIT(not monitors(MT2, C2)),
  ?assertEqual(#context{
    ref2lock = #{ RefX => #lock{ scope = Scope, term = x, nodes = #{ N1 => MX } } },
    locked = #{ {Scope, x, N1} => {MX, 1} }
  }, elock_test_utils:context(C2)),
  still_waiting(R1),

  ?assertEqual(ok, elock_test_utils:unlock(C2, RefX)),
  ?assertEqual(undefined, elock_test_utils:context(C2)),
  RefX1 = granted(R1),
  [ ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)) || Ref <- [RefX1, RefT2, RefY2] ],
  ?assertEqual(ok, elock_test_utils:unlock(C3, RefT3)),
  wait_idle(Nodes, Scope),
  stop([C1, C2, C3]).

%%=================================================================
%%  Failures
%%=================================================================
%%-----------------------------------------------------------------
%%  A fourth node's client holds t on [n1, n2] and another client of
%%  that node waits for it; the node is killed: the managers on n1
%%  and n2 drop both (their 'DOWN' comes as noconnection) and the
%%  nodes go idle, the scope sees the three nodes again and a client
%%  on n1 locks t on [n1, n2]
%%-----------------------------------------------------------------
client_node_dies_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2 | _] = Nodes = ?config(nodes, Config),
  N4 = distributed_tests_utils:start_node(#{name => n4}),
  _Holder4 = elock_test_utils:start_scope(N4, Scope),
  elock_test_utils:wait_ready(Scope, [N4 | Nodes]),
  C4 = elock_test_utils:client(N4),
  C5 = elock_test_utils:client(N4),

  {ok, _Ref4} = elock_test_utils:lock(C4, Scope, t, [N1, N2]),
  M1 = elock_test_utils:wait_manager(N1, Scope, t),
  M2 = elock_test_utils:wait_manager(N2, Scope, t),
  R5 = elock_test_utils:lock_queued([N1, N2], C5, Scope, t, [N1, N2], ?EXCLUSIVE),
  still_waiting(R5),
  ?assertEqual([{t, M1, 2}], elock_test_utils:locks(N1, Scope)),
  ?assertEqual([{t, M2, 2}], elock_test_utils:locks(N2, Scope)),

  ?assertEqual(ok, distributed_tests_utils:kill_node(N4)),
  wait_idle(Nodes, Scope),
  elock_test_utils:wait_ready(Scope, Nodes),

  C1 = elock_test_utils:client(N1),
  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [N1, N2]),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  wait_idle(Nodes, Scope),
  stop([C1]).

%%-----------------------------------------------------------------
%%  A client on n1 holds t on [n1, n4] and n4 is killed: the hold on
%%  n1 stays, the client's unlock is harmless (the unlock for n4
%%  goes nowhere), n1 goes idle, the context is cleaned, the
%%  survivors still see each other in the scope and a lock on
%%  [n1, n2] works
%%-----------------------------------------------------------------
manager_node_dies_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2 | _] = Nodes = ?config(nodes, Config),
  N4 = distributed_tests_utils:start_node(#{name => n4}),
  _Holder4 = elock_test_utils:start_scope(N4, Scope),
  elock_test_utils:wait_ready(Scope, [N4 | Nodes]),
  C1 = elock_test_utils:client(N1),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [N1, N4]),
  M1 = elock_test_utils:wait_manager(N1, Scope, t),
  M4 = elock_test_utils:wait_manager(N4, Scope, t),
  ?assertEqual(#context{
    ref2lock = #{ Ref1 => #lock{ scope = Scope, term = t, nodes = #{ N1 => M1, N4 => M4 } } },
    locked = #{ {Scope, t, N1} => {M1, 1}, {Scope, t, N4} => {M4, 1} }
  }, elock_test_utils:context(C1)),

  ?assertEqual(ok, distributed_tests_utils:kill_node(N4)),
  elock_test_utils:wait_ready(Scope, Nodes),
  ?assertEqual([{t, M1, 1}], elock_test_utils:locks(N1, Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  wait_idle(Nodes, Scope),

  {ok, Ref2} = elock_test_utils:lock(C1, Scope, t, [N1, N2]),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref2)),
  wait_idle(Nodes, Scope),
  stop([C1]).

%%-----------------------------------------------------------------
%%  A client on n2 waits for t held on n1 and is killed: the manager
%%  on n1 drops its request (no monitor, its proxy on n1 is gone),
%%  the holder's unlock leaves n1 idle
%%-----------------------------------------------------------------
remote_client_process_dies_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2 | _] = Nodes = ?config(nodes, Config),
  C1 = elock_test_utils:client(N1),
  C2 = elock_test_utils:client(N2),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [N1]),
  M1 = elock_test_utils:wait_manager(N1, Scope, t),
  R2 = elock_test_utils:lock_queued(N1, C2, Scope, t, [N1], ?EXCLUSIVE),
  still_waiting(R2),
  ?assert(monitors(M1, C2)),
  ?assertMatch([_Proxy], proxies(N1)),

  ?assertEqual(ok, elock_test_utils:stop(C2)),
  ?WAIT(not monitors(M1, C2)),
  ?WAIT(proxies(N1) =:= []),
  ?assertEqual([{t, M1, 2}], elock_test_utils:locks(N1, Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  wait_idle(Nodes, Scope),
  stop([C1]).

%%-----------------------------------------------------------------
%%  A client on n3 waits on [n1, n2] behind a holder and is killed:
%%  both managers drop its copies and kill its proxies (no monitor,
%%  no proxy left on either node), the holder's unlock leaves both
%%  nodes idle
%%-----------------------------------------------------------------
waiting_multi_node_client_dies_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2, N3] = Nodes = ?config(nodes, Config),
  C1 = elock_test_utils:client(N1),
  C3 = elock_test_utils:client(N3),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [N1, N2]),
  M1 = elock_test_utils:wait_manager(N1, Scope, t),
  M2 = elock_test_utils:wait_manager(N2, Scope, t),
  R3 = elock_test_utils:lock_queued([N1, N2], C3, Scope, t, [N1, N2], ?EXCLUSIVE),
  still_waiting(R3),
  ?assert(monitors(M1, C3) andalso monitors(M2, C3)),
  ?assertMatch([_Proxy], proxies(N1)),
  ?assertMatch([_Proxy], proxies(N2)),

  ?assertEqual(ok, elock_test_utils:stop(C3)),
  ?WAIT(not monitors(M1, C3) andalso not monitors(M2, C3)),
  ?WAIT(proxies(N1) =:= [] andalso proxies(N2) =:= []),
  ?assertEqual([{t, M1, 2}], elock_test_utils:locks(N1, Scope)),
  ?assertEqual([{t, M2, 2}], elock_test_utils:locks(N2, Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  wait_idle(Nodes, Scope),
  stop([C1]).

%%-----------------------------------------------------------------
%%  The workers of a multi node request (one per node, waiting in
%%  ecall_connection:call/4 on the client's node) go with the
%%  request: while the client on n3 waits on [n1, n2] there are two
%%  of them on n3; once the client is killed and the managers have
%%  dropped its copies, no worker is left waiting on n3
%%-----------------------------------------------------------------
dead_client_leaves_no_workers_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2, N3] = Nodes = ?config(nodes, Config),
  C1 = elock_test_utils:client(N1),
  C3 = elock_test_utils:client(N3),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [N1, N2]),
  M1 = elock_test_utils:wait_manager(N1, Scope, t),
  M2 = elock_test_utils:wait_manager(N2, Scope, t),
  R3 = elock_test_utils:lock_queued([N1, N2], C3, Scope, t, [N1, N2], ?EXCLUSIVE),
  still_waiting(R3),
  ?WAIT(length(workers(N3)) =:= 2),

  ?assertEqual(ok, elock_test_utils:stop(C3)),
  ?WAIT(not monitors(M1, C3) andalso not monitors(M2, C3)),
  ?WAIT(none_left(workers(N3))),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  wait_idle(Nodes, Scope),
  stop([C1]).

%%-----------------------------------------------------------------
%%  A node that does not exist among the nodes of a request: the
%%  request fails with {error, {badrpc, noconnection}}, the node
%%  granted meanwhile is released (n1 idle), no context
%%-----------------------------------------------------------------
unreachable_node_test(Config)->
  Scope = ?config(scope, Config),
  [N1 | _] = Nodes = ?config(nodes, Config),
  C1 = elock_test_utils:client(N1),

  ?assertEqual({error, {badrpc, noconnection}}, elock_test_utils:lock(C1, Scope, t, [N1, 'nonexistent@127.0.0.1'])),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  wait_idle(Nodes, Scope),
  stop([C1]).

%%-----------------------------------------------------------------
%%  A fourth node without the scope among the nodes of a request:
%%  the remote call crashes there (no table: badarg), the request
%%  fails with {error, {exit, badarg}}, n1 is released, no context,
%%  no manager on the fourth node
%%-----------------------------------------------------------------
node_without_scope_test(Config)->
  Scope = ?config(scope, Config),
  [N1 | _] = Nodes = ?config(nodes, Config),
  N4 = distributed_tests_utils:start_node(#{name => n4}),
  C1 = elock_test_utils:client(N1),

  ?assertEqual({error, {exit, badarg}}, elock_test_utils:lock(C1, Scope, t, [N1, N4])),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  wait_idle(Nodes, Scope),
  ?assertEqual([], elock_test_utils:managers(N4)),
  ?assertEqual([], ready_nodes(N4, Scope)),

  ?assertEqual(ok, distributed_tests_utils:stop_node(N4)),
  elock_test_utils:wait_ready(Scope, Nodes),
  stop([C1]).

%%=================================================================
%%  Scope
%%=================================================================
%%-----------------------------------------------------------------
%%  n3 is stopped gracefully: the others see [n1, n2]; a new node
%%  (under a new name) starts the scope: they see three again and a
%%  lock across all three works. The new node stays in the cluster
%%  in place of n3
%%-----------------------------------------------------------------
ready_nodes_after_restart_test(Config)->
  Scope = ?config(scope, Config),
  [N1, N2, N3] = ?config(nodes, Config),

  ?assertEqual(ok, distributed_tests_utils:stop_node(N3)),
  elock_test_utils:wait_ready(Scope, [N1, N2]),

  New = distributed_tests_utils:start_node(#{name => n3}),
  ?assertNotEqual(N3, New),
  ?assertEqual([N1, N2, New], distributed_tests_utils:nodes()),
  HolderNew = elock_test_utils:start_scope(New, Scope),
  Nodes = [N1, N2, New],
  elock_test_utils:wait_ready(Scope, Nodes),

  C1 = elock_test_utils:client(N1),
  {ok, Ref} = elock_test_utils:lock(C1, Scope, t, Nodes),
  #{ N1 := M1, N2 := M2, New := MNew } = managers_of(Nodes, Scope, t),
  ?assertEqual(New, node(MNew)),
  ?assertEqual(#context{
    ref2lock = #{ Ref => #lock{ scope = Scope, term = t, nodes = #{ N1 => M1, N2 => M2, New => MNew } } },
    locked = #{ {Scope, t, N1} => {M1, 1}, {Scope, t, N2} => {M2, 1}, {Scope, t, New} => {MNew, 1} }
  }, elock_test_utils:context(C1)),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)),
  wait_idle(Nodes, Scope),
  ?assertEqual(ok, elock_test_utils:finish_scope(HolderNew)),
  elock_test_utils:wait_ready(Scope, [N1, N2]),
  stop([C1]).

%%=================================================================
%%  Utilities
%%=================================================================
%%-----------------------------------------------------------------
%%  The cluster of the test cases: the first ?CLUSTER nodes of the
%%  harness. A node missing after a failed test case is replaced,
%%  the extra ones are stopped by end_per_testcase
%%-----------------------------------------------------------------
cluster()->
  Nodes = distributed_tests_utils:nodes(),
  Missing = [ distributed_tests_utils:start_node(#{name => replacement}) || _ <- lists:seq(1, ?CLUSTER - length(Nodes)) ],
  lists:sublist(Nodes ++ Missing, ?CLUSTER).

extra_nodes()->
  lists:nthtail(min(?CLUSTER, length(distributed_tests_utils:nodes())), distributed_tests_utils:nodes()).

% The scope on every node, ready on all of them: [{Node, Holder}]
start_scopes(Nodes, Scope)->
  Holders = [ {Node, elock_test_utils:start_scope(Node, Scope)} || Node <- Nodes ],
  elock_test_utils:wait_ready(Scope, Nodes),
  Holders.

holder(Node, Config)->
  proplists:get_value(Node, ?config(holders, Config)).

ready_nodes(Node, Scope)->
  rpc(Node, elock, ready_nodes, [Scope]).

stop(Clients)->
  [ elock_test_utils:stop(Client) || Client <- Clients ],
  ok.

wait_idle(Nodes, Scope)->
  [ elock_test_utils:wait_idle(Node, Scope) || Node <- Nodes ],
  ok.

% The managers of the term on the nodes: #{Node => Manager}
managers_of(Nodes, Scope, Term)->
  maps:from_list([ {Node, elock_test_utils:wait_manager(Node, Scope, Term)} || Node <- Nodes ]).

% Does the manager monitor the client, i.e. has it a request of it
monitors(Manager, Client)->
  case rpc(node(Manager), erlang, process_info, [Manager, monitors]) of
    {monitors, Monitors}->
      lists:member({process, Client}, Monitors);
    undefined->
      false
  end.

% The processes on the node waiting in elock_manager:lock/1: the
% proxies of the remote requests and the local clients that wait
proxies(Node)->
  rpc(Node, ?MODULE, processes_in, [{elock_manager, lock, 1}]).

% The processes on the node waiting in ecall_connection:call/4: the
% workers of the multi node requests, one per node of a request
workers(Node)->
  rpc(Node, ?MODULE, processes_in, [{ecall_connection, call, 4}]).

processes_in(MFA)->
  [ P || P <- erlang:processes(), process_info(P, current_function) =:= {current_function, MFA} ].

% No process is left, or the ones that are (for the error of a wait)
none_left([])->
  true;
none_left(Left)->
  {left, Left}.

% Where the client is: for the error of a request that never completed
stuck_in(Client)->
  rpc(node(Client), erlang, process_info, [Client, [current_function, current_stacktrace, message_queue_len]]).

rpc(Node, Module, Function, Args)->
  case rpc:call(Node, Module, Function, Args) of
    {badrpc, Reason}->
      erlang:error({badrpc, Node, Reason});
    Result->
      Result
  end.

% The client holds the term on both nodes under the ref: the context
% has exactly that, with the managers of the two nodes
holds_both(Client, Ref, Scope, N1, N2)->
  M1 = elock_test_utils:wait_manager(N1, Scope, t),
  M2 = elock_test_utils:wait_manager(N2, Scope, t),
  ?assertEqual(#context{
    ref2lock = #{ Ref => #lock{ scope = Scope, term = t, nodes = #{ N1 => M1, N2 => M2 } } },
    locked = #{ {Scope, t, N1} => {M1, 1}, {Scope, t, N2} => {M2, 1} }
  }, elock_test_utils:context(Client)).

% A scenario: every participant {Client, Held, Want} takes its Held
% locks, then the Want requests are issued in the order of the list,
% each one taken by the managers of its nodes before the next is
% issued. A lock is {Scope, Term, Nodes} (exclusive) or
% {Scope, Term, Nodes, Options}. The result is [{Client, Request, HeldRefs}]
setup(Participants)->
  Holding =
    [ {Client, [ take(Client, Lock) || Lock <- Held ], Want}
      || {Client, Held, Want} <- Participants ],
  [ {Client, ask(Client, Want), HeldRefs} || {Client, HeldRefs, Want} <- Holding ].

take(Client, {Scope, Term, Nodes})->
  take(Client, {Scope, Term, Nodes, #{}});
take(Client, {Scope, Term, Nodes, Options})->
  {ok, Ref} = elock_test_utils:lock(Client, Scope, Term, Nodes, Options),
  Ref.

ask(Client, {Scope, Term, Nodes})->
  ask(Client, {Scope, Term, Nodes, #{}});
ask(Client, {Scope, Term, Nodes, Options})->
  elock_test_utils:lock_queued(Nodes, Client, Scope, Term, Nodes, Options).

% The verdicts of the requests of a scenario, collected as they come:
% a loser ({error, deadlock}) releases what it holds, a winner
% ({ok, Ref}) releases the granted lock and what it holds - so the
% cycle resolves and the chain behind it drains. #{Client => Verdict}
resolve(Requests)->
  resolve(Requests, #{}).
resolve([], Verdicts)->
  Verdicts;
resolve(Requests, Verdicts)->
  {R, Verdict} = any_result([ Req || {_, Req, _} <- Requests ], ?DEADLINE),
  {value, {Client, R, Held}, Rest} = lists:keytake(R, 2, Requests),
  case Verdict of
    {ok, LockRef}->
      ?assertEqual(ok, elock_test_utils:unlock(Client, LockRef));
    {error, deadlock}->
      ok;
    Other->
      erlang:error({unexpected_verdict, Client, Other})
  end,
  [ ?assertEqual(ok, elock_test_utils:unlock(Client, Ref)) || Ref <- Held ],
  resolve(Rest, Verdicts#{ Client => Verdict }).

losers(Verdicts)->
  [ Client || {Client, {error, deadlock}} <- maps:to_list(Verdicts) ].

winners(Verdicts)->
  [ Client || {Client, {ok, _}} <- maps:to_list(Verdicts) ].

% The verdict of an asynchronous request: granted within the deadline
granted(R)->
  {ok, {ok, LockRef}} = elock_test_utils:result(R, ?DEADLINE),
  LockRef.

% The result of an asynchronous request within the deadline; a
% request that does not complete fails naming where the client is
verdict(Client, R)->
  case elock_test_utils:result(R, ?DEADLINE) of
    {ok, Result}->
      Result;
    timeout->
      erlang:error({no_verdict, Client, stuck_in(Client)})
  end.

% No verdict within the quiet window: the request is still waiting
still_waiting(R)->
  ?assertEqual(timeout, elock_test_utils:result(R, ?QUIET)).

% The first verdict among the requests: {Request, Verdict}. The other
% messages stay in the mailbox
any_result(Requests, Timeout)->
  Awaited = maps:from_keys(Requests, true),
  receive
    {R, Verdict} when is_map_key(R, Awaited)->
      {R, Verdict}
  after Timeout->
    erlang:error({any_result_timeout, Requests, elock_test_utils:flush()})
  end.

% An asynchronous request that measures its own time inside the
% client: the result is {Verdict, ElapsedMs}
timed_lock(Client, Scope, Term, Nodes, Options)->
  elock_test_utils:cast(Client, fun()->
    T0 = erlang:monotonic_time(millisecond),
    Verdict = elock:lock(Scope, Term, Nodes, Options),
    {Verdict, erlang:monotonic_time(millisecond) - T0}
  end).

% The elapsed time of a timed out request is within [Timeout, Timeout + 500]
elapsed_within(Elapsed, Timeout)->
  ?assertMatch(E when E >= Timeout andalso E =< Timeout + 500, Elapsed).
