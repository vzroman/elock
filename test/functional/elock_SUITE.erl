%%=================================================================
%%  Module tests of elock, elock_context and elock_scope: the client
%%  API and the logic behind it - the context of a client process,
%%  the request a client sends to a manager and the client side of a
%%  remote or a multi node request (wait_verdict/2), the scope.
%%
%%  Techniques:
%%  * fake manager: the test process poses as the manager of a term
%%    by inserting {Term, self(), 1} into the scope table. A client
%%    then takes ticket 2 and sends its #request{} to the test
%%    process, which answers with any verdict or with #queued{} as
%%    the manager does: ?reply(Tag, Message) with the tag of the
%%    request it got
%%  * fake nodes: a #waiting{} built by hand, the pending workers
%%    are spawned processes monitored with the request ref as the
%%    tag, like the ones of run_request/6, that report a crafted
%%    result, the managers are collectors that forward what they get
%%=================================================================
-module(elock_SUITE).

-include("elock.hrl").
-include("elock_test.hrl").

-define(WINNER, {winner_scope, winner_term, 'winner@node'}).

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

%% Test cases
-export([
  context_birth_test/1,
  wait_verdict_deadlock_verdict_test/1,
  wait_verdict_spawns_graph_test/1,
  wait_verdict_queued_after_result_test/1,
  validate_nodes_test/1,
  validate_options_test/1,
  legacy_lock_test/1,
  legacy_empty_nodes_test/1,
  legacy_timeout_test/1,
  legacy_deadlock_test/1,
  context_after_lock_test/1,
  reentrant_context_count_test/1,
  multi_scope_context_test/1,
  multi_node_context_test/1,
  unlock_dead_managers_test/1,
  add_lock_remove_lock_test/1,
  unlock_unknown_ref_test/1,
  unlock_twice_test/1,
  unlock_by_another_process_test/1,
  request_fields_test/1,
  verdicts_test/1,
  queued_publishes_holds_test/1,
  fake_manager_dies_test/1,
  wait_verdict_all_granted_test/1,
  wait_verdict_queued_then_granted_test/1,
  wait_verdict_granted_then_queued_test/1,
  wait_verdict_foreign_queued_left_in_mailbox_test/1,
  wait_verdict_failure_test/1,
  wait_verdict_failure_late_results_test/1,
  wait_verdict_failure_reasons_test/1,
  wait_verdict_requeued_test/1,
  receive_marker_test/1,
  single_remote_node_unreachable_test/1,
  unreachable_node_releases_granted_node_test/1,
  start_link_test/1,
  start_twice_test/1,
  ready_nodes_unknown_scope_test/1,
  lock_on_stopped_scope_test/1,
  many_scopes_test/1
]).

% mirrors elock_context.erl
-record(context,{
  ref2lock,
  locked,
  counts,
  birth
}).
-record(node,{manager, holder}).
-record(lock,{
  scope,
  term,
  nodes
}).
-record(waiting,{
  ref,
  scope,
  term,
  pending,
  nodes,
  queued,
  holds,
  birth,
  age,
  graph,
  graph_monitor
}).

% mirrors elock_manager.erl
-record(locked,{
  ref
}).
-record(timeout,{
  ref
}).
-record(retry,{
  ref
}).
-record(withdrawn,{ref}).
-define(reply(Tag, Message), {Tag, Message}).

-define(CONTEXT, '$elock_context$').
-define(PG_SCOPE(Scope), list_to_atom(atom_to_list(Scope) ++ "_$pg$")).

% The test cases that manage their scopes themselves
-define(NO_SCOPE_TESTS, [
  start_link_test,
  ready_nodes_unknown_scope_test,
  many_scopes_test
]).

all()->
  [
    {group, validation},
    {group, compatibility},
    {group, context},
    {group, request},
    {group, multi_node_client},
    {group, scope}
  ].

groups()->
  [
    {validation, [], [
      validate_nodes_test,
      validate_options_test
    ]},
    {compatibility, [], [
      legacy_lock_test,
      legacy_empty_nodes_test,
      legacy_timeout_test,
      legacy_deadlock_test
    ]},
    {context, [], [
      context_birth_test,
      context_after_lock_test,
      reentrant_context_count_test,
      multi_scope_context_test,
      multi_node_context_test,
      unlock_dead_managers_test,
      add_lock_remove_lock_test,
      unlock_unknown_ref_test,
      unlock_twice_test,
      unlock_by_another_process_test
    ]},
    {request, [], [
      request_fields_test,
      verdicts_test,
      queued_publishes_holds_test,
      fake_manager_dies_test
    ]},
    {multi_node_client, [], [
      wait_verdict_deadlock_verdict_test,
      wait_verdict_spawns_graph_test,
      wait_verdict_queued_after_result_test,
      wait_verdict_all_granted_test,
      wait_verdict_queued_then_granted_test,
      wait_verdict_granted_then_queued_test,
      wait_verdict_foreign_queued_left_in_mailbox_test,
      wait_verdict_failure_test,
      wait_verdict_failure_late_results_test,
      wait_verdict_failure_reasons_test,
      wait_verdict_requeued_test,
      receive_marker_test,
      single_remote_node_unreachable_test,
      unreachable_node_releases_granted_node_test
    ]},
    {scope, [], [
      start_link_test,
      start_twice_test,
      ready_nodes_unknown_scope_test,
      lock_on_stopped_scope_test,
      many_scopes_test
    ]}
  ].

suite()->
  [{timetrap, {minutes, 10}}].

init_per_suite(Config)->
  {ok, _} = application:ensure_all_started(elock),
  Config.

end_per_suite(_Config)->
  ok.

init_per_group(_Group, Config)->
  Config.

end_per_group(_Group, _Config)->
  ok.

%%-----------------------------------------------------------------
%%  Every test case gets its own scope named after it
%%-----------------------------------------------------------------
init_per_testcase(TestCase, Config)->
  case lists:member(TestCase, ?NO_SCOPE_TESTS) of
    true->
      Config;
    false->
      Holder = elock_test_utils:start_scope(TestCase),
      [{scope, TestCase}, {holder, Holder} | Config]
  end.

%%-----------------------------------------------------------------
%%  The scope must be idle (no entry, no manager) and every client
%%  and collector is stopped. A leak fails the test case
%%-----------------------------------------------------------------
end_per_testcase(_TestCase, Config)->
  elock_test_utils:stop_clients(),
  elock_test_utils:stop_collectors(),
  case ?config(holder, Config) of
    undefined->
      Left = ?safe(elock_test_utils:wait_until(
        fun()-> elock_test_utils:managers() =:= [] end, ?DEADLINE
      )),
      elock_test_utils:kill_managers(),
      case Left of
        ok-> ok;
        Error-> {fail, {managers_left, Error}}
      end;
    Holder->
      elock_test_utils:finish_scope(Holder)
  end.

%%=================================================================
%%  Validation
%%=================================================================
%%-----------------------------------------------------------------
%%  Nodes: not a list or an empty list -> {invalid_nodes, Nodes}, a
%%  node that is not an atom -> {invalid_node, N}. The API throws
%%  the same before anything is requested: the scope stays idle
%%-----------------------------------------------------------------
validate_nodes_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),

  ?assertEqual(ok, elock_context:validate_nodes([Node])),
  ?assertEqual(ok, elock_context:validate_nodes([n1, n2, n1])),

  ?assertThrow({invalid_nodes, []}, elock_context:validate_nodes([])),
  ?assertThrow({invalid_nodes, Node}, elock_context:validate_nodes(Node)),
  ?assertThrow({invalid_nodes, {Node}}, elock_context:validate_nodes({Node})),
  ?assertThrow({invalid_nodes, #{}}, elock_context:validate_nodes(#{})),
  ?assertThrow({invalid_nodes, <<"n1">>}, elock_context:validate_nodes(<<"n1">>)),
  ?assertThrow({invalid_nodes, undefined}, elock_context:validate_nodes(undefined)),

  ?assertThrow({invalid_node, "n1"}, elock_context:validate_nodes(["n1"])),
  ?assertThrow({invalid_node, 1}, elock_context:validate_nodes([Node, 1])),
  ?assertThrow({invalid_node, {Node}}, elock_context:validate_nodes([{Node}])),
  ?assertThrow({invalid_node, <<"n1">>}, elock_context:validate_nodes([n1, <<"n1">>, n2])),

  % through the API
  ?assertThrow({invalid_nodes, []}, elock:lock(Scope, t, [])),
  ?assertThrow({invalid_nodes, Node}, elock:lock(Scope, t, Node)),
  ?assertThrow({invalid_nodes, []}, elock:lock(Scope, t, [], #{})),
  ?assertThrow({invalid_node, 1}, elock:lock(Scope, t, [Node, 1])),
  ?assertThrow({invalid_node, "n1"}, elock:lock(Scope, t, ["n1"], #{is_shared => true})),

  ?assertEqual([], elock_test_utils:locks(Scope)),
  ?assertEqual(undefined, get(?CONTEXT)).

%%-----------------------------------------------------------------
%%  Options: the defaults, every invalid value, a non-map, and an
%%  unknown key that throws {invalid_option, Key}
%%-----------------------------------------------------------------
validate_options_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),

  ?assertEqual(#{is_shared => false, timeout => undefined}, elock_context:validate_options(#{})),
  ?assertEqual(#{is_shared => true, timeout => undefined}, elock_context:validate_options(#{is_shared => true})),
  ?assertEqual(#{is_shared => false, timeout => 1}, elock_context:validate_options(#{timeout => 1})),
  ?assertEqual(#{is_shared => false, timeout => undefined}, elock_context:validate_options(#{timeout => undefined})),
  ?assertEqual(#{is_shared => true, timeout => 5000}, elock_context:validate_options(#{is_shared => true, timeout => 5000})),

  ?assertThrow({invalid_options, []}, elock_context:validate_options([])),
  ?assertThrow({invalid_options, [{timeout, 1}]}, elock_context:validate_options([{timeout, 1}])),
  ?assertThrow({invalid_options, undefined}, elock_context:validate_options(undefined)),
  ?assertThrow({invalid_options, {is_shared, true}}, elock_context:validate_options({is_shared, true})),

  ?assertThrow({invalid_is_shared, yes}, elock_context:validate_options(#{is_shared => yes})),
  ?assertThrow({invalid_is_shared, 1}, elock_context:validate_options(#{is_shared => 1})),
  ?assertThrow({invalid_is_shared, undefined}, elock_context:validate_options(#{is_shared => undefined})),
  ?assertThrow({invalid_is_shared, "true"}, elock_context:validate_options(#{is_shared => "true"})),

  ?assertThrow({invalid_timeout, infinity}, elock_context:validate_options(#{timeout => infinity})),
  ?assertThrow({invalid_timeout, 0}, elock_context:validate_options(#{timeout => 0})),
  ?assertThrow({invalid_timeout, -1}, elock_context:validate_options(#{timeout => -1})),
  ?assertThrow({invalid_timeout, 1.5}, elock_context:validate_options(#{timeout => 1.5})),
  ?assertThrow({invalid_timeout, "100"}, elock_context:validate_options(#{timeout => "100"})),
  ?assertThrow({invalid_timeout, {100, ms}}, elock_context:validate_options(#{timeout => {100, ms}})),
  ?assertThrow({invalid_timeout, 0}, elock_context:validate_options(#{is_shared => true, timeout => 0})),

  ?assertThrow({invalid_option, unknown}, elock_context:validate_options(#{unknown => 1})),
  ?assertThrow({invalid_option, shared},
    elock_context:validate_options(#{is_shared => true, timeout => 10, shared => true})),

  % through the API
  ?assertThrow({invalid_options, []}, elock:lock(Scope, t, [Node], [])),
  ?assertThrow({invalid_is_shared, yes}, elock:lock(Scope, t, [Node], #{is_shared => yes})),
  ?assertThrow({invalid_timeout, 0}, elock:lock(Scope, t, [Node], #{timeout => 0})),
  ?assertThrow({invalid_timeout, infinity}, elock:lock(Scope, t, [Node], #{timeout => infinity})),
  ?assertThrow({invalid_option, foo},
    elock:lock(Scope, t, [Node], #{is_shared => false, foo => bar})),

  ?assertEqual([], elock_test_utils:locks(Scope)),
  ?assertEqual(undefined, get(?CONTEXT)).

%%=================================================================
%%  Compatibility API
%%=================================================================
%% The old arities share locks with the current API and return callbacks
%% that release them and clean up the owning process's context.
legacy_lock_test(Config)->
  Scope = ?config(scope, Config),
  Nodes = [node()],
  C1 = elock_test_utils:client(),
  C2 = elock_test_utils:client(),

  {ok, Unlock1} = elock_test_utils:call(C1,
    fun()-> elock:lock(Scope, t, true, infinity) end),
  {ok, Unlock2} = elock_test_utils:call(C2,
    fun()-> elock:lock(Scope, t, true, 1000, Nodes) end),
  ?assert(is_function(Unlock1, 0)),
  ?assert(is_function(Unlock2, 0)),
  ?assertEqual({error, timeout}, elock:lock(Scope, t, Nodes, #{timeout => 30})),

  {ok, Ref} = elock_test_utils:lock(C1, Scope, other, Nodes),
  ?assert(is_reference(Ref)),
  ?assertEqual(ok, elock_test_utils:call(C1, Unlock1)),
  ?assertEqual(ok, elock_test_utils:call(C1, Unlock1)),
  ?assertEqual({error, timeout}, elock:lock(Scope, t, Nodes, #{timeout => 30})),
  ?assertEqual(ok, elock_test_utils:call(C2, Unlock2)),
  ?assertEqual(undefined, elock_test_utils:context(C2)),

  {ok, ExclusiveRef} = elock:lock(Scope, t, Nodes),
  ?assertEqual(ok, elock:unlock(ExclusiveRef)),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  elock_test_utils:wait_idle(Scope).

%% An empty node list succeeds without a scope or a context, and its
%% callback leaves any other locks intact.
legacy_empty_nodes_test(Config)->
  Scope = ?config(scope, Config),
  {ok, EmptyUnlock} = elock:lock(unstarted_scope, t, false, infinity, []),
  ?assert(is_function(EmptyUnlock, 0)),
  ?assertEqual(undefined, get(?CONTEXT)),
  {ok, Ref} = elock:lock(Scope, t, [node()]),
  Context = get(?CONTEXT),
  {ok, ZeroUnlock} = elock:lock(Scope, t, false, 0, []),
  ?assertEqual(ok, EmptyUnlock()),
  ?assertEqual(ok, ZeroUnlock()),
  ?assertEqual(Context, get(?CONTEXT)),
  ?assertEqual(ok, elock:unlock(Ref)),
  elock_test_utils:wait_idle(Scope).

%% Finite and zero timeouts keep the previous error shape and leave
%% no locks behind for the unsuccessful caller.
legacy_timeout_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:client(),
  {ok, Unlock} = elock_test_utils:call(C1,
    fun()-> elock:lock(Scope, t, false, infinity, [node()]) end),
  ?assertEqual({error, timeout}, elock:lock(Scope, t, false, 30)),
  ?assertEqual({error, timeout}, elock:lock(Scope, t, true, 30, [node()])),
  ?assertEqual({error, timeout}, elock:lock(Scope, t, false, 0)),
  ?assertEqual({error, timeout}, elock:lock(Scope, other, false, 0, [node()])),
  ?assertEqual(undefined, get(?CONTEXT)),
  ?assertEqual(ok, elock_test_utils:call(C1, Unlock)),
  elock_test_utils:wait_idle(Scope).

%% The manager's richer deadlock verdict is reduced to the old atom
%% through both legacy arities. Neither failure creates a context.
legacy_deadlock_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:client(),
  true = ets:insert(Scope, {t, self(), 1}),
  Calls = [
    fun()-> elock:lock(Scope, t, false, infinity) end,
    fun()-> elock:lock(Scope, t, true, 1000, [node()]) end
  ],
  lists:foreach(fun({Call, Winner})->
    Pending = elock_test_utils:cast(C1, Call),
    #request{ref = Ref, tag = Tag} = ?RECEIVE(#request{}),
    C1 ! ?reply(Tag, #deadlock{ref = Ref, winner = Winner}),
    ?assertEqual({ok, {error, deadlock}}, elock_test_utils:result(Pending, ?DEADLINE)),
    ?assertEqual(undefined, elock_test_utils:context(C1))
  end, [{Call, ?WINNER} || Call <- Calls] ++
    [{hd(Calls), probe_limit}]),
  true = ets:delete(Scope, t).

%%=================================================================
%%  Context
%%=================================================================
%% Birth belongs to a context across scopes, reentries and partial release.
context_birth_test(Config)->
  Scope = ?config(scope, Config),
  Other = context_birth_other,
  Holder = elock_test_utils:start_scope(Other),
  C1 = elock_test_utils:client(),
  try
    ?assertEqual(undefined, elock_test_utils:context_birth(C1)),
    {ok, R1} = elock_test_utils:lock(C1, Scope, t, [node()]),
    Birth = elock_test_utils:context_birth(C1),
    {ok, R2} = elock_test_utils:lock(C1, Scope, t, [node()]),
    {ok, R3} = elock_test_utils:lock(C1, Other, t, [node()]),
    ?assertEqual(Birth, elock_test_utils:context_birth(C1)),
    [ begin
      ok = elock_test_utils:unlock(C1, Ref),
      ?assertEqual(Birth, elock_test_utils:context_birth(C1))
    end || Ref <- [R1, R2] ],
    ok = elock_test_utils:unlock(C1, R3),
    ?assertEqual(undefined, elock_test_utils:context_birth(C1)),
    {ok, R4} = elock_test_utils:lock(C1, Scope, t, [node()]),
    ?assert(elock_test_utils:context_birth(C1) > Birth),
    ok = elock_test_utils:unlock(C1, R4)
  after
    elock_test_utils:stop(C1),
    ok = elock_test_utils:finish_scope(Holder)
  end.

%%-----------------------------------------------------------------
%%  The exact context after one lock: the ref maps to the lock with
%%  the manager from the table, the held map names the manager of
%%  the key, nothing is counted for a key held once. Erased after
%%  the unlock, the scope is idle
%%-----------------------------------------------------------------
context_after_lock_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  C1 = elock_test_utils:client(),

  ?assertEqual(undefined, elock_test_utils:context(C1)),

  {ok, Ref} = elock_test_utils:lock(C1, Scope, t1, [Node]),
  ?assert(is_reference(Ref)),
  Manager = elock_test_utils:wait_manager(Scope, t1),
  ?assertEqual([{t1, Manager, 1}], elock_test_utils:locks(Scope)),

  ?assertEqual(#context{
    birth = elock_test_utils:context_birth(C1),
    ref2lock = #{
      Ref => #lock{
        scope = Scope,
        term = t1,
        nodes = #{ Node => #node{manager = Manager} }
      }
    },
    locked = #{
      {Scope, t1, Node} => Manager
    },
    counts = #{}
  }, elock_test_utils:context(C1)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  elock_test_utils:wait_idle(Scope),
  elock_test_utils:stop(C1).

%%-----------------------------------------------------------------
%%  The same term twice: two refs, one key in the held map and the
%%  count 2 for it aside. The first unlock drops the count, the
%%  held map is the same throughout, the manager and the entry
%%  stay; the second erases the context and the scope is idle
%%-----------------------------------------------------------------
reentrant_context_count_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  C1 = elock_test_utils:client(),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t1, [Node]),
  Manager = elock_test_utils:wait_manager(Scope, t1),
  Locked = #{ {Scope, t1, Node} => Manager },
  {ok, Ref2} = elock_test_utils:lock(C1, Scope, t1, [Node]),
  ?assertNotEqual(Ref1, Ref2),

  Lock = #lock{
    scope = Scope,
    term = t1,
    nodes = #{ Node => #node{manager = Manager} }
  },
  ?assertEqual(#context{
    birth = elock_test_utils:context_birth(C1),
    ref2lock = #{ Ref1 => Lock, Ref2 => Lock#lock{nodes = #{Node => #node{manager = Manager}}} },
    locked = Locked,
    counts = #{ {Scope, t1, Node} => 2 }
  }, elock_test_utils:context(C1)),
  % the key is held once as the managers see it
  ?assertEqual(1, map_size(elock_context:held_locks(elock_test_utils:context(C1)))),
  ?assertEqual([{t1, Manager, 2}], elock_test_utils:locks(Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual(#context{
    birth = elock_test_utils:context_birth(C1),
    ref2lock = #{ Ref2 => Lock#lock{nodes = #{Node => #node{manager = Manager}}} },
    locked = Locked,
    counts = #{}
  }, elock_test_utils:context(C1)),
  ?assertEqual([{t1, Manager, 2}], elock_test_utils:locks(Scope)),
  ?assert(is_process_alive(Manager)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref2)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  elock_test_utils:wait_idle(Scope),
  elock_test_utils:wait_dead(Manager),
  elock_test_utils:stop(C1).

%%-----------------------------------------------------------------
%%  The same term in two scopes gives two keys and managers. held_locks/1
%%  returns the context map; map_size/1 counts its distinct keys.
%%-----------------------------------------------------------------
multi_scope_context_test(Config)->
  Scope1 = ?config(scope, Config),
  Scope2 = list_to_atom(atom_to_list(Scope1) ++ "_second"),
  Holder2 = elock_test_utils:start_scope(Scope2),
  Node = node(),
  C1 = elock_test_utils:client(),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope1, t, [Node]),
  {ok, Ref2} = elock_test_utils:lock(C1, Scope2, t, [Node]),
  M1 = elock_test_utils:wait_manager(Scope1, t),
  M2 = elock_test_utils:wait_manager(Scope2, t),
  ?assertNotEqual(M1, M2),

  Context = elock_test_utils:context(C1),
  ?assertEqual(#context{
    birth = elock_test_utils:context_birth(C1),
    ref2lock = #{
      Ref1 => #lock{ scope = Scope1, term = t, nodes = #{ Node => #node{manager = M1} } },
      Ref2 => #lock{ scope = Scope2, term = t, nodes = #{Node => #node{manager = M2}} }
    },
    locked = #{
      {Scope1, t, Node} => M1,
      {Scope2, t, Node} => M2
    },
    counts = #{}
  }, Context),

  ?assertEqual(#{
    {Scope1, t, Node} => M1,
    {Scope2, t, Node} => M2
  }, elock_context:held_locks(Context)),
  ?assertEqual(2, map_size(elock_context:held_locks(Context))),
  ?assertEqual(#{}, elock_context:held_locks(undefined)),
  ?assertEqual(0, map_size(elock_context:held_locks(undefined))),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual(#{ {Scope2, t, Node} => M2 }, elock_context:held_locks(elock_test_utils:context(C1))),
  ?assertEqual(1, map_size(elock_context:held_locks(elock_test_utils:context(C1)))),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref2)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),

  elock_test_utils:wait_idle(Scope1),
  elock_test_utils:wait_idle(Scope2),
  elock_test_utils:stop(C1),
  elock_test_utils:stop_scope(Holder2).

%%-----------------------------------------------------------------
%%  The context of a multi node lock (locked/3 with the granted
%%  nodes, the managers are collectors): one key per node, the
%%  held map names every manager, and unlock/1 sends #unlock{} to
%%  the manager of every node and erases the context
%%-----------------------------------------------------------------
multi_node_context_test(Config)->
  Scope = ?config(scope, Config),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  Ref = make_ref(),
  Now = erlang:system_time(microsecond),
  ?assertEqual(undefined, get(?CONTEXT)),

  elock_context:locked(multi_node_request(Ref, Scope, ['n1@host', 'n2@host']), #{
    'n1@host' => #node{manager = M1},
    'n2@host' => #node{manager = M2}
  }, elock_context:init_context(Now)),

  ?assertEqual(Now, elock_test_utils:context_birth(self())),
  ?assertEqual(#context{
    birth = Now,
    ref2lock = #{
      Ref => #lock{
        scope = Scope,
        term = t,
        nodes = #{ 'n1@host' => #node{manager = M1}, 'n2@host' => #node{manager = M2} }
      }
    },
    locked = #{
      {Scope, t, 'n1@host'} => M1,
      {Scope, t, 'n2@host'} => M2
    },
    counts = #{}
  }, get(?CONTEXT)),
  ?assertEqual(#{
    {Scope, t, 'n1@host'} => M1,
    {Scope, t, 'n2@host'} => M2
  }, elock_context:held_locks(get(?CONTEXT))),
  ?assertEqual(2, map_size(elock_context:held_locks(get(?CONTEXT)))),

  ?assertEqual(ok, elock:unlock(Ref)),
  ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M1, 1)),
  ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M2, 1)),
  ?assertEqual(undefined, get(?CONTEXT)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  unlock/1 when the managers of the lock are dead is harmless
%%  (sending to a dead pid is safe): ok, the context is erased, a surviving
%%  manager still gets its #unlock{}. Multi node through locked/3
%%  with collectors as managers (one dead, then both), and single
%%  node with a real manager killed under the client: the client's
%%  other lock stays, remove_lock/2 runs with the same, dead, pid
%%-----------------------------------------------------------------
unlock_dead_managers_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  Nodes = ['n1@host', 'n2@host'],

  % one of the two managers is dead
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  Ref1 = make_ref(),
  Now1 = erlang:system_time(microsecond),
  elock_context:locked(multi_node_request(Ref1, Scope, Nodes), #{
    'n1@host' => #node{manager = M1},
    'n2@host' => #node{manager = M2}
  }, elock_context:init_context(Now1)),
  ?assertEqual(Now1, elock_test_utils:context_birth(self())),
  ?assertEqual(ok, elock_test_utils:stop(M1)),
  ?assertEqual(ok, elock:unlock(Ref1)),
  ?assertEqual(undefined, get(?CONTEXT)),
  ?assertEqual([#unlock{ref = Ref1}], elock_test_utils:collected(M2, 1)),
  ?NO_MESSAGE,

  % both are dead
  M3 = elock_test_utils:collector(),
  M4 = elock_test_utils:collector(),
  Ref2 = make_ref(),
  Now2 = erlang:system_time(microsecond),
  elock_context:locked(multi_node_request(Ref2, Scope, Nodes), #{
    'n1@host' => #node{manager = M3},
    'n2@host' => #node{manager = M4}
  }, elock_context:init_context(Now2)),
  ?assertEqual(Now2, elock_test_utils:context_birth(self())),
  ?assertEqual(ok, elock_test_utils:stop(M3)),
  ?assertEqual(ok, elock_test_utils:stop(M4)),
  ?assertEqual(ok, elock:unlock(Ref2)),
  ?assertEqual(undefined, get(?CONTEXT)),
  ?NO_MESSAGE,

  % a real manager killed under the client that holds another term as well
  C1 = elock_test_utils:client(),
  {ok, RefT1} = elock_test_utils:lock(C1, Scope, t1, [Node]),
  {ok, RefT2} = elock_test_utils:lock(C1, Scope, t2, [Node]),
  ManagerT1 = elock_test_utils:wait_manager(Scope, t1),
  ManagerT2 = elock_test_utils:wait_manager(Scope, t2),
  exit(ManagerT1, kill),
  elock_test_utils:wait_dead(ManagerT1),

  ?assertEqual(ok, elock_test_utils:unlock(C1, RefT1)),
  ?assertEqual(#context{
    birth = elock_test_utils:context_birth(C1),
    ref2lock = #{ RefT2 => #lock{
      scope = Scope,
      term = t2,
      nodes = #{Node => #node{manager = ManagerT2}}
    } },
    locked = #{ {Scope, t2, Node} => ManagerT2 },
    counts = #{}
  }, elock_test_utils:context(C1)),
  ?assertEqual(ok, elock_test_utils:unlock(C1, RefT2)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  ?assert(is_process_alive(C1)),

  % the entry of a killed manager stays behind (the next client of the
  % term deletes it on its 'DOWN'), dropped here to leave the scope idle
  ?assertEqual([{t1, ManagerT1, 1}], elock_test_utils:locks(Scope)),
  true = ets:delete(Scope, t1),
  elock_test_utils:wait_idle(Scope),
  elock_test_utils:stop(C1).

%%-----------------------------------------------------------------
%%  add_lock/2 and remove_lock/2 on hand-built pairs {Locked, Counts}.
%%  Locked is the held map, Key => Manager, Counts has a key only
%%  while it is held by two or more requests. The first hold adds
%%  the key to Locked alone, a re-entry counts from 2 up, a
%%  different manager for a known key is stale: the pid is replaced
%%  and the count dropped; a multi node lock goes node by node. The
%%  removal counts down, drops the count at 2 and the key when it
%%  is held once; a stale manager keeps everything, an unknown key
%%  is a no-op
%%-----------------------------------------------------------------
add_lock_remove_lock_test(_Config)->
  Node = node(),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  Ma = elock_test_utils:collector(),
  Mb = elock_test_utils:collector(),
  Key = {s, t, Node},
  Lock = #lock{ scope = s, term = t, nodes = #{ Node => #node{manager = M1} } },
  StaleLock = #lock{ scope = s, term = t, nodes = #{ Node => #node{manager = M2} } },
  OtherLock = #lock{ scope = s, term = other, nodes = #{ Node => #node{manager = M1} } },
  MultiLock = #lock{
    scope = s,
    term = t2,
    nodes = #{ n1 => #node{manager = Ma}, n2 => #node{manager = Mb} }
  },
  N1Lock = #lock{ scope = s, term = t2, nodes = #{ n1 => #node{manager = Ma} } },

  % the first hold: the key is in the held map, nothing is counted
  Held1 = elock_context:add_lock(Lock, {#{}, #{}}),
  ?assertEqual({#{ Key => M1 }, #{}}, Held1),
  % a re-entry from 1 and from 2: the held map is the same
  Held2 = elock_context:add_lock(Lock, Held1),
  ?assertEqual({#{ Key => M1 }, #{ Key => 2 }}, Held2),
  Held3 = elock_context:add_lock(Lock, Held2),
  ?assertEqual({#{ Key => M1 }, #{ Key => 3 }}, Held3),

  % a stale manager: the pid is replaced, the count is dropped
  ?assertEqual({#{ Key => M2 }, #{}}, elock_context:add_lock(StaleLock, Held3)),
  ?assertEqual({#{ Key => M2 }, #{}}, elock_context:add_lock(StaleLock, Held2)),
  ?assertEqual({#{ Key => M2 }, #{}}, elock_context:add_lock(StaleLock, Held1)),

  % one key per node, the other entries and their counts kept
  ?assertEqual({
    #{
      Key => M1,
      {s, t2, n1} => Ma,
      {s, t2, n2} => Mb
    },
    #{ Key => 2 }
  }, elock_context:add_lock(MultiLock, Held2)),
  % a multi node lock over a node that is held already: that node is
  % a re-entry, the other one the first hold
  Multi1 = elock_context:add_lock(MultiLock, elock_context:add_lock(N1Lock, {#{}, #{}})),
  ?assertEqual({
    #{
      {s, t2, n1} => Ma,
      {s, t2, n2} => Mb
    },
    #{ {s, t2, n1} => 2 }
  }, Multi1),

  % the removal from 3, from 2 (the count is dropped) and of a key
  % held once (the key is dropped)
  ?assertEqual(Held2, elock_context:remove_lock(Lock, Held3)),
  ?assertEqual(Held1, elock_context:remove_lock(Lock, Held2)),
  ?assertEqual({#{}, #{}}, elock_context:remove_lock(Lock, Held1)),

  % a stale manager keeps the entry and its count, an unknown key is a no-op
  ?assertEqual(Held3, elock_context:remove_lock(StaleLock, Held3)),
  ?assertEqual(Held2, elock_context:remove_lock(StaleLock, Held2)),
  ?assertEqual(Held1, elock_context:remove_lock(StaleLock, Held1)),
  ?assertEqual(Held2, elock_context:remove_lock(OtherLock, Held2)),
  ?assertEqual(Held1, elock_context:remove_lock(OtherLock, Held1)),
  ?assertEqual({#{}, #{}}, elock_context:remove_lock(Lock, {#{}, #{}})),

  % every node of a multi node lock is removed, each by its own count
  ?assertEqual(Held2, elock_context:remove_lock(MultiLock, elock_context:add_lock(MultiLock, Held2))),
  ?assertEqual({#{ {s, t2, n1} => Ma }, #{}}, elock_context:remove_lock(MultiLock, Multi1)),
  ?assertEqual({#{ {s, t2, n2} => Mb }, #{}}, elock_context:remove_lock(N1Lock, elock_context:remove_lock(N1Lock, Multi1))),

  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  unlock/1 of an unknown ref: without a context -> ok and still no
%%  context; with a context -> ok and the context unchanged, the
%%  lock is still held
%%-----------------------------------------------------------------
unlock_unknown_ref_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),

  ?assertEqual(undefined, get(?CONTEXT)),
  ?assertEqual(ok, elock:unlock(make_ref())),
  ?assertEqual(undefined, get(?CONTEXT)),

  C1 = elock_test_utils:client(),
  {ok, Ref} = elock_test_utils:lock(C1, Scope, t1, [Node]),
  Manager = elock_test_utils:wait_manager(Scope, t1),
  Context = elock_test_utils:context(C1),

  ?assertEqual(ok, elock_test_utils:unlock(C1, make_ref())),
  ?assertEqual(Context, elock_test_utils:context(C1)),
  ?assertEqual([{t1, Manager, 1}], elock_test_utils:locks(Scope)),
  ?assert(is_process_alive(Manager)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  elock_test_utils:wait_idle(Scope),
  elock_test_utils:stop(C1).

%%-----------------------------------------------------------------
%%  The second unlock of a ref is a no-op: ok, no context, the
%%  manager has exited normally, the scope is idle
%%-----------------------------------------------------------------
unlock_twice_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  C1 = elock_test_utils:client(),

  {ok, Ref} = elock_test_utils:lock(C1, Scope, t1, [Node]),
  Manager = elock_test_utils:wait_manager(Scope, t1),
  MonRef = erlang:monitor(process, Manager),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  ?assertEqual({'DOWN', MonRef, process, Manager, normal}, ?RECEIVE({'DOWN', MonRef, process, Manager, _})),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  elock_test_utils:wait_idle(Scope),

  % the lock is free for a new round
  {ok, Ref2} = elock_test_utils:lock(C1, Scope, t1, [Node]),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref2)),
  elock_test_utils:wait_idle(Scope),
  elock_test_utils:stop(C1).

%%-----------------------------------------------------------------
%%  A foreign process calling unlock with the ref of C1 does
%%  nothing: C1 still holds, its context is unchanged, the entry
%%  and the manager stay
%%-----------------------------------------------------------------
unlock_by_another_process_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  C1 = elock_test_utils:client(),
  C2 = elock_test_utils:client(),

  {ok, Ref} = elock_test_utils:lock(C1, Scope, t1, [Node]),
  Manager = elock_test_utils:wait_manager(Scope, t1),
  Context = elock_test_utils:context(C1),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref)),
  ?assertEqual(undefined, elock_test_utils:context(C2)),
  ?assertEqual(ok, elock:unlock(Ref)),

  ?assertEqual(Context, elock_test_utils:context(C1)),
  ?assertEqual([{t1, Manager, 1}], elock_test_utils:locks(Scope)),
  ?assert(is_process_alive(Manager)),
  % C2 can not take the lock, C1 holds it
  R2 = elock_test_utils:lock_async(C2, Scope, t1, [Node], #{}),
  ?assertEqual(timeout, elock_test_utils:result(R2, ?QUIET)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)),
  {ok, {ok, R2Ref}} = elock_test_utils:result(R2, ?DEADLINE),
  ?assert(is_reference(R2Ref)),
  ?assertEqual(ok, elock_test_utils:unlock(C2, R2Ref)),
  elock_test_utils:wait_idle(Scope),
  elock_test_utils:stop(C1),
  elock_test_utils:stop(C2).

%%-----------------------------------------------------------------
%%  Every request field follows the current proxy protocol. A fresh client
%%  is its own proxy; local acquisitions stay direct with held locks. Reentry adds a ref
%%  without adding a distinct held key, and duplicate nodes collapse.
%%-----------------------------------------------------------------
request_fields_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  Self = self(),
  C1 = elock_test_utils:client(),

  % a fresh client holds nothing
  true = ets:insert(Scope, {t1, Self, 1}),
  R1 = elock_test_utils:lock_async(C1, Scope, t1, [Node], #{is_shared => true, timeout => 1234}),
  #request{ref = Ref1, tag = Tag1} = Request1 = ?RECEIVE(#request{}),
  ?assert(is_reference(Ref1)),
  ?assert(is_reference(Tag1)),
  ?assertNotEqual(Ref1, Tag1),
  ?assertEqual(#request{
    ticket = 2,
    ref = Ref1,
    scope = Scope,
    term = t1,
    client = C1,
    proxy = C1,
    tag = Tag1,
    shared = true,
    timeout = 1234,
    notify_queued = false
  }, Request1),
  ?assert(elock_test_utils:pending(R1)),
  C1 ! ?reply(Tag1, #locked{ref = Ref1}),
  ?assertEqual({ok, {ok, Ref1}}, elock_test_utils:result(R1, ?DEADLINE)),

  % a real lock on another term is counted together with the fake one
  {ok, Ref0} = elock_test_utils:lock(C1, Scope, t0, [Node]),
  M0 = elock_test_utils:wait_manager(Scope, t0),
  ?assertEqual(#{
    {Scope, t0, Node} => M0,
    {Scope, t1, Node} => Self
  }, elock_context:held_locks(elock_test_utils:context(C1))),

  % the defaults of lock/3, duplicated nodes collapse to the single node path
  true = ets:insert(Scope, {t2, Self, 1}),
  R2 = elock_test_utils:cast(C1, fun()-> elock:lock(Scope, t2, [Node, Node]) end),
  #request{ref = Ref2, tag = Tag2, proxy = Proxy2} = Request2 = ?RECEIVE(#request{}),
  ?assert(is_pid(Proxy2)),
  ?assertEqual(C1, Proxy2),
  ?assertEqual(Node, node(Proxy2)),
  ?assertEqual(#request{
    ticket = 2,
    ref = Ref2,
    scope = Scope,
    term = t2,
    client = C1,
    proxy = Proxy2,
    tag = Tag2,
    shared = false,
    timeout = undefined,
    notify_queued = true
  }, Request2),
  Proxy2 ! ?reply(Tag2, #locked{ref = Ref2}),
  ?assertEqual({ok, {ok, Ref2}}, elock_test_utils:result(R2, ?DEADLINE)),

  % the second request of the same term takes the next ticket
  R3 = elock_test_utils:lock_async(C1, Scope, t2, [Node], #{}),
  #request{ref = Ref3, tag = Tag3, proxy = Proxy3} = Request3 = ?RECEIVE(#request{}),
  ?assert(is_pid(Proxy3)),
  ?assertEqual(C1, Proxy3),
  ?assertEqual(Node, node(Proxy3)),
  ?assertEqual(Request2#request{ticket = 3, ref = Ref3, tag = Tag3, proxy = Proxy3}, Request3),
  Proxy3 ! ?reply(Tag3, #locked{ref = Ref3}),
  ?assertEqual({ok, {ok, Ref3}}, elock_test_utils:result(R3, ?DEADLINE)),

  % four locks over three keys: a key held twice counts once
  R4 = elock_test_utils:lock_async(C1, Scope, t1, [Node], #{}),
  #request{ref = Ref4, tag = Tag4, proxy = Proxy4} = Request4 = ?RECEIVE(#request{}),
  ?assert(is_pid(Proxy4)),
  ?assertEqual(C1, Proxy4),
  ?assertEqual(Node, node(Proxy4)),
  ?assertEqual(Request1#request{ticket = 3, ref = Ref4, tag = Tag4,
    proxy = Proxy4, shared = false, timeout = undefined, notify_queued = true}, Request4),
  Proxy4 ! ?reply(Tag4, #timeout{ref = Ref4}),
  ?assertEqual({ok, {error, timeout}}, elock_test_utils:result(R4, ?DEADLINE)),

  % a tag of its own for every request
  Tags = [Tag1, Tag2, Tag3, Tag4],
  ?assert(lists:all(fun erlang:is_reference/1, Tags)),
  ?assertEqual(8, length(lists:usort(Tags ++ [Ref1, Ref2, Ref3, Ref4]))),

  % the unlocks reach the fake manager
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual(#unlock{ref = Ref1}, ?RECEIVE(#unlock{})),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref2)),
  ?assertEqual(#unlock{ref = Ref2}, ?RECEIVE(#unlock{})),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref3)),
  ?assertEqual(#unlock{ref = Ref3}, ?RECEIVE(#unlock{})),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref0)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  ?NO_MESSAGE,

  % A fresh request naming two distinct nodes uses a proxy even without holds.
  R5 = elock_test_utils:lock_async(C1, Scope, t2,
    [Node, 'nonexistent@nohost'], #{}),
  #request{ref = Ref5, tag = Tag5, proxy = Proxy5} = Request5 = ?RECEIVE(#request{}),
  ?assertNotEqual(C1, Proxy5),
  ?assert(is_pid(Proxy5)),
  ?assertEqual(Node, node(Proxy5)),
  ?assertEqual(#request{ticket = 4, ref = Ref5, scope = Scope, term = t2,
    client = C1, proxy = Proxy5, tag = Tag5, shared = false,
    timeout = undefined, notify_queued = true}, Request5),
  ?assertEqual(10, length(lists:usort(Tags ++ [Tag5, Ref1, Ref2, Ref3, Ref4, Ref5]))),
  Proxy5 ! ?reply(Tag5, #locked{ref = Ref5}),
  ?assertMatch({ok, {error, {badrpc, _}}}, elock_test_utils:result(R5, ?DEADLINE)),
  ?assertEqual(#unlock{ref = Ref5}, ?RECEIVE(#unlock{})),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  ?NO_MESSAGE,

  true = ets:delete(Scope, t1),
  true = ets:delete(Scope, t2),
  elock_test_utils:wait_idle(Scope),
  elock_test_utils:stop(C1).

%%-----------------------------------------------------------------
%%  The verdicts of the manager, each under the tag of the request:
%%  #locked{} -> {ok, Ref} and the context names the fake manager;
%%  #deadlock{} includes the winner lock and no context, #timeout{}
%%  -> {error, timeout}, #retry{} -> the client comes back with a
%%  new ticket and a new tag; the replies with another tag and the
%%  bare verdicts are left in the client's mailbox
%%-----------------------------------------------------------------
verdicts_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  Self = self(),
  C1 = elock_test_utils:client(),
  true = ets:insert(Scope, {t1, Self, 1}),

  % #locked{}
  R1 = elock_test_utils:lock_async(C1, Scope, t1, [Node], #{}),
  #request{notify_queued = false, ref = Ref1, ticket = 2, tag = Tag1} = ?RECEIVE(#request{}),
  C1 ! ?reply(Tag1, #locked{ref = Ref1}),
  ?assertEqual({ok, {ok, Ref1}}, elock_test_utils:result(R1, ?DEADLINE)),
  ?assertEqual(#context{
    birth = elock_test_utils:context_birth(C1),
    ref2lock = #{ Ref1 => #lock{
      scope = Scope,
      term = t1,
      nodes = #{ Node => #node{manager = Self} }
    } },
    locked = #{ {Scope, t1, Node} => Self },
    counts = #{}
  }, elock_test_utils:context(C1)),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual(#unlock{ref = Ref1}, ?RECEIVE(#unlock{})),
  ?assertEqual(undefined, elock_test_utils:context(C1)),

  % #deadlock{}
  R2 = elock_test_utils:lock_async(C1, Scope, t1, [Node], #{}),
  #request{notify_queued = false, ref = Ref2, ticket = 3, tag = Tag2} = ?RECEIVE(#request{}),
  C1 ! ?reply(Tag2, #deadlock{ref = Ref2, winner = ?WINNER}),
  ?assertEqual({ok, {error, {deadlock, ?WINNER}}}, elock_test_utils:result(R2, ?DEADLINE)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),

  % #timeout{}
  R3 = elock_test_utils:lock_async(C1, Scope, t1, [Node], #{timeout => 60000}),
  #request{notify_queued = false, ref = Ref3, ticket = 4, timeout = 60000, tag = Tag3} = ?RECEIVE(#request{}),
  C1 ! ?reply(Tag3, #timeout{ref = Ref3}),
  ?assertEqual({ok, {error, timeout}}, elock_test_utils:result(R3, ?DEADLINE)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),

  % #retry{}: a new ticket and a new tag, the same ref
  R4 = elock_test_utils:lock_async(C1, Scope, t1, [Node], #{}),
  #request{notify_queued = false, ref = Ref4, ticket = 5, tag = Tag4} = ?RECEIVE(#request{}),
  C1 ! ?reply(Tag4, #retry{ref = Ref4}),
  #request{notify_queued = false, ref = Ref4, ticket = 6, client = C1, proxy = C1, tag = Tag5} = ?RECEIVE(#request{}),
  ?assertNotEqual(Tag4, Tag5),
  ?assertEqual([{t1, Self, 6}], elock_test_utils:locks(Scope)),
  ?assert(elock_test_utils:pending(R4)),
  C1 ! ?reply(Tag5, #locked{ref = Ref4}),
  ?assertEqual({ok, {ok, Ref4}}, elock_test_utils:result(R4, ?DEADLINE)),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref4)),
  ?assertEqual(#unlock{ref = Ref4}, ?RECEIVE(#unlock{})),

  % only the replies with the tag of the request are taken: the bare
  % verdicts, even with its ref, and the replies with another tag are
  % left in the mailbox
  R5 = elock_test_utils:lock_async(C1, Scope, t1, [Node], #{}),
  #request{notify_queued = false, ref = Ref5, ticket = 7, tag = Tag6} = ?RECEIVE(#request{}),
  Foreign = [
    #locked{ref = Ref5},
    #deadlock{ref = Ref5, winner = ?WINNER},
    #timeout{ref = Ref5},
    #retry{ref = Ref5},
    ?reply(Tag4, #locked{ref = Ref5}),
    ?reply(Tag5, #deadlock{ref = Ref5, winner = ?WINNER}),
    ?reply(make_ref(), #timeout{ref = Ref5}),
    ?reply(Ref5, #retry{ref = Ref5})
  ],
  [ C1 ! Message || Message <- Foreign ],
  ?assertEqual(timeout, elock_test_utils:result(R5, ?QUIET)),
  ?assertEqual([{t1, Self, 7}], elock_test_utils:locks(Scope)),
  C1 ! ?reply(Tag6, #timeout{ref = Ref5}),
  ?assertEqual({ok, {error, timeout}}, elock_test_utils:result(R5, ?DEADLINE)),
  ?assertEqual({messages, Foreign}, process_info(C1, messages)),

  ?NO_MESSAGE,
  true = ets:delete(Scope, t1),
  elock_test_utils:stop(C1).

%%-----------------------------------------------------------------
%%  A direct waiter starts its graph worker on the tagged queued notice.
%%  Manager verdicts retire the graph; graph verdicts withdraw the request
%%  and consume exactly one correlated reply, preserving unrelated mail.
%%-----------------------------------------------------------------
queued_publishes_holds_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  Self = self(),
  C1 = elock_test_utils:client(),
  {ok, Ref0} = elock_test_utils:lock(C1, Scope, t0, [Node]),
  true = ets:insert(Scope, {t2, Self, 1}),
  lists:foreach(fun(Outcome)->
    Before = elock_test_utils:context(C1),
    Pending = elock_test_utils:lock_async(C1, Scope, t2, [Node], #{}),
    #request{ref = Ref, proxy = C1, tag = Tag, notify_queued = true} = ?RECEIVE(#request{}),
    ?assertEqual([], elock_test_utils:graph_rows(Scope, t2)),
    ?assertEqual([], [P || P <- processes(),
      process_info(P, current_function) =:= {current_function, {elock_graph, graph_loop, 1}}]),
    C1 ! ?reply(Tag, #queued{ref = Ref, manager = Self, node = Node}),
    ?WAIT(elock_test_utils:published(Node, C1, Scope, t2)),
    [#waiter{client = Graph} = Row] = elock_test_utils:graph_rows(Scope, t2),
    ?assertNotEqual(C1, Graph),
    ?assertNotEqual(Self, Graph),
    ?assertEqual(#waiter{lock = {Scope, t2, Node}, ref = Ref,
      birth = elock_test_utils:context_birth(C1), client = Graph,
      holds = elock_context:held_locks(Before)}, Row),
    ?assertEqual({monitors, [{process, C1}]}, process_info(Graph, monitors)),
    Foreign = case Outcome of
      locked-> [{make_ref(), #locked{ref = Ref}}, {application_message, Ref}];
      timeout-> [{make_ref(), #timeout{ref = Ref}}, {application_message, Ref}];
      _-> []
    end,
    [C1 ! Message || Message <- Foreign],
    case Outcome of
      grant->
        C1 ! ?reply(Tag, #locked{ref = Ref}),
        ?assertEqual({ok, {ok, Ref}}, elock_test_utils:result(Pending, ?DEADLINE)),
        ?assert(is_map_key({Scope, t2, Node},
          elock_context:held_locks(elock_test_utils:context(C1)))),
        ok = elock_test_utils:unlock(C1, Ref),
        ?RECEIVE(#unlock{ref = Ref});
      manager_deadlock->
        C1 ! ?reply(Tag, #deadlock{ref = Ref, winner = ?WINNER}),
        ?assertEqual({ok, {error, {deadlock, ?WINNER}}},
          elock_test_utils:result(Pending, ?DEADLINE)),
        ?assertEqual(Before, elock_test_utils:context(C1)),
        ?NO_MESSAGE;
      _GraphVerdict->
        Graph ! #deadlock{ref = Ref, winner = ?WINNER},
        ?assertEqual(#unlock{ref = Ref}, ?RECEIVE(#unlock{})),
        ?assertEqual(timeout, elock_test_utils:result(Pending, ?QUIET)),
        Reply = case Outcome of
          withdrawn-> #withdrawn{ref = Ref};
          locked-> #locked{ref = Ref};
          timeout-> #timeout{ref = Ref}
        end,
        C1 ! ?reply(Tag, Reply),
        ?assertEqual({ok, {error, {deadlock, ?WINNER}}},
          elock_test_utils:result(Pending, ?DEADLINE)),
        ?assertEqual(Before, elock_test_utils:context(C1))
    end,
    ?WAIT(elock_test_utils:graph_rows(Scope, t2) =:= []),
    elock_test_utils:wait_dead(Graph),
    ?assertEqual({messages, Foreign}, process_info(C1, messages)),
    ?assertEqual(Foreign, elock_test_utils:call(C1, fun elock_test_utils:flush/0)),
    ?NO_MESSAGE
  end, [grant, manager_deadlock, withdrawn, locked, timeout]),
  true = ets:delete(Scope, t2),
  ok = elock_test_utils:unlock(C1, Ref0).

%%-----------------------------------------------------------------
%%  The manager exits before the verdict: the client deletes the
%%  dead entry, retries, takes ticket 1 and starts a real manager
%%-----------------------------------------------------------------
fake_manager_dies_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  Self = self(),
  C1 = elock_test_utils:client(),

  Fake = spawn(fun()->
    Request = ?RECEIVE(#request{}),
    Self ! {fake_got, self(), Request}
  end),
  true = ets:insert(Scope, {t1, Fake, 1}),

  R1 = elock_test_utils:lock_async(C1, Scope, t1, [Node], #{}),
  {fake_got, Fake, #request{ref = Ref1, ticket = 2}} = ?RECEIVE({fake_got, Fake, _}),
  elock_test_utils:wait_dead(Fake),

  ?assertEqual({ok, {ok, Ref1}}, elock_test_utils:result(R1, ?DEADLINE)),
  Manager = elock_test_utils:wait_manager(Scope, t1),
  ?assertNotEqual(Fake, Manager),
  ?assert(is_process_alive(Manager)),
  ?assertEqual([{t1, Manager, 1}], elock_test_utils:locks(Scope)),
  ?assertEqual(#context{
    birth = elock_test_utils:context_birth(C1),
    ref2lock = #{ Ref1 => #lock{
      scope = Scope,
      term = t1,
      nodes = #{ Node => #node{manager = Manager} }
    } },
    locked = #{ {Scope, t1, Node} => Manager },
    counts = #{}
  }, elock_test_utils:context(C1)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  elock_test_utils:wait_idle(Scope),
  elock_test_utils:stop(C1).

%% Only a correlated deadlock exit is a verdict; unrelated death keeps waiting.
wait_verdict_deadlock_verdict_test(Config)->
  Scope = ?config(scope, Config),
  Parent = self(),
  lists:foreach(fun(Reason)->
    Ref = make_ref(),
    Client = elock_test_utils:client(),
    Graph = elock_test_utils:collector(),
    M1 = elock_test_utils:collector(),
    M2 = elock_test_utils:collector(),
    M3 = elock_test_utils:collector(),
    Pending = elock_test_utils:cast(Client, fun()->
      W1 = worker(Ref, n1, {ok, {ok, M1}}),
      W2 = worker(Ref, n2, {ok, {ok, M2}}),
      W3 = worker(Ref, n3, {ok, {ok, M3}}),
      Waiting = waiting(Ref, Scope, Graph, #{W1 => n1, W2 => n2, W3 => n3}),
      Parent ! {workers, Client, W1, W2, W3},
      elock_context:wait_verdict(Ref, Waiting)
    end),
    {workers, Client, W1, W2, W3} = ?RECEIVE({workers, Client, _, _, _}),
    finish_worker(W1),
    Client ! #queued{ref = Ref, manager = M2, node = n2},
    ?assertEqual([{add, Ref, n1, M1},
      #queued{ref = Ref, manager = M2, node = n2}],
      elock_test_utils:collected(Graph, 2)),
    case Reason of
      deadlock-> exit(Graph, {Ref, #deadlock{ref = Ref, winner = ?WINNER}});
      bare-> exit(Graph, #deadlock{ref = Ref, winner = ?WINNER});
      foreign_tag-> exit(Graph, {make_ref(), #deadlock{ref = Ref, winner = ?WINNER}});
      killed-> exit(Graph, kill)
    end,
    elock_test_utils:wait_dead(Graph),
    ?assertEqual(timeout, elock_test_utils:result(Pending, ?QUIET)),
    case Reason of
      deadlock->
        ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M1, 1)),
        ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M2, 1)),
        elock_test_utils:wait_dead(W1);
      _->
        ?assert(is_process_alive(W1)),
        ?NO_MESSAGE
    end,
    finish_worker(W3),
    finish_worker(W2),
    case Reason of
      deadlock->
        ?assertEqual({ok, {error, {deadlock, ?WINNER}}},
          elock_test_utils:result(Pending, ?DEADLINE)),
        ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M3, 1)),
        ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M2, 1)),
        elock_test_utils:wait_dead(W2),
        elock_test_utils:wait_dead(W3);
      _->
        Expected = #{n1 => #node{manager = M1, holder = W1},
          n2 => #node{manager = M2, holder = W2},
          n3 => #node{manager = M3, holder = W3}},
        ?assertEqual({ok, {ok, Expected}},
          elock_test_utils:result(Pending, ?DEADLINE)),
        ?assertEqual({messages, []}, process_info(Client, messages)),
        elock_test_utils:call(Client, fun()-> elock_context:release(Expected, Ref) end),
        [ ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M, 1))
          || M <- [M1, M2, M3] ]
    end,
    ?NO_MESSAGE
  end, [deadlock, bare, foreign_tag, killed]).

%%-----------------------------------------------------------------
%%  Each grant is sent to the graph worker, followed by request removal.
%%  The managers receive nothing.
%%-----------------------------------------------------------------
wait_verdict_all_granted_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  Graph = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  W1 = worker(Ref, n1, {ok, {ok, M1}}),
  W2 = worker(Ref, n2, {ok, {ok, M2}}),

  finish_worker(W1),
  finish_worker(W2),
  ?assertEqual({ok, #{
    n1 => #node{manager = M1, holder = W1},
    n2 => #node{manager = M2, holder = W2}
  }}, elock_context:wait_verdict(Ref, waiting(Ref, Scope, Graph, #{ W1 => n1, W2 => n2 }))),
  ?assertEqual([{add, Ref, n1, M1}, {add, Ref, n2, M2}, {remove, Ref}],
    elock_test_utils:collected(Graph, 3)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The queued notice is forwarded before the grants and final removal.
%%-----------------------------------------------------------------
wait_verdict_queued_then_granted_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  Graph = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  W1 = worker(Ref, n1, {ok, {ok, M1}}),
  W2 = worker(Ref, n2, {ok, {ok, M2}}),

  self() ! #queued{ref = Ref, manager = M2, node = n2},
  finish_worker(W1),
  finish_worker(W2),
  ?assertEqual({ok, #{
    n1 => #node{manager = M1, holder = W1},
    n2 => #node{manager = M2, holder = W2}
  }}, elock_context:wait_verdict(Ref, waiting(Ref, Scope, Graph, #{ W1 => n1, W2 => n2 }))),

  ?assertEqual([#queued{ref = Ref, manager = M2, node = n2},
    {add, Ref, n1, M1}, {add, Ref, n2, M2}, {remove, Ref}],
    elock_test_utils:collected(Graph, 4)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  Grant and queue events preserve order at the graph worker.
%%-----------------------------------------------------------------
wait_verdict_granted_then_queued_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  Graph = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  W1 = worker(Ref, n1, {ok, {ok, M1}}),
  W2 = worker(Ref, n2, {ok, {ok, M2}}),
  W3 = worker(Ref, n3, {ok, {ok, M3}}),

  finish_worker(W1),
  self() ! #queued{ref = Ref, manager = M2, node = n2},
  finish_worker(W3),
  finish_worker(W2),
  ?assertEqual({ok, #{
    n1 => #node{manager = M1, holder = W1},
    n2 => #node{manager = M2, holder = W2},
    n3 => #node{manager = M3, holder = W3}
  }},
    elock_context:wait_verdict(Ref, waiting(Ref, Scope, Graph, #{ W1 => n1, W2 => n2, W3 => n3 }))),

  ?assertEqual([{add, Ref, n1, M1},
    #queued{ref = Ref, manager = M2, node = n2},
    {add, Ref, n3, M3}, {add, Ref, n2, M2}, {remove, Ref}],
    elock_test_utils:collected(Graph, 5)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  #queued{} of a foreign ref and the reply of a worker of
%%  another request (its tag is the ref of that request) are left
%%  in the mailbox and do not disturb the request
%%-----------------------------------------------------------------
wait_verdict_foreign_queued_left_in_mailbox_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  Graph = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  W1 = worker(Ref, n1, {ok, {ok, M1}}),
  W2 = worker(Ref, n2, {ok, {ok, M2}}),
  Foreign = [
    #queued{ref = make_ref(), manager = M2, node = n2},
    {make_ref(), n2, {ok, {ok, M2}}}
  ],

  [ self() ! Message || Message <- Foreign ],
  finish_worker(W1),
  self() ! #queued{ref = Ref, manager = M2, node = n2},
  finish_worker(W2),
  ?assertEqual({ok, #{
    n1 => #node{manager = M1, holder = W1},
    n2 => #node{manager = M2, holder = W2}
  }}, elock_context:wait_verdict(Ref, waiting(Ref, Scope, Graph, #{ W1 => n1, W2 => n2 }))),

  ?assertEqual([{add, Ref, n1, M1},
    #queued{ref = Ref, manager = M2, node = n2},
    {add, Ref, n2, M2}, {remove, Ref}],
    elock_test_utils:collected(Graph, 4)),
  ?assertEqual(Foreign, elock_test_utils:flush()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A failure: n1 granted, n3 queued, n2 fails with a deadlock verdict
%%  -> #unlock{} to the granted and to the queued manager, the still
%%  pending n4 is awaited: it grants late and is unlocked too, a
%%  #queued{} arriving meanwhile is unlocked as well. The error is
%%  returned as is
%%-----------------------------------------------------------------
wait_verdict_failure_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  Graph = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  M4 = elock_test_utils:collector(),
  M5 = elock_test_utils:collector(),
  W1 = worker(Ref, n1, {ok, {ok, M1}}),
  W2 = worker(Ref, n2, {error, {deadlock, ?WINNER}}),
  W3 = worker(Ref, n3, {error, timeout}),
  W4 = worker(Ref, n4, {ok, {ok, M4}}),

  finish_worker(W1),
  self() ! #queued{ref = Ref, manager = M3, node = n3},
  finish_worker(W2),
  self() ! #queued{ref = Ref, manager = M5, node = n5},
  finish_worker(W4),
  finish_worker(W3),
  ?assertEqual({error, {deadlock, ?WINNER}},
    elock_context:wait_verdict(Ref,
      waiting(Ref, Scope, Graph, #{W1 => n1, W2 => n2, W3 => n3, W4 => n4}))),

  ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M1, 1)),
  ?assertEqual([
    #unlock{ref = Ref}
  ], elock_test_utils:collected(M3, 1)),
  ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M5, 1)),
  ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M4, 1)),
  elock_test_utils:wait_dead(W1),
  elock_test_utils:wait_dead(W4),
  % the failed node has nothing to unlock
  ?assertEqual([{add, Ref, n1, M1},
    #queued{ref = Ref, manager = M3, node = n3}, {remove, Ref}],
    elock_test_utils:collected(Graph, 3)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  After the failure the late results of the other nodes: a late
%%  failure is ignored, a late grant is unlocked
%%-----------------------------------------------------------------
wait_verdict_failure_late_results_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  Graph = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  W1 = worker(Ref, n1, {error, timeout}),
  W2 = worker(Ref, n2, {error, {badrpc, noconnection}}),
  W3 = worker(Ref, n3, {ok, {ok, M3}}),
  W4 = worker(Ref, n4, {error, {deadlock, ?WINNER}}),

  finish_worker(W1),
  finish_worker(W2),
  finish_worker(W3),
  finish_worker(W4),
  ?assertEqual({error, timeout},
    elock_context:wait_verdict(Ref,
      waiting(Ref, Scope, Graph, #{W1 => n1, W2 => n2, W3 => n3, W4 => n4}))),

  ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M3, 1)),
  elock_test_utils:wait_dead(W3),
  ?assertEqual([{remove, Ref}],
    elock_test_utils:collected(Graph, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The first non-ok result is returned as is, whatever it is: a
%%  timeout or a transport error returned by ecall
%%-----------------------------------------------------------------
wait_verdict_failure_reasons_test(Config)->
  Scope = ?config(scope, Config),
  lists:foreach(
    fun(Reason)->
      Ref = make_ref(),
      Graph = elock_test_utils:collector(),
      M1 = elock_test_utils:collector(),
      W1 = worker(Ref, n1, {ok, {ok, M1}}),
      W2 = worker(Ref, n2, Reason),
      finish_worker(W1),
      finish_worker(W2),
      ?assertEqual(Reason, elock_context:wait_verdict(Ref,
        waiting(Ref, Scope, Graph, #{W1 => n1, W2 => n2}))),
      ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M1, 1)),
      ?assertEqual([{add, Ref, n1, M1}, {remove, Ref}],
        elock_test_utils:collected(Graph, 2)),
      ?NO_MESSAGE
    end,
    [
      {error, timeout},
      {error, {deadlock, ?WINNER}},
      {error, {badrpc, noconnection}},
      {error, {badrpc, {'EXIT', {badarg, []}}}},
      {error, {exit, killed}}
    ]
  ).

%%-----------------------------------------------------------------
%%  Two managers reporting the same queued node are both forwarded.
%%  Failure withdraws both copies; a grant removes both from the queued map.
%%-----------------------------------------------------------------
wait_verdict_requeued_test(Config)->
  Scope = ?config(scope, Config),
  lists:foreach(fun(Granted)->
    Ref = make_ref(),
    Graph = elock_test_utils:collector(),
    M1 = elock_test_utils:collector(),
    M2a = elock_test_utils:collector(),
    M2b = elock_test_utils:collector(),
    W1 = worker(Ref, n1, {error, timeout}),
    W2 = worker(Ref, n2, {ok, {ok, M2b}}),
    Queued = [#queued{ref = Ref, manager = M2a, node = n2},
      #queued{ref = Ref, manager = M2b, node = n2}],
    [ self() ! Message || Message <- Queued ],
    case Granted of
      true-> finish_worker(W2), finish_worker(W1);
      false-> finish_worker(W1), finish_worker(W2)
    end,
    ?assertEqual({error, timeout}, elock_context:wait_verdict(Ref,
      waiting(Ref, Scope, Graph, #{W1 => n1, W2 => n2}))),
    Expected = case Granted of
      true-> [{add, Ref, n2, M2b}, {remove, Ref}];
      false-> [{remove, Ref}]
    end,
    ?assertEqual(Queued ++ Expected,
      elock_test_utils:collected(Graph, 2 + length(Expected))),
    case Granted of
      true-> ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M2b, 1));
      false->
        ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M2a, 1)),
        ?assertEqual([#unlock{ref = Ref}, #unlock{ref = Ref}],
          elock_test_utils:collected(M2b, 2))
    end,
    ?assertEqual([], elock_test_utils:collected(M1, 0)),
    ?NO_MESSAGE
  end, [false, true]).

%%-----------------------------------------------------------------
%%  The waits of a client for its workers do not scan the messages
%%  it had before the request: the request ref made in lock/4 marks
%%  the mailbox position, it is passed on as an argument of its own
%%  and every clause of the receives of wait_verdict/2 and of
%%  wait_unlock/2 matches it, their first parameter. A refactoring
%%  that hides the ref from the compiler (the receive takes it out
%%  of #waiting{}) passes every other test and only costs the scan.
%%  The holder receive also accepts logged unexpected messages.
%%-----------------------------------------------------------------
receive_marker_test(_Config)->
  ?assertEqual([
    {{lock, 4}, reserved_receive_marker},
    {{lock, 4}, passed_marker},
    {{wait_verdict, 2}, {used_receive_marker, {parameter, 1}}},
    {{wait_unlock, 2}, {used_receive_marker, {parameter, 1}}},
    {{hold, 3}, passed_marker},
    {{holding, 3}, unoptimized_selective_receive}
  ], elock_test_utils:recv_opt_info(elock_context)).

%%-----------------------------------------------------------------
%%  A single remote node that can not be reached: {error, {badrpc, _}}
%%  and no context
%%-----------------------------------------------------------------
single_remote_node_unreachable_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:client(),

  ?assertMatch({error, {badrpc, _}}, elock_test_utils:lock(C1, Scope, t, ['nonexistent@nohost'])),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  ?assertEqual([], elock_test_utils:locks(Scope)),
  elock_test_utils:stop(C1).

%%-----------------------------------------------------------------
%%  An unreachable node among the requested ones fails the whole
%%  request: {error, {badrpc, _}}, the local node was granted and is
%%  released again - the local scope is idle, no context
%%-----------------------------------------------------------------
unreachable_node_releases_granted_node_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  C1 = elock_test_utils:client(),

  ?assertMatch({error, {badrpc, _}}, elock_test_utils:lock(C1, Scope, t, [Node, 'nonexistent@nohost'])),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  elock_test_utils:wait_idle(Scope),

  % the term is free again
  {ok, Ref} = elock_test_utils:lock(C1, Scope, t, [Node]),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)),
  elock_test_utils:wait_idle(Scope),
  elock_test_utils:stop(C1).

%%=================================================================
%%  The scope
%%=================================================================
%%-----------------------------------------------------------------
%%  start_link/1: a named public set table owned by the returned
%%  pid, the pg scope process alive with the pid as its member,
%%  ready_nodes = [node()]. Killing the pid takes everything with it
%%-----------------------------------------------------------------
start_link_test(_Config)->
  Scope = ?FUNCTION_NAME,
  PgScope = ?PG_SCOPE(Scope),
  process_flag(trap_exit, true),

  {ok, Pid} = elock:start_link(Scope),
  ?assert(is_pid(Pid)),
  ?WAIT(is_reference(ets:whereis(Scope))),

  Info = ets:info(Scope),
  ?assertEqual(Pid, proplists:get_value(owner, Info)),
  ?assertEqual(true, proplists:get_value(named_table, Info)),
  ?assertEqual(public, proplists:get_value(protection, Info)),
  ?assertEqual(set, proplists:get_value(type, Info)),
  ?assertEqual(true, proplists:get_value(read_concurrency, Info)),
  ?assertEqual(auto, proplists:get_value(write_concurrency, Info)),
  ?assertEqual([], ets:tab2list(Scope)),

  ?WAIT(is_pid(whereis(PgScope)) andalso pg:get_members(PgScope, {elock, '$members$'}) =:= [Pid]),
  ?assertEqual([node()], elock:ready_nodes(Scope)),

  % the scope is functional
  {ok, Ref} = elock:lock(Scope, t, [node()]),
  ?assertEqual(ok, elock:unlock(Ref)),
  elock_test_utils:wait_idle(Scope),

  exit(Pid, kill),
  ?assertEqual({'EXIT', Pid, killed}, ?RECEIVE({'EXIT', Pid, _})),
  ?WAIT(ets:whereis(Scope) =:= undefined),
  ?WAIT(whereis(PgScope) =:= undefined),
  process_flag(trap_exit, false).

%%-----------------------------------------------------------------
%%  Starting a scope twice on a node: the second process crashes
%%  with badarg (ets:new) and the exit reaches the caller over the
%%  link, the first scope is not affected
%%-----------------------------------------------------------------
start_twice_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  Owner = proplists:get_value(owner, ets:info(Scope)),
  process_flag(trap_exit, true),

  {ok, Pid2} = elock:start_link(Scope),
  ?assertMatch({'EXIT', Pid2, {badarg, _}}, ?RECEIVE({'EXIT', Pid2, _})),

  ?assertEqual(Owner, proplists:get_value(owner, ets:info(Scope))),
  ?assert(is_process_alive(Owner)),
  ?assertEqual([Node], elock:ready_nodes(Scope)),
  ?assertEqual([], elock_test_utils:locks(Scope)),

  C1 = elock_test_utils:client(),
  {ok, Ref} = elock_test_utils:lock(C1, Scope, t, [Node]),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)),
  elock_test_utils:wait_idle(Scope),
  elock_test_utils:stop(C1),
  process_flag(trap_exit, false).

%%-----------------------------------------------------------------
%%  ready_nodes/1 of a scope that was never started. The pg of OTP 27
%%  answers [] for an unknown scope (pg:get_members/2 catches the
%%  badarg of the missing table), hence no members - no nodes
%%-----------------------------------------------------------------
ready_nodes_unknown_scope_test(_Config)->
  ?assertEqual([], elock:ready_nodes(?FUNCTION_NAME)),
  ?assertEqual(undefined, ets:whereis(?FUNCTION_NAME)),
  ?assertEqual(undefined, whereis(?PG_SCOPE(?FUNCTION_NAME))).

%%-----------------------------------------------------------------
%%  A lock on a stopped scope raises badarg (the table is gone), no
%%  context is left, the scope has no ready nodes any more
%%-----------------------------------------------------------------
lock_on_stopped_scope_test(Config)->
  Scope = ?config(scope, Config),
  Holder = ?config(holder, Config),
  Node = node(),
  C1 = elock_test_utils:client(),

  {ok, Ref} = elock_test_utils:lock(C1, Scope, t, [Node]),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)),
  elock_test_utils:wait_idle(Scope),

  ?assertEqual(ok, elock_test_utils:stop_scope(Holder)),
  ?assertEqual(undefined, ets:whereis(Scope)),

  ?assertError(badarg, elock:lock(Scope, t, [Node])),
  ?assertError(badarg, elock:lock(Scope, t, [Node], #{is_shared => true, timeout => 100})),
  ?assertEqual(undefined, get(?CONTEXT)),
  ?assertMatch({'EXIT', {error, badarg, _}}, elock_test_utils:lock(C1, Scope, t, [Node])),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  ?assertEqual([], elock:ready_nodes(Scope)),
  ?assertEqual([], elock_test_utils:managers()),
  elock_test_utils:stop(C1).

%%-----------------------------------------------------------------
%%  Ten scopes, the same term locked in each by one client: ten
%%  keys in the context, each with its own manager; unlocking all
%%  erases the context and every scope is idle
%%-----------------------------------------------------------------
many_scopes_test(_Config)->
  Node = node(),
  Scopes = [ list_to_atom(atom_to_list(?FUNCTION_NAME) ++ "_" ++ integer_to_list(I)) || I <- lists:seq(1, 10) ],
  Holders = [ elock_test_utils:start_scope(Scope) || Scope <- Scopes ],
  C1 = elock_test_utils:client(),

  Locks =
    [ begin
        {ok, Ref} = elock_test_utils:lock(C1, Scope, t, [Node]),
        {Scope, Ref}
      end || Scope <- Scopes ],
  Managers = [ elock_test_utils:wait_manager(Scope, t) || Scope <- Scopes ],
  ?assertEqual(10, length(lists:usort(Managers))),

  #context{ref2lock = Ref2Lock, locked = Locked, counts = Counts} = elock_test_utils:context(C1),
  ?assertEqual(
    maps:from_list([ {Ref, #lock{
      scope = Scope,
      term = t,
      nodes = #{Node => #node{manager = Manager}}
    }}
      || {{Scope, Ref}, Manager} <- lists:zip(Locks, Managers) ]),
    Ref2Lock
  ),
  ?assertEqual(
    maps:from_list([ {{Scope, t, Node}, Manager} || {Scope, Manager} <- lists:zip(Scopes, Managers) ]),
    Locked
  ),
  ?assertEqual(#{}, Counts),
  [ ?assertEqual([{t, Manager, 1}], elock_test_utils:locks(Scope)) || {Scope, Manager} <- lists:zip(Scopes, Managers) ],

  [ ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)) || {_Scope, Ref} <- Locks ],
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  [ elock_test_utils:wait_idle(Scope) || Scope <- Scopes ],

  elock_test_utils:stop(C1),
  [ elock_test_utils:stop_scope(Holder) || Holder <- Holders ],
  ok.

%%=================================================================
%%  Utilities
%%=================================================================
% The request Ref of the test process for the term t on Nodes, as
% locked/3 sees it once every node has granted
multi_node_request(Ref, Scope, _Nodes)->
  #request{
    ref = Ref,
    scope = Scope,
    term = t,
    client = self(),
    shared = false,
    timeout = undefined
  }.

% A #waiting{} of the request Ref for the term t with the given
% pending workers: #{Worker => Node}. A collector stands in for the graph.
waiting(Ref, Scope, Workers)->
  waiting(Ref, Scope, elock_test_utils:collector(), Workers).
waiting(Ref, Scope, Graph, Workers)->
  #waiting{ref = Ref, scope = Scope, term = t,
    pending = maps:from_list([ {Node, Worker} || {Worker, Node} <- maps:to_list(Workers) ]),
    nodes = #{}, queued = #{}, holds = #{},
    birth = erlang:system_time(microsecond), age = 1, graph = Graph,
    graph_monitor = erlang:monitor(process, Graph)}.

% A worker sends its node and result, then retains a remote grant until
% released. Its local client monitor also cleans it up at testcase exit.
worker(Ref, Node, Result)->
  Client = self(),
  spawn(fun()->
    ?RECEIVE(go),
    Client ! {Ref, Node, Result},
    case Result of
      {ok, {ok, Manager}}-> elock_context:hold(Manager, Ref, Client);
      _Failure-> ok
    end
  end).

% Wait until the worker's reply is in the mailbox, preserving call order.
finish_worker(Pid)->
  Pid ! go,
  ?WAIT(case process_info(Pid, current_function) of
    undefined-> true;
    {current_function, {elock_context, holding, 3}}-> true;
    _-> false
  end).

%% A lazy graph starts with context holds plus grants received before queuing,
%% then grows with later grants. Success and graph failure both retire it.
wait_verdict_spawns_graph_test(Config)->
  Scope = ?config(scope, Config),
  Parent = self(),
  Node = node(),
  lists:foreach(fun(Outcome)->
    Ref = make_ref(),
    Client = elock_test_utils:client(),
    BeforeManager = elock_test_utils:collector(),
    AfterManager = elock_test_utils:collector(),
    Holds = #{{Scope, held, Node} => Parent},
    Birth = erlang:system_time(microsecond),
    Pending = elock_test_utils:cast(Client, fun()->
      Before = worker(Ref, before_node, {ok, {ok, BeforeManager}}),
      After = worker(Ref, after_node, {ok, {ok, AfterManager}}),
      Local = worker(Ref, Node, {ok, {ok, Parent}}),
      Parent ! {lazy_workers, Client, Before, After, Local},
      elock_context:wait_verdict(Ref, #waiting{ref = Ref, scope = Scope, term = t,
        pending = #{before_node => Before, after_node => After, Node => Local},
        nodes = #{}, queued = #{}, holds = Holds, birth = Birth, age = 1})
    end),
    {lazy_workers, Client, Before, After, Local} = ?RECEIVE({lazy_workers, Client, _, _, _}),
    finish_worker(Before),
    ?WAIT(elock_test_utils:settled(Client, [Node])),
    ?assertEqual([], elock_test_utils:graph_rows(Scope, t)),
    ?assertEqual([], [P || P <- processes(), process_info(P, current_function) =:=
      {current_function, {elock_graph, graph_loop, 1}}]),
    Client ! #queued{ref = Ref, manager = Parent, node = Node},
    ExpectedHolds = Holds#{{Scope, t, before_node} => BeforeManager},
    ?WAIT(case elock_test_utils:graph_rows(Scope, t) of
      [#waiter{ref = Ref, birth = Birth, holds = ExpectedHolds}]-> true;
      _-> false
    end),
    [#waiter{client = Graph}] = elock_test_utils:graph_rows(Scope, t),
    ?assertEqual({monitors, [{process, Client}]}, process_info(Graph, monitors)),
    case Outcome of
      grant->
        finish_worker(After),
        ?WAIT(elock_test_utils:graph_rows(Scope, t) =:=
          [#waiter{lock = {Scope, t, Node}, ref = Ref, birth = Birth, client = Graph,
            holds = ExpectedHolds#{{Scope, t, after_node} => AfterManager}}]),
        finish_worker(Local),
        Nodes = #{before_node => #node{manager = BeforeManager, holder = Before},
          after_node => #node{manager = AfterManager, holder = After},
          Node => #node{manager = Parent, holder = Local}},
        ?assertEqual({ok, {ok, Nodes}}, elock_test_utils:result(Pending, ?DEADLINE)),
        elock_test_utils:call(Client, fun()-> elock_context:release(Nodes, Ref) end),
        ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(BeforeManager, 1)),
        ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(AfterManager, 1)),
        ?RECEIVE(#unlock{ref = Ref});
      deadlock->
        Graph ! #deadlock{ref = Ref, winner = ?WINNER},
        ?RECEIVE(#unlock{ref = Ref}),
        ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(BeforeManager, 1)),
        elock_test_utils:wait_dead(Before),
        ?assertEqual(timeout, elock_test_utils:result(Pending, ?QUIET)),
        % Withdrawal kills a local call worker; a remote late grant is drained.
        exit(Local, kill),
        finish_worker(After),
        ?assertEqual({ok, {error, {deadlock, ?WINNER}}},
          elock_test_utils:result(Pending, ?DEADLINE)),
        ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(AfterManager, 1))
    end,
    [elock_test_utils:wait_dead(P) || P <- [Before, After, Local, Graph]],
    ?WAIT(elock_test_utils:graph_rows(Scope, t) =:= []),
    ?assertEqual({messages, []}, process_info(Client, messages)),
    ?NO_MESSAGE
  end, [grant, deadlock]).

%% A result overtaking its queued notice must not resurrect publication or
%% remember a manager to withdraw when another node fails.
wait_verdict_queued_after_result_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  Graph = elock_test_utils:collector(),
  Manager = elock_test_utils:collector(),
  Stale = elock_test_utils:collector(),
  W1 = worker(Ref, n1, {ok, {ok, Manager}}),
  W2 = worker(Ref, n2, {error, timeout}),
  finish_worker(W1),
  self() ! #queued{ref = Ref, manager = Stale, node = n1},
  finish_worker(W2),
  ?assertEqual({error, timeout}, elock_context:wait_verdict(Ref,
    waiting(Ref, Scope, Graph, #{W1 => n1, W2 => n2}))),
  ?assertEqual([{add, Ref, n1, Manager}, {remove, Ref}],
    elock_test_utils:collected(Graph, 2)),
  ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(Manager, 1)),
  elock_test_utils:wait_dead(W1),
  ?NO_MESSAGE.
