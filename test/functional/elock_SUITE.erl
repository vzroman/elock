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
%%    tag, like the ones of run_request/3, that exit with a crafted
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

-export([
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
  queued_answered_with_held_locks_test/1,
  fake_manager_dies_test/1,
  wait_verdict_all_granted_test/1,
  wait_verdict_queued_then_granted_test/1,
  wait_verdict_granted_then_queued_test/1,
  wait_verdict_foreign_queued_left_in_mailbox_test/1,
  wait_verdict_failure_test/1,
  wait_verdict_failure_late_results_test/1,
  wait_verdict_failure_reasons_test/1,
  wait_verdict_held_queued_first_test/1,
  wait_verdict_held_queued_after_grant_test/1,
  wait_verdict_held_grant_after_queued_test/1,
  wait_verdict_held_failure_test/1,
  wait_verdict_held_lock_granted_again_test/1,
  wait_verdict_requeued_test/1,
  notify_queued_test/1,
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
  counts
}).
-record(lock,{
  scope,
  term,
  nodes
}).
-record(waiting,{
  ref,
  scope,
  term,
  held,
  pending,
  nodes,
  queued
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
      queued_answered_with_held_locks_test,
      fake_manager_dies_test
    ]},
    {multi_node_client, [], [
      wait_verdict_all_granted_test,
      wait_verdict_queued_then_granted_test,
      wait_verdict_granted_then_queued_test,
      wait_verdict_foreign_queued_left_in_mailbox_test,
      wait_verdict_failure_test,
      wait_verdict_failure_late_results_test,
      wait_verdict_failure_reasons_test,
      wait_verdict_held_queued_first_test,
      wait_verdict_held_queued_after_grant_test,
      wait_verdict_held_grant_after_queued_test,
      wait_verdict_held_failure_test,
      wait_verdict_held_lock_granted_again_test,
      wait_verdict_requeued_test,
      notify_queued_test,
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
  lists:foreach(fun(Call)->
    Pending = elock_test_utils:cast(C1, Call),
    #request{ref = Ref, tag = Tag} = ?RECEIVE(#request{}),
    C1 ! ?reply(Tag, #deadlock{ref = Ref, winner = ?WINNER}),
    ?assertEqual({ok, {error, deadlock}}, elock_test_utils:result(Pending, ?DEADLINE)),
    ?assertEqual(undefined, elock_test_utils:context(C1))
  end, Calls),
  true = ets:delete(Scope, t).

%%=================================================================
%%  Context
%%=================================================================
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
    ref2lock = #{
      Ref => #lock{
        scope = Scope,
        term = t1,
        nodes = #{ Node => Manager }
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
    nodes = #{ Node => Manager }
  },
  ?assertEqual(#context{
    ref2lock = #{ Ref1 => Lock, Ref2 => Lock },
    locked = Locked,
    counts = #{ {Scope, t1, Node} => 2 }
  }, elock_test_utils:context(C1)),
  % the key is held once as the managers see it
  ?assertEqual(1, elock_context:held_count(elock_test_utils:context(C1))),
  ?assertEqual([{t1, Manager, 2}], elock_test_utils:locks(Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual(#context{
    ref2lock = #{ Ref2 => Lock },
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
%%  The same term in two scopes is two locks: two keys, two
%%  managers; held_locks/1 is the locked map of the context itself
%%  and held_count/1 the number of its keys, without a context the
%%  map is empty and the count is 0
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
    ref2lock = #{
      Ref1 => #lock{ scope = Scope1, term = t, nodes = #{ Node => M1 } },
      Ref2 => #lock{ scope = Scope2, term = t, nodes = #{ Node => M2 } }
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
  ?assertEqual(2, elock_context:held_count(Context)),
  ?assertEqual(#{}, elock_context:held_locks(undefined)),
  ?assertEqual(0, elock_context:held_count(undefined)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual(#{ {Scope2, t, Node} => M2 }, elock_context:held_locks(elock_test_utils:context(C1))),
  ?assertEqual(1, elock_context:held_count(elock_test_utils:context(C1))),
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
  ?assertEqual(undefined, get(?CONTEXT)),

  elock_context:locked(multi_node_request(Ref, Scope, ['n1@host', 'n2@host']), #{ 'n1@host' => M1, 'n2@host' => M2 }, get(?CONTEXT)),

  ?assertEqual(#context{
    ref2lock = #{
      Ref => #lock{ scope = Scope, term = t, nodes = #{ 'n1@host' => M1, 'n2@host' => M2 } }
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
  ?assertEqual(2, elock_context:held_count(get(?CONTEXT))),

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
  elock_context:locked(multi_node_request(Ref1, Scope, Nodes), #{ 'n1@host' => M1, 'n2@host' => M2 }, get(?CONTEXT)),
  ?assertEqual(ok, elock_test_utils:stop(M1)),
  ?assertEqual(ok, elock:unlock(Ref1)),
  ?assertEqual(undefined, get(?CONTEXT)),
  ?assertEqual([#unlock{ref = Ref1}], elock_test_utils:collected(M2, 1)),
  ?NO_MESSAGE,

  % both are dead
  M3 = elock_test_utils:collector(),
  M4 = elock_test_utils:collector(),
  Ref2 = make_ref(),
  elock_context:locked(multi_node_request(Ref2, Scope, Nodes), #{ 'n1@host' => M3, 'n2@host' => M4 }, get(?CONTEXT)),
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
    ref2lock = #{ RefT2 => #lock{ scope = Scope, term = t2, nodes = #{ Node => ManagerT2 } } },
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
  Lock = #lock{ scope = s, term = t, nodes = #{ Node => M1 } },
  StaleLock = #lock{ scope = s, term = t, nodes = #{ Node => M2 } },
  OtherLock = #lock{ scope = s, term = other, nodes = #{ Node => M1 } },
  MultiLock = #lock{ scope = s, term = t2, nodes = #{ n1 => Ma, n2 => Mb } },
  N1Lock = #lock{ scope = s, term = t2, nodes = #{ n1 => Ma } },

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

%%=================================================================
%%  The request (fake manager)
%%=================================================================
%%-----------------------------------------------------------------
%%  The #request{} a client sends to the manager: every field. The
%%  tag is a reference of its own, not the ref of the request. The
%%  request carries no held map, only the number of the locks the
%%  client holds: 0 for a fresh client, then one per held key - a
%%  term managed by a real manager, the ones of the fake manager, a
%%  key held twice counts once. The defaults of lock/3, duplicated
%%  nodes collapse
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
    queue = 2,
    ref = Ref1,
    scope = Scope,
    term = t1,
    client = C1,
    proxy = C1,
    tag = Tag1,
    shared = true,
    held_count = 0,
    nodes = [Node],
    timeout = 1234
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
  #request{ref = Ref2, tag = Tag2} = Request2 = ?RECEIVE(#request{}),
  ?assertEqual(#request{
    queue = 2,
    ref = Ref2,
    scope = Scope,
    term = t2,
    client = C1,
    proxy = C1,
    tag = Tag2,
    shared = false,
    held_count = 2,
    nodes = [Node],
    timeout = undefined
  }, Request2),
  C1 ! ?reply(Tag2, #locked{ref = Ref2}),
  ?assertEqual({ok, {ok, Ref2}}, elock_test_utils:result(R2, ?DEADLINE)),

  % the second request of the same term takes the next ticket
  R3 = elock_test_utils:lock_async(C1, Scope, t2, [Node], #{}),
  #request{ref = Ref3, tag = Tag3} = Request3 = ?RECEIVE(#request{}),
  ?assertEqual(Request2#request{queue = 3, ref = Ref3, tag = Tag3, held_count = 3}, Request3),
  C1 ! ?reply(Tag3, #locked{ref = Ref3}),
  ?assertEqual({ok, {ok, Ref3}}, elock_test_utils:result(R3, ?DEADLINE)),

  % four locks over three keys: a key held twice counts once
  R4 = elock_test_utils:lock_async(C1, Scope, t1, [Node], #{}),
  #request{ref = Ref4, tag = Tag4} = Request4 = ?RECEIVE(#request{}),
  ?assertEqual(Request1#request{queue = 3, ref = Ref4, tag = Tag4, shared = false, held_count = 3, timeout = undefined}, Request4),
  C1 ! ?reply(Tag4, #timeout{ref = Ref4}),
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
  #request{ref = Ref1, queue = 2, tag = Tag1} = ?RECEIVE(#request{}),
  C1 ! ?reply(Tag1, #locked{ref = Ref1}),
  ?assertEqual({ok, {ok, Ref1}}, elock_test_utils:result(R1, ?DEADLINE)),
  ?assertEqual(#context{
    ref2lock = #{ Ref1 => #lock{ scope = Scope, term = t1, nodes = #{ Node => Self } } },
    locked = #{ {Scope, t1, Node} => Self },
    counts = #{}
  }, elock_test_utils:context(C1)),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual(#unlock{ref = Ref1}, ?RECEIVE(#unlock{})),
  ?assertEqual(undefined, elock_test_utils:context(C1)),

  % #deadlock{}
  R2 = elock_test_utils:lock_async(C1, Scope, t1, [Node], #{}),
  #request{ref = Ref2, queue = 3, tag = Tag2} = ?RECEIVE(#request{}),
  C1 ! ?reply(Tag2, #deadlock{ref = Ref2, winner = ?WINNER}),
  ?assertEqual({ok, {error, {deadlock, ?WINNER}}}, elock_test_utils:result(R2, ?DEADLINE)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),

  % #timeout{}
  R3 = elock_test_utils:lock_async(C1, Scope, t1, [Node], #{timeout => 60000}),
  #request{ref = Ref3, queue = 4, timeout = 60000, tag = Tag3} = ?RECEIVE(#request{}),
  C1 ! ?reply(Tag3, #timeout{ref = Ref3}),
  ?assertEqual({ok, {error, timeout}}, elock_test_utils:result(R3, ?DEADLINE)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),

  % #retry{}: a new ticket and a new tag, the same ref
  R4 = elock_test_utils:lock_async(C1, Scope, t1, [Node], #{}),
  #request{ref = Ref4, queue = 5, tag = Tag4} = ?RECEIVE(#request{}),
  C1 ! ?reply(Tag4, #retry{ref = Ref4}),
  #request{ref = Ref4, queue = 6, client = C1, proxy = C1, tag = Tag5} = ?RECEIVE(#request{}),
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
  #request{ref = Ref5, queue = 7, tag = Tag6} = ?RECEIVE(#request{}),
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
%%  The client answers #queued{}. A client that holds locks - one
%%  of a real manager and one of the fake manager - asks for a term
%%  of the fake manager. The request names only their number and
%%  nothing is sent until the manager asks. On #queued{} under the
%%  tag of the request the manager gets #add_held_locks{} with the
%%  ref of the request and the whole held map of the client, the
%%  locked map of its context as it is; the client keeps waiting
%%  and takes the verdict, a grant and a failure alike, its mailbox
%%  is empty afterwards
%%-----------------------------------------------------------------
queued_answered_with_held_locks_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  Self = self(),
  C1 = elock_test_utils:client(),

  % the locks of the client
  {ok, Ref0} = elock_test_utils:lock(C1, Scope, t0, [Node]),
  M0 = elock_test_utils:wait_manager(Scope, t0),
  true = ets:insert(Scope, {t1, Self, 1}),
  R1 = elock_test_utils:lock_async(C1, Scope, t1, [Node], #{is_shared => true}),
  #request{ref = Ref1, tag = Tag1, held_count = 1} = ?RECEIVE(#request{}),
  C1 ! ?reply(Tag1, #locked{ref = Ref1}),
  ?assertEqual({ok, {ok, Ref1}}, elock_test_utils:result(R1, ?DEADLINE)),
  Held = #{
    {Scope, t0, Node} => M0,
    {Scope, t1, Node} => Self
  },
  #context{locked = Locked} = Context = elock_test_utils:context(C1),
  ?assertEqual(Held, Locked),

  % the request waits at the fake manager and is asked
  true = ets:insert(Scope, {t2, Self, 1}),
  R2 = elock_test_utils:lock_async(C1, Scope, t2, [Node], #{}),
  #request{ref = Ref2, tag = Tag2} = Request2 = ?RECEIVE(#request{}),
  ?assertEqual(#request{
    queue = 2,
    ref = Ref2,
    scope = Scope,
    term = t2,
    client = C1,
    proxy = C1,
    tag = Tag2,
    shared = false,
    held_count = 2,
    nodes = [Node],
    timeout = undefined
  }, Request2),
  ?NO_MESSAGE,
  C1 ! ?reply(Tag2, #queued{ref = Ref2, manager = Self, node = Node}),
  ?assertEqual(#add_held_locks{ref = Ref2, held = Held}, ?RECEIVE(#add_held_locks{})),
  ?assertEqual(timeout, elock_test_utils:result(R2, ?QUIET)),
  ?assertEqual(Context, elock_test_utils:context(C1)),

  % the failure: nothing more to the manager, the locks of the client stay
  C1 ! ?reply(Tag2, #deadlock{ref = Ref2, winner = ?WINNER}),
  ?assertEqual({ok, {error, {deadlock, ?WINNER}}}, elock_test_utils:result(R2, ?DEADLINE)),
  ?assertEqual(Context, elock_test_utils:context(C1)),
  ?assertEqual({messages, []}, process_info(C1, messages)),
  ?NO_MESSAGE,

  % asked once more, the grant: the manager that was told the locks
  % of the client joins them
  R3 = elock_test_utils:lock_async(C1, Scope, t2, [Node], #{}),
  #request{ref = Ref3, queue = 3, tag = Tag3, held_count = 2} = ?RECEIVE(#request{}),
  C1 ! ?reply(Tag3, #queued{ref = Ref3, manager = Self, node = Node}),
  ?assertEqual(#add_held_locks{ref = Ref3, held = Held}, ?RECEIVE(#add_held_locks{})),
  ?assert(elock_test_utils:pending(R3)),
  C1 ! ?reply(Tag3, #locked{ref = Ref3}),
  ?assertEqual({ok, {ok, Ref3}}, elock_test_utils:result(R3, ?DEADLINE)),
  ?assertEqual(Held#{ {Scope, t2, Node} => Self }, elock_context:held_locks(elock_test_utils:context(C1))),
  ?assertEqual({messages, []}, process_info(C1, messages)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref3)),
  ?assertEqual(#unlock{ref = Ref3}, ?RECEIVE(#unlock{})),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual(#unlock{ref = Ref1}, ?RECEIVE(#unlock{})),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref0)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  ?NO_MESSAGE,

  true = ets:delete(Scope, t1),
  true = ets:delete(Scope, t2),
  elock_test_utils:wait_idle(Scope),
  elock_test_utils:stop(C1).

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
    receive
      #request{} = Request->
        Self ! {fake_got, self(), Request}
    end
  end),
  true = ets:insert(Scope, {t1, Fake, 1}),

  R1 = elock_test_utils:lock_async(C1, Scope, t1, [Node], #{}),
  {fake_got, Fake, #request{ref = Ref1, queue = 2}} = ?RECEIVE({fake_got, Fake, _}),
  elock_test_utils:wait_dead(Fake),

  ?assertEqual({ok, {ok, Ref1}}, elock_test_utils:result(R1, ?DEADLINE)),
  Manager = elock_test_utils:wait_manager(Scope, t1),
  ?assertNotEqual(Fake, Manager),
  ?assert(is_process_alive(Manager)),
  ?assertEqual([{t1, Manager, 1}], elock_test_utils:locks(Scope)),
  ?assertEqual(#context{
    ref2lock = #{ Ref1 => #lock{ scope = Scope, term = t1, nodes = #{ Node => Manager } } },
    locked = #{ {Scope, t1, Node} => Manager },
    counts = #{}
  }, elock_test_utils:context(C1)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  elock_test_utils:wait_idle(Scope),
  elock_test_utils:stop(C1).

%%=================================================================
%%  The client side of a multi node request (fake nodes)
%%=================================================================
%%-----------------------------------------------------------------
%%  Every node grants: {ok, #{Node => Manager}}, nothing is sent to
%%  the managers
%%-----------------------------------------------------------------
wait_verdict_all_granted_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  W1 = worker(Ref, {ok, {ok, M1}}),
  W2 = worker(Ref, {ok, {ok, M2}}),

  finish_worker(W1),
  finish_worker(W2),
  ?assertEqual({ok, #{ n1 => M1, n2 => M2 }}, elock_context:wait_verdict(Ref, waiting(Ref, Scope, #{ W1 => n1, W2 => n2 }))),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The client holds nothing. #queued{} from n2 before any grant:
%%  nothing to tell yet. Then n1 grants: the queued manager gets
%%  #add_held_locks{} with the n1 hold. Then n2 grants: ok, nothing
%%  else is sent
%%-----------------------------------------------------------------
wait_verdict_queued_then_granted_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  W1 = worker(Ref, {ok, {ok, M1}}),
  W2 = worker(Ref, {ok, {ok, M2}}),

  self() ! #queued{ref = Ref, manager = M2, node = n2},
  finish_worker(W1),
  finish_worker(W2),
  ?assertEqual({ok, #{ n1 => M1, n2 => M2 }}, elock_context:wait_verdict(Ref, waiting(Ref, Scope, #{ W1 => n1, W2 => n2 }))),

  ?assertEqual([#add_held_locks{
    ref = Ref,
    held = #{ {Scope, t, n1} => M1 }
  }], elock_test_utils:collected(M2, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The client holds nothing. n1 granted first, then #queued{} from
%%  n2: #add_held_locks{} with the n1 hold goes at once; a third
%%  node granting later is pushed to the queued manager as well
%%-----------------------------------------------------------------
wait_verdict_granted_then_queued_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  W1 = worker(Ref, {ok, {ok, M1}}),
  W2 = worker(Ref, {ok, {ok, M2}}),
  W3 = worker(Ref, {ok, {ok, M3}}),

  finish_worker(W1),
  self() ! #queued{ref = Ref, manager = M2, node = n2},
  finish_worker(W3),
  finish_worker(W2),
  ?assertEqual({ok, #{ n1 => M1, n2 => M2, n3 => M3 }},
    elock_context:wait_verdict(Ref, waiting(Ref, Scope, #{ W1 => n1, W2 => n2, W3 => n3 }))),

  ?assertEqual([
    #add_held_locks{ ref = Ref, held = #{ {Scope, t, n1} => M1 } },
    #add_held_locks{ ref = Ref, held = #{ {Scope, t, n3} => M3 } }
  ], elock_test_utils:collected(M2, 2)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  #queued{} of a foreign ref and the down message of a worker of
%%  another request (its tag is the ref of that request) are left
%%  in the mailbox and do not disturb the request
%%-----------------------------------------------------------------
wait_verdict_foreign_queued_left_in_mailbox_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  W1 = worker(Ref, {ok, {ok, M1}}),
  W2 = worker(Ref, {ok, {ok, M2}}),
  Foreign = [
    #queued{ref = make_ref(), manager = M2, node = n2},
    {make_ref(), make_ref(), process, self(), {ok, {ok, M2}}}
  ],

  [ self() ! Message || Message <- Foreign ],
  finish_worker(W1),
  self() ! #queued{ref = Ref, manager = M2, node = n2},
  finish_worker(W2),
  ?assertEqual({ok, #{ n1 => M1, n2 => M2 }}, elock_context:wait_verdict(Ref, waiting(Ref, Scope, #{ W1 => n1, W2 => n2 }))),

  ?assertEqual([#add_held_locks{ ref = Ref, held = #{ {Scope, t, n1} => M1 } }], elock_test_utils:collected(M2, 1)),
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
  M1 = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  M4 = elock_test_utils:collector(),
  M5 = elock_test_utils:collector(),
  W1 = worker(Ref, {ok, {ok, M1}}),
  W2 = worker(Ref, {error, {deadlock, ?WINNER}}),
  W4 = worker(Ref, {ok, {ok, M4}}),

  finish_worker(W1),
  self() ! #queued{ref = Ref, manager = M3, node = n3},
  finish_worker(W2),
  self() ! #queued{ref = Ref, manager = M5, node = n5},
  finish_worker(W4),
  ?assertEqual({error, {deadlock, ?WINNER}},
    elock_context:wait_verdict(Ref, waiting(Ref, Scope, #{ W1 => n1, W2 => n2, W4 => n4 }))),

  ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M1, 1)),
  ?assertEqual([
    #add_held_locks{ ref = Ref, held = #{ {Scope, t, n1} => M1 } },
    #unlock{ref = Ref}
  ], elock_test_utils:collected(M3, 2)),
  ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M5, 1)),
  ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M4, 1)),
  % the failed node has nothing to unlock
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  After the failure the late results of the other nodes: a late
%%  failure is ignored, a late grant is unlocked
%%-----------------------------------------------------------------
wait_verdict_failure_late_results_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  M3 = elock_test_utils:collector(),
  W1 = worker(Ref, {error, timeout}),
  W2 = worker(Ref, {error, {badrpc, noconnection}}),
  W3 = worker(Ref, {ok, {ok, M3}}),
  W4 = worker(Ref, {error, {deadlock, ?WINNER}}),

  finish_worker(W1),
  finish_worker(W2),
  finish_worker(W3),
  finish_worker(W4),
  ?assertEqual({error, timeout},
    elock_context:wait_verdict(Ref, waiting(Ref, Scope, #{ W1 => n1, W2 => n2, W3 => n3, W4 => n4 }))),

  ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M3, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The first non-ok result is returned as is, whatever it is: a
%%  timeout, a badrpc, a crash of the worker
%%-----------------------------------------------------------------
wait_verdict_failure_reasons_test(Config)->
  Scope = ?config(scope, Config),
  lists:foreach(
    fun(Reason)->
      Ref = make_ref(),
      M1 = elock_test_utils:collector(),
      W1 = worker(Ref, {ok, {ok, M1}}),
      W2 = worker(Ref, Reason),
      finish_worker(W1),
      finish_worker(W2),
      ?assertEqual(Reason, elock_context:wait_verdict(Ref, waiting(Ref, Scope, #{ W1 => n1, W2 => n2 }))),
      ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M1, 1)),
      ?NO_MESSAGE
    end,
    [
      {error, timeout},
      {error, {deadlock, ?WINNER}},
      {error, {badrpc, noconnection}},
      {error, {badrpc, {'EXIT', {badarg, []}}}},
      {error, {exit, killed}},
      {some, crash},
      killed
    ]
  ).

%%-----------------------------------------------------------------
%%  The client holds locks. #queued{} from n2 before any grant: the
%%  held map of the client goes to that manager at once, as it is,
%%  in a message of its own; the grant of n1 that follows is told
%%  alone. The managers of the held locks are told nothing
%%-----------------------------------------------------------------
wait_verdict_held_queued_first_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  H1 = elock_test_utils:collector(),
  H2 = elock_test_utils:collector(),
  Held = #{
    {other_scope, a, node()} => H1,
    {Scope, b, n9} => H2
  },
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  W1 = worker(Ref, {ok, {ok, M1}}),
  W2 = worker(Ref, {ok, {ok, M2}}),

  self() ! #queued{ref = Ref, manager = M2, node = n2},
  finish_worker(W1),
  finish_worker(W2),
  ?assertEqual({ok, #{ n1 => M1, n2 => M2 }},
    elock_context:wait_verdict(Ref, waiting(Ref, Scope, Held, #{ W1 => n1, W2 => n2 }))),

  ?assertEqual([
    #add_held_locks{ ref = Ref, held = Held },
    #add_held_locks{ ref = Ref, held = #{ {Scope, t, n1} => M1 } }
  ], elock_test_utils:collected(M2, 2)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The client holds locks. n1 granted first, then #queued{} from
%%  n2: the held map of the client and the grant so far go to that
%%  manager in one message; its own grant is not told to it
%%-----------------------------------------------------------------
wait_verdict_held_queued_after_grant_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  H1 = elock_test_utils:collector(),
  H2 = elock_test_utils:collector(),
  Held = #{
    {other_scope, a, node()} => H1,
    {Scope, b, n9} => H2
  },
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  W1 = worker(Ref, {ok, {ok, M1}}),
  W2 = worker(Ref, {ok, {ok, M2}}),

  finish_worker(W1),
  self() ! #queued{ref = Ref, manager = M2, node = n2},
  finish_worker(W2),
  ?assertEqual({ok, #{ n1 => M1, n2 => M2 }},
    elock_context:wait_verdict(Ref, waiting(Ref, Scope, Held, #{ W1 => n1, W2 => n2 }))),

  ?assertEqual([
    #add_held_locks{ ref = Ref, held = Held#{ {Scope, t, n1} => M1 } }
  ], elock_test_utils:collected(M2, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The client holds locks. n2 and n3 report the request queued,
%%  each manager gets the held map; a grant after that is told
%%  alone, to every manager the request is still queued at: the
%%  grant of n1 to both, the grant of n2 to the manager of n3
%%-----------------------------------------------------------------
wait_verdict_held_grant_after_queued_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  H1 = elock_test_utils:collector(),
  Held = #{ {other_scope, a, node()} => H1 },
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  W1 = worker(Ref, {ok, {ok, M1}}),
  W2 = worker(Ref, {ok, {ok, M2}}),
  W3 = worker(Ref, {ok, {ok, M3}}),

  self() ! #queued{ref = Ref, manager = M2, node = n2},
  self() ! #queued{ref = Ref, manager = M3, node = n3},
  finish_worker(W1),
  finish_worker(W2),
  finish_worker(W3),
  ?assertEqual({ok, #{ n1 => M1, n2 => M2, n3 => M3 }},
    elock_context:wait_verdict(Ref, waiting(Ref, Scope, Held, #{ W1 => n1, W2 => n2, W3 => n3 }))),

  ?assertEqual([
    #add_held_locks{ ref = Ref, held = Held },
    #add_held_locks{ ref = Ref, held = #{ {Scope, t, n1} => M1 } }
  ], elock_test_utils:collected(M2, 2)),
  ?assertEqual([
    #add_held_locks{ ref = Ref, held = Held },
    #add_held_locks{ ref = Ref, held = #{ {Scope, t, n1} => M1 } },
    #add_held_locks{ ref = Ref, held = #{ {Scope, t, n2} => M2 } }
  ], elock_test_utils:collected(M3, 3)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The failure of a request of a client that holds locks is the
%%  one of wait_verdict_failure_test: n1 granted, n3 queued - it has
%%  got the held map and the grant by then - n2 fails -> #unlock{}
%%  to the granted and to the queued manager; a #queued{} that
%%  comes after the failure is only unlocked, the held locks are of
%%  no use to it. The locks the client holds are not touched
%%-----------------------------------------------------------------
wait_verdict_held_failure_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  H1 = elock_test_utils:collector(),
  Held = #{ {other_scope, a, node()} => H1 },
  M1 = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  M4 = elock_test_utils:collector(),
  M5 = elock_test_utils:collector(),
  W1 = worker(Ref, {ok, {ok, M1}}),
  W2 = worker(Ref, {error, {deadlock, ?WINNER}}),
  W4 = worker(Ref, {ok, {ok, M4}}),

  finish_worker(W1),
  self() ! #queued{ref = Ref, manager = M3, node = n3},
  finish_worker(W2),
  self() ! #queued{ref = Ref, manager = M5, node = n5},
  finish_worker(W4),
  ?assertEqual({error, {deadlock, ?WINNER}},
    elock_context:wait_verdict(Ref, waiting(Ref, Scope, Held, #{ W1 => n1, W2 => n2, W4 => n4 }))),

  ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M1, 1)),
  ?assertEqual([
    #add_held_locks{ ref = Ref, held = Held#{ {Scope, t, n1} => M1 } },
    #unlock{ref = Ref}
  ], elock_test_utils:collected(M3, 2)),
  ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M5, 1)),
  ?assertEqual([#unlock{ref = Ref}], elock_test_utils:collected(M4, 1)),
  % nothing to the manager of the held lock
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A lock the client holds already among the grants: it holds the
%%  term on n1 and asks for it on n1 and n2. #queued{} from n2
%%  before the grant of n1: the queued manager gets the entry with
%%  the held map and once more as the grant (the graph drops the
%%  second one, see elock_graph_SUITE), the request completes. The
%%  grant of n1 before #queued{}: the entry is in the held map, the
%%  answer is that map
%%-----------------------------------------------------------------
wait_verdict_held_lock_granted_again_test(Config)->
  Scope = ?config(scope, Config),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  Held = #{ {Scope, t, n1} => M1 },

  % queued first
  Ref1 = make_ref(),
  W1 = worker(Ref1, {ok, {ok, M1}}),
  W2 = worker(Ref1, {ok, {ok, M2}}),
  self() ! #queued{ref = Ref1, manager = M2, node = n2},
  finish_worker(W1),
  finish_worker(W2),
  ?assertEqual({ok, #{ n1 => M1, n2 => M2 }},
    elock_context:wait_verdict(Ref1, waiting(Ref1, Scope, Held, #{ W1 => n1, W2 => n2 }))),

  ?assertEqual([
    #add_held_locks{ ref = Ref1, held = Held },
    #add_held_locks{ ref = Ref1, held = Held }
  ], elock_test_utils:collected(M2, 2)),
  ?NO_MESSAGE,

  % granted first
  Ref2 = make_ref(),
  W3 = worker(Ref2, {ok, {ok, M1}}),
  W4 = worker(Ref2, {ok, {ok, M2}}),
  finish_worker(W3),
  self() ! #queued{ref = Ref2, manager = M2, node = n2},
  finish_worker(W4),
  ?assertEqual({ok, #{ n1 => M1, n2 => M2 }},
    elock_context:wait_verdict(Ref2, waiting(Ref2, Scope, Held, #{ W3 => n1, W4 => n2 }))),

  ?assertEqual([#add_held_locks{ ref = Ref2, held = Held }], elock_test_utils:collected(M2, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The manager the request waits at on n2 dies and the proxy
%%  queues it at the next one: a second #queued{} from the node
%%  with another manager. The new manager is answered like the
%%  first one, with the held map and the grants so far, and takes
%%  its place: the grants that follow go to it alone
%%-----------------------------------------------------------------
wait_verdict_requeued_test(Config)->
  Scope = ?config(scope, Config),
  Ref = make_ref(),
  H1 = elock_test_utils:collector(),
  Held = #{ {other_scope, a, node()} => H1 },
  M1 = elock_test_utils:collector(),
  M2a = elock_test_utils:collector(),
  M2b = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  W1 = worker(Ref, {ok, {ok, M1}}),
  W2 = worker(Ref, {ok, {ok, M2b}}),
  W3 = worker(Ref, {ok, {ok, M3}}),

  self() ! #queued{ref = Ref, manager = M2a, node = n2},
  finish_worker(W1),
  self() ! #queued{ref = Ref, manager = M2b, node = n2},
  finish_worker(W3),
  finish_worker(W2),
  ?assertEqual({ok, #{ n1 => M1, n2 => M2b, n3 => M3 }},
    elock_context:wait_verdict(Ref, waiting(Ref, Scope, Held, #{ W1 => n1, W2 => n2, W3 => n3 }))),

  ?assertEqual([
    #add_held_locks{ ref = Ref, held = Held },
    #add_held_locks{ ref = Ref, held = #{ {Scope, t, n1} => M1 } }
  ], elock_test_utils:collected(M2a, 2)),
  ?assertEqual([
    #add_held_locks{ ref = Ref, held = Held#{ {Scope, t, n1} => M1 } },
    #add_held_locks{ ref = Ref, held = #{ {Scope, t, n3} => M3 } }
  ], elock_test_utils:collected(M2b, 2)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  notify_queued/3: every queued manager gets #add_held_locks{}
%%  with the ref and the held map as it is, a dead one is harmless;
%%  nothing is sent when nobody is queued or the map is empty
%%-----------------------------------------------------------------
notify_queued_test(_Config)->
  Ref = make_ref(),
  H1 = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  Dead = elock_test_utils:collector(),
  ok = elock_test_utils:stop(Dead),
  Held = #{ {s, a, node()} => H1, {s, t, n1} => M1 },

  ?assertEqual(ok, elock_context:notify_queued(#{}, Held, Ref)),
  ?assertEqual(ok, elock_context:notify_queued(#{ n2 => M2 }, #{}, Ref)),
  ?assertEqual(ok, elock_context:notify_queued(#{}, #{}, Ref)),
  ?NO_MESSAGE,

  ?assertEqual(ok, elock_context:notify_queued(#{ n1 => M1, n2 => M2, n3 => Dead }, Held, Ref)),
  ?assertEqual([#add_held_locks{ ref = Ref, held = Held }], elock_test_utils:collected(M1, 1)),
  ?assertEqual([#add_held_locks{ ref = Ref, held = Held }], elock_test_utils:collected(M2, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The waits of a client for its workers do not scan the messages
%%  it had before the request: the request ref made in lock/4 marks
%%  the mailbox position, it is passed on as an argument of its own
%%  and every clause of the receives of wait_verdict/2 and of
%%  wait_unlock/2 matches it, their first parameter. A refactoring
%%  that hides the ref from the compiler (the receive takes it out
%%  of #waiting{}) passes every other test and only costs the scan.
%%  No other receive of the module is reported
%%-----------------------------------------------------------------
receive_marker_test(_Config)->
  ?assertEqual([
    {{lock, 4}, reserved_receive_marker},
    {{lock, 4}, passed_marker},
    {{wait_verdict, 2}, {used_receive_marker, {parameter, 1}}},
    {{wait_unlock, 2}, {used_receive_marker, {parameter, 1}}}
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
    maps:from_list([ {Ref, #lock{ scope = Scope, term = t, nodes = #{ Node => Manager } }}
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
multi_node_request(Ref, Scope, Nodes)->
  #request{
    ref = Ref,
    scope = Scope,
    term = t,
    nodes = Nodes,
    client = self(),
    shared = false,
    held_count = 0,
    timeout = undefined
  }.

% A #waiting{} of the request Ref for the term t with the given
% pending workers: #{ Worker => Node }. Held is the held map of the
% client, empty when it is not given
waiting(Ref, Scope, Workers)->
  waiting(Ref, Scope, #{}, Workers).
waiting(Ref, Scope, Held, Workers)->
  #waiting{
    ref = Ref,
    scope = Scope,
    term = t,
    held = Held,
    pending = maps:fold(
      fun({_Pid, MonRef}, Node, Acc)-> Acc#{ MonRef => Node } end,
      #{},
      Workers
    ),
    nodes = #{},
    queued = #{}
  }.

% A worker of the request Ref that exits with Result when told to.
% It is monitored the way run_request/3 does it, with Ref as the
% tag: its down message is {Ref, MonRef, process, Pid, Result}
worker(Ref, Result)->
  spawn_opt(
    fun()->
      receive
        go-> exit(Result)
      end
    end,
    [{monitor, [{tag, Ref}]}]
  ).

% Let the worker go and wait until its down message is in the
% mailbox, so that the mailbox order is exactly the order of the calls
finish_worker({Pid, MonRef})->
  Pid ! go,
  ?WAIT(begin
    {messages, Messages} = process_info(self(), messages),
    lists:any(
      fun
        ({_Tag, M, process, P, _}) when M =:= MonRef, P =:= Pid-> true;
        (_)-> false
      end,
      Messages
    )
  end).
