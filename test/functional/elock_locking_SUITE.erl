%%=================================================================
%%  Functional tests of the lock semantics on a single node, through
%%  the client API with real clients and real managers: the modes,
%%  the order of the queue, re-entrancy and upgrades, the timeouts,
%%  the death of clients and of managers, unlock and the scope.
%%
%%  The clients are elock_test_utils clients: every lock and unlock
%%  of one logical client runs inside the same process, the lock
%%  owner. A request that has to wait is issued asynchronously
%%  (lock_async/5, or lock_queued/5 when the order in the queue
%%  matters) and its verdict is observed with result/2: no verdict
%%  within the quiet window means the request is still waiting
%%=================================================================
-module(elock_locking_SUITE).

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
  exclusive_blocks_exclusive_test/1,
  shared_shares_test/1,
  exclusive_blocks_shared_test/1,
  shared_blocks_exclusive_test/1,
  fifo_order_test/1,
  shared_batch_grant_test/1,
  newcomer_shared_does_not_overtake_test/1,
  reentrant_exclusive_test/1,
  reentrant_shared_with_exclusive_waiter_test/1,
  upgrade_test/1,
  upgrade_conflict_test/1,
  upgrade_priority_over_queue_test/1,
  exclusive_holder_requests_shared_test/1,
  manager_exits_after_last_unlock_test/1,
  different_terms_independent_test/1,
  different_scopes_independent_test/1,
  term_types_test/1,
  timeout_test/1,
  timeout_granted_first_test/1,
  timeout_shared_waiter_test/1,
  timeout_in_queue_middle_test/1,
  timeout_upgrade_test/1,
  timeout_one_ms_test/1,
  timeout_on_free_term_test/1,
  holder_dies_test/1,
  waiter_dies_test/1,
  last_holder_dies_test/1,
  shared_holder_dies_test/1,
  client_with_many_locks_dies_test/1,
  upgrading_client_dies_test/1,
  manager_killed_holder_unlocks_test/1,
  manager_killed_with_waiters_test/1,
  unlock_foreign_ref_test/1,
  unlock_after_failed_request_test/1,
  unlock_order_independent_test/1,
  ready_nodes_local_test/1,
  scope_isolation_test/1
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

% The test cases that need a second scope
-define(TWO_SCOPE_TESTS, [
  different_scopes_independent_test,
  ready_nodes_local_test,
  scope_isolation_test
]).

all()->
  [
    {group, basic},
    {group, timeouts},
    {group, client_death},
    {group, manager_crash},
    {group, unlock},
    {group, scope}
  ].

groups()->
  [
    {basic, [], [
      exclusive_blocks_exclusive_test,
      shared_shares_test,
      exclusive_blocks_shared_test,
      shared_blocks_exclusive_test,
      fifo_order_test,
      shared_batch_grant_test,
      newcomer_shared_does_not_overtake_test,
      reentrant_exclusive_test,
      reentrant_shared_with_exclusive_waiter_test,
      upgrade_test,
      upgrade_conflict_test,
      upgrade_priority_over_queue_test,
      exclusive_holder_requests_shared_test,
      manager_exits_after_last_unlock_test,
      different_terms_independent_test,
      different_scopes_independent_test,
      term_types_test
    ]},
    {timeouts, [], [
      timeout_test,
      timeout_granted_first_test,
      timeout_shared_waiter_test,
      timeout_in_queue_middle_test,
      timeout_upgrade_test,
      timeout_one_ms_test,
      timeout_on_free_term_test
    ]},
    {client_death, [], [
      holder_dies_test,
      waiter_dies_test,
      last_holder_dies_test,
      shared_holder_dies_test,
      client_with_many_locks_dies_test,
      upgrading_client_dies_test
    ]},
    {manager_crash, [], [
      manager_killed_holder_unlocks_test,
      manager_killed_with_waiters_test
    ]},
    {unlock, [], [
      unlock_foreign_ref_test,
      unlock_after_failed_request_test,
      unlock_order_independent_test
    ]},
    {scope, [], [
      ready_nodes_local_test,
      scope_isolation_test
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
%%  Every test case gets its own scope named after it, the cases of
%%  ?TWO_SCOPE_TESTS a second one as well
%%-----------------------------------------------------------------
init_per_testcase(TestCase, Config)->
  Holder = elock_test_utils:start_scope(TestCase),
  Config1 = [{scope, TestCase}, {holder, Holder} | Config],
  case lists:member(TestCase, ?TWO_SCOPE_TESTS) of
    true->
      Scope2 = list_to_atom(atom_to_list(TestCase) ++ "_second"),
      Holder2 = elock_test_utils:start_scope(Scope2),
      [{scope2, Scope2}, {holder2, Holder2} | Config1];
    false->
      Config1
  end.

%%-----------------------------------------------------------------
%%  Every client is stopped, every scope must be idle (no entry, no
%%  manager). A leak fails the test case
%%-----------------------------------------------------------------
end_per_testcase(_TestCase, Config)->
  elock_test_utils:stop_clients(),
  Results = [ elock_test_utils:finish_scope(Holder) || Holder <- [?config(holder, Config), proplists:get_value(holder2, Config)], is_pid(Holder) ],
  case [ Fail || {fail, _} = Fail <- Results ] of
    []-> ok;
    [Fail | _]-> Fail
  end.

%%=================================================================
%%  Basic
%%=================================================================
%%-----------------------------------------------------------------
%%  An exclusive holder blocks an exclusive request: the second
%%  client waits with the ticket 2 and no context, is granted by the
%%  unlock of the first and holds the lock with the same manager;
%%  its unlock leaves the scope idle
%%-----------------------------------------------------------------
exclusive_blocks_exclusive_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2] = clients(2),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node]),
  Manager = elock_test_utils:wait_manager(Scope, t),

  R2 = elock_test_utils:lock_async(C2, Scope, t, [Node], ?EXCLUSIVE),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t, Manager, 2}]),
  still_waiting(R2),
  ?assertEqual(undefined, elock_test_utils:context(C2)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref2 = granted(R2),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  ?assertEqual(#context{
    ref2lock = #{ Ref2 => #lock{ scope = Scope, term = t, nodes = #{ Node => Manager } } },
    locked = #{ {Scope, t, Node} => {Manager, 1} }
  }, elock_test_utils:context(C2)),
  ?assertEqual([{t, Manager, 2}], elock_test_utils:locks(Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  ?assertEqual(undefined, elock_test_utils:context(C2)),
  elock_test_utils:wait_idle(Scope),
  elock_test_utils:wait_dead(Manager),
  stop([C1, C2]).

%%-----------------------------------------------------------------
%%  Three shared requests hold the lock at once with one manager,
%%  every one with its own context; an exclusive request waits for
%%  all three of them and is granted by the last unlock
%%-----------------------------------------------------------------
shared_shares_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3, C4] = clients(4),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node], ?SHARED),
  Manager = elock_test_utils:wait_manager(Scope, t),
  {ok, Ref2} = elock_test_utils:lock(C2, Scope, t, [Node], ?SHARED),
  {ok, Ref3} = elock_test_utils:lock(C3, Scope, t, [Node], ?SHARED),
  ?assertEqual([{t, Manager, 3}], elock_test_utils:locks(Scope)),
  [ ?assertEqual(#context{
      ref2lock = #{ Ref => #lock{ scope = Scope, term = t, nodes = #{ Node => Manager } } },
      locked = #{ {Scope, t, Node} => {Manager, 1} }
    }, elock_test_utils:context(C)) || {C, Ref} <- [{C1, Ref1}, {C2, Ref2}, {C3, Ref3}] ],

  R4 = elock_test_utils:lock_async(C4, Scope, t, [Node], ?EXCLUSIVE),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t, Manager, 4}]),
  still_waiting(R4),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  still_waiting(R4),
  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  still_waiting(R4),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  Ref4 = granted(R4),
  ?assertEqual([{t, Manager, 4}], elock_test_utils:locks(Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C4, Ref4)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2, C3, C4]).

%%-----------------------------------------------------------------
%%  An exclusive holder blocks a shared request; once granted, the
%%  lock is shared and another shared request joins at once
%%-----------------------------------------------------------------
exclusive_blocks_shared_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3] = clients(3),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node], ?EXCLUSIVE),
  Manager = elock_test_utils:wait_manager(Scope, t),
  R2 = elock_test_utils:lock_async(C2, Scope, t, [Node], ?SHARED),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t, Manager, 2}]),
  still_waiting(R2),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref2 = granted(R2),
  {ok, Ref3} = elock_test_utils:lock(C3, Scope, t, [Node], ?SHARED),
  ?assertEqual([{t, Manager, 3}], elock_test_utils:locks(Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2, C3]).

%%-----------------------------------------------------------------
%%  A shared holder blocks an exclusive request, which is granted
%%  by the unlock and holds the lock exclusively: a shared newcomer
%%  waits for it
%%-----------------------------------------------------------------
shared_blocks_exclusive_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3] = clients(3),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node], ?SHARED),
  Manager = elock_test_utils:wait_manager(Scope, t),
  R2 = elock_test_utils:lock_async(C2, Scope, t, [Node], ?EXCLUSIVE),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t, Manager, 2}]),
  still_waiting(R2),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref2 = granted(R2),
  R3 = elock_test_utils:lock_async(C3, Scope, t, [Node], ?SHARED),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t, Manager, 3}]),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  Ref3 = granted(R3),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2, C3]).

%%-----------------------------------------------------------------
%%  The queue is FIFO by the ticket: four exclusive waiters queued
%%  in a known order are granted strictly in that order, one per
%%  unlock, the others keep waiting
%%-----------------------------------------------------------------
fifo_order_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3, C4, C5] = clients(5),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node]),
  Manager = elock_test_utils:wait_manager(Scope, t),
  R2 = elock_test_utils:lock_queued(C2, Scope, t, [Node], ?EXCLUSIVE),
  R3 = elock_test_utils:lock_queued(C3, Scope, t, [Node], ?EXCLUSIVE),
  R4 = elock_test_utils:lock_queued(C4, Scope, t, [Node], ?EXCLUSIVE),
  R5 = elock_test_utils:lock_queued(C5, Scope, t, [Node], ?EXCLUSIVE),
  ?assertEqual([{t, Manager, 5}], elock_test_utils:locks(Scope)),
  [ still_waiting(R) || R <- [R2, R3, R4, R5] ],

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref2 = granted(R2),
  [ still_waiting(R) || R <- [R3, R4, R5] ],

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  Ref3 = granted(R3),
  [ still_waiting(R) || R <- [R4, R5] ],

  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  Ref4 = granted(R4),
  still_waiting(R5),

  ?assertEqual(ok, elock_test_utils:unlock(C4, Ref4)),
  Ref5 = granted(R5),
  ?assertEqual([{t, Manager, 5}], elock_test_utils:locks(Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C5, Ref5)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2, C3, C4, C5]).

%%-----------------------------------------------------------------
%%  The queue [shared, shared, exclusive, shared] behind an
%%  exclusive holder is served in steps: the two shared together,
%%  the exclusive alone once both have left, the last shared after
%%  the exclusive
%%-----------------------------------------------------------------
shared_batch_grant_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3, C4, C5] = clients(5),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node], ?EXCLUSIVE),
  Manager = elock_test_utils:wait_manager(Scope, t),
  R2 = elock_test_utils:lock_queued(C2, Scope, t, [Node], ?SHARED),
  R3 = elock_test_utils:lock_queued(C3, Scope, t, [Node], ?SHARED),
  R4 = elock_test_utils:lock_queued(C4, Scope, t, [Node], ?EXCLUSIVE),
  R5 = elock_test_utils:lock_queued(C5, Scope, t, [Node], ?SHARED),
  ?assertEqual([{t, Manager, 5}], elock_test_utils:locks(Scope)),
  [ still_waiting(R) || R <- [R2, R3, R4, R5] ],

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref2 = granted(R2),
  Ref3 = granted(R3),
  [ still_waiting(R) || R <- [R4, R5] ],

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  [ still_waiting(R) || R <- [R4, R5] ],
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  Ref4 = granted(R4),
  still_waiting(R5),

  ?assertEqual(ok, elock_test_utils:unlock(C4, Ref4)),
  Ref5 = granted(R5),
  ?assertEqual([{t, Manager, 5}], elock_test_utils:locks(Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C5, Ref5)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2, C3, C4, C5]).

%%-----------------------------------------------------------------
%%  A shared newcomer does not overtake an exclusive waiter of a
%%  shared lock: it queues behind it and is granted after it
%%-----------------------------------------------------------------
newcomer_shared_does_not_overtake_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3] = clients(3),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node], ?SHARED),
  Manager = elock_test_utils:wait_manager(Scope, t),
  R2 = elock_test_utils:lock_queued(C2, Scope, t, [Node], ?EXCLUSIVE),
  R3 = elock_test_utils:lock_queued(C3, Scope, t, [Node], ?SHARED),
  ?assertEqual([{t, Manager, 3}], elock_test_utils:locks(Scope)),
  still_waiting(R2),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref2 = granted(R2),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  Ref3 = granted(R3),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2, C3]).

%%-----------------------------------------------------------------
%%  Re-entrant exclusive: the holder gets a second ref at once, the
%%  context counts 2 holds of the key; the lock is released only
%%  after both unlocks - the waiter is granted after the second
%%-----------------------------------------------------------------
reentrant_exclusive_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2] = clients(2),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node]),
  Manager = elock_test_utils:wait_manager(Scope, t),
  {ok, Ref2} = elock_test_utils:lock(C1, Scope, t, [Node]),
  ?assertNotEqual(Ref1, Ref2),
  Lock = #lock{ scope = Scope, term = t, nodes = #{ Node => Manager } },
  ?assertEqual(#context{
    ref2lock = #{ Ref1 => Lock, Ref2 => Lock },
    locked = #{ {Scope, t, Node} => {Manager, 2} }
  }, elock_test_utils:context(C1)),
  ?assertEqual([{t, Manager, 2}], elock_test_utils:locks(Scope)),

  R3 = elock_test_utils:lock_async(C2, Scope, t, [Node], ?EXCLUSIVE),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t, Manager, 3}]),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual(#context{
    ref2lock = #{ Ref2 => Lock },
    locked = #{ {Scope, t, Node} => {Manager, 1} }
  }, elock_test_utils:context(C1)),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref2)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  Ref3 = granted(R3),
  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref3)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2]).

%%-----------------------------------------------------------------
%%  A shared holder asking shared again is granted at once even
%%  with an exclusive waiter queued; the waiter is granted only
%%  after both of its refs are unlocked
%%-----------------------------------------------------------------
reentrant_shared_with_exclusive_waiter_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2] = clients(2),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node], ?SHARED),
  Manager = elock_test_utils:wait_manager(Scope, t),
  R2 = elock_test_utils:lock_queued(C2, Scope, t, [Node], ?EXCLUSIVE),
  still_waiting(R2),

  {ok, Ref3} = elock_test_utils:lock(C1, Scope, t, [Node], ?SHARED),
  Lock = #lock{ scope = Scope, term = t, nodes = #{ Node => Manager } },
  ?assertEqual(#context{
    ref2lock = #{ Ref1 => Lock, Ref3 => Lock },
    locked = #{ {Scope, t, Node} => {Manager, 2} }
  }, elock_test_utils:context(C1)),
  ?assertEqual([{t, Manager, 3}], elock_test_utils:locks(Scope)),
  still_waiting(R2),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref3)),
  still_waiting(R2),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref2 = granted(R2),
  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2]).

%%-----------------------------------------------------------------
%%  The upgrade: a shared holder asking exclusive waits for the
%%  other shared holder to leave, then holds both refs (the key
%%  counts 2); a shared newcomer waits behind the upgrade and while
%%  the lock is exclusive; unlocking the exclusive ref makes the
%%  lock shared again and grants the newcomer
%%-----------------------------------------------------------------
upgrade_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3] = clients(3),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node], ?SHARED),
  Manager = elock_test_utils:wait_manager(Scope, t),
  {ok, Ref2} = elock_test_utils:lock(C2, Scope, t, [Node], ?SHARED),

  Up = elock_test_utils:lock_async(C1, Scope, t, [Node], ?EXCLUSIVE),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t, Manager, 3}]),
  still_waiting(Up),

  R3 = elock_test_utils:lock_queued(C3, Scope, t, [Node], ?SHARED),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  Ref3 = granted(Up),
  Lock = #lock{ scope = Scope, term = t, nodes = #{ Node => Manager } },
  ?assertEqual(#context{
    ref2lock = #{ Ref1 => Lock, Ref3 => Lock },
    locked = #{ {Scope, t, Node} => {Manager, 2} }
  }, elock_test_utils:context(C1)),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref3)),
  Ref4 = granted(R3),
  ?assertEqual(#context{
    ref2lock = #{ Ref1 => Lock },
    locked = #{ {Scope, t, Node} => {Manager, 1} }
  }, elock_test_utils:context(C1)),
  ?assertEqual([{t, Manager, 4}], elock_test_utils:locks(Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref4)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2, C3]).

%%-----------------------------------------------------------------
%%  Two shared holders both upgrading: the second gets
%%  {error, deadlock} at once and keeps its shared lock; the first
%%  proceeds when the second unlocks its shared ref
%%-----------------------------------------------------------------
upgrade_conflict_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2] = clients(2),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node], ?SHARED),
  Manager = elock_test_utils:wait_manager(Scope, t),
  {ok, Ref2} = elock_test_utils:lock(C2, Scope, t, [Node], ?SHARED),

  Up1 = elock_test_utils:lock_async(C1, Scope, t, [Node], ?EXCLUSIVE),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t, Manager, 3}]),
  still_waiting(Up1),

  ?assertEqual({error, deadlock}, elock_test_utils:lock(C2, Scope, t, [Node], ?EXCLUSIVE)),
  Lock = #lock{ scope = Scope, term = t, nodes = #{ Node => Manager } },
  ?assertEqual(#context{
    ref2lock = #{ Ref2 => Lock },
    locked = #{ {Scope, t, Node} => {Manager, 1} }
  }, elock_test_utils:context(C2)),
  still_waiting(Up1),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  Ref3 = granted(Up1),
  ?assertEqual(#context{
    ref2lock = #{ Ref1 => Lock, Ref3 => Lock },
    locked = #{ {Scope, t, Node} => {Manager, 2} }
  }, elock_test_utils:context(C1)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref3)),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2]).

%%-----------------------------------------------------------------
%%  The pending upgrade has priority over the queue: an exclusive
%%  waiter queued before the upgrade was requested is granted only
%%  after the upgraded client has released both of its refs
%%-----------------------------------------------------------------
upgrade_priority_over_queue_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3] = clients(3),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node], ?SHARED),
  Manager = elock_test_utils:wait_manager(Scope, t),
  {ok, Ref2} = elock_test_utils:lock(C2, Scope, t, [Node], ?SHARED),
  R3 = elock_test_utils:lock_queued(C3, Scope, t, [Node], ?EXCLUSIVE),
  still_waiting(R3),

  Up = elock_test_utils:lock_async(C1, Scope, t, [Node], ?EXCLUSIVE),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t, Manager, 4}]),
  still_waiting(Up),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  Ref4 = granted(Up),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  still_waiting(R3),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref4)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  Ref3 = granted(R3),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2, C3]).

%%-----------------------------------------------------------------
%%  An exclusive holder asking shared is granted at once and the
%%  lock stays exclusive: a shared newcomer and an exclusive waiter
%%  keep waiting, also after the shared ref is unlocked; the
%%  exclusive waiter is served first once the exclusive ref goes
%%-----------------------------------------------------------------
exclusive_holder_requests_shared_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3] = clients(3),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node], ?EXCLUSIVE),
  Manager = elock_test_utils:wait_manager(Scope, t),
  R2 = elock_test_utils:lock_queued(C2, Scope, t, [Node], ?EXCLUSIVE),

  {ok, Ref3} = elock_test_utils:lock(C1, Scope, t, [Node], ?SHARED),
  Lock = #lock{ scope = Scope, term = t, nodes = #{ Node => Manager } },
  ?assertEqual(#context{
    ref2lock = #{ Ref1 => Lock, Ref3 => Lock },
    locked = #{ {Scope, t, Node} => {Manager, 2} }
  }, elock_test_utils:context(C1)),

  R4 = elock_test_utils:lock_queued(C3, Scope, t, [Node], ?SHARED),
  ?assertEqual([{t, Manager, 4}], elock_test_utils:locks(Scope)),
  still_waiting(R2),
  still_waiting(R4),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref3)),
  still_waiting(R2),
  still_waiting(R4),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref2 = granted(R2),
  still_waiting(R4),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  Ref4 = granted(R4),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref4)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2, C3]).

%%-----------------------------------------------------------------
%%  The last unlock releases the term: the entry is deleted and the
%%  manager exits normally; the next lock of the term starts a new
%%  manager with the ticket 1
%%-----------------------------------------------------------------
manager_exits_after_last_unlock_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2] = clients(2),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node]),
  Manager1 = elock_test_utils:wait_manager(Scope, t),
  MonRef = erlang:monitor(process, Manager1),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual({'DOWN', MonRef, process, Manager1, normal}, ?RECEIVE({'DOWN', MonRef, process, Manager1, _})),
  ?assertEqual([], elock_test_utils:locks(Scope)),
  ?assertEqual([], elock_test_utils:managers()),

  {ok, Ref2} = elock_test_utils:lock(C2, Scope, t, [Node]),
  Manager2 = elock_test_utils:wait_manager(Scope, t),
  ?assertNotEqual(Manager1, Manager2),
  ?assertEqual([{t, Manager2, 1}], elock_test_utils:locks(Scope)),
  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2]).

%%-----------------------------------------------------------------
%%  Different terms are independent locks: each has its own manager
%%  and entry, a waiter of one term is not affected by the unlock
%%  of the other
%%-----------------------------------------------------------------
different_terms_independent_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3] = clients(3),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t1, [Node]),
  {ok, Ref2} = elock_test_utils:lock(C2, Scope, t2, [Node]),
  M1 = elock_test_utils:wait_manager(Scope, t1),
  M2 = elock_test_utils:wait_manager(Scope, t2),
  ?assertNotEqual(M1, M2),
  ?assertEqual([{t1, M1, 1}, {t2, M2, 1}], lists:sort(elock_test_utils:locks(Scope))),

  R3 = elock_test_utils:lock_async(C3, Scope, t1, [Node], ?EXCLUSIVE),
  ?WAIT(lists:sort(elock_test_utils:locks(Scope)) =:= [{t1, M1, 2}, {t2, M2, 1}]),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t1, M1, 2}]),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref3 = granted(R3),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2, C3]).

%%-----------------------------------------------------------------
%%  The same term in two scopes is two locks: one client holds both
%%  with two managers and two keys in its context, a waiter in one
%%  scope is not affected by the unlock in the other
%%-----------------------------------------------------------------
different_scopes_independent_test(Config)->
  Scope1 = ?config(scope, Config),
  Scope2 = ?config(scope2, Config),
  Node = node(),
  [C1, C2] = clients(2),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope1, t, [Node]),
  {ok, Ref2} = elock_test_utils:lock(C1, Scope2, t, [Node]),
  M1 = elock_test_utils:wait_manager(Scope1, t),
  M2 = elock_test_utils:wait_manager(Scope2, t),
  ?assertNotEqual(M1, M2),
  ?assertEqual(#context{
    ref2lock = #{
      Ref1 => #lock{ scope = Scope1, term = t, nodes = #{ Node => M1 } },
      Ref2 => #lock{ scope = Scope2, term = t, nodes = #{ Node => M2 } }
    },
    locked = #{
      {Scope1, t, Node} => {M1, 1},
      {Scope2, t, Node} => {M2, 1}
    }
  }, elock_test_utils:context(C1)),

  R3 = elock_test_utils:lock_async(C2, Scope1, t, [Node], ?EXCLUSIVE),
  ?WAIT(elock_test_utils:locks(Scope1) =:= [{t, M1, 2}]),
  ?assertEqual([{t, M2, 1}], elock_test_utils:locks(Scope2)),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref2)),
  ?WAIT(elock_test_utils:locks(Scope2) =:= []),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref3 = granted(R3),
  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref3)),
  elock_test_utils:wait_idle(Scope1),
  stop([C1, C2]).

%%-----------------------------------------------------------------
%%  Any term is a lock: a tuple, a binary, a list, a map, a pid, a
%%  ref, an atom, an integer and a large term, each with its own
%%  manager; 1 and 1.0 are different locks (the set table keys them
%%  by =:=): 1.0 is free while 1 is held
%%-----------------------------------------------------------------
term_types_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2] = clients(2),
  Terms = [
    {tuple, 1, two},
    <<"binary">>,
    "list",
    #{ key => value },
    self(),
    make_ref(),
    atom,
    1,
    {large, lists:seq(1, 50000)}
  ],

  Locks =
    [ begin
        {ok, Ref} = elock_test_utils:lock(C1, Scope, Term, [Node]),
        {Term, Ref}
      end || Term <- Terms ],
  Managers = [ elock_test_utils:wait_manager(Scope, Term) || Term <- Terms ],
  ?assertEqual(length(Terms), length(lists:usort(Managers))),
  ?assertEqual(length(Terms), length(elock_test_utils:locks(Scope))),
  [ ?assertEqual([{Term, Manager, 1}], ets:lookup(Scope, Term)) || {Term, Manager} <- lists:zip(Terms, Managers) ],
  #context{ locked = Locked } = elock_test_utils:context(C1),
  ?assertEqual(
    maps:from_list([ {{Scope, Term, Node}, {Manager, 1}} || {Term, Manager} <- lists:zip(Terms, Managers) ]),
    Locked
  ),

  % 1.0 is another lock than 1
  {ok, RefFloat} = elock_test_utils:lock(C2, Scope, 1.0, [Node]),
  MFloat = elock_test_utils:wait_manager(Scope, 1.0),
  ?assertEqual([{1.0, MFloat, 1}], ets:lookup(Scope, 1.0)),
  ?assertEqual(length(Terms) + 1, length(elock_test_utils:locks(Scope))),
  RInt = elock_test_utils:lock_async(C2, Scope, 1, [Node], ?EXCLUSIVE),
  still_waiting(RInt),

  [ ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)) || {_Term, Ref} <- Locks ],
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  RefInt = granted(RInt),
  ?WAIT(length(elock_test_utils:locks(Scope)) =:= 2),
  ?assertEqual(ok, elock_test_utils:unlock(C2, RefInt)),
  ?assertEqual(ok, elock_test_utils:unlock(C2, RefFloat)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2]).

%%=================================================================
%%  Timeouts
%%=================================================================
%%-----------------------------------------------------------------
%%  A waiter with a timeout gets {error, timeout} within
%%  [Timeout, Timeout + 500] ms, has no context, is dropped by the
%%  manager (not monitored any more); the scope is idle after the
%%  holder unlocks and the term is free for the client that timed
%%  out
%%-----------------------------------------------------------------
timeout_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2] = clients(2),
  Timeout = 300,

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node]),
  Manager = elock_test_utils:wait_manager(Scope, t),

  R2 = timed_lock(C2, Scope, t, [Node], #{timeout => Timeout}),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t, Manager, 2}]),
  ?WAIT(monitors(Manager, C2)),
  {ok, {{error, timeout}, Elapsed}} = elock_test_utils:result(R2, ?DEADLINE),
  elapsed_within(Elapsed, Timeout),
  ?assertEqual(undefined, elock_test_utils:context(C2)),
  ?WAIT(not monitors(Manager, C2)),
  ?assertEqual([{t, Manager, 2}], elock_test_utils:locks(Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  elock_test_utils:wait_idle(Scope),

  {ok, Ref2} = elock_test_utils:lock(C2, Scope, t, [Node], #{timeout => Timeout}),
  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2]).

%%-----------------------------------------------------------------
%%  A waiter granted before its timeout keeps the lock past the
%%  timeout: a later exclusive request is still waiting after the
%%  timeout has elapsed and is granted by the unlock only
%%-----------------------------------------------------------------
timeout_granted_first_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3] = clients(3),
  Timeout = 300,

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node]),
  Manager = elock_test_utils:wait_manager(Scope, t),
  R2 = elock_test_utils:lock_async(C2, Scope, t, [Node], #{timeout => Timeout}),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t, Manager, 2}]),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref2 = granted(R2),

  R3 = elock_test_utils:lock_async(C3, Scope, t, [Node], ?EXCLUSIVE),
  ?assertEqual(timeout, elock_test_utils:result(R3, Timeout + 200)),
  ?assertEqual(#context{
    ref2lock = #{ Ref2 => #lock{ scope = Scope, term = t, nodes = #{ Node => Manager } } },
    locked = #{ {Scope, t, Node} => {Manager, 1} }
  }, elock_test_utils:context(C2)),
  ?assertEqual([{t, Manager, 3}], elock_test_utils:locks(Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  Ref3 = granted(R3),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2, C3]).

%%-----------------------------------------------------------------
%%  A shared waiter behind an exclusive holder times out; the shared
%%  waiter behind it without a timeout stays and is granted by the
%%  unlock
%%-----------------------------------------------------------------
timeout_shared_waiter_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3] = clients(3),
  Timeout = 200,

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node], ?EXCLUSIVE),
  Manager = elock_test_utils:wait_manager(Scope, t),
  R2 = elock_test_utils:lock_queued(C2, Scope, t, [Node], #{is_shared => true, timeout => Timeout}),
  R3 = elock_test_utils:lock_queued(C3, Scope, t, [Node], ?SHARED),
  ?assertEqual([{t, Manager, 3}], elock_test_utils:locks(Scope)),

  ?assertEqual({ok, {error, timeout}}, elock_test_utils:result(R2, ?DEADLINE)),
  ?assertEqual(undefined, elock_test_utils:context(C2)),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref3 = granted(R3),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2, C3]).

%%-----------------------------------------------------------------
%%  A timed out request in the middle of the queue is skipped: the
%%  order of the others is kept
%%-----------------------------------------------------------------
timeout_in_queue_middle_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3, C4] = clients(4),
  Timeout = 200,

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node]),
  Manager = elock_test_utils:wait_manager(Scope, t),
  R2 = elock_test_utils:lock_queued(C2, Scope, t, [Node], ?EXCLUSIVE),
  R3 = elock_test_utils:lock_queued(C3, Scope, t, [Node], #{timeout => Timeout}),
  R4 = elock_test_utils:lock_queued(C4, Scope, t, [Node], ?EXCLUSIVE),
  ?assertEqual([{t, Manager, 4}], elock_test_utils:locks(Scope)),

  ?assertEqual({ok, {error, timeout}}, elock_test_utils:result(R3, ?DEADLINE)),
  still_waiting(R2),
  still_waiting(R4),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref2 = granted(R2),
  still_waiting(R4),
  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  Ref4 = granted(R4),
  ?assertEqual(ok, elock_test_utils:unlock(C4, Ref4)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2, C3, C4]).

%%-----------------------------------------------------------------
%%  An upgrade with a timeout times out while the other shared
%%  holder stays; the client keeps its shared lock and upgrades at
%%  once when it is the only holder left
%%-----------------------------------------------------------------
timeout_upgrade_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2] = clients(2),
  Timeout = 200,

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node], ?SHARED),
  Manager = elock_test_utils:wait_manager(Scope, t),
  {ok, Ref2} = elock_test_utils:lock(C2, Scope, t, [Node], ?SHARED),

  R3 = timed_lock(C1, Scope, t, [Node], #{timeout => Timeout}),
  {ok, {{error, timeout}, Elapsed}} = elock_test_utils:result(R3, ?DEADLINE),
  elapsed_within(Elapsed, Timeout),
  Lock = #lock{ scope = Scope, term = t, nodes = #{ Node => Manager } },
  ?assertEqual(#context{
    ref2lock = #{ Ref1 => Lock },
    locked = #{ {Scope, t, Node} => {Manager, 1} }
  }, elock_test_utils:context(C1)),
  ?assertEqual([{t, Manager, 3}], elock_test_utils:locks(Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  {ok, Ref4} = elock_test_utils:lock(C1, Scope, t, [Node], #{timeout => Timeout}),
  ?assertEqual(#context{
    ref2lock = #{ Ref1 => Lock, Ref4 => Lock },
    locked = #{ {Scope, t, Node} => {Manager, 2} }
  }, elock_test_utils:context(C1)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref4)),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2]).

%%-----------------------------------------------------------------
%%  The smallest timeout: 1 ms times out, five times in a row, each
%%  within the window
%%-----------------------------------------------------------------
timeout_one_ms_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2] = clients(2),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node]),
  Manager = elock_test_utils:wait_manager(Scope, t),
  lists:foreach(
    fun(_)->
      R = timed_lock(C2, Scope, t, [Node], #{timeout => 1}),
      {ok, {{error, timeout}, Elapsed}} = elock_test_utils:result(R, ?DEADLINE),
      elapsed_within(Elapsed, 1),
      ?assertEqual(undefined, elock_test_utils:context(C2))
    end,
    lists:seq(1, 5)
  ),
  ?assertEqual([{t, Manager, 6}], elock_test_utils:locks(Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2]).

%%-----------------------------------------------------------------
%%  A timeout on a free term: granted at once, the timer never runs
%%  - the lock is kept long past the timeout
%%-----------------------------------------------------------------
timeout_on_free_term_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2] = clients(2),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node], #{timeout => 1}),
  Manager = elock_test_utils:wait_manager(Scope, t),
  R2 = elock_test_utils:lock_async(C2, Scope, t, [Node], ?EXCLUSIVE),
  ?assertEqual(timeout, elock_test_utils:result(R2, 300)),
  ?assertEqual([{t, Manager, 2}], elock_test_utils:locks(Scope)),
  ?assertEqual(#context{
    ref2lock = #{ Ref1 => #lock{ scope = Scope, term = t, nodes = #{ Node => Manager } } },
    locked = #{ {Scope, t, Node} => {Manager, 1} }
  }, elock_test_utils:context(C1)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref2 = granted(R2),
  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2]).

%%=================================================================
%%  Client death
%%=================================================================
%%-----------------------------------------------------------------
%%  The holder dies: the waiter is granted by the same manager
%%-----------------------------------------------------------------
holder_dies_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2] = clients(2),

  {ok, _Ref1} = elock_test_utils:lock(C1, Scope, t, [Node]),
  Manager = elock_test_utils:wait_manager(Scope, t),
  R2 = elock_test_utils:lock_queued(C2, Scope, t, [Node], ?EXCLUSIVE),
  still_waiting(R2),

  ?assertEqual(ok, elock_test_utils:stop(C1)),
  Ref2 = granted(R2),
  ?assertEqual([{t, Manager, 2}], elock_test_utils:locks(Scope)),
  ?assert(is_process_alive(Manager)),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  elock_test_utils:wait_idle(Scope),
  stop([C2]).

%%-----------------------------------------------------------------
%%  A waiter dies: the manager drops it (no monitor any more), the
%%  waiter behind it is granted by the unlock of the holder
%%-----------------------------------------------------------------
waiter_dies_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3] = clients(3),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node]),
  Manager = elock_test_utils:wait_manager(Scope, t),
  _R2 = elock_test_utils:lock_queued(C2, Scope, t, [Node], ?EXCLUSIVE),
  R3 = elock_test_utils:lock_queued(C3, Scope, t, [Node], ?EXCLUSIVE),
  ?assert(monitors(Manager, C2)),

  ?assertEqual(ok, elock_test_utils:stop(C2)),
  ?WAIT(not monitors(Manager, C2)),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref3 = granted(R3),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C3]).

%%-----------------------------------------------------------------
%%  The only holder dies: the term is released without any unlock,
%%  the manager exits, the next client starts a new one
%%-----------------------------------------------------------------
last_holder_dies_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2] = clients(2),

  {ok, _Ref1} = elock_test_utils:lock(C1, Scope, t, [Node]),
  Manager1 = elock_test_utils:wait_manager(Scope, t),
  MonRef = erlang:monitor(process, Manager1),

  ?assertEqual(ok, elock_test_utils:stop(C1)),
  ?assertEqual({'DOWN', MonRef, process, Manager1, normal}, ?RECEIVE({'DOWN', MonRef, process, Manager1, _})),
  elock_test_utils:wait_idle(Scope),

  {ok, Ref2} = elock_test_utils:lock(C2, Scope, t, [Node]),
  Manager2 = elock_test_utils:wait_manager(Scope, t),
  ?assertNotEqual(Manager1, Manager2),
  ?assertEqual([{t, Manager2, 1}], elock_test_utils:locks(Scope)),
  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  elock_test_utils:wait_idle(Scope),
  stop([C2]).

%%-----------------------------------------------------------------
%%  One of two shared holders dies: the exclusive waiter still waits
%%  for the other; when that one dies too the waiter is granted
%%-----------------------------------------------------------------
shared_holder_dies_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3] = clients(3),

  {ok, _Ref1} = elock_test_utils:lock(C1, Scope, t, [Node], ?SHARED),
  Manager = elock_test_utils:wait_manager(Scope, t),
  {ok, _Ref2} = elock_test_utils:lock(C2, Scope, t, [Node], ?SHARED),
  R3 = elock_test_utils:lock_queued(C3, Scope, t, [Node], ?EXCLUSIVE),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:stop(C1)),
  ?WAIT(not monitors(Manager, C1)),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:stop(C2)),
  Ref3 = granted(R3),
  ?assertEqual([{t, Manager, 3}], elock_test_utils:locks(Scope)),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  elock_test_utils:wait_idle(Scope),
  stop([C3]).

%%-----------------------------------------------------------------
%%  A client holding five terms and waiting for a sixth dies: the
%%  waiter of one of its terms is granted, the other four terms are
%%  released (their managers exit), its waiting request is dropped
%%  by the sixth manager - the unlock of that holder makes the term
%%  free
%%-----------------------------------------------------------------
client_with_many_locks_dies_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3] = clients(3),
  Held = [t1, t2, t3, t4, t5],

  {ok, Ref6} = elock_test_utils:lock(C2, Scope, t6, [Node]),
  M6 = elock_test_utils:wait_manager(Scope, t6),
  [ {ok, _} = elock_test_utils:lock(C1, Scope, Term, [Node]) || Term <- Held ],
  M3 = elock_test_utils:wait_manager(Scope, t3),
  ?assertEqual(6, length(elock_test_utils:locks(Scope))),
  ?assertEqual(6, length(elock_test_utils:managers())),

  R1 = elock_test_utils:lock_queued(C1, Scope, t6, [Node], ?EXCLUSIVE),
  R3 = elock_test_utils:lock_queued(C3, Scope, t3, [Node], ?EXCLUSIVE),
  still_waiting(R1),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:stop(C1)),
  Ref3 = granted(R3),
  ?WAIT(lists:sort(elock_test_utils:locks(Scope)) =:= [{t3, M3, 2}, {t6, M6, 2}]),
  ?WAIT(lists:sort(elock_test_utils:managers()) =:= lists:sort([M3, M6])),
  ?WAIT(not monitors(M6, C1)),
  ?assert(elock_test_utils:pending(R1)),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref6)),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t3, M3, 2}]),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  elock_test_utils:wait_idle(Scope),
  stop([C2, C3]).

%%-----------------------------------------------------------------
%%  A shared holder dies while its upgrade is pending: the upgrade
%%  and the hold are dropped, the shared newcomer that waited behind
%%  the upgrade joins the remaining shared holder
%%-----------------------------------------------------------------
upgrading_client_dies_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3] = clients(3),

  {ok, _Ref1} = elock_test_utils:lock(C1, Scope, t, [Node], ?SHARED),
  Manager = elock_test_utils:wait_manager(Scope, t),
  {ok, Ref2} = elock_test_utils:lock(C2, Scope, t, [Node], ?SHARED),
  Up = elock_test_utils:lock_async(C1, Scope, t, [Node], ?EXCLUSIVE),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t, Manager, 3}]),
  still_waiting(Up),
  R3 = elock_test_utils:lock_queued(C3, Scope, t, [Node], ?SHARED),
  still_waiting(R3),

  ?assertEqual(ok, elock_test_utils:stop(C1)),
  Ref3 = granted(R3),
  ?WAIT(not monitors(Manager, C1)),
  ?assertEqual([{t, Manager, 4}], elock_test_utils:locks(Scope)),

  % the lock is shared: one more shared joins at once
  {ok, Ref4} = elock_test_utils:lock(C3, Scope, t, [Node], ?SHARED),
  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref4)),
  elock_test_utils:wait_idle(Scope),
  stop([C2, C3]).

%%=================================================================
%%  Manager crash
%%=================================================================
%%-----------------------------------------------------------------
%%  The manager of a held term is killed: the entry stays behind
%%  with the dead pid, the holder's unlock is a harmless no-op that
%%  cleans its context; the next client finds the dead manager,
%%  deletes the entry and starts a new manager with the ticket 1
%%-----------------------------------------------------------------
manager_killed_holder_unlocks_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2] = clients(2),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node]),
  Manager1 = elock_test_utils:wait_manager(Scope, t),
  exit(Manager1, kill),
  elock_test_utils:wait_dead(Manager1),
  ?assertEqual([{t, Manager1, 1}], elock_test_utils:locks(Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  ?assert(is_process_alive(C1)),
  ?assertEqual([{t, Manager1, 1}], elock_test_utils:locks(Scope)),

  {ok, Ref2} = elock_test_utils:lock(C2, Scope, t, [Node]),
  Manager2 = elock_test_utils:wait_manager(Scope, t),
  ?assertNotEqual(Manager1, Manager2),
  ?assertEqual([{t, Manager2, 1}], elock_test_utils:locks(Scope)),
  ?assertEqual(#context{
    ref2lock = #{ Ref2 => #lock{ scope = Scope, term = t, nodes = #{ Node => Manager2 } } },
    locked = #{ {Scope, t, Node} => {Manager2, 1} }
  }, elock_test_utils:context(C2)),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2]).

%%-----------------------------------------------------------------
%%  The manager is killed with two waiters queued: both retry, one
%%  becomes the first holder of the new manager, the other waits
%%  behind it; the old holder's unlock is a no-op; the scope is idle
%%  after both unlock
%%-----------------------------------------------------------------
manager_killed_with_waiters_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3] = clients(3),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node]),
  Manager1 = elock_test_utils:wait_manager(Scope, t),
  R2 = elock_test_utils:lock_queued(C2, Scope, t, [Node], ?EXCLUSIVE),
  R3 = elock_test_utils:lock_queued(C3, Scope, t, [Node], ?EXCLUSIVE),
  ?assertEqual([{t, Manager1, 3}], elock_test_utils:locks(Scope)),

  exit(Manager1, kill),
  elock_test_utils:wait_dead(Manager1),
  {First, {ok, FirstRef}} = any_result([R2, R3], ?DEADLINE),
  [Second] = [R2, R3] -- [First],
  still_waiting(Second),
  Manager2 = elock_test_utils:wait_manager(Scope, t),
  ?assertNotEqual(Manager1, Manager2),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t, Manager2, 2}]),
  ?assertEqual([Manager2], elock_test_utils:managers()),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  still_waiting(Second),

  {FirstClient, SecondClient} =
    if
      First =:= R2-> {C2, C3};
      true-> {C3, C2}
    end,
  ?assertEqual(ok, elock_test_utils:unlock(FirstClient, FirstRef)),
  SecondRef = granted(Second),
  ?assertEqual([{t, Manager2, 2}], elock_test_utils:locks(Scope)),
  ?assertEqual(ok, elock_test_utils:unlock(SecondClient, SecondRef)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2, C3]).

%%=================================================================
%%  Unlock
%%=================================================================
%%-----------------------------------------------------------------
%%  unlock/1 with a foreign ref (of another client) or an unknown
%%  ref is a no-op: ok, the holder keeps the lock and its context,
%%  the waiter keeps waiting
%%-----------------------------------------------------------------
unlock_foreign_ref_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2, C3] = clients(3),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t, [Node]),
  Manager = elock_test_utils:wait_manager(Scope, t),
  Context = elock_test_utils:context(C1),
  R3 = elock_test_utils:lock_async(C3, Scope, t, [Node], ?EXCLUSIVE),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t, Manager, 2}]),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref1)),
  ?assertEqual(undefined, elock_test_utils:context(C2)),
  ?assertEqual(ok, elock_test_utils:unlock(C1, make_ref())),
  ?assertEqual(ok, elock:unlock(Ref1)),
  ?assertEqual(Context, elock_test_utils:context(C1)),
  still_waiting(R3),
  ?assertEqual([{t, Manager, 2}], elock_test_utils:locks(Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref3 = granted(R3),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2, C3]).

%%-----------------------------------------------------------------
%%  A failed request (timeout, deadlock on a second upgrade) leaves
%%  nothing to unlock: no context, the client's other locks are not
%%  affected and are unlocked normally
%%-----------------------------------------------------------------
unlock_after_failed_request_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1, C2] = clients(2),

  % a timeout
  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t1, [Node]),
  M1 = elock_test_utils:wait_manager(Scope, t1),
  ?assertEqual({error, timeout}, elock_test_utils:lock(C2, Scope, t1, [Node], #{timeout => 100})),
  ?assertEqual(undefined, elock_test_utils:context(C2)),
  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref1)),
  ?assertEqual(undefined, elock_test_utils:context(C2)),

  % a deadlock: the second upgrade of a shared lock
  {ok, Ref2} = elock_test_utils:lock(C1, Scope, t2, [Node], ?SHARED),
  {ok, Ref3} = elock_test_utils:lock(C2, Scope, t2, [Node], ?SHARED),
  M2 = elock_test_utils:wait_manager(Scope, t2),
  Up = elock_test_utils:lock_async(C1, Scope, t2, [Node], ?EXCLUSIVE),
  ?WAIT(lists:sort(elock_test_utils:locks(Scope)) =:= [{t1, M1, 2}, {t2, M2, 3}]),
  still_waiting(Up),
  ?assertEqual({error, deadlock}, elock_test_utils:lock(C2, Scope, t2, [Node], ?EXCLUSIVE)),
  ?assertEqual(#context{
    ref2lock = #{ Ref3 => #lock{ scope = Scope, term = t2, nodes = #{ Node => M2 } } },
    locked = #{ {Scope, t2, Node} => {M2, 1} }
  }, elock_test_utils:context(C2)),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref3)),
  ?assertEqual(undefined, elock_test_utils:context(C2)),
  Ref4 = granted(Up),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref4)),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref2)),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  elock_test_utils:wait_idle(Scope),
  stop([C1, C2]).

%%-----------------------------------------------------------------
%%  The refs of three terms and two re-entrant holds are unlocked in
%%  an order other than the one they were taken in: after every
%%  unlock the context holds exactly the remaining refs and the
%%  released terms are gone from the table
%%-----------------------------------------------------------------
unlock_order_independent_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [C1] = clients(1),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope, t1, [Node]),
  {ok, Ref2} = elock_test_utils:lock(C1, Scope, t2, [Node]),
  {ok, Ref3} = elock_test_utils:lock(C1, Scope, t3, [Node]),
  {ok, Ref4} = elock_test_utils:lock(C1, Scope, t1, [Node]),
  M1 = elock_test_utils:wait_manager(Scope, t1),
  M2 = elock_test_utils:wait_manager(Scope, t2),
  M3 = elock_test_utils:wait_manager(Scope, t3),
  L1 = #lock{ scope = Scope, term = t1, nodes = #{ Node => M1 } },
  L2 = #lock{ scope = Scope, term = t2, nodes = #{ Node => M2 } },
  L3 = #lock{ scope = Scope, term = t3, nodes = #{ Node => M3 } },
  ?assertEqual(#context{
    ref2lock = #{ Ref1 => L1, Ref2 => L2, Ref3 => L3, Ref4 => L1 },
    locked = #{ {Scope, t1, Node} => {M1, 2}, {Scope, t2, Node} => {M2, 1}, {Scope, t3, Node} => {M3, 1} }
  }, elock_test_utils:context(C1)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref2)),
  ?assertEqual(#context{
    ref2lock = #{ Ref1 => L1, Ref3 => L3, Ref4 => L1 },
    locked = #{ {Scope, t1, Node} => {M1, 2}, {Scope, t3, Node} => {M3, 1} }
  }, elock_test_utils:context(C1)),
  ?WAIT(lists:sort(elock_test_utils:locks(Scope)) =:= [{t1, M1, 2}, {t3, M3, 1}]),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  ?assertEqual(#context{
    ref2lock = #{ Ref3 => L3, Ref4 => L1 },
    locked = #{ {Scope, t1, Node} => {M1, 1}, {Scope, t3, Node} => {M3, 1} }
  }, elock_test_utils:context(C1)),
  ?assertEqual([{t1, M1, 2}, {t3, M3, 1}], lists:sort(elock_test_utils:locks(Scope))),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref3)),
  ?assertEqual(#context{
    ref2lock = #{ Ref4 => L1 },
    locked = #{ {Scope, t1, Node} => {M1, 1} }
  }, elock_test_utils:context(C1)),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{t1, M1, 2}]),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref4)),
  ?assertEqual(undefined, elock_test_utils:context(C1)),
  elock_test_utils:wait_idle(Scope),
  stop([C1]).

%%=================================================================
%%  Scope
%%=================================================================
%%-----------------------------------------------------------------
%%  ready_nodes/1 of a local scope is [node()], for each of two
%%  scopes; an unknown scope has none, a stopped scope none
%%-----------------------------------------------------------------
ready_nodes_local_test(Config)->
  Scope1 = ?config(scope, Config),
  Scope2 = ?config(scope2, Config),
  Holder2 = ?config(holder2, Config),
  Node = node(),

  ?assertEqual([Node], elock:ready_nodes(Scope1)),
  ?assertEqual([Node], elock:ready_nodes(Scope2)),
  ?assertEqual([], elock:ready_nodes(unknown_scope_of_ready_nodes_local_test)),

  ?assertEqual(ok, elock_test_utils:stop_scope(Holder2)),
  ?assertEqual([], elock:ready_nodes(Scope2)),
  ?assertEqual([Node], elock:ready_nodes(Scope1)).

%%-----------------------------------------------------------------
%%  Two scopes with the same term have independent queues: the
%%  unlock in one scope grants the waiter of that scope only, the
%%  tables are separate
%%-----------------------------------------------------------------
scope_isolation_test(Config)->
  Scope1 = ?config(scope, Config),
  Scope2 = ?config(scope2, Config),
  Node = node(),
  [C1, C2, C3, C4] = clients(4),

  {ok, Ref1} = elock_test_utils:lock(C1, Scope1, t, [Node]),
  {ok, Ref2} = elock_test_utils:lock(C2, Scope2, t, [Node]),
  M1 = elock_test_utils:wait_manager(Scope1, t),
  M2 = elock_test_utils:wait_manager(Scope2, t),
  R3 = elock_test_utils:lock_async(C3, Scope1, t, [Node], ?EXCLUSIVE),
  R4 = elock_test_utils:lock_async(C4, Scope2, t, [Node], ?EXCLUSIVE),
  ?WAIT(elock_test_utils:locks(Scope1) =:= [{t, M1, 2}]),
  ?WAIT(elock_test_utils:locks(Scope2) =:= [{t, M2, 2}]),
  still_waiting(R3),
  still_waiting(R4),

  ?assertEqual(ok, elock_test_utils:unlock(C2, Ref2)),
  Ref4 = granted(R4),
  still_waiting(R3),
  ?assertEqual([{t, M1, 2}], elock_test_utils:locks(Scope1)),

  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref1)),
  Ref3 = granted(R3),
  ?assertEqual(ok, elock_test_utils:unlock(C3, Ref3)),
  ?assertEqual(ok, elock_test_utils:unlock(C4, Ref4)),
  elock_test_utils:wait_idle(Scope1),
  elock_test_utils:wait_idle(Scope2),
  stop([C1, C2, C3, C4]).

%%=================================================================
%%  Utilities
%%=================================================================
% N clients
clients(N)->
  [ elock_test_utils:client() || _ <- lists:seq(1, N) ].

stop(Clients)->
  [ elock_test_utils:stop(Client) || Client <- Clients ],
  ok.

% The verdict of an asynchronous request: granted within the deadline
granted(R)->
  {ok, {ok, LockRef}} = elock_test_utils:result(R, ?DEADLINE),
  LockRef.

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

% Does the manager monitor the client, i.e. has it a request of it
monitors(Manager, Client)->
  case process_info(Manager, monitors) of
    {monitors, Monitors}->
      lists:member({process, Client}, Monitors);
    undefined->
      false
  end.
