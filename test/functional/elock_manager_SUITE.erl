%%=================================================================
%%  Module tests of elock_manager: the client side of the protocol
%%  (lock/1), the state transformations of the manager on hand-built
%%  states and the behaviour of the real manager process.
%%
%%  The test process poses as the manager when it calls the internal
%%  functions: the monitors and the timers then belong to it and are
%%  observable through process_info/2 and the mailbox. The clients
%%  and the proxies are collectors: every verdict they get comes to
%%  the test process as {Collector, Verdict}. The scope table is
%%  created by the test process and goes with it.
%%
%%  The functions that may exit(normal) (try_unlock/1 through
%%  handle_unlock/2, handle_down/2, next/1, handle_postpone_timeout/2)
%%  run in a monitored stand-in process, the exit is asserted as
%%  {'DOWN', _, process, _, normal}
%%=================================================================
-module(elock_manager_SUITE).

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
  lock_free_term_test/1,
  lock_busy_term_test/1,
  manager_dies_before_verdict_test/1,
  get_manager_test/1,
  add_request_shared_joins_test/1,
  add_request_exclusive_queues_test/1,
  add_request_shared_behind_exclusive_waiter_test/1,
  add_request_when_no_holders_test/1,
  add_request_shared_behind_pending_upgrade_test/1,
  add_request_timeout_test/1,
  handle_timeout_holder_ignored_test/1,
  handle_timeout_unknown_ignored_test/1,
  handle_unlock_last_holder_exits_test/1,
  handle_unlock_last_holder_new_ticket_test/1,
  handle_unlock_grants_next_test/1,
  handle_unlock_shared_batch_test/1,
  handle_unlock_unknown_ref_test/1,
  waiting_request_proxy_test/1,
  barging_exclusive_holder_test/1,
  barging_shared_again_with_exclusive_waiter_test/1,
  upgrade_only_holder_test/1,
  upgrade_waits_test/1,
  second_upgrade_deadlock_test/1,
  barging_dequeued_test/1,
  handle_deadlock_test/1,
  handle_down_test/1,
  handle_request_awaited_ticket_test/1,
  postponed_gap_test/1,
  postponed_gap_closed_test/1,
  postponed_out_of_order_test/1,
  postpone_timeout_test/1,
  postpone_timeout_releases_test/1,
  handle_postponed_stale_ticket_test/1,
  kill_proxy_test/1,
  notify_queued_test/1,
  start_timer_test/1,
  stop_timer_test/1,
  can_share_test/1,
  only_holder_test/1,
  client_monitor_test/1,
  enqueue_graph_test/1,
  handle_add_held_locks_test/1,
  handle_deadlock_probe_test/1,
  manager_lifecycle_test/1,
  manager_new_round_test/1,
  manager_ignores_unexpected_message_test/1
]).

% mirrors elock_manager.erl
-record(state,{
  holders,
  queue,
  requests,
  clients,
  scope,
  term,
  can_share,
  barging,
  last,
  postponed,
  postpone_timer,
  graph
}).
-record(req,{
  client,
  ref,
  queue,
  proxy,
  shared,
  has_lock,
  timer
}).
-record(client,{
  requests,
  monitor_ref
}).
-record(locked,{
  ref
}).
-record(timeout,{
  ref
}).
-record(retry,{
  ref
}).

% mirrors elock_graph.erl
-record(graph,{
  edges,
  index
}).

% The term every test locks
-define(TERM, term).

% The postpone timeout of the manager (?POSTPONE_TIMEOUT of elock_manager.erl)
-define(POSTPONE_TIMEOUT, 100).

all()->
  [
    {group, client_side},
    {group, requests_and_queue},
    {group, real_manager}
  ].

groups()->
  [
    {client_side, [], [
      lock_free_term_test,
      lock_busy_term_test,
      manager_dies_before_verdict_test,
      get_manager_test
    ]},
    {requests_and_queue, [], [
      add_request_shared_joins_test,
      add_request_exclusive_queues_test,
      add_request_shared_behind_exclusive_waiter_test,
      add_request_when_no_holders_test,
      add_request_shared_behind_pending_upgrade_test,
      add_request_timeout_test,
      handle_timeout_holder_ignored_test,
      handle_timeout_unknown_ignored_test,
      handle_unlock_last_holder_exits_test,
      handle_unlock_last_holder_new_ticket_test,
      handle_unlock_grants_next_test,
      handle_unlock_shared_batch_test,
      handle_unlock_unknown_ref_test,
      waiting_request_proxy_test,
      barging_exclusive_holder_test,
      barging_shared_again_with_exclusive_waiter_test,
      upgrade_only_holder_test,
      upgrade_waits_test,
      second_upgrade_deadlock_test,
      barging_dequeued_test,
      handle_deadlock_test,
      handle_down_test,
      handle_request_awaited_ticket_test,
      postponed_gap_test,
      postponed_gap_closed_test,
      postponed_out_of_order_test,
      postpone_timeout_test,
      postpone_timeout_releases_test,
      handle_postponed_stale_ticket_test,
      kill_proxy_test,
      notify_queued_test,
      start_timer_test,
      stop_timer_test,
      can_share_test,
      only_holder_test,
      client_monitor_test,
      enqueue_graph_test,
      handle_add_held_locks_test,
      handle_deadlock_probe_test
    ]},
    {real_manager, [], [
      manager_lifecycle_test,
      manager_new_round_test,
      manager_ignores_unexpected_message_test
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
%%  The scope table is owned by the test process: it goes with it
%%-----------------------------------------------------------------
init_per_testcase(TestCase, Config)->
  TestCase = ets:new(TestCase, [named_table, public, set]),
  [{scope, TestCase} | Config].

%%-----------------------------------------------------------------
%%  No manager process may be left, every client and collector is
%%  stopped
%%-----------------------------------------------------------------
end_per_testcase(_TestCase, Config)->
  Scope = ?config(scope, Config),
  elock_test_utils:stop_clients(),
  elock_test_utils:stop_collectors(),
  Left = (catch elock_test_utils:wait_until(fun()-> elock_test_utils:managers() =:= [] end, ?DEADLINE)),
  elock_test_utils:kill_managers(),
  catch ets:delete(Scope),
  case Left of
    ok-> ok;
    Error-> {fail, {managers_left, Error}}
  end.

%%=================================================================
%%  Client side
%%=================================================================
%%-----------------------------------------------------------------
%%  lock/1 on a free term: {ok, Manager}, the entry {Term, Manager, 1},
%%  the manager runs with the priority high and an off heap mailbox
%%  and monitors the client; #unlock{} of the only holder ends it
%%  normally and the table is empty
%%-----------------------------------------------------------------
lock_free_term_test(Config)->
  Scope = ?config(scope, Config),
  #request{ref = Ref} = Request = request(Scope, undefined, self(), false),

  {ok, Manager} = elock_manager:lock(Request),
  ?assert(is_pid(Manager)),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{?TERM, Manager, 1}]),
  ?WAIT(elock_test_utils:managers() =:= [Manager]),

  ?assertEqual({priority, high}, process_info(Manager, priority)),
  ?assertEqual({message_queue_data, off_heap}, process_info(Manager, message_queue_data)),
  ?WAIT(lists:member(Manager, monitored_by())),

  MonRef = erlang:monitor(process, Manager),
  Manager ! #unlock{ref = Ref},
  ?assertEqual({'DOWN', MonRef, process, Manager, normal}, ?RECEIVE({'DOWN', MonRef, process, Manager, _})),
  ?assertEqual([], elock_test_utils:locks(Scope)),
  ?assertEqual([], elock_test_utils:managers()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  lock/1 on a busy term (the test process is the manager): the
%%  exact #request{queue = 2, proxy = Client}, the client monitors the
%%  manager while it waits; every verdict; a foreign verdict is left
%%  in the client's mailbox; #retry{} makes the client take a new
%%  ticket
%%-----------------------------------------------------------------
lock_busy_term_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),
  true = ets:insert(Scope, {?TERM, Self, 1}),
  C1 = elock_test_utils:client(),
  #request{ref = Ref} = Request = request(Scope, undefined, C1, true),

  % #locked{}
  R1 = elock_test_utils:cast(C1, fun()-> elock_manager:lock(Request) end),
  ?assertEqual(Request#request{queue = 2, proxy = C1}, ?RECEIVE(#request{})),
  ?assertEqual([{?TERM, Self, 2}], elock_test_utils:locks(Scope)),
  ?assert(elock_test_utils:pending(R1)),
  % the client monitors the manager while it waits (its second monitor
  % of the test process: a util client always monitors its parent)
  ?assertEqual(2, monitors_by(C1)),
  C1 ! #locked{ref = Ref},
  ?assertEqual({ok, {ok, Self}}, elock_test_utils:result(R1, ?DEADLINE)),
  % the monitor is dropped with the verdict (demonitor is a signal, hence the wait)
  ?WAIT(monitors_by(C1) =:= 1),

  % #deadlock{}
  R2 = elock_test_utils:cast(C1, fun()-> elock_manager:lock(Request) end),
  ?assertEqual(Request#request{queue = 3, proxy = C1}, ?RECEIVE(#request{})),
  C1 ! #deadlock{ref = Ref},
  ?assertEqual({ok, {error, deadlock}}, elock_test_utils:result(R2, ?DEADLINE)),

  % #timeout{}
  R3 = elock_test_utils:cast(C1, fun()-> elock_manager:lock(Request) end),
  ?assertEqual(Request#request{queue = 4, proxy = C1}, ?RECEIVE(#request{})),
  C1 ! #timeout{ref = Ref},
  ?assertEqual({ok, {error, timeout}}, elock_test_utils:result(R3, ?DEADLINE)),

  % #retry{}: a new ticket, the same request
  R4 = elock_test_utils:cast(C1, fun()-> elock_manager:lock(Request) end),
  ?assertEqual(Request#request{queue = 5, proxy = C1}, ?RECEIVE(#request{})),
  C1 ! #retry{ref = Ref},
  ?assertEqual(Request#request{queue = 6, proxy = C1}, ?RECEIVE(#request{})),
  ?assertEqual([{?TERM, Self, 6}], elock_test_utils:locks(Scope)),
  ?assert(elock_test_utils:pending(R4)),
  C1 ! #locked{ref = Ref},
  ?assertEqual({ok, {ok, Self}}, elock_test_utils:result(R4, ?DEADLINE)),

  % the verdicts of other refs are left in the mailbox
  R5 = elock_test_utils:cast(C1, fun()-> elock_manager:lock(Request) end),
  ?assertEqual(Request#request{queue = 7, proxy = C1}, ?RECEIVE(#request{})),
  Foreign = [
    #locked{ref = make_ref()},
    #deadlock{ref = make_ref()},
    #timeout{ref = make_ref()},
    #retry{ref = make_ref()}
  ],
  [ C1 ! Verdict || Verdict <- Foreign ],
  ?assertEqual(timeout, elock_test_utils:result(R5, ?QUIET)),
  C1 ! #locked{ref = Ref},
  ?assertEqual({ok, {ok, Self}}, elock_test_utils:result(R5, ?DEADLINE)),
  ?assertEqual({messages, Foreign}, process_info(C1, messages)),

  ?NO_MESSAGE,
  elock_test_utils:stop(C1).

%%-----------------------------------------------------------------
%%  The manager dies before the verdict: the client deletes the dead
%%  entry, retries with ticket 1 and starts a real manager
%%-----------------------------------------------------------------
manager_dies_before_verdict_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),
  Fake = spawn(fun()->
    receive
      #request{} = Request->
        Self ! {fake_got, self(), Request}
    end
  end),
  true = ets:insert(Scope, {?TERM, Fake, 1}),
  C1 = elock_test_utils:client(),
  #request{ref = Ref} = Request = request(Scope, undefined, C1, false),

  R1 = elock_test_utils:cast(C1, fun()-> elock_manager:lock(Request) end),
  ?assertEqual({fake_got, Fake, Request#request{queue = 2, proxy = C1}}, ?RECEIVE({fake_got, Fake, _})),
  elock_test_utils:wait_dead(Fake),

  {ok, {ok, Manager}} = elock_test_utils:result(R1, ?DEADLINE),
  ?assert(is_pid(Manager)),
  ?assertNotEqual(Fake, Manager),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{?TERM, Manager, 1}]),
  ?WAIT(elock_test_utils:managers() =:= [Manager]),

  MonRef = erlang:monitor(process, Manager),
  Manager ! #unlock{ref = Ref},
  ?assertEqual({'DOWN', MonRef, process, Manager, normal}, ?RECEIVE({'DOWN', MonRef, process, Manager, _})),
  ?assertEqual([], elock_test_utils:locks(Scope)),
  ?NO_MESSAGE,
  elock_test_utils:stop(C1).

%%-----------------------------------------------------------------
%%  get_manager/3: no entry -> retry, the counter behind the ticket
%%  -> retry, the manager pid at or ahead of the ticket -> the pid,
%%  the pid not written yet -> polls until it appears (or until the
%%  entry is gone -> retry)
%%-----------------------------------------------------------------
get_manager_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),

  ?assertEqual(retry, elock_manager:get_manager(Scope, ?TERM, 2)),

  true = ets:insert(Scope, {?TERM, Self, 1}),
  ?assertEqual(retry, elock_manager:get_manager(Scope, ?TERM, 2)),

  true = ets:insert(Scope, {?TERM, Self, 2}),
  ?assertEqual(Self, elock_manager:get_manager(Scope, ?TERM, 2)),
  true = ets:insert(Scope, {?TERM, Self, 5}),
  ?assertEqual(Self, elock_manager:get_manager(Scope, ?TERM, 2)),

  % the manager has not written its pid yet: the poll ends when it does
  true = ets:insert(Scope, {?TERM, 0, 2}),
  {Poller1, Mon1} = spawn_monitor(fun()-> exit({got, elock_manager:get_manager(Scope, ?TERM, 2)}) end),
  ?assertEqual(polling, receive {'DOWN', Mon1, process, Poller1, Result1}-> Result1 after ?QUIET-> polling end),
  true = ets:update_element(Scope, ?TERM, {2, Self}),
  ?assertEqual({'DOWN', Mon1, process, Poller1, {got, Self}}, ?RECEIVE({'DOWN', Mon1, process, Poller1, _})),

  % the entry goes away while polling
  true = ets:insert(Scope, {?TERM, 0, 2}),
  {Poller2, Mon2} = spawn_monitor(fun()-> exit({got, elock_manager:get_manager(Scope, ?TERM, 2)}) end),
  ?assertEqual(polling, receive {'DOWN', Mon2, process, Poller2, Result2}-> Result2 after ?QUIET-> polling end),
  true = ets:delete(Scope, ?TERM),
  ?assertEqual({'DOWN', Mon2, process, Poller2, {got, retry}}, ?RECEIVE({'DOWN', Mon2, process, Poller2, _})),
  ?NO_MESSAGE.

%%=================================================================
%%  Requests and the queue (hand-built states, self() is the manager)
%%=================================================================
%%-----------------------------------------------------------------
%%  A shared request joins a shared lock at once: a second holder,
%%  the client registered with a monitor, #locked{} to the proxy,
%%  the queue empty, the lock still shared
%%-----------------------------------------------------------------
add_request_shared_joins_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, true),
  State0 = initial_state(Scope, Req1),

  State1 = elock_manager:add_request(Req2, State0),

  #state{clients = #{ C2 := #client{monitor_ref = Mon2} }} = State1,
  ?assert(is_reference(Mon2)),
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1}, Ref2 => {true, C2} },
    requests = (State0#state.requests)#{
      Ref2 => #req{ client = C2, ref = Ref2, queue = 2, proxy = undefined, shared = true, has_lock = true, timer = undefined }
    },
    clients = (State0#state.clients)#{
      C2 => #client{ requests = #{ Ref2 => true }, monitor_ref = Mon2 }
    },
    can_share = true
  }), plain(State1)),
  ?assertEqual([#locked{ref = Ref2}], elock_test_utils:collected(C2, 1)),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}]), monitors()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  An exclusive request queues behind a holder, and a shared one
%%  behind an exclusive holder: the request waits with its proxy,
%%  the client is registered, nothing is sent
%%-----------------------------------------------------------------
add_request_exclusive_queues_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),

  % exclusive behind shared
  Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  #state{clients = #{ C2 := #client{monitor_ref = Mon2} }} = State1,
  ?assert(is_reference(Mon2)),
  ?assertEqual(plain(State0#state{
    queue = gb_sets:from_list([{2, Ref2}]),
    requests = (State0#state.requests)#{
      Ref2 => #req{ client = C2, ref = Ref2, queue = 2, proxy = C2, shared = false, has_lock = false, timer = undefined }
    },
    clients = (State0#state.clients)#{
      C2 => #client{ requests = #{ Ref2 => false }, monitor_ref = Mon2 }
    }
  }), plain(State1)),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}]), monitors()),
  ?NO_MESSAGE,

  % shared behind exclusive
  Req1x = request(Scope, 1, C1, false),
  #request{ref = Ref3} = Req3 = request(Scope, 2, C3, true),
  State0x = initial_state(Scope, Req1x),
  State1x = elock_manager:add_request(Req3, State0x),
  #state{clients = #{ C3 := #client{monitor_ref = Mon3} }} = State1x,
  ?assertEqual(plain(State0x#state{
    queue = gb_sets:from_list([{2, Ref3}]),
    requests = (State0x#state.requests)#{
      Ref3 => #req{ client = C3, ref = Ref3, queue = 2, proxy = C3, shared = true, has_lock = false, timer = undefined }
    },
    clients = (State0x#state.clients)#{
      C3 => #client{ requests = #{ Ref3 => true }, monitor_ref = Mon3 }
    },
    can_share = false
  }), plain(State1x)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A shared newcomer does not overtake an exclusive waiter of a
%%  shared lock: it queues behind it
%%-----------------------------------------------------------------
add_request_shared_behind_exclusive_waiter_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),
  Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C3, true),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),

  State2 = elock_manager:add_request(Req3, State1),

  #state{clients = #{ C3 := #client{monitor_ref = Mon3} }} = State2,
  ?assertEqual(plain(State1#state{
    queue = gb_sets:from_list([{2, Ref2}, {3, Ref3}]),
    requests = (State1#state.requests)#{
      Ref3 => #req{ client = C3, ref = Ref3, queue = 3, proxy = C3, shared = true, has_lock = false, timer = undefined }
    },
    clients = (State1#state.clients)#{
      C3 => #client{ requests = #{ Ref3 => true }, monitor_ref = Mon3 }
    }
  }), plain(State2)),
  ?assertEqual(State0#state.holders, State2#state.holders),
  ?assertEqual(true, State2#state.can_share),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  Nobody holds the lock (the state after a reset): a shared or an
%%  exclusive request takes it at once
%%-----------------------------------------------------------------
add_request_when_no_holders_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 2, C1, true),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false),
  State0 = empty_state(Scope),

  State1 = elock_manager:add_request(Req1, State0),
  #state{clients = #{ C1 := #client{monitor_ref = Mon1} }} = State1,
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1} },
    requests = #{
      Ref1 => #req{ client = C1, ref = Ref1, queue = 2, proxy = undefined, shared = true, has_lock = true, timer = undefined }
    },
    clients = #{
      C1 => #client{ requests = #{ Ref1 => true }, monitor_ref = Mon1 }
    },
    can_share = true
  }), plain(State1)),
  ?assertEqual([#locked{ref = Ref1}], elock_test_utils:collected(C1, 1)),

  State2 = elock_manager:add_request(Req2, State0),
  #state{clients = #{ C2 := #client{monitor_ref = Mon2} }} = State2,
  ?assertEqual(plain(State0#state{
    holders = #{ Ref2 => {false, C2} },
    requests = #{
      Ref2 => #req{ client = C2, ref = Ref2, queue = 2, proxy = undefined, shared = false, has_lock = true, timer = undefined }
    },
    clients = #{
      C2 => #client{ requests = #{ Ref2 => false }, monitor_ref = Mon2 }
    },
    can_share = false
  }), plain(State2)),
  ?assertEqual([#locked{ref = Ref2}], elock_test_utils:collected(C2, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A shared newcomer does not join a shared lock while an upgrade
%%  is pending: it queues. The upgrade is served first when the
%%  other holder leaves; when the client releases its exclusive ref
%%  the lock is shared again and the newcomer joins
%%-----------------------------------------------------------------
add_request_shared_behind_pending_upgrade_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, true),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C1, false),
  #request{ref = Ref4} = Req4 = request(Scope, 4, C3, true),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  [#locked{ref = Ref2}] = elock_test_utils:collected(C2, 1),
  State2 = elock_manager:add_request(Req3, State1),
  ?assertEqual(Req3, State2#state.barging),

  State3 = elock_manager:add_request(Req4, State2),

  ?assertEqual([{4, Ref4}], gb_sets:to_list(State3#state.queue)),
  ?assertEqual(#{ Ref1 => {true, C1}, Ref2 => {true, C2} }, State3#state.holders),
  ?assertEqual(true, State3#state.can_share),
  #state{requests = #{ Ref4 := #req{ has_lock = false, proxy = C3 } }} = State3,
  ?NO_MESSAGE,

  % the other holder leaves: the upgrade goes first, the newcomer waits
  State4 = elock_manager:handle_unlock(Ref2, State3),
  ?assertEqual([#locked{ref = Ref3}], elock_test_utils:collected(C1, 1)),
  ?assertEqual(#{ Ref1 => {true, C1}, Ref3 => {false, C1} }, State4#state.holders),
  ?assertEqual(undefined, State4#state.barging),
  ?assertEqual(false, State4#state.can_share),
  ?assertEqual([{4, Ref4}], gb_sets:to_list(State4#state.queue)),
  ?NO_MESSAGE,

  % the exclusive ref is released: the lock is shared again, the newcomer joins
  State5 = elock_manager:handle_unlock(Ref3, State4),
  ?assertEqual([#locked{ref = Ref4}], elock_test_utils:collected(C3, 1)),
  #state{clients = #{ C3 := #client{monitor_ref = Mon3} }} = State5,
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1}, Ref4 => {true, C3} },
    requests = (State0#state.requests)#{
      Ref4 => #req{ client = C3, ref = Ref4, queue = 4, proxy = undefined, shared = true, has_lock = true, timer = undefined }
    },
    clients = (State0#state.clients)#{
      C3 => #client{ requests = #{ Ref4 => true }, monitor_ref = Mon3 }
    },
    can_share = true
  }), plain(State5)),
  ?assertEqual(lists:sort([{process, C1}, {process, C3}]), monitors()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A waiting request with a timeout: #req.timer is a reference and
%%  {timeout, Timer, {timeout, Ref}} arrives when it runs out;
%%  handle_timeout/2 sends #timeout{} to the proxy, removes the
%%  request and the client (its monitor is gone) and pushes the
%%  queue: the shared waiter behind the aborted exclusive one joins
%%  the shared holder
%%-----------------------------------------------------------------
add_request_timeout_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false, #{timeout => 100}),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C3, true),
  State0 = initial_state(Scope, Req1),

  State1 = elock_manager:add_request(Req2, State0),
  #state{requests = #{ Ref2 := #req{timer = Timer} }} = State1,
  ?assert(is_reference(Timer)),
  State2 = elock_manager:add_request(Req3, State1),
  ?assertEqual([{2, Ref2}, {3, Ref3}], gb_sets:to_list(State2#state.queue)),
  ?assertEqual({timeout, Timer, {timeout, Ref2}}, ?RECEIVE({timeout, Timer, _})),

  State3 = elock_manager:handle_timeout(Ref2, State2),

  ?assertEqual([#timeout{ref = Ref2}], elock_test_utils:collected(C2, 1)),
  ?assertEqual([#locked{ref = Ref3}], elock_test_utils:collected(C3, 1)),
  #state{clients = #{ C3 := #client{monitor_ref = Mon3} }} = State3,
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1}, Ref3 => {true, C3} },
    requests = (State0#state.requests)#{
      Ref3 => #req{ client = C3, ref = Ref3, queue = 3, proxy = undefined, shared = true, has_lock = true, timer = undefined }
    },
    clients = (State0#state.clients)#{
      C3 => #client{ requests = #{ Ref3 => true }, monitor_ref = Mon3 }
    },
    can_share = true
  }), plain(State3)),
  ?assertEqual(lists:sort([{process, C1}, {process, C3}]), monitors()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A timeout that fires after the grant is ignored: the holder
%%  keeps the lock, the state is identical, nothing is sent
%%-----------------------------------------------------------------
handle_timeout_holder_ignored_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, true, #{timeout => 100}),
  State0 = initial_state(Scope, Req1),
  % the shared request joins at once, it never waited: no timer
  State1 = elock_manager:add_request(Req2, State0),
  #state{requests = #{ Ref2 := #req{timer = undefined, has_lock = true} }} = State1,
  [#locked{ref = Ref2}] = elock_test_utils:collected(C2, 1),

  ?assertEqual(State1, elock_manager:handle_timeout(Ref2, State1)),
  ?assertEqual(State1, elock_manager:handle_timeout(Ref1, State1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A timeout of an unknown ref changes nothing
%%-----------------------------------------------------------------
handle_timeout_unknown_ignored_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  Req1 = request(Scope, 1, C1, false),
  Req2 = request(Scope, 2, C2, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),

  ?assertEqual(State1, elock_manager:handle_timeout(make_ref(), State1)),
  ?assertEqual(State0, elock_manager:handle_timeout(make_ref(), State0)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The last holder unlocks and nobody waits: the entry is deleted
%%  and the manager (the stand-in) exits normally
%%-----------------------------------------------------------------
handle_unlock_last_holder_exits_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, false),
  State0 = initial_state(Scope, Req1),

  {StandIn, MonRef} = stand_in(fun()-> elock_manager:handle_unlock(Ref1, State0) end),
  true = ets:insert(Scope, {?TERM, StandIn, 1}),
  StandIn ! go,

  ?assertEqual({'DOWN', MonRef, process, StandIn, normal}, ?RECEIVE({'DOWN', MonRef, process, StandIn, _})),
  ?assertEqual([], elock_test_utils:locks(Scope)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The last holder unlocks but a new client has taken a ticket
%%  already (the entry counter is ahead of last): the entry stays,
%%  the state is reset for the next round exactly as try_unlock/1
%%  does, the postpone timer is armed, the leaving client is
%%  demonitored, no exit
%%-----------------------------------------------------------------
handle_unlock_last_holder_new_ticket_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),
  C1 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, false),
  State0 = initial_state(Scope, Req1),
  ?assertEqual([{process, C1}], monitors()),
  true = ets:insert(Scope, {?TERM, Self, 2}),

  State1 = elock_manager:handle_unlock(Ref1, State0),

  #state{postpone_timer = Timer} = State1,
  ?assert(is_reference(Timer)),
  ?assertEqual(plain(State0#state{
    holders = #{},
    queue = gb_sets:empty(),
    requests = #{},
    clients = #{},
    can_share = true,
    graph = undefined,
    postpone_timer = Timer
  }), plain(State1)),
  ?assertEqual(1, State1#state.last),
  ?assertEqual([{?TERM, Self, 2}], elock_test_utils:locks(Scope)),
  ?assertEqual([], monitors()),
  ?assertEqual({timeout, Timer, postpone_timeout}, ?RECEIVE({timeout, Timer, _})),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The exclusive holder unlocks, an exclusive waiter is next: it
%%  is granted, becomes the only holder, the leaving client is gone
%%  with its monitor
%%-----------------------------------------------------------------
handle_unlock_grants_next_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, false),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  #state{clients = #{ C2 := #client{monitor_ref = Mon2} }} = State1,

  State2 = elock_manager:handle_unlock(Ref1, State1),

  ?assertEqual([#locked{ref = Ref2}], elock_test_utils:collected(C2, 1)),
  ?assertEqual(plain(State0#state{
    holders = #{ Ref2 => {false, C2} },
    requests = #{
      Ref2 => #req{ client = C2, ref = Ref2, queue = 2, proxy = undefined, shared = false, has_lock = true, timer = undefined }
    },
    clients = #{
      C2 => #client{ requests = #{ Ref2 => false }, monitor_ref = Mon2 }
    },
    can_share = false
  }), plain(State2)),
  ?assertEqual([{process, C2}], monitors()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The queue [shared, shared, exclusive, shared] behind an exclusive
%%  holder is served in steps: the two shared together, then the
%%  exclusive alone, then the last shared; the last unlock releases
%%  the term (the stand-in exits)
%%-----------------------------------------------------------------
handle_unlock_shared_batch_test(Config)->
  Scope = ?config(scope, Config),
  [C1, C2, C3, C4, C5] = [ elock_test_utils:collector() || _ <- lists:seq(1, 5) ],
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, false),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, true),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C3, true),
  #request{ref = Ref4} = Req4 = request(Scope, 4, C4, false),
  #request{ref = Ref5} = Req5 = request(Scope, 5, C5, true),
  State0 = initial_state(Scope, Req1),
  State1 = lists:foldl(fun elock_manager:add_request/2, State0, [Req2, Req3, Req4, Req5]),
  ?assertEqual([{2, Ref2}, {3, Ref3}, {4, Ref4}, {5, Ref5}], gb_sets:to_list(State1#state.queue)),
  ?NO_MESSAGE,

  % the two shared join together
  State2 = elock_manager:handle_unlock(Ref1, State1),
  ?assertEqual([#locked{ref = Ref2}], elock_test_utils:collected(C2, 1)),
  ?assertEqual([#locked{ref = Ref3}], elock_test_utils:collected(C3, 1)),
  ?assertEqual(#{ Ref2 => {true, C2}, Ref3 => {true, C3} }, State2#state.holders),
  ?assertEqual([{4, Ref4}, {5, Ref5}], gb_sets:to_list(State2#state.queue)),
  ?assertEqual(true, State2#state.can_share),
  ?NO_MESSAGE,

  % the exclusive one waits for both
  State3 = elock_manager:handle_unlock(Ref2, State2),
  ?assertEqual(#{ Ref3 => {true, C3} }, State3#state.holders),
  ?assertEqual([{4, Ref4}, {5, Ref5}], gb_sets:to_list(State3#state.queue)),
  ?NO_MESSAGE,

  State4 = elock_manager:handle_unlock(Ref3, State3),
  ?assertEqual([#locked{ref = Ref4}], elock_test_utils:collected(C4, 1)),
  ?assertEqual(#{ Ref4 => {false, C4} }, State4#state.holders),
  ?assertEqual([{5, Ref5}], gb_sets:to_list(State4#state.queue)),
  ?assertEqual(false, State4#state.can_share),
  ?NO_MESSAGE,

  % the last shared after the exclusive
  State5 = elock_manager:handle_unlock(Ref4, State4),
  ?assertEqual([#locked{ref = Ref5}], elock_test_utils:collected(C5, 1)),
  #state{clients = #{ C5 := #client{monitor_ref = Mon5} }} = State5,
  ?assertEqual(plain(State0#state{
    holders = #{ Ref5 => {true, C5} },
    requests = #{
      Ref5 => #req{ client = C5, ref = Ref5, queue = 5, proxy = undefined, shared = true, has_lock = true, timer = undefined }
    },
    clients = #{
      C5 => #client{ requests = #{ Ref5 => true }, monitor_ref = Mon5 }
    },
    can_share = true
  }), plain(State5)),
  ?assertEqual([{process, C5}], monitors()),
  ?NO_MESSAGE,

  % the last one leaves: the term is released. The requests were added
  % directly, hence last is set by hand to the ticket the entry carries
  State6 = State5#state{last = 5},
  {StandIn, MonRef} = stand_in(fun()-> elock_manager:handle_unlock(Ref5, State6) end),
  true = ets:insert(Scope, {?TERM, StandIn, 5}),
  StandIn ! go,
  ?assertEqual({'DOWN', MonRef, process, StandIn, normal}, ?RECEIVE({'DOWN', MonRef, process, StandIn, _})),
  ?assertEqual([], elock_test_utils:locks(Scope)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  #unlock{} of an unknown ref changes nothing
%%-----------------------------------------------------------------
handle_unlock_unknown_ref_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  Req1 = request(Scope, 1, C1, false),
  Req2 = request(Scope, 2, C2, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),

  ?assertEqual(State1, elock_manager:handle_unlock(make_ref(), State1)),
  ?assertEqual(State0, elock_manager:handle_unlock(make_ref(), State0)),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}]), monitors()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The proxy of a waiting multi node request (a worker other than
%%  the client): the client is told where the request queued up;
%%  the withdrawal of the request by #unlock{} kills the proxy and
%%  drops the request and the client; a timeout and a deadlock leave
%%  the proxy alive - it delivers the verdict; a dead client's
%%  waiting request loses its proxy as well
%%-----------------------------------------------------------------
waiting_request_proxy_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),
  Nodes = [node(), 'n2@host'],
  C1 = elock_test_utils:collector(),
  Req1 = request(Scope, 1, C1, false),
  State0 = initial_state(Scope, Req1),

  % withdrawn by #unlock{}
  C2 = elock_test_utils:collector(),
  P2 = elock_test_utils:collector(),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false, #{proxy => P2, nodes => Nodes}),
  State1 = elock_manager:add_request(Req2, State0),
  ?assertEqual([#queued{ ref = Ref2, manager = Self, node = node() }], elock_test_utils:collected(C2, 1)),
  ?assertEqual([{2, Ref2}], gb_sets:to_list(State1#state.queue)),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}]), monitors()),
  State2 = elock_manager:handle_unlock(Ref2, State1),
  elock_test_utils:wait_dead(P2),
  ?assertEqual(plain(State0), plain(State2)),
  ?assertEqual([{process, C1}], monitors()),
  ?NO_MESSAGE,

  % the timeout is delivered by the proxy
  C3 = elock_test_utils:collector(),
  P3 = elock_test_utils:collector(),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C3, false, #{proxy => P3, nodes => Nodes, timeout => 100}),
  State3 = elock_manager:add_request(Req3, State0),
  [#queued{ref = Ref3}] = elock_test_utils:collected(C3, 1),
  #state{requests = #{ Ref3 := #req{ timer = Timer3, proxy = P3 } }} = State3,
  ?assertEqual({timeout, Timer3, {timeout, Ref3}}, ?RECEIVE({timeout, Timer3, _})),
  State4 = elock_manager:handle_timeout(Ref3, State3),
  ?assertEqual([#timeout{ref = Ref3}], elock_test_utils:collected(P3, 1)),
  ?assert(is_process_alive(P3)),
  ?assertEqual(plain(State0), plain(State4)),
  ?NO_MESSAGE,

  % the deadlock verdict is delivered by the proxy
  C4 = elock_test_utils:collector(),
  P4 = elock_test_utils:collector(),
  #request{ref = Ref4} = Req4 = request(Scope, 4, C4, false, #{proxy => P4, nodes => Nodes}),
  State5 = elock_manager:add_request(Req4, State0),
  [#queued{ref = Ref4}] = elock_test_utils:collected(C4, 1),
  State6 = elock_manager:handle_deadlock(Ref4, State5),
  ?assertEqual([#deadlock{ref = Ref4}], elock_test_utils:collected(P4, 1)),
  ?assert(is_process_alive(P4)),
  ?assertEqual(plain(State0), plain(State6)),
  ?NO_MESSAGE,

  % the client is gone: nobody to serve, the proxy is killed
  C5 = elock_test_utils:collector(),
  P5 = elock_test_utils:collector(),
  #request{ref = Ref5} = Req5 = request(Scope, 5, C5, false, #{proxy => P5, nodes => Nodes}),
  State7 = elock_manager:add_request(Req5, State0),
  [#queued{ref = Ref5}] = elock_test_utils:collected(C5, 1),
  State8 = elock_manager:handle_down(C5, State7),
  elock_test_utils:wait_dead(P5),
  ?assertEqual(plain(State0), plain(State8)),
  ?assertEqual([{process, C1}], monitors()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  An exclusive holder asking again, shared or exclusive, is
%%  granted at once even with an exclusive waiter queued: the
%%  client holds several refs, one monitor, the waiter stays
%%-----------------------------------------------------------------
barging_exclusive_holder_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, false),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C1, true),
  #request{ref = Ref4} = Req4 = request(Scope, 4, C1, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),

  State2 = elock_manager:add_request(Req3, State1),
  ?assertEqual([#locked{ref = Ref3}], elock_test_utils:collected(C1, 1)),
  State3 = elock_manager:add_request(Req4, State2),
  ?assertEqual([#locked{ref = Ref4}], elock_test_utils:collected(C1, 1)),

  #state{clients = #{ C1 := #client{monitor_ref = Mon1} }} = State0,
  ?assertEqual(plain(State1#state{
    holders = #{ Ref1 => {false, C1}, Ref3 => {true, C1}, Ref4 => {false, C1} },
    requests = (State1#state.requests)#{
      Ref3 => #req{ client = C1, ref = Ref3, queue = 3, proxy = undefined, shared = true, has_lock = true, timer = undefined },
      Ref4 => #req{ client = C1, ref = Ref4, queue = 4, proxy = undefined, shared = false, has_lock = true, timer = undefined }
    },
    clients = (State1#state.clients)#{
      C1 => #client{ requests = #{ Ref1 => false, Ref3 => true, Ref4 => false }, monitor_ref = Mon1 }
    },
    can_share = false
  }), plain(State3)),
  ?assertEqual([{2, Ref2}], gb_sets:to_list(State3#state.queue)),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}]), monitors()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A shared holder asking shared again is granted at once even
%%  with an exclusive waiter queued; the waiter stays, the lock
%%  stays shared
%%-----------------------------------------------------------------
barging_shared_again_with_exclusive_waiter_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C1, true),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),

  State2 = elock_manager:add_request(Req3, State1),

  ?assertEqual([#locked{ref = Ref3}], elock_test_utils:collected(C1, 1)),
  #state{clients = #{ C1 := #client{monitor_ref = Mon1} }} = State0,
  ?assertEqual(plain(State1#state{
    holders = #{ Ref1 => {true, C1}, Ref3 => {true, C1} },
    requests = (State1#state.requests)#{
      Ref3 => #req{ client = C1, ref = Ref3, queue = 3, proxy = undefined, shared = true, has_lock = true, timer = undefined }
    },
    clients = (State1#state.clients)#{
      C1 => #client{ requests = #{ Ref1 => true, Ref3 => true }, monitor_ref = Mon1 }
    },
    can_share = true
  }), plain(State2)),
  ?assertEqual([{2, Ref2}], gb_sets:to_list(State2#state.queue)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The only shared holder upgrades: granted at once, the lock
%%  becomes exclusive, no barging request pending
%%-----------------------------------------------------------------
upgrade_only_holder_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C1, false),
  State0 = initial_state(Scope, Req1),

  State1 = elock_manager:add_request(Req2, State0),

  ?assertEqual([#locked{ref = Ref2}], elock_test_utils:collected(C1, 1)),
  #state{clients = #{ C1 := #client{monitor_ref = Mon1} }} = State0,
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1}, Ref2 => {false, C1} },
    requests = (State0#state.requests)#{
      Ref2 => #req{ client = C1, ref = Ref2, queue = 2, proxy = undefined, shared = false, has_lock = true, timer = undefined }
    },
    clients = #{
      C1 => #client{ requests = #{ Ref1 => true, Ref2 => false }, monitor_ref = Mon1 }
    },
    can_share = false,
    barging = undefined
  }), plain(State1)),
  ?assertEqual([{process, C1}], monitors()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  An upgrade with another shared holder waits out of the queue as
%%  the barging request (has_lock false, not queued, registered
%%  with the client); when the other holder unlocks it is granted:
%%  #locked{}, barging undefined, the lock exclusive
%%-----------------------------------------------------------------
upgrade_waits_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, true),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C1, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  [#locked{ref = Ref2}] = elock_test_utils:collected(C2, 1),

  State2 = elock_manager:add_request(Req3, State1),

  #state{clients = #{ C1 := #client{monitor_ref = Mon1} }} = State0,
  ?assertEqual(plain(State1#state{
    requests = (State1#state.requests)#{
      Ref3 => #req{ client = C1, ref = Ref3, queue = 3, proxy = C1, shared = false, has_lock = false, timer = undefined }
    },
    clients = (State1#state.clients)#{
      C1 => #client{ requests = #{ Ref1 => true, Ref3 => false }, monitor_ref = Mon1 }
    },
    barging = Req3
  }), plain(State2)),
  ?assertEqual([], gb_sets:to_list(State2#state.queue)),
  ?assertEqual(#{ Ref1 => {true, C1}, Ref2 => {true, C2} }, State2#state.holders),
  ?assertEqual(true, State2#state.can_share),
  ?NO_MESSAGE,

  % the other holder leaves: the upgrade is served
  State3 = elock_manager:handle_unlock(Ref2, State2),

  ?assertEqual([#locked{ref = Ref3}], elock_test_utils:collected(C1, 1)),
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1}, Ref3 => {false, C1} },
    requests = (State0#state.requests)#{
      Ref3 => #req{ client = C1, ref = Ref3, queue = 3, proxy = undefined, shared = false, has_lock = true, timer = undefined }
    },
    clients = #{
      C1 => #client{ requests = #{ Ref1 => true, Ref3 => false }, monitor_ref = Mon1 }
    },
    can_share = false,
    barging = undefined
  }), plain(State3)),
  ?assertEqual([{process, C1}], monitors()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A second upgrade while one is pending gets #deadlock{} at once
%%  and is not registered: the state is identical
%%-----------------------------------------------------------------
second_upgrade_deadlock_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, true),
  Req3 = request(Scope, 3, C1, false),
  #request{ref = Ref4} = Req4 = request(Scope, 4, C2, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  [#locked{ref = Ref2}] = elock_test_utils:collected(C2, 1),
  State2 = elock_manager:add_request(Req3, State1),
  ?assertEqual(Req3, State2#state.barging),

  ?assertEqual(State2, elock_manager:add_request(Req4, State2)),

  ?assertEqual([#deadlock{ref = Ref4}], elock_test_utils:collected(C2, 1)),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}]), monitors()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The pending upgrade times out: #timeout{} to its proxy, barging
%%  undefined, the client keeps its shared hold, the other holder is
%%  untouched
%%-----------------------------------------------------------------
barging_dequeued_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, true),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C1, false, #{timeout => 100}),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  [#locked{ref = Ref2}] = elock_test_utils:collected(C2, 1),
  State2 = elock_manager:add_request(Req3, State1),
  #state{requests = #{ Ref3 := #req{timer = Timer, has_lock = false} }, barging = Req3} = State2,
  ?assert(is_reference(Timer)),
  ?assertEqual({timeout, Timer, {timeout, Ref3}}, ?RECEIVE({timeout, Timer, _})),

  State3 = elock_manager:handle_timeout(Ref3, State2),

  ?assertEqual([#timeout{ref = Ref3}], elock_test_utils:collected(C1, 1)),
  ?assertEqual(plain(State1), plain(State3)),
  ?assertEqual(undefined, State3#state.barging),
  ?assertEqual(#{ Ref1 => {true, C1}, Ref2 => {true, C2} }, State3#state.holders),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}]), monitors()),
  ?NO_MESSAGE,

  % a deadlock verdict on the pending upgrade dequeues it the same way
  State4 = elock_manager:handle_deadlock(Ref3, State2),
  ?assertEqual([#deadlock{ref = Ref3}], elock_test_utils:collected(C1, 1)),
  ?assertEqual(plain(State1), plain(State4)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  #deadlock{} on a waiting request: #deadlock{} to its proxy, it
%%  is dequeued and the queue is pushed (the shared waiter behind it
%%  joins the shared holder); on a holder or an unknown ref: ignored
%%-----------------------------------------------------------------
handle_deadlock_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C3, true),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  State2 = elock_manager:add_request(Req3, State1),
  ?assertEqual([{2, Ref2}, {3, Ref3}], gb_sets:to_list(State2#state.queue)),

  State3 = elock_manager:handle_deadlock(Ref2, State2),

  ?assertEqual([#deadlock{ref = Ref2}], elock_test_utils:collected(C2, 1)),
  ?assertEqual([#locked{ref = Ref3}], elock_test_utils:collected(C3, 1)),
  #state{clients = #{ C3 := #client{monitor_ref = Mon3} }} = State3,
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1}, Ref3 => {true, C3} },
    requests = (State0#state.requests)#{
      Ref3 => #req{ client = C3, ref = Ref3, queue = 3, proxy = undefined, shared = true, has_lock = true, timer = undefined }
    },
    clients = (State0#state.clients)#{
      C3 => #client{ requests = #{ Ref3 => true }, monitor_ref = Mon3 }
    },
    can_share = true
  }), plain(State3)),
  ?assertEqual(lists:sort([{process, C1}, {process, C3}]), monitors()),

  % a holder is ignored, an unknown ref is ignored
  ?assertEqual(State3, elock_manager:handle_deadlock(Ref1, State3)),
  ?assertEqual(State3, elock_manager:handle_deadlock(Ref3, State3)),
  ?assertEqual(State3, elock_manager:handle_deadlock(make_ref(), State3)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The client is gone: every request of it, held and pending, is
%%  dropped and the queue is pushed; an unknown pid is ignored; the
%%  last holder dying releases the term (the stand-in exits)
%%-----------------------------------------------------------------
handle_down_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C4 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, true),
  Req3 = request(Scope, 3, C2, false),
  #request{ref = Ref4} = Req4 = request(Scope, 4, C4, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  [#locked{ref = Ref2}] = elock_test_utils:collected(C2, 1),
  % C2 holds shared and has an upgrade pending, C4 waits behind
  State2 = elock_manager:add_request(Req3, State1),
  ?assertEqual(Req3, State2#state.barging),
  State3 = elock_manager:add_request(Req4, State2),
  ?assertEqual([{4, Ref4}], gb_sets:to_list(State3#state.queue)),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}, {process, C4}]), monitors()),

  State4 = elock_manager:handle_down(C2, State3),

  #state{clients = #{ C4 := #client{monitor_ref = Mon4} }} = State3,
  ?assertEqual(plain(State0#state{
    queue = gb_sets:from_list([{4, Ref4}]),
    requests = (State0#state.requests)#{
      Ref4 => #req{ client = C4, ref = Ref4, queue = 4, proxy = C4, shared = false, has_lock = false, timer = undefined }
    },
    clients = (State0#state.clients)#{
      C4 => #client{ requests = #{ Ref4 => false }, monitor_ref = Mon4 }
    },
    barging = undefined
  }), plain(State4)),
  ?assertEqual(#{ Ref1 => {true, C1} }, State4#state.holders),
  ?assertEqual(true, State4#state.can_share),
  ?assertEqual(lists:sort([{process, C1}, {process, C4}]), monitors()),
  ?NO_MESSAGE,

  % an unknown pid
  Stranger = elock_test_utils:collector(),
  ?assertEqual(State4, elock_manager:handle_down(Stranger, State4)),

  % the holder dies: the waiter is granted
  State5 = elock_manager:handle_down(C1, State4),
  ?assertEqual([#locked{ref = Ref4}], elock_test_utils:collected(C4, 1)),
  ?assertEqual(#{ Ref4 => {false, C4} }, State5#state.holders),
  ?assertEqual([], gb_sets:to_list(State5#state.queue)),
  ?assertEqual(#{ C4 => #client{ requests = #{ Ref4 => false }, monitor_ref = Mon4 } }, State5#state.clients),
  ?assertEqual([{process, C4}], monitors()),
  ?NO_MESSAGE,

  % the last holder dies: the term is released
  {StandIn, MonRef} = stand_in(fun()-> elock_manager:handle_down(C4, State5) end),
  true = ets:insert(Scope, {?TERM, StandIn, 1}),
  StandIn ! go,
  ?assertEqual({'DOWN', MonRef, process, StandIn, normal}, ?RECEIVE({'DOWN', MonRef, process, StandIn, _})),
  ?assertEqual([], elock_test_utils:locks(Scope)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The awaited ticket (last + 1) is taken at once: the request is
%%  queued, last moves on, nothing is postponed, no timer
%%-----------------------------------------------------------------
handle_request_awaited_ticket_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),
  Req1 = request(Scope, 1, C1, false),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C3, false),
  State0 = initial_state(Scope, Req1),

  State1 = elock_manager:handle_request(Req2, State0),

  #state{clients = #{ C2 := #client{monitor_ref = Mon2} }} = State1,
  ?assertEqual(plain(State0#state{
    queue = gb_sets:from_list([{2, Ref2}]),
    requests = (State0#state.requests)#{
      Ref2 => #req{ client = C2, ref = Ref2, queue = 2, proxy = C2, shared = false, has_lock = false, timer = undefined }
    },
    clients = (State0#state.clients)#{
      C2 => #client{ requests = #{ Ref2 => false }, monitor_ref = Mon2 }
    },
    last = 2,
    postponed = [],
    postpone_timer = undefined
  }), plain(State1)),

  State2 = elock_manager:handle_request(Req3, State1),
  ?assertEqual([{2, Ref2}, {3, Ref3}], gb_sets:to_list(State2#state.queue)),
  ?assertEqual(3, State2#state.last),
  ?assertEqual([], State2#state.postponed),
  ?assertEqual(undefined, State2#state.postpone_timer),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}, {process, C3}]), monitors()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A ticket ahead of last + 1 is postponed and the postpone timer
%%  is armed, nothing else changes; the timer fires after
%%  ?POSTPONE_TIMEOUT
%%-----------------------------------------------------------------
postponed_gap_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),
  Req1 = request(Scope, 1, C1, false),
  Req3 = request(Scope, 3, C3, false),
  State0 = initial_state(Scope, Req1),

  State1 = elock_manager:handle_request(Req3, State0),

  #state{postpone_timer = Timer} = State1,
  ?assert(is_reference(Timer)),
  ?assertEqual(State0#state{
    postponed = [Req3],
    postpone_timer = Timer
  }, State1),
  ?assertEqual([{process, C1}], monitors()),
  ?assertEqual({timeout, Timer, postpone_timeout}, ?RECEIVE({timeout, Timer, _})),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The awaited ticket closes the gap: it and the postponed one are
%%  taken in the ticket order, nothing is postponed any more, the
%%  postpone timer is cancelled (no message) and cleared
%%-----------------------------------------------------------------
postponed_gap_closed_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),
  Req1 = request(Scope, 1, C1, false),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C3, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:handle_request(Req3, State0),
  ?assertEqual([Req3], State1#state.postponed),

  State2 = elock_manager:handle_request(Req2, State1),

  ?assertEqual([{2, Ref2}, {3, Ref3}], gb_sets:to_list(State2#state.queue)),
  ?assertEqual([], State2#state.postponed),
  ?assertEqual(undefined, State2#state.postpone_timer),
  ?assertEqual(3, State2#state.last),
  ?assertEqual(lists:sort([Ref2, Ref3, (Req1#request.ref)]), lists:sort(maps:keys(State2#state.requests))),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}, {process, C3}]), monitors()),
  % the cancelled timer does not fire
  elock_test_utils:no_message(2 * ?POSTPONE_TIMEOUT).

%%-----------------------------------------------------------------
%%  The tickets 4, 3, 2 arrive in that order with last = 1: all of
%%  them wait, sorted by the ticket, on one timer; the ticket 2
%%  lets them all in, in order, last = 4
%%-----------------------------------------------------------------
postponed_out_of_order_test(Config)->
  Scope = ?config(scope, Config),
  [C1, C2, C3, C4] = [ elock_test_utils:collector() || _ <- lists:seq(1, 4) ],
  Req1 = request(Scope, 1, C1, false),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C3, false),
  #request{ref = Ref4} = Req4 = request(Scope, 4, C4, false),
  State0 = initial_state(Scope, Req1),

  State1 = elock_manager:handle_request(Req4, State0),
  #state{postpone_timer = Timer} = State1,
  ?assert(is_reference(Timer)),
  ?assertEqual([Req4], State1#state.postponed),

  State2 = elock_manager:handle_request(Req3, State1),
  ?assertEqual([Req3, Req4], State2#state.postponed),
  ?assertEqual(Timer, State2#state.postpone_timer),
  ?assertEqual(State0#state{postponed = [Req3, Req4], postpone_timer = Timer}, State2),
  ?assertEqual([{process, C1}], monitors()),

  State3 = elock_manager:handle_request(Req2, State2),
  ?assertEqual([{2, Ref2}, {3, Ref3}, {4, Ref4}], gb_sets:to_list(State3#state.queue)),
  ?assertEqual([], State3#state.postponed),
  ?assertEqual(undefined, State3#state.postpone_timer),
  ?assertEqual(4, State3#state.last),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}, {process, C3}, {process, C4}]), monitors()),
  elock_test_utils:no_message(2 * ?POSTPONE_TIMEOUT).

%%-----------------------------------------------------------------
%%  The postpone timer fires with a postponed ticket: the missing
%%  one is stepped over, the postponed request is taken, last jumps;
%%  the late ticket gets #retry{}; a stale timer ref is ignored; a
%%  fired timer with nothing postponed is only cleared
%%-----------------------------------------------------------------
postpone_timeout_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),
  Req1 = request(Scope, 1, C1, false),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C3, false),
  State0 = initial_state(Scope, Req1),
  Timer = erlang:start_timer(?POSTPONE_TIMEOUT, self(), postpone_timeout),
  State1 = State0#state{ postponed = [Req3], postpone_timer = Timer },
  ?assertEqual({timeout, Timer, postpone_timeout}, ?RECEIVE({timeout, Timer, _})),

  % a stale timer ref is ignored
  ?assertEqual(State1, elock_manager:handle_postpone_timeout(make_ref(), State1)),

  State2 = elock_manager:handle_postpone_timeout(Timer, State1),

  #state{clients = #{ C3 := #client{monitor_ref = Mon3} }} = State2,
  ?assertEqual(plain(State0#state{
    queue = gb_sets:from_list([{3, Ref3}]),
    requests = (State0#state.requests)#{
      Ref3 => #req{ client = C3, ref = Ref3, queue = 3, proxy = C3, shared = false, has_lock = false, timer = undefined }
    },
    clients = (State0#state.clients)#{
      C3 => #client{ requests = #{ Ref3 => false }, monitor_ref = Mon3 }
    },
    last = 3,
    postponed = [],
    postpone_timer = undefined
  }), plain(State2)),
  ?NO_MESSAGE,

  % the late ticket has been passed by
  ?assertEqual(State2, elock_manager:handle_request(Req2, State2)),
  ?assertEqual([#retry{ref = Ref2}], elock_test_utils:collected(C2, 1)),
  ?assertEqual(lists:sort([{process, C1}, {process, C3}]), monitors()),
  ?NO_MESSAGE,

  % a fired timer with nothing postponed and a holder: only cleared
  Timer2 = make_ref(),
  ?assertEqual(State2, elock_manager:handle_postpone_timeout(Timer2, State2#state{postpone_timer = Timer2})),
  ?assertEqual(State2#state{postpone_timer = Timer2}, elock_manager:handle_postpone_timeout(make_ref(), State2#state{postpone_timer = Timer2})),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The postpone timer fires after a reset (nothing postponed, no
%%  holders, empty queue): the missing ticket is stepped over and
%%  the term is released with last + 1 (the stand-in exits); if yet
%%  another ticket has been taken meanwhile the entry stays and the
%%  timer is armed again
%%-----------------------------------------------------------------
postpone_timeout_releases_test(Config)->
  Scope = ?config(scope, Config),
  Timer = make_ref(),
  State0 = (empty_state(Scope))#state{ last = 1, postpone_timer = Timer },

  % the entry carries the ticket 2 that never came: released
  {StandIn, MonRef} = stand_in(fun()-> elock_manager:handle_postpone_timeout(Timer, State0) end),
  true = ets:insert(Scope, {?TERM, StandIn, 2}),
  StandIn ! go,
  ?assertEqual({'DOWN', MonRef, process, StandIn, normal}, ?RECEIVE({'DOWN', MonRef, process, StandIn, _})),
  ?assertEqual([], elock_test_utils:locks(Scope)),

  % the entry is ahead again: the next round is awaited
  {StandIn2, MonRef2} = stand_in(fun()-> elock_manager:handle_postpone_timeout(Timer, State0) end),
  true = ets:insert(Scope, {?TERM, StandIn2, 3}),
  StandIn2 ! go,
  {'DOWN', MonRef2, process, StandIn2, {returned, State1}} = ?RECEIVE({'DOWN', MonRef2, process, StandIn2, _}),
  #state{postpone_timer = Timer2} = State1,
  ?assert(is_reference(Timer2)),
  ?assertNotEqual(Timer, Timer2),
  ?assertEqual(State0#state{ last = 2, postpone_timer = Timer2 }, State1),
  ?assertEqual([{?TERM, StandIn2, 3}], elock_test_utils:locks(Scope)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  handle_postponed/1 with a postponed ticket at or behind last (the
%%  defensive clause): the request gets #retry{} and is dropped, its
%%  client is not registered, everything else is identical; a real
%%  gap behind it is still awaited (timer armed)
%%-----------------------------------------------------------------
handle_postponed_stale_ticket_test(Config)->
  Scope = ?config(scope, Config),
  C0 = elock_test_utils:collector(),
  C1 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),
  Req1 = request(Scope, 1, C1, false),
  #request{ref = RefStale} = ReqStale = request(Scope, 1, C0, false),
  #request{ref = RefBehind} = ReqBehind = request(Scope, 0, C0, false),
  Req3 = request(Scope, 3, C3, false),
  State0 = initial_state(Scope, Req1),

  % the ticket equal to last
  State1 = elock_manager:handle_postponed(State0#state{ postponed = [ReqStale], postpone_timer = undefined }),
  ?assertEqual([#retry{ref = RefStale}], elock_test_utils:collected(C0, 1)),
  ?assertEqual(State0, State1),
  ?assertEqual([{process, C1}], monitors()),
  ?NO_MESSAGE,

  % a ticket behind last followed by a ticket ahead: retry, then wait
  State2 = elock_manager:handle_postponed(State0#state{ postponed = [ReqBehind, Req3], postpone_timer = undefined }),
  ?assertEqual([#retry{ref = RefBehind}], elock_test_utils:collected(C0, 1)),
  #state{postpone_timer = Timer} = State2,
  ?assert(is_reference(Timer)),
  ?assertEqual(State0#state{ postponed = [Req3], postpone_timer = Timer }, State2),
  ?assertEqual([{process, C1}], monitors()),
  ?assertEqual({timeout, Timer, postpone_timeout}, ?RECEIVE({timeout, Timer, _})),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  kill_proxy/1: a proxy other than the client is killed, the
%%  client itself as the proxy is left alone, no proxy is fine
%%-----------------------------------------------------------------
kill_proxy_test(_Config)->
  Client = elock_test_utils:collector(),
  Proxy = elock_test_utils:collector(),

  ?assertEqual(ok, elock_manager:kill_proxy(#req{ client = Client, proxy = Proxy })),
  elock_test_utils:wait_dead(Proxy),
  ?assert(is_process_alive(Client)),

  ?assertEqual(ok, elock_manager:kill_proxy(#req{ client = Client, proxy = Client })),
  ?assertEqual(ok, elock_manager:kill_proxy(#req{ client = Client, proxy = undefined })),
  ?assert(is_process_alive(Client)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  notify_queued/1: the client of a multi node request is told
%%  where the request queued up, a single node request tells nothing
%%-----------------------------------------------------------------
notify_queued_test(_Config)->
  Ref = make_ref(),
  Self = self(),
  Client = elock_test_utils:collector(),

  elock_manager:notify_queued(#request{ ref = Ref, client = Self, nodes = [a, b] }),
  ?assertEqual(#queued{ ref = Ref, manager = Self, node = node() }, ?RECEIVE(#queued{})),

  elock_manager:notify_queued(#request{ ref = Ref, client = Client, nodes = [node(), 'n2@host', 'n3@host'] }),
  ?assertEqual([#queued{ ref = Ref, manager = Self, node = node() }], elock_test_utils:collected(Client, 1)),

  elock_manager:notify_queued(#request{ ref = Ref, client = Self, nodes = [node()] }),
  elock_manager:notify_queued(#request{ ref = Ref, client = Client, nodes = [a] }),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  start_timer/2: no timeout -> no timer; N ms -> a timer that
%%  delivers {timeout, Timer, {timeout, Ref}} to the manager
%%-----------------------------------------------------------------
start_timer_test(_Config)->
  Ref = make_ref(),
  Req = #req{ ref = Ref, timer = undefined },

  ?assertEqual(Req, elock_manager:start_timer(Req, undefined)),
  ?NO_MESSAGE,

  #req{timer = Timer} = Req1 = elock_manager:start_timer(Req, 50),
  ?assert(is_reference(Timer)),
  ?assertEqual(Req#req{timer = Timer}, Req1),
  ?assertEqual({timeout, Timer, {timeout, Ref}}, ?RECEIVE({timeout, Timer, _})),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  stop_timer/1 cancels the timer (it does not fire within twice
%%  its time) and clears the field; a request without a timer is
%%  left as it is
%%-----------------------------------------------------------------
stop_timer_test(_Config)->
  Ref = make_ref(),
  Req = #req{ ref = Ref, timer = undefined },
  Timeout = 100,

  #req{timer = Timer} = Req1 = elock_manager:start_timer(Req, Timeout),
  ?assert(is_reference(Timer)),
  ?assertEqual(Req, elock_manager:stop_timer(Req1)),
  elock_test_utils:no_message(2 * Timeout),

  ?assertEqual(Req, elock_manager:stop_timer(Req)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  can_share/1: every holder shared, incl. no holders at all
%%-----------------------------------------------------------------
can_share_test(_Config)->
  C = self(),
  R1 = make_ref(),
  R2 = make_ref(),
  R3 = make_ref(),
  lists:foreach(
    fun({Expected, Holders})->
      ?assertEqual(Expected, elock_manager:can_share(Holders))
    end,
    [
      {true, #{}},
      {true, #{ R1 => {true, C} }},
      {true, #{ R1 => {true, C}, R2 => {true, C}, R3 => {true, C} }},
      {false, #{ R1 => {false, C} }},
      {false, #{ R1 => {true, C}, R2 => {false, C} }},
      {false, #{ R1 => {false, C}, R2 => {true, C}, R3 => {true, C} }},
      {false, #{ R1 => {false, C}, R2 => {false, C} }}
    ]
  ).

%%-----------------------------------------------------------------
%%  only_holder/3: the client's requests minus its waiting ones
%%  account for every holder
%%-----------------------------------------------------------------
only_holder_test(_Config)->
  C = self(),
  R1 = make_ref(),
  R2 = make_ref(),
  R3 = make_ref(),
  lists:foreach(
    fun({Expected, ClientRequests, Holders, Waiting})->
      ?assertEqual(Expected, elock_manager:only_holder(ClientRequests, Holders, Waiting))
    end,
    [
      % the only holder asks for the upgrade (not registered yet)
      {true, #{ R1 => true }, #{ R1 => {true, C} }, 0},
      % two holds of the client, both holders
      {true, #{ R1 => true, R2 => true }, #{ R1 => {true, C}, R2 => {true, C} }, 0},
      % another client holds as well
      {false, #{ R1 => true }, #{ R1 => {true, C}, R2 => {true, other} }, 0},
      % the pending upgrade is registered with the client, not a holder
      {true, #{ R1 => true, R2 => false }, #{ R1 => {true, C} }, 1},
      {false, #{ R1 => true, R2 => false }, #{ R1 => {true, C}, R3 => {true, other} }, 1},
      % no holders at all
      {true, #{}, #{}, 0},
      {true, #{ R2 => false }, #{}, 1}
    ]
  ).

%%-----------------------------------------------------------------
%%  One monitor per client across several requests, dropped with
%%  the last request
%%-----------------------------------------------------------------
client_monitor_test(_Config)->
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  Ref1 = make_ref(),
  Ref2 = make_ref(),
  Ref3 = make_ref(),

  Clients1 = elock_manager:add_client_request(C1, Ref1, true, #{}),
  #{ C1 := #client{monitor_ref = Mon1} } = Clients1,
  ?assert(is_reference(Mon1)),
  ?assertEqual(#{ C1 => #client{ requests = #{ Ref1 => true }, monitor_ref = Mon1 } }, Clients1),
  ?assertEqual([{process, C1}], monitors()),

  Clients2 = elock_manager:add_client_request(C1, Ref2, false, Clients1),
  ?assertEqual(#{ C1 => #client{ requests = #{ Ref1 => true, Ref2 => false }, monitor_ref = Mon1 } }, Clients2),
  ?assertEqual([{process, C1}], monitors()),

  Clients3 = elock_manager:add_client_request(C2, Ref3, true, Clients2),
  #{ C2 := #client{monitor_ref = Mon2} } = Clients3,
  ?assertEqual(#{
    C1 => #client{ requests = #{ Ref1 => true, Ref2 => false }, monitor_ref = Mon1 },
    C2 => #client{ requests = #{ Ref3 => true }, monitor_ref = Mon2 }
  }, Clients3),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}]), monitors()),

  Clients4 = elock_manager:remove_client_request(C1, Ref1, Clients3),
  ?assertEqual(#{
    C1 => #client{ requests = #{ Ref2 => false }, monitor_ref = Mon1 },
    C2 => #client{ requests = #{ Ref3 => true }, monitor_ref = Mon2 }
  }, Clients4),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}]), monitors()),

  Clients5 = elock_manager:remove_client_request(C1, Ref2, Clients4),
  ?assertEqual(#{ C2 => #client{ requests = #{ Ref3 => true }, monitor_ref = Mon2 } }, Clients5),
  ?assertEqual([{process, C2}], monitors()),

  ?assertEqual(#{}, elock_manager:remove_client_request(C2, Ref3, Clients5)),
  ?assertEqual([], monitors()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  enqueue/2: a waiter that holds something joins the graph and the
%%  managers of its held locks are probed; one that holds nothing
%%  leaves the graph undefined; the graph goes when the waiter is
%%  granted
%%-----------------------------------------------------------------
enqueue_graph_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  K1 = {other_scope, other_term, node()},
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, false),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false, #{held => #{ K1 => M1 }}),
  Req3 = request(Scope, 3, C3, false),
  State0 = initial_state(Scope, Req1),

  State1 = elock_manager:enqueue(Req2, State0),

  ?assertEqual(#graph{
    edges = #{ K1 => #{ Ref2 => 1 } },
    index = #{ Ref2 => {1, #{ K1 => M1 }} }
  }, State1#state.graph),
  ?assertEqual([#deadlock_probe{
    ref = Ref2,
    edge = {Scope, ?TERM, node()},
    manager = self(),
    weight = 1,
    sent_to = #{ self() => true, M1 => true }
  }], elock_test_utils:collected(M1, 1)),
  ?assertEqual([{2, Ref2}], gb_sets:to_list(State1#state.queue)),
  #state{clients = #{ C2 := #client{monitor_ref = Mon2} }} = State1,
  ?assertEqual(plain(State0#state{
    queue = gb_sets:from_list([{2, Ref2}]),
    requests = (State0#state.requests)#{
      Ref2 => #req{ client = C2, ref = Ref2, queue = 2, proxy = C2, shared = false, has_lock = false, timer = undefined }
    },
    clients = (State0#state.clients)#{
      C2 => #client{ requests = #{ Ref2 => false }, monitor_ref = Mon2 }
    },
    graph = State1#state.graph
  }), plain(State1)),
  ?NO_MESSAGE,

  % holding nothing: no graph
  State2 = elock_manager:enqueue(Req3, State0),
  ?assertEqual(undefined, State2#state.graph),
  ?NO_MESSAGE,

  % the waiter is granted: it leaves the graph
  State3 = elock_manager:handle_unlock(Ref1, State1),
  ?assertEqual([#locked{ref = Ref2}], elock_test_utils:collected(C2, 1)),
  ?assertEqual(undefined, State3#state.graph),
  ?assertEqual(#{ Ref2 => {false, C2} }, State3#state.holders),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  #add_held_locks{} for a waiting request extends its held map in
%%  the graph and the new manager is probed; for a holder or an
%%  unknown ref the state is identical
%%-----------------------------------------------------------------
handle_add_held_locks_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  K1 = {other_scope, t1, node()},
  K2 = {Scope, t2, 'n2@host'},
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, false),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false, #{held => #{ K1 => M1 }, nodes => [node(), 'n2@host']}),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  [_Probe1] = elock_test_utils:collected(M1, 1),
  [#queued{ref = Ref2}] = elock_test_utils:collected(C2, 1),

  State2 = elock_manager:handle_add_held_locks(#add_held_locks{ ref = Ref2, held = #{ K2 => M2 } }, State1),

  ?assertEqual(State1#state{
    graph = #graph{
      edges = #{
        K1 => #{ Ref2 => 1 },
        K2 => #{ Ref2 => 1 }
      },
      index = #{ Ref2 => {1, #{ K1 => M1, K2 => M2 }} }
    }
  }, State2),
  ?assertEqual([#deadlock_probe{
    ref = Ref2,
    edge = {Scope, ?TERM, node()},
    manager = self(),
    weight = 1,
    sent_to = #{ self() => true, M2 => true }
  }], elock_test_utils:collected(M2, 1)),
  ?NO_MESSAGE,

  % a holder and an unknown ref
  ?assertEqual(State2, elock_manager:handle_add_held_locks(#add_held_locks{ ref = Ref1, held = #{ K2 => M2 } }, State2)),
  ?assertEqual(State2, elock_manager:handle_add_held_locks(#add_held_locks{ ref = make_ref(), held = #{ K2 => M2 } }, State2)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A probe from another manager: without a graph the state is
%%  identical and nothing is forwarded; a lighter local closer gets
%%  #deadlock{}, is dequeued, the queue is pushed and the probe goes
%%  on to the managers of the remaining waiters; an origin that
%%  loses is told and the state is identical
%%-----------------------------------------------------------------
handle_deadlock_probe_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),
  C4 = elock_test_utils:collector(),
  OM = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  OEdge = {origin_scope, origin_term, node()},
  K3 = {Scope, t3, node()},
  ORef = make_ref(),
  Probe = #deadlock_probe{
    ref = ORef,
    edge = OEdge,
    manager = OM,
    weight = 2,
    sent_to = #{ OM => true }
  },
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, false),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false, #{held => #{ OEdge => OM }}),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C3, false, #{held => #{ K3 => M3 }}),
  #request{ref = Ref4} = Req4 = request(Scope, 4, C4, false, #{held => #{ OEdge => OM, K3 => M3, {Scope, t5, node()} => M3 }}),
  State0 = initial_state(Scope, Req1),

  % no graph
  ?assertEqual(State0, elock_manager:handle_deadlock_probe(Probe, State0)),
  ?NO_MESSAGE,

  % a lighter closer (weight 1 < 2) and another waiter behind it
  State1 = elock_manager:add_request(Req2, State0),
  [_] = elock_test_utils:collected(OM, 1),
  State2 = elock_manager:add_request(Req3, State1),
  [_] = elock_test_utils:collected(M3, 1),

  State3 = elock_manager:handle_deadlock_probe(Probe, State2),

  ?assertEqual([#deadlock{ref = Ref2}], elock_test_utils:collected(C2, 1)),
  ?assertEqual([Probe#deadlock_probe{ sent_to = #{ OM => true, M3 => true } }], elock_test_utils:collected(M3, 1)),
  #state{clients = #{ C3 := #client{monitor_ref = Mon3} }} = State2,
  ?assertEqual(plain(State0#state{
    queue = gb_sets:from_list([{3, Ref3}]),
    requests = (State0#state.requests)#{
      Ref3 => #req{ client = C3, ref = Ref3, queue = 3, proxy = C3, shared = false, has_lock = false, timer = undefined }
    },
    clients = (State0#state.clients)#{
      C3 => #client{ requests = #{ Ref3 => false }, monitor_ref = Mon3 }
    },
    graph = #graph{
      edges = #{ K3 => #{ Ref3 => 1 } },
      index = #{ Ref3 => {1, #{ K3 => M3 }} }
    }
  }), plain(State3)),
  ?assertEqual(#{ Ref1 => {false, C1} }, State3#state.holders),
  ?assertEqual(lists:sort([{process, C1}, {process, C3}]), monitors()),
  ?NO_MESSAGE,

  % a heavier closer (weight 3 > 2): the origin loses, nothing else moves
  State4 = elock_manager:add_request(Req4, State3),
  [_] = elock_test_utils:collected(OM, 1),
  [_] = elock_test_utils:collected(M3, 1),
  ?assertEqual([{3, Ref3}, {4, Ref4}], gb_sets:to_list(State4#state.queue)),

  ?assertEqual(State4, elock_manager:handle_deadlock_probe(Probe, State4)),

  ?assertEqual([#deadlock{ref = ORef}], elock_test_utils:collected(OM, 1)),
  ?NO_MESSAGE.

%%=================================================================
%%  The real manager
%%=================================================================
%%-----------------------------------------------------------------
%%  Ticket 1 starts the manager, ticket 2 waits, #unlock{} of the
%%  first grants the second, #unlock{} of the second ends the
%%  manager and the table is empty
%%-----------------------------------------------------------------
manager_lifecycle_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:client(),
  C2 = elock_test_utils:client(),
  #request{ref = Ref1} = Req1 = request(Scope, undefined, C1, false),
  #request{ref = Ref2} = Req2 = request(Scope, undefined, C2, false),

  {ok, Manager} = elock_test_utils:call(C1, fun()-> elock_manager:lock(Req1) end),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{?TERM, Manager, 1}]),
  MonRef = erlang:monitor(process, Manager),

  R2 = elock_test_utils:cast(C2, fun()-> elock_manager:lock(Req2) end),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{?TERM, Manager, 2}]),
  ?assertEqual(timeout, elock_test_utils:result(R2, ?QUIET)),

  Manager ! #unlock{ref = Ref1},
  ?assertEqual({ok, {ok, Manager}}, elock_test_utils:result(R2, ?DEADLINE)),
  ?assertEqual([{?TERM, Manager, 2}], elock_test_utils:locks(Scope)),
  ?assert(is_process_alive(Manager)),

  Manager ! #unlock{ref = Ref2},
  ?assertEqual({'DOWN', MonRef, process, Manager, normal}, ?RECEIVE({'DOWN', MonRef, process, Manager, _})),
  ?assertEqual([], elock_test_utils:locks(Scope)),
  ?assertEqual([], elock_test_utils:managers()),
  ?NO_MESSAGE,
  elock_test_utils:stop(C1),
  elock_test_utils:stop(C2).

%%-----------------------------------------------------------------
%%  A new client takes a ticket right before the last holder unlocks:
%%  the manager stays for the next round, serves the request and
%%  the entry shows the same pid; the unlock of the new holder ends
%%  it normally
%%-----------------------------------------------------------------
manager_new_round_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:client(),
  #request{ref = Ref1} = Req1 = request(Scope, undefined, C1, false),

  {ok, Manager} = elock_test_utils:call(C1, fun()-> elock_manager:lock(Req1) end),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{?TERM, Manager, 1}]),
  MonRef = erlang:monitor(process, Manager),

  % the test process is the second client: its ticket is taken before
  % the unlock reaches the manager, its request after
  ?assertEqual(2, ets:update_counter(Scope, ?TERM, {3, 1}, {?TERM, 0, 0})),
  Manager ! #unlock{ref = Ref1},
  #request{ref = Ref2} = Req2 = request(Scope, 2, self(), false),
  Manager ! Req2,

  ?assertEqual(#locked{ref = Ref2}, ?RECEIVE(#locked{})),
  ?assertEqual([{?TERM, Manager, 2}], elock_test_utils:locks(Scope)),

  Manager ! #unlock{ref = Ref2},
  ?assertEqual({'DOWN', MonRef, process, Manager, normal}, ?RECEIVE({'DOWN', MonRef, process, Manager, _})),
  ?assertEqual([], elock_test_utils:locks(Scope)),
  ?assertEqual([], elock_test_utils:managers()),
  ?NO_MESSAGE,
  elock_test_utils:stop(C1).

%%-----------------------------------------------------------------
%%  An unexpected message is logged and ignored: the manager goes on
%%  serving in order and ends normally
%%-----------------------------------------------------------------
manager_ignores_unexpected_message_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:client(),
  C2 = elock_test_utils:client(),
  #request{ref = Ref1} = Req1 = request(Scope, undefined, C1, false),
  Req2 = request(Scope, undefined, C2, false),

  {ok, Manager} = elock_test_utils:call(C1, fun()-> elock_manager:lock(Req1) end),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{?TERM, Manager, 1}]),
  MonRef = erlang:monitor(process, Manager),

  Manager ! garbage,
  Manager ! {unexpected, self(), make_ref()},
  R2 = elock_test_utils:cast(C2, fun()-> elock_manager:lock(Req2) end),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{?TERM, Manager, 2}]),
  ?assertEqual(timeout, elock_test_utils:result(R2, ?QUIET)),
  ?assert(is_process_alive(Manager)),

  Manager ! #unlock{ref = Ref1},
  ?assertEqual({ok, {ok, Manager}}, elock_test_utils:result(R2, ?DEADLINE)),
  Manager ! #unlock{ref = (Req2#request.ref)},
  ?assertEqual({'DOWN', MonRef, process, Manager, normal}, ?RECEIVE({'DOWN', MonRef, process, Manager, _})),
  ?assertEqual([], elock_test_utils:locks(Scope)),
  ?NO_MESSAGE,
  elock_test_utils:stop(C1),
  elock_test_utils:stop(C2).

%%=================================================================
%%  Utilities
%%=================================================================
% The request of Client with the given ticket (undefined: lock/1
% takes it), the proxy is the client as for a single node request
% unless the options name another one
request(Scope, Ticket, Client, Shared)->
  request(Scope, Ticket, Client, Shared, #{}).
request(Scope, Ticket, Client, Shared, Options)->
  #request{
    queue = Ticket,
    ref = make_ref(),
    scope = Scope,
    term = ?TERM,
    client = Client,
    proxy = maps:get(proxy, Options, Client),
    shared = Shared,
    held = maps:get(held, Options, #{}),
    nodes = maps:get(nodes, Options, [node()]),
    timeout = maps:get(timeout, Options, undefined)
  }.

% The state of a manager just started by the winner of the ticket 1,
% as init/1 builds it, with the test process as the manager: the
% monitor of the client belongs to the test process. The request
% that starts a manager has no proxy
initial_state(Scope, #request{
  ref = Ref,
  client = Client,
  shared = Shared
})->
  #state{
    holders = #{ Ref => {Shared, Client} },
    queue = gb_sets:empty(),
    requests = #{
      Ref => #req{
        client = Client,
        ref = Ref,
        queue = 1,
        proxy = undefined,
        shared = Shared,
        has_lock = true,
        timer = undefined
      }
    },
    clients = #{
      Client => #client{
        requests = #{ Ref => Shared },
        monitor_ref = erlang:monitor(process, Client)
      }
    },
    scope = Scope,
    term = ?TERM,
    can_share = Shared,
    barging = undefined,
    last = 1,
    postponed = [],
    postpone_timer = undefined,
    graph = undefined
  }.

% The state after a reset: nobody holds, nobody waits
empty_state(Scope)->
  #state{
    holders = #{},
    queue = gb_sets:empty(),
    requests = #{},
    clients = #{},
    scope = Scope,
    term = ?TERM,
    can_share = true,
    barging = undefined,
    last = 1,
    postponed = [],
    postpone_timer = undefined,
    graph = undefined
  }.

% The state with the queue as a list, comparable with ?assertEqual
plain(#state{queue = Queue} = State)->
  State#state{ queue = gb_sets:to_list(Queue) }.

% The monitors of the test process
monitors()->
  {monitors, Monitors} = process_info(self(), monitors),
  lists:sort(Monitors).

% The processes monitoring the test process, one entry per monitor
monitored_by()->
  {monitored_by, Pids} = process_info(self(), monitored_by),
  Pids.

% How many monitors Pid has on the test process
monitors_by(Pid)->
  length([ P || P <- monitored_by(), P =:= Pid ]).

% A monitored process that runs Fun when told to go and exits with
% {returned, Result} unless Fun exits by itself
stand_in(Fun)->
  spawn_monitor(fun()->
    receive
      go-> exit({returned, Fun()})
    end
  end).
