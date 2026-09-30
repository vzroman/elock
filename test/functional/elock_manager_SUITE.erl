%%=================================================================
%%  Module tests of elock_manager: the client side of the protocol
%%  (lock/2 of the client itself, lock/1 of a proxy on its behalf),
%%  the state transformations of the manager on hand-built states
%%  and the behaviour of the real manager process.
%%
%%  The test process poses as the manager when it calls the internal
%%  functions: the monitors and the timers then belong to it and are
%%  observable through process_info/2 and the mailbox. The clients
%%  and the proxies are collectors. Whatever the manager sends to a
%%  proxy, a verdict or #queued{}, is ?reply(Tag, Message) with the
%%  tag of the request and comes to the test process as
%%  {Collector, ?reply(Tag, Message)}. The scope table is created by
%%  the test process and goes with it.
%%
%%  A request carries only the number of the locks its client holds
%%  and a waiter is not in the graph until the client answers
%%  #queued{}: the cases that need it there feed the answer through
%%  handle_add_held_locks/2 (see waiting/3).
%%
%%  The functions that may exit(normal) (try_unlock/1 through
%%  handle_unlock/2, handle_down/2, next/1, handle_postpone_timeout/2)
%%  run in a monitored stand-in process, the exit is asserted as
%%  {'DOWN', _, process, _, normal}
%%=================================================================
-module(elock_manager_SUITE).

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
  lock_free_term_test/1,
  lock_busy_term_test/1,
  lock_queued_notice_test/1,
  lock_queued_answer_test/1,
  manager_dies_before_verdict_test/1,
  manager_dies_after_queued_test/1,
  receive_marker_test/1,
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
  waiting_client_requests_again_test/1,
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
  postponed_asked_when_taken_test/1,
  kill_proxy_test/1,
  notify_queued_test/1,
  start_timer_test/1,
  stop_timer_test/1,
  can_share_test/1,
  only_holder_test/1,
  client_monitor_test/1,
  holder_has_no_tag_test/1,
  untagged_request_test/1,
  enqueue_graph_test/1,
  barging_graph_test/1,
  handle_add_held_locks_test/1,
  handle_deadlock_probe_test/1,
  manager_lifecycle_test/1,
  manager_new_round_test/1,
  manager_ignores_unexpected_message_test/1,
  manager_tagged_replies_test/1
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
  tag,
  shared,
  held_count,
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
-define(reply(Tag, Message), {Tag, Message}).

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
      lock_queued_notice_test,
      lock_queued_answer_test,
      manager_dies_before_verdict_test,
      manager_dies_after_queued_test,
      receive_marker_test,
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
      waiting_client_requests_again_test,
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
      postponed_asked_when_taken_test,
      kill_proxy_test,
      notify_queued_test,
      start_timer_test,
      stop_timer_test,
      can_share_test,
      only_holder_test,
      client_monitor_test,
      holder_has_no_tag_test,
      untagged_request_test,
      enqueue_graph_test,
      barging_graph_test,
      handle_add_held_locks_test,
      handle_deadlock_probe_test
    ]},
    {real_manager, [], [
      manager_lifecycle_test,
      manager_new_round_test,
      manager_ignores_unexpected_message_test,
      manager_tagged_replies_test
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
%%  A free term is taken the same way in both modes, by the client
%%  itself (lock/2) and by a proxy on its behalf (lock/1):
%%  {ok, Manager}, the entry {Term, Manager, 1}, the manager runs
%%  with the priority high and an off heap mailbox and monitors the
%%  client, not the proxy; #unlock{} of the only holder ends it
%%  normally and the table is empty
%%-----------------------------------------------------------------
lock_free_term_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),
  Proxy = elock_test_utils:client(),

  lists:foreach(
    fun(Lock)->
      #request{ref = Ref} = Request = request(Scope, undefined, Self, false),

      {ok, Manager} = Lock(Request),
      ?assert(is_pid(Manager)),
      ?WAIT(elock_test_utils:locks(Scope) =:= [{?TERM, Manager, 1}]),
      ?WAIT(elock_test_utils:managers() =:= [Manager]),

      ?assertEqual({priority, high}, process_info(Manager, priority)),
      ?assertEqual({message_queue_data, off_heap}, process_info(Manager, message_queue_data)),
      ?WAIT(lists:member(Manager, monitored_by())),
      ?assertEqual({monitors, [{process, Self}]}, process_info(Manager, monitors)),

      MonRef = erlang:monitor(process, Manager),
      Manager ! #unlock{ref = Ref},
      ?assertEqual({'DOWN', MonRef, process, Manager, normal}, ?RECEIVE({'DOWN', MonRef, process, Manager, _})),
      ?assertEqual([], elock_test_utils:locks(Scope)),
      ?assertEqual([], elock_test_utils:managers()),
      ?NO_MESSAGE
    end,
    [
      % the client itself, it holds nothing
      fun(Request)-> elock_manager:lock(Request, #{}) end,
      % a proxy on behalf of the client
      fun(Request)-> elock_test_utils:call(Proxy, fun()-> elock_manager:lock(Request) end) end
    ]
  ),
  elock_test_utils:stop(Proxy).

%%-----------------------------------------------------------------
%%  lock/2 on a busy term (the test process is the manager): the
%%  exact #request{queue = 2, proxy = Client, tag = Tag}, the tag is
%%  a reference made for the attempt, the client monitors the
%%  manager while it waits; every verdict is taken with that tag;
%%  #retry{} makes the client take a new ticket under a new tag;
%%  the replies with another tag, the one of the attempt before
%%  #retry{} included, and the bare records are left in the
%%  client's mailbox
%%-----------------------------------------------------------------
lock_busy_term_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),
  true = ets:insert(Scope, {?TERM, Self, 1}),
  C1 = elock_test_utils:client(),
  #request{ref = Ref} = Request = request(Scope, undefined, C1, true),
  Lock = fun()-> elock_manager:lock(Request, #{}) end,

  % #locked{}
  R1 = elock_test_utils:cast(C1, Lock),
  #request{tag = Tag1} = Request1 = ?RECEIVE(#request{}),
  ?assert(is_reference(Tag1)),
  ?assertEqual(Request#request{queue = 2, proxy = C1, tag = Tag1}, Request1),
  ?assertEqual([{?TERM, Self, 2}], elock_test_utils:locks(Scope)),
  ?assert(elock_test_utils:pending(R1)),
  % the client monitors the manager while it waits (its second monitor
  % of the test process: a util client always monitors its parent)
  ?assertEqual(2, monitors_by(C1)),
  C1 ! ?reply(Tag1, #locked{ref = Ref}),
  ?assertEqual({ok, {ok, Self}}, elock_test_utils:result(R1, ?DEADLINE)),
  % the monitor is dropped with the verdict (demonitor is a signal, hence the wait)
  ?WAIT(monitors_by(C1) =:= 1),

  % #deadlock{}
  R2 = elock_test_utils:cast(C1, Lock),
  #request{tag = Tag2} = Request2 = ?RECEIVE(#request{}),
  ?assertEqual(Request#request{queue = 3, proxy = C1, tag = Tag2}, Request2),
  C1 ! ?reply(Tag2, #deadlock{ref = Ref, winner = ?WINNER}),
  ?assertEqual({ok, {error, {deadlock, ?WINNER}}}, elock_test_utils:result(R2, ?DEADLINE)),

  % #timeout{}
  R3 = elock_test_utils:cast(C1, Lock),
  #request{tag = Tag3} = Request3 = ?RECEIVE(#request{}),
  ?assertEqual(Request#request{queue = 4, proxy = C1, tag = Tag3}, Request3),
  C1 ! ?reply(Tag3, #timeout{ref = Ref}),
  ?assertEqual({ok, {error, timeout}}, elock_test_utils:result(R3, ?DEADLINE)),

  % #retry{}: a new ticket and a new tag, the same request
  R4 = elock_test_utils:cast(C1, Lock),
  #request{tag = Tag4} = Request4 = ?RECEIVE(#request{}),
  ?assertEqual(Request#request{queue = 5, proxy = C1, tag = Tag4}, Request4),
  C1 ! ?reply(Tag4, #retry{ref = Ref}),
  #request{tag = Tag5} = Request5 = ?RECEIVE(#request{}),
  ?assertEqual(Request#request{queue = 6, proxy = C1, tag = Tag5}, Request5),
  ?assertEqual([{?TERM, Self, 6}], elock_test_utils:locks(Scope)),
  ?assert(elock_test_utils:pending(R4)),
  % one monitor per attempt: the one of the attempt before #retry{} is dropped
  ?WAIT(monitors_by(C1) =:= 2),
  C1 ! ?reply(Tag5, #locked{ref = Ref}),
  ?assertEqual({ok, {ok, Self}}, elock_test_utils:result(R4, ?DEADLINE)),

  % only the replies with the tag of the attempt are taken
  R5 = elock_test_utils:cast(C1, Lock),
  #request{tag = Tag6} = Request6 = ?RECEIVE(#request{}),
  ?assertEqual(Request#request{queue = 7, proxy = C1, tag = Tag6}, Request6),
  Foreign = [
    % the verdicts as they are inside the replies, without a tag
    #locked{ref = Ref},
    #deadlock{ref = Ref, winner = ?WINNER},
    #timeout{ref = Ref},
    #retry{ref = Ref},
    % another tag: the ones of the attempts before, a stranger, none
    ?reply(Tag4, #locked{ref = Ref}),
    ?reply(Tag5, #deadlock{ref = Ref, winner = ?WINNER}),
    ?reply(make_ref(), #timeout{ref = Ref}),
    ?reply(make_ref(), #retry{ref = Ref}),
    ?reply(undefined, #locked{ref = Ref}),
    % the manager is monitored by the tag, another monitor is not the one
    {'DOWN', make_ref(), process, Self, normal}
  ],
  [ C1 ! Message || Message <- Foreign ],
  ?assertEqual(timeout, elock_test_utils:result(R5, ?QUIET)),
  ?assertEqual([{?TERM, Self, 7}], elock_test_utils:locks(Scope)),
  C1 ! ?reply(Tag6, #locked{ref = Ref}),
  ?assertEqual({ok, {ok, Self}}, elock_test_utils:result(R5, ?DEADLINE)),
  ?assertEqual({messages, Foreign}, process_info(C1, messages)),

  % every attempt had a tag of its own
  Tags = [Tag1, Tag2, Tag3, Tag4, Tag5, Tag6],
  ?assert(lists:all(fun erlang:is_reference/1, Tags)),
  ?assertEqual(length(Tags), length(lists:usort(Tags))),

  ?NO_MESSAGE,
  elock_test_utils:stop(C1).

%%-----------------------------------------------------------------
%%  Proxy mode, lock/1 on behalf of the client: the tagged #queued{}
%%  of the manager reaches the client as the bare #queued{}, before
%%  the grant; the proxy has no held map, it answers nothing and
%%  keeps waiting; a #queued{} with another tag and a bare one are
%%  left alone
%%-----------------------------------------------------------------
lock_queued_notice_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),
  true = ets:insert(Scope, {?TERM, Self, 1}),
  Client = elock_test_utils:collector(),
  Proxy = elock_test_utils:client(),
  #request{ref = Ref} = Request = request(Scope, undefined, Client, false,
    #{nodes => [node(), 'n2@host']}),
  R = elock_test_utils:cast(Proxy, fun()-> elock_manager:lock(Request) end),
  #request{tag = Tag} = Request1 = ?RECEIVE(#request{}),
  ?assert(is_reference(Tag)),
  ?assertEqual(Request#request{queue = 2, proxy = Proxy, tag = Tag}, Request1),
  Queued = #queued{ref = Ref, manager = Self, node = node()},
  Foreign = [
    ?reply(make_ref(), Queued),
    Queued
  ],

  [ Proxy ! Message || Message <- Foreign ],
  Proxy ! ?reply(Tag, Queued),
  ?assertEqual([Queued], elock_test_utils:collected(Client, 1)),
  ?assertEqual(timeout, elock_test_utils:result(R, ?QUIET)),
  ?assertEqual({messages, Foreign}, process_info(Proxy, messages)),

  Proxy ! ?reply(Tag, #locked{ref = Ref}),
  ?assertEqual({ok, {ok, Self}}, elock_test_utils:result(R, ?DEADLINE)),
  ?assertEqual({messages, Foreign}, process_info(Proxy, messages)),
  % nothing came back to the manager, nothing else went to the client
  ?NO_MESSAGE,
  elock_test_utils:stop(Proxy).

%%-----------------------------------------------------------------
%%  Client mode, lock/2 with the held map of the client (the test
%%  process is the manager): the tagged #queued{} is answered to
%%  the manager with #add_held_locks{} that carries the ref of the
%%  request and exactly that map, the client passes nothing on to
%%  itself and keeps waiting; a #queued{} with another tag and a
%%  bare one are left alone, unanswered. After #retry{} the manager
%%  asks again under the new tag and is answered again; the verdict
%%  that follows is taken
%%-----------------------------------------------------------------
lock_queued_answer_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),
  true = ets:insert(Scope, {?TERM, Self, 1}),
  C1 = elock_test_utils:client(),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  Held = #{
    {other_scope, t1, node()} => M1,
    {Scope, t2, 'n2@host'} => M2
  },
  #request{ref = Ref} = Request = request(Scope, undefined, C1, false, #{held => Held}),
  R = elock_test_utils:cast(C1, fun()-> elock_manager:lock(Request, Held) end),
  #request{tag = Tag1} = Request1 = ?RECEIVE(#request{}),
  ?assertEqual(Request#request{queue = 2, proxy = C1, tag = Tag1}, Request1),
  Queued = #queued{ref = Ref, manager = Self, node = node()},
  Foreign = [
    ?reply(make_ref(), Queued),
    Queued
  ],

  % not the questions of this attempt: no answer
  [ C1 ! Message || Message <- Foreign ],
  ?NO_MESSAGE,

  C1 ! ?reply(Tag1, Queued),
  ?assertEqual(#add_held_locks{ref = Ref, held = Held}, ?RECEIVE(#add_held_locks{})),
  ?assertEqual(timeout, elock_test_utils:result(R, ?QUIET)),
  ?assertEqual({messages, Foreign}, process_info(C1, messages)),

  % a new attempt: asked under the new tag, answered once more
  C1 ! ?reply(Tag1, #retry{ref = Ref}),
  #request{tag = Tag2} = Request2 = ?RECEIVE(#request{}),
  ?assertNotEqual(Tag1, Tag2),
  ?assertEqual(Request#request{queue = 3, proxy = C1, tag = Tag2}, Request2),
  C1 ! ?reply(Tag1, Queued),
  ?NO_MESSAGE,
  C1 ! ?reply(Tag2, Queued),
  ?assertEqual(#add_held_locks{ref = Ref, held = Held}, ?RECEIVE(#add_held_locks{})),
  ?assert(elock_test_utils:pending(R)),

  C1 ! ?reply(Tag2, #locked{ref = Ref}),
  ?assertEqual({ok, {ok, Self}}, elock_test_utils:result(R, ?DEADLINE)),
  ?assertEqual({messages, Foreign ++ [?reply(Tag1, Queued)]}, process_info(C1, messages)),
  % the managers of the held locks are not the business of the client
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

  R1 = elock_test_utils:cast(C1, fun()-> elock_manager:lock(Request, #{}) end),
  {fake_got, Fake, #request{tag = Tag} = Got} = ?RECEIVE({fake_got, Fake, _}),
  ?assert(is_reference(Tag)),
  ?assertEqual(Request#request{queue = 2, proxy = C1, tag = Tag}, Got),
  elock_test_utils:wait_dead(Fake),

  {ok, {ok, Manager}} = elock_test_utils:result(R1, ?DEADLINE),
  ?assert(is_pid(Manager)),
  ?assertNotEqual(Fake, Manager),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{?TERM, Manager, 1}]),
  ?WAIT(elock_test_utils:managers() =:= [Manager]),
  ?assertEqual({messages, []}, process_info(C1, messages)),

  MonRef = erlang:monitor(process, Manager),
  Manager ! #unlock{ref = Ref},
  ?assertEqual({'DOWN', MonRef, process, Manager, normal}, ?RECEIVE({'DOWN', MonRef, process, Manager, _})),
  ?assertEqual([], elock_test_utils:locks(Scope)),
  ?NO_MESSAGE,
  elock_test_utils:stop(C1).

%%-----------------------------------------------------------------
%%  The manager dies between #queued{} and the answer of the client:
%%  the answer is lost with it. The client takes a new ticket, the
%%  manager it finds then (the test process, which has taken the
%%  entry over) gets the request under a new tag, asks again and
%%  gets the held map
%%-----------------------------------------------------------------
manager_dies_after_queued_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),
  Fake = spawn(fun()->
    receive
      #request{ref = Ref, proxy = Proxy, tag = Tag} = Request->
        Self ! {fake_got, self(), Request},
        receive
          go->
            Proxy ! ?reply(Tag, #queued{ref = Ref, manager = self(), node = node()})
        end
    end
  end),
  true = ets:insert(Scope, {?TERM, Fake, 1}),
  C1 = elock_test_utils:client(),
  M1 = elock_test_utils:collector(),
  Held = #{ {other_scope, t1, node()} => M1 },
  #request{ref = Ref} = Request = request(Scope, undefined, C1, false, #{held => Held}),

  R1 = elock_test_utils:cast(C1, fun()-> elock_manager:lock(Request, Held) end),
  {fake_got, Fake, #request{tag = Tag1} = Got} = ?RECEIVE({fake_got, Fake, _}),
  ?assertEqual(Request#request{queue = 2, proxy = C1, tag = Tag1}, Got),

  % the next manager is there by the time the client comes back
  true = ets:insert(Scope, {?TERM, Self, 2}),
  Fake ! go,
  elock_test_utils:wait_dead(Fake),

  #request{tag = Tag2} = Request2 = ?RECEIVE(#request{}),
  ?assertNotEqual(Tag1, Tag2),
  ?assertEqual(Request#request{queue = 3, proxy = C1, tag = Tag2}, Request2),
  % the answer to the dead manager has not come here
  ?NO_MESSAGE,
  C1 ! ?reply(Tag2, #queued{ref = Ref, manager = Self, node = node()}),
  ?assertEqual(#add_held_locks{ref = Ref, held = Held}, ?RECEIVE(#add_held_locks{})),
  ?assert(elock_test_utils:pending(R1)),

  C1 ! ?reply(Tag2, #locked{ref = Ref}),
  ?assertEqual({ok, {ok, Self}}, elock_test_utils:result(R1, ?DEADLINE)),
  ?assertEqual({messages, []}, process_info(C1, messages)),
  ?NO_MESSAGE,
  elock_test_utils:stop(C1).

%%-----------------------------------------------------------------
%%  The wait for the verdict does not scan the messages the process
%%  had before the attempt: the monitor reference made in lock/2
%%  marks the mailbox position, it is passed on as it is and every
%%  clause of the receive of wait_verdict/4 matches it, its first
%%  parameter. A refactoring that hides the reference from the
%%  compiler (it goes into a record, the receive takes it out)
%%  passes every other test and only costs the scan
%%-----------------------------------------------------------------
receive_marker_test(_Config)->
  ?assertEqual([
    {{lock, 2}, reserved_receive_marker},
    {{lock, 2}, passed_marker},
    {{wait_verdict, 4}, {used_receive_marker, {parameter, 1}}}
  ], [ Info || {{Function, _Arity}, _} = Info <- elock_test_utils:recv_opt_info(elock_manager),
    lists:member(Function, [lock, wait_verdict]) ]).

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
%%  the queue empty, the lock still shared. It has never waited:
%%  it is not asked for the locks it holds, the graph is not
%%  touched, the holder keeps its held count
%%-----------------------------------------------------------------
add_request_shared_joins_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, true,
    #{held => #{ {other_scope, t1, node()} => M1, {Scope, t2, node()} => M1 }}),
  State0 = initial_state(Scope, Req1),

  State1 = elock_manager:add_request(Req2, State0),

  #state{clients = #{ C2 := #client{monitor_ref = Mon2} }} = State1,
  ?assert(is_reference(Mon2)),
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1}, Ref2 => {true, C2} },
    requests = (State0#state.requests)#{
      Ref2 => #req{
        client = C2, ref = Ref2, queue = 2, proxy = undefined, tag = undefined,
        shared = true, held_count = 2, has_lock = true, timer = undefined
      }
    },
    clients = (State0#state.clients)#{
      C2 => #client{ requests = #{ Ref2 => true }, monitor_ref = Mon2 }
    },
    can_share = true
  }), plain(State1)),
  ?assertEqual([?reply(Tag2, #locked{ref = Ref2})], elock_test_utils:collected(C2, 1)),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}]), monitors()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  An exclusive request queues behind a holder, and a shared one
%%  behind an exclusive holder: the request waits with its proxy
%%  and its tag, the client is registered, nothing is sent
%%-----------------------------------------------------------------
add_request_exclusive_queues_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),

  % exclusive behind shared
  Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  #state{clients = #{ C2 := #client{monitor_ref = Mon2} }} = State1,
  ?assert(is_reference(Mon2)),
  ?assertEqual(plain(State0#state{
    queue = gb_sets:from_list([{2, Ref2}]),
    requests = (State0#state.requests)#{
      Ref2 => #req{
        client = C2, ref = Ref2, queue = 2, proxy = C2, tag = Tag2,
        shared = false, held_count = 0, has_lock = false, timer = undefined
      }
    },
    clients = (State0#state.clients)#{
      C2 => #client{ requests = #{ Ref2 => false }, monitor_ref = Mon2 }
    }
  }), plain(State1)),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}]), monitors()),
  ?NO_MESSAGE,

  % shared behind exclusive
  Req1x = request(Scope, 1, C1, false),
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 2, C3, true),
  State0x = initial_state(Scope, Req1x),
  State1x = elock_manager:add_request(Req3, State0x),
  #state{clients = #{ C3 := #client{monitor_ref = Mon3} }} = State1x,
  ?assertEqual(plain(State0x#state{
    queue = gb_sets:from_list([{2, Ref3}]),
    requests = (State0x#state.requests)#{
      Ref3 => #req{
        client = C3, ref = Ref3, queue = 2, proxy = C3, tag = Tag3,
        shared = true, held_count = 0, has_lock = false, timer = undefined
      }
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
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C3, true),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),

  State2 = elock_manager:add_request(Req3, State1),

  #state{clients = #{ C3 := #client{monitor_ref = Mon3} }} = State2,
  ?assertEqual(plain(State1#state{
    queue = gb_sets:from_list([{2, Ref2}, {3, Ref3}]),
    requests = (State1#state.requests)#{
      Ref3 => #req{
        client = C3, ref = Ref3, queue = 3, proxy = C3, tag = Tag3,
        shared = true, held_count = 0, has_lock = false, timer = undefined
      }
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
  #request{ref = Ref1, tag = Tag1} = Req1 = request(Scope, 2, C1, true),
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, false),
  State0 = empty_state(Scope),

  State1 = elock_manager:add_request(Req1, State0),
  #state{clients = #{ C1 := #client{monitor_ref = Mon1} }} = State1,
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1} },
    requests = #{
      Ref1 => #req{
        client = C1, ref = Ref1, queue = 2, proxy = undefined, tag = undefined,
        shared = true, held_count = 0, has_lock = true, timer = undefined
      }
    },
    clients = #{
      C1 => #client{ requests = #{ Ref1 => true }, monitor_ref = Mon1 }
    },
    can_share = true
  }), plain(State1)),
  ?assertEqual([?reply(Tag1, #locked{ref = Ref1})], elock_test_utils:collected(C1, 1)),

  State2 = elock_manager:add_request(Req2, State0),
  #state{clients = #{ C2 := #client{monitor_ref = Mon2} }} = State2,
  ?assertEqual(plain(State0#state{
    holders = #{ Ref2 => {false, C2} },
    requests = #{
      Ref2 => #req{
        client = C2, ref = Ref2, queue = 2, proxy = undefined, tag = undefined,
        shared = false, held_count = 0, has_lock = true, timer = undefined
      }
    },
    clients = #{
      C2 => #client{ requests = #{ Ref2 => false }, monitor_ref = Mon2 }
    },
    can_share = false
  }), plain(State2)),
  ?assertEqual([?reply(Tag2, #locked{ref = Ref2})], elock_test_utils:collected(C2, 1)),
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
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, true),
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C1, false),
  #request{ref = Ref4, tag = Tag4} = Req4 = request(Scope, 4, C3, true),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  [?reply(Tag2, #locked{ref = Ref2})] = elock_test_utils:collected(C2, 1),
  State2 = elock_manager:add_request(Req3, State1),
  ?assertEqual(Req3, State2#state.barging),

  State3 = elock_manager:add_request(Req4, State2),

  ?assertEqual([{4, Ref4}], gb_sets:to_list(State3#state.queue)),
  ?assertEqual(#{ Ref1 => {true, C1}, Ref2 => {true, C2} }, State3#state.holders),
  ?assertEqual(true, State3#state.can_share),
  #state{requests = #{ Ref4 := #req{ has_lock = false, proxy = C3, tag = Tag4 } }} = State3,
  ?NO_MESSAGE,

  % the other holder leaves: the upgrade goes first, the newcomer waits
  State4 = elock_manager:handle_unlock(Ref2, State3),
  ?assertEqual([?reply(Tag3, #locked{ref = Ref3})], elock_test_utils:collected(C1, 1)),
  ?assertEqual(#{ Ref1 => {true, C1}, Ref3 => {false, C1} }, State4#state.holders),
  ?assertEqual(undefined, State4#state.barging),
  ?assertEqual(false, State4#state.can_share),
  ?assertEqual([{4, Ref4}], gb_sets:to_list(State4#state.queue)),
  ?NO_MESSAGE,

  % the exclusive ref is released: the lock is shared again, the newcomer joins
  State5 = elock_manager:handle_unlock(Ref3, State4),
  ?assertEqual([?reply(Tag4, #locked{ref = Ref4})], elock_test_utils:collected(C3, 1)),
  #state{clients = #{ C3 := #client{monitor_ref = Mon3} }} = State5,
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1}, Ref4 => {true, C3} },
    requests = (State0#state.requests)#{
      Ref4 => #req{
        client = C3, ref = Ref4, queue = 4, proxy = undefined, tag = undefined,
        shared = true, held_count = 0, has_lock = true, timer = undefined
      }
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
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, false, #{timeout => 100}),
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C3, true),
  State0 = initial_state(Scope, Req1),

  State1 = elock_manager:add_request(Req2, State0),
  #state{requests = #{ Ref2 := #req{timer = Timer} }} = State1,
  ?assert(is_reference(Timer)),
  State2 = elock_manager:add_request(Req3, State1),
  ?assertEqual([{2, Ref2}, {3, Ref3}], gb_sets:to_list(State2#state.queue)),
  ?assertEqual({timeout, Timer, {timeout, Ref2}}, ?RECEIVE({timeout, Timer, _})),

  State3 = elock_manager:handle_timeout(Ref2, State2),

  ?assertEqual([?reply(Tag2, #timeout{ref = Ref2})], elock_test_utils:collected(C2, 1)),
  ?assertEqual([?reply(Tag3, #locked{ref = Ref3})], elock_test_utils:collected(C3, 1)),
  #state{clients = #{ C3 := #client{monitor_ref = Mon3} }} = State3,
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1}, Ref3 => {true, C3} },
    requests = (State0#state.requests)#{
      Ref3 => #req{
        client = C3, ref = Ref3, queue = 3, proxy = undefined, tag = undefined,
        shared = true, held_count = 0, has_lock = true, timer = undefined
      }
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
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, true, #{timeout => 100}),
  State0 = initial_state(Scope, Req1),
  % the shared request joins at once, it never waited: no timer
  State1 = elock_manager:add_request(Req2, State0),
  #state{requests = #{ Ref2 := #req{timer = undefined, has_lock = true} }} = State1,
  [?reply(Tag2, #locked{ref = Ref2})] = elock_test_utils:collected(C2, 1),

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
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  #state{clients = #{ C2 := #client{monitor_ref = Mon2} }} = State1,

  State2 = elock_manager:handle_unlock(Ref1, State1),

  ?assertEqual([?reply(Tag2, #locked{ref = Ref2})], elock_test_utils:collected(C2, 1)),
  ?assertEqual(plain(State0#state{
    holders = #{ Ref2 => {false, C2} },
    requests = #{
      Ref2 => #req{
        client = C2, ref = Ref2, queue = 2, proxy = undefined, tag = undefined,
        shared = false, held_count = 0, has_lock = true, timer = undefined
      }
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
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, true),
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C3, true),
  #request{ref = Ref4, tag = Tag4} = Req4 = request(Scope, 4, C4, false),
  #request{ref = Ref5, tag = Tag5} = Req5 = request(Scope, 5, C5, true),
  State0 = initial_state(Scope, Req1),
  State1 = lists:foldl(fun elock_manager:add_request/2, State0, [Req2, Req3, Req4, Req5]),
  ?assertEqual([{2, Ref2}, {3, Ref3}, {4, Ref4}, {5, Ref5}], gb_sets:to_list(State1#state.queue)),
  ?NO_MESSAGE,

  % the two shared join together
  State2 = elock_manager:handle_unlock(Ref1, State1),
  ?assertEqual([?reply(Tag2, #locked{ref = Ref2})], elock_test_utils:collected(C2, 1)),
  ?assertEqual([?reply(Tag3, #locked{ref = Ref3})], elock_test_utils:collected(C3, 1)),
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
  ?assertEqual([?reply(Tag4, #locked{ref = Ref4})], elock_test_utils:collected(C4, 1)),
  ?assertEqual(#{ Ref4 => {false, C4} }, State4#state.holders),
  ?assertEqual([{5, Ref5}], gb_sets:to_list(State4#state.queue)),
  ?assertEqual(false, State4#state.can_share),
  ?NO_MESSAGE,

  % the last shared after the exclusive
  State5 = elock_manager:handle_unlock(Ref4, State4),
  ?assertEqual([?reply(Tag5, #locked{ref = Ref5})], elock_test_utils:collected(C5, 1)),
  #state{clients = #{ C5 := #client{monitor_ref = Mon5} }} = State5,
  ?assertEqual(plain(State0#state{
    holders = #{ Ref5 => {true, C5} },
    requests = #{
      Ref5 => #req{
        client = C5, ref = Ref5, queue = 5, proxy = undefined, tag = undefined,
        shared = true, held_count = 0, has_lock = true, timer = undefined
      }
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
%%  the client): the proxy is told where the request queued up and
%%  asked for the locks it holds - #queued{} with the tag of the
%%  request; the withdrawal of the request by #unlock{} kills the
%%  proxy and drops the request and the client; a timeout and a
%%  deadlock leave the proxy alive - it delivers the verdict; a dead
%%  client's waiting request loses its proxy as well
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
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, false, #{proxy => P2, nodes => Nodes}),
  State1 = elock_manager:add_request(Req2, State0),
  ?assertEqual([?reply(Tag2, #queued{ ref = Ref2, manager = Self, node = node() })],
    elock_test_utils:collected(P2, 1)),
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
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C3, false, #{proxy => P3, nodes => Nodes, timeout => 100}),
  State3 = elock_manager:add_request(Req3, State0),
  [?reply(Tag3, #queued{ref = Ref3})] = elock_test_utils:collected(P3, 1),
  #state{requests = #{ Ref3 := #req{ timer = Timer3, proxy = P3, tag = Tag3 } }} = State3,
  ?assertEqual({timeout, Timer3, {timeout, Ref3}}, ?RECEIVE({timeout, Timer3, _})),
  State4 = elock_manager:handle_timeout(Ref3, State3),
  ?assertEqual([?reply(Tag3, #timeout{ref = Ref3})], elock_test_utils:collected(P3, 1)),
  ?assert(is_process_alive(P3)),
  ?assertEqual(plain(State0), plain(State4)),
  ?NO_MESSAGE,

  % the deadlock verdict is delivered by the proxy
  C4 = elock_test_utils:collector(),
  P4 = elock_test_utils:collector(),
  #request{ref = Ref4, tag = Tag4} = Req4 = request(Scope, 4, C4, false, #{proxy => P4, nodes => Nodes}),
  State5 = elock_manager:add_request(Req4, State0),
  [?reply(Tag4, #queued{ref = Ref4})] = elock_test_utils:collected(P4, 1),
  State6 = elock_manager:handle_deadlock(#deadlock{ref = Ref4, winner = ?WINNER}, State5),
  ?assertEqual([?reply(Tag4, #deadlock{ref = Ref4, winner = ?WINNER})], elock_test_utils:collected(P4, 1)),
  ?assert(is_process_alive(P4)),
  ?assertEqual(plain(State0), plain(State6)),
  ?NO_MESSAGE,

  % the client is gone: nobody to serve, the proxy is killed
  C5 = elock_test_utils:collector(),
  P5 = elock_test_utils:collector(),
  #request{ref = Ref5, tag = Tag5} = Req5 = request(Scope, 5, C5, false, #{proxy => P5, nodes => Nodes}),
  State7 = elock_manager:add_request(Req5, State0),
  [?reply(Tag5, #queued{ref = Ref5})] = elock_test_utils:collected(P5, 1),
  State8 = elock_manager:handle_down(C5, State7),
  elock_test_utils:wait_dead(P5),
  ?assertEqual(plain(State0), plain(State8)),
  ?assertEqual([{process, C1}], monitors()),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A client that only waits asks again, shared or exclusive: both
%%  requests stay queued behind the exclusive holder, in ticket order
%%-----------------------------------------------------------------
waiting_client_requests_again_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, false),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  #state{clients = #{ C2 := #client{monitor_ref = Mon2} }} = State1,

  lists:foreach(
    fun(Shared)->
      #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C2, Shared),
      State2 = elock_manager:add_request(Req3, State1),
      ?assertEqual(plain(State1#state{
        queue = gb_sets:from_list([{2, Ref2}, {3, Ref3}]),
        requests = (State1#state.requests)#{
          Ref3 => #req{
            client = C2, ref = Ref3, queue = 3, proxy = C2, tag = Tag3,
            shared = Shared, held_count = 0, has_lock = false, timer = undefined
          }
        },
        clients = (State1#state.clients)#{
          C2 => #client{
            requests = #{ Ref2 => false, Ref3 => Shared },
            monitor_ref = Mon2
          }
        }
      }), plain(State2)),
      ?assertEqual(#{ Ref1 => {false, C1} }, State2#state.holders),
      ?assertMatch(#req{has_lock = false}, maps:get(Ref2, State2#state.requests)),
      ?assertEqual(lists:sort([{process, C1}, {process, C2}]), monitors()),
      ?NO_MESSAGE
    end,
    [true, false]
  ).

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
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C1, true),
  #request{ref = Ref4, tag = Tag4} = Req4 = request(Scope, 4, C1, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),

  State2 = elock_manager:add_request(Req3, State1),
  ?assertEqual([?reply(Tag3, #locked{ref = Ref3})], elock_test_utils:collected(C1, 1)),
  State3 = elock_manager:add_request(Req4, State2),
  ?assertEqual([?reply(Tag4, #locked{ref = Ref4})], elock_test_utils:collected(C1, 1)),

  #state{clients = #{ C1 := #client{monitor_ref = Mon1} }} = State0,
  ?assertEqual(plain(State1#state{
    holders = #{ Ref1 => {false, C1}, Ref3 => {true, C1}, Ref4 => {false, C1} },
    requests = (State1#state.requests)#{
      Ref3 => #req{
        client = C1, ref = Ref3, queue = 3, proxy = undefined, tag = undefined,
        shared = true, held_count = 0, has_lock = true, timer = undefined
      },
      Ref4 => #req{
        client = C1, ref = Ref4, queue = 4, proxy = undefined, tag = undefined,
        shared = false, held_count = 0, has_lock = true, timer = undefined
      }
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
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C1, true),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),

  State2 = elock_manager:add_request(Req3, State1),

  ?assertEqual([?reply(Tag3, #locked{ref = Ref3})], elock_test_utils:collected(C1, 1)),
  #state{clients = #{ C1 := #client{monitor_ref = Mon1} }} = State0,
  ?assertEqual(plain(State1#state{
    holders = #{ Ref1 => {true, C1}, Ref3 => {true, C1} },
    requests = (State1#state.requests)#{
      Ref3 => #req{
        client = C1, ref = Ref3, queue = 3, proxy = undefined, tag = undefined,
        shared = true, held_count = 0, has_lock = true, timer = undefined
      }
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
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C1, false),
  State0 = initial_state(Scope, Req1),

  State1 = elock_manager:add_request(Req2, State0),

  ?assertEqual([?reply(Tag2, #locked{ref = Ref2})], elock_test_utils:collected(C1, 1)),
  #state{clients = #{ C1 := #client{monitor_ref = Mon1} }} = State0,
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1}, Ref2 => {false, C1} },
    requests = (State0#state.requests)#{
      Ref2 => #req{
        client = C1, ref = Ref2, queue = 2, proxy = undefined, tag = undefined,
        shared = false, held_count = 0, has_lock = true, timer = undefined
      }
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
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, true),
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C1, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  [?reply(Tag2, #locked{ref = Ref2})] = elock_test_utils:collected(C2, 1),

  State2 = elock_manager:add_request(Req3, State1),

  #state{clients = #{ C1 := #client{monitor_ref = Mon1} }} = State0,
  ?assertEqual(plain(State1#state{
    requests = (State1#state.requests)#{
      Ref3 => #req{
        client = C1, ref = Ref3, queue = 3, proxy = C1, tag = Tag3,
        shared = false, held_count = 0, has_lock = false, timer = undefined
      }
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

  ?assertEqual([?reply(Tag3, #locked{ref = Ref3})], elock_test_utils:collected(C1, 1)),
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1}, Ref3 => {false, C1} },
    requests = (State0#state.requests)#{
      Ref3 => #req{
        client = C1, ref = Ref3, queue = 3, proxy = undefined, tag = undefined,
        shared = false, held_count = 0, has_lock = true, timer = undefined
      }
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
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, true),
  Req3 = request(Scope, 3, C1, false),
  #request{ref = Ref4, tag = Tag4} = Req4 = request(Scope, 4, C2, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  [?reply(Tag2, #locked{ref = Ref2})] = elock_test_utils:collected(C2, 1),
  State2 = elock_manager:add_request(Req3, State1),
  ?assertEqual(Req3, State2#state.barging),

  ?assertEqual(State2, elock_manager:add_request(Req4, State2)),

  ?assertEqual([?reply(Tag4, #deadlock{ref = Ref4, winner = {Scope, ?TERM, node()}})],
    elock_test_utils:collected(C2, 1)),
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
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, true),
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C1, false, #{timeout => 100}),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  [?reply(Tag2, #locked{ref = Ref2})] = elock_test_utils:collected(C2, 1),
  State2 = elock_manager:add_request(Req3, State1),
  #state{requests = #{ Ref3 := #req{timer = Timer, has_lock = false} }, barging = Req3} = State2,
  ?assert(is_reference(Timer)),
  ?assertEqual({timeout, Timer, {timeout, Ref3}}, ?RECEIVE({timeout, Timer, _})),

  State3 = elock_manager:handle_timeout(Ref3, State2),

  ?assertEqual([?reply(Tag3, #timeout{ref = Ref3})], elock_test_utils:collected(C1, 1)),
  ?assertEqual(plain(State1), plain(State3)),
  ?assertEqual(undefined, State3#state.barging),
  ?assertEqual(#{ Ref1 => {true, C1}, Ref2 => {true, C2} }, State3#state.holders),
  ?assertEqual(lists:sort([{process, C1}, {process, C2}]), monitors()),
  ?NO_MESSAGE,

  % a deadlock verdict on the pending upgrade dequeues it the same way
  State4 = elock_manager:handle_deadlock(#deadlock{ref = Ref3, winner = ?WINNER}, State2),
  ?assertEqual([?reply(Tag3, #deadlock{ref = Ref3, winner = ?WINNER})], elock_test_utils:collected(C1, 1)),
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
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, false),
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C3, true),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  State2 = elock_manager:add_request(Req3, State1),
  ?assertEqual([{2, Ref2}, {3, Ref3}], gb_sets:to_list(State2#state.queue)),

  State3 = elock_manager:handle_deadlock(#deadlock{ref = Ref2, winner = ?WINNER}, State2),

  ?assertEqual([?reply(Tag2, #deadlock{ref = Ref2, winner = ?WINNER})], elock_test_utils:collected(C2, 1)),
  ?assertEqual([?reply(Tag3, #locked{ref = Ref3})], elock_test_utils:collected(C3, 1)),
  #state{clients = #{ C3 := #client{monitor_ref = Mon3} }} = State3,
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1}, Ref3 => {true, C3} },
    requests = (State0#state.requests)#{
      Ref3 => #req{
        client = C3, ref = Ref3, queue = 3, proxy = undefined, tag = undefined,
        shared = true, held_count = 0, has_lock = true, timer = undefined
      }
    },
    clients = (State0#state.clients)#{
      C3 => #client{ requests = #{ Ref3 => true }, monitor_ref = Mon3 }
    },
    can_share = true
  }), plain(State3)),
  ?assertEqual(lists:sort([{process, C1}, {process, C3}]), monitors()),

  % a holder is ignored, an unknown ref is ignored
  ?assertEqual(State3,
    elock_manager:handle_deadlock(#deadlock{ref = Ref1, winner = ?WINNER}, State3)),
  ?assertEqual(State3,
    elock_manager:handle_deadlock(#deadlock{ref = Ref3, winner = ?WINNER}, State3)),
  ?assertEqual(State3,
    elock_manager:handle_deadlock(#deadlock{ref = make_ref(), winner = ?WINNER}, State3)),
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
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, true),
  Req3 = request(Scope, 3, C2, false),
  #request{ref = Ref4, tag = Tag4} = Req4 = request(Scope, 4, C4, false),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  [?reply(Tag2, #locked{ref = Ref2})] = elock_test_utils:collected(C2, 1),
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
      Ref4 => #req{
        client = C4, ref = Ref4, queue = 4, proxy = C4, tag = Tag4,
        shared = false, held_count = 0, has_lock = false, timer = undefined
      }
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
  ?assertEqual([?reply(Tag4, #locked{ref = Ref4})], elock_test_utils:collected(C4, 1)),
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
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, false),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C3, false),
  State0 = initial_state(Scope, Req1),

  State1 = elock_manager:handle_request(Req2, State0),

  #state{clients = #{ C2 := #client{monitor_ref = Mon2} }} = State1,
  ?assertEqual(plain(State0#state{
    queue = gb_sets:from_list([{2, Ref2}]),
    requests = (State0#state.requests)#{
      Ref2 => #req{
        client = C2, ref = Ref2, queue = 2, proxy = C2, tag = Tag2,
        shared = false, held_count = 0, has_lock = false, timer = undefined
      }
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
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, false),
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C3, false),
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
      Ref3 => #req{
        client = C3, ref = Ref3, queue = 3, proxy = C3, tag = Tag3,
        shared = false, held_count = 0, has_lock = false, timer = undefined
      }
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
  ?assertEqual([?reply(Tag2, #retry{ref = Ref2})], elock_test_utils:collected(C2, 1)),
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
  #request{ref = RefStale, tag = TagStale} = ReqStale = request(Scope, 1, C0, false),
  #request{ref = RefBehind, tag = TagBehind} = ReqBehind = request(Scope, 0, C0, false),
  Req3 = request(Scope, 3, C3, false),
  State0 = initial_state(Scope, Req1),

  % the ticket equal to last
  State1 = elock_manager:handle_postponed(State0#state{ postponed = [ReqStale], postpone_timer = undefined }),
  ?assertEqual([?reply(TagStale, #retry{ref = RefStale})], elock_test_utils:collected(C0, 1)),
  ?assertEqual(State0, State1),
  ?assertEqual([{process, C1}], monitors()),
  ?NO_MESSAGE,

  % a ticket behind last followed by a ticket ahead: retry, then wait
  State2 = elock_manager:handle_postponed(State0#state{ postponed = [ReqBehind, Req3], postpone_timer = undefined }),
  ?assertEqual([?reply(TagBehind, #retry{ref = RefBehind})], elock_test_utils:collected(C0, 1)),
  #state{postpone_timer = Timer} = State2,
  ?assert(is_reference(Timer)),
  ?assertEqual(State0#state{ postponed = [Req3], postpone_timer = Timer }, State2),
  ?assertEqual([{process, C1}], monitors()),
  ?assertEqual({timeout, Timer, postpone_timeout}, ?RECEIVE({timeout, Timer, _})),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A request postponed for a missing ticket is asked for the locks
%%  it holds only when it is taken into the queue, by the postpone
%%  timeout or by the ticket that closes the gap: no #queued{} while
%%  it is postponed
%%-----------------------------------------------------------------
postponed_asked_when_taken_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  Req1 = request(Scope, 1, C1, false),
  #request{ref = Ref2} = Req2 = request(Scope, 2, C2, false),
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C3, false,
    #{held => #{ {other_scope, other_term, node()} => M1 }}),
  Queued = ?reply(Tag3, #queued{ ref = Ref3, manager = Self, node = node() }),
  State0 = initial_state(Scope, Req1),

  State1 = elock_manager:handle_request(Req3, State0),
  #state{postpone_timer = Timer} = State1,
  ?assertEqual(State0#state{ postponed = [Req3], postpone_timer = Timer }, State1),

  % the missing ticket did not come, nobody has been asked meanwhile:
  % the request is taken and asked
  ?assertEqual({timeout, Timer, postpone_timeout}, ?RECEIVE({timeout, Timer, _})),
  ?NO_MESSAGE,
  State2 = elock_manager:handle_postpone_timeout(Timer, State1),
  ?assertEqual([Queued], elock_test_utils:collected(C3, 1)),
  ?assertEqual([{3, Ref3}], gb_sets:to_list(State2#state.queue)),
  ?assertEqual(undefined, State2#state.graph),
  ?NO_MESSAGE,

  % the missing ticket comes instead: both are taken, the one that
  % holds something is asked
  State3 = elock_manager:handle_request(Req2, State1),
  ?assertEqual([Queued], elock_test_utils:collected(C3, 1)),
  ?assertEqual([{2, Ref2}, {3, Ref3}], gb_sets:to_list(State3#state.queue)),
  ?assertEqual(undefined, State3#state.graph),
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
%%  notify_queued/1: #queued{} with the tag of the request goes to
%%  the proxy when the request holds or may hold something - the
%%  client held locks when it asked (held_count > 0) or the request
%%  names several nodes; a request that holds nothing and names one
%%  node tells nothing
%%-----------------------------------------------------------------
notify_queued_test(_Config)->
  Self = self(),
  Client = elock_test_utils:collector(),
  Proxy = elock_test_utils:collector(),

  lists:foreach(
    fun({HeldCount, Nodes, Asked})->
      Ref = make_ref(),
      Tag = make_ref(),
      Queued = ?reply(Tag, #queued{ ref = Ref, manager = Self, node = node() }),

      % a proxy on behalf of the client
      elock_manager:notify_queued(#request{
        ref = Ref, client = Client, proxy = Proxy, tag = Tag, held_count = HeldCount, nodes = Nodes
      }),
      % the client is its own proxy
      elock_manager:notify_queued(#request{
        ref = Ref, client = Self, proxy = Self, tag = Tag, held_count = HeldCount, nodes = Nodes
      }),

      case Asked of
        true->
          ?assertEqual([Queued], elock_test_utils:collected(Proxy, 1)),
          ?assertEqual(Queued, ?RECEIVE(?reply(Tag, #queued{})));
        false->
          ok
      end,
      % nothing else, nothing to the client behind the proxy
      ?NO_MESSAGE
    end,
    [
      % one node, the client held nothing: it can not be on a cycle
      {0, [node()], false},
      {0, ['n2@host'], false},
      % one node, the client held locks
      {1, [node()], true},
      {5, ['n2@host'], true},
      % several nodes, the client held nothing: the grants are to come
      {0, [node(), 'n2@host'], true},
      {0, ['n2@host', 'n3@host', 'n4@host'], true},
      % several nodes, the client held locks
      {2, [node(), 'n2@host', 'n3@host'], true}
    ]
  ).

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
%%  A holder has no proxy and no tag, the manager has nothing to
%%  tell it any more. new_req/1 takes both from the request, with
%%  the held count; locked/2 sends #locked{} with the tag to the
%%  proxy and drops both from the #req{}, the timer is stopped, the
%%  held count stays. The first holder has neither as well: the
%%  state a real manager enters its loop with (see init_state/1) is
%%  the one initial_state/2 builds
%%-----------------------------------------------------------------
holder_has_no_tag_test(Config)->
  Scope = ?config(scope, Config),
  C0 = elock_test_utils:collector(),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  P2 = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  Held = #{ {other_scope, t1, node()} => M1, {other_scope, t2, node()} => M1, {Scope, t3, node()} => M1 },
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, true,
    #{proxy => P2, held => Held, nodes => [node(), 'n2@host']}),
  State0 = initial_state(Scope, Req1),

  % the waiter
  Req = elock_manager:new_req(Req2),
  ?assertEqual(#req{
    client = C2, ref = Ref2, queue = 2, proxy = P2, tag = Tag2,
    shared = true, held_count = 3, has_lock = false, timer = undefined
  }, Req),
  Timeout = 100,
  #req{timer = Timer} = Waiting = elock_manager:start_timer(Req, Timeout),
  ?assert(is_reference(Timer)),

  % the holder
  State1 = elock_manager:locked(Waiting, State0),

  ?assertEqual([?reply(Tag2, #locked{ref = Ref2})], elock_test_utils:collected(P2, 1)),
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1}, Ref2 => {true, C2} },
    requests = (State0#state.requests)#{
      Ref2 => #req{
        client = C2, ref = Ref2, queue = 2, proxy = undefined, tag = undefined,
        shared = true, held_count = 3, has_lock = true, timer = undefined
      }
    }
  }), plain(State1)),
  % the timer does not fire
  elock_test_utils:no_message(2 * Timeout),

  % the first holder: the request as elock:lock/4 builds it, the winner
  % of the ticket 1 starts the manager with it
  #request{ref = Ref0} = Req0 = request(Scope, undefined, C0, false, #{held => Held}),
  true = ets:insert(Scope, {?TERM, 0, 1}),
  {StandIn, Init} = init_state(Req0),
  ?assertEqual([{?TERM, StandIn, 1}], elock_test_utils:locks(Scope)),
  #state{clients = #{ C0 := #client{monitor_ref = Mon0} }} = Init,
  ?assert(is_reference(Mon0)),
  Expected = initial_state(Scope, Req0),
  ?assertEqual(plain(Expected#state{
    clients = #{ C0 => #client{ requests = #{ Ref0 => false }, monitor_ref = Mon0 } }
  }), plain(Init)),
  ?assertMatch(#{ Ref0 := #req{ proxy = undefined, tag = undefined, has_lock = true } }, Init#state.requests),
  true = ets:delete(Scope, ?TERM),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A request without a tag (#request.tag is undefined, its sender
%%  named none): the manager has nothing else to put on its
%%  messages, every one of them goes as ?reply(undefined, Message)
%%-----------------------------------------------------------------
untagged_request_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  P2 = elock_test_utils:collector(),
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, false),
  Tagged = request(Scope, 2, C2, false, #{proxy => P2, nodes => [node(), 'n2@host']}),
  #request{ref = Ref2} = Req2 = Tagged#request{tag = undefined},
  State0 = initial_state(Scope, Req1),

  % #queued{}
  State1 = elock_manager:add_request(Req2, State0),
  ?assertEqual([?reply(undefined, #queued{ ref = Ref2, manager = Self, node = node() })],
    elock_test_utils:collected(P2, 1)),
  ?assertMatch(#{ Ref2 := #req{ proxy = P2, tag = undefined, has_lock = false } }, State1#state.requests),

  % #deadlock{}, #timeout{}, #locked{}
  elock_manager:handle_deadlock(#deadlock{ref = Ref2, winner = ?WINNER}, State1),
  ?assertEqual([?reply(undefined, #deadlock{ref = Ref2, winner = ?WINNER})], elock_test_utils:collected(P2, 1)),
  elock_manager:handle_timeout(Ref2, State1),
  ?assertEqual([?reply(undefined, #timeout{ref = Ref2})], elock_test_utils:collected(P2, 1)),
  elock_manager:handle_unlock(Ref1, State1),
  ?assertEqual([?reply(undefined, #locked{ref = Ref2})], elock_test_utils:collected(P2, 1)),

  % #retry{}
  ?assertEqual(State0, elock_manager:handle_request(Req2#request{queue = 1}, State0)),
  ?assertEqual([?reply(undefined, #retry{ref = Ref2})], elock_test_utils:collected(P2, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  enqueue/2 does not touch the graph and probes nobody, whatever
%%  the request holds: a waiter that holds something is only asked
%%  for its locks (#queued{}), one that holds nothing is not even
%%  asked. The waiter joins the graph when its answer comes in
%%  through handle_add_held_locks/2, the managers of its locks are
%%  probed then; it leaves the graph when it is granted, and when
%%  it stops waiting any other way: withdrawn by #unlock{}, timed
%%  out, its client gone
%%-----------------------------------------------------------------
enqueue_graph_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  K1 = {other_scope, other_term, node()},
  Held = #{ K1 => M1 },
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, false),
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, false, #{held => Held}),
  #request{ref = Ref3} = Req3 = request(Scope, 3, C3, false),
  State0 = initial_state(Scope, Req1),

  State1 = elock_manager:enqueue(Req2, State0),

  #state{clients = #{ C2 := #client{monitor_ref = Mon2} }} = State1,
  ?assertEqual(plain(State0#state{
    queue = gb_sets:from_list([{2, Ref2}]),
    requests = (State0#state.requests)#{
      Ref2 => #req{
        client = C2, ref = Ref2, queue = 2, proxy = C2, tag = Tag2,
        shared = false, held_count = 1, has_lock = false, timer = undefined
      }
    },
    clients = (State0#state.clients)#{
      C2 => #client{ requests = #{ Ref2 => false }, monitor_ref = Mon2 }
    },
    graph = undefined
  }), plain(State1)),
  % the client is asked, the manager of its lock is not probed
  ?assertEqual([?reply(Tag2, #queued{ ref = Ref2, manager = Self, node = node() })],
    elock_test_utils:collected(C2, 1)),
  ?NO_MESSAGE,

  % holding nothing: no graph, no question
  State2 = elock_manager:enqueue(Req3, State0),
  ?assertEqual(undefined, State2#state.graph),
  ?NO_MESSAGE,

  % the answer: the waiter joins the graph, its lock is probed
  State3 = elock_manager:handle_add_held_locks(#add_held_locks{ ref = Ref2, held = Held }, State1),
  ?assertEqual(State1#state{
    graph = #graph{
      edges = #{ K1 => #{ Ref2 => 1 } },
      index = #{ Ref2 => {1, Held} }
    }
  }, State3),
  ?assertEqual([#deadlock_probe{
    ref = Ref2,
    edge = {Scope, ?TERM, node()},
    manager = Self,
    weight = 1,
    sent_to = #{ Self => true, M1 => true }
  }], elock_test_utils:collected(M1, 1)),
  ?NO_MESSAGE,

  % the next waiter leaves the graph as it is
  State4 = elock_manager:enqueue(Req3, State3),
  ?assertEqual(State3#state.graph, State4#state.graph),
  ?assertEqual([{2, Ref2}, {3, Ref3}], gb_sets:to_list(State4#state.queue)),
  ?NO_MESSAGE,

  % the waiter is granted: it leaves the graph
  State5 = elock_manager:handle_unlock(Ref1, State4),
  ?assertEqual([?reply(Tag2, #locked{ref = Ref2})], elock_test_utils:collected(C2, 1)),
  ?assertEqual(undefined, State5#state.graph),
  ?assertEqual(#{ Ref2 => {false, C2} }, State5#state.holders),
  ?assertEqual([{3, Ref3}], gb_sets:to_list(State5#state.queue)),
  ?NO_MESSAGE,

  % it is withdrawn, it times out, its client is gone: out of the
  % graph with the queue
  lists:foreach(
    fun(Leave)->
      Left = Leave(State4),
      ?assertEqual(undefined, Left#state.graph),
      ?assertEqual([{3, Ref3}], gb_sets:to_list(Left#state.queue)),
      ?assertEqual(#{ Ref1 => {false, C1} }, Left#state.holders)
    end,
    [
      fun(State)-> elock_manager:handle_unlock(Ref2, State) end,
      fun(State)-> elock_manager:handle_timeout(Ref2, State) end,
      fun(State)-> elock_manager:handle_down(C2, State) end
    ]
  ),
  ?assertEqual([?reply(Tag2, #timeout{ref = Ref2})], elock_test_utils:collected(C2, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The pending upgrade of a client that holds the term shared (it
%%  holds at least this term, its held count is never 0):
%%  enqueue_barging/2 asks the proxy for the locks and leaves the
%%  graph alone. The answer names this very term at this manager: it
%%  is in the edges, this manager is not probed, the manager of the
%%  other lock is. The request leaves the graph when it is dequeued
%%  by a verdict and when it is granted
%%-----------------------------------------------------------------
barging_graph_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  Edge = {Scope, ?TERM, node()},
  K1 = {other_scope, other_term, node()},
  Held = #{ Edge => Self, K1 => M1 },
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, true),
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, true),
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C1, false, #{held => Held}),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  [?reply(Tag2, #locked{ref = Ref2})] = elock_test_utils:collected(C2, 1),

  State2 = elock_manager:add_request(Req3, State1),

  #state{clients = #{ C1 := #client{monitor_ref = Mon1} }} = State0,
  ?assertEqual(plain(State1#state{
    requests = (State1#state.requests)#{
      Ref3 => #req{
        client = C1, ref = Ref3, queue = 3, proxy = C1, tag = Tag3,
        shared = false, held_count = 2, has_lock = false, timer = undefined
      }
    },
    clients = (State1#state.clients)#{
      C1 => #client{ requests = #{ Ref1 => true, Ref3 => false }, monitor_ref = Mon1 }
    },
    barging = Req3,
    graph = undefined
  }), plain(State2)),
  ?assertEqual([?reply(Tag3, #queued{ ref = Ref3, manager = Self, node = node() })],
    elock_test_utils:collected(C1, 1)),
  ?NO_MESSAGE,

  % the answer
  State3 = elock_manager:handle_add_held_locks(#add_held_locks{ ref = Ref3, held = Held }, State2),
  ?assertEqual(State2#state{
    graph = #graph{
      edges = #{
        Edge => #{ Ref3 => 2 },
        K1 => #{ Ref3 => 2 }
      },
      index = #{ Ref3 => {2, Held} }
    }
  }, State3),
  ?assertEqual([#deadlock_probe{
    ref = Ref3,
    edge = Edge,
    manager = Self,
    weight = 2,
    sent_to = #{ Self => true, M1 => true }
  }], elock_test_utils:collected(M1, 1)),
  ?NO_MESSAGE,

  % dequeued by a deadlock verdict: out of the graph
  State4 = elock_manager:handle_deadlock(#deadlock{ref = Ref3, winner = ?WINNER}, State3),
  ?assertEqual([?reply(Tag3, #deadlock{ref = Ref3, winner = ?WINNER})], elock_test_utils:collected(C1, 1)),
  ?assertEqual(plain(State1), plain(State4)),
  ?NO_MESSAGE,

  % granted instead, the other holder leaves: out of the graph
  State5 = elock_manager:handle_unlock(Ref2, State3),
  ?assertEqual([?reply(Tag3, #locked{ref = Ref3})], elock_test_utils:collected(C1, 1)),
  ?assertEqual(plain(State0#state{
    holders = #{ Ref1 => {true, C1}, Ref3 => {false, C1} },
    requests = (State0#state.requests)#{
      Ref3 => #req{
        client = C1, ref = Ref3, queue = 3, proxy = undefined, tag = undefined,
        shared = false, held_count = 2, has_lock = true, timer = undefined
      }
    },
    clients = #{
      C1 => #client{ requests = #{ Ref1 => true, Ref3 => false }, monitor_ref = Mon1 }
    },
    can_share = false,
    barging = undefined,
    graph = undefined
  }), plain(State5)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  #add_held_locks{} for a waiting request: its holds join the
%%  graph with #req.held_count as the weight and are probed. The
%%  weight is the held count whatever the answer carries: 0 for a
%%  multi node request whose client held nothing, and a grant that
%%  comes later does not change it. For a holder or an unknown ref
%%  the state is identical
%%-----------------------------------------------------------------
handle_add_held_locks_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),
  Nodes = [node(), 'n2@host', 'n3@host'],
  Edge = {Scope, ?TERM, node()},
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  K1 = {other_scope, t1, node()},
  K2 = {Scope, ?TERM, 'n2@host'},
  K3 = {Scope, ?TERM, 'n3@host'},
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, false),
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, false, #{held => #{ K1 => M1 }, nodes => Nodes}),
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C3, false, #{nodes => Nodes}),
  State0 = initial_state(Scope, Req1),
  State1 = elock_manager:add_request(Req2, State0),
  [?reply(Tag2, #queued{ref = Ref2})] = elock_test_utils:collected(C2, 1),
  ?assertEqual(undefined, State1#state.graph),
  ?assertMatch(#{ Ref2 := #req{ held_count = 1, has_lock = false } }, State1#state.requests),

  % the answer to #queued{}: the lock of the client and the grant of
  % n2 in one message, the weight is the held count, not their number
  State2 = elock_manager:handle_add_held_locks(#add_held_locks{ ref = Ref2, held = #{ K1 => M1, K2 => M2 } }, State1),

  ?assertEqual(State1#state{
    graph = #graph{
      edges = #{
        K1 => #{ Ref2 => 1 },
        K2 => #{ Ref2 => 1 }
      },
      index = #{ Ref2 => {1, #{ K1 => M1, K2 => M2 }} }
    }
  }, State2),
  Probe2 = #deadlock_probe{
    ref = Ref2,
    edge = Edge,
    manager = Self,
    weight = 1,
    sent_to = #{ Self => true, M1 => true, M2 => true }
  },
  ?assertEqual([Probe2], elock_test_utils:collected(M1, 1)),
  ?assertEqual([Probe2], elock_test_utils:collected(M2, 1)),
  ?NO_MESSAGE,

  % a later grant: the weight stays
  State3 = elock_manager:handle_add_held_locks(#add_held_locks{ ref = Ref2, held = #{ K3 => M3 } }, State2),

  ?assertEqual(State1#state{
    graph = #graph{
      edges = #{
        K1 => #{ Ref2 => 1 },
        K2 => #{ Ref2 => 1 },
        K3 => #{ Ref2 => 1 }
      },
      index = #{ Ref2 => {1, #{ K1 => M1, K2 => M2, K3 => M3 }} }
    }
  }, State3),
  ?assertEqual([#deadlock_probe{
    ref = Ref2,
    edge = Edge,
    manager = Self,
    weight = 1,
    sent_to = #{ Self => true, M3 => true }
  }], elock_test_utils:collected(M3, 1)),
  ?NO_MESSAGE,

  % a multi node request of a client that held nothing: the weight 0,
  % with the first grant and with the second
  State4 = elock_manager:add_request(Req3, State3),
  [?reply(Tag3, #queued{ref = Ref3})] = elock_test_utils:collected(C3, 1),
  ?assertEqual(State3#state.graph, State4#state.graph),
  ?assertMatch(#{ Ref3 := #req{ held_count = 0, has_lock = false } }, State4#state.requests),
  State5 = elock_manager:handle_add_held_locks(#add_held_locks{ ref = Ref3, held = #{ K2 => M2 } }, State4),
  State6 = elock_manager:handle_add_held_locks(#add_held_locks{ ref = Ref3, held = #{ K3 => M3 } }, State5),

  ?assertEqual(State4#state{
    graph = #graph{
      edges = #{
        K1 => #{ Ref2 => 1 },
        K2 => #{ Ref2 => 1, Ref3 => 0 },
        K3 => #{ Ref2 => 1, Ref3 => 0 }
      },
      index = #{
        Ref2 => {1, #{ K1 => M1, K2 => M2, K3 => M3 }},
        Ref3 => {0, #{ K2 => M2, K3 => M3 }}
      }
    }
  }, State6),
  Probe3 = #deadlock_probe{
    ref = Ref3,
    edge = Edge,
    manager = Self,
    weight = 0
  },
  ?assertEqual([Probe3#deadlock_probe{ sent_to = #{ Self => true, M2 => true } }], elock_test_utils:collected(M2, 1)),
  ?assertEqual([Probe3#deadlock_probe{ sent_to = #{ Self => true, M3 => true } }], elock_test_utils:collected(M3, 1)),
  ?NO_MESSAGE,

  % a holder and an unknown ref
  ?assertEqual(State6, elock_manager:handle_add_held_locks(#add_held_locks{ ref = Ref1, held = #{ K2 => M2 } }, State6)),
  ?assertEqual(State6, elock_manager:handle_add_held_locks(#add_held_locks{ ref = make_ref(), held = #{ K2 => M2 } }, State6)),
  % an answer that comes after the grant: the request is a holder by then
  State7 = elock_manager:handle_unlock(Ref1, State1),
  [?reply(Tag2, #locked{ref = Ref2})] = elock_test_utils:collected(C2, 1),
  ?assertEqual(State7, elock_manager:handle_add_held_locks(#add_held_locks{ ref = Ref2, held = #{ K1 => M1 } }, State7)),
  ?assertEqual(undefined, State7#state.graph),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A probe from another manager: without a graph the state is
%%  identical and nothing is forwarded, a waiter whose answer has
%%  not come in yet is not seen; a lighter local closer gets
%%  #deadlock{}, is dequeued, the queue is pushed and the probe goes
%%  on to the managers of the remaining waiters; an origin that
%%  loses is told and the state is identical. The waiters are in
%%  the graph by their answers to #queued{} (see waiting/3)
%%-----------------------------------------------------------------
handle_deadlock_probe_test(Config)->
  Scope = ?config(scope, Config),
  C1 = elock_test_utils:collector(),
  C2 = elock_test_utils:collector(),
  C3 = elock_test_utils:collector(),
  C4 = elock_test_utils:collector(),
  OM = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  OEdge = {origin_scope, origin_term, 'origin@node'},
  K3 = {Scope, t3, node()},
  ORef = make_ref(),
  Probe = #deadlock_probe{
    ref = ORef,
    edge = OEdge,
    manager = OM,
    weight = 2,
    sent_to = #{ OM => true }
  },
  Held2 = #{ OEdge => OM },
  Held3 = #{ K3 => M3 },
  Held4 = #{ OEdge => OM, K3 => M3, {Scope, t5, node()} => M3 },
  #request{ref = Ref1} = Req1 = request(Scope, 1, C1, false),
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, C2, false, #{held => Held2}),
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, C3, false, #{held => Held3}),
  #request{ref = Ref4} = Req4 = request(Scope, 4, C4, false, #{held => Held4}),
  State0 = initial_state(Scope, Req1),

  % no graph
  ?assertEqual(State0, elock_manager:handle_deadlock_probe(Probe, State0)),
  ?NO_MESSAGE,

  % a closer that has been asked and has not answered yet: still no graph
  Asked = elock_manager:add_request(Req2, State0),
  [?reply(Tag2, #queued{ref = Ref2})] = elock_test_utils:collected(C2, 1),
  ?assertEqual(Asked, elock_manager:handle_deadlock_probe(Probe, Asked)),
  ?NO_MESSAGE,

  % its answer comes in: a lighter closer (weight 1 < 2), and another
  % waiter behind it
  State1 = elock_manager:handle_add_held_locks(#add_held_locks{ ref = Ref2, held = Held2 }, Asked),
  [_] = elock_test_utils:collected(OM, 1),
  State2 = waiting(Req3, Held3, State1),
  [_] = elock_test_utils:collected(M3, 1),

  State3 = elock_manager:handle_deadlock_probe(Probe, State2),

  ?assertEqual([?reply(Tag2, #deadlock{ref = Ref2, winner = OEdge})], elock_test_utils:collected(C2, 1)),
  ?assertEqual([Probe#deadlock_probe{ sent_to = #{ OM => true, M3 => true } }], elock_test_utils:collected(M3, 1)),
  #state{clients = #{ C3 := #client{monitor_ref = Mon3} }} = State2,
  ?assertEqual(plain(State0#state{
    queue = gb_sets:from_list([{3, Ref3}]),
    requests = (State0#state.requests)#{
      Ref3 => #req{
        client = C3, ref = Ref3, queue = 3, proxy = C3, tag = Tag3,
        shared = false, held_count = 1, has_lock = false, timer = undefined
      }
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
  State4 = waiting(Req4, Held4, State3),
  [_] = elock_test_utils:collected(OM, 1),
  [_] = elock_test_utils:collected(M3, 1),
  ?assertEqual([{3, Ref3}, {4, Ref4}], gb_sets:to_list(State4#state.queue)),

  ?assertEqual(State4, elock_manager:handle_deadlock_probe(Probe, State4)),

  ?assertEqual([#deadlock{ref = ORef, winner = {Scope, ?TERM, node()}}],
    elock_test_utils:collected(OM, 1)),
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

  {ok, Manager} = elock_test_utils:call(C1, fun()-> elock_manager:lock(Req1, #{}) end),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{?TERM, Manager, 1}]),
  MonRef = erlang:monitor(process, Manager),

  R2 = elock_test_utils:cast(C2, fun()-> elock_manager:lock(Req2, #{}) end),
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

  {ok, Manager} = elock_test_utils:call(C1, fun()-> elock_manager:lock(Req1, #{}) end),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{?TERM, Manager, 1}]),
  MonRef = erlang:monitor(process, Manager),

  % the test process is the second client: its ticket is taken before
  % the unlock reaches the manager, its request after
  ?assertEqual(2, ets:update_counter(Scope, ?TERM, {3, 1}, {?TERM, 0, 0})),
  Manager ! #unlock{ref = Ref1},
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, self(), false),
  Manager ! Req2,

  ?assertEqual(?reply(Tag2, #locked{ref = Ref2}), ?RECEIVE(?reply(_, #locked{}))),
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

  {ok, Manager} = elock_test_utils:call(C1, fun()-> elock_manager:lock(Req1, #{}) end),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{?TERM, Manager, 1}]),
  MonRef = erlang:monitor(process, Manager),

  Manager ! garbage,
  Manager ! {unexpected, self(), make_ref()},
  R2 = elock_test_utils:cast(C2, fun()-> elock_manager:lock(Req2, #{}) end),
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

%%-----------------------------------------------------------------
%%  The real manager puts the tag of the request on every message
%%  to the proxy. The test process is the client and the proxy of
%%  the requests, their tickets are taken by hand: #queued{} for a
%%  request that holds something - its answer is probed with the
%%  held count as the weight - and #deadlock{} on the verdict that
%%  aborts it, #timeout{}, #retry{} for a ticket behind the last
%%  one, #locked{}
%%-----------------------------------------------------------------
manager_tagged_replies_test(Config)->
  Scope = ?config(scope, Config),
  Self = self(),
  C1 = elock_test_utils:client(),
  M1 = elock_test_utils:collector(),
  Held = #{ {other_scope, other_term, node()} => M1 },
  #request{ref = Ref1} = Req1 = request(Scope, undefined, C1, false),

  {ok, Manager} = elock_test_utils:call(C1, fun()-> elock_manager:lock(Req1, #{}) end),
  ?WAIT(elock_test_utils:locks(Scope) =:= [{?TERM, Manager, 1}]),
  MonRef = erlang:monitor(process, Manager),

  % #queued{}, the answer joins the graph of the manager, #deadlock{}
  ?assertEqual(2, ets:update_counter(Scope, ?TERM, {3, 1})),
  #request{ref = Ref2, tag = Tag2} = Req2 = request(Scope, 2, Self, false, #{held => Held}),
  Manager ! Req2,
  ?assertEqual(?reply(Tag2, #queued{ ref = Ref2, manager = Manager, node = node() }), ?RECEIVE(?reply(_, #queued{}))),
  Manager ! #add_held_locks{ ref = Ref2, held = Held },
  ?assertEqual([#deadlock_probe{
    ref = Ref2,
    edge = {Scope, ?TERM, node()},
    manager = Manager,
    weight = 1,
    sent_to = #{ Manager => true, M1 => true }
  }], elock_test_utils:collected(M1, 1)),
  Manager ! #deadlock{ref = Ref2, winner = ?WINNER},
  ?assertEqual(?reply(Tag2, #deadlock{ref = Ref2, winner = ?WINNER}), ?RECEIVE(?reply(_, #deadlock{}))),

  % #timeout{}
  ?assertEqual(3, ets:update_counter(Scope, ?TERM, {3, 1})),
  #request{ref = Ref3, tag = Tag3} = Req3 = request(Scope, 3, Self, false, #{timeout => 100}),
  Manager ! Req3,
  ?assertEqual(?reply(Tag3, #timeout{ref = Ref3}), ?RECEIVE(?reply(_, #timeout{}))),

  % #retry{}: the ticket has been passed by
  #request{ref = Ref4, tag = Tag4} = Req4 = request(Scope, 2, Self, false),
  Manager ! Req4,
  ?assertEqual(?reply(Tag4, #retry{ref = Ref4}), ?RECEIVE(?reply(_, #retry{}))),

  % #locked{}
  ?assertEqual(4, ets:update_counter(Scope, ?TERM, {3, 1})),
  #request{ref = Ref5, tag = Tag5} = Req5 = request(Scope, 4, Self, true),
  Manager ! Req5,
  ?NO_MESSAGE,
  Manager ! #unlock{ref = Ref1},
  ?assertEqual(?reply(Tag5, #locked{ref = Ref5}), ?RECEIVE(?reply(_, #locked{}))),

  Manager ! #unlock{ref = Ref5},
  ?assertEqual({'DOWN', MonRef, process, Manager, normal}, ?RECEIVE({'DOWN', MonRef, process, Manager, _})),
  ?assertEqual([], elock_test_utils:locks(Scope)),
  ?assertEqual([], elock_test_utils:managers()),
  ?NO_MESSAGE,
  elock_test_utils:stop(C1).

%%=================================================================
%%  Utilities
%%=================================================================
% The request of Client. Without a ticket it is the request as
% elock:lock/4 builds it: lock/2 takes the ticket and names the
% proxy and the tag. With a ticket it is the request as the manager
% gets it: the proxy is the client as for a single node request
% unless the options name another one, the tag is its own for every
% request. The held map of the options is the locks the client
% holds when it asks: the request carries only their number
request(Scope, Ticket, Client, Shared)->
  request(Scope, Ticket, Client, Shared, #{}).
request(Scope, undefined, Client, Shared, Options)->
  #request{
    queue = undefined,
    ref = make_ref(),
    scope = Scope,
    term = ?TERM,
    client = Client,
    proxy = undefined,
    tag = undefined,
    shared = Shared,
    held_count = map_size(maps:get(held, Options, #{})),
    nodes = maps:get(nodes, Options, [node()]),
    timeout = maps:get(timeout, Options, undefined)
  };
request(Scope, Ticket, Client, Shared, Options)->
  Request = request(Scope, undefined, Client, Shared, Options),
  Request#request{
    queue = Ticket,
    proxy = maps:get(proxy, Options, Client),
    tag = make_ref()
  }.

% The request waits in the state and its client has answered
% #queued{}: the request is taken by add_request/2, its proxy is
% asked and the held map of the answer joins the graph through
% handle_add_held_locks/2, the managers of the map are probed
waiting(#request{ref = Ref, proxy = Proxy, tag = Tag} = Request, Held, State0)->
  State = elock_manager:add_request(Request, State0),
  [?reply(Tag, #queued{ref = Ref})] = elock_test_utils:collected(Proxy, 1),
  elock_manager:handle_add_held_locks(#add_held_locks{ ref = Ref, held = Held }, State).

% The state of a manager just started by the winner of the ticket 1,
% as init/1 builds it (see holder_has_no_tag_test), with the test
% process as the manager: the monitor of the client belongs to the
% test process. The request that starts a manager has never waited:
% no proxy, no tag, and its held count is not taken
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
        tag = undefined,
        shared = Shared,
        held_count = undefined,
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

% The state a real manager enters its loop with: init/1 runs in a
% stand-in process, its call of loop/1 is caught by a call trace
% and the stand-in is killed. The monitor of the client belonged to
% the stand-in
init_state(Request)->
  1 = erlang:trace_pattern({elock_manager, loop, 1}, true, [local]),
  {StandIn, MonRef} = stand_in(fun()-> elock_manager:init(Request) end),
  1 = erlang:trace(StandIn, true, [call]),
  StandIn ! go,
  {trace, StandIn, call, {elock_manager, loop, [State]}} = ?RECEIVE({trace, StandIn, call, _}),
  exit(StandIn, kill),
  ?RECEIVE({'DOWN', MonRef, process, StandIn, killed}),
  1 = erlang:trace_pattern({elock_manager, loop, 1}, false, [local]),
  {StandIn, State}.

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
