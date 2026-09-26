%%=================================================================
%%  Highly concurrent scenarios across nodes. Three peer nodes run
%%  the scope of every test case, clients_per_node clients on every
%%  one of them lock random terms on random non-empty subsets of the
%%  nodes. The sizes come from functional.config
%%  (ct:get_config(concurrency, #{})) merged over ?DEFAULTS.
%%
%%  Every test is a coordinator on the controller (the ct node): it
%%  spawns the clients on the peers, hands each its work with
%%  cast/2, collects the results with a deadline (a client that does
%%  not answer fails the test by name; a crash inside the work comes
%%  back as {'EXIT', _} and is reported as a bad result) and asserts
%%  at the end: every result has an allowed shape, every node is
%%  idle, no manager is left anywhere, every client's context is
%%  undefined and no violation of the critical section was recorded.
%%
%%  A multi node request may lose a cycle even when its client holds
%%  nothing else: two requests for the same term granted on two
%%  nodes in the opposite order wait for each other and one of them
%%  gets {error, deadlock} (see same_term_opposite_order_deadlock_test
%%  of elock_multi_node_SUITE). The work of the scenarios without
%%  deadlocks of their own repeats such a request - the client holds
%%  nothing, the retry is what any client would do - and reports the
%%  deadlock to the coordinator, which counts them.
%%
%%  The critical sections are checked per {Term, Node} through
%%  public named ETS tables on the controller, reached by the
%%  clients through rpc (one call to enter, one to leave): an
%%  exclusive holder marks every {Term, Node} of its lock on entry
%%  with insert_new - a marker of another live process inside is a
%%  violation; with modes the holders of a {Term, Node} are listed
%%  with their modes and every entry checks that they are all shared
%%  or that it is alone. The violations are recorded in a table,
%%  never crash the client
%%=================================================================
-module(elock_multi_node_concurrency_SUITE).

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
  distributed_mutual_exclusion_test/1,
  distributed_shared_exclusive_test/1,
  distributed_deadlock_storm_test/1,
  distributed_timeouts_under_load_test/1,
  node_kill_under_load_test/1
]).

%% Internal: the critical section checks the clients run on the
%% controller through rpc
-export([
  enter/4,
  leave/3,
  enter_mode/5,
  leave_mode/4
]).

% The sizes of the scenarios, overridden by the concurrency map of
% functional.config
-define(DEFAULTS, #{
  clients_per_node => 50,   % clients on every node of the cluster
  rounds => 20,             % lock/unlock rounds per client
  terms => 20,              % distinct terms the clients pick from
  max_timeout => 50,        % ms, distributed_timeouts_under_load_test
  deadlock_rounds => 20,    % cap of the rounds of the deadlock storm
  kill_after => 0.25        % node_kill_under_load_test: the share of the rounds done before the node is killed
}).

% The deadline of a batch of work, ms
-define(BATCH, 60000).

% The size of the cluster the test cases run on
-define(CLUSTER, 3).

% The check tables on the controller, named so that the clients on
% the peers can reach them
-define(CHECK, elock_multi_node_check).
-define(VIOLATIONS, elock_multi_node_violations).

all()->
  [
    {group, mutual_exclusion},
    {group, verdicts},
    {group, failures}
  ].

groups()->
  [
    {mutual_exclusion, [], [
      distributed_mutual_exclusion_test,
      distributed_shared_exclusive_test
    ]},
    {verdicts, [], [
      distributed_deadlock_storm_test,
      distributed_timeouts_under_load_test
    ]},
    {failures, [], [
      node_kill_under_load_test
    ]}
  ].

suite()->
  [{timetrap, {minutes, 20}}].

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
%%  of the cluster, ready on all of them, and the sizes; the
%%  deadlock storm a second scope as well
%%-----------------------------------------------------------------
init_per_testcase(TestCase, Config)->
  Nodes = cluster(),
  Holders = start_scopes(Nodes, TestCase),
  Sizes = maps:merge(?DEFAULTS, ct:get_config(concurrency, #{})),
  Config1 = [{nodes, Nodes}, {scope, TestCase}, {holders, Holders}, {sizes, Sizes} | Config],
  case TestCase of
    distributed_deadlock_storm_test->
      Scope2 = list_to_atom(atom_to_list(TestCase) ++ "_second"),
      Holders2 = start_scopes(Nodes, Scope2),
      [{scope2, Scope2}, {holders2, Holders2} | Config1];
    _->
      Config1
  end.

%%-----------------------------------------------------------------
%%  Every client is stopped, every scope must be idle on every node
%%  (no entry, no manager). A leak fails the test case. A fourth
%%  node left behind by a failed test is stopped
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
%%  Mutual exclusion
%%=================================================================
%%-----------------------------------------------------------------
%%  N clients on every node x R rounds of exclusive lock/unlock of a
%%  random term out of M on a random non-empty subset of the nodes:
%%  never two holders of a {Term, Node} inside the critical section
%%  at once; every request ok (a request that lost a cycle is
%%  repeated, see the header); every node idle, no manager anywhere,
%%  no context left
%%-----------------------------------------------------------------
distributed_mutual_exclusion_test(Config)->
  #{ clients_per_node := N, rounds := Rounds, terms := Terms } = ?config(sizes, Config),
  Scope = ?config(scope, Config),
  Nodes = ?config(nodes, Config),
  Check = check_table(),
  Violations = violations_table(),
  Clients = clients(Nodes, N),

  Results = run(Clients, exclusive_work(#{
    scope => Scope,
    nodes => Nodes,
    terms => Terms,
    rounds => Rounds,
    check => Check,
    hold => 0
  })),

  check_results(Results, [ok]),
  ?assertEqual([], ets:tab2list(Violations)),
  ?assertEqual(length(Clients) * Rounds, length(lists:append(maps:values(Results)))),
  ct:pal("distributed mutual exclusion: ~p requests repeated after a deadlock", [deadlocks()]),
  finish([Scope], Nodes, Clients).

%%-----------------------------------------------------------------
%%  Random modes on random subsets: at any moment the holders of a
%%  {Term, Node} are all shared or exactly one exclusive
%%-----------------------------------------------------------------
distributed_shared_exclusive_test(Config)->
  #{ clients_per_node := N, rounds := Rounds, terms := Terms } = ?config(sizes, Config),
  Scope = ?config(scope, Config),
  Nodes = ?config(nodes, Config),
  Check = mode_table(),
  Violations = violations_table(),
  Clients = clients(Nodes, N),

  Results = run(Clients, mode_work(#{
    scope => Scope,
    nodes => Nodes,
    terms => Terms,
    rounds => Rounds,
    check => Check,
    hold => 0
  })),

  check_results(Results, [ok]),
  ?assertEqual([], ets:tab2list(Violations)),
  ?assertEqual(length(Clients) * Rounds, length(lists:append(maps:values(Results)))),
  ct:pal("distributed shared/exclusive: ~p requests repeated after a deadlock", [deadlocks()]),
  finish([Scope], Nodes, Clients).

%%=================================================================
%%  Verdicts under load
%%=================================================================
%%-----------------------------------------------------------------
%%  The deadlock storm: every client holds one random term on a
%%  random subset while it asks for another on a random subset (no
%%  timeout), R times per round; the odd clients hold in the first
%%  scope and ask in the second, the even ones the other way round,
%%  so that cycles can form. Every request ends with ok or
%%  {error, deadlock} - nothing hangs - and every node is idle after
%%  every round. Rounds run until a deadlock has been observed
%%  (capped), and at least one must have been
%%-----------------------------------------------------------------
distributed_deadlock_storm_test(Config)->
  #{ clients_per_node := N, rounds := Rounds, terms := Terms, deadlock_rounds := Cap } = ?config(sizes, Config),
  Scope1 = ?config(scope, Config),
  Scope2 = ?config(scope2, Config),
  Nodes = ?config(nodes, Config),
  Clients = clients(Nodes, N),
  Work = deadlock_work(#{
    scope1 => Scope1,
    scope2 => Scope2,
    nodes => Nodes,
    terms => Terms,
    rounds => Rounds
  }),

  Deadlocks = storm_rounds(Clients, Work, [Scope1, Scope2], Nodes, Cap, 1, 0),
  ct:pal("distributed deadlock storm: ~p deadlocks", [Deadlocks]),
  ?assert(Deadlocks > 0),
  finish([Scope1, Scope2], Nodes, Clients).

storm_rounds(_Clients, _Work, _Scopes, _Nodes, Cap, Round, Deadlocks) when Round > Cap; Deadlocks > 0->
  Deadlocks;
storm_rounds(Clients, Work, Scopes, Nodes, Cap, Round, _Deadlocks)->
  Results = run(Clients, Work),
  check_results(Results, [ok, {error, deadlock}]),
  [ elock_test_utils:wait_idle(Node, Scope) || Node <- Nodes, Scope <- Scopes ],
  Deadlocks = length([ O || O <- lists:append(maps:values(Results)), O =:= {error, deadlock} ]),
  ct:pal("distributed deadlock storm round ~p: ~p deadlocks, ~p holds repeated after a deadlock", [Round, Deadlocks, deadlocks()]),
  storm_rounds(Clients, Work, Scopes, Nodes, Cap, Round + 1, Deadlocks).

%%-----------------------------------------------------------------
%%  Short random timeouts on random subsets under contention: every
%%  request ends with ok or {error, timeout}, nothing else; both
%%  happen; mutual exclusion holds per {Term, Node} for the granted
%%  ones; every node idle after
%%-----------------------------------------------------------------
distributed_timeouts_under_load_test(Config)->
  #{ clients_per_node := N, rounds := Rounds, terms := Terms, max_timeout := MaxTimeout } = ?config(sizes, Config),
  Scope = ?config(scope, Config),
  Nodes = ?config(nodes, Config),
  Check = check_table(),
  Violations = violations_table(),
  Clients = clients(Nodes, N),

  Results = run(Clients, timeout_work(#{
    scope => Scope,
    nodes => Nodes,
    terms => Terms,
    rounds => Rounds,
    max_timeout => MaxTimeout,
    check => Check
  })),

  check_results(Results, [ok, {error, timeout}]),
  ?assertEqual([], ets:tab2list(Violations)),
  Outcomes = lists:append(maps:values(Results)),
  Granted = length([ O || O <- Outcomes, O =:= ok ]),
  TimedOut = length([ O || O <- Outcomes, O =:= {error, timeout} ]),
  ct:pal("distributed timeouts under load: ~p granted, ~p timed out, ~p requests repeated after a deadlock",
    [Granted, TimedOut, deadlocks()]),
  ?assert(Granted > 0),
  ?assert(TimedOut > 0),
  finish([Scope], Nodes, Clients).

%%=================================================================
%%  Failures
%%=================================================================
%%-----------------------------------------------------------------
%%  Clients on n1 and n2 lock random terms on random subsets of
%%  [n1, n2, n4] while n4 is killed once a share of the rounds is
%%  done: every request ends with ok or {error, _} - never hangs -
%%  and some of them fail on the dead node ({error, {badrpc, _}});
%%  the survivors are idle at the end and see the three nodes of the
%%  cluster in the scope; after the run a lock on [n1, n2] works
%%-----------------------------------------------------------------
node_kill_under_load_test(Config)->
  #{ clients_per_node := N, rounds := Rounds, terms := Terms, kill_after := KillAfter } = ?config(sizes, Config),
  Scope = ?config(scope, Config),
  [N1, N2 | _] = Nodes = ?config(nodes, Config),
  N4 = distributed_tests_utils:start_node(#{name => n4}),
  _Holder4 = elock_test_utils:start_scope(N4, Scope),
  elock_test_utils:wait_ready(Scope, [N4 | Nodes]),
  Clients = clients([N1, N2], N),

  Requests = start(Clients, kill_work(#{
    scope => Scope,
    nodes => [N1, N2, N4],
    terms => Terms,
    rounds => Rounds,
    hold => 1
  })),
  wait_progress(round(length(Clients) * Rounds * KillAfter)),
  ?assert(lists:any(fun elock_test_utils:pending/1, maps:keys(Requests))),
  ?assertEqual(ok, distributed_tests_utils:kill_node(N4)),

  Results = collect(Requests),
  check_results(Results, fun(ok)-> true; ({error, _})-> true; (_)-> false end),
  Outcomes = lists:append(maps:values(Results)),
  ?assertEqual(length(Clients) * Rounds, length(Outcomes)),
  Granted = length([ O || O <- Outcomes, O =:= ok ]),
  Unreachable = length([ O || O <- Outcomes, unreachable(O) ]),
  Other = lists:usort([ O || O <- Outcomes, O =/= ok, not unreachable(O) ]),
  ct:pal("node kill under load: ~p granted, ~p failed on the dead node, other errors: ~p", [Granted, Unreachable, Other]),
  ?assert(Granted > 0),
  ?assert(Unreachable > 0),

  elock_test_utils:wait_ready(Scope, Nodes),
  finish([Scope], Nodes, Clients),
  C1 = elock_test_utils:client(N1),
  {ok, Ref} = elock_test_utils:lock(C1, Scope, t, [N1, N2]),
  ?assertEqual(ok, elock_test_utils:unlock(C1, Ref)),
  [ elock_test_utils:wait_idle(Node, Scope) || Node <- Nodes ],
  elock_test_utils:stop(C1).

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

% N clients on every node
clients(Nodes, N)->
  [ elock_test_utils:client(Node) || Node <- Nodes, _ <- lists:seq(1, N) ].

% Every client runs Work(Index) - the requests: #{ Request => Client }
start(Clients, Work)->
  maps:from_list(
    [ {elock_test_utils:cast(Client, fun()-> Work(Index) end), Client}
      || {Client, Index} <- lists:zip(Clients, lists:seq(1, length(Clients))) ]
  ).

% The results of the requests within the batch deadline:
% #{ Client => Result }. The clients that did not answer in time
% fail the test by name
collect(Requests)->
  collect(Requests, erlang:monotonic_time(millisecond) + ?BATCH, #{}).

collect(Requests, _Deadline, Results) when map_size(Requests) =:= 0->
  Results;
collect(Requests, Deadline, Results)->
  Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
  receive
    {Request, Result} when is_map_key(Request, Requests)->
      {Client, Rest} = maps:take(Request, Requests),
      collect(Rest, Deadline, Results#{ Client => Result })
  after Timeout->
    erlang:error({clients_did_not_finish, [ {Client, stuck_in(Client)} || Client <- maps:values(Requests) ]})
  end.

% Run the work in every client and collect
run(Clients, Work)->
  collect(start(Clients, Work)).

% Every result is a list of outcomes of the allowed shapes (a list
% of outcomes, or a predicate); a crash of the work ({'EXIT', _}) or
% anything else is reported as is
check_results(Results, Allowed)->
  ?assertEqual([], [ {Client, Result} || {Client, Result} <- maps:to_list(Results), not is_list(Result) ]),
  ?assertEqual([], [ {Client, Outcome} || {Client, Outcomes} <- maps:to_list(Results), Outcome <- Outcomes, not allowed(Outcome, Allowed) ]).

allowed(Outcome, Allowed) when is_function(Allowed, 1)->
  Allowed(Outcome);
allowed(Outcome, Allowed)->
  lists:member(Outcome, Allowed).

% The end of every scenario: every scope is idle on every node, no
% manager is left anywhere, every client is alive without a
% context, then the clients are stopped
finish(Scopes, Nodes, Clients)->
  [ elock_test_utils:wait_idle(Node, Scope) || Node <- Nodes, Scope <- Scopes ],
  ?assertEqual([], [ {Node, Manager} || Node <- Nodes, Manager <- elock_test_utils:managers(Node) ]),
  ?assertEqual([], [ Client || Client <- Clients, not alive(Client) ]),
  ?assertEqual([], [ {Client, Context} || Client <- Clients, Context <- [elock_test_utils:context(Client)], Context =/= undefined ]),
  [ elock_test_utils:stop(Client) || Client <- Clients ],
  ok.

alive(Pid)->
  rpc:call(node(Pid), erlang, is_process_alive, [Pid]) =:= true.

% The verdict of a request that named the dead node
unreachable({error, {badrpc, _}})->
  true;
unreachable(_Outcome)->
  false.

% Where the client is: for the error of a batch that did not finish
stuck_in(Client)->
  rpc:call(node(Client), erlang, process_info, [Client, [current_function, current_stacktrace, message_queue_len]]).

% The deadlocks the clients reported since the last count
deadlocks()->
  receive
    {deadlock, _Client}->
      1 + deadlocks()
  after 0->
    0
  end.

% The progress reports of the clients (one per round) until Count of
% them are in, within the batch deadline
wait_progress(Count)->
  wait_progress(Count, erlang:monotonic_time(millisecond) + ?BATCH).

wait_progress(0, _Deadline)->
  ok;
wait_progress(Count, Deadline)->
  Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
  receive
    {progress, _Client}->
      wait_progress(Count - 1, Deadline)
  after Timeout->
    erlang:error({no_progress, Count})
  end.

%%-----------------------------------------------------------------
%%  The check tables on the controller. The exclusive check table
%%  holds one marker per {Term, Node}, the mode table one entry per
%%  holder of a {Term, Node} with its mode, the violations table
%%  whatever was seen wrong. They are owned by the test case process
%%  and go with it
%%-----------------------------------------------------------------
check_table()->
  ets:new(?CHECK, [named_table, public, set, {write_concurrency, true}]).

mode_table()->
  ets:new(?CHECK, [named_table, public, bag, {write_concurrency, true}]).

violations_table()->
  ets:new(?VIOLATIONS, [named_table, public, bag]).

% Enter the critical sections of the keys exclusively: a marker of
% another process inside is a violation unless that process is dead
enter(Check, Violations, Keys, Holder)->
  [ enter_key(Check, Violations, Key, Holder) || Key <- Keys ],
  ok.

enter_key(Check, Violations, Key, Holder)->
  case ets:insert_new(Check, {Key, Holder}) of
    true->
      ok;
    false->
      case ets:lookup(Check, Key) of
        [{Key, Other}]->
          case alive(Other) of
            true->
              ets:insert(Violations, {Key, {exclusive_violation, Holder, Other}}),
              ok;
            false->
              ets:delete_object(Check, {Key, Other}),
              enter_key(Check, Violations, Key, Holder)
          end;
        []->
          enter_key(Check, Violations, Key, Holder)
      end
  end.

leave(Check, Keys, Holder)->
  [ ets:delete_object(Check, {Key, Holder}) || Key <- Keys ],
  ok.

% Enter the critical sections of the keys in a mode: the other
% holders of a key must all be shared as this one, or there must be
% none if this one is exclusive
enter_mode(Check, Violations, Keys, Shared, Holder)->
  [ begin
      ets:insert(Check, {Key, Holder, Shared}),
      Others = [ {Pid, Mode} || {_Key, Pid, Mode} <- ets:lookup(Check, Key), Pid =/= Holder ],
      Conflict =
        if
          Shared-> lists:keymember(false, 2, Others);
          true-> Others =/= []
        end,
      case Conflict of
        true->
          ets:insert(Violations, {Key, {mode_violation, Holder, Shared, Others}});
        false->
          ok
      end
    end || Key <- Keys ],
  ok.

leave_mode(Check, Keys, Shared, Holder)->
  [ ets:delete_object(Check, {Key, Holder, Shared}) || Key <- Keys ],
  ok.

% The critical section calls the clients make: the keys of a lock
% are {Term, Node} for every node of it, the tables live on the
% controller
enter(Controller, Check, Violations, Term, Nodes, Holder)->
  ok = rpc:call(Controller, ?MODULE, enter, [Check, Violations, keys(Term, Nodes), Holder]).

leave(Controller, Check, Term, Nodes, Holder)->
  ok = rpc:call(Controller, ?MODULE, leave, [Check, keys(Term, Nodes), Holder]).

enter_mode(Controller, Check, Violations, Term, Nodes, Shared, Holder)->
  ok = rpc:call(Controller, ?MODULE, enter_mode, [Check, Violations, keys(Term, Nodes), Shared, Holder]).

leave_mode(Controller, Check, Term, Nodes, Shared, Holder)->
  ok = rpc:call(Controller, ?MODULE, leave_mode, [Check, keys(Term, Nodes), Shared, Holder]).

keys(Term, Nodes)->
  [ {Term, Node} || Node <- Nodes ].

% Time inside the critical section: a scheduler switch or a sleep
hold(0)->
  erlang:yield();
hold(Ms)->
  timer:sleep(Ms).

term(Terms)->
  {t, rand:uniform(Terms)}.

% A random non-empty subset of the nodes
subset(Nodes)->
  case [ Node || Node <- Nodes, rand:uniform(2) =:= 1 ] of
    []-> [lists:nth(rand:uniform(length(Nodes)), Nodes)];
    Subset-> Subset
  end.

% A request repeated while it loses a cycle (see the header), every
% deadlock reported to the coordinator: {ok, Ref} or {error, timeout}
request(Scope, Term, Nodes, Options, Coordinator)->
  case elock:lock(Scope, Term, Nodes, Options) of
    {error, deadlock}->
      Coordinator ! {deadlock, self()},
      request(Scope, Term, Nodes, Options, Coordinator);
    Verdict->
      Verdict
  end.

%%-----------------------------------------------------------------
%%  The work, closures the clients run on the peers. Every one
%%  returns the list of the outcomes of its rounds. The controller
%%  and the coordinator (the test case process) are captured when
%%  the closure is made
%%-----------------------------------------------------------------
% Exclusive lock/unlock of random terms on random subsets with the
% critical section checked
exclusive_work(#{
  scope := Scope,
  nodes := Nodes,
  terms := Terms,
  rounds := Rounds,
  check := Check,
  hold := Hold
})->
  Controller = node(),
  Coordinator = self(),
  fun(_Index)->
    [ begin
        Term = term(Terms),
        Subset = subset(Nodes),
        {ok, Ref} = request(Scope, Term, Subset, #{}, Coordinator),
        enter(Controller, Check, ?VIOLATIONS, Term, Subset, self()),
        hold(Hold),
        leave(Controller, Check, Term, Subset, self()),
        ok = elock:unlock(Ref),
        ok
      end || _ <- lists:seq(1, Rounds) ]
  end.

% Random modes on random subsets with the mode invariant checked
mode_work(#{
  scope := Scope,
  nodes := Nodes,
  terms := Terms,
  rounds := Rounds,
  check := Check,
  hold := Hold
})->
  Controller = node(),
  Coordinator = self(),
  fun(_Index)->
    [ begin
        Term = term(Terms),
        Subset = subset(Nodes),
        Shared = rand:uniform(2) =:= 1,
        {ok, Ref} = request(Scope, Term, Subset, #{is_shared => Shared}, Coordinator),
        enter_mode(Controller, Check, ?VIOLATIONS, Term, Subset, Shared, self()),
        hold(Hold),
        leave_mode(Controller, Check, Term, Subset, Shared, self()),
        ok = elock:unlock(Ref),
        ok
      end || _ <- lists:seq(1, Rounds) ]
  end.

% Random short timeouts on random subsets, a short hold when granted
timeout_work(#{
  scope := Scope,
  nodes := Nodes,
  terms := Terms,
  rounds := Rounds,
  max_timeout := MaxTimeout,
  check := Check
})->
  Controller = node(),
  Coordinator = self(),
  fun(_Index)->
    [ begin
        Term = term(Terms),
        Subset = subset(Nodes),
        case request(Scope, Term, Subset, #{timeout => rand:uniform(MaxTimeout)}, Coordinator) of
          {ok, Ref}->
            enter(Controller, Check, ?VIOLATIONS, Term, Subset, self()),
            hold(rand:uniform(3)),
            leave(Controller, Check, Term, Subset, self()),
            ok = elock:unlock(Ref),
            ok;
          Other->
            Other
        end
      end || _ <- lists:seq(1, Rounds) ]
  end.

% Hold a random term of one scope on a random subset while asking
% for a random term of the other scope on a random subset, no
% timeout. The odd clients hold in the first scope and ask in the
% second, the even ones the other way round - otherwise every
% wait-for edge would point from the first scope to the second and
% no cycle could form. The hold itself may lose a cycle of its own
% and is repeated then (see the header)
deadlock_work(#{
  scope1 := Scope1,
  scope2 := Scope2,
  nodes := Nodes,
  terms := Terms,
  rounds := Rounds
})->
  Coordinator = self(),
  fun(Index)->
    {HoldScope, AskScope} =
      case Index rem 2 of
        1-> {Scope1, Scope2};
        0-> {Scope2, Scope1}
      end,
    [ begin
        {ok, Ref1} = request(HoldScope, term(Terms), subset(Nodes), #{}, Coordinator),
        Outcome =
          case elock:lock(AskScope, term(Terms), subset(Nodes)) of
            {ok, Ref2}->
              ok = elock:unlock(Ref2),
              ok;
            Other->
              Other
          end,
        ok = elock:unlock(Ref1),
        Outcome
      end || _ <- lists:seq(1, Rounds) ]
  end.

% Exclusive lock/unlock of random terms on random subsets of the
% nodes, one of which dies during the run: every verdict is an
% outcome, the granted locks are held a moment and unlocked; the
% coordinator gets a progress report per round
kill_work(#{
  scope := Scope,
  nodes := Nodes,
  terms := Terms,
  rounds := Rounds,
  hold := Hold
})->
  Coordinator = self(),
  fun(_Index)->
    [ begin
        Outcome =
          case elock:lock(Scope, term(Terms), subset(Nodes)) of
            {ok, Ref}->
              hold(Hold),
              ok = elock:unlock(Ref),
              ok;
            Other->
              Other
          end,
        Coordinator ! {progress, self()},
        Outcome
      end || _ <- lists:seq(1, Rounds) ]
  end.
