%%=================================================================
%%  Highly concurrent scenarios on a single node. The sizes come
%%  from functional.config (ct:get_config(concurrency, #{})) merged
%%  over ?DEFAULTS.
%%
%%  Every test is a coordinator: it spawns N elock_test_utils
%%  clients, hands each its work with cast/2, collects the results
%%  with a deadline (a client that does not answer fails the test by
%%  name; a crash inside the work comes back as {'EXIT', _} and is
%%  reported as a bad result) and asserts at the end: every result
%%  has an allowed shape, the scope is idle, no manager is left,
%%  every client's context is undefined (read while the client is
%%  alive) and no violation of the critical section was recorded.
%%
%%  The critical sections are checked through a public ETS table
%%  per test: an exclusive holder marks {Term, self()} on entry with
%%  insert_new - a marker of another live process inside is a
%%  violation; with modes the holders of a term are listed with
%%  their modes and every entry checks that they are all shared or
%%  that it is alone. The violations are recorded in a table, never
%%  crash the client
%%=================================================================
-module(elock_concurrency_SUITE).

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
  exclusive_mutual_exclusion_test/1,
  shared_exclusive_invariant_test/1,
  single_term_hammer_test/1,
  reentrancy_under_load_test/1,
  timeouts_under_load_test/1,
  deadlock_storm_test/1,
  cross_scope_deadlock_storm_test/1,
  client_churn_test/1,
  manager_churn_test/1,
  many_terms_test/1
]).

% The sizes of the scenarios, overridden by the concurrency map of
% functional.config
-define(DEFAULTS, #{
  clients => 200,           % clients per scenario
  rounds => 20,             % lock/unlock rounds per client
  terms => 20,              % distinct terms the clients pick from
  hammer_clients => 500,    % single_term_hammer_test
  many_terms => 10000,      % many_terms_test
  max_timeout => 50,        % ms, timeouts_under_load_test
  deadlock_rounds => 20     % cap of the rounds of the deadlock storms
}).

% The deadline of a batch of work, ms
-define(BATCH, 60000).

% The pace of the churn: one kill per interval, ms
-define(CHURN_INTERVAL, 5).

all()->
  [
    {group, mutual_exclusion},
    {group, verdicts},
    {group, churn},
    {group, scale}
  ].

groups()->
  [
    {mutual_exclusion, [], [
      exclusive_mutual_exclusion_test,
      shared_exclusive_invariant_test,
      single_term_hammer_test,
      reentrancy_under_load_test
    ]},
    {verdicts, [], [
      timeouts_under_load_test,
      deadlock_storm_test,
      cross_scope_deadlock_storm_test
    ]},
    {churn, [], [
      client_churn_test,
      manager_churn_test
    ]},
    {scale, [], [
      many_terms_test
    ]}
  ].

suite()->
  [{timetrap, {minutes, 20}}].

init_per_suite(Config)->
  Config.

end_per_suite(_Config)->
  ok.

init_per_group(_Group, Config)->
  Config.

end_per_group(_Group, _Config)->
  ok.

%%-----------------------------------------------------------------
%%  Every test case gets its own scope named after it and the sizes;
%%  the cross scope storm a second scope as well
%%-----------------------------------------------------------------
init_per_testcase(TestCase, Config)->
  Holder = elock_test_utils:start_scope(TestCase),
  Sizes = maps:merge(?DEFAULTS, ct:get_config(concurrency, #{})),
  Config1 = [{scope, TestCase}, {holder, Holder}, {sizes, Sizes} | Config],
  case TestCase of
    cross_scope_deadlock_storm_test->
      Scope2 = list_to_atom(atom_to_list(TestCase) ++ "_second"),
      Holder2 = elock_test_utils:start_scope(Scope2),
      [{scope2, Scope2}, {holder2, Holder2} | Config1];
    _->
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
%%  Mutual exclusion
%%=================================================================
%%-----------------------------------------------------------------
%%  N clients x R rounds of exclusive lock/unlock of a random term
%%  out of M: never two holders of a term inside the critical
%%  section at once
%%-----------------------------------------------------------------
exclusive_mutual_exclusion_test(Config)->
  #{ clients := N, rounds := Rounds, terms := Terms } = ?config(sizes, Config),
  Scope = ?config(scope, Config),
  Check = check_table(),
  Violations = violations_table(),
  Clients = clients(N),

  Results = run(Clients, exclusive_work(#{
    scope => Scope,
    terms => Terms,
    rounds => Rounds,
    check => Check,
    violations => Violations,
    hold => 0
  })),

  check_results(Results, [ok]),
  ?assertEqual([], ets:tab2list(Violations)),
  ?assertEqual(N * Rounds, length(lists:append(maps:values(Results)))),
  finish(Scope, Clients).

%%-----------------------------------------------------------------
%%  Random modes: at any moment the holders of a term are all shared
%%  or exactly one exclusive
%%-----------------------------------------------------------------
shared_exclusive_invariant_test(Config)->
  #{ clients := N, rounds := Rounds, terms := Terms } = ?config(sizes, Config),
  Scope = ?config(scope, Config),
  Check = mode_table(),
  Violations = violations_table(),
  Clients = clients(N),

  Results = run(Clients, mode_work(#{
    scope => Scope,
    terms => Terms,
    rounds => Rounds,
    check => Check,
    violations => Violations,
    hold => 0
  })),

  check_results(Results, [ok]),
  ?assertEqual([], ets:tab2list(Violations)),
  finish(Scope, Clients).

%%-----------------------------------------------------------------
%%  500 clients hammer one term exclusively for R rounds: mutual
%%  exclusion holds, one manager serves the whole run and the scope
%%  is idle afterwards
%%-----------------------------------------------------------------
single_term_hammer_test(Config)->
  #{ hammer_clients := N, rounds := Rounds } = ?config(sizes, Config),
  Scope = ?config(scope, Config),
  Check = check_table(),
  Violations = violations_table(),
  Clients = clients(N),

  Results = run(Clients, exclusive_work(#{
    scope => Scope,
    terms => 1,
    rounds => Rounds,
    check => Check,
    violations => Violations,
    hold => 0
  })),

  check_results(Results, [ok]),
  ?assertEqual([], ets:tab2list(Violations)),
  ?assertEqual(N * Rounds, length(lists:append(maps:values(Results)))),
  finish(Scope, Clients).

%%-----------------------------------------------------------------
%%  Re-entrant holds under load: every round takes 1..3 refs of a
%%  random term in one mode and unlocks them in a random order; the
%%  mode invariant holds and every client ends without a context
%%-----------------------------------------------------------------
reentrancy_under_load_test(Config)->
  #{ clients := N, rounds := Rounds, terms := Terms } = ?config(sizes, Config),
  Scope = ?config(scope, Config),
  Check = mode_table(),
  Violations = violations_table(),
  Clients = clients(N),

  Results = run(Clients, reentrant_work(#{
    scope => Scope,
    terms => Terms,
    rounds => Rounds,
    check => Check,
    violations => Violations
  })),

  check_results(Results, [ok]),
  ?assertEqual([], ets:tab2list(Violations)),
  finish(Scope, Clients).

%%=================================================================
%%  Verdicts under load
%%=================================================================
%%-----------------------------------------------------------------
%%  Short random timeouts under contention: every request ends with
%%  ok or {error, timeout}, nothing else; both happen; mutual
%%  exclusion holds for the granted ones
%%-----------------------------------------------------------------
timeouts_under_load_test(Config)->
  #{ clients := N, rounds := Rounds, terms := Terms, max_timeout := MaxTimeout } = ?config(sizes, Config),
  Scope = ?config(scope, Config),
  Check = check_table(),
  Violations = violations_table(),
  Clients = clients(N),

  Results = run(Clients, timeout_work(#{
    scope => Scope,
    terms => Terms,
    rounds => Rounds,
    max_timeout => MaxTimeout,
    check => Check,
    violations => Violations
  })),

  check_results(Results, [ok, {error, timeout}]),
  ?assertEqual([], ets:tab2list(Violations)),
  Outcomes = lists:append(maps:values(Results)),
  Granted = length([ O || O <- Outcomes, O =:= ok ]),
  TimedOut = length([ O || O <- Outcomes, O =:= {error, timeout} ]),
  ct:pal("timeouts under load: ~p granted, ~p timed out", [Granted, TimedOut]),
  ?assert(Granted > 0),
  ?assert(TimedOut > 0),
  finish(Scope, Clients).

%%-----------------------------------------------------------------
%%  The deadlock storm: every client holds one random term while it
%%  asks for another (no timeout), R times per round. Every request
%%  ends with ok or {error, deadlock} - nothing hangs - and the
%%  scope is idle after every round. Rounds run until a deadlock has
%%  been observed (capped), and at least one must have been
%%-----------------------------------------------------------------
deadlock_storm_test(Config)->
  Scope = ?config(scope, Config),
  deadlock_storm(Config, Scope, Scope).

%%-----------------------------------------------------------------
%%  The deadlock storm across two scopes: the odd clients hold a
%%  term of the first scope and ask for one of the second, the even
%%  clients the other way round
%%-----------------------------------------------------------------
cross_scope_deadlock_storm_test(Config)->
  Scope1 = ?config(scope, Config),
  Scope2 = ?config(scope2, Config),
  deadlock_storm(Config, Scope1, Scope2).

deadlock_storm(Config, Scope1, Scope2)->
  #{ clients := N, rounds := Rounds, terms := Terms, deadlock_rounds := Cap } = ?config(sizes, Config),
  Clients = clients(N),
  Work = deadlock_work(#{
    scope1 => Scope1,
    scope2 => Scope2,
    terms => Terms,
    rounds => Rounds
  }),

  Deadlocks = storm_rounds(Clients, Work, [Scope1, Scope2], Cap, 1, 0),
  ct:pal("deadlock storm: ~p deadlocks", [Deadlocks]),
  ?assert(Deadlocks > 0),
  [ ?assertEqual(undefined, elock_test_utils:context(Client)) || Client <- Clients ],
  finish(Scope1, Clients),
  elock_test_utils:wait_idle(Scope2).

storm_rounds(_Clients, _Work, _Scopes, Cap, Round, Deadlocks) when Round > Cap; Deadlocks > 0->
  Deadlocks;
storm_rounds(Clients, Work, Scopes, Cap, Round, _Deadlocks)->
  Results = run(Clients, Work),
  check_results(Results, [ok, {error, deadlock}]),
  [ elock_test_utils:wait_idle(Scope) || Scope <- lists:usort(Scopes) ],
  Deadlocks = length([ O || O <- lists:append(maps:values(Results)), O =:= {error, deadlock} ]),
  ct:pal("deadlock storm round ~p: ~p deadlocks", [Round, Deadlocks]),
  storm_rounds(Clients, Work, Scopes, Cap, Round + 1, Deadlocks).

%%=================================================================
%%  Churn
%%=================================================================
%%-----------------------------------------------------------------
%%  Random clients are killed while they hold or wait (a tenth of
%%  them, one every few ms from the start of the load): the rest
%%  complete with mutual exclusion intact, the locks of the dead are
%%  released by their managers, the scope is idle
%%-----------------------------------------------------------------
client_churn_test(Config)->
  #{ clients := N, rounds := Rounds, terms := Terms } = ?config(sizes, Config),
  Scope = ?config(scope, Config),
  Check = check_table(),
  Violations = violations_table(),
  Clients = clients(N),
  Victims = lists:sublist(shuffle(Clients), max(1, N div 10)),

  Requests = start(Clients, exclusive_work(#{
    scope => Scope,
    terms => Terms,
    rounds => Rounds,
    check => Check,
    violations => Violations,
    hold => 1
  })),
  Churned = churn_clients(Victims, Requests),
  ct:pal("client churn: ~p of ~p victims killed under load", [length(Churned), length(Victims)]),

  Survivors = Clients -- Victims,
  Results = collect(maps:filter(fun(_, Client)-> lists:member(Client, Survivors) end, Requests)),
  check_results(Results, [ok]),
  ?assertEqual([], ets:tab2list(Violations)),
  ?assertEqual(lists:sort(Survivors), lists:sort(maps:keys(Results))),
  ?assert(length(Churned) >= length(Victims) div 2),
  finish(Scope, Survivors).

%%-----------------------------------------------------------------
%%  Random managers are killed during the load: the waiters retry
%%  and everything completes; the holders of a killed manager lose
%%  the lock silently (the contract), hence no critical section
%%  check here. The entries the killed managers leave behind are
%%  dropped at the end (the next client of such a term would drop
%%  them on its 'DOWN'), then the scope is idle
%%-----------------------------------------------------------------
manager_churn_test(Config)->
  #{ clients := N, rounds := Rounds, terms := Terms } = ?config(sizes, Config),
  Scope = ?config(scope, Config),
  Clients = clients(N),

  Requests = start(Clients, exclusive_work(#{
    scope => Scope,
    terms => Terms,
    rounds => Rounds,
    check => undefined,
    violations => undefined,
    hold => 1
  })),
  Kills = churn_managers(Requests, erlang:monotonic_time(millisecond) + ?BATCH, 0),
  Results = collect(Requests),
  check_results(Results, [ok]),
  ?assertEqual(N * Rounds, length(lists:append(maps:values(Results)))),

  Stale = [ Entry || {_Term, Manager, _Ticket} = Entry <- elock_test_utils:locks(Scope), not is_process_alive(Manager) ],
  [ true = ets:delete_object(Scope, Entry) || Entry <- Stale ],
  ct:pal("manager churn: ~p managers killed, ~p stale entries left behind", [Kills, length(Stale)]),
  ?assert(Kills > 0),
  finish(Scope, Clients).

%%=================================================================
%%  Scale
%%=================================================================
%%-----------------------------------------------------------------
%%  10 000 distinct terms locked at once by ten clients (one entry
%%  and one manager each) and unlocked: the scope is idle and the
%%  process count is back to the baseline
%%-----------------------------------------------------------------
many_terms_test(Config)->
  #{ many_terms := N } = ?config(sizes, Config),
  Scope = ?config(scope, Config),
  Node = node(),
  ClientCount = 10,
  PerClient = N div ClientCount,
  Clients = clients(ClientCount),
  Baseline = length(erlang:processes()),

  Locked = run(Clients, fun(Index)->
    [ begin
        {ok, Ref} = elock:lock(Scope, {t, Index, J}, [Node]),
        Ref
      end || J <- lists:seq(1, PerClient) ]
  end),
  ?assertEqual(N, length(lists:append(maps:values(Locked)))),
  ?assertEqual(N, length(elock_test_utils:locks(Scope))),
  ?assertEqual(N, length(elock_test_utils:managers())),
  ?assertEqual(N, length([ 1 || {_Term, Manager, 1} <- elock_test_utils:locks(Scope), is_pid(Manager) ])),

  Unlocked = run(Clients, fun(Index)->
    Refs = maps:get(lists:nth(Index, Clients), Locked),
    [ elock:unlock(Ref) || Ref <- Refs ]
  end),
  check_results(Unlocked, [ok]),
  ?assertEqual(N, length(lists:append(maps:values(Unlocked)))),

  elock_test_utils:wait_idle(Scope),
  ?WAIT(length(erlang:processes()) =< Baseline),
  finish(Scope, Clients).

%%=================================================================
%%  Utilities
%%=================================================================
% N clients
clients(N)->
  [ elock_test_utils:client() || _ <- lists:seq(1, N) ].

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
    erlang:error({clients_did_not_finish, maps:values(Requests)})
  end.

% Run the work in every client and collect
run(Clients, Work)->
  collect(start(Clients, Work)).

% Every result is a list of outcomes of the allowed shapes; a crash
% of the work ({'EXIT', _}) or anything else is reported as is
check_results(Results, Allowed)->
  ?assertEqual([], [ {Client, Result} || {Client, Result} <- maps:to_list(Results), not is_list(Result) ]),
  ?assertEqual([], [ {Client, Outcome} || {Client, Outcomes} <- maps:to_list(Results), Outcome <- Outcomes, not lists:member(Outcome, Allowed) ]).

% The end of every scenario: the scope is idle, no manager is left,
% every client is alive without a context, then the clients are
% stopped
finish(Scope, Clients)->
  elock_test_utils:wait_idle(Scope),
  ?assertEqual([], elock_test_utils:managers()),
  ?assertEqual([], [ Client || Client <- Clients, not is_process_alive(Client) ]),
  ?assertEqual([], [ {Client, Context} || Client <- Clients, Context <- [elock_test_utils:context(Client)], Context =/= undefined ]),
  [ elock_test_utils:stop(Client) || Client <- Clients ],
  ok.

%%-----------------------------------------------------------------
%%  The check tables. The exclusive check table holds one marker per
%%  term, the mode table one entry per holder with its mode, the
%%  violations table whatever was seen wrong
%%-----------------------------------------------------------------
check_table()->
  ets:new(check, [public, set, {write_concurrency, true}]).

mode_table()->
  ets:new(mode_check, [public, bag, {write_concurrency, true}]).

violations_table()->
  ets:new(violations, [public, bag]).

% Enter the critical section of Term exclusively: a marker of another
% process inside is a violation unless that process is dead (a
% killed client leaves its marker behind - see client_churn_test)
enter(undefined, _Violations, _Term)->
  ok;
enter(Check, Violations, Term)->
  case ets:insert_new(Check, {Term, self()}) of
    true->
      ok;
    false->
      case ets:lookup(Check, Term) of
        [{Term, Other}]->
          case is_process_alive(Other) of
            true->
              ets:insert(Violations, {Term, {exclusive_violation, self(), Other}}),
              ok;
            false->
              ets:delete_object(Check, {Term, Other}),
              enter(Check, Violations, Term)
          end;
        []->
          enter(Check, Violations, Term)
      end
  end.

leave(undefined, _Term)->
  ok;
leave(Check, Term)->
  ets:delete_object(Check, {Term, self()}),
  ok.

% Enter the critical section of Term in a mode: the other holders
% must all be shared as this one, or there must be none if this one
% is exclusive
enter_mode(Check, Violations, Term, Shared)->
  ets:insert(Check, {Term, self(), Shared}),
  Others = [ {Pid, Mode} || {_Term, Pid, Mode} <- ets:lookup(Check, Term), Pid =/= self() ],
  Conflict =
    if
      Shared-> lists:keymember(false, 2, Others);
      true-> Others =/= []
    end,
  case Conflict of
    true->
      ets:insert(Violations, {Term, {mode_violation, self(), Shared, Others}});
    false->
      ok
  end,
  ok.

leave_mode(Check, Term, Shared)->
  ets:delete_object(Check, {Term, self(), Shared}),
  ok.

% Time inside the critical section: a scheduler switch or a sleep
hold(0)->
  erlang:yield();
hold(Ms)->
  timer:sleep(Ms).

term(Terms)->
  {t, rand:uniform(Terms)}.

%%-----------------------------------------------------------------
%%  The work, closures the clients run. Every one returns the list
%%  of the outcomes of its rounds
%%-----------------------------------------------------------------
% Exclusive lock/unlock of random terms with the critical section
% checked
exclusive_work(#{
  scope := Scope,
  terms := Terms,
  rounds := Rounds,
  check := Check,
  violations := Violations,
  hold := Hold
})->
  Node = node(),
  fun(_Index)->
    [ begin
        Term = term(Terms),
        {ok, Ref} = elock:lock(Scope, Term, [Node]),
        enter(Check, Violations, Term),
        hold(Hold),
        leave(Check, Term),
        ok = elock:unlock(Ref),
        ok
      end || _ <- lists:seq(1, Rounds) ]
  end.

% Random modes with the mode invariant checked
mode_work(#{
  scope := Scope,
  terms := Terms,
  rounds := Rounds,
  check := Check,
  violations := Violations,
  hold := Hold
})->
  Node = node(),
  fun(_Index)->
    [ begin
        Term = term(Terms),
        Shared = rand:uniform(2) =:= 1,
        {ok, Ref} = elock:lock(Scope, Term, [Node], #{is_shared => Shared}),
        enter_mode(Check, Violations, Term, Shared),
        hold(Hold),
        leave_mode(Check, Term, Shared),
        ok = elock:unlock(Ref),
        ok
      end || _ <- lists:seq(1, Rounds) ]
  end.

% 1..3 re-entrant holds of a random term in one mode, unlocked in a
% random order
reentrant_work(#{
  scope := Scope,
  terms := Terms,
  rounds := Rounds,
  check := Check,
  violations := Violations
})->
  Node = node(),
  fun(_Index)->
    [ begin
        Term = term(Terms),
        Shared = rand:uniform(2) =:= 1,
        Refs =
          [ begin
              {ok, Ref} = elock:lock(Scope, Term, [Node], #{is_shared => Shared}),
              Ref
            end || _ <- lists:seq(1, rand:uniform(3)) ],
        enter_mode(Check, Violations, Term, Shared),
        hold(0),
        leave_mode(Check, Term, Shared),
        [ ok = elock:unlock(Ref) || Ref <- shuffle(Refs) ],
        ok
      end || _ <- lists:seq(1, Rounds) ]
  end.

% Random short timeouts, a short hold when granted
timeout_work(#{
  scope := Scope,
  terms := Terms,
  rounds := Rounds,
  max_timeout := MaxTimeout,
  check := Check,
  violations := Violations
})->
  Node = node(),
  fun(_Index)->
    [ begin
        Term = term(Terms),
        case elock:lock(Scope, Term, [Node], #{timeout => rand:uniform(MaxTimeout)}) of
          {ok, Ref}->
            enter(Check, Violations, Term),
            hold(rand:uniform(3)),
            leave(Check, Term),
            ok = elock:unlock(Ref),
            ok;
          Other->
            Other
        end
      end || _ <- lists:seq(1, Rounds) ]
  end.

% Hold a random term of one scope while asking for a random term of
% the other, no timeout. The odd clients hold in the first scope and
% ask in the second, the even ones the other way round - otherwise
% every wait-for edge would point from the first scope to the second
% and no cycle could form. With a single scope the two terms differ
deadlock_work(#{
  scope1 := Scope1,
  scope2 := Scope2,
  terms := Terms,
  rounds := Rounds
})->
  Node = node(),
  fun(Index)->
    {HoldScope, AskScope} =
      case Index rem 2 of
        1-> {Scope1, Scope2};
        0-> {Scope2, Scope1}
      end,
    [ begin
        Term1 = term(Terms),
        Term2 = other_term(HoldScope, AskScope, Terms, Term1),
        {ok, Ref1} = elock:lock(HoldScope, Term1, [Node]),
        Outcome =
          case elock:lock(AskScope, Term2, [Node]) of
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

other_term(Scope, Scope, Terms, Term1)->
  case term(Terms) of
    Term1-> other_term(Scope, Scope, Terms, Term1);
    Term2-> Term2
  end;
other_term(_Scope1, _Scope2, Terms, _Term1)->
  term(Terms).

%%-----------------------------------------------------------------
%%  The churn
%%-----------------------------------------------------------------
% Kill the victims one by one at the churn pace while their work is
% pending (a victim that has finished already is left alone): the
% ones killed under load
churn_clients(Victims, Requests)->
  Pending = maps:fold(fun(Request, Client, Acc)-> Acc#{ Client => Request } end, #{}, Requests),
  lists:filter(
    fun(Victim)->
      receive after ?CHURN_INTERVAL-> ok end,
      case elock_test_utils:pending(maps:get(Victim, Pending)) of
        true->
          elock_test_utils:stop(Victim),
          true;
        false->
          false
      end
    end,
    Victims
  ).

% Kill a random manager at the churn pace while the work is pending
% (or until the deadline): the count of the kills
churn_managers(Requests, Deadline, Kills)->
  Pending = lists:any(fun elock_test_utils:pending/1, maps:keys(Requests)),
  case Pending andalso erlang:monotonic_time(millisecond) < Deadline of
    true->
      receive after ?CHURN_INTERVAL-> ok end,
      case elock_test_utils:managers() of
        []->
          churn_managers(Requests, Deadline, Kills);
        Managers->
          Manager = lists:nth(rand:uniform(length(Managers)), Managers),
          exit(Manager, kill),
          churn_managers(Requests, Deadline, Kills + 1)
      end;
    false->
      Kills
  end.

shuffle(List)->
  [ Item || {_, Item} <- lists:sort([ {rand:uniform(), Item} || Item <- List ]) ].
