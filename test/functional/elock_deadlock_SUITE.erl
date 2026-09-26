%%=================================================================
%%  Functional tests of the deadlock detection on a single node:
%%  cycles within a scope and across scopes, the weights and the
%%  coin, chains that are no cycles, what the loser keeps and how
%%  the winners drain.
%%
%%  A scenario is a list of participants {Client, Held, Want}: every
%%  client takes its Held locks, then the Want requests are issued
%%  one by one with lock_queued/5, so that every request stands in
%%  its manager's queue - and its probes have been sent - before the
%%  next one is issued. The probe of the request that closes the
%%  cycle finds the rest of the cycle in place, hence the cycle gets
%%  exactly one verdict. The property asserted everywhere: exactly
%%  one loser per cycle, and every winner is granted once the loser
%%  releases what it holds
%%=================================================================
-module(elock_deadlock_SUITE).

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
  two_cycle_test/1,
  concurrent_two_cycle_test/1,
  weight_decides_test/1,
  tie_test/1,
  three_cycle_test/1,
  three_cycle_with_weights_test/1,
  two_independent_cycles_test/1,
  shared_locks_cycle_test/1,
  shared_holder_in_cycle_test/1,
  upgrade_cycle_test/1,
  no_false_deadlock_chain_test/1,
  holder_of_nothing_never_loses_test/1,
  loser_keeps_other_locks_test/1,
  loser_retries_after_winner_test/1,
  retry_recreates_cycle_test/1,
  cross_scope_cycle_test/1,
  cross_scope_different_terms_cycle_test/1,
  cross_scope_no_false_positive_test/1,
  cross_scope_weight_test/1,
  deadlock_and_timeout_test/1,
  ring_test/1
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

% The verdict of a cycle comes within a second
-define(VERDICT, 1000).

% The test cases that need a second scope
-define(TWO_SCOPE_TESTS, [
  cross_scope_cycle_test,
  cross_scope_different_terms_cycle_test,
  cross_scope_no_false_positive_test,
  cross_scope_weight_test
]).

all()->
  [
    {group, cycles},
    {group, no_deadlock},
    {group, loser},
    {group, cross_scope},
    {group, mixed}
  ].

groups()->
  [
    {cycles, [], [
      two_cycle_test,
      concurrent_two_cycle_test,
      weight_decides_test,
      tie_test,
      three_cycle_test,
      three_cycle_with_weights_test,
      two_independent_cycles_test,
      shared_locks_cycle_test,
      shared_holder_in_cycle_test,
      upgrade_cycle_test
    ]},
    {no_deadlock, [], [
      no_false_deadlock_chain_test,
      holder_of_nothing_never_loses_test
    ]},
    {loser, [], [
      loser_keeps_other_locks_test,
      loser_retries_after_winner_test,
      retry_recreates_cycle_test
    ]},
    {cross_scope, [], [
      cross_scope_cycle_test,
      cross_scope_different_terms_cycle_test,
      cross_scope_no_false_positive_test,
      cross_scope_weight_test
    ]},
    {mixed, [], [
      deadlock_and_timeout_test,
      ring_test
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
%%  Cycles
%%=================================================================
%%-----------------------------------------------------------------
%%  A holds t1, B holds t2, each asks for the other's term - with
%%  either request closing the cycle: exactly one of them gets
%%  {error, deadlock} within a second, the other keeps waiting until
%%  the loser unlocks its term, then it is granted; the loser is
%%  left with no context; idle afterwards
%%-----------------------------------------------------------------
two_cycle_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  lists:foreach(
    fun(Closing)->
      [A, B] = clients(2),
      {ok, RefA} = elock_test_utils:lock(A, Scope, t1, [Node]),
      {ok, RefB} = elock_test_utils:lock(B, Scope, t2, [Node]),
      Requests =
        case Closing of
          a_closes->
            [{B, ask(B, Node, {Scope, t1}), [RefB]}, {A, ask(A, Node, {Scope, t2}), [RefA]}];
          b_closes->
            [{A, ask(A, Node, {Scope, t2}), [RefA]}, {B, ask(B, Node, {Scope, t1}), [RefB]}]
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
      elock_test_utils:wait_idle(Scope),
      stop([A, B])
    end,
    [a_closes, b_closes]
  ).

%%-----------------------------------------------------------------
%%  The racing version of two_cycle_test: both closing requests are
%%  fired at once (no ordering), so the two probes race. Fifty
%%  rounds. Every request gets exactly one verdict: the first one is
%%  a deadlock within the deadline; the other is either a deadlock
%%  as well (two losers - racing probes may abort both, the rounds
%%  where it happens are counted and logged) or it is still waiting
%%  and is granted once the loser releases its term. Never zero
%%  losers, the scope is idle after every round
%%-----------------------------------------------------------------
concurrent_two_cycle_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  Rounds = 50,
  TwoLoserRounds =
    lists:foldl(
      fun(_Round, Acc)->
        [A, B] = clients(2),
        {ok, RefA} = elock_test_utils:lock(A, Scope, t1, [Node]),
        {ok, RefB} = elock_test_utils:lock(B, Scope, t2, [Node]),
        RA = elock_test_utils:lock_async(A, Scope, t2, [Node], #{}),
        RB = elock_test_utils:lock_async(B, Scope, t1, [Node], #{}),

        {LoserR, Verdict} = any_result([RA, RB], ?DEADLINE),
        ?assertEqual({error, deadlock}, Verdict),
        {Loser, LoserHeld, Other, OtherR, OtherHeld} =
          case LoserR of
            RA-> {A, RefA, B, RB, RefB};
            RB-> {B, RefB, A, RA, RefA}
          end,
        Losers =
          case elock_test_utils:result(OtherR, ?QUIET) of
            {ok, {error, deadlock}}->
              % both lost: both release, there is nothing to grant
              ?assertEqual(ok, elock_test_utils:unlock(Loser, LoserHeld)),
              ?assertEqual(ok, elock_test_utils:unlock(Other, OtherHeld)),
              2;
            timeout->
              % the other one waits for the loser's term
              ?assertEqual(ok, elock_test_utils:unlock(Loser, LoserHeld)),
              WinnerRef = granted(OtherR),
              ?assertEqual(ok, elock_test_utils:unlock(Other, WinnerRef)),
              ?assertEqual(ok, elock_test_utils:unlock(Other, OtherHeld)),
              1;
            {ok, Unexpected}->
              erlang:error({unexpected_verdict, Unexpected})
          end,
        elock_test_utils:wait_idle(Scope),
        % no request got a second verdict
        ?assert(elock_test_utils:pending(RA)),
        ?assert(elock_test_utils:pending(RB)),
        ?assertEqual(undefined, elock_test_utils:context(A)),
        ?assertEqual(undefined, elock_test_utils:context(B)),
        stop([A, B]),
        Acc + Losers - 1
      end,
      0,
      lists:seq(1, Rounds)
    ),
  ct:pal("concurrent two-cycle: ~p of ~p rounds with two losers", [TwoLoserRounds, Rounds]).

%%-----------------------------------------------------------------
%%  The weight decides: the client holding two terms never loses
%%  against the one holding one, whichever request closes the cycle,
%%  ten times each
%%-----------------------------------------------------------------
weight_decides_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  lists:foreach(
    fun({Closing, _Round})->
      [Heavy, Light] = clients(2),
      HeavyPart = {Heavy, [{Scope, t1}, {Scope, t3}], {Scope, t2}},
      LightPart = {Light, [{Scope, t2}], {Scope, t1}},
      Requests =
        case Closing of
          heavy_closes-> setup(Node, [LightPart, HeavyPart]);
          light_closes-> setup(Node, [HeavyPart, LightPart])
        end,
      Verdicts = resolve(Requests),
      ?assertEqual([Light], losers(Verdicts)),
      ?assertEqual([Heavy], winners(Verdicts)),
      elock_test_utils:wait_idle(Scope),
      stop([Heavy, Light])
    end,
    [ {Closing, Round} || Round <- lists:seq(1, 10), Closing <- [heavy_closes, light_closes] ]
  ).

%%-----------------------------------------------------------------
%%  Equal weights: exactly one loser every time, twenty cycles with
%%  the closing request alternating (which one loses is up to the
%%  coin and is not asserted)
%%-----------------------------------------------------------------
tie_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  lists:foreach(
    fun(Round)->
      [A, B] = clients(2),
      PartA = {A, [{Scope, t1}], {Scope, t2}},
      PartB = {B, [{Scope, t2}], {Scope, t1}},
      Requests =
        case Round rem 2 of
          0-> setup(Node, [PartA, PartB]);
          1-> setup(Node, [PartB, PartA])
        end,
      Verdicts = resolve(Requests),
      ?assertEqual(1, length(losers(Verdicts))),
      ?assertEqual(1, length(winners(Verdicts))),
      elock_test_utils:wait_idle(Scope),
      stop([A, B])
    end,
    lists:seq(1, 20)
  ).

%%-----------------------------------------------------------------
%%  A cycle of three (A holds t1 asks t2, B holds t2 asks t3, C
%%  holds t3 asks t1): exactly one loser, the two others are granted
%%  as the locks are released along the chain
%%-----------------------------------------------------------------
three_cycle_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [A, B, C] = clients(3),
  Requests = setup(Node, [
    {A, [{Scope, t1}], {Scope, t2}},
    {B, [{Scope, t2}], {Scope, t3}},
    {C, [{Scope, t3}], {Scope, t1}}
  ]),
  Verdicts = resolve(Requests),
  ?assertEqual(1, length(losers(Verdicts))),
  ?assertEqual(2, length(winners(Verdicts))),
  [ ?assertEqual(undefined, elock_test_utils:context(Client)) || Client <- [A, B, C] ],
  elock_test_utils:wait_idle(Scope),
  stop([A, B, C]).

%%-----------------------------------------------------------------
%%  A cycle of three where A holds two terms: A never loses, one of
%%  the two light ones does, whichever request closes the cycle -
%%  ten rounds with the order of the requests rotating
%%-----------------------------------------------------------------
three_cycle_with_weights_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  lists:foreach(
    fun(Round)->
      [A, B, C] = clients(3),
      Parts = [
        {A, [{Scope, t1}, {Scope, t4}], {Scope, t2}},
        {B, [{Scope, t2}], {Scope, t3}},
        {C, [{Scope, t3}], {Scope, t1}}
      ],
      Requests = setup(Node, rotate(Parts, Round)),
      Verdicts = resolve(Requests),
      ?assertMatch([Loser] when Loser =:= B; Loser =:= C, losers(Verdicts)),
      ?assert(lists:member(A, winners(Verdicts))),
      elock_test_utils:wait_idle(Scope),
      stop([A, B, C])
    end,
    lists:seq(1, 10)
  ).

%%-----------------------------------------------------------------
%%  Two independent cycles (A-B on t1/t2, C-D on t3/t4): one loser
%%  per cycle
%%-----------------------------------------------------------------
two_independent_cycles_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [A, B, C, D] = clients(4),
  Requests = setup(Node, [
    {A, [{Scope, t1}], {Scope, t2}},
    {C, [{Scope, t3}], {Scope, t4}},
    {B, [{Scope, t2}], {Scope, t1}},
    {D, [{Scope, t4}], {Scope, t3}}
  ]),
  Verdicts = resolve(Requests),
  Losers = losers(Verdicts),
  ?assertEqual(2, length(Losers)),
  ?assertEqual(1, length([ L || L <- Losers, L =:= A orelse L =:= B ])),
  ?assertEqual(1, length([ L || L <- Losers, L =:= C orelse L =:= D ])),
  elock_test_utils:wait_idle(Scope),
  stop([A, B, C, D]).

%%-----------------------------------------------------------------
%%  Shared holds close a cycle as well: A holds t1 shared, B holds
%%  t2 shared, both ask the other's term exclusively - exactly one
%%  loser, the winner gets its exclusive lock once the loser
%%  releases its shared one
%%-----------------------------------------------------------------
shared_locks_cycle_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [A, B] = clients(2),
  Requests = setup(Node, [
    {A, [{Scope, t1, ?SHARED}], {Scope, t2}},
    {B, [{Scope, t2, ?SHARED}], {Scope, t1}}
  ]),
  {LoserR, Verdict} = any_result([ R || {_, R, _} <- Requests ], ?VERDICT),
  ?assertEqual({error, deadlock}, Verdict),
  {value, {Loser, LoserR, [LoserHeld]}, [{Winner, WinnerR, [WinnerHeld]}]} = lists:keytake(LoserR, 2, Requests),
  still_waiting(WinnerR),

  ?assertEqual(ok, elock_test_utils:unlock(Loser, LoserHeld)),
  WinnerRef = granted(WinnerR),
  ?assertEqual(ok, elock_test_utils:unlock(Winner, WinnerRef)),
  ?assertEqual(ok, elock_test_utils:unlock(Winner, WinnerHeld)),
  elock_test_utils:wait_idle(Scope),
  stop([A, B]).

%%-----------------------------------------------------------------
%%  A shared holder that is not on the cycle: A and C hold t1
%%  shared, B holds t2; A asks t2, B asks t1 exclusively. One of A
%%  and B loses, C is never involved: if A loses, B still waits for
%%  C; if B loses, A is granted while C keeps holding
%%-----------------------------------------------------------------
shared_holder_in_cycle_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [A, B, C] = clients(3),
  {ok, RefC} = elock_test_utils:lock(C, Scope, t1, [Node], ?SHARED),
  [{A, RA, [RefA]}, {B, RB, [RefB]}] = setup(Node, [
    {A, [{Scope, t1, ?SHARED}], {Scope, t2}},
    {B, [{Scope, t2}], {Scope, t1}}
  ]),
  {LoserR, Verdict} = any_result([RA, RB], ?VERDICT),
  ?assertEqual({error, deadlock}, Verdict),
  case LoserR of
    RA->
      ?assertEqual(ok, elock_test_utils:unlock(A, RefA)),
      still_waiting(RB),
      ?assertEqual(ok, elock_test_utils:unlock(C, RefC)),
      LockB = granted(RB),
      ?assertEqual(ok, elock_test_utils:unlock(B, LockB)),
      ?assertEqual(ok, elock_test_utils:unlock(B, RefB));
    RB->
      ?assertEqual(ok, elock_test_utils:unlock(B, RefB)),
      LockA = granted(RA),
      ?assertEqual(ok, elock_test_utils:unlock(A, LockA)),
      ?assertEqual(ok, elock_test_utils:unlock(A, RefA)),
      ?assertEqual(ok, elock_test_utils:unlock(C, RefC))
  end,
  [ ?assertEqual(undefined, elock_test_utils:context(Client)) || Client <- [A, B, C] ],
  elock_test_utils:wait_idle(Scope),
  stop([A, B, C]).

%%-----------------------------------------------------------------
%%  Two shared holders of t1 both upgrading is a cycle the manager
%%  settles by itself: the second upgrade gets {error, deadlock} at
%%  once, the first is granted when the second releases its shared
%%  lock
%%-----------------------------------------------------------------
upgrade_cycle_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [A, B] = clients(2),
  {ok, RefA} = elock_test_utils:lock(A, Scope, t1, [Node], ?SHARED),
  {ok, RefB} = elock_test_utils:lock(B, Scope, t1, [Node], ?SHARED),
  Manager = elock_test_utils:wait_manager(Scope, t1),

  UpA = upgrade_queued(A, Scope, t1, Node),
  still_waiting(UpA),
  UpB = elock_test_utils:lock_async(B, Scope, t1, [Node], #{}),
  ?assertEqual({ok, {error, deadlock}}, elock_test_utils:result(UpB, ?VERDICT)),
  still_waiting(UpA),
  ?assertEqual([{t1, Manager, 4}], elock_test_utils:locks(Scope)),

  ?assertEqual(ok, elock_test_utils:unlock(B, RefB)),
  ?assertEqual(undefined, elock_test_utils:context(B)),
  LockA = granted(UpA),
  ?assertEqual(ok, elock_test_utils:unlock(A, LockA)),
  ?assertEqual(ok, elock_test_utils:unlock(A, RefA)),
  elock_test_utils:wait_idle(Scope),
  stop([A, B]).

%%=================================================================
%%  No deadlock
%%=================================================================
%%-----------------------------------------------------------------
%%  A chain is no cycle: A holds t1; B holds t2 and waits for t1; C
%%  holds t3 and waits for t2; D waits for t3. Nobody gets a
%%  deadlock; everything drains once A unlocks
%%-----------------------------------------------------------------
no_false_deadlock_chain_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [A, B, C, D] = clients(4),
  {ok, RefA} = elock_test_utils:lock(A, Scope, t1, [Node]),
  Requests = setup(Node, [
    {B, [{Scope, t2}], {Scope, t1}},
    {C, [{Scope, t3}], {Scope, t2}},
    {D, [], {Scope, t3}}
  ]),
  [ still_waiting(R) || {_, R, _} <- Requests ],

  ?assertEqual(ok, elock_test_utils:unlock(A, RefA)),
  Verdicts = resolve(Requests),
  ?assertEqual([], losers(Verdicts)),
  ?assertEqual(lists:sort([B, C, D]), lists:sort(winners(Verdicts))),
  [ ?assertEqual(undefined, elock_test_utils:context(Client)) || Client <- [A, B, C, D] ],
  elock_test_utils:wait_idle(Scope),
  stop([A, B, C, D]).

%%-----------------------------------------------------------------
%%  A request that holds nothing can not lose: C waits for t1
%%  holding nothing while A and B close a cycle on t1/t2 - the loser
%%  is A or B, C is granted in its turn. Five rounds
%%-----------------------------------------------------------------
holder_of_nothing_never_loses_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  lists:foreach(
    fun(_Round)->
      [A, B, C] = clients(3),
      Requests = setup(Node, [
        {A, [{Scope, t1}], {Scope, t2}},
        {C, [], {Scope, t1}},
        {B, [{Scope, t2}], {Scope, t1}}
      ]),
      Verdicts = resolve(Requests),
      ?assertMatch([Loser] when Loser =:= A; Loser =:= B, losers(Verdicts)),
      ?assertMatch({ok, _}, maps:get(C, Verdicts)),
      elock_test_utils:wait_idle(Scope),
      stop([A, B, C])
    end,
    lists:seq(1, 5)
  ).

%%=================================================================
%%  The loser
%%=================================================================
%%-----------------------------------------------------------------
%%  The loser keeps every lock it holds: its context has exactly its
%%  two held refs after the verdict, a waiter of its other term is
%%  granted only when the loser unlocks that term; the winner is
%%  granted when the loser unlocks the term of the cycle
%%-----------------------------------------------------------------
loser_keeps_other_locks_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [A, B, E] = clients(3),
  Requests = setup(Node, [
    {A, [{Scope, t1}, {Scope, t3}], {Scope, t2}},
    {B, [{Scope, t2}, {Scope, t4}], {Scope, t1}}
  ]),
  {LoserR, Verdict} = any_result([ R || {_, R, _} <- Requests ], ?VERDICT),
  ?assertEqual({error, deadlock}, Verdict),
  {value, {Loser, LoserR, [CycleRef, OtherRef]}, [{Winner, WinnerR, [WinnerCycleRef, WinnerOtherRef]}]} =
    lists:keytake(LoserR, 2, Requests),
  {CycleTerm, OtherTerm} =
    if
      Loser =:= A-> {t1, t3};
      true-> {t2, t4}
    end,
  CycleManager = elock_test_utils:wait_manager(Scope, CycleTerm),
  OtherManager = elock_test_utils:wait_manager(Scope, OtherTerm),
  ?assertEqual(#context{
    ref2lock = #{
      CycleRef => #lock{ scope = Scope, term = CycleTerm, nodes = #{ Node => CycleManager } },
      OtherRef => #lock{ scope = Scope, term = OtherTerm, nodes = #{ Node => OtherManager } }
    },
    locked = #{
      {Scope, CycleTerm, Node} => {CycleManager, 1},
      {Scope, OtherTerm, Node} => {OtherManager, 1}
    }
  }, elock_test_utils:context(Loser)),

  RE = elock_test_utils:lock_queued(E, Scope, OtherTerm, [Node], #{}),
  still_waiting(WinnerR),
  still_waiting(RE),

  ?assertEqual(ok, elock_test_utils:unlock(Loser, CycleRef)),
  WinnerRef = granted(WinnerR),
  still_waiting(RE),

  ?assertEqual(ok, elock_test_utils:unlock(Loser, OtherRef)),
  ?assertEqual(undefined, elock_test_utils:context(Loser)),
  RefE = granted(RE),
  ?assertEqual(ok, elock_test_utils:unlock(E, RefE)),
  ?assertEqual(ok, elock_test_utils:unlock(Winner, WinnerRef)),
  ?assertEqual(ok, elock_test_utils:unlock(Winner, WinnerCycleRef)),
  ?assertEqual(ok, elock_test_utils:unlock(Winner, WinnerOtherRef)),
  ?assertEqual(undefined, elock_test_utils:context(Winner)),
  elock_test_utils:wait_idle(Scope),
  stop([A, B, E]).

%%-----------------------------------------------------------------
%%  The loser releases its term, the winner completes and releases
%%  everything, then the loser repeats its request and is granted
%%-----------------------------------------------------------------
loser_retries_after_winner_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [A, B] = clients(2),
  Requests = setup(Node, [
    {A, [{Scope, t1}], {Scope, t2}},
    {B, [{Scope, t2}], {Scope, t1}}
  ]),
  {LoserR, Verdict} = any_result([ R || {_, R, _} <- Requests ], ?VERDICT),
  ?assertEqual({error, deadlock}, Verdict),
  {value, {Loser, LoserR, [LoserHeld]}, [{Winner, WinnerR, [WinnerHeld]}]} = lists:keytake(LoserR, 2, Requests),
  {LoserWants, WinnerWants} =
    if
      Loser =:= A-> {t2, t1};
      true-> {t1, t2}
    end,

  ?assertEqual(ok, elock_test_utils:unlock(Loser, LoserHeld)),
  WinnerRef = granted(WinnerR),
  ?assertEqual(ok, elock_test_utils:unlock(Winner, WinnerRef)),
  ?assertEqual(ok, elock_test_utils:unlock(Winner, WinnerHeld)),
  elock_test_utils:wait_idle(Scope),

  {ok, Retry} = elock_test_utils:lock(Loser, Scope, LoserWants, [Node]),
  {ok, Again} = elock_test_utils:lock(Loser, Scope, WinnerWants, [Node]),
  ?assertEqual(ok, elock_test_utils:unlock(Loser, Retry)),
  ?assertEqual(ok, elock_test_utils:unlock(Loser, Again)),
  ?assertEqual(undefined, elock_test_utils:context(Loser)),
  elock_test_utils:wait_idle(Scope),
  stop([A, B]).

%%-----------------------------------------------------------------
%%  The loser repeats its request while it still holds its term: the
%%  cycle is there again and is settled again - exactly one verdict
%%  within a second (either of the two may lose this time), the
%%  other is granted once that loser releases its term
%%-----------------------------------------------------------------
retry_recreates_cycle_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [A, B] = clients(2),
  Requests = setup(Node, [
    {A, [{Scope, t1}], {Scope, t2}},
    {B, [{Scope, t2}], {Scope, t1}}
  ]),
  {LoserR, Verdict} = any_result([ R || {_, R, _} <- Requests ], ?VERDICT),
  ?assertEqual({error, deadlock}, Verdict),
  {value, {Loser, LoserR, [LoserHeld]}, [{Winner, WinnerR, [WinnerHeld]}]} = lists:keytake(LoserR, 2, Requests),
  LoserWants =
    if
      Loser =:= A-> t2;
      true-> t1
    end,
  still_waiting(WinnerR),

  Retry = elock_test_utils:lock_queued(Loser, Scope, LoserWants, [Node], #{}),
  {SecondLoserR, SecondVerdict} = any_result([Retry, WinnerR], ?VERDICT),
  ?assertEqual({error, deadlock}, SecondVerdict),
  {SecondLoser, SecondLoserHeld, SecondWinner, SecondWinnerR, SecondWinnerHeld} =
    case SecondLoserR of
      Retry-> {Loser, LoserHeld, Winner, WinnerR, WinnerHeld};
      WinnerR-> {Winner, WinnerHeld, Loser, Retry, LoserHeld}
    end,
  still_waiting(SecondWinnerR),

  ?assertEqual(ok, elock_test_utils:unlock(SecondLoser, SecondLoserHeld)),
  ?assertEqual(undefined, elock_test_utils:context(SecondLoser)),
  SecondWinnerRef = granted(SecondWinnerR),
  ?assertEqual(ok, elock_test_utils:unlock(SecondWinner, SecondWinnerRef)),
  ?assertEqual(ok, elock_test_utils:unlock(SecondWinner, SecondWinnerHeld)),
  ?assertEqual(undefined, elock_test_utils:context(SecondWinner)),
  elock_test_utils:wait_idle(Scope),
  stop([A, B]).

%%=================================================================
%%  Cross scope
%%=================================================================
%%-----------------------------------------------------------------
%%  The same term in two scopes: A holds t in the first scope, B
%%  holds t in the second, each asks for t in the other scope -
%%  exactly one loser, with either request closing the cycle
%%-----------------------------------------------------------------
cross_scope_cycle_test(Config)->
  Scope1 = ?config(scope, Config),
  Scope2 = ?config(scope2, Config),
  Node = node(),
  lists:foreach(
    fun(Closing)->
      [A, B] = clients(2),
      PartA = {A, [{Scope1, t}], {Scope2, t}},
      PartB = {B, [{Scope2, t}], {Scope1, t}},
      Requests =
        case Closing of
          a_closes-> setup(Node, [PartB, PartA]);
          b_closes-> setup(Node, [PartA, PartB])
        end,
      Verdicts = resolve(Requests),
      ?assertEqual(1, length(losers(Verdicts))),
      ?assertEqual(1, length(winners(Verdicts))),
      ?WAIT(elock_test_utils:locks(Scope1) =:= []),
      elock_test_utils:wait_idle(Scope2),
      stop([A, B])
    end,
    [a_closes, b_closes]
  ).

%%-----------------------------------------------------------------
%%  Different terms in two scopes: A holds t1 in the first scope, B
%%  holds t2 in the second, A asks t2 in the second, B asks t1 in
%%  the first - exactly one loser
%%-----------------------------------------------------------------
cross_scope_different_terms_cycle_test(Config)->
  Scope1 = ?config(scope, Config),
  Scope2 = ?config(scope2, Config),
  Node = node(),
  [A, B] = clients(2),
  Requests = setup(Node, [
    {A, [{Scope1, t1}], {Scope2, t2}},
    {B, [{Scope2, t2}], {Scope1, t1}}
  ]),
  {LoserR, Verdict} = any_result([ R || {_, R, _} <- Requests ], ?VERDICT),
  ?assertEqual({error, deadlock}, Verdict),
  {value, {Loser, LoserR, [LoserHeld]}, [{Winner, WinnerR, [WinnerHeld]}]} = lists:keytake(LoserR, 2, Requests),
  still_waiting(WinnerR),

  ?assertEqual(ok, elock_test_utils:unlock(Loser, LoserHeld)),
  WinnerRef = granted(WinnerR),
  ?assertEqual(ok, elock_test_utils:unlock(Winner, WinnerRef)),
  ?assertEqual(ok, elock_test_utils:unlock(Winner, WinnerHeld)),
  ?WAIT(elock_test_utils:locks(Scope1) =:= []),
  elock_test_utils:wait_idle(Scope2),
  stop([A, B]).

%%-----------------------------------------------------------------
%%  No false positive across scopes: A holds t in the first scope
%%  and waits for t in the second, held by B; C waits for t in the
%%  first scope holding nothing; B asks t in the second scope again
%%  (re-entrant) and gets it at once. Nobody gets a deadlock, the
%%  chain drains once B unlocks
%%-----------------------------------------------------------------
cross_scope_no_false_positive_test(Config)->
  Scope1 = ?config(scope, Config),
  Scope2 = ?config(scope2, Config),
  Node = node(),
  [A, B, C] = clients(3),
  {ok, RefB} = elock_test_utils:lock(B, Scope2, t, [Node]),
  Requests = setup(Node, [
    {A, [{Scope1, t}], {Scope2, t}},
    {C, [], {Scope1, t}}
  ]),
  [ still_waiting(R) || {_, R, _} <- Requests ],
  {ok, RefB2} = elock_test_utils:lock(B, Scope2, t, [Node]),
  [ still_waiting(R) || {_, R, _} <- Requests ],

  ?assertEqual(ok, elock_test_utils:unlock(B, RefB2)),
  ?assertEqual(ok, elock_test_utils:unlock(B, RefB)),
  Verdicts = resolve(Requests),
  ?assertEqual([], losers(Verdicts)),
  ?assertEqual(lists:sort([A, C]), lists:sort(winners(Verdicts))),
  [ ?assertEqual(undefined, elock_test_utils:context(Client)) || Client <- [A, B, C] ],
  ?WAIT(elock_test_utils:locks(Scope1) =:= []),
  elock_test_utils:wait_idle(Scope2),
  stop([A, B, C]).

%%-----------------------------------------------------------------
%%  The weight counts the holds of every scope: A holds t1 in the
%%  first scope and tx in the second (weight 2), B holds t2 in the
%%  first (weight 1); A asks t2 and B asks t1, both in the first
%%  scope - B always loses, whichever request closes the cycle, ten
%%  rounds each. The same with the cycle across the scopes: A holds
%%  t1 and tx in the first, B holds t2 in the second, A asks t2 in
%%  the second, B asks t1 in the first
%%-----------------------------------------------------------------
cross_scope_weight_test(Config)->
  Scope1 = ?config(scope, Config),
  Scope2 = ?config(scope2, Config),
  Node = node(),
  Shapes = [
    % the cycle within the first scope, the extra hold in the second
    {[{Scope1, t1}, {Scope2, tx}], {Scope1, t2}, [{Scope1, t2}], {Scope1, t1}},
    % the cycle across the scopes, the extra hold in the first
    {[{Scope1, t1}, {Scope1, tx}], {Scope2, t2}, [{Scope2, t2}], {Scope1, t1}}
  ],
  lists:foreach(
    fun({{HeavyHeld, HeavyWant, LightHeld, LightWant}, Closing, _Round})->
      [Heavy, Light] = clients(2),
      HeavyPart = {Heavy, HeavyHeld, HeavyWant},
      LightPart = {Light, LightHeld, LightWant},
      Requests =
        case Closing of
          heavy_closes-> setup(Node, [LightPart, HeavyPart]);
          light_closes-> setup(Node, [HeavyPart, LightPart])
        end,
      Verdicts = resolve(Requests),
      ?assertEqual([Light], losers(Verdicts)),
      ?assertEqual([Heavy], winners(Verdicts)),
      ?WAIT(elock_test_utils:locks(Scope1) =:= []),
      elock_test_utils:wait_idle(Scope2),
      stop([Heavy, Light])
    end,
    [ {Shape, Closing, Round} || Shape <- Shapes, Round <- lists:seq(1, 10), Closing <- [heavy_closes, light_closes] ]
  ).

%%=================================================================
%%  Mixed
%%=================================================================
%%-----------------------------------------------------------------
%%  Both requests of a cycle have a timeout: the loser gets
%%  {error, deadlock} at once, the winner - still waiting for the
%%  loser's term - gets {error, timeout} when its timer runs out;
%%  each gets exactly one verdict, both keep their held terms
%%-----------------------------------------------------------------
deadlock_and_timeout_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  [A, B] = clients(2),
  Timeout = 1000,
  [{A, RA, [RefA]}, {B, RB, [RefB]}] = setup(Node, [
    {A, [{Scope, t1}], {Scope, t2, #{timeout => Timeout}}},
    {B, [{Scope, t2}], {Scope, t1, #{timeout => Timeout}}}
  ]),
  {LoserR, Verdict} = any_result([RA, RB], ?VERDICT),
  ?assertEqual({error, deadlock}, Verdict),
  [WinnerR] = [RA, RB] -- [LoserR],
  ?assertEqual({ok, {error, timeout}}, elock_test_utils:result(WinnerR, Timeout + ?DEADLINE)),
  ?NO_MESSAGE,

  M1 = elock_test_utils:wait_manager(Scope, t1),
  M2 = elock_test_utils:wait_manager(Scope, t2),
  ?assertEqual(#context{
    ref2lock = #{ RefA => #lock{ scope = Scope, term = t1, nodes = #{ Node => M1 } } },
    locked = #{ {Scope, t1, Node} => {M1, 1} }
  }, elock_test_utils:context(A)),
  ?assertEqual(#context{
    ref2lock = #{ RefB => #lock{ scope = Scope, term = t2, nodes = #{ Node => M2 } } },
    locked = #{ {Scope, t2, Node} => {M2, 1} }
  }, elock_test_utils:context(B)),
  ?assertEqual([{t1, M1, 2}, {t2, M2, 2}], lists:sort(elock_test_utils:locks(Scope))),

  ?assertEqual(ok, elock_test_utils:unlock(A, RefA)),
  ?assertEqual(ok, elock_test_utils:unlock(B, RefB)),
  elock_test_utils:wait_idle(Scope),
  stop([A, B]).

%%-----------------------------------------------------------------
%%  A ring of ten: every client holds its own term and asks for the
%%  next one, the last request closes the cycle. Exactly one loser
%%  (the requests are issued one after another, so only the closing
%%  request's probe finds a cycle), every other client is granted
%%  as the ring drains from the loser on
%%-----------------------------------------------------------------
ring_test(Config)->
  Scope = ?config(scope, Config),
  Node = node(),
  N = 10,
  Clients = clients(N),
  Parts =
    [ {Client, [{Scope, {t, I}}], {Scope, {t, I rem N + 1}}}
      || {Client, I} <- lists:zip(Clients, lists:seq(1, N)) ],
  Requests = setup(Node, Parts),
  {LoserR, Verdict} = any_result([ R || {_, R, _} <- Requests ], ?VERDICT),
  ?assertEqual({error, deadlock}, Verdict),
  {value, {Loser, LoserR, [LoserHeld]}, Rest} = lists:keytake(LoserR, 2, Requests),
  [ still_waiting(R) || {_, R, _} <- Rest ],

  ?assertEqual(ok, elock_test_utils:unlock(Loser, LoserHeld)),
  Verdicts = resolve(Rest),
  ?assertEqual([], losers(Verdicts)),
  ?assertEqual(N - 1, length(winners(Verdicts))),
  [ ?assertEqual(undefined, elock_test_utils:context(Client)) || Client <- Clients ],
  elock_test_utils:wait_idle(Scope),
  stop(Clients).

%%=================================================================
%%  Utilities
%%=================================================================
% N clients
clients(N)->
  [ elock_test_utils:client() || _ <- lists:seq(1, N) ].

stop(Clients)->
  [ elock_test_utils:stop(Client) || Client <- Clients ],
  ok.

% A scenario: every participant {Client, Held, Want} takes its Held
% locks, then the Want requests are issued in the order of the list,
% each one taken by its manager before the next is issued. A lock is
% {Scope, Term} (exclusive) or {Scope, Term, Options}. The result is
% [{Client, Request, HeldRefs}]
setup(Node, Participants)->
  Holding =
    [ {Client, [ take(Client, Node, Lock) || Lock <- Held ], Want}
      || {Client, Held, Want} <- Participants ],
  [ {Client, ask(Client, Node, Want), HeldRefs} || {Client, HeldRefs, Want} <- Holding ].

take(Client, Node, {Scope, Term})->
  take(Client, Node, {Scope, Term, #{}});
take(Client, Node, {Scope, Term, Options})->
  {ok, Ref} = elock_test_utils:lock(Client, Scope, Term, [Node], Options),
  Ref.

ask(Client, Node, {Scope, Term})->
  ask(Client, Node, {Scope, Term, #{}});
ask(Client, Node, {Scope, Term, Options})->
  elock_test_utils:lock_queued(Client, Scope, Term, [Node], Options).

% An upgrade request: the client holds the term shared already, so
% it is monitored since its first request and lock_queued/5 can not
% tell whether the manager has taken the upgrade. It has once the
% client waits for the verdict in elock_manager:lock/1 (i.e. the
% request is sent) and the manager's mailbox is empty
upgrade_queued(Client, Scope, Term, Node)->
  Manager = elock_test_utils:wait_manager(Scope, Term),
  R = elock_test_utils:lock_async(Client, Scope, Term, [Node], #{}),
  ?WAIT(
    process_info(Client, current_function) =:= {current_function, {elock_manager, lock, 1}}
    andalso process_info(Client, status) =:= {status, waiting}
    andalso process_info(Manager, message_queue_len) =:= {message_queue_len, 0}
  ),
  R.

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

% The list rotated by N positions
rotate(List, N)->
  Shift = N rem length(List),
  {Head, Tail} = lists:split(Shift, List),
  Tail ++ Head.

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
