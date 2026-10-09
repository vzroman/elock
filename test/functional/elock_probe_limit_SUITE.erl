
%%=================================================================
%%  Context lifetime and bounded deadlock walks. The graph fixtures
%%  describe reachable waiter rows; the age test uses real managers
%%  on two nodes and observes the client's graph publications.
%%=================================================================
-module(elock_probe_limit_SUITE).

-include("elock.hrl").
-include_lib("stdlib/include/assert.hrl").

-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  context_lifetime_test/1,
  acyclic_budget_test/1,
  age_budget_test/1,
  initial_frontier_budget_test/1,
  merged_holds_budget_test/1,
  ordinary_cycle_test/1,
  obsolete_origin_test/1,
  replacement_origin_test/1,
  remote_budget_test/1,
  probe_limit_cancellation_test/1,
  fixed_request_age_test/1,
  start_scope/0,
  client_loop/0
]).

-record(waiter,{
  lock :: lock_key(),
  ref :: reference(),
  birth :: non_neg_integer(),
  client :: pid(),
  holds :: held_locks()
}).

-define(SCOPE, elock_probe_limit_scope).
-define(CONTEXT, '$elock_context$').

%%=================================================================
%%  Common Test
%%=================================================================
-spec all() -> [atom()].
all()->
  [
    context_lifetime_test,
    acyclic_budget_test,
    age_budget_test,
    initial_frontier_budget_test,
    merged_holds_budget_test,
    ordinary_cycle_test,
    obsolete_origin_test,
    replacement_origin_test,
    remote_budget_test,
    probe_limit_cancellation_test,
    fixed_request_age_test
  ].

-spec init_per_suite(list()) -> list().
init_per_suite(Config)->
  start_distribution(),
  {ok, _} = application:ensure_all_started(elock),
  Scope = start_scope(),
  Paths = lists:usort([
    filename:dirname(code:which(?MODULE)),
    filename:dirname(code:which(elock)),
    filename:dirname(code:which(ecall))
  ]),
  {ok, Peer, Node} = peer:start_link(#{
    name => list_to_atom("elock_probe_peer_" ++ os:getpid()),
    connection => standard_io,
    args => ["+S", "2", "-setcookie", atom_to_list(erlang:get_cookie()), "-pa" | Paths]
  }),
  unlink(Peer),
  {ok, _} = erpc:call(Node, application, ensure_all_started, [elock]),
  _RemoteScope = erpc:call(Node, ?MODULE, start_scope, []),
  [{scope, Scope}, {peer, Peer}, {node, Node} | Config].

-spec end_per_suite(list()) -> ok.
end_per_suite(Config)->
  peer:stop(proplists:get_value(peer, Config)),
  exit(proplists:get_value(scope, Config), shutdown),
  application:stop(elock),
  application:stop(ecall),
  ok.

%%=================================================================
%%  Context lifetime: reentry and unknown unlock keep the context;
%%  the last release erases it, and the next lock starts a new one.
%%=================================================================
-spec context_lifetime_test(list()) -> ok.
context_lifetime_test(_Config)->
  ?assertEqual(undefined, get(?CONTEXT)),
  {ok, First} = elock:lock(?SCOPE, held, [node()]),
  Context = get(?CONTEXT),
  Birth = element(5, Context),
  Held = element(3, Context),
  {ok, Second} = elock:lock(?SCOPE, held, [node()]),
  Reentered = get(?CONTEXT),
  ?assertEqual(Birth, element(5, Reentered)),
  ?assertEqual(Held, element(3, Reentered)),
  ?assertEqual(2, map_size(element(2, Reentered))),
  ok = elock:unlock(make_ref()),
  ?assertEqual(Reentered, get(?CONTEXT)),
  ok = elock:unlock(First),
  Remaining = get(?CONTEXT),
  ?assertEqual(Birth, element(5, Remaining)),
  ?assertEqual(Held, element(3, Remaining)),
  ?assertEqual(1, map_size(element(2, Remaining))),
  ok = elock:unlock(Second),
  ?assertEqual(undefined, get(?CONTEXT)),
  {ok, Third} = elock:lock(?SCOPE, next, [node()]),
  ?assert(element(5, get(?CONTEXT)) > Birth),
  ok = elock:unlock(Third),
  ?assertEqual(undefined, get(?CONTEXT)),
  ok.

%%=================================================================
%%  The minimum includes the origin and scheduled entry. Repeated
%%  keys at exactly the limit are harmless; one new key exceeds it.
%%=================================================================
-spec acyclic_budget_test(list()) -> ok.
acyclic_budget_test(_Config)->
  fanout(0, 98, quiet),
  fanout(0, 99, probe_limit),
  Origin = key(node()),
  Held = key(node()),
  Ref = make_ref(),
  Row = #waiter{
    lock = Origin, ref = Ref, birth = 1, client = self(),
    holds = #{Held => self()}
  },
  Repeated = #waiter{
    lock = Held, ref = make_ref(), birth = 2, client = self(),
    holds = #{Held => self()}
  },
  Visited = maps:map(fun(_Key, _Manager)-> true end, leaves(98, node())),
  Probe = #deadlock_probe{
    ref = Ref, edge = Origin, client = self(), birth = 1,
    limit = 100, expand = [Held],
    visited = Visited#{Origin => true, Held => true}
  },
  ets:insert(elock_graph, [Row, Repeated]),
  try
    ok = elock_graph:handle_probe(Probe),
    no_deadlock(Ref),
    ok = elock_graph:handle_probe(Probe#deadlock_probe{expand = []}),
    no_deadlock(Ref)
  after
    ets:delete_object(elock_graph, Row),
    ets:delete_object(elock_graph, Repeated)
  end.

%%=================================================================
%%  Milliseconds increase the budget without a step. The ceiling
%%  still applies to an old context.
%%=================================================================
-spec age_budget_test(list()) -> ok.
age_budget_test(_Config)->
  fanout(100, 108, quiet),
  fanout(100, 109, probe_limit),
  fanout(20000, 9998, quiet),
  fanout(20000, 9999, probe_limit).

%%=================================================================
%%  The origin itself consumes a slot: ten thousand distinct held
%%  keys already exceed the ceiling before any waiter is expanded.
%%=================================================================
-spec initial_frontier_budget_test(list()) -> ok.
initial_frontier_budget_test(_Config)->
  Ref = make_ref(),
  Holds = leaves(10000, node()),
  Row = #waiter{
    lock = key(node()), ref = Ref, birth = 1, client = self(),
    holds = Holds
  },
  ets:insert(elock_graph, Row),
  try
    ok = elock_graph:launch(Row, maps:keys(Holds), 0),
    deadlock(Ref, probe_limit)
  after
    ets:delete_object(elock_graph, Row)
  end.

%%=================================================================
%%  A partial grant gets the budget of all merged holds, not just
%%  the new hold whose branch it launches.
%%=================================================================
-spec merged_holds_budget_test(list()) -> ok.
merged_holds_budget_test(_Config)->
  Origin = key(node()),
  Held = key(node()),
  Ref = make_ref(),
  Old = #waiter{
    lock = Origin, ref = Ref, birth = 1, client = self(),
    holds = leaves(10, node())
  },
  Branch = #waiter{
    lock = Held, ref = make_ref(), birth = 2, client = self(),
    holds = leaves(104, node())
  },
  ets:insert(elock_graph, [Old, Branch]),
  try
    ok = elock_graph:handle_add_edges(#add_edges{
      lock = Origin, ref = Ref, birth = 1, age = 0,
      client = self(), holds = #{Held => self()}
    }),
    [Merged] = ets:lookup(elock_graph, Origin),
    ?assertEqual(11, map_size(Merged#waiter.holds)),
    ?assertEqual(Old#waiter.holds, maps:remove(Held, Merged#waiter.holds)),
    no_deadlock(Ref)
  after
    ets:delete(elock_graph, Origin),
    ets:delete_object(elock_graph, Branch)
  end.

%%=================================================================
%%  The bound does not change ordinary cycle detection or its winner.
%%=================================================================
-spec ordinary_cycle_test(list()) -> ok.
ordinary_cycle_test(_Config)->
  Origin = key(node()),
  Held = key(node()),
  Ref = make_ref(),
  Row = #waiter{
    lock = Origin, ref = Ref, birth = 2, client = self(),
    holds = #{Held => self()}
  },
  Closer = #waiter{
    lock = Held, ref = make_ref(), birth = 1, client = self(),
    holds = #{Origin => self()}
  },
  ets:insert(elock_graph, [Row, Closer]),
  try
    ok = elock_graph:launch(Row, [Held], 0),
    deadlock(Ref, Held)
  after
    ets:delete_object(elock_graph, Row),
    ets:delete_object(elock_graph, Closer)
  end.

%%=================================================================
%%  Delayed launches and returning continuations retire when the
%%  exact origin request is gone. Another waiter is not that origin.
%%=================================================================
-spec obsolete_origin_test(list()) -> ok.
obsolete_origin_test(_Config)->
  Origin = key(node()),
  Held = key(node()),
  Ref = make_ref(),
  Row = #waiter{
    lock = Origin, ref = Ref, birth = 1, client = self(),
    holds = #{Held => self()}
  },
  Branch = #waiter{
    lock = Held, ref = make_ref(), birth = 2, client = self(),
    holds = leaves(101, node())
  },
  Other = Row#waiter{ref = make_ref()},
  ets:insert(elock_graph, Branch),
  try
    ok = elock_graph:launch(Row, [Held], 0),
    no_deadlock(Ref),
    ets:insert(elock_graph, Other),
    ok = elock_graph:launch(Row, [Held], 0),
    no_deadlock(Ref),
    ok = elock_graph:handle_probe(#deadlock_probe{
      ref = Ref, edge = Origin, client = self(), birth = 1,
      limit = 100, expand = [Held], visited = #{Origin => true, Held => true}
    }),
    no_deadlock(Ref)
  after
    ets:delete(elock_graph, Origin),
    ets:delete_object(elock_graph, Branch)
  end.

%%=================================================================
%%  Insert-before-delete can expose two versions of the origin.
%%  Both carry the same live request and must not retire its walk.
%%=================================================================
-spec replacement_origin_test(list()) -> ok.
replacement_origin_test(_Config)->
  Origin = key(node()),
  Held = key(node()),
  Ref = make_ref(),
  Row = #waiter{
    lock = Origin, ref = Ref, birth = 1, client = self(),
    holds = #{Held => self()}
  },
  Replacement = Row#waiter{holds = (Row#waiter.holds)#{key(node()) => self()}},
  Branch = #waiter{
    lock = Held, ref = make_ref(), birth = 2, client = self(),
    holds = leaves(101, node())
  },
  ets:insert(elock_graph, [Row, Replacement, Branch]),
  try
    ok = elock_graph:launch(Row, [Held], 0),
    deadlock(Ref, probe_limit)
  after
    ets:delete(elock_graph, Origin),
    ets:delete_object(elock_graph, Branch)
  end.

%%=================================================================
%%  A real cast carries the launch budget to the other node. That
%%  node has no origin row: it must continue, then reject at 111
%%  visited keys with the frozen 110-key budget.
%%=================================================================
-spec remote_budget_test(list()) -> ok.
remote_budget_test(Config)->
  Node = proplists:get_value(node, Config),
  Origin = key(node()),
  Held = key(Node),
  Ref = make_ref(),
  Row = #waiter{
    lock = Origin, ref = Ref, birth = 1, client = self(),
    holds = #{Held => self()}
  },
  Branch = #waiter{
    lock = Held, ref = make_ref(), birth = 2, client = self(),
    holds = leaves(109, Node)
  },
  ets:insert(elock_graph, Row),
  true = erpc:call(Node, ets, insert, [elock_graph, Branch]),
  try
    ok = elock_graph:launch(Row, [Held], 100),
    deadlock(Ref, probe_limit)
  after
    ets:delete_object(elock_graph, Row),
    erpc:call(Node, ets, delete_object, [elock_graph, Branch])
  end.

%%=================================================================
%%  The public API returns the probe-limit verdict, withdraws the
%%  failed acquisition, and leaves earlier locks for the caller.
%%=================================================================
-spec probe_limit_cancellation_test(list()) -> ok.
probe_limit_cancellation_test(_Config)->
  Term = make_ref(),
  HeldTerm = make_ref(),
  Blocker = spawn(?MODULE, client_loop, []),
  Client = spawn(?MODULE, client_loop, []),
  Branch = #waiter{
    lock = {?SCOPE, HeldTerm, node()},
    ref = make_ref(), birth = 1, client = self(),
    holds = leaves(10001, node())
  },
  try
    {ok, BlockerRef} = call(Blocker, fun()-> elock:lock(?SCOPE, Term, [node()]) end),
    {ok, HeldRef} = call(Client, fun()-> elock:lock(?SCOPE, HeldTerm, [node()]) end),
    Context = call(Client, fun()-> get(?CONTEXT) end),
    ets:insert(elock_graph, Branch),
    ?assertEqual({error, {deadlock, probe_limit}},
      call(Client, fun()-> elock:lock(?SCOPE, Term, [node()]) end)),
    ?assertEqual(Context, call(Client, fun()-> get(?CONTEXT) end)),
    ets:delete_object(elock_graph, Branch),
    ok = call(Blocker, fun()-> elock:unlock(BlockerRef) end),
    {ok, Acquired} = call(Client, fun()-> elock:lock(?SCOPE, Term, [node()]) end),
    ok = call(Client, fun()-> elock:unlock(Acquired), elock:unlock(HeldRef) end),
    ?assertEqual(undefined, call(Client, fun()-> get(?CONTEXT) end))
  after
    ets:delete_object(elock_graph, Branch),
    [exit(Pid, kill) || Pid <- [Client, Blocker]]
  end,
  ok.

%%=================================================================
%%  One public acquisition queues on both nodes. A delayed grant
%%  updates the still-waiting copy with exactly the same age.
%%=================================================================
-spec fixed_request_age_test(list()) -> ok.
fixed_request_age_test(Config)->
  Node = proplists:get_value(node, Config),
  Term = make_ref(),
  LocalBlocker = spawn(?MODULE, client_loop, []),
  RemoteBlocker = spawn(?MODULE, client_loop, []),
  Client = spawn(?MODULE, client_loop, []),
  Session = trace:session_create(?MODULE, self(), []),
  try
    {ok, LocalRef} = call(LocalBlocker, fun()-> elock:lock(?SCOPE, Term, [node()]) end),
    {ok, RemoteRef} = call(RemoteBlocker, fun()-> elock:lock(?SCOPE, Term, [Node]) end),
    {ok, HeldRef} = call(Client, fun()-> elock:lock(?SCOPE, make_ref(), [node()]) end),
    1 = trace:function(Session, {ecall, call, 4}, true, [local]),
    1 = trace:process(Session, Client, true, [call, set_on_spawn]),
    Started = erlang:system_time(microsecond),
    Request = cast(Client, fun()-> elock:lock(?SCOPE, Term, [node(), Node]) end),
    First = publication(),
    Second = publication(),
    ?assertEqual(First#add_edges.ref, Second#add_edges.ref),
    ?assertEqual(First#add_edges.age, Second#add_edges.age),
    ?assert(First#add_edges.age =< (erlang:system_time(microsecond) - First#add_edges.birth) div 1000),
    ?assert(First#add_edges.age >= max(0, (Started - First#add_edges.birth) div 1000)),
    receive after 30-> ok end,
    ok = call(LocalBlocker, fun()-> elock:unlock(LocalRef) end),
    Later = publication(),
    ?assertEqual({?SCOPE, Term, Node}, Later#add_edges.lock),
    ?assertEqual(First#add_edges.ref, Later#add_edges.ref),
    ?assertEqual(First#add_edges.age, Later#add_edges.age),
    ?assert((erlang:system_time(microsecond) - Later#add_edges.birth) div 1000 >= Later#add_edges.age + 20),
    ok = call(RemoteBlocker, fun()-> elock:unlock(RemoteRef) end),
    {ok, Acquired} = result(Request),
    ok = call(Client, fun()-> elock:unlock(Acquired), elock:unlock(HeldRef) end),
    ?assertEqual(undefined, call(Client, fun()-> get(?CONTEXT) end))
  after
    trace:session_destroy(Session),
    [exit(Pid, kill) || Pid <- [Client, LocalBlocker, RemoteBlocker]]
  end,
  ok.

%%=================================================================
%%  Helpers
%%=================================================================
-spec fanout(non_neg_integer(), non_neg_integer(), quiet | probe_limit) -> ok.
fanout(Age, Count, Expected)->
  Origin = key(node()),
  Held = key(node()),
  Ref = make_ref(),
  Row = #waiter{
    lock = Origin, ref = Ref, birth = 1, client = self(),
    holds = #{Held => self()}
  },
  % Held repeats an already scheduled key; it must not consume credit.
  Branch = #waiter{
    lock = Held, ref = make_ref(), birth = 2, client = self(),
    holds = (leaves(Count, node()))#{Held => self()}
  },
  ets:insert(elock_graph, [Row, Branch]),
  try
    ok = elock_graph:launch(Row, [Held, Held], Age),
    case Expected of
      quiet-> no_deadlock(Ref);
      probe_limit-> deadlock(Ref, probe_limit)
    end
  after
    ets:delete_object(elock_graph, Row),
    ets:delete_object(elock_graph, Branch)
  end.

-spec key(node()) -> lock_key().
key(Node)->
  {?SCOPE, make_ref(), Node}.

-spec leaves(non_neg_integer(), node()) -> held_locks().
leaves(Count, Node)->
  maps:from_list([{key(Node), self()} || _ <- lists:seq(1, Count)]).

-spec deadlock(reference(), term()) -> ok.
deadlock(Ref, Winner)->
  receive
    #deadlock{ref = Ref, winner = Actual}-> ?assertEqual(Winner, Actual)
  after 5000->
    ct:fail(no_deadlock_verdict)
  end.

-spec no_deadlock(reference()) -> ok.
no_deadlock(Ref)->
  receive
    #deadlock{ref = Ref} = Deadlock-> ct:fail({unexpected_deadlock, Deadlock})
  after 50->
    ok
  end.

-spec publication() -> #add_edges{}.
publication()->
  receive
    {trace, _Pid, call, {ecall, call, [_Node, elock_graph, handle_add_edges, [Message]]}}->
      Message
  after 5000->
    ct:fail(no_graph_publication)
  end.

-spec start_scope() -> pid().
start_scope()->
  {ok, Scope} = elock:start_link(?SCOPE),
  unlink(Scope),
  await(fun()-> ets:whereis(?SCOPE) =/= undefined end),
  Scope.

-spec start_distribution() -> ok.
start_distribution()->
  case node() of
    nonode@nohost->
      Epmd = filename:join([code:root_dir(), "erts-" ++ erlang:system_info(version), "bin", "epmd"]),
      _ = os:cmd(Epmd ++ " -daemon"),
      Name = list_to_atom("elock_probe_controller_" ++ os:getpid()),
      {ok, _} = net_kernel:start([Name, shortnames]),
      ok;
    _->
      ok
  end.

-spec client_loop() -> no_return().
client_loop()->
  receive
    {From, Ref, Fun}->
      From ! {Ref, Fun()},
      client_loop()
  end.

-spec cast(pid(), fun(() -> term())) -> reference().
cast(Client, Fun)->
  Ref = make_ref(),
  Client ! {self(), Ref, Fun},
  Ref.

-spec call(pid(), fun(() -> term())) -> term().
call(Client, Fun)->
  result(cast(Client, Fun)).

-spec result(reference()) -> term().
result(Ref)->
  receive
    {Ref, Result}-> Result
  after 5000->
    ct:fail(client_timeout)
  end.

-spec await(fun(() -> boolean())) -> ok.
await(Fun)->
  await(Fun, erlang:monotonic_time(millisecond) + 5000).

-spec await(fun(() -> boolean()), integer()) -> ok.
await(Fun, Deadline)->
  case Fun() of
    true->
      ok;
    false->
      ?assert(erlang:monotonic_time(millisecond) < Deadline),
      receive after 1-> ok end,
      await(Fun, Deadline)
  end.
