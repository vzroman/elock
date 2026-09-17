-module(elock_manager_performance_SUITE).

-include("elock.hrl").
-include_lib("common_test/include/ct.hrl").

%% Common Test API
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  locks_per_second/1
]).

%% Peer API
-export([
  scope_owner/3
]).

-define(LOCKER_COUNT, 16000).
-define(LOCKS_PER_LOCKER, 10000).
-define(RUN_COUNT, 5).
-define(SCOPE, elock_manager_performance_scope).

all()->
  [locks_per_second].

init_per_suite(Config)->
  DistributionStarted = ensure_distribution(),
  CodePaths = lists:usort([
    filename:dirname(code:which(?MODULE)),
    filename:dirname(code:which(elock_manager)),
    filename:dirname(code:which(ecall))
  ]),
  PeerArgs = lists:append([
    ["-pa", Path]
    || Path <- CodePaths
  ]),
  {ok, Peer, PeerNode} = peer:start(#{
    name => peer:random_name(elock_manager_performance),
    args => PeerArgs
  }),
  Nodes = [node(), PeerNode],
  ok = ensure_ecall_started(Nodes),
  [
    {nodes, Nodes},
    {peer, Peer},
    {distribution_started, DistributionStarted}
  |Config].

end_per_suite(Config)->
  Peer = ?config(peer, Config),
  ok = peer:stop(Peer),
  case ?config(distribution_started, Config) of
    true->
      ok = net_kernel:stop();
    false->
      ok
  end.

locks_per_second(Config)->
  Nodes = ?config(nodes, Config),
  {ScopeRef, ScopeOwners} = start_scope_owners(Nodes, ?SCOPE),
  try
    TotalLocks = ?LOCKER_COUNT * ?LOCKS_PER_LOCKER,
    Results = [
      locks_per_second(Nodes, ?SCOPE, TotalLocks)
      || _ <- lists:seq(1, ?RUN_COUNT)
    ],
    Maximum = lists:max(Results),
    Minimum = lists:min(Results),
    Average = round(lists:sum(Results) / ?RUN_COUNT),
    ct:pal(
      "locks/sec over ~B runs (~B locks by ~B lockers on ~p "
      "across ~B nodes per run): max=~B min=~B avg=~B",
      [
        ?RUN_COUNT,
        TotalLocks,
        ?LOCKER_COUNT,
        node(),
        length(Nodes),
        Maximum,
        Minimum,
        Average
      ]
    ),
    ok
  after
    stop_scope_owners(ScopeOwners, ScopeRef)
  end.

locks_per_second(Nodes, Scope, TotalLocks)->
  RunRef = make_ref(),
  Lockers = [
    spawn_monitor(
      fun()-> locker(RunRef, Nodes, Scope) end
    )
    || _ <- lists:seq(1, ?LOCKER_COUNT)
  ],
  {ElapsedMicroseconds, ok} = timer:tc(
    fun()->
      start_lockers(Lockers, RunRef),
      wait_for_lockers(Lockers)
    end
  ),
  ok = wait_until_released(Nodes, Scope),
  round(TotalLocks * 1000000 / ElapsedMicroseconds).

locker(RunRef, Nodes, Scope)->
  receive
    {start, RunRef}->
      ok = lock_and_unlock(Nodes, Scope, ?LOCKS_PER_LOCKER)
  end.

start_lockers(Lockers, RunRef)->
  lists:foreach(
    fun({Locker, _MonitorRef})->
      Locker ! {start, RunRef}
    end,
    Lockers
  ).

wait_for_lockers([])->
  ok;
wait_for_lockers([{Locker, MonitorRef}|Rest])->
  receive
    {'DOWN', MonitorRef, process, Locker, normal}->
      wait_for_lockers(Rest);
    {'DOWN', MonitorRef, process, Locker, Reason}->
      error({locker_failed, Locker, Reason})
  end.

lock_and_unlock(_Nodes, _Scope, 0)->
  ok;
lock_and_unlock(Nodes, Scope, Remaining)->
  Term = make_ref(),
  Request = #request{
    ref = Term,
    scope = Scope,
    term = Term,
    client = self(),
    reply_to = self(),
    shared = false,
    held = [],
    nodes = [],
    timeout = infinity
  },
  {Replies, []} = ecall:call_all_wait(
    Nodes,
    elock_manager,
    lock,
    [Request]
  ),
  NodeCount = length(Nodes),
  NodeCount = length(Replies),
  Unlocks = [
    Unlock
    || {_Node, {ok, Unlock}} <- Replies
  ],
  NodeCount = length(Unlocks),
  lists:foreach(
    fun(Unlock)->
      ok = elock_manager:unlock(Unlock)
    end,
    Unlocks
  ),
  lock_and_unlock(Nodes, Scope, Remaining - 1).

start_scope_owners(Nodes, Scope)->
  ScopeRef = make_ref(),
  Parent = self(),
  ScopeOwners = [
    spawn_monitor(
      Node,
      ?MODULE,
      scope_owner,
      [Parent, ScopeRef, Scope]
    )
    || Node <- Nodes
  ],
  ok = wait_for_scope_owners(ScopeOwners, ScopeRef),
  {ScopeRef, ScopeOwners}.

-spec scope_owner(pid(), reference(), atom()) -> ok.
scope_owner(Parent, ScopeRef, Scope)->
  Scope = ets:new(Scope, [
    named_table,
    public,
    ordered_set,
    {write_concurrency, auto}
  ]),
  Parent ! {scope_ready, ScopeRef, self()},
  receive
    {stop_scope, ScopeRef}->
      ok;
    Unexpected->
      error({unexpected_message, Unexpected})
  end.

wait_for_scope_owners([], _ScopeRef)->
  ok;
wait_for_scope_owners([{Owner, MonitorRef}|Rest], ScopeRef)->
  receive
    {scope_ready, ScopeRef, Owner}->
      wait_for_scope_owners(Rest, ScopeRef);
    {'DOWN', MonitorRef, process, Owner, Reason}->
      error({scope_owner_failed, Owner, Reason})
  end.

stop_scope_owners(ScopeOwners, ScopeRef)->
  lists:foreach(
    fun({Owner, _MonitorRef})->
      Owner ! {stop_scope, ScopeRef}
    end,
    ScopeOwners
  ),
  wait_for_scope_owners_down(ScopeOwners).

wait_for_scope_owners_down([])->
  ok;
wait_for_scope_owners_down([{Owner, MonitorRef}|Rest])->
  receive
    {'DOWN', MonitorRef, process, Owner, normal}->
      wait_for_scope_owners_down(Rest);
    {'DOWN', MonitorRef, process, Owner, Reason}->
      error({scope_owner_failed, Owner, Reason})
  end.

wait_until_released(Nodes, Scope)->
  wait_until_released(Nodes, Scope, 10000).

wait_until_released(Nodes, Scope, 0)->
  [0, 0] = scope_sizes(Nodes, Scope),
  ok;
wait_until_released(Nodes, Scope, Attempts)->
  case scope_sizes(Nodes, Scope) of
    [0, 0]->
      ok;
    _PendingLocks->
      receive after 1 -> ok end,
      wait_until_released(Nodes, Scope, Attempts - 1)
  end.

scope_sizes(Nodes, Scope)->
  {Replies, []} = ecall:call_all_wait(
    Nodes,
    ets,
    info,
    [Scope, size]
  ),
  lists:sort([
    Size
    || {_Node, Size} <- Replies
  ]).

ensure_ecall_started(Nodes)->
  {Replies, []} = ecall:call_all_wait(
    Nodes,
    application,
    ensure_all_started,
    [ecall]
  ),
  NodeCount = length(Nodes),
  NodeCount = length([
    ok
    || {_Node, {ok, _StartedApplications}} <- Replies
  ]),
  ok.

ensure_distribution()->
  case node() of
    nonode@nohost->
      {ok, _NetKernel} = net_kernel:start([
        elock_manager_performance_node1,
        shortnames
      ]),
      true;
    _Node->
      false
  end.
