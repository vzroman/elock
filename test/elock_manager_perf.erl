%%=================================================================
%%  Throughput harness for elock_manager.
%%
%%  It drives elock_manager:lock/1 and elock_manager:unlock/1
%%  directly - no elock facade, no process dictionary, no pg, no
%%  ecall, one node - so that the numbers describe the manager and
%%  nothing around it. Every request carries held = [] and
%%  nodes = [], which is exactly the can_not_have_deadlocks shortcut
%%  of elock_deadlock:check_deadlock/6, therefore no deadlock checker
%%  is ever spawned. The default timeout is infinity, so no timeout
%%  timer is armed either.
%%
%%  A client loops "take the lock, release it at once, ask for the
%%  next one". Either it does that a fixed number of times
%%  (requests => N per client), or, when requests is undefined, for
%%  as long as the duration lasts. The metric is completed
%%  lock/unlock cycles per second either way.
%%
%%  Scenarios - what the N clients compete for:
%%
%%    unique_term - a fresh make_ref() per request. No contention:
%%                  every lock is won by ets:update_counter/4 on the
%%                  first try and costs one manager process.
%%    fixed_terms - M predefined terms, picked at random per request.
%%                  Contention grows with N/M.
%%    single_term - one predefined term. Every client of the run goes
%%                  through the same manager and the same queue.
%%
%%  Modes - what they ask for:
%%
%%    shared      - every request is shared
%%    exclusive   - every request is exclusive
%%    mixed       - shared or exclusive at random, 50/50
%%
%%  Shell usage:
%%
%%    elock_manager_perf:print(#{scenario => single_term, mode => exclusive}).
%%    elock_manager_perf:print_all(#{clients => 10000, requests => 1000,
%%                                   terms => 100, duration => 600000,
%%                                   warmup => 0, runs => 1}).
%%
%%  Every option can also come from the environment, which is what
%%  the common_test suite uses for a short smoke run:
%%
%%    ELOCK_PERF_CLIENTS ELOCK_PERF_TERMS ELOCK_PERF_REQUESTS
%%    ELOCK_PERF_DURATION ELOCK_PERF_WARMUP ELOCK_PERF_RUNS
%%    ELOCK_PERF_TABLE_TYPE ELOCK_PERF_PROCESS_LIMIT
%%=================================================================
-module(elock_manager_perf).

-include("elock.hrl").

%%=================================================================
%%	API
%%=================================================================
-export([
  run/0, run/1,
  run_all/0, run_all/1,
  print/0, print/1,
  print_all/0, print_all/1
]).

-export([
  defaults/0,
  scenarios/0,
  modes/0,
  format_run/1,
  format_matrix/1
]).

-define(SCOPE, elock_manager_perf_scope).

%% The tick of the controller. Every client may overrun the deadline
%% by one tick plus the lock/unlock cycle it has already started
-define(TICK, 2).

%% How long to wait for the managers to disappear after a run
-define(IDLE_ATTEMPTS, 5000).

%%=================================================================
%%  Options
%%=================================================================
scenarios()->
  [unique_term, fixed_terms, single_term].

modes()->
  [shared, exclusive, mixed].

defaults()->
  #{
    scenario    => single_term,          % see scenarios/0
    mode        => exclusive,            % see modes/0
    clients     => env_int("ELOCK_PERF_CLIENTS", 100),
    terms       => env_int("ELOCK_PERF_TERMS", 10),      % M, fixed_terms only
    %% Either a fixed amount of work per client (requests), or, when
    %% it is undefined, whatever fits into the duration
    requests    => env_int("ELOCK_PERF_REQUESTS", undefined),
    %% ms. The measured window when requests is undefined, otherwise
    %% the cap after which an unfinished run is given up
    duration    => env_int("ELOCK_PERF_DURATION", 5000),
    warmup      => env_int("ELOCK_PERF_WARMUP", 1000),   % ms, discarded
    runs        => env_int("ELOCK_PERF_RUNS", 3),
    table_type  => env_atom("ELOCK_PERF_TABLE_TYPE", ordered_set), % set | ordered_set
    scope       => ?SCOPE,
    timeout     => infinity,             % #request.timeout
    %% A run is given up if the node grows past this many processes,
    %% i.e. before spawning a manager would fail outright. The
    %% uncontended scenario spawns a manager per lock and the clients
    %% can outrun them
    process_limit => env_int("ELOCK_PERF_PROCESS_LIMIT", erlang:system_info(process_limit) - 1000)
  }.

%%=================================================================
%%  Running
%%=================================================================
run()->
  run(#{}).

run(Opts0) when is_map(Opts0)->
  Opts = maps:merge(defaults(), Opts0),
  #{
    scope := Scope,
    table_type := TableType,
    warmup := Warmup,
    duration := Duration,
    runs := RunCount
  } = Opts,

  Terms = terms(Opts),

  with_scope(Scope, TableType, fun()->
    if
      Warmup > 0 ->
        _Discarded = measure(Opts, Terms, Warmup),
        ok;
      true ->
        ok
    end,
    Runs = [ measure(Opts, Terms, Duration) || _ <- lists:seq(1, RunCount) ],
    Rates = [ Rate || #{rate := Rate} <- Runs ],
    #{
      opts => Opts,
      runs => Runs,
      min => lists:min(Rates),
      max => lists:max(Rates),
      avg => round(lists:sum(Rates) / length(Rates))
    }
  end).

%% The whole scenario x mode matrix in one ETS table
run_all()->
  run_all(#{}).

run_all(Opts0) when is_map(Opts0)->
  Opts = maps:merge(defaults(), Opts0),
  #{scope := Scope, table_type := TableType} = Opts,
  with_scope(Scope, TableType, fun()->
    [
      {Scenario, Mode, run(Opts#{scenario => Scenario, mode => Mode})}
      || Scenario <- scenarios(), Mode <- modes()
    ]
  end).

print()->
  print(#{}).

print(Opts)->
  Result = run(Opts),
  io:format("~s", [format_run(Result)]),
  Result.

print_all()->
  print_all(#{}).

print_all(Opts)->
  Results = run_all(Opts),
  io:format("~s~s", [
    [ format_run(Result) || {_Scenario, _Mode, Result} <- Results ],
    format_matrix(Results)
  ]),
  Results.

%%=================================================================
%%  One run
%%=================================================================
measure(#{clients := ClientCount, scope := Scope, requests := Requests} = Opts, Terms, Duration)->

  Ref = make_ref(),
  Parent = self(),

  % The clients poll this instead of the clock: an atomic read is
  % cheaper than a BIF call and it lets the controller stop the run
  % on overload as well as on the deadline
  Stop = atomics:new(1, [{signed, false}]),

  Clients = [
    spawn_monitor(fun()-> client(Parent, Ref, Stop, Opts, Terms) end)
    || _ <- lists:seq(1, ClientCount)
  ],
  ok = wait_ready(ClientCount, Ref),

  T0 = erlang:monotonic_time(),
  Deadline = erlang:monotonic_time(millisecond) + Duration,

  % Above the high priority managers, otherwise the deadline is not
  % honoured on a contended run
  Controller = spawn_opt(
    fun()-> controller(Parent, Ref, Stop, Deadline, Opts) end,
    [{priority, max}]
  ),

  _Started = [ ClientPID ! {start, Ref} || {ClientPID, _MonRef} <- Clients ],

  Locks = collect(ClientCount, Ref, 0),
  T1 = erlang:monotonic_time(),

  Controller ! {finish, Ref},
  {PeakProcesses, PeakMemory, RawStopReason} = wait_controller(Controller, Ref),
  _Demonitored = [ erlang:demonitor(MonRef, [flush]) || {_ClientPID, MonRef} <- Clients ],

  Elapsed = erlang:convert_time_unit(T1 - T0, native, microsecond),

  #{
    locks => Locks,
    expected => expected_locks(ClientCount, Requests),
    elapsed_us => Elapsed,
    rate => round(Locks * 1000000 / Elapsed),
    peak_processes => PeakProcesses,
    peak_memory => PeakMemory,
    stop_reason => stop_reason(RawStopReason, Requests),
    % Managers that are still alive would contend with the next run
    leftover => wait_until_idle(Scope, ?IDLE_ATTEMPTS)
  }.

expected_locks(_ClientCount, undefined)->
  undefined;
expected_locks(ClientCount, Requests)->
  ClientCount * Requests.

%% A run that ended the way its mode is supposed to end. In the
%% duration mode the deadline is the end of the measurement, in the
%% requests mode it means the clients did not get through their work
stop_reason(deadline, undefined)->
  ok;
stop_reason(completed, Requests) when Requests =/= undefined->
  ok;
stop_reason(deadline, _Requests)->
  {unfinished, deadline};
stop_reason(Other, _Requests)->
  Other.

%%-----------------------------------------------------------------
%%  The client
%%-----------------------------------------------------------------
client(Parent, Ref, Stop, #{
  scenario := Scenario,
  mode := Mode,
  scope := Scope,
  timeout := Timeout,
  requests := Requests
}, Terms)->
  Parent ! {ready, Ref, self()},
  Left =
    case Requests of
      undefined-> infinity;
      _-> Requests
    end,
  receive
    {start, Ref}->
      Count = client_loop(Scenario, Mode, Terms, Scope, Timeout, Stop, Left, 0),
      Parent ! {done, Ref, self(), Count}
  end.

% Left is the work still to do - infinity in the duration mode, where
% only the stop flag ends the loop
client_loop(_Scenario, _Mode, _Terms, _Scope, _Timeout, _Stop, 0, Count)->
  Count;
client_loop(Scenario, Mode, Terms, Scope, Timeout, Stop, Left, Count)->
  case atomics:get(Stop, 1) of
    0->
      Term = pick_term(Scenario, Terms),
      Request = #request{
        ref = make_ref(),
        scope = Scope,
        term = Term,
        client = self(),
        reply_to = self(),
        shared = pick_shared(Mode),
        held = [],
        nodes = [],
        timeout = Timeout
      },
      case elock_manager:lock(Request) of
        {ok, Unlock}->
          ok = elock_manager:unlock(Unlock),
          client_loop(Scenario, Mode, Terms, Scope, Timeout, Stop, decrement(Left), Count + 1);
        Error->
          error({lock_failed, Error, Term})
      end;
    _Stopped->
      Count
  end.

decrement(infinity)->
  infinity;
decrement(Left)->
  Left - 1.

pick_term(unique_term, _Terms)->
  make_ref();
pick_term(single_term, Terms)->
  element(1, Terms);
pick_term(fixed_terms, Terms)->
  element(rand:uniform(tuple_size(Terms)), Terms).

pick_shared(exclusive)->
  false;
pick_shared(shared)->
  true;
pick_shared(mixed)->
  rand:uniform(2) =:= 1.

terms(#{scenario := unique_term})->
  {};
terms(#{scenario := single_term})->
  {{?MODULE, 1}};
terms(#{scenario := fixed_terms, terms := TermCount}) when TermCount > 0->
  list_to_tuple([ {?MODULE, I} || I <- lists:seq(1, TermCount) ]).

%%-----------------------------------------------------------------
%%  The controller owns the deadline and the overload check
%%-----------------------------------------------------------------
controller(Parent, Ref, Stop, Deadline, #{process_limit := Limit})->
  controller_loop(Parent, Ref, Stop, Deadline, Limit, 0, 0).

controller_loop(Parent, Ref, Stop, Deadline, Limit, PeakProcesses0, PeakMemory0)->
  Processes = erlang:system_info(process_count),
  PeakProcesses = erlang:max(PeakProcesses0, Processes),
  PeakMemory = erlang:max(PeakMemory0, erlang:memory(total)),
  Now = erlang:monotonic_time(millisecond),
  if
    Processes > Limit->
      atomics:put(Stop, 1, 1),
      Parent ! {stats, Ref, PeakProcesses, PeakMemory, {process_limit, Processes}};
    Now >= Deadline->
      atomics:put(Stop, 1, 1),
      Parent ! {stats, Ref, PeakProcesses, PeakMemory, deadline};
    true->
      % In the requests mode the clients finish on their own
      receive
        {finish, Ref}->
          Parent ! {stats, Ref, PeakProcesses, PeakMemory, completed}
      after ?TICK ->
        controller_loop(Parent, Ref, Stop, Deadline, Limit, PeakProcesses, PeakMemory)
      end
  end.

%%-----------------------------------------------------------------
%%  Collecting
%%-----------------------------------------------------------------
wait_ready(0, _Ref)->
  ok;
wait_ready(Left, Ref)->
  receive
    {ready, Ref, _ClientPID}->
      wait_ready(Left - 1, Ref);
    {'DOWN', _MonRef, process, ClientPID, Reason}->
      error({client_failed, ClientPID, Reason})
  end.

%% The 'DOWN's of the clients that have already reported are left in
%% the mailbox on purpose - flushing them by the monitor reference
%% after the clock is stopped keeps them out of the measurement
collect(0, _Ref, Acc)->
  Acc;
collect(Left, Ref, Acc)->
  receive
    {done, Ref, _ClientPID, Count}->
      collect(Left - 1, Ref, Acc + Count);
    {'DOWN', _MonRef, process, _ClientPID, normal}->
      collect(Left, Ref, Acc);
    {'DOWN', _MonRef, process, ClientPID, Reason}->
      error({client_failed, ClientPID, Reason})
  end.

wait_controller(Controller, Ref)->
  MonRef = erlang:monitor(process, Controller),
  Result =
    receive
      {stats, Ref, PeakProcesses, PeakMemory, StopReason}->
        {PeakProcesses, PeakMemory, StopReason};
      {'DOWN', MonRef, process, Controller, Reason}->
        error({controller_failed, Reason})
    end,
  erlang:demonitor(MonRef, [flush]),
  Result.

%%=================================================================
%%  The ETS scope
%%=================================================================
%% A nested call (run/1 inside run_all/1) reuses the table that is
%% already there, so the whole matrix runs against one table
with_scope(Scope, TableType, Fun)->
  case ets:info(Scope, name) of
    undefined->
      Owner = start_scope(Scope, TableType),
      try Fun()
      after
        stop_scope(Owner)
      end;
    _Existing->
      Fun()
  end.

start_scope(Scope, TableType)->
  Parent = self(),
  Ref = make_ref(),
  Owner = spawn(fun()->
    % The same options as elock:start_link/1 gives the real scope
    Scope = ets:new(Scope, [
      named_table,
      public,
      TableType,
      {read_concurrency, true},
      {write_concurrency, auto}
    ]),
    Parent ! {scope_ready, Ref, self()},
    receive
      {stop_scope, Ref}-> ok
    end
  end),
  MonRef = erlang:monitor(process, Owner),
  receive
    {scope_ready, Ref, Owner}->
      erlang:demonitor(MonRef, [flush]),
      {Owner, Ref};
    {'DOWN', MonRef, process, Owner, Reason}->
      error({scope_failed, Scope, Reason})
  end.

stop_scope({Owner, Ref})->
  MonRef = erlang:monitor(process, Owner),
  Owner ! {stop_scope, Ref},
  receive
    {'DOWN', MonRef, process, Owner, _Reason}-> ok
  end.

%% A manager exits as soon as it has removed its lock entry, so an
%% empty table means every manager of the run is gone. A non zero
%% result is reported, not raised - it is a property of the module
%% under test, and the numbers of the run are still the numbers
wait_until_idle(Scope, 0)->
  table_size(Scope);
wait_until_idle(Scope, Attempts)->
  case table_size(Scope) of
    0->
      0;
    _Pending->
      receive after 1 -> ok end,
      wait_until_idle(Scope, Attempts - 1)
  end.

table_size(Scope)->
  case ets:info(Scope, size) of
    undefined-> 0;
    Size-> Size
  end.

%%=================================================================
%%  Reporting
%%=================================================================
format_run(#{opts := Opts, runs := Runs, min := Min, max := Max, avg := Avg})->
  #{
    scenario := Scenario,
    mode := Mode,
    clients := Clients,
    requests := Requests,
    duration := Duration,
    warmup := Warmup,
    table_type := TableType,
    timeout := Timeout
  } = Opts,
  [
    io_lib:format(
      "~n~s / ~s: ~B locks/sec (avg of ~B runs)~n",
      [Scenario, Mode, Avg, length(Runs)]
    ),
    io_lib:format(
      "  clients=~B terms=~s ~s warmup=~Bms "
      "table=~s timeout=~p schedulers=~B~n",
      [
        Clients,
        terms_label(Opts),
        work_label(Requests, Duration),
        Warmup,
        TableType,
        Timeout,
        erlang:system_info(schedulers)
      ]
    ),
    [ format_single_run(Index, Run)
      || {Index, Run} <- lists:zip(lists:seq(1, length(Runs)), Runs) ],
    io_lib:format("  locks/sec: min=~B avg=~B max=~B~n", [Min, Avg, Max])
  ].

terms_label(#{scenario := unique_term})->
  "1/request";
terms_label(Opts)->
  integer_to_list(tuple_size(terms(Opts))).

work_label(undefined, Duration)->
  io_lib:format("duration=~Bms", [Duration]);
work_label(Requests, Duration)->
  io_lib:format("requests=~B/client (cap=~Bms)", [Requests, Duration]).

format_single_run(Index, #{
  locks := Locks,
  expected := Expected,
  elapsed_us := Elapsed,
  rate := Rate,
  peak_processes := PeakProcesses,
  peak_memory := PeakMemory,
  stop_reason := StopReason,
  leftover := Leftover
})->
  [
    io_lib:format(
      "  run ~B: ~B locks in ~.1f ms -> ~B locks/sec "
      "(peak_processes=~B peak_memory=~BMb)",
      [Index, Locks, Elapsed / 1000, Rate, PeakProcesses, PeakMemory div (1024*1024)]
    ),
    case StopReason of
      ok-> "";
      _-> io_lib:format(" GAVE UP: ~p", [StopReason])
    end,
    case Expected of
      undefined-> "";
      Locks-> "";
      _-> io_lib:format(" INCOMPLETE: ~B of ~B", [Locks, Expected])
    end,
    case Leftover of
      0-> "";
      _-> io_lib:format(" LEFTOVER LOCKS: ~B", [Leftover])
    end,
    "\n"
  ].

%% The matrix of the average rates - the artefact to diff between two
%% versions of elock_manager
format_matrix(Results)->
  Header = io_lib:format("~n~-14s|~12s |~12s |~12s~n", ["scenario", "shared", "exclusive", "mixed"]),
  Rule = io_lib:format("~s+~s+~s+~s~n", [
    lists:duplicate(14, $-), lists:duplicate(13, $-),
    lists:duplicate(13, $-), lists:duplicate(12, $-)
  ]),
  Rows = [
    io_lib:format("~-14s|~12B |~12B |~12B~n", [
      atom_to_list(Scenario),
      rate(Scenario, shared, Results),
      rate(Scenario, exclusive, Results),
      rate(Scenario, mixed, Results)
    ])
    || Scenario <- scenarios(), lists:keymember(Scenario, 1, Results)
  ],
  [Header, Rule, Rows, "avg locks/sec\n"].

rate(Scenario, Mode, Results)->
  case [ Avg || {S, M, #{avg := Avg}} <- Results, S =:= Scenario, M =:= Mode ] of
    [Avg|_]-> Avg;
    []-> 0
  end.

%%=================================================================
%%  Environment
%%=================================================================
env_int(Name, Default)->
  case os:getenv(Name) of
    false->
      Default;
    Value->
      try list_to_integer(string:trim(Value))
      catch _:_-> error({bad_env, Name, Value})
      end
  end.

env_atom(Name, Default)->
  case os:getenv(Name) of
    false-> Default;
    Value-> list_to_atom(string:trim(Value))
  end.
