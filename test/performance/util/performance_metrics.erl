%%=================================================================
%%  The metrics of the performance points. Ported from
%%  ecall/test/performance/util/performance_metrics.erl.
%%
%%  The point runner of every node starts a collector on its node
%%  when the point starts (start/1) and takes its result when the
%%  clients of the node are done (finish/1). The collector is linked
%%  to the runner and goes with it. It takes the scheduler wall time
%%  and the octets sent over the distribution to the other
%%  participating nodes at the start and at the end of the window,
%%  and samples the memory and the run queue every
%%  ?SAMPLE_INTERVAL_MS ms, at the start and at the end. The memory
%%  is reported at the start of the window next to its maximum: a
%%  node boots with about 2 GB, mostly the process table of +P.
%%
%%  The module owns the files the performance report reads, all in
%%  the priv_dir of the suite. point/2 logs the result of a point
%%  and writes it as JSON to performance_data: every .json there is
%%  a point for the report. running/2 writes the marker of the point
%%  that runs now, performance_running.json next to
%%  performance_data, stopped/1 deletes it. The marker carries the
%%  OS pid of the ct node: a marker left by a killed run names a
%%  dead process and the report takes it for not running
%%=================================================================
-module(performance_metrics).

-include_lib("common_test/include/ct.hrl").

%% API
-export([
  start/1,
  finish/1,
  point/2,
  running/2,
  stopped/1
]).

-define(TAG, ?MODULE).
-define(SAMPLE_INTERVAL_MS, 100).

-record(state, {
  dist_ports,
  send_octets,
  schedulers,
  scheduler_times,
  memory_start,
  memory_max = 0,
  run_queue_max = 0
}).

%%=================================================================
%%  API
%%=================================================================
%%-----------------------------------------------------------------
%%  Open the window: a collector linked to the caller, it has taken
%%  the base counters when the call returns. Nodes are the other
%%  participating nodes
%%-----------------------------------------------------------------
start(Nodes)->
  Owner = self(),
  Collector = spawn_link(fun()-> collector_init(Owner, Nodes) end),
  receive
    {?TAG, Collector, started}->
      Collector
  end.

%%-----------------------------------------------------------------
%%  Close the window: the metrics of the node, the collector exits
%%-----------------------------------------------------------------
finish(Collector)->
  Collector ! {?TAG, finish},
  receive
    {?TAG, Collector, Result}->
      Result
  end.

%%-----------------------------------------------------------------
%%  Log the result of a point and write it as JSON. The file is
%%  named after the point in the nesting order of the suite:
%%  <clients_per_node>.<locks>.<intersect>.<exclusive>.<path>.json
%%-----------------------------------------------------------------
point(Config, Result)->
  ct:pal("Transaction performance point completed: ~p", [Result]),
  File = filename:join([?config(priv_dir, Config), "performance_data", file_name(Result)]),
  ok = filelib:ensure_dir(File),
  ok = file:write_file(File, json:encode(Result)).

file_name(#{
  clients_per_node := Clients,
  locks_per_transaction := Locks,
  intersect_percent := Intersect,
  exclusive_percent := Exclusive,
  path := Path
})->
  lists:flatten(io_lib:format("~B.~B.~B.~B.~s.json", [Clients, Locks, Intersect, Exclusive, Path])).

%%-----------------------------------------------------------------
%%  The marker of the point that runs now: its inputs, index and
%%  total, when it has started and the OS pid of the ct node. It is
%%  written under a temporary name and renamed, so the report never
%%  reads a half of it
%%-----------------------------------------------------------------
running(Config, Point)->
  File = running_file(Config),
  Temporary = File ++ ".tmp",
  ok = file:write_file(Temporary, json:encode(Point#{
    started_at => erlang:system_time(millisecond),
    os_pid => list_to_integer(os:getpid())
  })),
  ok = file:rename(Temporary, File).

%%-----------------------------------------------------------------
%%  The test case is over: no point runs
%%-----------------------------------------------------------------
stopped(Config)->
  case file:delete(running_file(Config)) of
    ok->
      ok;
    {error, enoent}->
      % The case has run no point: its paths skip every point of the config
      ok
  end.

running_file(Config)->
  filename:join(?config(priv_dir, Config), "performance_running.json").

%%=================================================================
%%  The collector
%%=================================================================
%%-----------------------------------------------------------------
%%  The scheduler wall time is measured while the process that has
%%  turned it on is alive: the collector turns it on for the window
%%-----------------------------------------------------------------
collector_init(Owner, Nodes)->
  _ = erlang:system_flag(scheduler_wall_time, true),
  DistPorts = [ Port || {Node, Port} <- erlang:system_info(dist_ctrl), lists:member(Node, Nodes) ],
  #state{memory_max = MemoryStart} = State = sample(#state{
    dist_ports = DistPorts,
    send_octets = send_octets(DistPorts),
    schedulers = erlang:system_info(schedulers_online),
    scheduler_times = scheduler_times()
  }),
  Owner ! {?TAG, self(), started},
  schedule_sample(),
  collector_loop(Owner, State#state{memory_start = MemoryStart}).

collector_loop(Owner, State)->
  receive
    {?TAG, sample}->
      schedule_sample(),
      collector_loop(Owner, sample(State));
    {?TAG, finish}->
      Owner ! {?TAG, self(), result(sample(State))}
  end.

schedule_sample()->
  erlang:send_after(?SAMPLE_INTERVAL_MS, self(), {?TAG, sample}).

sample(#state{memory_max = MemoryMax, run_queue_max = RunQueueMax} = State)->
  State#state{
    memory_max = max(MemoryMax, erlang:memory(total)),
    run_queue_max = max(RunQueueMax, erlang:statistics(total_run_queue_lengths))
  }.

result(#state{
  dist_ports = DistPorts,
  send_octets = SendOctets,
  schedulers = Schedulers,
  scheduler_times = SchedulerTimes,
  memory_start = MemoryStart,
  memory_max = MemoryMax,
  run_queue_max = RunQueueMax
})->
  #{
    memory => #{
      start_bytes => MemoryStart,
      maximum_bytes => MemoryMax
    },
    network => #{
      send_octets => send_octets(DistPorts) - SendOctets
    },
    schedulers => #{
      utilization_percent => utilization(Schedulers, SchedulerTimes),
      maximum_run_queue_length => RunQueueMax
    }
  }.

%%=================================================================
%%  Counters
%%=================================================================
send_octets(DistPorts)->
  lists:foldl(
    fun(Port, Sum)->
      {ok, [{send_oct, Octets}]} = inet:getstat(Port, [send_oct]),
      Sum + Octets
    end,
    0,
    DistPorts
  ).

scheduler_times()->
  maps:from_list([ {Id, {Active, Total}} || {Id, Active, Total} <- erlang:statistics(scheduler_wall_time) ]).

%%-----------------------------------------------------------------
%%  The active share of the wall time of the normal schedulers
%%  over the window. The list of statistics(scheduler_wall_time)
%%  is unsorted and holds the dirty schedulers as well
%%-----------------------------------------------------------------
utilization(Schedulers, Base)->
  Final = scheduler_times(),
  {Active, Total} = lists:foldl(
    fun(Id, {ActiveSum, TotalSum})->
      {BaseActive, BaseTotal} = maps:get(Id, Base),
      {FinalActive, FinalTotal} = maps:get(Id, Final),
      {ActiveSum + FinalActive - BaseActive, TotalSum + FinalTotal - BaseTotal}
    end,
    {0, 0},
    lists:seq(1, Schedulers)
  ),
  Active * 100 / Total.
