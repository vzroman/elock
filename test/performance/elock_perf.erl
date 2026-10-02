
%%=================================================================
%%  The load of the performance comparison: one scenario run on this
%%  node through the public API of elock, so the same module measures
%%  every implementation (see elock_perf_compare.escript).
%%
%%  The clients loop "lock, unlock at once" through the warm-up and
%%  the measured window. A client holds one lock at a time, or takes
%%  the locks of a transaction in the order of the terms: no scenario
%%  has a deadlock, every request ends with {ok, Ref}.
%%
%%  Not contended, no request ever waits:
%%
%%    unique_terms     - every request locks a term of its own
%%    own_term         - a client locks its own term again and again
%%    unique_txn       - a transaction: ?TXN_SIZE terms of its own
%%                       held together
%%
%%  Heavily contended, all the clients compete for the same terms:
%%
%%    single_exclusive - one term, exclusive
%%    single_shared    - one term, shared: nobody waits, but every
%%                       request goes through the same manager
%%    single_mixed     - one term, shared or exclusive at random, 50/50
%%    hot_exclusive    - ?HOT_TERMS terms, one picked at random,
%%                       exclusive
%%    hot_txn          - a transaction: ?TXN_SIZE of the hot terms
%%                       locked in their order and held together, so
%%                       the requests wait while their clients hold
%%                       locks
%%
%%  The metrics of the measured window: the locks per second, the
%%  latency of elock:lock/4 and the peaks of the processes and of the
%%  memory of the node.
%%=================================================================
-module(elock_perf).

%%=================================================================
%%	API
%%=================================================================
-export([
  scenarios/0,
  run/1,
  main/1
]).

-define(SCOPE, elock_perf_scope).

-define(HOT_TERMS, 8).
-define(HOT(N), {hot, N}).
-define(TXN_SIZE, 3).

% The tick of the peaks of the node, ms
-define(TICK, 100).

%%-----------------------------------------------------------------
%%  The counters of a run. The first is the number of the locks, the
%%  rest is the histogram of the latency in nanoseconds: 8 buckets
%%  per power of two, so the bound of a bucket is within 12.5% of the
%%  latency. The last bucket is above an hour
%%-----------------------------------------------------------------
-define(LOCKS, 1).
-define(LATENCY, 2).
-define(BUCKETS, 320).

-type scenario() ::
  unique_terms | own_term | unique_txn |
  single_exclusive | single_shared | single_mixed | hot_exclusive | hot_txn.

-type options() :: #{
  scenario := scenario(),
  clients := pos_integer(),
  warmup := non_neg_integer(),  % ms, not measured
  duration := pos_integer()     % ms, the measured window
}.

-type result() :: #{
  scenario := scenario(),
  clients := pos_integer(),
  schedulers := pos_integer(),
  locks := non_neg_integer(),
  elapsed := pos_integer(),     % ns, the measured window
  rate := non_neg_integer(),    % locks per second
  p50 := non_neg_integer(),     % ns, the latency of elock:lock/4
  p99 := non_neg_integer(),
  p999 := non_neg_integer(),
  peak_processes := pos_integer(),
  peak_memory := pos_integer()  % bytes
}.

-record(client,{
  id :: pos_integer(),
  scenario :: scenario(),
  stop :: atomics:atomics_ref(),
  stats :: counters:counters_ref()
}).

%%=================================================================
%%	API
%%=================================================================
-spec scenarios() -> [scenario()].
scenarios()->
  [
    unique_terms,
    own_term,
    unique_txn,
    single_exclusive,
    single_shared,
    single_mixed,
    hot_exclusive,
    hot_txn
  ].

%%-----------------------------------------------------------------
%%  The entry point of the VM of a run:
%%    erl -run elock_perf main Scenario Clients Warmup Duration ResultFile
%%-----------------------------------------------------------------
-spec main([string()]) -> no_return().
main([Scenario, Clients, Warmup, Duration, ResultFile])->
  Result = run(#{
    scenario => list_to_atom(Scenario),
    clients => list_to_integer(Clients),
    warmup => list_to_integer(Warmup),
    duration => list_to_integer(Duration)
  }),
  ok = file:write_file(ResultFile, io_lib:format("~0p.~n", [Result])),
  erlang:halt(0).

%%-----------------------------------------------------------------
%%  The clients are linked: a request that fails takes the run with
%%  it. High priority: the window is cut on time whatever the load
%%-----------------------------------------------------------------
-spec run(options()) -> result().
run(#{
  scenario := Scenario,
  clients := ClientsCount,
  warmup := Warmup,
  duration := Duration
})->
  process_flag(priority, high),
  {ok, _} = application:ensure_all_started(ecall),
  start_scope(),

  Stats = counters:new(?LATENCY + ?BUCKETS, [write_concurrency]),
  Stop = atomics:new(1, []),
  [
    spawn_opt(
      fun()->
        client_init(#client{
          id = Id,
          scenario = Scenario,
          stop = Stop,
          stats = Stats
        })
      end,
      [link, monitor]
    )
    || Id <- lists:seq(1, ClientsCount)
  ],

  timer:sleep(Warmup),
  Stats0 = snapshot(Stats),
  Start = erlang:monotonic_time(nanosecond),
  {PeakProcesses, PeakMemory} = watch(Start + Duration * 1000000, {0, 0}),
  Stats1 = snapshot(Stats),
  Elapsed = erlang:monotonic_time(nanosecond) - Start,

  atomics:put(Stop, 1, 1),
  wait_clients(ClientsCount),

  [Locks | Latency] = lists:zipwith(fun(S1, S0)-> S1 - S0 end, Stats1, Stats0),
  #{
    scenario => Scenario,
    clients => ClientsCount,
    schedulers => erlang:system_info(schedulers_online),
    locks => Locks,
    elapsed => Elapsed,
    rate => Locks * 1000000000 div Elapsed,
    p50 => percentile(0.5, Latency),
    p99 => percentile(0.99, Latency),
    p999 => percentile(0.999, Latency),
    peak_processes => PeakProcesses,
    peak_memory => PeakMemory
  }.

%%=================================================================
%%  Scope
%%=================================================================
-spec start_scope() -> ok.
start_scope()->
  {ok, _} = elock:start_link(?SCOPE),
  wait_ready().

% An implementation may return from start_link/1 before the scope is
% ready to lock
-spec wait_ready() -> ok.
wait_ready()->
  case lists:member(node(), elock:ready_nodes(?SCOPE)) of
    true->
      ok;
    false->
      timer:sleep(1),
      wait_ready()
  end.

%%=================================================================
%%  Clients
%%=================================================================
% The clients are the only monitored processes
-spec wait_clients(non_neg_integer()) -> ok.
wait_clients(0)->
  ok;
wait_clients(Count)->
  receive
    {'DOWN', _MonitorRef, process, _PID, normal}->
      wait_clients(Count - 1)
  end.

-spec client_init(#client{}) -> ok.
client_init(#client{id = Id} = Client)->
  rand:seed(exsss, Id),
  client_loop(Client, 1).

-spec client_loop(#client{}, pos_integer()) -> ok.
client_loop(#client{stop = Stop} = Client, I)->
  case atomics:get(Stop, 1) of
    0->
      cycle(Client, I),
      client_loop(Client, I + 1);
    1->
      ok
  end.

-spec cycle(#client{}, pos_integer()) -> ok.
cycle(#client{scenario = unique_terms, id = Id} = Client, I)->
  elock:unlock(lock(Client, {Id, I}, false));
cycle(#client{scenario = own_term, id = Id} = Client, _I)->
  elock:unlock(lock(Client, Id, false));
cycle(#client{scenario = unique_txn, id = Id} = Client, I)->
  txn(Client, [{Id, I, N} || N <- lists:seq(1, ?TXN_SIZE)]);
cycle(#client{scenario = single_exclusive} = Client, _I)->
  elock:unlock(lock(Client, ?HOT(1), false));
cycle(#client{scenario = single_shared} = Client, _I)->
  elock:unlock(lock(Client, ?HOT(1), true));
cycle(#client{scenario = single_mixed} = Client, _I)->
  elock:unlock(lock(Client, ?HOT(1), rand:uniform(2) =:= 1));
cycle(#client{scenario = hot_exclusive} = Client, _I)->
  elock:unlock(lock(Client, ?HOT(rand:uniform(?HOT_TERMS)), false));
cycle(#client{scenario = hot_txn} = Client, _I)->
  txn(Client, hot_terms([])).

% Every client locks the terms in the same order: no deadlock
-spec txn(#client{}, [term()]) -> ok.
txn(Client, Terms)->
  Refs = [ lock(Client, Term, false) || Term <- Terms ],
  [ elock:unlock(Ref) || Ref <- Refs ],
  ok.

% ?TXN_SIZE different hot terms in their order
-spec hot_terms(ordsets:ordset(term())) -> ordsets:ordset(term()).
hot_terms(Terms) when length(Terms) =:= ?TXN_SIZE->
  Terms;
hot_terms(Terms)->
  hot_terms(ordsets:add_element(?HOT(rand:uniform(?HOT_TERMS)), Terms)).

-spec lock(#client{}, term(), boolean()) -> reference().
lock(#client{stats = Stats}, Term, IsShared)->
  Start = erlang:monotonic_time(nanosecond),
  {ok, Ref} = elock:lock(?SCOPE, Term, [node()], #{is_shared => IsShared}),
  Latency = erlang:monotonic_time(nanosecond) - Start,
  counters:add(Stats, ?LATENCY + bucket(Latency), 1),
  counters:add(Stats, ?LOCKS, 1),
  Ref.

%%=================================================================
%%  Statistics
%%=================================================================
-spec snapshot(counters:counters_ref()) -> [integer()].
snapshot(Stats)->
  [ counters:get(Stats, I) || I <- lists:seq(1, ?LATENCY + ?BUCKETS) ].

% The peaks of the node until the end of the measured window
-spec watch(integer(), {non_neg_integer(), non_neg_integer()}) ->
  {pos_integer(), pos_integer()}.
watch(Deadline, {Processes, Memory})->
  Peaks = {
    max(Processes, erlang:system_info(process_count)),
    max(Memory, erlang:memory(total))
  },
  case erlang:monotonic_time(nanosecond) < Deadline of
    true->
      timer:sleep(?TICK),
      watch(Deadline, Peaks);
    false->
      Peaks
  end.

% The latencies below 16 ns are their own buckets
-spec bucket(non_neg_integer()) -> non_neg_integer().
bucket(Latency) when Latency < 16->
  Latency;
bucket(Latency)->
  Shift = trunc(math:log2(Latency)) - 3,
  Shift * 8 + (Latency bsr Shift).

% The upper bound of the bucket, exclusive
-spec bucket_bound(non_neg_integer()) -> pos_integer().
bucket_bound(Bucket) when Bucket < 16->
  Bucket + 1;
bucket_bound(Bucket)->
  (Bucket rem 8 + 9) bsl (Bucket div 8 - 1).

% The bound of the bucket that holds the Share of the latencies
-spec percentile(float(), [non_neg_integer()]) -> non_neg_integer().
percentile(Share, Latency)->
  percentile(Share * lists:sum(Latency), Latency, 0).

-spec percentile(float(), [non_neg_integer()], non_neg_integer()) -> non_neg_integer().
percentile(Rest, [Count | _Latency], Bucket) when Rest =< Count->
  bucket_bound(Bucket);
percentile(Rest, [Count | Latency], Bucket)->
  percentile(Rest - Count, Latency, Bucket + 1).
