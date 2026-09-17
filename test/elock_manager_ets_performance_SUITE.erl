-module(elock_manager_ets_performance_SUITE).

-include("elock.hrl").
-include_lib("common_test/include/ct.hrl").

%% Common Test API
-export([
  all/0,
  writes_per_second/1,
  writes_per_second_without_locking/1,
  writes_per_second_with_batched_ticker/1
]).

-define(WRITER_COUNT, 10000).
-define(WRITES_PER_WRITER, 10000).
-define(ROTATION_INTERVAL_MILLISECONDS, 10).
-define(BATCH_SIZE, 1000).
-define(SCOPE, elock_manager_ets_performance_scope).

-record(batched_ticker,{
  parent,
  parent_monitor,
  run_ref,
  meta_ets,
  actual_ets,
  total_writes,
  writes = 0,
  rotations = 0,
  next_rotation
}).

-spec all() -> [atom()].
all()->
  [
    writes_per_second,
    writes_per_second_without_locking,
    writes_per_second_with_batched_ticker
  ].

-spec writes_per_second(list()) -> ok.
writes_per_second(_Config)->
  Scope = ets:new(?SCOPE, [
    named_table,
    public,
    ordered_set,
    {write_concurrency, auto}
  ]),
  try
    run_and_report(with_locking, Scope)
  after
    ok = wait_until_released(Scope),
    true = ets:delete(Scope)
  end.

-spec writes_per_second_without_locking(list()) -> ok.
writes_per_second_without_locking(_Config)->
  run_and_report(without_locking, undefined).

-spec writes_per_second_with_batched_ticker(list()) -> ok.
writes_per_second_with_batched_ticker(_Config)->
  TotalWrites = ?WRITER_COUNT * ?WRITES_PER_WRITER,
  {ElapsedMicroseconds, Rotations} = run_batched_benchmark(TotalWrites),
  WritesPerSecond = round(
    TotalWrites * 1000000 / ElapsedMicroseconds
  ),
  ct:pal(
    "writes/sec with batched ticker (~B writes by ~B writers, "
    "batch size ~B, ActualEts rotated every ~B ms): ~B "
    "(~B rotations)",
    [
      TotalWrites,
      ?WRITER_COUNT,
      ?BATCH_SIZE,
      ?ROTATION_INTERVAL_MILLISECONDS,
      WritesPerSecond,
      Rotations
    ]
  ),
  ok.

run_and_report(Locking, Scope)->
  TotalWrites = ?WRITER_COUNT * ?WRITES_PER_WRITER,
  {ElapsedMicroseconds, Rotations} = run_benchmark(Locking, Scope),
  WritesPerSecond = round(
    TotalWrites * 1000000 / ElapsedMicroseconds
  ),
  ct:pal(
    "writes/sec ~s (~B writes by ~B writers, ActualEts rotated "
    "every ~B ms): ~B (~B rotations)",
    [
      locking_description(Locking),
      TotalWrites,
      ?WRITER_COUNT,
      ?ROTATION_INTERVAL_MILLISECONDS,
      WritesPerSecond,
      Rotations
    ]
  ),
  ok.

locking_description(with_locking)->
  "with locking";
locking_description(without_locking)->
  "without locking".

run_batched_benchmark(TotalWrites)->
  RunRef = make_ref(),
  {Ticker, TickerMonitor} = start_batched_ticker(
    RunRef,
    TotalWrites
  ),
  Writers = [
    spawn_monitor(
      fun()-> batched_writer(RunRef, Ticker) end
    )
    || _ <- lists:seq(1, ?WRITER_COUNT)
  ],
  try
    {ElapsedMicroseconds, PendingWriters} = timer:tc(
      fun()->
        Ticker ! {start_benchmark, RunRef},
        start_writers(Writers, RunRef),
        wait_for_batched_writes(
          Writers,
          Ticker,
          TickerMonitor,
          RunRef
        )
      end
    ),
    ok = wait_for_writers(PendingWriters),
    Ticker ! {stop_ticker, RunRef, self()},
    Rotations = wait_for_ticker_report(Ticker, RunRef),
    {ElapsedMicroseconds, Rotations}
  after
    ok = stop_ticker(Ticker, TickerMonitor, RunRef)
  end.

start_batched_ticker(RunRef, TotalWrites)->
  Parent = self(),
  {Ticker, TickerMonitor} = spawn_opt(
    fun()-> batched_ticker(Parent, RunRef, TotalWrites) end,
    [
      monitor,
      {priority, high},
      {message_queue_data, off_heap}
    ]
  ),
  receive
    {ticker_ready, RunRef, Ticker}->
      {Ticker, TickerMonitor};
    {'DOWN', TickerMonitor, process, Ticker, Reason}->
      error({ticker_failed, Ticker, Reason})
  end.

batched_ticker(Parent, RunRef, TotalWrites)->
  ParentMonitor = erlang:monitor(process, Parent),
  MetaEts = ets:new(meta_ets, [
    set,
    protected,
    {read_concurrency, true}
  ]),
  ActualEts = new_actual_ets(),
  true = ets:insert(MetaEts, {actual_ets, ActualEts}),
  Parent ! {ticker_ready, RunRef, self()},
  receive
    {start_benchmark, RunRef}->
      NextRotation = next_rotation(),
      batched_ticker_loop(#batched_ticker{
        parent = Parent,
        parent_monitor = ParentMonitor,
        run_ref = RunRef,
        meta_ets = MetaEts,
        actual_ets = ActualEts,
        total_writes = TotalWrites,
        next_rotation = NextRotation
      });
    {'DOWN', ParentMonitor, process, Parent, _Reason}->
      ok;
    Unexpected->
      error({unexpected_message, Unexpected})
  end.

batched_ticker_loop(#batched_ticker{
  parent = Parent,
  parent_monitor = ParentMonitor,
  run_ref = RunRef,
  next_rotation = NextRotation
} = State)->
  RotationTimeout = max(
    0,
    NextRotation - erlang:monotonic_time(millisecond)
  ),
  receive
    {do, {write, Record}}->
      Batch = [
        Record
      |collect_write_requests(_Count = 1, ?BATCH_SIZE)
      ],
      batched_ticker_loop(maybe_rotate(write_batch(Batch, State)));
    {stop_ticker, RunRef, Parent}->
      Parent ! {
        ticker_stopped,
        RunRef,
        self(),
        State#batched_ticker.rotations
      },
      ok;
    {'DOWN', ParentMonitor, process, Parent, _Reason}->
      ok;
    Unexpected->
      error({unexpected_message, Unexpected})
  after RotationTimeout ->
    batched_ticker_loop(rotate_batched_actual_ets(State))
  end.

collect_write_requests(Count, BatchSize)
  when 0 < Count, Count < BatchSize->
  receive
    {do, {write, Record}}->
      [Record|collect_write_requests(Count + 1, BatchSize)]
  after 0 ->
    []
  end;
collect_write_requests(_Count, _BatchSize)->
  [].

write_batch(Batch, #batched_ticker{
  parent = Parent,
  run_ref = RunRef,
  actual_ets = ActualEts,
  total_writes = TotalWrites,
  writes = Writes0
} = State)->
  true = ets:insert(ActualEts, Batch),
  Writes = Writes0 + length(Batch),
  if
    Writes =:= TotalWrites->
      Parent ! {batched_writes_complete, RunRef, self()};
    Writes < TotalWrites->
      ok
  end,
  State#batched_ticker{
    writes = Writes
  }.

maybe_rotate(#batched_ticker{
  next_rotation = NextRotation
} = State)->
  case erlang:monotonic_time(millisecond) >= NextRotation of
    true->
      rotate_batched_actual_ets(State);
    false->
      State
  end.

rotate_batched_actual_ets(#batched_ticker{
  meta_ets = MetaEts,
  actual_ets = PreviousActualEts,
  rotations = Rotations
} = State)->
  ActualEts = new_actual_ets(),
  true = ets:insert(MetaEts, {actual_ets, ActualEts}),
  true = ets:delete(PreviousActualEts),
  State#batched_ticker{
    actual_ets = ActualEts,
    rotations = Rotations + 1,
    next_rotation = next_rotation()
  }.

next_rotation()->
  erlang:monotonic_time(millisecond)
  + ?ROTATION_INTERVAL_MILLISECONDS.

batched_writer(RunRef, Ticker)->
  receive
    {start_benchmark, RunRef}->
      send_write_requests(Ticker, ?WRITES_PER_WRITER);
    Unexpected->
      error({unexpected_message, Unexpected})
  end.

send_write_requests(_Ticker, 0)->
  ok;
send_write_requests(Ticker, Remaining)->
  Ticker ! {do, {write, {make_ref(), self(), Remaining}}},
  send_write_requests(Ticker, Remaining - 1).

wait_for_batched_writes(
    Writers,
    Ticker,
    TickerMonitor,
    RunRef
)->
  receive
    {batched_writes_complete, RunRef, Ticker}->
      Writers;
    {'DOWN', TickerMonitor, process, Ticker, Reason}->
      error({ticker_failed, Ticker, Reason});
    {'DOWN', MonitorRef, process, Writer, normal}->
      PendingWriters = lists:delete(
        {Writer, MonitorRef},
        Writers
      ),
      wait_for_batched_writes(
        PendingWriters,
        Ticker,
        TickerMonitor,
        RunRef
      );
    {'DOWN', _MonitorRef, process, Writer, Reason}->
      error({writer_failed, Writer, Reason})
  end.

run_benchmark(Locking, Scope)->
  RunRef = make_ref(),
  {Ticker, TickerMonitor, MetaEts} = start_ticker(
    Locking,
    Scope,
    RunRef
  ),
  Writers = [
    spawn_monitor(
      fun()-> writer(RunRef, Locking, Scope, MetaEts) end
    )
    || _ <- lists:seq(1, ?WRITER_COUNT)
  ],
  try
    {ElapsedMicroseconds, ok} = timer:tc(
      fun()->
        Ticker ! {start_benchmark, RunRef},
        start_writers(Writers, RunRef),
        wait_for_writers(Writers)
      end
    ),
    Ticker ! {stop_ticker, RunRef, self()},
    Rotations = wait_for_ticker_report(Ticker, RunRef),
    {ElapsedMicroseconds, Rotations}
  after
    ok = stop_ticker(Ticker, TickerMonitor, RunRef)
  end.

start_ticker(Locking, Scope, RunRef)->
  Parent = self(),
  {Ticker, TickerMonitor} = spawn_monitor(
    fun()-> ticker(Parent, RunRef, Locking, Scope) end
  ),
  receive
    {ticker_ready, RunRef, Ticker, MetaEts}->
      {Ticker, TickerMonitor, MetaEts};
    {'DOWN', TickerMonitor, process, Ticker, Reason}->
      error({ticker_failed, Ticker, Reason})
  end.

ticker(Parent, RunRef, Locking, Scope)->
  ParentMonitor = erlang:monitor(process, Parent),
  MetaEts = ets:new(meta_ets, [
    set,
    protected,
    {read_concurrency, true}
  ]),
  ActualEts = new_actual_ets(),
  true = ets:insert(MetaEts, {actual_ets, ActualEts}),
  Parent ! {ticker_ready, RunRef, self(), MetaEts},
  receive
    {start_benchmark, RunRef}->
      ticker_loop(
        Parent,
        ParentMonitor,
        RunRef,
        Locking,
        Scope,
        MetaEts,
        ActualEts,
        0
      );
    {'DOWN', ParentMonitor, process, Parent, _Reason}->
      ok;
    Unexpected->
      error({unexpected_message, Unexpected})
  end.

ticker_loop(
    Parent,
    ParentMonitor,
    RunRef,
    Locking,
    Scope,
    MetaEts,
    ActualEts,
    Rotations
)->
  receive
    {stop_ticker, RunRef, Parent}->
      Parent ! {ticker_stopped, RunRef, self(), Rotations},
      ok;
    {'DOWN', ParentMonitor, process, Parent, _Reason}->
      ok;
    Unexpected->
      error({unexpected_message, Unexpected})
  after ?ROTATION_INTERVAL_MILLISECONDS ->
    NextActualEts = rotate_actual_ets(
      Locking,
      Scope,
      MetaEts,
      ActualEts
    ),
    ticker_loop(
      Parent,
      ParentMonitor,
      RunRef,
      Locking,
      Scope,
      MetaEts,
      NextActualEts,
      Rotations + 1
    )
  end.

rotate_actual_ets(
    with_locking,
    Scope,
    MetaEts,
    PreviousActualEts
)->
  {ok, Unlock} = lock(Scope, MetaEts, false),
  ActualEts = new_actual_ets(),
  true = ets:insert(MetaEts, {actual_ets, ActualEts}),
  ok = elock_manager:unlock(Unlock),
  true = ets:delete(PreviousActualEts),
  ActualEts;

rotate_actual_ets(
    without_locking,
    _Scope,
    MetaEts,
    PreviousActualEts
)->
  ActualEts = new_actual_ets(),
  true = ets:insert(MetaEts, {actual_ets, ActualEts}),
  true = ets:delete(PreviousActualEts),
  ActualEts.

new_actual_ets()->
  ets:new(actual_ets, [
    set,
    public,
    {write_concurrency, auto}
  ]).

writer(RunRef, Locking, Scope, MetaEts)->
  receive
    {start_benchmark, RunRef}->
      write_records(
        Locking,
        Scope,
        MetaEts,
        ?WRITES_PER_WRITER
      );
    Unexpected->
      error({unexpected_message, Unexpected})
  end.

write_records(_Locking, _Scope, _MetaEts, 0)->
  ok;
write_records(with_locking, Scope, MetaEts, Remaining)->
  {ok, Unlock} = lock(Scope, MetaEts, true),
  [{actual_ets, ActualEts}] = ets:lookup(MetaEts, actual_ets),
  true = ets:insert(ActualEts, {make_ref(), self(), Remaining}),
  ok = elock_manager:unlock(Unlock),
  write_records(with_locking, Scope, MetaEts, Remaining - 1);
write_records(without_locking, Scope, MetaEts, Remaining)->
  ok = write_record_without_locking(MetaEts, Remaining),
  write_records(without_locking, Scope, MetaEts, Remaining - 1).

write_record_without_locking(MetaEts, Remaining)->
  [{actual_ets, ActualEts}] = ets:lookup(MetaEts, actual_ets),
  try ets:insert(ActualEts, {make_ref(), self(), Remaining}) of
    true->
      ok
  catch
    error:badarg->
      write_record_without_locking(MetaEts, Remaining)
  end.

lock(Scope, Term, Shared)->
  Ref = make_ref(),
  elock_manager:lock(#request{
    ref = Ref,
    scope = Scope,
    term = Term,
    client = self(),
    reply_to = self(),
    shared = Shared,
    held = [],
    nodes = [],
    timeout = infinity
  }).

start_writers(Writers, RunRef)->
  lists:foreach(
    fun({Writer, _MonitorRef})->
      Writer ! {start_benchmark, RunRef}
    end,
    Writers
  ).

wait_for_writers([])->
  ok;
wait_for_writers([{Writer, MonitorRef}|Rest])->
  receive
    {'DOWN', MonitorRef, process, Writer, normal}->
      wait_for_writers(Rest);
    {'DOWN', MonitorRef, process, Writer, Reason}->
      error({writer_failed, Writer, Reason})
  end.

wait_for_ticker_report(Ticker, RunRef)->
  receive
    {ticker_stopped, RunRef, Ticker, Rotations}->
      Rotations
  end.

stop_ticker(Ticker, TickerMonitor, RunRef)->
  Ticker ! {stop_ticker, RunRef, self()},
  receive
    {'DOWN', TickerMonitor, process, Ticker, normal}->
      ok;
    {'DOWN', TickerMonitor, process, Ticker, Reason}->
      error({ticker_failed, Ticker, Reason})
  end.

wait_until_released(Scope)->
  wait_until_released(Scope, 10000).

wait_until_released(Scope, 0)->
  0 = ets:info(Scope, size),
  ok;
wait_until_released(Scope, Attempts)->
  case ets:info(Scope, size) of
    0->
      ok;
    _PendingLocks->
      receive after 1 -> ok end,
      wait_until_released(Scope, Attempts - 1)
  end.
