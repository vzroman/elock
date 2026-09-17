-module(counter_performance_SUITE).

-include_lib("common_test/include/ct.hrl").

%% Common Test API
-export([
  all/0,
  atomics_add_get_per_second/1,
  ets_update_counter_per_second/1
]).

-define(WRITER_COUNT, 10000).
-define(WRITES_PER_WRITER, 1000).

-spec all() -> [atom()].
all()->
  [
    atomics_add_get_per_second,
    ets_update_counter_per_second
  ].

-spec atomics_add_get_per_second(list()) -> ok.
atomics_add_get_per_second(_Config)->
  Arity = erlang:system_info(logical_processors),
  Atomics = atomics:new(Arity, []),
  Term = make_ref(),
  ElapsedMicroseconds = run_benchmark(
    fun()->
      atomics_writer(Atomics, Arity, Term, ?WRITES_PER_WRITER)
    end
  ),
  TotalWrites = total_writes(),
  Index = erlang:phash2(Term, Arity) + 1,
  TotalWrites = atomics:get(Atomics, Index),
  report(atomics_add_get, TotalWrites, ElapsedMicroseconds, Arity),
  ok.

-spec ets_update_counter_per_second(list()) -> ok.
ets_update_counter_per_second(_Config)->
  Table = ets:new(ets_update_counter, [
    set,
    public,
    {write_concurrency, auto}
  ]),
  Term = make_ref(),
  try
    ElapsedMicroseconds = run_benchmark(
      fun()-> ets_writer(Table, Term, ?WRITES_PER_WRITER) end
    ),
    TotalWrites = total_writes(),
    [{Term, TotalWrites}] = ets:lookup(Table, Term),
    1 = ets:info(Table, size),
    report(
      ets_update_counter,
      TotalWrites,
      ElapsedMicroseconds,
      undefined
    )
  after
    true = ets:delete(Table)
  end,
  ok.

run_benchmark(WriterFun)->
  Parent = self(),
  RunRef = make_ref(),
  Writers = [
    spawn_monitor(
      fun()-> writer(Parent, RunRef, WriterFun) end
    )
    || _ <- lists:seq(1, ?WRITER_COUNT)
  ],
  try
    {ElapsedMicroseconds, ok} = timer:tc(
      fun()->
        start_writers(Writers, RunRef),
        wait_for_writers(Writers, RunRef)
      end
    ),
    finish_writers(Writers, RunRef),
    ok = wait_for_writer_exits(Writers),
    ElapsedMicroseconds
  after
    stop_writers(Writers)
  end.

writer(Parent, RunRef, WriterFun)->
  receive
    {start_benchmark, RunRef}->
      WriterFun(),
      Parent ! {writer_done, RunRef, self()},
      receive
        {finish_benchmark, RunRef}->
          ok
      end
  end.

start_writers(Writers, RunRef)->
  lists:foreach(
    fun({Writer, _Monitor})->
      Writer ! {start_benchmark, RunRef}
    end,
    Writers
  ).

wait_for_writers([], _RunRef)->
  ok;
wait_for_writers(Writers, RunRef)->
  receive
    {writer_done, RunRef, Writer}->
      wait_for_writers(lists:keydelete(Writer, 1, Writers), RunRef);
    {'DOWN', Monitor, process, Writer, Reason}->
      {Writer, Monitor} = lists:keyfind(Writer, 1, Writers),
      error({writer_failed, Writer, Reason})
  end.

finish_writers(Writers, RunRef)->
  lists:foreach(
    fun({Writer, _Monitor})->
      Writer ! {finish_benchmark, RunRef}
    end,
    Writers
  ).

wait_for_writer_exits([])->
  ok;
wait_for_writer_exits([{Writer, Monitor}|Writers])->
  receive
    {'DOWN', Monitor, process, Writer, normal}->
      wait_for_writer_exits(Writers);
    {'DOWN', Monitor, process, Writer, Reason}->
      error({writer_failed, Writer, Reason})
  end.

stop_writers(Writers)->
  lists:foreach(
    fun({Writer, Monitor})->
      exit(Writer, kill),
      erlang:demonitor(Monitor, [flush])
    end,
    Writers
  ).

atomics_writer(_Atomics, _Arity, _Term, 0)->
  ok;
atomics_writer(Atomics, Arity, Term, WritesLeft)->
  Index = erlang:phash2(Term, Arity) + 1,
  _Counter = atomics:add_get(Atomics, Index, 1),
  atomics_writer(Atomics, Arity, Term, WritesLeft - 1).

ets_writer(_Table, _Term, 0)->
  ok;
ets_writer(Table, Term, WritesLeft)->
  _Counter = ets:update_counter(Table, Term, {2, 1}, {Term, 0}),
  ets_writer(Table, Term, WritesLeft - 1).

total_writes()->
  ?WRITER_COUNT * ?WRITES_PER_WRITER.

report(Operation, TotalWrites, ElapsedMicroseconds, Arity)->
  WritesPerSecond = round(
    TotalWrites * 1000000 / ElapsedMicroseconds
  ),
  ct:pal(
    "~p: ~B writes/sec (~B writes by ~B writers, ~B per writer, "
    "one shared make_ref term, atomics arity: ~p, "
    "elapsed: ~.3f seconds)",
    [
      Operation,
      WritesPerSecond,
      TotalWrites,
      ?WRITER_COUNT,
      ?WRITES_PER_WRITER,
      Arity,
      ElapsedMicroseconds / 1000000
    ]
  ).
