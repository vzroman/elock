#!/usr/bin/env escript
%%=================================================================
%%  The performance of the working tree against a git revision of
%%  elock (a branch, a tag, a commit):
%%
%%    make perf
%%    make perf PERF_BASE=main
%%
%%  Both implementations are compiled into _build/perf. A run is a
%%  scenario of elock_perf.erl at a number of clients in a VM of its
%%  own with all the schedulers of the machine. The implementations
%%  take turns, the table shows the median run of each.
%%
%%  The dependencies are taken from _build/default/lib as they are,
%%  ./rebar3 compile comes first.
%%
%%  The environment:
%%
%%    PERF_SCENARIOS - the scenarios, all of elock_perf:scenarios/0
%%                     by default
%%    PERF_CLIENTS   - the numbers of the clients, one per scheduler
%%                     and 64 per scheduler by default
%%    PERF_RUNS      - the runs of an implementation, 3 by default
%%    PERF_WARMUP    - ms, 1000 by default
%%    PERF_DURATION  - the measured window, ms, 3000 by default
%%
%%    make perf PERF_SCENARIOS="single_exclusive hot_txn" PERF_CLIENTS="16 100000"
%%=================================================================
-mode(compile).

-define(BUILD, "_build/perf").
% Without the application itself: its two implementations are compiled here
-define(DEPS, filelib:wildcard("_build/default/lib/*/ebin") -- ["_build/default/lib/elock/ebin"]).
-define(BENCH, "test/performance/elock_perf.erl").

% As in config/vm.args
-define(PROCESS_LIMIT, "2097152").

-record(impl,{
  name :: string(),     % the column of the table
  revision :: string(), % what was compiled
  ebin :: file:filename()
}).

-record(config,{
  scenarios :: [atom()],
  clients :: [pos_integer()],
  runs :: pos_integer(),
  warmup :: non_neg_integer(),
  duration :: pos_integer()
}).

main([BaseRef])->
  ok = file:set_cwd(filename:join([filename:dirname(escript:script_name()), "..", ".."])),
  Base = build_base(BaseRef),
  Current = build_current(),
  build_bench(),
  Config = config(),
  header(Base, Current, Config),
  Results = run(Base, Current, Config),
  table(Base, Current, Config, Results);
main(_Args)->
  io:format("usage: elock_perf_compare.escript Revision~n"),
  halt(1).

%%=================================================================
%%  Build
%%=================================================================
% The sources of the revision are exported next to its ebin
build_base(Ref)->
  case exec("git", ["rev-parse", "--verify", "--quiet", "--short", Ref ++ "^{commit}"]) of
    {0, Output}->
      Commit = string:trim(Output),
      Dir = filename:join(?BUILD, "base"),
      clean(Dir),
      {0, _} = exec("sh", ["-c", "git archive " ++ Commit ++ " src include | tar -x -C " ++ Dir]),
      #impl{
        name = Ref,
        revision = Ref ++ " " ++ Commit,
        ebin = compile(Dir, filename:join(Dir, "ebin"))
      };
    _->
      io:format("unknown revision: ~s~n", [Ref]),
      halt(1)
  end.

build_current()->
  {0, Branch} = exec("git", ["rev-parse", "--abbrev-ref", "HEAD"]),
  {0, Commit} = exec("git", ["rev-parse", "--short", "HEAD"]),
  {0, Changes} = exec("git", ["status", "--porcelain", "--", "src", "include"]),
  Dir = filename:join(?BUILD, "current"),
  clean(Dir),
  #impl{
    name = string:trim(Branch),
    revision = string:trim(Branch) ++ " " ++ string:trim(Commit) ++
      case Changes of
        "" -> "";
        _ -> " and the changes of the working tree"
      end,
    ebin = compile(".", filename:join(Dir, "ebin"))
  }.

build_bench()->
  ok = filelib:ensure_path(bench_ebin()),
  {ok, _} = compile:file(?BENCH, [{outdir, bench_ebin()}, report]),
  true = code:add_patha(bench_ebin()).

bench_ebin()->
  filename:join(?BUILD, "bench").

% The beams of a module that has left the sources must not stay
clean(Dir)->
  case file:del_dir_r(Dir) of
    ok -> ok;
    {error, enoent} -> ok
  end,
  ok = filelib:ensure_path(Dir).

compile(Dir, Ebin)->
  ok = filelib:ensure_path(Ebin),
  Options = [{outdir, Ebin}, {i, filename:join(Dir, "include")}, report],
  [ {ok, _} = compile:file(File, Options) || File <- filelib:wildcard(filename:join([Dir, "src", "*.erl"])) ],
  Ebin.

%%=================================================================
%%  Config
%%=================================================================
config()->
  Schedulers = erlang:system_info(schedulers_online),
  Scenarios = [ list_to_atom(S) || S <- env("PERF_SCENARIOS", []) ],
  case Scenarios -- elock_perf:scenarios() of
    []->
      ok;
    Unknown->
      io:format("unknown scenarios: ~p, known: ~p~n", [Unknown, elock_perf:scenarios()]),
      halt(1)
  end,
  #config{
    scenarios =
      case Scenarios of
        [] -> elock_perf:scenarios();
        _ -> Scenarios
      end,
    clients = env_integers("PERF_CLIENTS", [Schedulers, 64 * Schedulers]),
    runs = hd(env_integers("PERF_RUNS", [3])),
    warmup = hd(env_integers("PERF_WARMUP", [1000])),
    duration = hd(env_integers("PERF_DURATION", [3000]))
  }.

env(Name, Default)->
  case os:getenv(Name, "") of
    "" -> Default;
    Value -> string:lexemes(Value, " ,")
  end.

env_integers(Name, Default)->
  case env(Name, []) of
    [] -> Default;
    Values -> [ list_to_integer(V) || V <- Values ]
  end.

%%=================================================================
%%  Runs
%%=================================================================
%%-----------------------------------------------------------------
%%  #{ {Scenario, Clients, ImplName} => the results of the runs }.
%%  The order of the implementations alternates from run to run
%%-----------------------------------------------------------------
run(Base, Current, #config{
  scenarios = Scenarios,
  clients = ClientsCounts,
  runs = Runs
} = Config)->
  Plan = [
    {Scenario, Clients, Impl}
    || Scenario <- Scenarios,
       Clients <- ClientsCounts,
       Run <- lists:seq(1, Runs),
       Impl <- case Run rem 2 of 1 -> [Base, Current]; 0 -> [Current, Base] end
  ],
  {Results, _N} =
    lists:foldl(
      fun({Scenario, Clients, #impl{name = Name} = Impl}, {Acc, N})->
        Result = run_vm(Impl, Scenario, Clients, Config),
        progress(N, length(Plan), Name, Result),
        Key = {Scenario, Clients, Name},
        {Acc#{ Key => [Result | maps:get(Key, Acc, [])] }, N + 1}
      end,
      {#{}, 1},
      Plan
    ),
  Results.

run_vm(#impl{ebin = Ebin}, Scenario, Clients, #config{warmup = Warmup, duration = Duration})->
  ResultFile = filename:join(?BUILD, "result.term"),
  PathArgs = lists:append([ ["-pa", Path] || Path <- [Ebin, bench_ebin() | ?DEPS] ]),
  {0, Output} = exec("erl", [
    "-noshell",
    "+P", ?PROCESS_LIMIT
    | PathArgs
  ] ++ [
    "-run", "elock_perf", "main",
    atom_to_list(Scenario),
    integer_to_list(Clients),
    integer_to_list(Warmup),
    integer_to_list(Duration),
    ResultFile
  ]),
  % The log of the node, if any
  io:put_chars(Output),
  {ok, [Result]} = file:consult(ResultFile),
  Result.

exec(Executable, Args)->
  Port = open_port({spawn_executable, os:find_executable(Executable)}, [
    {args, Args},
    exit_status,
    stderr_to_stdout
  ]),
  exec_output(Port, []).

exec_output(Port, Output)->
  receive
    {Port, {data, Data}}->
      exec_output(Port, [Output | Data]);
    {Port, {exit_status, Status}}->
      {Status, lists:flatten(Output)}
  end.

%%=================================================================
%%  Report
%%=================================================================
header(
    #impl{name = BaseName, revision = BaseRevision},
    #impl{name = CurrentName, revision = CurrentRevision},
    #config{
      runs = Runs,
      warmup = Warmup,
      duration = Duration
    }
)->
  io:format(
    "elock performance: ~s against ~s~n"
    "  ~s: ~s~n"
    "  ~s: ~s~n"
    "  OTP ~s, ~p schedulers, ~p runs of ~p ms after ~p ms of warm-up~n~n",
    [
      CurrentName, BaseName,
      BaseName, BaseRevision,
      CurrentName, CurrentRevision,
      erlang:system_info(otp_release), erlang:system_info(schedulers_online),
      Runs, Duration, Warmup
    ]
  ).

progress(N, Total, Name, #{
  scenario := Scenario,
  clients := Clients,
  rate := Rate,
  p50 := P50,
  p99 := P99
})->
  io:format(
    "[~3w/~w] ~-16s ~7w clients  ~-16s ~9w locks/s  p50 ~s us  p99 ~s us~n",
    [N, Total, Scenario, Clients, Name, Rate, us(P50), us(P99)]
  ).

%%-----------------------------------------------------------------
%%  A line per scenario and number of clients: the median run of the
%%  base, the median run of the working tree and the ratio of their
%%  rates
%%-----------------------------------------------------------------
table(
    #impl{name = BaseName},
    #impl{name = CurrentName},
    #config{
      scenarios = Scenarios,
      clients = ClientsCounts
    },
    Results
)->
  Columns = "~10s ~8s ~8s ~9s ~8s ~6s",
  Titles = ["locks/s", "p50,us", "p99,us", "p99.9,us", "procs", "MB"],
  Width = lists:flatlength(io_lib:format(Columns, Titles)),
  io:format(
    "~n~-16s ~7s | ~-*s | ~-*s | ~s~n",
    ["", "", Width, BaseName, Width, CurrentName, CurrentName ++ " /"]
  ),
  io:format(
    "~-16s ~7s | " ++ Columns ++ " | " ++ Columns ++ " | ~s~n",
    ["scenario", "clients"] ++ Titles ++ Titles ++ [BaseName]
  ),
  [
    begin
      #{rate := BaseRate} = BaseResult = median(maps:get({Scenario, Clients, BaseName}, Results)),
      #{rate := CurrentRate} = CurrentResult = median(maps:get({Scenario, Clients, CurrentName}, Results)),
      io:format(
        "~-16s ~7w | " ++ Columns ++ " | " ++ Columns ++ " | x~.2f~n",
        [Scenario, Clients] ++ cells(BaseResult) ++ cells(CurrentResult) ++ [CurrentRate / BaseRate]
      )
    end
    || Scenario <- Scenarios, Clients <- ClientsCounts
  ],
  io:format(
    "~nlocks/s: the locks taken and released per second. p50, p99, p99.9: the latency of~n"
    "elock:lock/4. procs, MB: the peaks of the processes and of the memory of the node.~n"
  ).

% The run with the median rate
median(Results)->
  Sorted = lists:sort(fun(#{rate := A}, #{rate := B})-> A =< B end, Results),
  lists:nth((length(Sorted) + 1) div 2, Sorted).

cells(#{
  rate := Rate,
  p50 := P50,
  p99 := P99,
  p999 := P999,
  peak_processes := Processes,
  peak_memory := Memory
})->
  [
    integer_to_list(Rate),
    us(P50),
    us(P99),
    us(P999),
    integer_to_list(Processes),
    integer_to_list(Memory div (1024 * 1024))
  ].

% Nanoseconds as microseconds
us(Time) when Time < 100000->
  float_to_list(Time / 1000, [{decimals, 1}]);
us(Time)->
  integer_to_list(Time div 1000).
