%%=================================================================
%%  elock_manager throughput.
%%
%%  The measurement itself lives in elock_manager_perf - this suite
%%  only spreads its scenario x mode matrix over common_test groups
%%  so that a single combination can be run on its own while working
%%  on docs/optimization.md:
%%
%%    rebar3 ct --suite test/elock_manager_perf_SUITE --group single_term
%%    rebar3 ct --suite test/elock_manager_perf_SUITE --group single_term --case exclusive
%%
%%  The smoke case runs the whole matrix with tiny parameters and
%%  asserts that the harness works at all - it is not a measurement:
%%
%%    rebar3 ct --suite test/elock_manager_perf_SUITE --case smoke
%%
%%  The size of a run comes from elock_manager_perf:defaults/0, which
%%  reads the ELOCK_PERF_* environment variables. The same keys can
%%  be given in a common_test config file:
%%
%%    {clients, 200}. {terms, 10}. {duration, 5000}.
%%    {warmup, 1000}. {runs, 3}. {table_type, ordered_set}.
%%=================================================================
-module(elock_manager_perf_SUITE).

-include_lib("common_test/include/ct.hrl").

%% Common Test API
-export([
  all/0,
  groups/0,
  suite/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_group/2,
  end_per_group/2
]).

%% Test cases
-export([
  smoke/1,
  shared/1,
  exclusive/1,
  mixed/1
]).

%% The keys elock_manager_perf:run/1 accepts from ct:get_config/1
-define(CONFIG_KEYS, [
  clients,
  terms,
  requests,
  duration,
  warmup,
  runs,
  table_type,
  timeout,
  scope,
  process_limit
]).

%% Everything short enough to be a sanity check, not a measurement
-define(SMOKE_OPTS, #{
  clients => 8,
  terms => 4,
  requests => undefined,
  duration => 200,
  warmup => 0,
  runs => 1
}).

suite()->
  [{timetrap, {hours, 1}}].

all()->
  [
    smoke,
    {group, unique_term},
    {group, fixed_terms},
    {group, single_term}
  ].

%% One group per scenario, one case per mode
groups()->
  [
    {Scenario, [sequence], elock_manager_perf:modes()}
    || Scenario <- elock_manager_perf:scenarios()
  ].

init_per_suite(Config)->
  [{perf_opts, config_opts()}|Config].

end_per_suite(_Config)->
  ok.

init_per_group(Scenario, Config)->
  [{scenario, Scenario}|Config].

end_per_group(_Scenario, _Config)->
  ok.

%%=================================================================
%%  The matrix
%%=================================================================
shared(Config)->
  measure(shared, Config).

exclusive(Config)->
  measure(exclusive, Config).

mixed(Config)->
  measure(mixed, Config).

measure(Mode, Config)->
  Scenario = ?config(scenario, Config),
  Opts = maps:merge(?config(perf_opts, Config), #{
    scenario => Scenario,
    mode => Mode
  }),
  Result = elock_manager_perf:run(Opts),
  ct:pal("~s", [elock_manager_perf:format_run(Result)]),
  #{avg := Avg} = Result,
  {comment, lists:flatten(io_lib:format("~B locks/sec", [Avg]))}.

%%=================================================================
%%  Smoke
%%=================================================================
smoke(Config)->
  Opts = maps:merge(?config(perf_opts, Config), ?SMOKE_OPTS),
  Results = elock_manager_perf:run_all(Opts),
  ct:pal("~s~s", [
    [ elock_manager_perf:format_run(Result) || {_S, _M, Result} <- Results ],
    elock_manager_perf:format_matrix(Results)
  ]),
  9 = length(Results),
  lists:foreach(fun({Scenario, Mode, #{min := Min, runs := Runs}})->
    case Min > 0 of
      true-> ok;
      false-> ct:fail({no_locks, Scenario, Mode})
    end,
    lists:foreach(fun(#{stop_reason := StopReason, leftover := Leftover})->
      case StopReason of
        ok-> ok;
        _-> ct:fail({gave_up, Scenario, Mode, StopReason})
      end,
      case Leftover of
        0-> ok;
        _-> ct:fail({leftover_locks, Scenario, Mode, Leftover})
      end
    end, Runs)
  end, Results),
  ok.

%%=================================================================
%%  Utilities
%%=================================================================
config_opts()->
  lists:foldl(fun(Key, Acc)->
    case ct:get_config(Key) of
      undefined-> Acc;
      Value-> Acc#{Key => Value}
    end
  end, #{}, ?CONFIG_KEYS).
