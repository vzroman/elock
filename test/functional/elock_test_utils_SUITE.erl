%% Regression tests for the shared test helpers and failure reporting.
-module(elock_test_utils_SUITE).

-include("elock_test.hrl").

%% Common Test API
-export([all/0]).

%% Test cases
-export([
  safe_result_test/1,
  safe_exception_test/1,
  finish_scope_failure_test/1
]).

all()->
  [safe_result_test, safe_exception_test, finish_scope_failure_test].

%% Evaluate once in the calling process and preserve its result.
safe_result_test(_Config)->
  Ref = make_ref(),
  ?assertEqual({ok, Ref}, ?safe(begin
    self() ! {safe_evaluated, Ref},
    {ok, Ref}
  end)),
  ?RECEIVE({safe_evaluated, Ref}),
  ?NO_MESSAGE.

%% Repeated and nested expansions preserve every exception class and stack.
safe_exception_test(_Config)->
  Reason = {failure, make_ref()},
  Stack = [{?MODULE, safe_exception_test, 1, [{line, ?LINE}]}],
  ?assertEqual(Reason, ?safe(throw(Reason))),
  ?assertEqual({'EXIT', Reason}, ?safe(exit(Reason))),
  ?assertMatch({'EXIT', {Reason, [_ | _]}}, ?safe(erlang:error(Reason))),
  ?assertEqual({'EXIT', {Reason, Stack}},
    ?safe(erlang:raise(error, Reason, Stack))),
  ?assertEqual(Reason, ?safe(?safe(throw(Reason)))),
  ?assertEqual({'EXIT', Reason}, ?safe(?safe(exit(Reason)))).

%% A leaked entry fails teardown with its original error and stack;
%% the scope is still stopped after the failure.
finish_scope_failure_test(_Config)->
  Scope = ?FUNCTION_NAME,
  Holder = elock_test_utils:start_scope(Scope),
  try
    true = ets:insert(Scope, {leaked_entry, self(), 1}),
    ?assertMatch(
      {fail, {scope_not_idle, {'EXIT', {{wait_until_timeout, _}, [_ | _]}}}},
      elock_test_utils:finish_scope(Holder)
    ),
    ?assertNot(is_process_alive(Holder)),
    ?assertEqual(undefined, ets:whereis(Scope))
  after
    elock_test_utils:stop_scope(Holder)
  end.
