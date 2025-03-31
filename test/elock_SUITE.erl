
-module(elock_SUITE).

-include("elock.hrl").
-include_lib("common_test/include/ct.hrl").

%% API
-export([
  all/0,
  groups/0,
  init_per_testcase/2,
  end_per_testcase/2,
  init_per_group/2,
  end_per_group/2,
  init_per_suite/1,
  end_per_suite/1
]).


-export([
  test_simple_lock/1
  ,test_concurrent_lock/1
  ,test_lock_timeout/1
]).

all()->
  [
    test_simple_lock
    ,test_concurrent_lock
    ,test_lock_timeout
  ].

groups()->
  [].

init_per_suite(Config)->
  Scope = test_scope,
  ScopePID = spawn(fun()->
    {ok, _PID} = elock:start_link( Scope ),
    timer:sleep( infinity )
  end),
  [
    {scope, Scope},
    {scope_pid, ScopePID}
  |Config].
end_per_suite( Config )->
  ScopePID = ?config(scope_pid, Config),
  exit(ScopePID, shutdown),
  ok.

init_per_group(_,Config)->
  Config.

end_per_group(_,_Config)->
  ok.

init_per_testcase(_,Config)->
  Config.

end_per_testcase(_,_Config)->
  ok.

%%========================================================================
%%    Simple lock without concurrency
%%========================================================================
test_simple_lock( Config )->

  Scope=?config(scope, Config),
  {ok, U1} = elock:lock(Scope, term1, _IsShared = false, _Timeout = infinity ),
  ?LOGDEBUG("locks: ~p",[ets:tab2list( Scope )]),

  U1(),
  timer:sleep(1000),
  ?LOGDEBUG("locks: ~p",[ets:tab2list( Scope )]),

  ok.

%%========================================================================
%%    Simple lock with concurrency
%%========================================================================
test_concurrent_lock( Config )->

  Scope=?config(scope, Config),

  spawn(fun()->
    {ok, U1} = elock:lock(Scope, term1, false, infinity ),
    ?LOGDEBUG("locks: ~p",[ets:tab2list( Scope )]),
    timer:sleep(5000),
    U1()
  end),

  timer:sleep(1000),
  {ok, U2} = elock:lock(Scope, term1, _IsShared = false, _Timeout = infinity ),
  ?LOGDEBUG("locks: ~p",[ets:tab2list( Scope )]),

  timer:sleep(1000),
  U2(),
  timer:sleep(1000),
  ?LOGDEBUG("locks: ~p",[ets:tab2list( Scope )]),

  ok.

%%========================================================================
%%    lock timeout
%%========================================================================
test_lock_timeout( Config )->

  Scope=?config(scope, Config),

  spawn(fun()->
    {ok, U1} = elock:lock(Scope, term1, false, infinity ),
    ?LOGDEBUG("locks: ~p",[ets:tab2list( Scope )]),
    timer:sleep(10000),
    U1()
  end),

  timer:sleep(1000),
  {error, timeout} = elock:lock(Scope, term1, _IsShared = false, _Timeout = 1000 ),
  ?LOGDEBUG("locks: ~p",[ets:tab2list( Scope )]),

  timer:sleep(1000),
  {ok, U2} = elock:lock(Scope, term1, false, infinity ),
  timer:sleep(1000),

  U2(),
  timer:sleep(1000),
  ?LOGDEBUG("locks: ~p",[ets:tab2list( Scope )]),

  ok.

