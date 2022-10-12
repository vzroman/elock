-module(elock_local_SUITE).
-include_lib("elock_test.hrl").

-export([all/0, init_per_testcase/2, end_per_testcase/2]).

%% tests
-export([
  simple_test/1,
  lock_test/1,
  deadlock_test/1,
  shared_lock_test1/1,
  shared_lock_test2/1,
  shared_deadlock_test/1
]).

-define(MY_LOCK, '$my_lock').
-define(graph(Locks),list_to_atom(atom_to_list(Locks)++"_$graph$")).

-define(SYNC_TIME, 2000).

all() ->
  [
    simple_test,
    lock_test,
    deadlock_test,
    shared_lock_test1,
    shared_lock_test2,
    shared_deadlock_test
  ].

init_per_testcase(_Case, Config) ->
  {ok, _Pid} = elock:start_link(?MY_LOCK),
  Config.

end_per_testcase(_Case, _Config) ->
  ok.

simple_test(_Config) ->
  Master = self(),
  SpawnProc =
    fun(T1, T2) ->
      case elock:lock(?MY_LOCK, term, false, T1) of
        {ok, Unlock} ->
          ct:pal("get lock, pid ~p", [self()]),
          timer:sleep(T2), Unlock(), Master ! {unlocked};
        Error ->
          ct:pal("error ~p, pid ~p", [Error, self()]), Master ! Error
      end,
      receive {die, Master} -> ct:pal("pid ~p dying", [self()]) end
    end,
  Check =
    fun Check(Level, {Ok, Error}) ->
      if
        Level >= 2 -> {Ok, Error};
        true ->
          receive
            {unlocked} -> Check(Level + 1, {Ok + 1, Error});
            {error, _} -> Check(Level + 1, {Ok, Error + 1})
          end
      end
    end,

  Timeouts = [
    {500, 1000},
    {1000, 500}
  ],
  [begin
     Pid1 = spawn(fun() -> SpawnProc(T1, T2) end),
     Pid2 = spawn(fun() -> SpawnProc(T1, T2) end),
     ct:pal("pid1 ~p, pid2 ~p", [Pid1, Pid2]),
     timer:sleep(T1 + T2),
     if
       T1 < T2 -> {1, 1} = Check(0, {0, 0});
       true -> {2, 0} = Check(0, {0, 0})
     end,
     Pid1 ! {die, Master}, Pid2 ! {die, Master}
  end||{T1, T2} <- Timeouts],

  [] = ets:match(?MY_LOCK, '$1'),
  [] = ets:match(?graph(?MY_LOCK), '$1'),
  ok.

%% Terms are numbers from 1 to QueueNum
lock_test(_Config) ->
  Master = self(),
  QueueNum = 5,

  LockTerm = fun(Term) ->
      {ok, Unlock} = elock:lock(?MY_LOCK, Term, false, infinity),
      Master ! {unlock, Unlock}
  end,
  ProcNum = 1000,
  [begin
     Term = rand:uniform(QueueNum),
     _Pid = spawn(fun() -> LockTerm(Term) end)
   end
    ||_I<-lists:seq(1, ProcNum)],

  [receive {unlock, Unlock} -> Unlock() end || _I<-lists:seq(1, ProcNum)],

%%  ct:pal("my_lock ~p", [ets:match(?MY_LOCK,'$1')]),
%%  ct:pal("graph ~p", [ets:match(?graph(?MY_LOCK),'$1')]),
  % TODO It`s hack. But i do not know what to do
  % We read table before child process or processes deleted content,
  % because of race condition, at least i think so
  timer:sleep(?SYNC_TIME),
  [] = ets:match(?MY_LOCK,'$1'),
  [] = ets:match(?graph(?MY_LOCK), '$1'),
  ok.

deadlock_test(_Config) ->
  CycleLen = 100,
  Master = self(),
  LockTerm = fun(Term) ->
    case elock:lock(?MY_LOCK, Term, false, infinity) of
      {ok, Unlock1} -> Master ! {unlock, Unlock1};
      Error1 ->
        ct:pal("error1 ~p, pid ~p", [Error1, self()])
    end,
    receive {follow, Next} ->
      case elock:lock(?MY_LOCK, Next, false, infinity) of
        {ok, Unlock2} -> Master ! {unlock, Unlock2};
        Error2 ->
          ct:pal("error2 ~p, pid ~p", [Error2, self()]),
          Master ! Error2
      end
    end
  end,
  Terms = [ I ||I <- lists:seq(1, CycleLen)],
  Pids = [spawn(fun() -> LockTerm(Term) end) || Term<-Terms],
%%  ct:pal("pid ~p", [Pids]),
  [First|Rest] = Terms,
  Terms1 = Rest++[First],
  [Pid ! {follow, T} ||{T, Pid}<-lists:zip(Terms1, Pids)],

  Rec = [receive
     {unlock, Unlock} -> fun() -> Unlock(), 0 end;
     {error, deadlock} -> fun() -> 1 end
   end
    ||_I <- lists:seq(1, 2 * CycleLen)],

  ?assertNotEqual(0, lists:sum([F()||F<- Rec]) ),
%%  ct:pal("DetectedDeadlocks ~p", [DetectedDeadlocks]),

  % TODO It`s hack. But i do not know what to do
  % We read table before child process or processes deleted content,
  % because of race condition, at least i think so
  timer:sleep(?SYNC_TIME),

  [] = ets:match(?MY_LOCK, '$1'),
  [] = ets:match(?graph(?MY_LOCK), '$1'),
  ok.

shared_lock_test1(_Config) ->
  MaxQueueLen = 1000,
  QueueNum = 5,
  Master = self(),
  LockTerm = fun(Term) ->
    case elock:lock(?MY_LOCK, Term, true, infinity) of
      {ok, Unlock} -> Master ! {unlock, Unlock};
      Error ->
        ct:pal("Error ~p, pid ~p", [Error, self()]),
        Master ! Error
    end
  end,
  _PIDs = [spawn(fun() -> LockTerm(I rem QueueNum) end)||  I<-lists:seq(1, MaxQueueLen)],

  [ receive {unlock, Unlock} -> Unlock() end||_I<-lists:seq(1, MaxQueueLen)],

  % TODO It`s hack. But i do not know what to do
  % We read table before child process or processes deleted content,
  % because of race condition, at least i think so
  timer:sleep(?SYNC_TIME),
  [] = ets:match(?MY_LOCK, '$1'),
  [] = ets:match(?graph(?MY_LOCK), '$1'),
  ok.

shared_lock_test2(_Config) ->
  Master = self(),
  LockTerm = fun(T1) ->
    case elock:lock(?MY_LOCK, term, true, T1) of
      {ok, Unlock} ->
        Master ! {unlock, Unlock};
      Error ->
        ct:pal("Error ~p, pid ~p", [Error, self()]),
        Master ! Error
    end
  end,
  Timeouts = [
    {1000, 500},
    {500, 1000}
  ],
  [begin
     {ok, Unlock} = elock:lock(?MY_LOCK, term, false, T1),
     [spawn(fun() -> LockTerm(T1) end) ||_I <-lists:seq(1, 5)],
     timer:sleep(T2),
     Unlock(),
     Free =
       if
         T1 < T2 -> [receive {error, timeout} -> fun() -> ok end end||_I <-lists:seq(1, 5)];
         true -> [receive {unlock, UnlockShared} -> UnlockShared end || _I <-lists:seq(1, 5)]
       end,
     [F()||F<-Free]
   end
    ||{T1, T2} <- Timeouts],

  timer:sleep(?SYNC_TIME),
  [] = ets:match(?MY_LOCK, '$1'),
  [] = ets:match(?graph(?MY_LOCK), '$1'),
  ok.

shared_deadlock_test(_Config) ->

  ok.


%%=================================================================
%% Utilities
%%=================================================================



