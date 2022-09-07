
-module(elock).

%%=================================================================
%%	API
%%=================================================================
-export([
  start_link/1,
  lock/4, lock/5
]).


-define(LOGERROR(Text),lager:error(Text)).
-define(LOGERROR(Text,Params),lager:error(Text,Params)).
-define(LOGWARNING(Text),lager:warning(Text)).
-define(LOGWARNING(Text,Params),lager:warning(Text,Params)).
-define(LOGINFO(Text),lager:info(Text)).
-define(LOGINFO(Text,Params),lager:info(Text,Params)).
-define(LOGDEBUG(Text),lager:debug(Text)).
-define(LOGDEBUG(Text,Params),lager:debug(Text,Params)).

%------------call it from OTP supervisor as a permanent worker------
start_link( Name )->
  {ok, spawn_link(fun()->init(Name) end)}.

init( Name )->

  % Prepare the storage for locks
  ets:new(Name,[named_table,public,set]),

  timer:sleep(infinity).

%-----------Lock request------------------------------------------
lock(Locks, Term, IsShared, Timeout )->
  case lock( Locks, Term, IsShared, Timeout, _Holder = self() ) of
    {ok, {Locker, LockRef}} ->
      {ok, fun()->Locker ! {unlock, LockRef}, ok  end};
    Error -> Error
  end.

lock( Locks, Term, IsShared, Timeout, Holder) when is_pid(Holder)->

  Locker =
    spawn(fun()->
      set_lock(Locks, Holder, Term, IsShared)
    end),

  receive
    {locked, LockRef}->
      {ok, {Locker, LockRef}}
  after
    Timeout->
      Locker ! {timeout, Holder},
      % Drop tail lock
      receive
        {locked, _LockRef}->
          % No it's too late already
          {error, timeout}
      after
        0->
          % Ok I tried
          {error, timeout}
      end
  end;

lock(Locks, Term, IsShared, Timeout, [Node] ) when Node=:=node()->
  lock( Locks, Term, IsShared, Timeout, _Holder = self() );

lock(Locks, Term, IsShared, Timeout, Nodes ) when is_list(Nodes)->
  case ecall:call_all(Nodes, ?MODULE, lock, [ Locks, Term, IsShared, Timeout, _Holder=self() ]) of
    {ok, Results} ->
      Unlock =
        fun() ->
          [Locker ! {unlock, LockRef} || {_N, {Locker, LockRef} } <- Results], ok
        end,
      {ok, Unlock};
    {error,Error}->
      {error,Error}
  end.

%-------------------------------------------------------------------
%%	Algorithm
%%------------------------------------------------------------------
% Lock = { {lock,Term}, LockRef, Queue }
% Queue = { {queue,LockRef,MyQueue}, self() }
%
% Try lock:
% ets:update_counter(?LOCKS, {lock,Term}, {3,1}, {{lock,Term},0,0})
%   1 -> --------------Locked--------------
%     I locked:
%       LockRef = make_ref()
%       ets:update_element(?LOCKS, {lock,Term}, {2,LockRef})
%       Holder ! {locked,LockRef}
%       go to wait unlock
%--------------------Not locked---------------------------
%  MyQueue -> queued, wait
%     Get the lock ref and take place in the queue
%     [ { _Lock, LockRef, _Queue } ] = ets:lookup( ?LOCKS, {lock,Term} ),
%     ets:insert(?LOCKS,{ {queue,LockRef,MyQueue}, self() })
%     go to wait lock
%-------------------Unlock-----------------------------
% try unlock:
%     ets:delete_object(?LOCKS, {{lock,Term}, LockRef, MyQueue})
% check unlocked:
%     ets:lookup(?LOCKS, Term)
%       [] -> Unlocked
%         unlocked
%       [_] -> somebody is waiting
%         who is the next, he must be there, iterate until
%         [{_,NextLocker}] = ets:take(?LOCKS,{queue,LockRef,MyQueue+1}),
%         give him the lock
%         NextLocker ! {take_it, LockRef, GlobalUnlock}
%-------------------Wait lock--------------------------------
% receive {take_it, LockRef, GlobalLock}
%   Holder ! {locked, LockRef}
%   go to wait unlock
%-------------------Wait unlock-------------------------
% receive {unlock,LockRef} or {'DOWN', _Ref, process, Holder, Reason}
%   go to unlock
%-----------------THE END--------------------------------

%-------------------SET LOCK-----------------------------
-define(lock(T),{lock,T}).
-define(queue(R,Q),{queue,R,Q}).

-record(lock,{locks, term, holder, shared, lock_ref, queue}).

set_lock(Locks, Holder, Term, IsShared)->

  Locker = self(),
  % I want to know if you die
  erlang:monitor(process, Holder),

  case ets:update_counter(Locks, ?lock(Term), {3,1}, {?lock(Term),0,0}) of % try to set lock
    1-> %------------------locked------------------
      ?LOGDEBUG("~p set local lock: holder ~p, locker ~p",[ Term, Holder, Locker ]),

      LockRef = make_ref(),
      ets:update_element(Locks, ?lock(Term), {2,LockRef}),

      Holder ! {locked, LockRef},
      wait_unlock(#lock{
        locks = Locks,
        term = Term,
        holder= Holder,
        lock_ref=LockRef,
        queue=1
      });

    MyQueue-> %------------queued-------------------

      LockRef = get_lock_ref(Locks,?lock(Term)),
      ets:insert(Locks,{?queue(LockRef,MyQueue), Locker}),
      if
        IsShared ->
          % Shared locks, the holder can be sure that it's locked
          Holder ! {locked, LockRef};
        true ->
          % Need exclusive?
          wait
      end,
      ?LOGDEBUG("~p lock queued: holder ~p, locker ~p, queue ~p",[Term, Holder, Locker, MyQueue]),
      wait_lock(#lock{
        locks = Locks,
        term = Term,
        holder= Holder,
        lock_ref=LockRef,
        queue= MyQueue
      })
  end.

get_lock_ref( Locks, Lock )->
  case ets:lookup( Locks, Lock ) of
    [ { _Lock, LockRef, _Queue } ]-> LockRef;
    []->
      % The locker has not registered the lock yet wait
      receive after 5 -> ok end,
      get_lock_ref(Locks, Lock )
  end.

%-----------------WAIT UNLOCK-----------------------
wait_unlock(#lock{term = Term, lock_ref = LockRef, holder = Holder }=Lock)->
  Locker = self(),
  receive
    {unlock, LockRef}->
      ?LOGDEBUG("~p try unlock, holder ~p, locker ~p",[ Term, Holder, Locker ]),
      unlock(Lock);
    {timeout, Holder}->
      ?LOGDEBUG("~p holder ~p timeout, locker ~p unlock",[ Term, Holder, Locker ]),
      unlock(Lock);
    {'DOWN', _Ref, process, Holder, Reason}->
      ?LOGDEBUG("~p holder ~p down, reason ~p locker ~p unlock",[ Term, Holder, Reason, Locker ]),
      unlock(Lock)
  end.

%-----------------UNLOCK-----------------------
unlock(#lock{locks = Locks,term = Term, lock_ref = LockRef, queue = MyQueue})->
  % try unlock
  ets:delete_object(Locks, {?lock(Term), LockRef, MyQueue}),
  % check unlocked
  case ets:lookup(Locks, ?lock(Term)) of
    []->  % unlocked, nobody is waiting
      ?LOGDEBUG("~p unlocked"),
      ok;
    [_]-> % not unlocked there is a queue
          % who is the next, he must be there, iterate until
      NextLocker = get_next_locker(Locks,LockRef,MyQueue+1),
      NextLocker ! {take_it, LockRef}
  end.

get_next_locker(Locks, LockRef, Queue )->
  case ets:take(Locks,?queue(LockRef,Queue)) of
    [{_,NextLocker}]->
      NextLocker;
    []-> % He is not registered himself yet, wait
      receive after 5 -> ok end,
      get_next_locker(Locks, LockRef, Queue )
  end.

%-----------------WAIT LOCK-----------------------
wait_lock(#lock{lock_ref = LockRef, holder = Holder, term = Term}=Lock)->
  receive
    {take_it, LockRef}->
      Holder ! {locked, LockRef},
      wait_unlock(Lock);
    {timeout, Holder}->
      % Holder is not waiting anymore, but I can't brake the queue
      wait_lock_unlock( Lock);
    {'DOWN', _, process, Holder, Reason}->
      ?LOGDEBUG("~p holder ~p died while waiting, reason ~p",[Term,Holder,Reason]),
      wait_lock_unlock(Lock)
  end.
wait_lock_unlock(#lock{ lock_ref = LockRef}=Lock)->
  receive
    {take_it, LockRef}-> unlock(Lock)
  end.

%%  elock:start_link(test).
%%{ok, Unlock} = elock:lock(test, test1,_IsShared = false, infinity ).
%%
%%spawn(fun()-> {ok, U} = elock:lock(test, test1,_IsShared = false, infinity ), io:format("locked\r\n") end).