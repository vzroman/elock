
-module(elock).

%%=================================================================
%%	API
%%=================================================================
-export([
  start_link/1,
  lock/5
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
  % Prepare the storage for subscriptions
  ets:new(Name,[named_table,public,set]),
  register(Name, self()),
  ok.

%-----------Lock request------------------------------------------
lock(Locks, Term, IsShared, Nodes, Timeout )->
  Holder = self(),
  Locker = spawn(fun()->set_lock(Locks, Holder, Term, IsShared, Nodes, Timeout)  end),
  receive
    {locked, LockRef}->
      {ok, fun()-> Locker ! {unlock, LockRef}  end}
  after
    Timeout->
      Locker ! {timeout, Holder},
      % Drop tail lock
      receive
        {locked, _LockRef}->
          % No it's too late already
          {error, timeout}
      after
        10->
          % Ok I tried
          {error, timeout}
      end
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

-record(lock,{locks, term, holder, shared, lock_ref, g_unlock, queue}).

set_lock(Locks, Holder, Term, IsShared, Nodes, Timeout)->

  Locker = self(),
  % I want to know if you die
  erlang:monitor(process, Holder),

  case ets:update_counter(Locks, ?lock(Term), {3,1}, {?lock(Term),0,0}) of % try to set lock
    1-> %------------------locked------------------
      ?LOGDEBUG("~p set local lock: holder ~p, locker ~p",[ Term, Holder, Locker ]),

      LockRef = make_ref(),
      ets:update_element(Locks, ?lock(Term), {2,LockRef}),

      case set_global_lock(Locks, Nodes, Term, Timeout ) of % set global lock
        {ok, GlobalUnlock}->
          ?LOGDEBUG("~p set global lock: holder ~p, locker ~p",[ Term, Holder, Locker ]),

          Holder ! {locked, LockRef},
          wait_unlock(#lock{
            locks = Locks,
            term = Term,
            holder= Holder,
            lock_ref=LockRef,
            g_unlock =GlobalUnlock,
            queue=1
          });
        timeout->
          % Global timeout means that my holder is not waiting either
          unlock(#lock{
            locks = Locks,
            term = Term,
            holder= Holder,
            lock_ref=LockRef,
            g_unlock=undefined,
            queue=1
          })
      end;
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
        g_unlock=undefined,
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
unlock(#lock{locks = Locks,term = Term, lock_ref = LockRef, g_unlock = GlobalUnlock, queue = MyQueue})->
  % try unlock
  ets:delete_object(Locks, {?lock(Term), LockRef, MyQueue}),
  % check unlocked
  case ets:lookup(Locks, ?lock(Term)) of
    []->  % unlocked, nobody is waiting
      ?LOGDEBUG("~p unlocked"),
      global_unlock(GlobalUnlock);
    [_]-> % not unlocked there is a queue
          % who is the next, he must be there, iterate until
      NextLocker = get_next_locker(Locks,LockRef,MyQueue+1),
      NextLocker ! {take_it, LockRef, GlobalUnlock}
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
    {take_it, LockRef, GlobalUnlock}->
      Holder ! {locked, LockRef},
      wait_unlock(Lock#lock{g_unlock = GlobalUnlock});
    {timeout, Holder}->
      % Holder is not waiting anymore, but I can't brake the queue
      wait_lock_unlock( Lock);
    {'DOWN', _, process, Holder, Reason}->
      ?LOGDEBUG("~p holder ~p died while waiting, reason ~p",[Term,Holder,Reason]),
      wait_lock_unlock(Lock)
  end.
wait_lock_unlock(#lock{ lock_ref = LockRef}=Lock)->
  receive
    {take_it, LockRef, GlobalUnlock}->
      unlock(Lock#lock{g_unlock = GlobalUnlock})
  end.

set_global_lock(_Locks, [], _Term, _Timeout )->
  % Global lock not required
  undefined;
set_global_lock(Locks, Nodes, Term, Timeout )->
  % Tell other nodes to set local lock
  case ecall:call_all(Nodes,?MODULE,lock,[Locks, Term,Timeout,_IsGlobal = false]) of
    {ok,Unlocks}->
      ?LOGINFO("~p global lock acquired"),
      {ok, Unlocks};
    {error, none_is_available}->
      % No other nodes
      undefined;
    {error,Timeouts}->
      % The locker answers either {ok, Unlock} or {error, Timeout}
      ?LOGDEBUG("~p global lock timeout ~p",[Term, Timeouts ]),
      timeout
  end.

global_unlock(undefined)->
  ok;
global_unlock(Unlocks)->
  [Unlock() || {_N,Unlock} <- Unlocks],
  ok.