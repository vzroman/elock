
-module(elock).

%%=================================================================
%%	API
%%=================================================================
-export([
  start_link/1,
  lock/4, lock/5
]).

%%=================================================================
%%	Internal API
%%=================================================================
-export([
  do_lock/6
]).

-define(LOGERROR(Text),lager:error(Text)).
-define(LOGERROR(Text,Params),lager:error(Text,Params)).
-define(LOGWARNING(Text),lager:warning(Text)).
-define(LOGWARNING(Text,Params),lager:warning(Text,Params)).
-define(LOGINFO(Text),lager:info(Text)).
-define(LOGINFO(Text,Params),lager:info(Text,Params)).
-define(LOGDEBUG(Text),lager:debug(Text)).
-define(LOGDEBUG(Text,Params),lager:debug(Text,Params)).

-define(graph(Locks),list_to_atom(atom_to_list(Locks)++"_$graph$")).

%------------call it from OTP supervisor as a permanent worker------
start_link( Name )->
  {ok, spawn_link(fun()->init(Name) end)}.

init( Name )->

  % Prepare the storage for locks
  ets:new(Name,[named_table,public,set]),

  ets:new(?graph(Name),[named_table,public,bag]),

  timer:sleep(infinity).

%-----------Lock request------------------------------------------
lock(Locks, Term, IsShared, Timeout )->
  case do_lock( Locks, Term, IsShared, Timeout, _Holder = self(), _Nodes= [] ) of
    {ok, {Locker, LockRef}} ->
      {ok, fun()-> catch Locker ! {unlock, LockRef}, ok  end};
    Error -> Error
  end.

lock(Locks, Term, IsShared, Timeout, [Node] ) when Node=:=node()->
  case do_lock( Locks, Term, IsShared, Timeout, _Holder = self(), _Nodes= [] ) of
    {ok,{Locker,LockRef}}->
      {ok,fun()-> catch Locker ! {unlock, LockRef} end};
    Error->
      Error
  end;

lock(Locks, Term, IsShared, Timeout, Nodes ) when is_list(Nodes)->
  case ecall:call_all(Nodes, ?MODULE, do_lock, [ Locks, Term, IsShared, Timeout, _Holder=self(), Nodes ]) of
    {ok, Results} ->
      Unlock =
        fun() ->
          [catch Locker ! {unlock, LockRef} || {_N, {Locker, LockRef} } <- Results], ok
        end,
      {ok, Unlock};
    {error,Error}->
      {error,Error}
  end.

do_lock( Locks, Term, IsShared, Timeout, Holder, Nodes) when is_pid(Holder)->

  Locker =
    spawn(fun()->
      set_lock(Locks, Holder, Term, IsShared, Nodes)
    end),

  receive
    {locked, Locker, LockRef}->
      {ok, {Locker, LockRef}};
    {deadlock, Locker}->
      {error, deadlock}
  after
    Timeout->
      Locker ! {timeout, Holder},
      % Drop tail lock
      receive
        {locked, Locker, _LockRef}->ok;
        {deadlock,Locker}->ok
      after
        0-> ok
      end,
      % Ok I tried
      {error, timeout}
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
%       Holder ! {locked, self(), LockRef}
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
%   Holder ! {locked, self(), LockRef}
%   go to wait unlock
%-------------------Wait unlock-------------------------
% receive {unlock,LockRef} or {'DOWN', _Ref, process, Holder, Reason}
%   go to unlock
%-----------------THE END--------------------------------

%-------------------SET LOCK-----------------------------
-define(lock(T),{lock,T}).
-define(queue(R,Q),{queue,R,Q}).

-define(holder(H),{holder,H}).
-define(wait(T),{wait,T}).

-record(lock,{locks, term, holder, shared, lock_ref, queue, held, nodes}).

set_lock(Locks, Holder, Term, IsShared, Nodes)->

  HeldLocks =
    [ T || {_,T} <- ets:lookup(?graph(Locks),?holder(Holder))],
  case lists:member(Term,HeldLocks) of
    true->
      LockRef = make_ref(),
      Holder ! {locked, self(), LockRef },
      already_locked_i_can_die;
    _->

      process_flag(trap_exit,true),

      do_set_lock(Locks, Holder, Term, IsShared, HeldLocks, Nodes--[node()])

  end.

do_set_lock(Locks, Holder, Term, IsShared, HeldLocks, Nodes)->
  Locker = self(),

  % I want to know if you die
  erlang:monitor(process, Holder),

  case ets:update_counter(Locks, ?lock(Term), {3,1}, {?lock(Term),0,0}) of % try to set lock
    1-> %------------------locked------------------
      ?LOGDEBUG("~p set local lock: holder ~p, locker ~p",[ Term, Holder, Locker ]),

      LockRef = make_ref(),
      ets:insert(Locks,{?queue(LockRef,_MyQueue=1), Locker}),
      ets:update_element(Locks, ?lock(Term), {2,LockRef}),

      wait_unlock(#lock{
        locks = Locks,
        term = Term,
        holder= Holder,
        lock_ref=LockRef,
        shared = IsShared,
        queue=1,
        held = [],
        nodes = Nodes
      });

    MyQueue-> %------------queued-------------------

      LockRef = get_lock_ref(Locks,?lock(Term)),
      ets:insert(Locks,{?queue(LockRef,MyQueue), Locker}),

      Lock = #lock{
        locks = Locks,
        term = Term,
        holder= Holder,
        lock_ref=LockRef,
        shared = IsShared,
        queue= MyQueue,
        held = HeldLocks,
        nodes = Nodes
      },

      ?LOGDEBUG("~p lock queued: holder ~p, locker ~p, queue ~p",[Term, Holder, Locker, MyQueue]),

      NextState =
        if
          IsShared ->
            case request_share(Locks, LockRef, MyQueue-1 ) of
              queued ->
                fun wait_lock/1;
              locked->
                Holder ! {locked, Locker, LockRef},
                fun wait_unlock/1
            end;
          true ->
            % Need exclusive?
            fun wait_lock/1
        end,
      NextState( Lock )
  end.

get_lock_ref( Locks, Lock )->
  case ets:lookup( Locks, Lock ) of
    [ { _Lock, LockRef, _Queue } ] when is_reference(LockRef)-> LockRef;
    _->
      % The locker has not registered the lock yet wait
      receive after 5 -> ok end,
      get_lock_ref(Locks, Lock )
  end.

request_share(Locks, LockRef, Queue )->
  case ets:lookup(Locks,?queue(LockRef,Queue)) of
    [{_, Locker}]->
      Locker ! {wait_share, LockRef, self()},
      queued;
    _->
      % The locker either not registered yet or already deleted it's queue
      receive
        {take_it, LockRef}->
          % The locker removed it's queue. The lock is mine
          locked
      after
        5->
          request_share( Locks, LockRef, Queue )
      end
  end.

%-----------------WAIT UNLOCK-----------------------
wait_unlock(#lock{
  locks = Locks,
  term = Term,
  lock_ref = LockRef,
  holder = Holder,
  shared = IsShared,
  nodes = Nodes,
  held = HeldLocks
}=Lock)->

  Locker = self(),

  Graph = ?graph(Locks),
  ets:insert(Graph,{?holder(Holder),Term,IsShared}),

  % Locked
  Holder ! {locked, Locker, LockRef},

  % Init cross nodes deadlock check process
  DeadLock = check_deadlock(Graph, [{Term,N} || N <- Nodes], [Term|HeldLocks]),

  NextState = wait_unlock(DeadLock, Lock),

  catch exit(DeadLock,shutdown),
  catch ets:delete_object(Graph,{?holder(Holder),Term,IsShared}),

  NextState( Lock ).

wait_unlock( DeadLock, #lock{
  term = Term,
  lock_ref = LockRef,
  holder = Holder,
  shared = IsShared
}=Lock )->
  Locker = self(),
  receive
    {unlock, LockRef}->
      ?LOGDEBUG("~p try unlock, holder ~p, locker ~p",[ Term, Holder, Locker ]),
      fun unlock/1;
    {wait_share, LockRef, NextLocker} when IsShared->
      NextLocker ! {take_share,LockRef},
      wait_unlock(DeadLock, Lock);
    {'EXIT',DeadLock,deadlock}->
      Holder ! {deadlock, self()},
      fun unlock/1;
    {timeout, Holder}->
      ?LOGDEBUG("~p holder ~p timeout, locker ~p unlock",[ Term, Holder, Locker ]),
      fun unlock/1;
    {'DOWN', _Ref, process, Holder, Reason}->
      ?LOGDEBUG("~p holder ~p down, reason ~p locker ~p unlock",[ Term, Holder, Reason, Locker ]),
      fun unlock/1
  end.

%-----------------UNLOCK-----------------------
unlock(#lock{locks = Locks,term = Term, lock_ref = LockRef, queue = MyQueue})->

  % Delete my queue
  ets:delete(Locks,?queue(LockRef,MyQueue)),

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
  case ets:lookup(Locks,?queue(LockRef,Queue)) of
    [{_,NextLocker}]->
      NextLocker;
    []-> % He is not registered himself yet, wait
      receive after 5 -> ok end,
      get_next_locker(Locks, LockRef, Queue )
  end.

%-----------------WAIT LOCK-----------------------
wait_lock(#lock{
  locks = Locks,
  term = Term,
  held = HeldLocks,
  nodes = Nodes
}=Lock)->

  % Init deadlock check process
  DeadLock = check_deadlock(?graph(Locks), [Term], HeldLocks++[{Term,N} || N <- Nodes]),

  NextState= wait_lock(DeadLock, Lock),

  catch exit(DeadLock,shutdown),

  NextState( Lock ).

wait_lock(DeadLock, #lock{
  lock_ref = LockRef,
  holder = Holder,
  term = Term,
  shared = IsShared
})->
  receive
    {take_it, LockRef}->
      fun wait_unlock/1;
    {take_share,LockRef} when IsShared->
      fun shared_lock/1;
    {'EXIT',DeadLock,deadlock}->
      Holder ! {deadlock, self()},
      fun wait_lock_unlock/1;
    {timeout, Holder}->
      % Holder is not waiting anymore, but I can't brake the queue
      fun wait_lock_unlock/1;
    {'DOWN', _, process, Holder, Reason}->
      ?LOGDEBUG("~p holder ~p died while waiting, reason ~p",[Term,Holder,Reason]),
      fun wait_lock_unlock/1
  end.

%-----------------SHARED LOCK-----------------------
shared_lock(#lock{
  locks = Locks,
  lock_ref = LockRef,
  term = Term,
  holder = Holder,
  shared = IsShared,
  nodes = Nodes,
  held = HeldLocks
} = Lock)->

  Graph = ?graph(Locks),
  ets:insert(Graph,{?holder(Holder),Term,IsShared}),

  Holder ! {locked, self(), LockRef},

  % Init cross nodes deadlock check process
  DeadLock = check_deadlock(Graph, [{Term,N} || N <- Nodes], [Term|HeldLocks]),

  NextState = wait_shared_lock(DeadLock, Lock),

  catch ets:delete_object(Graph,{?holder(Holder),Term,IsShared}),
  catch exit(DeadLock,shutdown),

  NextState( Lock ).

wait_shared_lock(DeadLock, #lock{
  term = Term,
  lock_ref = LockRef,
  holder = Holder
}=Lock)->

  Locker = self(),
  receive
    {unlock, LockRef}->
      ?LOGDEBUG("~p try unlock, holder ~p, locker ~p",[ Term, Holder, Locker ]),
      fun wait_lock_unlock/1;
    {take_it, LockRef}->
      wait_shared_unlock( Lock );
    {wait_share, LockRef, NextLocker}->
      NextLocker ! {take_share,LockRef},
      wait_shared_lock(DeadLock, Lock );
    {'EXIT',DeadLock,deadlock}->
      Holder ! {deadlock, self()},
      fun wait_lock_unlock/1;
    {timeout, Holder}->
      ?LOGDEBUG("~p holder ~p timeout, locker ~p unlock",[ Term, Holder, Locker ]),
      fun wait_lock_unlock/1;
    {'DOWN', _Ref, process, Holder, Reason}->
      ?LOGDEBUG("~p holder ~p down, reason ~p locker ~p unlock",[ Term, Holder, Reason, Locker ]),
      fun wait_lock_unlock/1
  end.

wait_shared_unlock( #lock{
  term = Term,
  lock_ref = LockRef,
  holder = Holder
} = Lock)->
  Locker = self(),
  receive
    {unlock, LockRef}->
      ?LOGDEBUG("~p try unlock, holder ~p, locker ~p",[ Term, Holder, Locker ]),
      fun unlock/1;
    {wait_share, LockRef, NextLocker}->
      NextLocker ! {take_share,LockRef},
      wait_shared_unlock( Lock );
    {timeout, Holder}->
      ?LOGDEBUG("~p holder ~p timeout, locker ~p unlock",[ Term, Holder, Locker ]),
      fun unlock/1;
    {'DOWN', _Ref, process, Holder, Reason}->
      ?LOGDEBUG("~p holder ~p down, reason ~p locker ~p unlock",[ Term, Holder, Reason, Locker ]),
      fun unlock/1
  end.


%-----------------Keep queue------------------------------------------
wait_lock_unlock(#lock{
  lock_ref = LockRef,
  shared = IsShared
}=Lock)->
  receive
    {take_it, LockRef}->
      unlock(Lock);
    {wait_share, LockRef, NextLocker} when IsShared->
      NextLocker ! {take_share,LockRef},
      wait_lock_unlock( Lock )
  end.

%-----------------------------------------------------------------------
% Deadlocks detection
%-----------------------------------------------------------------------
check_deadlock(_Graph, WaitTerms, HeldLocks) when WaitTerms=:=[]; HeldLocks=:=[] ->
  can_not_have_deadlocks;
check_deadlock(Graph, WaitTerms, HeldLocks)->
  spawn_link(fun()->
    process_flag(trap_exit,true),

    Checker = self(),
    [ ets:insert(Graph,[{?wait(Wait),Held, Checker} || Held <- HeldLocks]) || Wait <- WaitTerms ],

    try check_deadlock_loop( Graph, WaitTerms, HeldLocks )
    after
      [ [ catch ets:delete_object(Graph,{?wait(Wait),Held,Checker}) || Held <- HeldLocks] || Wait <- WaitTerms ]
    end
  end).

check_deadlock_loop( Graph, WaitTerms, HeldLocks )->

  [find_deadlocks(HeldLocks, Graph, WaitTerm, fun( Checker )->
    catch Checker ! {compare_locks, self() ,HeldLocks}
  end) || WaitTerm <- WaitTerms ],

  receive
    {'EXIT',_,_} ->
      unlock;
    {compare_locks, From ,Locks}->
      if
        length( Locks ) > length( HeldLocks ); Locks > HeldLocks->
          exit( deadlock );
        true->
          catch From ! { yield }
      end;
    { yield }->
      exit( deadlock )
  after 10->
    check_deadlock_loop( Graph, WaitTerms, HeldLocks )
  end.

find_deadlocks([Term|Rest], Graph, WaitTerm, CheckFun )->

  WhoIsWaiting = ets:lookup( Graph, ?wait(Term) ),

  [ CheckFun(Checker) || {_,T, Checker} <- WhoIsWaiting, T=:=WaitTerm ],

  find_deadlocks([ T || {_,T,_} <- WhoIsWaiting, T=/=WaitTerm ], Graph, WaitTerm, CheckFun ),

  find_deadlocks( Rest, Graph, WaitTerm, CheckFun );
find_deadlocks( [], _Graph, _WaitTerm, _CheckFun )->
  ok.



%%  elock:start_link(test).
%%{ok, Unlock} = elock:lock(test, test1,_IsShared = false, infinity ).
%%
%%spawn(fun()-> {ok, U} = elock:lock(test, test1,_IsShared = false, infinity ), io:format("locked\r\n") end).