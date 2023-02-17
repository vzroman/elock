
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
  do_lock/2,
  find_deadlocks/5
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
  {ok, spawn_link(fun()->

    % Prepare the storage for locks
    ets:new(Name,[
      named_table,
      public,
      set,
      {read_concurrency, true},
      {write_concurrency, true}
    ]),

    ets:new(?graph(Name),[
      named_table,
      public,
      bag,
      {read_concurrency, true},
      {write_concurrency, true}
    ]),

    timer:sleep(infinity)
  end)}.

%-----------Lock request------------------------------------------
lock(Locks, Term, IsShared, Timeout )->
  lock(Locks, Term, IsShared, Timeout, [node()]).

lock(_Locks, _Term, _IsShared, _Timeout, [] )->
  {ok,fun()->ok end};

lock(Locks, Term, IsShared, Timeout, [Node]=Nodes ) when Node=:=node()->
  in_context(Locks, Term, IsShared, Nodes, fun(Lock)->
    case do_lock( Lock, Timeout ) of
      {ok,Unlock}->{ok,[Unlock]};
      Error->Error
    end
  end);

lock(Locks, Term, IsShared, Timeout, Nodes ) when is_list(Nodes)->
  in_context( Locks, Term, IsShared, Nodes, fun( Lock )->
    case ecall:call_all_wait(Nodes, ?MODULE, do_lock, [Lock, Timeout ]) of
      {OKs,[]}->
        {ok,[ Unlock || {_N, {ok,Unlock}} <- OKs ]};
      {OKs,[{_N,Error}|_]}->
        [catch Locker ! {unlock, LockRef} || {_, {ok,{Locker, LockRef}} } <- OKs],
        {error,Error}
    end
  end).


-record(lock,{locks, graph, term, reply_to, holder, shared, held, nodes, lock_ref, queue, deadlock}).

in_context(Locks, Term, IsShared, Nodes, SetLock)->

  HeldLocks =
    case get('$elock$') of
      _Held when is_list(_Held)-> _Held;
      _->[]
    end,

  NodesToLock =
    Nodes -- [N || {L,T,N,S} <-  HeldLocks,
      (L=:=Locks)
      andalso T=:=Term
      andalso (S=:=IsShared) % If the holder already holds the term but the lock is of different type the Locker will receive {upgrade, Holder}
    ],
  if
    length(NodesToLock)=:=0->
      % The term is already locked
      {ok,fun()-> ok end};
    true->
      Lock = #lock{
        locks = Locks,
        graph = ?graph(Locks),
        term = Term,
        holder = self(),
        shared = IsShared,
        held = [{T,N} || {_L,T,N,_S} <- HeldLocks] ,
        nodes = NodesToLock
      },
      case SetLock( Lock ) of
        {ok, Lockers}->
          AddedLocks = [{Locks,Term,N,IsShared} || N <- NodesToLock],
          put('$elock$', HeldLocks ++ AddedLocks),

          Unlock =
            fun()->
              [ catch Locker ! {unlock, LockRef} || {Locker, LockRef} <- Lockers ],
              case erase('$elock$') of
                UnlockHeldLocks when is_list(UnlockHeldLocks)->
                  case UnlockHeldLocks -- AddedLocks of
                    []->ok;
                    RestLocks->
                      put('$elock$',RestLocks)
                  end;
                _->
                  why
              end,
              ok
            end,
          {ok, Unlock};
        Error->
          Error
      end
  end.

do_lock(Lock, Timeout)->

  ReplyTo = self(),
  Locker =
    spawn_link(fun()-> set_lock(Lock#lock{reply_to = ReplyTo}) end),

  receive
    {locked, Locker, LockRef}->
      {ok, {Locker, LockRef}};
    {deadlock, Locker}->
      {error, deadlock};
    {'EXIT', Locker, Reason}->
      {error,Reason}
  after
    Timeout->
      Locker ! {timeout, ReplyTo},
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

set_lock(#lock{
  graph = Graph,
  term = Term,
  holder = Holder,
  nodes = Nodes
}=Lock)->

  process_flag(trap_exit,true),

  % I want to know if you die
  erlang:monitor(process, Holder),

  % Upgrade check
  [ catch Locker ! {upgrade,Holder} || {_,T,Locker} <- ets:lookup(Graph,?holder(Holder)), T=:=Term],

  enqueue( Lock#lock{ nodes = Nodes--[node()] }).

enqueue(#lock{
  locks = Locks,
  term = Term,
  holder = Holder
} = Lock) ->

  Locker = self(),

  case ets:update_counter(Locks, ?lock(Term), {3,1}, {?lock(Term),0,0}) of % try to set lock
    1-> %------------------locked------------------
      ?LOGDEBUG("~p set local lock: holder ~p, locker ~p",[ Term, Holder, Locker ]),

      LockRef = make_ref(),
      ets:update_element(Locks, ?lock(Term), {2,LockRef}),

      Lock1 = Lock#lock{ lock_ref = LockRef, queue = 1 },

      ets:insert(Locks,{?queue(LockRef,1), self()}),

      locked( Lock1 ),
      wait_unlock( Lock1 );

    MyQueue-> %------------queued-------------------

      ?LOGDEBUG("~p lock queued: holder ~p, locker ~p, queue ~p",[Term, Holder, Locker, MyQueue]),

      LockRef = get_lock_ref(Locks,?lock(Term)),

      claim_queue( Lock#lock{ lock_ref=LockRef, queue= MyQueue } )

  end.

locked(#lock{
  reply_to = ReplyTo,
  lock_ref = LockRef,
  term = Term,
  graph = Graph,
  holder = Holder,
  shared = IsShared,
  deadlock = Deadlock
})->

  ?LOGDEBUG("~p locked by ~p shared ~p",[Term,Holder,IsShared]),

  % Stop deadlock checker (if it was started)
  catch Deadlock ! { stop, self() },

  ets:insert(Graph,{?holder(Holder),Term,self()}),

  % Locked
  unlink(ReplyTo),
  ReplyTo ! {locked, self(), LockRef},

  ok.

wait_unlock(#lock{
  term = Term,
  lock_ref = LockRef,
  holder = Holder,
  shared = IsShared
}=Lock )->
  Locker = self(),
  receive
    {unlock, LockRef}->
      ?LOGDEBUG("~p try unlock, holder ~p, locker ~p",[ Term, Holder, Locker ]),
      unlock( Lock );
    {wait_share, LockRef, NextLocker} when IsShared->
      NextLocker ! {take_share,LockRef},
      wait_unlock( Lock);
    {upgrade,Holder}->
      ?LOGDEBUG("~p holder ~p ugrade, locker ~p unlock",[ Term, Holder, Locker ]),
      unlock( Lock );
    {timeout, Holder}->
      ?LOGDEBUG("~p holder ~p timeout, locker ~p unlock",[ Term, Holder, Locker ]),
      unlock( Lock );
    {'DOWN', _Ref, process, Holder, Reason}->
      ?LOGDEBUG("~p holder ~p down, reason ~p locker ~p unlock",[ Term, Holder, Reason, Locker ]),
      unlock( Lock )
  end.

unlock(#lock{
  locks = Locks,
  term = Term,
  holder = Holder,
  lock_ref = LockRef,
  graph = Graph,
  queue = MyQueue
})->

  ?LOGDEBUG("~p unlocked by ~p",[Term,Holder]),

  catch ets:delete_object(Graph,{?holder(Holder),Term,self()}),

  % try to remove the lock
  ets:delete_object(Locks, {?lock(Term), LockRef, MyQueue}),
  % check unlocked
  case ets:lookup(Locks, ?lock(Term)) of
    [{_,LockRef,_}]-> % not unlocked there is a queue
      % who is the next, he must be there, iterate until
      NextLocker = get_next_locker(Locks,LockRef,MyQueue+1),
      catch NextLocker ! {take_it, LockRef};
    _->  % unlocked, nobody is waiting
      ?LOGDEBUG("~p unlocked"),
      ok
  end,

  catch ets:delete(Locks,?queue(LockRef,MyQueue)),
  ok.

claim_queue(#lock{
  locks = Locks,
  lock_ref = LockRef,
  queue = MyQueue,
  shared = IsShared
}=Lock)->

  ets:insert(Locks,{?queue(LockRef,MyQueue), self()}),

  if
    IsShared->
      ask_for_share( Lock );
    true->
      claim_wait( Lock )
  end.


ask_for_share(#lock{
  locks = Locks,
  lock_ref = LockRef,
  queue = MyQueue
}=Lock)->
  case ets:lookup(Locks,?queue(LockRef, MyQueue-1)) of
    [{_, Locker}]->
      Locker ! {wait_share, LockRef, self()},
      claim_wait( Lock );
    _->
      % The locker either not registered yet or already deleted it's queue
      receive
        {take_it, LockRef}->
          % The locker removed it's queue. The lock is mine
          locked( Lock ),
          wait_unlock( Lock )
      after
        5-> ask_for_share( Lock )
      end
  end.

claim_wait(#lock{
  term = Term,
  graph = Graph,
  held = HeldLocks,
  nodes = Nodes
}=Lock)->

  % Init deadlock check process
  Deadlock = check_deadlock(Graph, [{Term,node()}], HeldLocks ++ [{Term,N} || N <- Nodes]),

  wait_lock( Lock#lock{ deadlock = Deadlock }).


wait_lock(#lock{
  lock_ref = LockRef,
  holder = Holder,
  reply_to = ReplyTo,
  term = Term,
  shared = IsShared,
  deadlock = Deadlock
} = Lock)->
  receive
    {take_it, LockRef}->
      locked( Lock ),
      wait_unlock( Lock );
    {take_share,LockRef} when IsShared->
      locked( Lock ),
      wait_shared_lock( Lock );
    {deadlock, Deadlock}->
      ?LOGDEBUG("~p hodler ~p deadlock",[Term,Holder]),
      ReplyTo ! {deadlock, self()},
      wait_lock_unlock( Lock );
    {timeout, Holder}->
      ?LOGDEBUG("~p waiter ~p timeout",[Term,Holder]),
      % Stop deadlock checker
      catch Deadlock ! { stop, self() },
      % Holder is not waiting anymore, but I can't brake the queue
      wait_lock_unlock( Lock );
    {'DOWN', _, process, Holder, Reason}->
      ?LOGDEBUG("~p holder ~p died while waiting, reason ~p",[Term,Holder,Reason]),
      catch Deadlock ! { stop, self() },
      wait_lock_unlock( Lock );
    {'EXIT', ReplyTo, Reason}->
      ?LOGDEBUG("~p reply_to ~p died while waiting, reason ~p",[Term,ReplyTo,Reason]),
      catch Deadlock ! { stop, self() },
      wait_lock_unlock( Lock )
  end.

wait_shared_lock(#lock{
  term = Term,
  lock_ref = LockRef
}=Lock )->
  receive
    {take_it, LockRef}->
      wait_unlock( Lock );
    {wait_share, LockRef, NextLocker}->
      NextLocker ! {take_share,LockRef},
      wait_shared_lock( Lock );
    {timeout, Holder}->
      ?LOGDEBUG("~p hodler ~p timeout after shared lock",[Term,Holder]),
      wait_lock_unlock( Lock );
    {'DOWN', _, process, Holder, Reason}->
      ?LOGDEBUG("~p holder ~p died having shared lock, reason ~p",[Term,Holder,Reason]),
      wait_lock_unlock( Lock )
  end.

%-----------------Keep queue------------------------------------------
wait_lock_unlock(#lock{
  lock_ref = LockRef
}=Lock)->
  receive
    {take_it, LockRef}->
      unlock(Lock);
    {wait_share, LockRef, NextLocker}->
      % I don't need the lock any more even if it's exclusive
      NextLocker ! {take_share,LockRef},
      wait_lock_unlock( Lock )
  end.

%-----------------------------------------------------------------------
% Queue utilities
%-----------------------------------------------------------------------
get_lock_ref( Locks, Lock )->
  case ets:lookup( Locks, Lock ) of
    [ { _Lock, LockRef, _Queue } ] when is_reference(LockRef)-> LockRef;
    _->
      % The locker has not registered the lock yet wait
      receive after 5 -> ok end,
      get_lock_ref(Locks, Lock )
  end.

get_next_locker(Locks, LockRef, Queue )->
  case ets:lookup(Locks,?queue(LockRef,Queue)) of
    [{_,NextLocker}]->
      NextLocker;
    []-> % He is not registered himself yet, wait
      receive after 5 -> ok end,
      get_next_locker(Locks, LockRef, Queue )
  end.

%-----------------------------------------------------------------------
% Deadlocks detection
%-----------------------------------------------------------------------
check_deadlock(_Graph, WaitTerms, HeldLocks) when WaitTerms=:=[]; HeldLocks=:=[] ->
  can_not_have_deadlocks;
check_deadlock(Graph, WaitTerms, HeldLocks)->
  Locker = self(),
  spawn(fun()->
    erlang:monitor(process, Locker),

    Checker = self(),
    [ ets:insert(Graph,[{?wait(Wait),Held, Checker} || Held <- HeldLocks]) || Wait <- WaitTerms ],

    try check_deadlock_loop( Graph, WaitTerms, HeldLocks, Locker )
    after
      [ catch ets:delete_object(Graph,{?wait(Wait),Held,Checker}) || Held <- HeldLocks, Wait <- WaitTerms ]
    end
  end).

check_deadlock_loop( Graph, WaitTerms, HeldLocks, Locker )->

  [ find_deadlocks(HeldTerm, Graph, WaitTerm, HeldLocks, self()) || WaitTerm <- WaitTerms, HeldTerm <- HeldLocks ],

  receive
    {stop, Locker} ->
      stop;
    {'DOWN', _Ref, process, Locker, _Reason} ->
      unlock;
    {compare_locks, From ,Locks}->
      if
        length( HeldLocks ) > length( Locks ); HeldLocks > Locks->
          catch From ! { yield };
        true->
          Locker ! {deadlock, self()}
      end;
    { yield }->
      Locker ! {deadlock, self()}
  after 10->
    check_deadlock_loop( Graph, WaitTerms, HeldLocks, Locker )
  end.

find_deadlocks({_,Node}=Term, Graph, WaitTerm, HeldLocks, Self) when Node=:=node()->

  WhoIsWaiting = ets:lookup( Graph, ?wait(Term) ),

  % To reduce message passing
  [ catch Checker ! {compare_locks, self() ,HeldLocks} || {_,T, Checker} <- WhoIsWaiting,
    T=:=WaitTerm, % He owns the term that I'm waiting for
    Checker > self() % just to reduce message passing
  ],

  [ find_deadlocks(T, Graph, WaitTerm, HeldLocks, Self) || {_,T,_} <- WhoIsWaiting, T=/=WaitTerm ],

  ok;
find_deadlocks({_,Node}=Term, Graph, WaitTerm, HeldLocks, Self)->
  rpc:cast(Node,?MODULE,?FUNCTION_NAME,[[Term],Graph,WaitTerm,HeldLocks,Self]).



%%  elock:start_link(test).
%%  {ok, U1} = elock:lock(test, t1, false, infinity ).
%%  {ok, U2} = elock:lock(test, t2, false, infinity ).
%%
%%  spawn(fun()-> elock:lock(test, t3, false, infinity ), io:format("t3 locked\r\n"), spawn(fun()->elock:lock(test, t4, false, infinity ), io:format("t4 locked\r\n"), io:format("t1 lock: ~p\r\n",[elock:lock(test, t1, false, infinity )]) end ), timer:sleep(1000), elock:lock(test, t4, false, infinity ), io:format("t4 locked2\r\n"), timer:sleep(10000)  end).
%%
%%  {ok, U3} = elock:lock(test, t3, false, infinity ).