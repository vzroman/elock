
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
  {ok, spawn_link(fun()->

    % Prepare the storage for locks
    ets:new(Name,[named_table,public,set]),

    ets:new(?graph(Name),[named_table,public,bag]),

    timer:sleep(infinity)
  end)}.

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
  case ecall:call_all_wait(Nodes, ?MODULE, do_lock, [ Locks, Term, IsShared, Timeout, _Holder=self(), Nodes ]) of
    {OKs,[]} ->
      Unlock =
        fun() ->
          [catch Locker ! {unlock, LockRef} || {_N, {Locker, LockRef} } <- OKs], ok
        end,
      {ok, Unlock};
    {OKs,[{_N,Error}|_]}->
      [catch Locker ! {unlock, LockRef} || {_, {Locker, LockRef} } <- OKs],
      {error,Error}
  end.

do_lock( Locks, Term, IsShared, Timeout, Holder, Nodes) when is_pid(Holder)->

  ReplyTo = self(),
  Locker =
    spawn(fun()-> set_lock(ReplyTo = self(), Locks, Holder, Term, IsShared, Nodes) end),

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
        {locked, Locker, _LockRef}->ignore;
        {deadlock,Locker}->ignore
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

-record(lock,{locks, graph, term, reply_to, holder, shared, held, nodes, lock_ref, queue, deadlock}).

set_lock(ReplyTo, Locks, Holder, Term, IsShared, Nodes)->

  process_flag(trap_exit,true),

  HeldLocks =
    [ T || {_,T,S} <- ets:lookup(?graph(Locks),?holder(Holder)), (S=:=IsShared) or (S=:=false)],
  case lists:member(Term,HeldLocks) of
    true->
      LockRef = make_ref(),
      ReplyTo ! {locked, self(), LockRef },
      already_locked;
    _->

      % I want to know if you die
      erlang:monitor(process, Holder),

      Lock = #lock{
        locks = Locks,
        graph = ?graph(Locks),
        term = Term,
        reply_to = ReplyTo,
        holder = Holder,
        shared = IsShared,
        held = HeldLocks,
        nodes = Nodes--[node()]
      },

      enqueue( Lock )
  end.

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
  held = HeldLocks,
  nodes = Nodes,
  deadlock = Deadlock
})->

  ?LOGDEBUG("~p locked by ~p shared ~p",[Term,Holder,IsShared]),

  % Stop deadlock checker (if it was started)
  catch Deadlock ! { stop, self() },

  ets:insert(Graph,{?holder(Holder),Term,IsShared}),

  % Locked
  ReplyTo ! {locked, self(), LockRef},

  % Init cross nodes deadlock checker process.
  % If 2 processes try to lock the same term on the same (or intersecting) nodes
  % And P1 is successful at node A and the P2 on node B then:
  % At node A:
  %   P1 claims that it has the Term and is waiting for term at node B
  %   P2 is queued but claims that it has the term at node B (may be lies)
  % At node B:
  %   P1 is queued but claims that it has the term at node A
  %   P2 claims that it has the Term and is waiting for term at node A
  % Then at node A (and the opposite at node B):
  % P1 has Term, waiting {Term,nodeB}
  % P2 has {Term,nodeB}, waiting Term
  % Deadlock
  check_deadlock(Graph, [{Term,N} || N <- Nodes], [Term|HeldLocks]),

  % As the Locker has already replied with 'locked' it doesn't listen to 'deadlock'
  % The holder will catch the deadlock at some other nodes and unlock the term everywhere.
  % The deadlock checker will die with the locker after unlocking
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
  queue = MyQueue,
  shared = IsShared
})->

  ?LOGDEBUG("~p unlocked by ~p",[Term,Holder]),

  catch ets:delete_object(Graph,{?holder(Holder),Term,IsShared}),

  % try to remove the lock
  ets:delete_object(Locks, {?lock(Term), LockRef, MyQueue}),
  % check unlocked
  case ets:lookup(Locks, ?lock(Term)) of
    []->  % unlocked, nobody is waiting
      ?LOGDEBUG("~p unlocked"),
      ok;
    [_]-> % not unlocked there is a queue
      % who is the next, he must be there, iterate until
      NextLocker = get_next_locker(Locks,LockRef,MyQueue+1),
      catch NextLocker ! {take_it, LockRef}
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
  Deadlock = check_deadlock(Graph, [Term], HeldLocks ++ [{Term,N} || N <- Nodes]),

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
      [ [ catch ets:delete_object(Graph,{?wait(Wait),Held,Checker}) || Held <- HeldLocks] || Wait <- WaitTerms ]
    end
  end).

check_deadlock_loop( Graph, WaitTerms, HeldLocks, Locker )->

  [find_deadlocks(HeldLocks, Graph, WaitTerm, fun( Checker )->
    catch Checker ! {compare_locks, self() ,HeldLocks}
  end) || WaitTerm <- WaitTerms ],

  receive
    {stop, Locker} ->
      stop;
    {'DOWN', _Ref, process, Locker, _Reason} ->
      unlock;
    {compare_locks, From ,Locks}->
      if
        length( Locks ) > length( HeldLocks ); Locks > HeldLocks->
          Locker ! {deadlock, self()};
        true->
          catch From ! { yield }
      end;
    { yield }->
      Locker ! {deadlock, self()}
  after 10->
    check_deadlock_loop( Graph, WaitTerms, HeldLocks, Locker )
  end.

find_deadlocks([Term|Rest], Graph, WaitTerm, CheckFun )->

  WhoIsWaiting = ets:lookup( Graph, ?wait(Term) ),

  % To reduce message passing
  [ CheckFun(Checker) || {_,T, Checker} <- WhoIsWaiting,
    T=:=WaitTerm, % He owns the term that I'm waiting for
    Checker > self() % just to reduce message passing
  ],

  find_deadlocks([ T || {_,T,_} <- WhoIsWaiting, T=/=WaitTerm ], Graph, WaitTerm, CheckFun ),

  find_deadlocks( Rest, Graph, WaitTerm, CheckFun );
find_deadlocks( [], _Graph, _WaitTerm, _CheckFun )->
  ok.



%%  elock:start_link(test).
%%{ok, Unlock} = elock:lock(test, test1,_IsShared = false, infinity ).
%%
%%spawn(fun()-> {ok, U} = elock:lock(test, test1,_IsShared = false, infinity ), io:format("locked\r\n") end).