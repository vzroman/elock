
-module(elock).

-include("elock.hrl").

%%=================================================================
%%	API
%%=================================================================
-export([
  start_link/1,
  lock/4, lock/5,
  ready_nodes/1
]).

%%=================================================================
%%	Internal API
%%=================================================================
-export([
  do_lock/2,
  registered_locks/3
]).

-export([
  test/0,
  try_test_lock/3
]).

-define(deadlock_scope(Locks),list_to_atom(atom_to_list(Locks)++"_$deadlock_scope$")).

%------------call it from OTP supervisor as a permanent worker------
start_link( Name )->
  {ok, spawn_link(fun()->

    % Prepare the storage for locks
    ets:new(Name,[
      named_table,
      public,
      ordered_set,
      {read_concurrency, true},
      {write_concurrency, auto}
    ]),

    DeadLockScope = ?deadlock_scope( Name ),
    case pg:start_link( DeadLockScope ) of
      {ok,_} -> ok;
      {error,{already_started,_}}->ok;
      {error,Error}-> throw({pg_error, Error})
    end,
    pg:join(DeadLockScope, {?MODULE,'$members$'}, self() ),

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
        ?LOGDEBUG("lock OKs: ~p",[OKs]),
        {ok,[ Unlock || {_N, {ok,Unlock}} <- OKs ]};
      {OKs,[{_N,Error}|_]}->
        ?LOGDEBUG("rollback OKs: ~p",[OKs]),
        ?LOGDEBUG("rollback OKs parsed: ~p",[[{Locker , {unlock, LockRef}} || {_, {ok,{Locker, LockRef}} } <- OKs]]),
        [catch Locker ! {unlock, LockRef} || {_, {ok,{Locker, LockRef}} } <- OKs],
        {error,Error}
    end
  end).

ready_nodes( Locks )->
  [ node(PID) ||PID <- pg:get_members(?deadlock_scope(Locks), {?MODULE,'$members$'})].


-record(lock,{
  locks,
  term,
  reply_to,
  holder,
  shared,
  held,
  nodes,
  lock_ref,
  queue,
  deadlock_scope,
  deadlock,
  has_share,
  prev
}).

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
        deadlock_scope = ?deadlock_scope(Locks),
        term = Term,
        holder = self(),
        shared = IsShared,
        held = [{T,N} || {_L,T,N,_S} <- HeldLocks] ,
        nodes = NodesToLock,
        has_share = undefined
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

set_lock(#lock{
  locks = Locks,
  term = Term,
  holder = Holder,
  nodes = Nodes
}=Lock)->

  process_flag(trap_exit,true),

  % I want to know if you die
  erlang:monitor(process, Holder),

  % Upgrade check
  [ catch Locker ! {upgrade,Holder} || Locker <- registered_locks( Locks, Term, Holder ) ],

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
  locks = Locks,
  reply_to = ReplyTo,
  lock_ref = LockRef,
  term = Term,
  holder = Holder,
  shared = IsShared,
  deadlock_scope = DeadLockScope,
  deadlock = Deadlock
})->

  ?LOGDEBUG("~p locked by ~p shared ~p",[Term,Holder,IsShared]),

  % Stop deadlock checker (if it was started)
  catch Deadlock ! { stop, self() },

  register_lock( Locks, DeadLockScope, Term, Holder ),

  % Locked
  unlink(ReplyTo),
  ReplyTo ! {locked, self(), LockRef},

  ok.

wait_unlock(#lock{
  term = Term,
  lock_ref = LockRef,
  holder = Holder,
  reply_to = ReplyTo,
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
    {timeout, ReplyTo}->
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
  deadlock_scope = DeadLockScope,
  queue = MyQueue
})->

  ?LOGDEBUG("~p unlocking by ~p",[Term,Holder]),

  unregister_lock( Locks, DeadLockScope, Term, Holder ),

  % try to remove the lock
  ets:delete_object(Locks, {?lock(Term), LockRef, MyQueue}),
  % check unlocked
  case ets:lookup(Locks, ?lock(Term)) of
    [{_,LockRef,_}]-> % not unlocked there is a queue
      % wait for the next to claim the queue
      receive {next, LockRef, _Next} -> ok end;
    _->  % unlocked, nobody is waiting
      ?LOGDEBUG("~p unlocked",[Term]),
      ok
  end,

  catch ets:delete(Locks,?queue(LockRef,MyQueue)),
  exit(normal).

claim_queue(#lock{
  locks = Locks,
  lock_ref = LockRef,
  queue = MyQueue
}=Lock)->

  % Register the queue
  ets:insert(Locks,{?queue(LockRef,MyQueue), self()}),

  % Notify the previous
  Lock1 = claim_next( Lock ),

  % Init waiting
  claim_wait( Lock1 ).

claim_next(#lock{
  locks = Locks,
  lock_ref = LockRef,
  queue = MyQueue,
  shared = IsShared
}=Lock)->
  Prev = get_queue_pid(Locks, ?queue(LockRef, MyQueue-1) ),
  ?LOGDEBUG("~p queue:~p prev:~p",[ LockRef, MyQueue, Prev ]),
  monitor(process, Prev),
  Prev ! {next, LockRef, self()},
  if
    IsShared ->
      Prev ! {wait_share, LockRef, self()};
    true ->
      ignore
  end,
  Lock#lock{ prev = Prev }.

claim_wait(#lock{
  locks = Locks,
  term = Term,
  holder = Holder,
  held = HeldLocks,
  nodes = Nodes,
  deadlock_scope = DeadLockScope
}=Lock)->

  % Init deadlock check process
  Deadlock = check_deadlock(Locks, DeadLockScope, Holder ,Term, Nodes, HeldLocks ),

  wait_lock( Lock#lock{ deadlock = Deadlock }).


wait_lock(#lock{
  lock_ref = LockRef,
  holder = Holder,
  reply_to = ReplyTo,
  term = Term,
  shared = IsShared,
  deadlock = Deadlock,
  prev = Prev
} = Lock)->
  receive
    {'DOWN', _, process, Prev, _Reason}->
      ?LOGDEBUG("~p prev ~p is down",[ LockRef, Prev ]),
      case update_prev( Lock ) of
        undefined ->
          % The lock is free
          ?LOGDEBUG("~p no previous processes, get the lock",[ LockRef ]),
          locked( Lock ),
          wait_unlock( Lock );
        NewPrev ->
          % Keep waiting
          ?LOGDEBUG("~p update previous process ~p",[ LockRef, NewPrev ]),
          wait_lock( Lock#lock{ prev = NewPrev })
      end;
    {take_share,LockRef} when IsShared->
      ?LOGDEBUG("~p got shared lock",[ LockRef ]),
      locked( Lock ),
      wait_shared_lock( Lock#lock{ has_share = true } );
    {deadlock, Deadlock}->
      ?LOGDEBUG("~p hodler ~p deadlock",[Term,Holder]),
      ReplyTo ! {deadlock, self()},
      leave_queue( Lock );
    {timeout, ReplyTo}->
      ?LOGDEBUG("~p waiter ~p timeout",[Term,Holder]),
      % Stop deadlock checker
      catch Deadlock ! { stop, self() },
      % Holder is not waiting anymore, but I can't brake the queue
      leave_queue( Lock );
    {'DOWN', _, process, Holder, Reason}->
      ?LOGDEBUG("~p holder ~p died while waiting, reason ~p",[Term,Holder,Reason]),
      catch Deadlock ! { stop, self() },
      leave_queue( Lock );
    {'EXIT', ReplyTo, Reason}->
      ?LOGDEBUG("~p reply_to ~p died while waiting, reason ~p",[Term,ReplyTo,Reason]),
      catch Deadlock ! { stop, self() },
      leave_queue( Lock )
  end.

wait_shared_lock(#lock{
  term = Term,
  lock_ref = LockRef,
  reply_to = ReplyTo,
  holder = Holder,
  prev = Prev
}=Lock )->
  receive
    {'DOWN', _, process, Prev, _Reason}->
      ?LOGDEBUG("~p prev ~p is down",[ LockRef, Prev ]),
      case update_prev( Lock ) of
        undefined ->
          ?LOGDEBUG("~p no previous processes, wait unlock",[ LockRef ]),
          wait_unlock( Lock );
        NewPrev ->
          ?LOGDEBUG("~p update previous process ~p",[ LockRef, NewPrev ]),
          wait_shared_lock( Lock#lock{ prev = NewPrev })
      end;
    {wait_share, LockRef, NextLocker}->
      NextLocker ! {take_share,LockRef},
      wait_shared_lock( Lock );
    {timeout, ReplyTo}->
      ?LOGDEBUG("~p hodler ~p timeout after shared lock",[Term,Holder]),
      leave_queue( Lock );
    {'DOWN', _, process, Holder, Reason}->
      ?LOGDEBUG("~p holder ~p died having shared lock, reason ~p",[Term,Holder,Reason]),
      leave_queue( Lock )
  end.

update_prev(#lock{
  locks = Locks,
  lock_ref = LockRef,
  queue = Queue,
  shared = IsShared
})->
  case find_prev( Locks, LockRef, Queue ) of
    undefined ->
      undefined;
    Prev ->
      % An intermediate process has left the queue
      monitor(process, Prev),
      if
        IsShared ->
          Prev ! {wait_share, LockRef, self()};
        true ->
          ignore
      end,
      Prev
  end.

%-----------------Leave queue------------------------------------------
leave_queue(#lock{
  locks = Locks,
  lock_ref = LockRef,
  queue = MyQueue,
  prev = Prev,
  has_share = HasShare
}=Lock) when HasShare =/= true->
  ?LOGDEBUG("~p enter leave queue has share: ~p",[ LockRef, HasShare ]),
  receive
    {next, LockRef, _Next}->
      ?LOGDEBUG("~p next process has claimed, exit",[ LockRef ]),
      catch ets:delete(Locks,?queue(LockRef,MyQueue)),
      exit(normal);
    {'DOWN', _, process, Prev, _Reason}->
      ?LOGDEBUG("~p prev ~p is down",[ LockRef, Prev ]),
      case update_prev( Lock ) of
        undefined ->
          ?LOGDEBUG("~p no previous processes, unlock",[ LockRef ]),
          unlock(Lock);
        NewPrev ->
          ?LOGDEBUG("~p update previous process ~p",[ LockRef, NewPrev ]),
          leave_queue( Lock#lock{ prev = NewPrev })
      end;
    {take_share,LockRef}->
      leave_queue( Lock#lock{ has_share = true } )
  end;
leave_queue(#lock{
  locks = Locks,
  lock_ref = LockRef,
  queue = MyQueue,
  has_share = true,
  prev = Prev
}=Lock)->
  ?LOGDEBUG("~p enter leave queue has share: true",[ LockRef ]),
  receive
    {next, LockRef, _Next}->
      ?LOGDEBUG("~p next process has claimed, exit",[ LockRef ]),
      catch ets:delete(Locks,?queue(LockRef,MyQueue)),
      exit(normal);
    {'DOWN', _, process, Prev, _Reason}->
      ?LOGDEBUG("~p prev ~p is down",[ LockRef, Prev ]),
      case update_prev( Lock ) of
        undefined ->
          ?LOGDEBUG("~p no previous processes, unlock",[ LockRef ]),
          unlock(Lock);
        NewPrev ->
          ?LOGDEBUG("~p update previous process ~p",[ LockRef, NewPrev ]),
          leave_queue( Lock#lock{ prev = NewPrev })
      end;
    {wait_share, LockRef, NextLocker}->
      NextLocker ! {take_share,LockRef},
      leave_queue( Lock )
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

get_queue_pid(Locks, Queue)->
  case ets:lookup(Locks, Queue) of
    [{_, PID}]-> PID;
    _->
      % The queue isn't registered yet
      timer:sleep(5),
      get_queue_pid( Locks, Queue )
  end.

find_prev( Locks, LockRef, Queue )->
  case ets:prev(Locks, ?queue(LockRef, Queue)) of
    ?queue(LockRef, PrevQueue)->
      case ets:lookup(Locks, ?queue(LockRef, PrevQueue)) of
        [{_, PID}]->
          PID;
        _->
          find_prev( Locks, LockRef, PrevQueue )
      end;
    _->
      undefined
  end.
%-----------------------------------------------------------------------
% Deadlocks detection
%-----------------------------------------------------------------------
-record(deadlock,{ scope, holder, wait_term, held_locks, locker }).

check_deadlock(_Locks, _Scope, _Holder, _Term, _Nodes = [], _HeldLocks = []) ->
  can_not_have_deadlocks;
check_deadlock(Locks, Scope, Holder, Term, Nodes, HeldLocks)->
  Locker = self(),
  spawn(fun()->
    erlang:monitor(process, Locker),

    % Register the term that I wait
    WaitTerm = { Term, node() },
    pg:join( Scope, ?wait(WaitTerm), self() ),

    % Check for lock success on neighbour nodes
    NeighbourLocks = check_neighbours(Locks, Scope, Holder, Term, Nodes),

    % Subscribe to lock success on neighbour nodes
    AllHeldLocks = ordsets:from_list( HeldLocks ++ NeighbourLocks ),

    ?LOGDEBUG("~p start deadlock detect, held ~p, neighbour ~p, all ~p", [WaitTerm, HeldLocks, NeighbourLocks, AllHeldLocks ] ),

    InitState = lists:foldl(fun add_held_lock/2, #deadlock{
      scope = Scope,
      holder = Holder,
      wait_term = WaitTerm,
      held_locks = [],
      locker = Locker
    }, AllHeldLocks ),

    check_deadlock_loop( InitState )

  end).

check_deadlock_loop(#deadlock{
  wait_term = WaitTerm,
  scope = Scope,
  holder = Holder,
  locker = Locker,
  held_locks = HeldLocks
} = State )->
  receive
    {stop, Locker} ->
      ?LOGDEBUG("~p stop deadlock detect",[ WaitTerm ]),
      stop;
    {'DOWN', _Ref, process, Locker, _Reason} ->
      ?LOGDEBUG("~p down deadlock detect",[ WaitTerm ]),
      unlock;

    {check_deadlock, _From, ItsHolder, _ItsWaitTerm} when ItsHolder =:= Holder->
      % This is the request from the remote agent of the same lock. We are doing a common work
      % so we can't have deadlocks
      ?LOGDEBUG("~p neighbour check deadlock node ~p",[ WaitTerm, node( _From ) ]),
      check_deadlock_loop( State );
    {check_deadlock, From , _ItsHolder , ItsWaitTerm}->
      ?LOGDEBUG("~p check deadlock, wait term ~p",[ WaitTerm, ItsWaitTerm ]),
      case ordsets:is_element( ItsWaitTerm, HeldLocks ) of
        true ->
          % THE DEADLOCK DETECTED! I send it my held locks to decide who has to yield
          ?LOGDEBUG("~p deadlock detected, node ~p, wait term ~p",[ WaitTerm, node( From ), ItsWaitTerm ]),
          catch From ! {deadlock_detected, self() , HeldLocks},
          check_deadlock_loop( State );
        _->
          % As it holds the lock I'm waiting for so from now I'm also waiting for it's term

          ?LOGDEBUG("~p join wait term ~p",[ WaitTerm, ItsWaitTerm ]),
          pg:join( Scope, ?wait( ItsWaitTerm ), self() ),

          check_deadlock_loop( State )
      end;

    { add_held_lock, NeighbourTerm }->

      ?LOGDEBUG("~p add held lock ~p",[ WaitTerm, NeighbourTerm ]),

      % The neighbour has succeed to get the lock. From now I'm also holding this lock
      check_deadlock_loop( add_held_lock( NeighbourTerm, State ) );

    {_Ref, join, ?wait( _ItsWaitTerm ), OtherWaiters}->

      ?LOGDEBUG("~p wait my held lock ~p",[ WaitTerm, _ItsWaitTerm ]),

      % Someone is waiting for one of the locks that I'm holding
      send_check_deadlock( OtherWaiters, State ),

      check_deadlock_loop( State );

    {deadlock_detected, From ,Locks}->
      ?LOGDEBUG("~p deadlock detected request ~p",[ WaitTerm, Locks ]),
      if
        length( HeldLocks ) > length( Locks )->
          ?LOGDEBUG("~p deadlock opponent ~p yield",[ WaitTerm, From ]),
          % I have heavier held locks, the opponent has to yield
          catch From ! { yield },
          check_deadlock_loop( State );
        true ->
          HashHeld = erlang:phash2( HeldLocks ),
          HashLocks = erlang:phash2( Locks ),
          if
            HashHeld >= HashLocks ->
              catch From ! { yield },
              check_deadlock_loop( State );
            true ->
              % The opponent has heavier held locks, I has to yield
              Locker ! {deadlock, self()}
          end
      end;
    { yield }->
      ?LOGDEBUG("~p deadlock yield request",[ WaitTerm ]),
      Locker ! {deadlock, self()};
    _Other ->
      check_deadlock_loop( State )
  end.

add_held_lock( LockedTerm, #deadlock{
  scope = Scope,
  held_locks = HeldLocks
} = State)->

  % Subscribe to who is waiting for the term that I hold
  {_Ref, WhoIsWaiting} = pg:monitor( Scope, ?wait( LockedTerm ) ),

  send_check_deadlock( WhoIsWaiting, State ),

  State#deadlock{ held_locks = ordsets:add_element( LockedTerm, HeldLocks ) }.

send_check_deadlock(PIDs, #deadlock{
  wait_term = WaitTerm,
  holder = Holder
} )->
  Self = self(),
  [ catch P ! { check_deadlock, self(), Holder, WaitTerm } || P <- PIDs, P > Self ],
  ok.

%-----------------------------------------------------------------------
% Deadlocks cross-nodes API
%-----------------------------------------------------------------------
register_lock( Locks, DeadLockScope, Term, Holder )->

  GlobalTerm = {Term, node()},
  Group = ?holder( Holder, GlobalTerm ),

  ?LOGDEBUG("~p register lock, holder ~p",[ GlobalTerm, Holder ]),

  [ catch P ! { add_held_lock, GlobalTerm } || P <- pg:get_members( DeadLockScope, Group )],

  ets:insert(Locks, { Group , self() }),

  ok.

unregister_lock( Locks, DeadLockScope, Term, Holder )->

  GlobalTerm = {Term, node()},
  Group = ?holder( Holder, GlobalTerm ),

  ?LOGDEBUG("~p unregister lock, holder ~p",[ GlobalTerm, Holder ]),

  [ catch P ! { remove_held_lock, GlobalTerm } || P <- pg:get_members( DeadLockScope, Group )],

  catch ets:delete_object(Locks, { Group , self() }),

  ok.

registered_locks( Locks, Term, Holder )->
  [ Locker || {_, Locker} <- ets:lookup(Locks, ?holder(Holder, {Term, node()}))].

check_neighbours(Locks, Scope, Holder, Term, Nodes )->

  % Subscribe
  [ pg:join( Scope, ?holder( Holder, { Term, N } ), self() ) ||  N <- Nodes ],

  {Replies, _Rejects} = ecall:call_all_wait( Nodes, ?MODULE, registered_locks, [Locks, Term, Holder] ),

 [ {Term, Node} || { Node, Lockers } <- Replies, length( Lockers ) > 0 ].



test()->
  Nodes = ['n1@127.0.0.1', 'n2@127.0.0.1', 'n3@127.0.0.1'],
  Scope = test_scope,
  Term = test_term,
  spawn(fun()->test_loop(Nodes, Scope, Term) end).

test_loop( Nodes, Scope, Term )->
  ?LOGINFO("try lock"),
  try_lock( Nodes, Scope, Term ),
  timer:sleep( 1000 ),
  test_loop( Nodes, Scope, Term ).

try_lock( Nodes, Scope, Term )->
  ecall:call_all_wait( Nodes, ?MODULE, try_test_lock, [Nodes, Scope, Term] ).

try_test_lock( Nodes, Scope, Term )->
  case elock:lock( Scope, Term, _IsShared=false, _Timeout=infinity, Nodes ) of
    {ok, Unlock}->
      ?LOGINFO("locked!"),
      Unlock();
    {error, Error}->
      ?LOGINFO("error: ~p",[Error])
  end.


%%  elock:start_link(test_scope).
%%  {ok, U1} = elock:lock(test, t1, false, infinity ).
%%  {ok, U2} = elock:lock(test, t2, false, infinity ).
%%
%%  spawn(fun()-> elock:lock(test, t3, false, infinity ), io:format("t3 locked\r\n"), spawn(fun()->elock:lock(test, t4, false, infinity ), io:format("t4 locked\r\n"), io:format("t1 lock: ~p\r\n",[elock:lock(test, t1, false, infinity )]) end ), timer:sleep(1000), elock:lock(test, t4, false, infinity ), io:format("t4 locked2\r\n"), timer:sleep(10000)  end).
%%
%%  {ok, U3} = elock:lock(test, t3, false, infinity ).