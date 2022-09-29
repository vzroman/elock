
-module(elock).

-behaviour(gen_statem).

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

%%=================================================================
%%	state machine callbacks
%%=================================================================
-export([
  callback_mode/0,
  start_link/2,
  init/1,
  code_change/3,
  handle_event/4,
  terminate/3
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

  {ok,Locker} = gen_statem:start_link(?MODULE, [_ReplyTo = self(),Locks, Holder, Term, IsShared, Nodes], []),

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

-record(lock,{locks, graph, term, reply_to, holder, shared, held, nodes, lock_ref, queue, deadlock}).

callback_mode() ->
  [
    handle_event_function
  ].

init([ReplyTo, Locks, Holder, Term, IsShared, Nodes])->

  process_flag(trap_exit,true),

  HeldLocks =
    [ T || {_,T,S} <- ets:lookup(?graph(Locks),?holder(Holder)), (S=:=IsShared) or (S=:=false)],
  case lists:member(Term,HeldLocks) of
    true->
      LockRef = make_ref(),
      ReplyTo ! {locked, self(), LockRef },
      {ok, stop, #lock{}, [ {state_timeout, 0, enter } ]};
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

      {ok, enqueue, Lock, [ {state_timeout, 0, enter } ]}
  end.

handle_event(state_timeout, enter, enqueue, #lock{
  locks = Locks,
  term = Term,
  holder = Holder,
  shared = IsShared
} = Lock) ->

  Locker = self(),

  case ets:update_counter(Locks, ?lock(Term), {3,1}, {?lock(Term),0,0}) of % try to set lock
    1-> %------------------locked------------------
      ?LOGDEBUG("~p set local lock: holder ~p, locker ~p",[ Term, Holder, Locker ]),

      LockRef = make_ref(),
      ets:update_element(Locks, ?lock(Term), {2,LockRef}),

      {next_state, locked, Lock#lock{ lock_ref = LockRef, queue = 1 }, [ {state_timeout, 0, enter } ] };

    MyQueue-> %------------queued-------------------

      LockRef = get_lock_ref(Locks,?lock(Term)),
      ets:insert(Locks,{?queue(LockRef,MyQueue), Locker}),

      Lock1 = #lock{ lock_ref=LockRef, queue= MyQueue},

      ?LOGDEBUG("~p lock queued: holder ~p, locker ~p, queue ~p",[Term, Holder, Locker, MyQueue]),

      NextState =
        if
          IsShared ->
            request_share(Locks, LockRef, MyQueue-1 );
          true ->
            % Need exclusive?
            wait_lock
        end,
      {next_state, NextState, Lock1, [ {state_timeout, 0, enter } ] }
  end;

handle_event(state_timeout, enter, locked, #lock{
  term = Term,
  graph = Graph,
  reply_to = ReplyTo,
  holder = Holder,
  shared = IsShared,
  lock_ref = LockRef,
  held = HeldLocks,
  nodes = Nodes
} = Lock) ->

  ets:insert(Graph,{?holder(Holder),Term,IsShared}),

  % Locked
  ReplyTo ! {locked, self(), LockRef},

  % Init cross nodes deadlock check process
  DeadLock = check_deadlock(Graph, [{Term,N} || N <- Nodes], [Term|HeldLocks]),

  {next_state, wait_unlock, Lock};

handle_event(info, {unlock, LockRef}, wait_unlock, #lock{
  holder = Holder,
  term = Term,
  lock_ref = LockRef
} = Lock) ->

  ?LOGDEBUG("~p try unlock, holder ~p, locker ~p",[ Term, Holder, self() ]),

  {next_state, unlock, Lock, [ {state_timeout, 0, enter } ] };

handle_event(info, {timeout, Holder}, wait_unlock, #lock{
  holder = Holder,
  term = Term
} = Lock) ->

  ?LOGDEBUG("~p holder ~p timeout, locker ~p unlock",[ Term, Holder, self() ]),

  {next_state, unlock, Lock, [ {state_timeout, 0, enter } ] };

handle_event(info, {'DOWN', _Ref, process, Holder, Reason}, wait_unlock, #lock{
  holder = Holder,
  term = Term
} = Lock) ->

  ?LOGDEBUG("~p holder ~p down, reason ~p locker ~p unlock",[ Term, Holder, Reason, self() ]),

  {next_state, unlock, Lock, [ {state_timeout, 0, enter } ] };


handle_event(state_timeout, enter, unlock, #lock{
  locks = Locks,
  lock_ref = LockRef,
  queue = MyQueue,
  holder = Holder,
  term = Term,
  shared = IsShared
} = Lock) ->

  catch ets:delete_object(?graph(Locks),{?holder(Holder),Term,IsShared}),

  % Delete my queue
  catch ets:delete(Locks,?queue(LockRef,MyQueue)),

  % try unlock
  catch ets:delete_object(Locks, {?lock(Term), LockRef, MyQueue}),

  % check unlocked
  case ets:lookup(Locks, ?lock(Term)) of
    []->  % unlocked, nobody is waiting
      ?LOGDEBUG("~p unlocked");
    [_]-> % not unlocked there is a queue
      % who is the next, he must be there, iterate until
      NextLocker = get_next_locker(Locks,LockRef,MyQueue+1),
      NextLocker ! {take_it, LockRef}
  end,

  {next_state, stop, Lock, [{state_timeout, 0, enter }]};



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

request_share(Locks, LockRef, Queue )->
  case ets:lookup(Locks,?queue(LockRef,Queue)) of
    [{_, Locker}]->
      Locker ! {wait_share, LockRef, self()},
      wait_lock;
    _->
      % The locker either not registered yet or already deleted it's queue
      receive
        {take_it, LockRef}->
          % The locker removed it's queue. The lock is mine
          wait_unlock
      after
        5->
          request_share( Locks, LockRef, Queue )
      end
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