
-module(elock_deadlock).

-include("elock.hrl").

%%=================================================================
%%	Internal API
%%=================================================================
-export([
  check_deadlock/6,
  register_lock/4,
  unregister_lock/4,
  registered_locks/3,
  wait_local/3
]).

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
    NeighbourLocks = check_neighbours(Locks, Scope, Holder, Locker, Term, Nodes),
    ?LOGDEBUG("~p neigbour locks: ~p",[ Term, NeighbourLocks ]),
    
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
          case lists:member(self(), pg:get_local_members(Scope, ?wait( ItsWaitTerm ))) of
            true ->
              ?LOGDEBUG("~p join wait term ~p skip, already exists",[ WaitTerm, ItsWaitTerm ]),
              ignore;
            _->
              % As it holds the lock I'm waiting for so from now I'm also waiting for it's term
              ?LOGDEBUG("~p join wait term ~p",[ WaitTerm, ItsWaitTerm ]),
              pg:join( Scope, ?wait( ItsWaitTerm ), self() )
          end,
          check_deadlock_loop( State )
      end;

    {add_held_lock, NeighbourTerm }->

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
              ?LOGDEBUG("~p deadlock opponent ~p yield",[ WaitTerm, From ]),
              catch From ! { yield },
              check_deadlock_loop( State );
            true ->
              % The opponent has heavier held locks, I has to yield
              ?LOGDEBUG("~p have to yield to ~p",[ WaitTerm, From ]),
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
  [ catch P ! {check_deadlock, self(), Holder, WaitTerm } || P <- PIDs, P =/= Self ],
  ok.

%-----------------------------------------------------------------------
% Deadlocks cross-nodes API
%-----------------------------------------------------------------------
register_lock( Locks, DeadLockScope, Term, Holder )->

  GlobalTerm = {Term, node()},
  Group = ?holder( Holder, GlobalTerm ),

  ?LOGDEBUG("~p register lock, holder ~p",[ GlobalTerm, Holder ]),
  ets:insert(Locks, { Group , self() }),
  [ catch P ! {add_held_lock, GlobalTerm } || P <- pg:get_members( DeadLockScope, Group )],

  ok.

unregister_lock( Locks, DeadLockScope, Term, Holder )->

  GlobalTerm = {Term, node()},
  Group = ?holder( Holder, GlobalTerm ),

  ?LOGDEBUG("~p unregister lock, holder ~p",[ GlobalTerm, Holder ]),
  [ catch P ! {remove_held_lock, GlobalTerm } || P <- pg:get_members( DeadLockScope, Group )],
  catch ets:delete_object(Locks, { Group , self() }),

  ok.

registered_locks( Locks, Term, Holder )->
  [ Locker || {_, Locker} <- ets:lookup(Locks, ?holder(Holder, {Term, node()}))].

check_neighbours(Locks, Scope, Holder, Locker, Term, Nodes )->

  % Subscribe
  Monitors =
    [ begin
        HolderGroup = ?holder( Holder, { Term, N } ),
        pg:join( Scope, HolderGroup, self() ),
        spawn_monitor(N, ?MODULE, wait_local, [Scope, HolderGroup, self() ])
      end ||  N <- Nodes ],
  wait_consistency( Monitors, Locker ),
  ?LOGDEBUG("~p holder locks are registered on ~p",[Term, Nodes]),

  {Replies, _Rejects} = ecall:call_all_wait( Nodes, ?MODULE, registered_locks, [Locks, Term, Holder] ),

 [ {Term, Node} || { Node, Lockers } <- Replies, length( Lockers ) > 0 ].

wait_local(Scope, Group, Member)->
  erlang:monitor(process, Member),
  {Ref, WhoIsWaiting} = pg:monitor(Scope, Group),
  case lists:member(Member, WhoIsWaiting) of
    true->
      exit(normal);
    _->
      wait_local(Ref, Member)
  end.

wait_local(Ref, Member)->
  receive
    {Ref, join, _, WhoIsWaiting}->
      case lists:member(Member, WhoIsWaiting) of
        true->
          exit(normal);
        _->
          wait_local(Ref, Member)
      end;
    {'DOWN', _Ref, process, Member, Reason}->
      exit(Reason);
    _->
      wait_local(Ref, Member)
  end.

wait_consistency([{PID, Ref}|Rest], Locker)->
  receive
    {'DOWN', Ref, process, PID, _Reason}->
      wait_consistency( Rest, Locker );
    {'DOWN', _Ref, process, Locker, Reason}->
      exit(Reason);
    {stop, Locker}->
      exit(stop)
  end;
wait_consistency([], _Locker)->
  ok.
