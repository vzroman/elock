
%%=================================================================
%%  One manager process per locked Term.
%%
%%  The lock itself is a single entry in the Scope ETS table:
%%
%%      { ?lock(Term), ManagerPID, LastTicket }
%%
%%  Taking a ticket with ets:update_counter/4 on the third element
%%  is the only synchronization point between the clients:
%%
%%    * ticket 1 - the lock was free. The client is its first holder
%%      and spawns the manager, which writes its own PID into the
%%      entry.
%%    * ticket N - the lock is busy. The client sends its request to
%%      the manager and waits for the verdict: #locked{}, #deadlock{},
%%      #timeout{} or #retry{}.
%%
%%  The ticket is also the ordering token. The counter serializes the
%%  concurrent clients, but their messages may reach the manager in
%%  any order, therefore the manager replays them strictly by the
%%  ticket number (see handle_request/2). The queue is FIFO by the
%%  moment of the ets:update_counter/4 call, not by the moment of the
%%  message delivery.
%%
%%  Everything else belongs to the manager: it grants shared and
%%  exclusive locks, queues the requests it can not grant, monitors
%%  the clients, owns the timeout timers and the deadlock checkers.
%%  It exits as soon as the lock entry is removed from ETS - the next
%%  client will start a new manager.
%%=================================================================
-module(elock_manager).

-include("elock.hrl").

%%=================================================================
%%	API
%%=================================================================
-export([
  lock/1,
  unlock/1
]).

%%=================================================================
%%  Client <-> manager protocol
%%=================================================================
%%-----------------------------------------------------------------
%%  The verdict on a queued request. #retry{} means the manager has
%%  already passed the ticket by, the client has to take a new one
%%-----------------------------------------------------------------
-record(locked,{
  ref
}).
-record(deadlock,{
  ref
}).
-record(timeout,{
  ref
}).
-record(retry,{
  ref
}).

%%-----------------------------------------------------------------
%%  The client's handle to the acquired lock
%%-----------------------------------------------------------------
-record(unlock,{
  manager,
  ref
}).

%%=================================================================
%%  Client side
%%=================================================================
lock(#request{
  ref = Ref,
  scope = Scope,
  term = Term
} = Request
)->
  LockKey = ?lock(Term),
  case ets:update_counter(Scope, LockKey, {3,1}, {LockKey,0,0}) of % try to set lock
    1->
      %------------------locked------------------
      % The lock was free. This client is its first holder and starts
      % the manager for those who queue up behind it
      Manager = start_manager(Request),
      {ok, #unlock{
        manager = Manager,
        ref = Ref
      }};

    RequestQueue->
      %------------enqueued-------------------
      % Somebody is ahead. The ticket tells the manager where the
      % request stands among the other clients
      case get_manager(Scope, LockKey, RequestQueue) of
        Manager when is_pid(Manager) ->
          % The verdict lives in the manager's mailbox. If the manager
          % exits before it has replied then the request goes with the
          % mailbox and nobody will ever answer - monitor it
          MonitorRef = erlang:monitor(process, Manager),
          Manager ! Request#request{ queue = RequestQueue, reply_to = self() },
          Verdict =
            receive
              #locked{ref = Ref}->
                {ok, #unlock{
                  manager = Manager,
                  ref = Ref
                }};
              #deadlock{ref = Ref}->
                {error, deadlock};
              #timeout{ref = Ref}->
                {error, timeout};
              #retry{ref = Ref}->
                % The ticket is not valid any longer, start over
                retry;
              {'DOWN', MonitorRef, process, Manager, _Reason}->
                % The manager is gone and the request went with it.
                % The lock entry is removed before the manager exits,
                % so the new ticket starts the next round
                ets:match_delete(Scope, {LockKey, Manager, '_'}),
                retry
            end,
          erlang:demonitor(MonitorRef, [flush]),
          case Verdict of
            retry ->
              lock(Request);
            _->
              Verdict
          end;
        _->
          lock(Request)
      end
  end.

unlock(#unlock{manager = Manager}=Unlock)->
  catch Manager ! Unlock,
  ok.

%%-----------------------------------------------------------------
%%  Client side utilities
%%-----------------------------------------------------------------
get_manager(Scope, LockKey, MyQueue)->
  case ets:lookup(Scope, LockKey) of
    [ { _Lock, Manager, Queue } ] when is_pid(Manager)->
      if
        Queue >= MyQueue ->
          Manager;
        true ->
          retry
      end;
    []->
      retry;
    _->
      % The manager has not registered itself yet, wait
      receive after 1 -> ok end,
      get_manager(Scope, LockKey, MyQueue)
  end.

% Every client of the Term goes through the manager, hence the
% priority. The off heap mailbox keeps the bursts of the incoming
% requests out of the manager's garbage collection
start_manager(Request)->
  spawn_opt(fun()->init(Request) end, [
    {priority, high},
    {message_queue_data, off_heap}
  ]).

%%=================================================================
%%  Manager
%%=================================================================
%% How long the manager waits for a ticket that has not arrived yet
%% before it steps over it (see handle_postpone_timeout/2)
-define(POSTPONE_TIMEOUT, 100).

-record(state,{
  holders,          % #{ Ref => {Shared, ClientPID} } of the holding requests
  queue,            % gb_sets of {Ticket, Ref}, ordered by the ticket
  requests,         % #{ Ref => #req{} }, both holders and waiters
  clients,          % #{ ClientPID => #client{} }
  scope,            % the ETS table of the locks
  lock_key,         % ?lock(Term)
  can_share,        % the lock is shared, i.e. every holder is shared
  deadlock_scope,   % pg scope of the deadlock checkers
  barging,          % the pending upgrade request, it is out of the queue
  last,             % the last ticket taken into the queue
  postponed,        % the requests that came before their turn
  postpone_timer    % set while waiting for a missing ticket
}).

-record(req,{
  client,           % the process that asked for the lock
  ref,              % unique reference of the request
  queue,            % the ticket, the key of the request in #state.queue
  reply_to,         % the process waiting for the verdict
  shared,           % the requested lock type
  has_lock,         % true - holds the lock, false - waits in the queue
  deadlock,         % the deadlock checker, only while waiting
  timer             % the timeout timer, only while waiting
}).

-record(client,{
  requests,         % #{ Ref => Shared } of all the requests of the client
  monitor_ref       % one monitor per client, while it has requests
}).

% The manager is spawned by the winner of the ets:update_counter/4
% race, therefore it starts with the lock already held
init(#request{
  ref = Ref,
  scope = Scope,
  term = Term,
  client = Client,
  reply_to = ReplyTo,
  shared = Shared
})->

  LockKey = ?lock(Term),
  % From now on the queued clients can find the manager
  ets:update_element(Scope, LockKey, {2,self()}),

  State = #state{
    holders = #{ Ref => {Shared, Client} },
    queue = gb_sets:empty(),
    requests = #{
      Ref => #req{
        client = Client,
        ref = Ref,
        % The manager is started by the winner of the ticket 1
        queue = 1,
        reply_to = ReplyTo,
        shared = Shared,
        has_lock = true,
        deadlock = undefined,
        timer = undefined
      }
    },
    clients = #{
      Client => #client{
        requests = #{ Ref => Shared },
        monitor_ref = erlang:monitor(process, Client)
      }
    },
    scope = Scope,
    lock_key = LockKey,
    can_share = Shared,
    deadlock_scope = ?deadlock_scope(Scope),
    barging = undefined,
    last = 1,
    postponed = [],
    postpone_timer = undefined
  },

  loop(State).


loop(State0)->
  State =
    receive
      #unlock{ref = Ref}->
        handle_unlock(Ref, State0);
      #request{} = Request->
        handle_request(Request, State0);
      {timeout, _TimerRef, {timeout, Ref}}->
        handle_timeout(Ref, State0);
      #deadlock{ref = Ref}->
        handle_deadlock(Ref, State0);
      {'DOWN', _Ref, process, ClientPID, _Reason}->
        handle_down(ClientPID, State0);
      {timeout, TimerRef, postpone_timeout}->
        handle_postpone_timeout(TimerRef, State0);
      Unexpected->
        ?LOGWARNING("unexpected message received: ~p",[Unexpected]),
        State0
    end,
  loop( State ).

%%=================================================================
%%  New requests
%%
%%  The requests are taken into the queue strictly in the order of
%%  their tickets. The one that comes too early is postponed until
%%  its predecessors arrive, but no longer than the postpone timeout
%%  - their clients may be descheduled or gone
%%=================================================================
%%-----------------------------------------------------------------
%%  The awaited ticket
%%-----------------------------------------------------------------
handle_request(
    #request{
      queue = Queue
    } = Request,
    #state{
      last = Last
    } = State0
) when (Last+1) =:= Queue->

  State = add_request(Request, State0),

  % The gap is closed here, but another one may be left behind the
  % postponed requests - handle_postponed/1 owns the timer and
  % decides whether it is still needed
  handle_postponed(State#state{
    last = Queue
  });

%%-----------------------------------------------------------------
%%  A ticket from ahead - the requests in between are still on their
%%  way. Wait for them to keep the queue in the ticket order
%%  the guard:
%%  * the ticket is not the awaited one (the clause above)
%%  * it is ahead of the last taken (Queue > Last), i.e. there is a gap
%%-----------------------------------------------------------------
handle_request(
    #request{
      queue = Queue
    } = Request,
    #state{
      last = Last,
      postponed = Postponed
    } = State
) when Queue > Last->

  % #request.queue is the first field of the record, therefore the
  % ordset keeps the postponed requests sorted by their tickets
  arm_postpone_timer(State#state{
    postponed = ordsets:add_element(Request, Postponed)
  });

%%-----------------------------------------------------------------
%%  The ticket has already been passed by (see
%%  handle_postpone_timeout/1). The request can not take its place
%%  in the queue any more, let the client take a new ticket
%%  the guard:
%%  * the ticket is neither the awaited one nor ahead (the clauses
%%    above), hence it is behind the last taken
%%-----------------------------------------------------------------
handle_request(
    #request{
      ref = Ref,
      reply_to = ReplyTo
    },
    State
)->
  catch ReplyTo ! #retry{ref = Ref},
  State.

%%-----------------------------------------------------------------
%%  Take in the postponed requests while they follow each other
%%  the guard:
%%  * the first postponed ticket is the awaited one (Last + 1 =:= Queue)
%%-----------------------------------------------------------------
handle_postponed(#state{
  postponed = [#request{
    queue = Queue
  } = Request|Rest],
  last = Last
} = State0)
  when Last+1 =:= Queue->

  State = add_request(Request, State0),

  handle_postponed(State#state{
    postponed = Rest,
    last = Queue
  });

%%-----------------------------------------------------------------
%%  There is still a gap ahead of the postponed requests - wait for
%%  it to be filled
%%  the guard:
%%  * the first postponed ticket is not the awaited one (the clause
%%    above)
%%  * it is ahead of the last taken
%%-----------------------------------------------------------------
handle_postponed(#state{
  postponed = [#request{
    queue = Queue
  }|_],
  last = Last
} = State)
  when Queue > Last->
  arm_postpone_timer(State);

handle_postponed(#state{
  postponed = [#request{
    ref = Ref,
    reply_to = ReplyTo
  }|Rest]
} = State)->
  ReplyTo ! #retry{ref = Ref},
  handle_postponed(State#state{
    postponed = Rest
  });

%%-----------------------------------------------------------------
%%  Nothing is postponed
%%-----------------------------------------------------------------
handle_postponed(State)->
  cancel_postpone_timer(State).

%%-----------------------------------------------------------------
%%  The missing requests did not come in time. Step over their
%%  tickets - if they come later they will be told to retry
%%-----------------------------------------------------------------
handle_postpone_timeout(
    TimerRef,
    #state{
      postpone_timer = PostponeTimerRef
    } =State
) when TimerRef =/= PostponeTimerRef ->
  State;
handle_postpone_timeout(
    _TimerRef,
    #state{
      postponed = [#request{
        queue = Queue
      } = Request|Rest]
    } =State0)->
  State = add_request(Request, postpone_timer_fired(State0)),
  handle_postponed(State#state{
    postponed = Rest,
    last = Queue
  });
handle_postpone_timeout(
    _TimerRef,
    #state{
      holders = Holders,
      queue = Queue,
      postponed = [],
      last = Last
    } =State0) when map_size(Holders) =:= 0->
  State = postpone_timer_fired(State0),
  case gb_sets:is_empty(Queue) of
    true->
      try_unlock(State#state{
        last = Last + 1
      });
    false->
      State
  end;
handle_postpone_timeout(_TimerRef, State)->
  postpone_timer_fired(State).

%%-----------------------------------------------------------------
%%  The postpone timer
%%
%%  #state.postpone_timer is written here and nowhere else. One timer
%%  serves every postponed request: it is armed by the first one that
%%  has to wait and it keeps running - never restarted from zero -
%%  until no ticket is missing any more
%%-----------------------------------------------------------------
arm_postpone_timer(#state{postpone_timer = Timer} = State) when is_reference(Timer)->
  State;
arm_postpone_timer(State)->
  State#state{
    postpone_timer = erlang:start_timer(?POSTPONE_TIMEOUT, self(), postpone_timeout)
  }.

cancel_postpone_timer(#state{postpone_timer = Timer} = State) when is_reference(Timer)->
  erlang:cancel_timer(Timer),
  State#state{
    postpone_timer = undefined
  };
cancel_postpone_timer(State)->
  State.

%% A fired timer is gone, but a cancelled one may have fired already -
%% hence the reference guard of handle_postpone_timeout/2
postpone_timer_fired(State)->
  State#state{
    postpone_timer = undefined
  }.

%%=================================================================
%%  Leaving requests
%%
%%  A request leaves the manager when the client unlocks, when the
%%  timeout fires, when a deadlock is detected or when the client
%%  itself is gone
%%=================================================================
%%-----------------------------------------------------------------
%%  The last known request - Term UNLOCK
%%  the guard:
%%  * the unlocking request is the only holder
%%  * nothing is waiting in the queue
%%  * there is no pending barging request
%%  i.e. nobody needs the Term any more
%%-----------------------------------------------------------------
handle_unlock(
    Ref,
    #state{
      holders = Holders,
      queue = Queue,
      barging = undefined,
      requests = Requests,
      clients = Clients
    } = State0
) when map_size(Holders) =:= 1, is_map_key(Ref, Holders)->

  case gb_sets:is_empty(Queue) of
    true->
      State = try_unlock(State0),
      % TODO. Unregister lock

      % If here, then it's not unlocked: a new client has taken a ticket
      % and the state is already reset for it. The monitor of the leaving
      % client is not in the reset state, drop it explicitly
      #req{client = Client} = maps:get(Ref, Requests),
      #client{monitor_ref = MonRef} =  maps:get(Client, Clients),
      erlang:demonitor(MonRef),

      State;
    false->
      % Somebody is waiting for the Term
      leave_lock(Ref, State0)
  end;

%%-----------------------------------------------------------------
%%  One of the requests - the lock itself stays
%%  the guard:
%%  * the Term is still needed: there are other holders, or a queue,
%%    or a pending barging request (the clause above)
%%-----------------------------------------------------------------
handle_unlock(Ref, State)->
  leave_lock(Ref, State).

leave_lock(
    Ref,
    #state{
      requests = Requests
    } = State
)->
  case Requests of
    #{ Ref := Req}->
      remove_request(Req, State);
    _->
      % unexpected request ref
      State
  end.

%%-----------------------------------------------------------------
%%  A waiting request has run out of its timeout. The timer of a
%%  request that has got the lock is cancelled, but it may have
%%  fired just before - such a message is ignored
%%-----------------------------------------------------------------
handle_timeout(
    Ref,
    #state{
      requests = Requests
    } = State0
)->
  case Requests of
    #{Ref := #req{ has_lock = false, reply_to = ReplyTo } = Req}->
      catch ReplyTo ! #timeout{ref = Ref},
      % The timer has just fired, there is nothing to cancel. Cancelling
      % it here would cost a round trip to the scheduler that owns it -
      % a fired timer is no longer in the manager's own timer tree
      State = dequeue(Req#req{timer = undefined}, State0),
      next(State);
    _->
      % unexpected request ref
      State0
  end.

%%-----------------------------------------------------------------
%%  The deadlock checker of a waiting request reports a cycle. The
%%  checker is stopped as soon as the request gets the lock, so only
%%  a waiting request can be reported
%%-----------------------------------------------------------------
handle_deadlock(
    Ref,
    #state{
      requests = Requests
    } = State0
)->
  case Requests of
    #{Ref := Req = #req{
      has_lock = false,
      reply_to = ReplyTo}
    }->
      catch ReplyTo ! #deadlock{ ref = Ref },
      State = dequeue(Req, State0),
      next(State);
    _->
      % unexpected request ref
      State0
  end.

%%-----------------------------------------------------------------
%%  The client is gone - drop all its requests, held and queued
%%-----------------------------------------------------------------
handle_down(
    ClientPID,
    #state{
      clients = Clients
    } = State
)->
  case Clients of
    #{ ClientPID := #client{requests = ClientRequests}}->
      maps:fold(
        fun(Ref, _Shared, #state{requests = RequestsAcc} = StateAcc)->
          Req = #req{
            reply_to = ReplyTo
          } = maps:get(Ref, RequestsAcc),
          if
            % For a multi node lock the verdict is awaited not by the
            % client itself but by a worker on its behalf. There is
            % nobody to serve any more
            is_pid(ReplyTo), ReplyTo =/= ClientPID ->
              exit(ReplyTo, kill);
            true ->
              ignore
          end,
          remove_request(Req, StateAcc)
        end,
        State,
        ClientRequests
      );
    _->
      % unexpected PID
      State
  end.

%%=================================================================
%%  The queue
%%=================================================================
%%-----------------------------------------------------------------
%%  Add a shared request to a shared lock
%%  the guard:
%%  * the request is for shared lock
%%  * the actual lock is shared
%%  * there is no pending barging request
%%  and, in the body, that no request is queued. The last two keep
%%  the queue fair: a newcomer does not overtake the exclusive
%%  requests that are already waiting
%%-----------------------------------------------------------------
add_request(
    #request{
      shared = true
    } = Request,
    #state{
      can_share = true,
      queue = Queue,
      barging = undefined
    } = State
)->
  case gb_sets:is_empty(Queue) of
    true->
      get_lock(Request, State);
    false->
      add_busy_request(Request, State)
  end;

add_request(Request, State)->
  add_busy_request(Request, State).

%%-----------------------------------------------------------------
%%  Nobody is holding the lock - take it, shared or not
%%  the guard:
%%  * there are no holders
%%-----------------------------------------------------------------
add_busy_request(
    Request,
    #state{
      holders = Holders
    } = State
) when map_size(Holders) =:= 0->
  get_lock(Request, State);

%%-----------------------------------------------------------------
%%  The lock is busy - the request goes to the tail of the queue.
%%  A client that is already among the holders may barge in instead
%%  the guard:
%%  * the lock is held and the request can not join it: it is
%%    exclusive, or the lock is exclusive, or somebody is already
%%    waiting for it (the clauses above)
%%-----------------------------------------------------------------
add_busy_request(
    #request{
      client = ClientPID
    } = Request,
    #state{
      clients = Clients0
    } = State
)->
  if
    is_map_key(ClientPID, Clients0)->
      % The client already holds the lock
      try_barging(Request, State);
    true ->
      enqueue(Request, State)
  end.

%%-----------------------------------------------------------------
%%  Remove a queued request
%%-----------------------------------------------------------------
remove_request(
    #req{has_lock = false} = Req,
    State0
)->
  State = dequeue(Req, State0),
  next(State);

%%-----------------------------------------------------------------
%%  Remove a holding request
%%-----------------------------------------------------------------
remove_request(
    #req{has_lock = true} = Req,
    State0
)->
  State = unlocked(Req, State0),
  next(State).

%%-----------------------------------------------------------------
%%  The #req{} of a new request. The ticket is what keys it in
%%  #state.queue, has_lock is turned on by locked/2 when the request
%%  gets the lock
%%-----------------------------------------------------------------
new_req(#request{
  client = ClientPID,
  ref = Ref,
  queue = Ticket,
  reply_to = ReplyTo,
  shared = Shared
})->
  #req{
    client = ClientPID,
    ref = Ref,
    queue = Ticket,
    reply_to = ReplyTo,
    shared = Shared,
    has_lock = false
  }.

%%-----------------------------------------------------------------
%%  An enqueued barging request is always exclusive - it is the
%%  upgrade the client is waiting for. It never enters #state.queue,
%%  the ticket is kept only to keep every #req{} alike
%%-----------------------------------------------------------------
new_barging_req(Request)->
  (new_req(Request))#req{
    shared = false
  }.

%%-----------------------------------------------------------------
%%  The client starts waiting here: the timeout timer and the
%%  deadlock checker live as long as the request is in the queue
%%-----------------------------------------------------------------
enqueue(
    #request{
      client = ClientPID,
      ref = Ref,
      queue = Ticket,
      shared = Shared
    } = Request,
    #state{
      queue = Queue0,
      requests = Requests0,
      clients = Clients0
    } = State
)->

  Req = start_waiting(Request, State, new_req(Request)),
  Requests = Requests0#{
    Ref => Req
  },
  Clients = add_client_request(ClientPID, Ref, Shared, Clients0),
  % The tickets are unique and grow with the queue, hence the set is
  % ordered by the arrival and the head of the queue is its smallest
  % element
  Queue = gb_sets:insert({Ticket, Ref}, Queue0),

  State#state{
    queue = Queue,
    requests = Requests,
    clients = Clients
  }.

%%-----------------------------------------------------------------
%%  The upgrade request waits out of the queue - it is served as
%%  soon as its client is the only holder left (see next/1). Only
%%  one barging request at a time (see try_barging/2)
%%-----------------------------------------------------------------
enqueue_barging(
    #request{
      client = ClientPID,
      ref = Ref
    } = Request,
    #state{
      requests = Requests0,
      clients = Clients0
    } =State
)->
  Req = start_waiting(Request, State, new_barging_req(Request)),
  Requests = Requests0#{
    Ref => Req
  },
  Clients = add_client_request(ClientPID, Ref, _Shared = false, Clients0),

  State#state{
    requests = Requests,
    clients = Clients,
    barging = Request
  }.

%%-----------------------------------------------------------------
%%  A waiting request gives up: timeout, deadlock or a dead client.
%%  The pending barging request
%%  the guard:
%%  * the request is the pending barging one
%%-----------------------------------------------------------------
dequeue(
    #req{
      client = ClientPID,
      ref = Ref
    } = Req,
    #state{
      barging = #request{
        ref = Ref
      },
      requests = Requests0,
      clients = Clients0
    } = State
)->
  % Dequeue barging request
  stop_waiting(Req),
  Requests = maps:remove(Ref, Requests0),
  Clients = remove_client_request(ClientPID, Ref, Clients0),

  State#state{
    requests = Requests,
    clients = Clients,
    barging = undefined
  };

%%-----------------------------------------------------------------
%%  A request from the queue
%%  the guard:
%%  * the request is not the pending barging one (the clause above),
%%    hence it stands in the queue
%%-----------------------------------------------------------------
dequeue(
    #req{
      client = ClientPID,
      ref = Ref,
      queue = Ticket
    } = Req,
    #state{
      requests = Requests0,
      clients = Clients0,
      queue = Queue0
    } = State
)->
  stop_waiting(Req),

  State#state{
    queue = gb_sets:delete_any({Ticket, Ref}, Queue0),
    requests = maps:remove(Ref, Requests0),
    clients = remove_client_request(ClientPID, Ref, Clients0)
  }.

%%-----------------------------------------------------------------
%%  Grant the lock to a request that has never been queued
%%-----------------------------------------------------------------
get_lock(
    #request{
      client = ClientPID,
      ref = Ref,
      shared = Shared
    } = Request,
    #state{
      clients = Clients0
    } = State0
)->
  State = locked(new_req(Request), State0),
  Clients = add_client_request(ClientPID, Ref, Shared, Clients0),

  State#state{
    clients = Clients
  }.

%%-----------------------------------------------------------------
%%  The request becomes a holder: the client is notified, the
%%  waiting attributes are dropped
%%-----------------------------------------------------------------
locked(
    #req{
      client = ClientPID,
      ref = Ref,
      queue = Ticket,
      reply_to = ReplyTo,
      shared = Shared
    } = Req0,
    #state{
      holders = Holders0,
      queue = Queue0,
      requests = Requests0,
      can_share = CanShare0
    } = State)->

  ReplyTo ! #locked{ref = Ref},
  Req = stop_waiting(Req0#req{
    has_lock = true,
    reply_to = undefined
  }),

  % TODO. Register lock

  Requests = Requests0#{
    Ref => Req
  },
  Holders = Holders0#{ Ref => {Shared, ClientPID} },
  % locked/2 is also reached by requests that never stood in the
  % queue - get_lock/2 and the barging clause of next/1
  Queue = gb_sets:delete_any({Ticket, Ref}, Queue0),

  % If the actual lock is inclusive, then it's
  % a barging request, it doesn't downgrade the lock
  CanShare = Shared andalso CanShare0,

  State#state{
    holders = Holders,
    queue = Queue,
    requests = Requests,
    can_share = CanShare
  }.

%%-----------------------------------------------------------------
%%  A holder releases the lock
%%-----------------------------------------------------------------
unlocked(
    #req{
      client = ClientPID,
      ref = Ref,
      shared = Shared
    },
    #state{
      clients = Clients0,
      holders = Holders0,
      requests = Requests0,
      can_share = CanShare0
    } = State
)->

  % TODO. Unregister lock

  Clients = remove_client_request(ClientPID, Ref, Clients0),
  Holders = maps:remove(Ref, Holders0),
  Requests = maps:remove(Ref, Requests0),

  % If the lock was already shared or the removed request was shared
  % then it can not change the state of the lock.
  % Otherwise we need to check if there are any exclusive requests
  % among the rest of the holders
  CanShare =
    if
      CanShare0; Shared ->
        CanShare0;
      true ->
        can_share(Holders)
    end,

  State#state{
    holders = Holders,
    requests = Requests,
    clients = Clients,
    can_share = CanShare
  }.

%%-----------------------------------------------------------------
%%  The client is already holding the lock and requested
%%  it again.
%%-----------------------------------------------------------------
try_barging(
    #request{
      ref = Ref,
      client = ClientPID,
      shared = Shared,
      reply_to = ReplyTo
    } = Request,
    #state{
      holders = Holders,
      can_share = CanShare,
      barging = BargingRequest,
      clients = Clients
    } = State
)->
  if
    CanShare =:= false ->
      % Client is already holding the exclusive lock.
      % It has the highest priority.
      get_lock(Request, State);
    Shared->
      % Client requested one more shared lock.
      % The request gets the lock even if there is:
      % * a queued exclusive request or
      % * pending exclusive barging request
      get_lock(Request, State);
    BargingRequest =:= undefined ->
      % Client requested exclusive lock while holding shared - upgrade.
      % It can get it only when no other clients hold the lock.
      #client{
        requests = ClientRequests
      } = maps:get(ClientPID, Clients),
      % The request is not registered with the client yet
      case only_holder(ClientRequests, Holders, _Waiting = 0) of
        true ->
          % All the holding requests belong to the same client
          get_lock(Request, State);
        false->
          % There are other clients holding the lock - wait
          enqueue_barging(Request, State)
      end;
    true->
      % Client requested lock upgrade, but there is already another client
      % waiting for upgrade - deadlock. The first enqueued wins.
      catch ReplyTo ! #deadlock{ref = Ref},
      State
  end.

%%=================================================================
%%  Push the queue
%%
%%  Called after every change of the holders. It grants the lock to
%%  as many waiting requests as the actual lock state allows and
%%  releases the Term if there is nobody left
%%=================================================================
%%-----------------------------------------------------------------
%%  There is a queued barging request. The upgrade has the highest
%%  priority, it only waits for the other clients to release
%%  the guard:
%%  * there is a pending barging request
%%-----------------------------------------------------------------
next(#state{
  barging = #request{
    client = ClientPID,
    ref = Ref
  },
  holders = Holders,
  clients = Clients,
  requests = Requests
} = State)->

  #client{
    requests = ClientRequests
  } = maps:get(ClientPID, Clients),

  % The barging request itself is registered with the client, but it
  % is not a holder
  case only_holder(ClientRequests, Holders, _Waiting = 1) of
    true ->
      % the only client is holding the lock
      Req = maps:get(Ref, Requests),
      locked(Req, State#state{
        barging = undefined
      });
    false->
      State
  end;

%%-----------------------------------------------------------------
%%  Nobody is holding a lock - the head of the queue takes it, or
%%  the Term is released if nobody is waiting either
%%  the guard:
%%  * there is no pending barging request (the clause above)
%%  * there are no holders
%%-----------------------------------------------------------------
next(#state{
  holders = Holders,
  queue = Queue,
  requests = Requests
} = State0) when map_size(Holders) =:= 0->
  case gb_sets:is_empty(Queue) of
    true->
      try_unlock(State0);
    false->
      {_Ticket, Ref} = gb_sets:smallest(Queue),
      Req = maps:get(Ref, Requests),
      State = locked(Req, State0),
      % If the head was shared the next ones may join it
      next(State)
  end;

%%-----------------------------------------------------------------
%%  The actual lock is shared. It may share
%%  the lock with the head of the queue
%%  the guard:
%%  * there is no pending barging request
%%  * the lock is held
%%  * the lock is shared
%%-----------------------------------------------------------------
next(#state{
  can_share = true,
  queue = Queue,
  requests = Requests
} = State0)->
  case gb_sets:is_empty(Queue) of
    true->
      State0;
    false->
      {_Ticket, Ref} = gb_sets:smallest(Queue),
      case Requests of
        #{Ref := Req = #req{shared = true}} ->
          State = locked(Req, State0),
          next(State);
        _->
          % The head of the queue is exclusive, it stops the sharing
          State0
      end
  end;

%%-----------------------------------------------------------------
%%  The lock is exclusive, nobody can join it
%%-----------------------------------------------------------------
next(State)->
  State.

%%-----------------------------------------------------------------
%%  The lock is not needed any more - try to remove it from ETS.
%%  The entry is removed only if it still carries the last ticket
%%  known to the manager, otherwise a new client has already queued
%%  up and its request is on the way
%%-----------------------------------------------------------------
try_unlock(#state{
  scope = Scope,
  lock_key = LockKey,
  last = LastQueue
} = State)->
  % try to remove the lock
  Self = self(),
  ets:delete_object(Scope, {LockKey, Self, LastQueue}),

  % check unlocked
  case ets:lookup(Scope, LockKey) of
    [{_,Self,_}]->
      % not unlocked there is a queue
      % The entry is still ours: a new client has taken a ticket and
      % its request is on the way. Start the next round with a clean
      % state and wait for it
      arm_postpone_timer(State#state{
        holders = #{},
        queue = gb_sets:empty(),
        requests = #{},
        clients = #{},
        can_share = true
      });
    _->
      % unlocked, the next client will start a new manager
      exit(normal)
  end.

%%=================================================================
%%  Utilities
%%=================================================================
%%-----------------------------------------------------------------
%%  A client is monitored while it has at least one request
%%-----------------------------------------------------------------
add_client_request(ClientPID, Ref, Shared, Clients)->
  Client =
    case Clients of
      #{ClientPID := Client0}->
        #client{ requests = Requests} = Client0,
        Client0#client{
          requests = Requests#{ Ref => Shared }
        };
      _->
        #client{
          requests = #{ Ref => Shared },
          monitor_ref = erlang:monitor(process, ClientPID)
        }
    end,
  Clients#{
    ClientPID => Client
  }.

remove_client_request(ClientPID, Ref, Clients0)->
  Client0 = maps:get(ClientPID, Clients0),
  #client{
    requests = Requests0,
    monitor_ref = MonRef
  } = Client0,

  case maps:remove(Ref, Requests0) of
    Requests when map_size(Requests) =:= 0 ->
      erlang:demonitor(MonRef),
      maps:remove(ClientPID, Clients0);
    Requests->
      Client = Client0#client{
        requests = Requests
      },
      Clients0#{
        ClientPID => Client
      }
  end.

%%-----------------------------------------------------------------
%%  Does any client other than this one hold the lock?
%%
%%  A client that holds the lock never enters the queue (see
%%  add_busy_request/2), therefore every request of such a client is
%%  a holder as well - except its own pending barging request, which
%%  the caller counts in Waiting
%%-----------------------------------------------------------------
only_holder(ClientRequests, Holders, Waiting)->
  map_size(ClientRequests) - Waiting =:= map_size(Holders).

%%-----------------------------------------------------------------
%%  The attributes of a waiting request: the timeout timer and the
%%  deadlock checker. Both are dropped as soon as it stops waiting
%%-----------------------------------------------------------------
start_waiting(
    #request{
      client = Client,
      ref = Ref,
      timeout = Timeout,
      held = HeldLocks,
      nodes = Nodes
    },
    #state{
      scope = Scope,
      deadlock_scope = DeadlockScope,
      lock_key = ?lock(Term)
    },
    Req
)->
  Timer =
    if
      is_integer(Timeout), Timeout > 0 ->
        erlang:start_timer(Timeout, self(), {timeout, Ref});
      true ->
        undefined
    end,

  % Init deadlock check process
  Deadlock = elock_deadlock:check_deadlock(Scope, DeadlockScope, Client ,Term, Nodes, HeldLocks),

  Req#req{
    deadlock = Deadlock,
    timer = Timer
  }.

stop_waiting(#req{
  deadlock = Deadlock,
  timer = Timer
} = Req)->
  if
    is_pid(Deadlock) ->
      catch Deadlock ! {stop, Deadlock};
    true->
      ignore
  end,
  if
    is_reference(Timer)->
      catch erlang:cancel_timer(Timer);
    true ->
      ignore
  end,

  Req#req{
    deadlock = undefined,
    timer = undefined
  }.

%%-----------------------------------------------------------------
%%  The lock stays shared while every holder is shared
%%-----------------------------------------------------------------
can_share(Holders)->
  can_share_loop( maps:next( maps:iterator(Holders) ) ).

can_share_loop({_Ref, {_Shared = false, _ClientPID}, _Iterator})->
  false;
can_share_loop({_Ref, {_Shared, _ClientPID}, Iterator})->
  can_share_loop( maps:next(Iterator) );
can_share_loop(none)->
  true.
