
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
      Manager = get_manager(Scope, LockKey),
      Manager ! Request#request{ queue = RequestQueue },
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
          lock(Request)
      end
  end.

unlock(#unlock{manager = Manager}=Unlock)->
  catch Manager ! Unlock,
  ok.

%%-----------------------------------------------------------------
%%  Client side utilities
%%-----------------------------------------------------------------
get_manager(Scope, LockKey)->
  case ets:lookup(Scope, LockKey) of
    [ { _Lock, Manager, _Queue } ] when is_pid(Manager)-> Manager;
    _->
      % The manager has not registered itself yet, wait
      receive after 1 -> ok end,
      get_manager(Scope, LockKey)
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
-record(state,{
  holders,          % refs of the requests holding the lock
  queue,            % refs of the waiting requests, FIFO
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
  reply_to,         % the process waiting for the verdict
  shared,           % the requested lock type
  has_lock,         % true - holds the lock, false - waits in the queue
  deadlock,         % the deadlock checker, only while waiting
  timer             % the timeout timer, only while waiting
}).

-record(client,{
  requests,         % refs of all the requests of the client
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
    holders = [Ref],
    queue = ordsets:new(),
    requests = #{
      Ref => #req{
        client = Client,
        ref = Ref,
        reply_to = ReplyTo,
        shared = Shared,
        has_lock = true,
        deadlock = undefined,
        timer = undefined
      }
    },
    clients = #{
      Client => #client{
        requests = [Ref],
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
      {timeout, _TimerRef, postpone_timeout}->
        handle_postpone_timeout(State0);
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
      last = Last,
      postpone_timer = PostponeTimer
    } = State0
) when (Last+1) =:= Queue->

  % The gap is closed. If the postponed requests keep another one
  % then handle_postponed/1 sets the timer again
  State1 =
    if
      is_reference(PostponeTimer)->
        erlang:cancel_timer(PostponeTimer),
        State0#state{
          postpone_timer = undefined
        };
      true ->
        State0
    end,

  State = add_request(Request, State1),

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
      postponed = Postponed,
      postpone_timer = PostponeTimer0
    } = State
) when Queue > Last->

  PostponeTimer =
    if
      is_reference(PostponeTimer0)->
        PostponeTimer0;
      true->
        erlang:start_timer(_Timeout = 100, self(), postpone_timeout)
    end,

  % #request.queue is the first field of the record, therefore the
  % ordset keeps the postponed requests sorted by their tickets
  State#state{
    postponed = ordsets:add_element(Request, Postponed),
    postpone_timer = PostponeTimer
  };

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
  State#state{
    postpone_timer = erlang:start_timer(_Timeout = 100, self(), postpone_timeout)
  };

%%-----------------------------------------------------------------
%%  Nothing is postponed
%%-----------------------------------------------------------------
handle_postponed(State)->
  State.

%%-----------------------------------------------------------------
%%  The missing requests did not come in time. Step over their
%%  tickets - if they come later they will be told to retry
%%-----------------------------------------------------------------
handle_postpone_timeout(#state{
  postponed = [#request{
    queue = Queue
  } = Request|Rest]
} =State0)->
  State = add_request(Request, State0),
  handle_postponed(State#state{
    postponed = Rest,
    last = Queue
  });
handle_postpone_timeout(#state{
  postponed = []
} =State)->
  State.

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
      holders = [Ref],
      queue = [],
      barging = undefined,
      requests = Requests,
      clients = Clients
    } = State0
)->

  State = try_unlock(State0),

  % If here, then it's not unlocked: a new client has taken a ticket
  % and the state is already reset for it. The monitor of the leaving
  % client is not in the reset state, drop it explicitly
  #req{client = Client} = maps:get(Ref, Requests),
  #client{monitor_ref = MonRef} =  maps:get(Client, Clients),
  erlang:demonitor(MonRef),

  State;

%%-----------------------------------------------------------------
%%  One of the requests - the lock itself stays
%%  the guard:
%%  * the Term is still needed: there are other holders, or a queue,
%%    or a pending barging request (the clause above)
%%-----------------------------------------------------------------
handle_unlock(
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
      State = dequeue(Req, State0),
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
    #{Ref := Req}->
      #req{
        has_lock = false,
        reply_to = ReplyTo
      } = Req,
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
    #{ ClientPID := #client{requests = Requests}}->
      lists:foldl(
        fun(Ref, #state{requests = RequestsAcc} = StateAcc)->
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
        Requests
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
%%  * there is no exclusive lock request queued (the queue = [])
%%  * there is no pending barging request
%%  The last two keep the queue fair: a newcomer does not overtake
%%  the exclusive requests that are already waiting
%%-----------------------------------------------------------------
add_request(
    #request{
      shared = true
    } = Request,
    #state{
      can_share = true,
      queue = [],
      barging = undefined
    } = State
)->
  get_lock(Request, State);

%%-----------------------------------------------------------------
%%  Nobody is holding the lock - take it, shared or not
%%  the guard:
%%  * there are no holders
%%-----------------------------------------------------------------
add_request(
    Request,
    #state{
      holders = []
    } = State
)->
  get_lock(Request, State);

%%-----------------------------------------------------------------
%%  The lock is busy - the request goes to the tail of the queue.
%%  A client that is already among the holders may barge in instead
%%  the guard:
%%  * the lock is held and the request can not join it: it is
%%    exclusive, or the lock is exclusive, or somebody is already
%%    waiting for it (the clauses above)
%%-----------------------------------------------------------------
add_request(
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
%%  The client starts waiting here: the timeout timer and the
%%  deadlock checker live as long as the request is in the queue
%%-----------------------------------------------------------------
enqueue(
    #request{
      client = ClientPID,
      ref = Ref,
      reply_to = ReplyTo,
      shared = Shared
    } = Request,
    #state{
      queue = Queue0,
      requests = Requests0,
      clients = Clients0
    } = State
)->

  Req0 = #req{
    client = ClientPID,
    ref = Ref,
    reply_to = ReplyTo,
    shared = Shared,
    has_lock = false
  },
  Req = start_waiting(Request, State, Req0),
  Requests = Requests0#{
    Ref => Req
  },
  Clients = add_client_request(ClientPID, Ref, Clients0),
  Queue = Queue0 ++ [Ref],

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
      ref = Ref,
      reply_to = ReplyTo
    } = Request,
    #state{
      requests = Requests0,
      clients = Clients0
    } =State
)->
  Req0 = #req{
    client = ClientPID,
    ref = Ref,
    reply_to = ReplyTo,
    shared = false, % Enqueued barging request is always exclusive
    has_lock = false
  },
  Req = start_waiting(Request, State, Req0),
  Requests = Requests0#{
    Ref => Req
  },
  Clients = add_client_request(ClientPID, Ref, Clients0),

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
      ref = Ref
    } = Req,
    #state{
      requests = Requests0,
      clients = Clients0,
      queue = Queue0,
      barging = Barging
    } = State0
)->
  stop_waiting(Req),
  Requests = maps:remove(Ref, Requests0),
  Clients = remove_client_request(ClientPID, Ref, Clients0),

  State = State0#state{
    requests = Requests,
    clients = Clients
  },
  case Barging of
    #request{ ref = Ref }->
      % Dequeue barging request
      State#state{
        barging = undefined
      };
    _->
      State#state{
        queue = Queue0 -- [Ref],
        requests = Requests,
        clients = Clients
      }
  end.

%%-----------------------------------------------------------------
%%  Grant the lock to a request that has never been queued
%%-----------------------------------------------------------------
get_lock(
    #request{
      client = ClientPID,
      ref = Ref,
      reply_to = ReplyTo,
      shared = Shared
    },
    #state{
      clients = Clients0
    } = State0
)->
  Req = #req{
    client = ClientPID,
    ref = Ref,
    reply_to = ReplyTo,
    shared = Shared
  },

  State = locked(Req, State0),
  Clients = add_client_request(ClientPID, Ref, Clients0),

  State#state{
    clients = Clients
  }.

%%-----------------------------------------------------------------
%%  The request becomes a holder: the client is notified, the
%%  waiting attributes are dropped
%%-----------------------------------------------------------------
locked(
    #req{
      ref = Ref,
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
  Holders = Holders0 ++ [Ref],
  Queue = Queue0 -- [Ref],

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
  Holders = Holders0 -- [Ref],
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
        can_share(Holders, Requests)
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
      case Holders -- ClientRequests of
        [] ->
          % All the holding requests belong to the same client
          get_lock(Request, State);
        _->
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

  case Holders -- ClientRequests of
    [] ->
      % the only client is holding the lock
      Req = maps:get(Ref, Requests),
      locked(Req, State#state{
        barging = undefined
      });
    _->
      State
  end;

%%-----------------------------------------------------------------
%%  Nobody is holding a lock
%%  the guard:
%%  * there is no pending barging request (the clause above)
%%  * there are no holders
%%  * the queue is not empty
%%-----------------------------------------------------------------
next(#state{
  holders = [],
  queue = [Ref|_],
  requests = Requests
} = State0)->
  Req = maps:get(Ref, Requests),
  State = locked(Req, State0),
  % If the head was shared the next ones may join it
  next(State);

%%-----------------------------------------------------------------
%%  Nobody is holding and no queue
%%  the guard:
%%  * there is no pending barging request
%%  * there are no holders
%%  * the queue is empty
%%-----------------------------------------------------------------
next(#state{
  holders = [],
  queue = []
} = State)->
 try_unlock(State);

%%-----------------------------------------------------------------
%%  The actual lock is shared. It may share
%%  the lock with the head of the queue
%%  the guard:
%%  * there is no pending barging request
%%  * the lock is shared
%%  * the queue is not empty
%%-----------------------------------------------------------------
next(#state{
  can_share = true,
  queue = [Ref|_],
  requests = Requests
} = State0)->
  case Requests of
    #{Ref := Req = #req{shared = true}} ->
      State = locked(Req, State0),
      next(State);
    _->
      % The head of the queue is exclusive, it stops the sharing
      State0
  end;

%%-----------------------------------------------------------------
%%  The lock is exclusive, nobody can join it
%%-----------------------------------------------------------------
next(#state{can_share = false} = State)->
  State;

%%-----------------------------------------------------------------
%%  Nothing to push
%%  the guard:
%%  * there is no pending barging request
%%  * the lock is held and shared
%%  * the queue is empty
%%-----------------------------------------------------------------
next(#state{queue = []} = State)->
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
      State#state{
        holders = [],
        queue = [],
        requests = #{},
        clients = #{},
        can_share = true
      };
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
add_client_request(ClientPID, Ref, Clients)->
  Client =
    case Clients of
      #{ClientPID := Client0}->
        #client{ requests = Requests} = Client0,
        Client0#client{
          requests = Requests ++ [Ref]
        };
      _->
        #client{
          requests = [Ref],
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

  case Requests0 -- [Ref] of
    [] ->
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
can_share([Ref|Rest], Requests)->
  case Requests of
    #{ Ref := #req{ shared = false }} ->
      false;
    _->
      can_share(Rest, Requests)
  end;
can_share([], _Requests)->
  true.
