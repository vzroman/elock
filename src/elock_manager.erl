
-module(elock_manager).

-include("elock.hrl").

%%=================================================================
%%	API
%%=================================================================
-export([
  lock/1,
  unlock/1
]).


-record(locked,{
  ref
}).
-record(unlock,{
  manager,
  ref
}).
-record(deadlock,{
  ref
}).
-record(timeout,{
  ref
}).

%-----------Lock request------------------------------------------
lock(#request{
  ref = Ref,
  scope = Scope,
  term = Term,
  client = Client
} = Request
)->
  LockKey = ?lock(Term),
  case ets:update_counter(Scope, LockKey, {3,1}, {LockKey,0,0}) of % try to set lock
    1->
      %------------------locked------------------
      Manager = start_manager(Request),
      ?LOGDEBUG("~p set local lock: client ~p, manager ~p",[Term, Client, Manager]),

      {ok, #unlock{
        manager = Manager,
        ref = Ref
      }};

    RequestQueue->
      %------------enqueued-------------------
      Manager = get_manager(Scope, LockKey),
      ?LOGDEBUG("~p lock queued: holder ~p, locker ~p, request queue ~p",[Term, Client, Manager, RequestQueue]),

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
          {error, timeout}
      end
  end.

unlock(#unlock{manager = Manager}=Unlock)->
  catch Manager ! Unlock,
  ok.

get_manager(Scope, LockKey)->
  case ets:lookup(Scope, LockKey) of
    [ { _Lock, Manager, _Queue } ] when is_pid(Manager)-> Manager;
    _->
      % The manager has not registered itself yet, wait
      receive after 1 -> ok end,
      get_manager(Scope, LockKey)
  end.

start_manager(Request)->
  spawn_opt(fun()->init(Request) end, [
    {priority, high},
    {message_queue_data, off_heap}
  ]).

-record(state,{
  holders,
  queue,
  requests,
  clients,
  postponed,
  scope,
  lock_key,
  can_share,
  deadlock_scope,
  last,
  barging
}).

-record(req,{
  client,
  ref,
  reply_to,
  shared,
  has_lock,
  deadlock,
  timer
}).

-record(client,{
  requests,
  monitor_ref
}).

init(#request{
  ref = Ref,
  scope = Scope,
  term = Term,
  client = Client,
  reply_to = ReplyTo,
  shared = Shared
})->

  LockKey = ?lock(Term),
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
    postponed = [],
    scope = Scope,
    lock_key = LockKey,
    can_share = Shared,
    deadlock_scope = ?deadlock_scope(Scope),
    last = 1,
    barging = undefined
  },

  loop(State).


loop(State0)->
  State =
    receive
      #unlock{ref = Ref}->
        handle_unlock(Ref, State0);
      #request{} = Request->
        handle_request(Request, State0);
      {timeout, Ref}->
        handle_timeout(Ref, State0);
      #deadlock{ref = Ref}->
        handle_deadlock(Ref, State0);
      {'DOWN', _Ref, process, ClientPID, _Reason}->
        handle_down(ClientPID, State0);
      Unexpected->
        ?LOGWARNING("unexpected message received: ~p",[Unexpected]),
        State0
    end,
  loop( State ).

%---------------------------------------------------------
%   The last known request - UNLOCK
%---------------------------------------------------------
handle_unlock(
    Ref,
    #state{
      holders = [Ref],
      queue = [],
      requests = Requests,
      clients = Clients,
      lock_key = LockKey,
      scope = Scope,
      last = LastQueue
    } = State0
)->

  % try to remove the lock
  Self = self(),
  ets:delete_object(Scope, {LockKey, Self, LastQueue}),

  % check unlocked
  case ets:lookup(Scope, LockKey) of
    [{_,Self,_}]->
      % not unlocked there is a queue
      % wait for the request

      #req{client = Client} = maps:get(Ref, Requests),
      #client{monitor_ref = MonRef} =  maps:get(Client, Clients),
      erlang:demonitor(MonRef),

      State0#state{
        holders = [],
        queue = [],
        requests = #{},
        clients = #{}
      };
    _->
      % unlocked
      exit(normal)
  end;

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

handle_request(
    #request{
      queue = Queue
    } = Request,
    #state{
      last = Last
    } = State0
) when (Last+1) =:= Queue->

  State = add_request(Request, State0),

  handle_postponed(State#state{
    last = Queue
  });

handle_request(
    Request,
    #state{
      postponed = Postponed
    } = State
)->
  State#state{
    postponed = ordsets:add_element(Request, Postponed)
  }.

handle_timeout(
    Ref,
    #state{
      requests = Requests
    } = State0
)->
  case Requests of
    #{Ref := #req{ has_lock = false } = Req}->
      State = dequeue(Req, State0),
      next(State);
    _->
      % unexpected request ref
      State0
  end.

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

handle_down(
    ClientPID,
    #state{
      clients = Clients
    } = State
)->
  case Clients of
    #{ ClientPID := #client{requests = Requests}}->
      lists:foldl(fun remove_request/2, State, Requests );
    _->
      % unexpected PID
      State
  end.

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

handle_postponed(State)->
  State.


%---------------------------------------------------------
%   Add a shared request to a shared lock
%   the guard:
%   * the request is for shared lock
%   * the actual lock is shared
%   * there is no exclusive lock request queued (the queue = [])
%   * there is no pending barging request
%---------------------------------------------------------
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

%---------------------------------------------------------
%   Remove a queued request
%---------------------------------------------------------
remove_request(
    #req{has_lock = false} = Req,
    State0
)->
  State = dequeue(Req, State0),
  next(State);

%---------------------------------------------------------
%   Remove a holding request
%---------------------------------------------------------
remove_request(
    #req{has_lock = true} = Req,
    State0
)->
  State = unlocked(Req, State0),
  next(State).

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
  % then it can not change the state of state of the lock.
  % Otherwise we need to check if there are any exclusive requests
  % among the holders
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

%---------------------------------------------------------
%   The client is already holding the lock and requested
%   it again.
%---------------------------------------------------------
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
      % it can get it only when no other clients hold the lock.
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

%---------------------------------------------------------
%   Push the queue
%---------------------------------------------------------
%---------------------------------------------------------
%   Case 1: There is a queued barging request
%---------------------------------------------------------
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
%---------------------------------------------------------
%   case 2: The actual lock is shared. It may share
%   the lock.
%---------------------------------------------------------
next(#state{
  can_share = true,
  queue = [Ref|_],
  requests = Requests
} = State0)->
  case Requests of
    #{Ref := #req{shared = true}} ->
      State = locked(Ref, State0),
      next(State);
    _->
      State0
  end;

next(#state{can_share = false} = State)->
  State;
next(#state{queue = []} = State)->
  State.

%---------------------------------------------------------
%   Utilities
%---------------------------------------------------------
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

can_share([Ref|Rest], Requests)->
  case Requests of
    #{ Ref := #req{ shared = false }} ->
      false;
    _->
      can_share(Rest, Requests)
  end;
can_share([], _Requests)->
  true.



