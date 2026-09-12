
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
        handle_timeout(State0);
      {'DOWN', _Ref, process, Holder, _Reason}->
        handle_down(Holder, State0);
      #deadlock{ref = Ref}->
        handle_deadlock(Ref, State0);
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

add_request(
    Request,
    State0
)->
  todo.

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

stop_waiting(#req{
  deadlock = Deadlock,
  timer = Timeout
} = Req)->
  catch Deadlock ! {stop, Deadlock},
  catch erlang:cancel_timer(Timeout),
  Req#req{
    deadlock = undefined,
    timer = undefined
  }.

enqueue(
    #request{
      shared = true
    },
    #state{
      can_share = true
    }
)->
  todo.

dequeue(
    #req{
      client = ClientPID,
      ref = Ref
    } = Req,
    #state{
      requests = Requests0,
      clients = Clients0,
      queue = Queue0
    } = State
)->
  stop_waiting(Req),
  Requests = maps:remove(Ref, Requests0),
  Clients = remove_client_request(ClientPID, Ref, Clients0),

  Queue = Queue0 -- [Ref],

  State#state{
    queue = Queue,
    requests = Requests,
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
      requests = Requests0
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

  State#state{
    holders = Holders,
    queue = Queue,
    requests = Requests,
    can_share = Shared
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

can_share([Ref|Rest], Requests)->
  case Requests of
    #{ Ref := #req{ shared = false }} ->
      false;
    _->
      can_share(Rest, Requests)
  end;
can_share([], _Requests)->
  true.


%---------------------------------------------------------
%   Pending barging request
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
%   shared lock queue
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


