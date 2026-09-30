
%%=================================================================
%%  One manager process per locked Term.
%%
%%  The lock is an entry in the Scope ETS table:
%%
%%      { Term, ManagerPID, LastTicket }
%%
%%  A ticket taken by ets:update_counter/4 is the only synchronization
%%  between the clients:
%%    * ticket 1 - the lock was free. The client holds it and spawns
%%      the manager, which writes its PID into the entry.
%%    * ticket N - the client sends the request to the manager and
%%      waits for #locked{}, #deadlock{}, #timeout{} or #retry{}.
%%
%%  The requests may arrive out of order, the manager takes them in
%%  by the ticket (see handle_request/2).
%%
%%  The manager exits once it has removed the entry from ETS.
%%=================================================================
-module(elock_manager).

-include("elock.hrl").

%%=================================================================
%%	API
%%=================================================================
-export([
  lock/1, lock/2
]).

%%=================================================================
%%  Client <-> manager protocol
%%=================================================================
%%-----------------------------------------------------------------
%%  Every message from the manager to the proxy. Tag is #request.tag
%%-----------------------------------------------------------------
-define(reply(Tag, Message), {Tag, Message}).

%%-----------------------------------------------------------------
%%  The verdicts. #retry{}: the ticket is void, take a new one.
%%  #deadlock{} is in elock.hrl
%%-----------------------------------------------------------------
-record(locked,{
  ref
}).
-record(timeout,{
  ref
}).
-record(retry,{
  ref
}).

%%=================================================================
%%  Client side
%%
%%  The proxy takes the ticket and waits for the verdict: the client
%%  itself (lock/2) or a worker on its behalf (lock/1)
%%=================================================================
%%-----------------------------------------------------------------
%%  The worker of elock:run_request/3, remote apply. It does not have
%%  the held locks
%%-----------------------------------------------------------------
-spec lock(#request{}) ->
  {ok, pid()} | {error, timeout | {deadlock, {atom(), term(), node()}}}.
lock(Request)->
  lock(Request, undefined).

%%-----------------------------------------------------------------
%%  HeldLocks is undefined for a worker
%%-----------------------------------------------------------------
-spec lock(#request{}, #{{atom(), term(), node()} => pid()} | undefined) ->
  {ok, pid()} | {error, timeout | {deadlock, {atom(), term(), node()}}}.
lock(
    #request{
      scope = Scope,
      term = Term
    } = Request,
    HeldLocks
)->
  case ets:update_counter(Scope, Term, {3,1}, {Term,0,0}) of
    1->
      Manager = start_manager(Request),
      {ok, Manager};

    RequestQueue->
      case get_manager(Scope, Term, RequestQueue) of
        Manager when is_pid(Manager) ->
          % A manager that exits takes the request with it and never
          % answers. The monitor ref is also the tag of the replies
          MonitorRef = erlang:monitor(process, Manager),
          Manager ! Request#request{ queue = RequestQueue, proxy = self(), tag = MonitorRef },
          Verdict = wait_verdict(MonitorRef, Manager, Request, HeldLocks),
          erlang:demonitor(MonitorRef, [flush]),
          case Verdict of
            retry ->
              lock(Request, HeldLocks);
            _->
              Verdict
          end;
        _->
          lock(Request, HeldLocks)
      end
  end.

%%-----------------------------------------------------------------
%%  Every clause matches MonitorRef, a plain argument from
%%  erlang:monitor/2 in lock/2: the receive skips the older messages.
%%  The request Ref can not do it: it is made in elock, maybe on
%%  another node. A worker passes #queued{} on to the client, the
%%  client answers it with HeldLocks
%%-----------------------------------------------------------------
wait_verdict(
    MonitorRef,
    Manager,
    #request{
      ref = Ref,
      scope = Scope,
      term = Term,
      client = ClientPID
    } = Request,
    HeldLocks
)->
  receive
    ?reply(MonitorRef, #locked{})->
      {ok, Manager};
    ?reply(MonitorRef, #queued{} = Queued)->
      case HeldLocks of
        undefined ->
          ecall:send(ClientPID, Queued);
        _->
          Manager ! #add_held_locks{ref = Ref, held = HeldLocks}
      end,
      wait_verdict(MonitorRef, Manager, Request, HeldLocks);
    ?reply(MonitorRef, #deadlock{winner = Winner})->
      {error, {deadlock, Winner}};
    ?reply(MonitorRef, #timeout{})->
      {error, timeout};
    ?reply(MonitorRef, #retry{})->
      retry;
    {'DOWN', MonitorRef, process, Manager, _Reason}->
      % A crashed manager leaves its entry behind
      ets:match_delete(Scope, {Term, Manager, '_'}),
      retry
  end.

%%-----------------------------------------------------------------
%%  Client side utilities
%%-----------------------------------------------------------------
get_manager(Scope, Term, MyQueue)->
  case ets:lookup(Scope, Term) of
    [ { _Lock, Manager, Queue } ] when is_pid(Manager)->
      % Queue < MyQueue: the entry has been recreated since the ticket
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
      get_manager(Scope, Term, MyQueue)
  end.

% High priority: every client of the Term waits for the manager.
% Off heap mailbox: request bursts stay out of its garbage collection
start_manager(Request)->
  spawn_opt(fun()->init(Request) end, [
    {priority, high},
    {message_queue_data, off_heap}
  ]).

%%=================================================================
%%  Manager
%%=================================================================
%% How long a missing ticket is waited for (see handle_postpone_timeout/2)
-define(POSTPONE_TIMEOUT, 100).

-record(state,{
  holders,          % #{ Ref => {Shared, ClientPID} }
  queue,            % gb_sets of {Ticket, Ref}, the head is the smallest
  requests,         % #{ Ref => #req{} }, the holders and the waiters
  clients,          % #{ ClientPID => #client{} }
  scope,
  term,
  can_share,        % every holder is shared
  barging,          % #request{} of the pending upgrade, not in the queue
  last,             % the last ticket taken in
  postponed,        % ordset of #request{} that came ahead of a missing ticket
  postpone_timer,
  graph             % elock_graph
}).

-record(req,{
  client,
  ref,
  queue,            % the ticket
  proxy,            % undefined once the lock is held
  tag,              % undefined once the lock is held
  shared,
  held_count,
  has_lock,
  timer             % the timeout timer, while waiting
}).

-record(client,{
  requests,         % #{ Ref => Shared }, the holders and the waiters
  monitor_ref       % one monitor per client, while it has requests
}).

% Started by the holder of ticket 1
init(#request{
  ref = Ref,
  scope = Scope,
  term = Term,
  client = Client,
  proxy = Proxy,
  shared = Shared
})->

  ets:update_element(Scope, Term, {2,self()}),

  State = #state{
    holders = #{ Ref => {Shared, Client} },
    queue = gb_sets:empty(),
    requests = #{
      Ref => #req{
        client = Client,
        ref = Ref,
        queue = 1,
        proxy = Proxy,
        shared = Shared,
        has_lock = true,
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
    term = Term,
    can_share = Shared,
    barging = undefined,
    last = 1,
    postponed = [],
    postpone_timer = undefined,
    graph = undefined
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
      #deadlock{} = Deadlock->
        handle_deadlock(Deadlock, State0);
      #deadlock_probe{} = Probe->
        handle_deadlock_probe(Probe, State0);
      #add_held_locks{} = Update->
        handle_add_held_locks(Update, State0);
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
%%  Taken in by the ticket. A request ahead of a missing ticket is
%%  postponed for at most POSTPONE_TIMEOUT: the missing client may be
%%  descheduled or dead
%%=================================================================
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
    #request{
      queue = Queue
    } = Request,
    #state{
      last = Last,
      postponed = Postponed
    } = State
) when Queue > Last->

  % Sorted by the ticket, the first field of #request{}
  arm_postpone_timer(State#state{
    postponed = ordsets:add_element(Request, Postponed)
  });

%%-----------------------------------------------------------------
%%  The ticket has been stepped over (see handle_postpone_timeout/2)
%%-----------------------------------------------------------------
handle_request(
    #request{
      ref = Ref,
      proxy = Proxy,
      tag = Tag
    },
    State
)->
  Proxy ! ?reply(Tag, #retry{ref = Ref}),
  State.

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

handle_postponed(#state{
  postponed = [#request{
    queue = Queue
  }|_],
  last = Last
} = State)
  when Queue > Last->
  arm_postpone_timer(State);

% The ticket has been stepped over
handle_postponed(#state{
  postponed = [#request{
    ref = Ref,
    proxy = Proxy,
    tag = Tag
  }|Rest]
} = State)->
  Proxy ! ?reply(Tag, #retry{ref = Ref}),
  handle_postponed(State#state{
    postponed = Rest
  });

handle_postponed(State)->
  cancel_postpone_timer(State).

%%-----------------------------------------------------------------
%%  Step over the missing tickets, they will get #retry{}
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
% The ticket try_unlock/1 has waited for
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
%%  The postpone timer. The only writers of #state.postpone_timer.
%%  One timer for all the postponed requests, it is not restarted
%%  while any ticket is missing
%%-----------------------------------------------------------------
arm_postpone_timer(#state{postpone_timer = Timer} = State) when is_reference(Timer)->
  State;
arm_postpone_timer(State)->
  State#state{
    postpone_timer = erlang:start_timer(?POSTPONE_TIMEOUT, self(), postpone_timeout)
  }.

cancel_postpone_timer(#state{postpone_timer = Timer} = State) when is_reference(Timer)->
  erlang:cancel_timer(Timer,[{async, true},{info, false}]),
  State#state{
    postpone_timer = undefined
  };
cancel_postpone_timer(State)->
  State.

%% A cancelled timer may have fired already, hence the reference guard
%% of handle_postpone_timeout/2
postpone_timer_fired(State)->
  State#state{
    postpone_timer = undefined
  }.

%%=================================================================
%%  Leaving requests
%%=================================================================
%%-----------------------------------------------------------------
%%  The only holder, no barging request, and in the body no queue:
%%  nobody needs the Term
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

      % Not unlocked: a new ticket is taken and the state is reset. The
      % reset state has lost the leaving client's monitor, drop it here
      Req = #req{client = Client} = maps:get(Ref, Requests),
      kill_proxy(Req),
      #client{monitor_ref = MonRef} =  maps:get(Client, Clients),
      erlang:demonitor(MonRef),

      State;
    false->
      leave_lock(Ref, State0)
  end;
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
      State
  end.

%%-----------------------------------------------------------------
%%  A timer cancelled on the grant may have fired already: ignored
%%-----------------------------------------------------------------
handle_timeout(
    Ref,
    #state{
      requests = Requests
    } = State0
)->
  case Requests of
    #{Ref := #req{ has_lock = false, proxy = Proxy, tag = Tag } = Req}->
      Proxy ! ?reply(Tag, #timeout{ref = Ref}),
      % Cancelling a fired timer costs a round trip to another scheduler
      State = dequeue(Req#req{timer = undefined}, State0),
      next(State);
    _->
      State0
  end.

%%-----------------------------------------------------------------
%%  The answer to this manager's probe, or a local closer that has
%%  lost to a foreign probe (see handle_deadlock_probe/2)
%%-----------------------------------------------------------------
handle_deadlock(
    #deadlock{ref = Ref} = Deadlock,
    #state{
      requests = Requests
    } = State0
)->
  case Requests of
    #{Ref := Req = #req{
      has_lock = false,
      proxy = Proxy,
      tag = Tag
    }}->
      Proxy ! ?reply(Tag, Deadlock),
      State = dequeue(Req, State0),
      next(State);
    _->
      % Granted or left meanwhile
      State0
  end.

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
          Req = maps:get(Ref, RequestsAcc),
          remove_request(Req, StateAcc)
        end,
        State,
        ClientRequests
      );
    _->
      State
  end.

%%=================================================================
%%  The queue
%%=================================================================
%%-----------------------------------------------------------------
%%  A shared request joins a shared lock only if nobody waits: it must
%%  not overtake a queued or barging exclusive request
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

add_busy_request(
    Request,
    #state{
      holders = Holders
    } = State
) when map_size(Holders) =:= 0->
  get_lock(Request, State);

%%-----------------------------------------------------------------
%%  The request can not join the holders. A client that holds the lock
%%  barges, the others queue
%%-----------------------------------------------------------------
add_busy_request(
    #request{
      client = ClientPID
    } = Request,
    #state{
      clients = Clients,
      requests = Requests
    } = State
)->
  case Clients of
    #{ ClientPID := Client }->
      case client_holds_lock(Client, Requests) of
        true ->
          try_barging(Request, State);
        _->
          enqueue(Request, State)
      end;
    _->
      enqueue(Request, State)
  end.

remove_request(
    #req{has_lock = false} = Req,
    State0
)->
  kill_proxy(Req),
  State = dequeue(Req, State0),
  next(State);

remove_request(
    #req{has_lock = true} = Req,
    State0
)->
  kill_proxy(Req),
  State = unlocked(Req, State0),
  next(State).

kill_proxy(#req{
  client = ClientPID,
  proxy = Proxy
})->
  if
    % A worker (see lock/1)
    is_pid(Proxy), Proxy =/= ClientPID ->
      exit(Proxy, kill);
    true ->
      ignore
  end,
  ok.

new_req(#request{
  client = ClientPID,
  ref = Ref,
  queue = Ticket,
  proxy = Proxy,
  tag = Tag,
  shared = Shared,
  held_count = HeldCount
})->
  #req{
    client = ClientPID,
    ref = Ref,
    queue = Ticket,
    proxy = Proxy,
    tag = Tag,
    shared = Shared,
    held_count = HeldCount,
    has_lock = false
  }.

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

  Req = start_waiting(Request),
  Requests = Requests0#{
    Ref => Req
  },
  Clients = add_client_request(ClientPID, Ref, Shared, Clients0),
  Queue = gb_sets:insert({Ticket, Ref}, Queue0),

  State#state{
    queue = Queue,
    requests = Requests,
    clients = Clients
  }.

%%-----------------------------------------------------------------
%%  The upgrade waits out of the queue until its client is the only
%%  holder (see next/1). One at a time (see try_barging/2)
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
  Req = start_waiting(Request),
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
%%  A waiting request leaves: timeout, deadlock or a dead client
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
      clients = Clients0,
      graph = Graph0
    } = State
)->
  {_, Graph} = stop_waiting(Req, Graph0),
  Requests = maps:remove(Ref, Requests0),
  Clients = remove_client_request(ClientPID, Ref, Clients0),

  State#state{
    requests = Requests,
    clients = Clients,
    barging = undefined,
    graph = Graph
  };

dequeue(
    #req{
      client = ClientPID,
      ref = Ref,
      queue = Ticket
    } = Req,
    #state{
      requests = Requests0,
      clients = Clients0,
      queue = Queue0,
      graph = Graph0
    } = State
)->
  Queue = gb_sets:delete_any({Ticket, Ref}, Queue0),
  Requests = maps:remove(Ref, Requests0),
  Clients = remove_client_request(ClientPID, Ref, Clients0),
  {_, Graph} = stop_waiting(Req, Graph0),

  State#state{
    queue = Queue,
    requests = Requests,
    clients = Clients,
    graph = Graph
  }.

%%-----------------------------------------------------------------
%%  Grants a request that has never waited
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

locked(
    #req{
      client = ClientPID,
      ref = Ref,
      queue = Ticket,
      proxy = Proxy,
      tag = Tag,
      shared = Shared
    } = Req0,
    #state{
      holders = Holders0,
      queue = Queue0,
      requests = Requests0,
      can_share = CanShare0,
      graph = Graph0
    } = State)->

  Proxy ! ?reply(Tag, #locked{ref = Ref}),
  {Req, Graph} = stop_waiting(
    Req0#req{
      has_lock = true,
      proxy = undefined,
      tag = undefined
    },
    Graph0
  ),

  Requests = Requests0#{
    Ref => Req
  },
  Holders = Holders0#{ Ref => {Shared, ClientPID} },
  % Not in the queue if granted by get_lock/2 or as barging by next/1
  Queue = gb_sets:delete_any({Ticket, Ref}, Queue0),

  % A shared grant on an exclusive lock is a barging one: it stays exclusive
  CanShare = Shared andalso CanShare0,

  State#state{
    holders = Holders,
    queue = Queue,
    requests = Requests,
    can_share = CanShare,
    graph = Graph
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
  Clients = remove_client_request(ClientPID, Ref, Clients0),
  Holders = maps:remove(Ref, Holders0),
  Requests = maps:remove(Ref, Requests0),

  % Only the release of an exclusive hold can make the lock shared
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
%%  The client already holds the lock
%%-----------------------------------------------------------------
try_barging(
    #request{
      ref = Ref,
      client = ClientPID,
      shared = Shared,
      proxy = Proxy,
      tag = Tag
    } = Request,
    #state{
      holders = Holders,
      can_share = CanShare,
      barging = BargingRequest,
      clients = Clients,
      scope = Scope,
      term = Term
    } = State
)->
  if
    CanShare =:= false ->
      % The client holds it exclusively
      get_lock(Request, State);
    Shared->
      % Ahead of the queued and the barging exclusive requests
      get_lock(Request, State);
    BargingRequest =:= undefined ->
      % Upgrade
      #client{
        requests = ClientRequests
      } = maps:get(ClientPID, Clients),
      % The request is not registered with the client yet
      case only_holder(ClientRequests, Holders, _Waiting = 0) of
        true ->
          get_lock(Request, State);
        false->
          enqueue_barging(Request, State)
      end;
    true->
      % A second upgrade: both wait for each other, the first one wins
      Proxy ! ?reply(Tag, #deadlock{ref = Ref, winner = {Scope, Term, node()}}),
      State
  end.

%%=================================================================
%%  Push the queue, after every change of the holders
%%=================================================================
%%-----------------------------------------------------------------
%%  The barging request goes first
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

  % The barging request is registered with the client, but it is not a holder
  case only_holder(ClientRequests, Holders, _Waiting = 1) of
    true ->
      Req = maps:get(Ref, Requests),
      locked(Req, State#state{
        barging = undefined
      });
    false->
      State
  end;

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
      % A shared head may be joined by the next ones
      next(State)
  end;

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
          State0
      end
  end;

next(State)->
  State.

%%-----------------------------------------------------------------
%%  The entry is removed only if no ticket has been taken after the
%%  last one known here
%%-----------------------------------------------------------------
try_unlock(#state{
  scope = Scope,
  term = Term,
  last = LastQueue
} = State)->
  Self = self(),
  ets:delete_object(Scope, {Term, Self, LastQueue}),

  case ets:lookup(Scope, Term) of
    [{_,Self,_}]->
      % A new ticket is taken, its request is on the way: a new round
      arm_postpone_timer(State#state{
        holders = #{},
        queue = gb_sets:empty(),
        requests = #{},
        clients = #{},
        can_share = true,
        graph = undefined
      });
    _->
      exit(normal)
  end.

%%=================================================================
%%  Deadlock probes (see elock_graph)
%%
%%  A waiting request joins the graph in handle_add_held_locks/2 and
%%  leaves it in stop_waiting/2
%%=================================================================
handle_add_held_locks(
    #add_held_locks{
      ref = Ref,
      held = Update
    },
    #state{
      requests = Requests,
      scope = Scope,
      term = Term,
      graph = Graph0
    } = State
)->
  case Requests of
    #{Ref := #req{
      has_lock = false,
      held_count = Weight
    }}->
      Graph = elock_graph:add_edges(Ref, {Scope, Term, node()}, Update, Weight, Graph0),
      State#state{
        graph = Graph
      };
    _->
      % Granted or left meanwhile
      State
  end.

%%-----------------------------------------------------------------
%%  The closers are aborted by the ref: an abort may grant a later
%%  closer, which handle_deadlock/2 then skips. The probe is forwarded
%%  on the graph after the aborts
%%-----------------------------------------------------------------
handle_deadlock_probe(
    #deadlock_probe{edge = Winner} = Probe,
    #state{
      graph = Graph0,
      scope = Scope,
      term = Term
    } = State0
)->
  case elock_graph:probe(Probe, {Scope, Term, node()}, Graph0) of
    {forward, AbortRefs}->
      State = #state{graph = Graph} = lists:foldl(
        fun(Ref, Acc)->
          handle_deadlock(#deadlock{ref = Ref, winner = Winner}, Acc)
        end,
        State0,
        AbortRefs
      ),
      elock_graph:forward(Probe, Graph),
      State;
    stop->
      State0
  end.

%%=================================================================
%%  Utilities
%%=================================================================
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

client_holds_lock(
    #client{
      requests = ClientRequests
    },
    Requests
)->
  lists:any(
    fun(Ref)->
      case Requests of
        #{ Ref := #req{has_lock = true} } -> true;
        _-> false
      end
    end,
    maps:keys(ClientRequests)
  ).

%%-----------------------------------------------------------------
%%  Is the client the only holder? A holding client never queues (see
%%  add_busy_request/2), so all its requests are holders except the
%%  barging one, counted in Waiting
%%-----------------------------------------------------------------
only_holder(ClientRequests, Holders, Waiting)->
  map_size(ClientRequests) - Waiting =:= map_size(Holders).

start_waiting(#request{
  timeout = Timeout
} = Request)->
  notify_queued(Request),
  start_timer(new_req(Request), Timeout).

stop_waiting(
    #req{
      ref = Ref
    } = Req0,
    Graph0
)->
  Req = stop_timer(Req0),
  Graph = elock_graph:remove_edges(Ref, Graph0),
  {Req, Graph}.

%%-----------------------------------------------------------------
%%  The held map is asked for only on a wait: most requests never
%%  wait. A single node request of a client that holds nothing can
%%  not be on a cycle
%%-----------------------------------------------------------------
notify_queued(#request{
  ref = Ref,
  proxy = Proxy,
  tag = Tag,
  held_count = HeldCount,
  nodes = Nodes
})->
  if
    HeldCount > 0; length(Nodes) > 1->
      Proxy ! ?reply(Tag, #queued{
        ref = Ref,
        manager = self(),
        node = node()
      });
    true ->
      ignore
  end.

start_timer(
    #req{
      ref = Ref
    } =Req,
    Timeout
)->
  Timer =
    if
      is_integer(Timeout), Timeout > 0 ->
        erlang:start_timer(Timeout, self(), {timeout, Ref});
      true ->
        undefined
    end,
  Req#req{
    timer = Timer
  }.

stop_timer(
    #req{
      timer = Timer
    } =Req
)->
  if
    is_reference(Timer)->
      erlang:cancel_timer(Timer, [{async, true}, {info, false}]),
      Req#req{
        timer = undefined
      };
    true ->
      Req
  end.

can_share(Holders)->
  can_share_loop( maps:next( maps:iterator(Holders) ) ).

can_share_loop({_Ref, {_Shared = false, _ClientPID}, _Iterator})->
  false;
can_share_loop({_Ref, {_Shared, _ClientPID}, Iterator})->
  can_share_loop( maps:next(Iterator) );
can_share_loop(none)->
  true.
