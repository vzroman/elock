
%%=================================================================
%%  A fixed pool of permanent manager processes per scope, started
%%  by elock_scope. A Term is hashed to a slot of the pool, a manager
%%  keeps the locks of the terms of its slot.
%%
%%  A Term that is not free has an entry in the ETS table of its
%%  slot:
%%
%%      { Term, Count, Ref, ClientPID, Shared }
%%
%%  The client takes the entry by ets:update_counter/4. Count is not
%%  a ticket, it only tells whether the Term was free:
%%    * 1 - the Term was free. The client holds the lock, it tells
%%      the manager with #hold{} and does not wait for an answer.
%%    * N - the client sends the request to the manager and waits
%%      for #locked{}, #deadlock{} or #timeout{}.
%%
%%  Ref, ClientPID and Shared are the hold of the client that has
%%  taken the entry: a request may reach the manager before #hold{}.
%%
%%  The requests of a Term that is not free are served in the order
%%  they arrive at the manager: its mailbox is the only
%%  synchronization between them.
%%
%%  The manager deletes the entry when the Term is released.
%%
%%  The clients that take the entries by themselves do not wait for
%%  the manager and can send faster than it handles. A manager with
%%  a long mailbox raises the busy flag of its slot: the clients
%%  send requests and wait until it has caught up.
%%
%%  A manager lives as long as its scope, its table lives as long as
%%  the manager.
%%=================================================================
-module(elock_manager).
-moduledoc false.

-include("elock.hrl").

%%=================================================================
%%	API
%%=================================================================
-export([
  lock/1, lock/2
]).

%%=================================================================
%%	Pool API
%%=================================================================
-export([
  start_link/1,
  init/2
]).

-type lock_result() ::
  {ok, pid()} | {error, timeout | {deadlock, lock_key()}}.
-type holder() :: {boolean(), pid()}.
-type holders() :: #{reference() => holder()}.
-type client_requests() :: #{reference() => true}.

%%=================================================================
%%  Client <-> manager protocol
%%=================================================================
%%-----------------------------------------------------------------
%%  Every message from the manager to the proxy. Tag is #request.tag
%%-----------------------------------------------------------------
-define(reply(Tag, Message), {Tag, Message}).

%%-----------------------------------------------------------------
%%  The verdicts. #deadlock{} is in elock.hrl
%%-----------------------------------------------------------------
-record(locked,{}).
-record(timeout,{}).

%%-----------------------------------------------------------------
%%  The entry of a Term in the table of its slot
%%-----------------------------------------------------------------
-define(entry(Term, Count, Ref, ClientPID, Shared), {Term, Count, Ref, ClientPID, Shared}).
-define(count, 2).

%%-----------------------------------------------------------------
%%  Client -> manager: the client has taken the entry of a free Term
%%-----------------------------------------------------------------
-record(hold,{
  ref :: reference(),
  term :: term(),
  client :: pid(),
  shared :: boolean()
}).

%%=================================================================
%%  Client side
%%
%%  The client takes the entry of a free Term by itself (lock/2).
%%  Otherwise the proxy sends the request and waits for the verdict:
%%  the client itself (lock/2) or a worker on its behalf (lock/1)
%%=================================================================
%%-----------------------------------------------------------------
%%  The worker of elock_context:run_request/3, remote apply. It does
%%  not have the held locks. It does not take the entry either: its
%%  #hold{} would not be ordered with #unlock{} and 'DOWN' of the
%%  client
%%-----------------------------------------------------------------
-spec lock(#request{}) -> lock_result().
lock(
    #request{
      scope = Scope,
      term = Term
    } = Request
)->
  {_Table, Manager, _Busy} = elock_scope:slot(Scope, Term),
  request(Manager, Request, undefined).

%%-----------------------------------------------------------------
%%  The client itself. A free Term is locked without a round trip to
%%  the manager: #hold{}, #unlock{} and 'DOWN' of the client reach it
%%  in this order. The entry of a client that has died before the
%%  send stays until the next request for the Term (see
%%  handle_request/2). A busy manager is waited for (see
%%  check_load/1), it takes the entry itself. badarg from the table:
%%  the scope is stopped on the node
%%-----------------------------------------------------------------
-spec lock(#request{}, held_locks()) -> lock_result().
lock(
    #request{
      ref = Ref,
      scope = Scope,
      term = Term,
      shared = Shared
    } = Request,
    HeldLocks
)->
  {Table, Manager, Busy} = elock_scope:slot(Scope, Term),
  case atomics:get(Busy, 1) of
    0->
      case ets:update_counter(Table, Term, {?count, 1}, ?entry(Term, 0, Ref, self(), Shared)) of
        1->
          Manager ! #hold{ref = Ref, term = Term, client = self(), shared = Shared},
          {ok, Manager};
        _->
          request(Manager, Request, HeldLocks)
      end;
    _->
      request(Manager, Request, HeldLocks)
  end.

%%-----------------------------------------------------------------
%%  HeldLocks is undefined for a worker
%%-----------------------------------------------------------------
-spec request(pid(), #request{}, held_locks() | undefined) -> lock_result().
request(Manager, Request, HeldLocks)->
  % A manager is down only with its scope. The monitor ref is also the
  % tag of the replies
  MonitorRef = erlang:monitor(process, Manager),
  Manager ! Request#request{ proxy = self(), tag = MonitorRef },
  Verdict = wait_verdict(MonitorRef, Manager, Request, HeldLocks),
  erlang:demonitor(MonitorRef, [flush]),
  Verdict.

%%-----------------------------------------------------------------
%%  Every clause matches MonitorRef, a plain argument from
%%  erlang:monitor/2 in request/3: the receive skips the older messages.
%%  The request Ref can not do it: it is made in elock_context, maybe
%%  on another node. A worker passes #queued{} on to the client, the
%%  client answers it with HeldLocks
%%-----------------------------------------------------------------
-spec wait_verdict(reference(), pid(), #request{}, held_locks() | undefined) ->
  lock_result().
wait_verdict(
    MonitorRef,
    Manager,
    #request{
      ref = Ref,
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
    {'DOWN', MonitorRef, process, Manager, _Reason}->
      % The scope is stopped on the node, its locks are lost
      erlang:error(badarg)
  end.

%%=================================================================
%%  Manager
%%=================================================================
% One per live Term. Dropped when it has no holders (see put_lock/1)
-record(lock,{
  term :: term(), % the canonical copy, see #req.term
  holders :: holders(),
  queue :: gb_sets:set({pos_integer(), reference()}), % {Seq, Ref}, the head is the smallest
  can_share :: boolean(), % every holder is shared
  barging :: reference() | undefined, % the pending upgrade, not in the queue
  graph :: elock_graph:graph() | undefined
}).

-record(req,{
  client :: pid(),
  ref :: reference(),
  term :: term(), % #lock.term, the lock of the request
  seq :: pos_integer() | undefined, % the sort key in #lock.queue, set when the request enters it
  proxy :: pid() | undefined, % undefined once the lock is held
  tag :: reference() | undefined, % undefined once the lock is held
  shared :: boolean(),
  held_count :: non_neg_integer() | undefined, % the weight in a deadlock, set when the request starts waiting
  has_lock :: boolean(),
  timer :: reference() | undefined % the timeout timer, while waiting
}).

-record(client,{
  monitor_ref :: reference(), % one monitor per client, while it has requests
  locks :: #{term() => client_requests()} % Term => the holders and the waiters of the client
}).

-type locks() :: #{term() => #lock{}}.
-type requests() :: #{reference() => #req{}}.
-type clients() :: #{pid() => #client{}}.

%%-----------------------------------------------------------------
%%  The load of the manager (see check_load/1): the messages between
%%  two looks at the mailbox, and the mailbox of a busy manager
%%-----------------------------------------------------------------
-define(LOAD_PERIOD, 32).
-define(BUSY_QUEUE, 128).

% One per pool slot
-record(state,{
  scope :: atom(),
  table :: ets:table(), % the entries of the terms of the slot
  busy :: atomics:atomics_ref(), % the busy flag of the slot
  load_check :: non_neg_integer(), % the messages until the next check_load/1
  locks :: locks(), % the live terms of the slot
  requests :: requests(), % the holders and the waiters of all the terms
  clients :: clients(),
  seq :: non_neg_integer() % the number of the last queued request
}).

% The lock in flight: a handler loads it, the queue functions pass it
% on together with the state, put_lock/1 stores it
-type ls() :: {#lock{}, #state{}}.

%%-----------------------------------------------------------------
%%  A slot of the pool, called by the scope.
%%  High priority: every client of a slot waits for its manager.
%%  Off heap mailbox: request bursts stay out of its garbage
%%  collection
%%-----------------------------------------------------------------
-spec start_link(atom()) -> {ets:table(), pid(), atomics:atomics_ref()}.
start_link(Scope)->
  Manager = spawn_opt(?MODULE, init, [Scope, self()], [
    link,
    {priority, high},
    {message_queue_data, off_heap}
  ]),
  receive
    {Manager, Table, Busy}->
      {Table, Manager, Busy}
  end.

% The table is owned by the manager: they stop together
-spec init(atom(), pid()) -> no_return().
init(Scope, ScopePID)->
  Table = ets:new(?MODULE, [public, set, {write_concurrency, auto}]),
  Busy = atomics:new(1, []),
  ScopePID ! {self(), Table, Busy},
  loop(#state{
    scope = Scope,
    table = Table,
    busy = Busy,
    load_check = ?LOAD_PERIOD,
    locks = #{},
    requests = #{},
    clients = #{},
    seq = 0
  }).


-spec loop(#state{}) -> no_return().
loop(#state{load_check = 0} = State)->
  loop(check_load(State));
loop(#state{load_check = LoadCheck} = State0)->
  State =
    receive
      #hold{} = Hold->
        handle_hold(Hold, State0);
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
      Unexpected->
        ?LOGWARNING("unexpected message received: ~p",[Unexpected]),
        State0
    end,
  loop(State#state{
    load_check = LoadCheck - 1
  }).

%%-----------------------------------------------------------------
%%  The busy flag of the slot follows the mailbox. It is looked at
%%  every ?LOAD_PERIOD messages, so the flag of a manager that has
%%  nothing to do is down: its mailbox has been seen short on the
%%  way to empty
%%-----------------------------------------------------------------
-spec check_load(#state{}) -> #state{}.
check_load(#state{busy = Busy} = State)->
  {message_queue_len, Length} = process_info(self(), message_queue_len),
  if
    Length > ?BUSY_QUEUE -> atomics:put(Busy, 1, 1);
    true -> atomics:put(Busy, 1, 0)
  end,
  State#state{
    load_check = ?LOAD_PERIOD
  }.

%%=================================================================
%%  New holds and requests
%%
%%  A Term that is not among the locks has no holder the manager
%%  knows about
%%=================================================================
%%-----------------------------------------------------------------
%%  The client has taken the entry of a free Term (see lock/2). The
%%  Term is among the locks if a request has come first and has taken
%%  the hold from the entry (see handle_request/2)
%%-----------------------------------------------------------------
-spec handle_hold(#hold{}, #state{}) -> #state{}.
handle_hold(
    #hold{
      ref = Ref,
      term = Term,
      client = ClientPID,
      shared = Shared
    },
    #state{
      locks = Locks
    } = State
)->
  case is_map_key(Term, Locks) of
    false->
      put_lock(hold(Ref, ClientPID, Shared, {new_lock(Term), State}));
    true->
      State
  end.

%%-----------------------------------------------------------------
%%  A request for a held Term joins the holders, barges or queues
%%  (see add_request/2). For a Term that is not among the locks the
%%  manager takes the entry the way a client does (see lock/2)
%%-----------------------------------------------------------------
-spec handle_request(#request{}, #state{}) -> #state{}.
handle_request(
    #request{
      ref = Ref,
      term = Term,
      client = ClientPID,
      shared = Shared
    } = Request,
    #state{
      table = Table,
      locks = Locks
    } = State
)->
  case Locks of
    #{Term := Lock}->
      put_lock(add_request(Request, {Lock, State}));
    _->
      LS = {new_lock(Term), State},
      case ets:update_counter(Table, Term, {?count, 1}, ?entry(Term, 0, Ref, ClientPID, Shared)) of
        1->
          % The request of a worker, or the Term has been released while
          % the request was on the way
          put_lock(grant(Request, LS));
        _->
          % A client has taken the entry. Its #hold{} is on the way, or the
          % client has died before the send: the hold is in the entry
          [?entry(_Term, _Count, HoldRef, HoldPID, HoldShared)] = ets:lookup(Table, Term),
          put_lock(add_request(Request, hold(HoldRef, HoldPID, HoldShared, LS)))
      end
  end.

%%=================================================================
%%  Leaving requests
%%=================================================================
%%-----------------------------------------------------------------
%%  An unknown ref is ignored: a failed multi node request unlocks
%%  every manager it is queued at, also the one whose verdict has
%%  ended it
%%-----------------------------------------------------------------
-spec handle_unlock(reference(), #state{}) -> #state{}.
handle_unlock(Ref, State)->
  case get_req(Ref, State) of
    {Req, Lock}->
      put_lock(remove_request(Req, {Lock, State}));
    undefined->
      State
  end.

%%-----------------------------------------------------------------
%%  A timer cancelled on the grant may have fired already: ignored
%%-----------------------------------------------------------------
-spec handle_timeout(reference(), #state{}) -> #state{}.
handle_timeout(Ref, State)->
  case get_req(Ref, State) of
    {#req{ has_lock = false, proxy = Proxy, tag = Tag } = Req, Lock}->
      Proxy ! ?reply(Tag, #timeout{}),
      % No stop_timer/1: cancelling a fired timer costs a round trip to
      % another scheduler
      LS = dequeue(Req, {Lock, State}),
      put_lock(next(LS));
    _->
      State
  end.

%%-----------------------------------------------------------------
%%  The answer to this manager's probe: the origin has lost to a
%%  closer (see elock_graph:probe/2)
%%-----------------------------------------------------------------
-spec handle_deadlock(#deadlock{}, #state{}) -> #state{}.
handle_deadlock(
    #deadlock{
      ref = Ref,
      winner = Winner
    },
    State
)->
  case get_req(Ref, State) of
    {_Req, Lock}->
      put_lock(abort_waiter(Ref, Winner, {Lock, State}));
    undefined->
      % Left meanwhile
      State
  end.

%%-----------------------------------------------------------------
%%  The terms of the client one by one: load, remove the requests of
%%  the client, store. The requests are taken from the state in
%%  flight: the removal of one may grant another
%%-----------------------------------------------------------------
-spec handle_down(pid(), #state{}) -> #state{}.
handle_down(
    ClientPID,
    #state{
      clients = Clients
    } = State
)->
  case Clients of
    #{ ClientPID := #client{locks = ClientLocks}}->
      maps:fold(
        fun(Term, ClientRequests, #state{locks = LocksAcc} = StateAcc)->
          LS = maps:fold(
            fun(Ref, true, {_LockAcc, #state{requests = RequestsAcc}} = LSAcc)->
              Req = maps:get(Ref, RequestsAcc),
              remove_request(Req, LSAcc)
            end,
            {maps:get(Term, LocksAcc), StateAcc},
            ClientRequests
          ),
          put_lock(LS)
        end,
        State,
        ClientLocks
      );
    _->
      % The monitor is dropped without flush: 'DOWN' was in the mailbox
      State
  end.

%%=================================================================
%%  The queue
%%=================================================================
%%-----------------------------------------------------------------
%%  A shared request joins a shared lock only if nobody waits: it must
%%  not overtake a queued or barging exclusive request
%%-----------------------------------------------------------------
-spec add_request(#request{}, ls()) -> ls().
add_request(
    #request{
      shared = true
    } = Request,
    {
      #lock{
        can_share = true,
        queue = Queue,
        barging = undefined
      },
      _State
    } = LS
)->
  case gb_sets:is_empty(Queue) of
    true->
      grant(Request, LS);
    false->
      add_busy_request(Request, LS)
  end;

add_request(Request, LS)->
  add_busy_request(Request, LS).

%%-----------------------------------------------------------------
%%  The request can not join the holders. A client that holds the lock
%%  barges, the others queue
%%-----------------------------------------------------------------
-spec add_busy_request(#request{}, ls()) -> ls().
add_busy_request(
    #request{
      client = ClientPID
    } = Request,
    {
      #lock{
        term = Term
      },
      #state{
        clients = Clients,
        requests = Requests
      }
    } = LS
)->
  case Clients of
    #{ ClientPID := #client{locks = #{Term := ClientRequests}} }->
      case client_holds_lock(ClientRequests, Requests) of
        true ->
          try_barging(Request, LS);
        _->
          enqueue(Request, LS)
      end;
    _->
      enqueue(Request, LS)
  end.

-spec remove_request(#req{}, ls()) -> ls().
remove_request(
    #req{has_lock = true} = Req,
    LS0
)->
  LS = unlocked(Req, LS0),
  next(LS);

remove_request(
    #req{
      has_lock = false,
      timer = Timer
    } = Req,
    LS0
)->
  kill_proxy(Req),
  stop_timer(Timer),
  LS = dequeue(Req, LS0),
  next(LS).

%%-----------------------------------------------------------------
%%  A waiter has lost a deadlock: the origin of this manager's probe,
%%  or a closer of a probe that has come in (see
%%  handle_deadlock_probe/2)
%%-----------------------------------------------------------------
-spec abort_waiter(reference(), lock_key(), ls()) -> ls().
abort_waiter(
    Ref,
    Winner,
    {
      _Lock,
      #state{
        requests = Requests
      }
    } = LS0
)->
  case Requests of
    #{Ref := Req = #req{
      has_lock = false,
      proxy = Proxy,
      tag = Tag,
      timer = Timer
    }}->
      Proxy ! ?reply(Tag, #deadlock{ref = Ref, winner = Winner}),
      stop_timer(Timer),
      LS = dequeue(Req, LS0),
      next(LS);
    _->
      % Granted meanwhile
      LS0
  end.

-spec kill_proxy(#req{}) -> ok.
kill_proxy(#req{
  client = ClientPID,
  proxy = Proxy
})->
  if
    % A worker (see lock/1)
    Proxy =/= ClientPID ->
      exit(Proxy, kill);
    true ->
      ignore
  end,
  ok.

%%-----------------------------------------------------------------
%%  The only writer of #state.seq: the queued requests are numbered
%%  in the order they arrive
%%-----------------------------------------------------------------
-spec enqueue(#request{}, ls()) -> ls().
enqueue(
    #request{
      client = ClientPID,
      ref = Ref
    } = Request,
    {
      #lock{
        term = Term,
        queue = Queue0
      } = Lock,
      #state{
        requests = Requests0,
        clients = Clients0,
        seq = Seq0
      } = State
    }
)->

  Seq = Seq0 + 1,
  Req = start_waiting(Request, Term, Seq),
  Requests = Requests0#{
    Ref => Req
  },
  Clients = add_client_request(ClientPID, Term, Ref, Clients0),
  Queue = gb_sets:insert({Seq, Ref}, Queue0),

  {
    Lock#lock{
      queue = Queue
    },
    State#state{
      requests = Requests,
      clients = Clients,
      seq = Seq
    }
  }.

%%-----------------------------------------------------------------
%%  The upgrade waits out of the queue until its client is the only
%%  holder (see next/1). One at a time (see try_barging/2)
%%-----------------------------------------------------------------
-spec enqueue_barging(#request{}, ls()) -> ls().
enqueue_barging(
    #request{
      client = ClientPID,
      ref = Ref
    } = Request,
    {
      #lock{
        term = Term
      } = Lock,
      #state{
        requests = Requests0,
        clients = Clients0
      } = State
    }
)->
  Req = start_waiting(Request, Term, _Seq = undefined),
  Requests = Requests0#{
    Ref => Req
  },
  Clients = add_client_request(ClientPID, Term, Ref, Clients0),

  {
    Lock#lock{
      barging = Ref
    },
    State#state{
      requests = Requests,
      clients = Clients
    }
  }.

%%-----------------------------------------------------------------
%%  A waiting request leaves: timeout, deadlock or a dead client. Its
%%  timer is stopped by the caller, unless it is the one that has
%%  fired (see handle_timeout/2)
%%-----------------------------------------------------------------
-spec dequeue(#req{}, ls()) -> ls().
dequeue(
    #req{
      client = ClientPID,
      ref = Ref
    },
    {
      #lock{
        term = Term,
        barging = Ref,
        graph = Graph0
      } = Lock,
      #state{
        requests = Requests0,
        clients = Clients0
      } = State
    }
)->
  Requests = maps:remove(Ref, Requests0),
  Clients = remove_client_request(ClientPID, Term, Ref, Clients0),
  Graph = elock_graph:remove_edges(Ref, Graph0),

  {
    Lock#lock{
      barging = undefined,
      graph = Graph
    },
    State#state{
      requests = Requests,
      clients = Clients
    }
  };

dequeue(
    #req{
      client = ClientPID,
      ref = Ref,
      seq = Seq
    },
    {
      #lock{
        term = Term,
        queue = Queue0,
        graph = Graph0
      } = Lock,
      #state{
        requests = Requests0,
        clients = Clients0
      } = State
    }
)->
  Queue = gb_sets:delete({Seq, Ref}, Queue0),
  Requests = maps:remove(Ref, Requests0),
  Clients = remove_client_request(ClientPID, Term, Ref, Clients0),
  Graph = elock_graph:remove_edges(Ref, Graph0),

  {
    Lock#lock{
      queue = Queue,
      graph = Graph
    },
    State#state{
      requests = Requests,
      clients = Clients
    }
  }.

%%-----------------------------------------------------------------
%%  Grants a request that has never waited: it has no timer, no edges
%%  in the graph and no place in the queue
%%-----------------------------------------------------------------
-spec grant(#request{}, ls()) -> ls().
grant(
    #request{
      client = ClientPID,
      ref = Ref,
      proxy = Proxy,
      tag = Tag,
      shared = Shared
    },
    LS
)->
  Proxy ! ?reply(Tag, #locked{}),
  hold(Ref, ClientPID, Shared, LS).

%%-----------------------------------------------------------------
%%  A new holder that has never waited: granted by the manager (see
%%  grant/2) or by the entry (see lock/2)
%%-----------------------------------------------------------------
-spec hold(reference(), pid(), boolean(), ls()) -> ls().
hold(
    Ref,
    ClientPID,
    Shared,
    {
      #lock{
        term = Term,
        holders = Holders0,
        can_share = CanShare0
      } = Lock,
      #state{
        requests = Requests0,
        clients = Clients0
      } = State
    }
)->
  Requests = Requests0#{
    Ref => #req{
      client = ClientPID,
      ref = Ref,
      term = Term,
      shared = Shared,
      has_lock = true
    }
  },
  Clients = add_client_request(ClientPID, Term, Ref, Clients0),
  Holders = Holders0#{ Ref => {Shared, ClientPID} },

  % A shared grant on an exclusive lock is a barging one: it stays exclusive
  CanShare = Shared andalso CanShare0,

  {
    Lock#lock{
      holders = Holders,
      can_share = CanShare
    },
    State#state{
      requests = Requests,
      clients = Clients
    }
  }.

%%-----------------------------------------------------------------
%%  Grants a waiting request, next/1 has taken it out of the queue.
%%  Every holder is shared by then, so the lock takes the mode of the
%%  request
%%-----------------------------------------------------------------
-spec locked(#req{}, ls()) -> ls().
locked(
    #req{
      client = ClientPID,
      ref = Ref,
      proxy = Proxy,
      tag = Tag,
      shared = Shared,
      timer = Timer
    } = Req,
    {
      #lock{
        holders = Holders0,
        graph = Graph0
      } = Lock,
      #state{
        requests = Requests0
      } = State
    }
)->

  Proxy ! ?reply(Tag, #locked{}),
  stop_timer(Timer),
  Graph = elock_graph:remove_edges(Ref, Graph0),

  Requests = Requests0#{
    Ref => Req#req{
      has_lock = true,
      proxy = undefined,
      tag = undefined,
      timer = undefined
    }
  },
  Holders = Holders0#{ Ref => {Shared, ClientPID} },

  {
    Lock#lock{
      holders = Holders,
      can_share = Shared,
      graph = Graph
    },
    State#state{
      requests = Requests
    }
  }.

-spec unlocked(#req{}, ls()) -> ls().
unlocked(
    #req{
      client = ClientPID,
      ref = Ref,
      shared = Shared
    },
    {
      #lock{
        term = Term,
        holders = Holders0,
        can_share = CanShare0
      } = Lock,
      #state{
        requests = Requests0,
        clients = Clients0
      } = State
    }
)->
  Clients = remove_client_request(ClientPID, Term, Ref, Clients0),
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

  {
    Lock#lock{
      holders = Holders,
      can_share = CanShare
    },
    State#state{
      requests = Requests,
      clients = Clients
    }
  }.

%%-----------------------------------------------------------------
%%  The client already holds the lock
%%-----------------------------------------------------------------
-spec try_barging(#request{}, ls()) -> ls().
try_barging(
    #request{
      ref = Ref,
      client = ClientPID,
      shared = Shared,
      proxy = Proxy,
      tag = Tag
    } = Request,
    {
      #lock{
        term = Term,
        holders = Holders,
        can_share = CanShare,
        barging = Barging
      },
      #state{
        scope = Scope,
        clients = Clients
      }
    } = LS
)->
  if
    CanShare =:= false ->
      % The client holds it exclusively
      grant(Request, LS);
    Shared->
      % Ahead of the queued and the barging exclusive requests
      grant(Request, LS);
    Barging =:= undefined ->
      % Upgrade
      #client{
        locks = #{Term := ClientRequests}
      } = maps:get(ClientPID, Clients),
      % The request is not registered with the client yet
      case only_holder(ClientRequests, Holders, _Waiting = 0) of
        true ->
          grant(Request, LS);
        false->
          enqueue_barging(Request, LS)
      end;
    true->
      % A second upgrade: both wait for each other, the first one wins
      Proxy ! ?reply(Tag, #deadlock{ref = Ref, winner = {Scope, Term, node()}}),
      LS
  end.

%%=================================================================
%%  Push the queue, after every change of the holders
%%=================================================================
%%-----------------------------------------------------------------
%%  The barging request goes first
%%-----------------------------------------------------------------
-spec next(ls()) -> ls().
next({
  #lock{
    term = Term,
    holders = Holders,
    barging = Ref
  } = Lock,
  #state{
    requests = Requests,
    clients = Clients
  } = State
} = LS) when is_reference(Ref)->

  Req = #req{client = ClientPID} = maps:get(Ref, Requests),
  #client{
    locks = #{Term := ClientRequests}
  } = maps:get(ClientPID, Clients),

  % The barging request is registered with the client, but it is not a holder
  case only_holder(ClientRequests, Holders, _Waiting = 1) of
    true ->
      locked(Req, {
        Lock#lock{
          barging = undefined
        },
        State
      });
    false->
      LS
  end;

next({
  #lock{
    holders = Holders,
    queue = Queue0
  } = Lock,
  #state{
    requests = Requests
  } = State
} = LS0) when map_size(Holders) =:= 0->
  case gb_sets:is_empty(Queue0) of
    true->
      % Nobody needs the Term, put_lock/1 drops it
      LS0;
    false->
      {{_Seq, Ref}, Queue} = gb_sets:take_smallest(Queue0),
      Req = maps:get(Ref, Requests),
      LS = locked(Req, {
        Lock#lock{
          queue = Queue
        },
        State
      }),
      % A shared head may be joined by the next ones
      next(LS)
  end;

next({
  #lock{
    can_share = true,
    queue = Queue0
  } = Lock,
  #state{
    requests = Requests
  } = State
} = LS0)->
  case gb_sets:is_empty(Queue0) of
    true->
      LS0;
    false->
      {_Seq, Ref} = gb_sets:smallest(Queue0),
      case Requests of
        #{Ref := Req = #req{shared = true}} ->
          {_, Queue} = gb_sets:take_smallest(Queue0),
          LS = locked(Req, {
            Lock#lock{
              queue = Queue
            },
            State
          }),
          next(LS);
        _->
          LS0
      end
  end;

next(LS)->
  LS.

%%=================================================================
%%  The locks of the manager
%%
%%  A handler loads the #lock{} of one Term, passes it through the
%%  queue with the state and stores it once. The queue never touches
%%  #state.locks
%%=================================================================
-spec new_lock(term()) -> #lock{}.
new_lock(Term)->
  #lock{
    term = Term,
    holders = #{},
    queue = gb_sets:empty(),
    can_share = true,
    barging = undefined,
    graph = undefined
  }.

%%-----------------------------------------------------------------
%%  A request by its ref, and its lock. undefined: the request has
%%  left
%%-----------------------------------------------------------------
-spec get_req(reference(), #state{}) -> {#req{}, #lock{}} | undefined.
get_req(
    Ref,
    #state{
      locks = Locks,
      requests = Requests
    }
)->
  case Requests of
    #{Ref := #req{term = Term} = Req}->
      {Req, maps:get(Term, Locks)};
    _->
      undefined
  end.

%%-----------------------------------------------------------------
%%  The only place a Term is created or dropped. No holders: next/1
%%  has left nobody waiting either, the Term is free. Its entry goes,
%%  the next client takes it by itself (see lock/2)
%%-----------------------------------------------------------------
-spec put_lock(ls()) -> #state{}.
put_lock({
  #lock{
    term = Term,
    holders = Holders
  },
  #state{
    table = Table,
    locks = Locks
  } = State
}) when map_size(Holders) =:= 0->
  ets:delete(Table, Term),
  State#state{
    locks = maps:remove(Term, Locks)
  };
put_lock({
  #lock{
    term = Term
  } = Lock,
  #state{
    locks = Locks
  } = State
})->
  State#state{
    locks = Locks#{ Term => Lock }
  }.

%%=================================================================
%%  Deadlock probes (see elock_graph)
%%
%%  A waiting request joins the graph of its lock in
%%  handle_add_held_locks/2 and leaves it in locked/2 or dequeue/2
%%=================================================================
-spec handle_add_held_locks(#add_held_locks{}, #state{}) -> #state{}.
handle_add_held_locks(
    #add_held_locks{
      ref = Ref,
      held = Update
    },
    #state{
      scope = Scope
    } = State
)->
  case get_req(Ref, State) of
    {
      #req{
        has_lock = false,
        held_count = Weight
      },
      #lock{
        term = Term,
        graph = Graph0
      } = Lock
    }->
      Graph = elock_graph:add_edges(Ref, {Scope, Term, node()}, Update, Weight, Graph0),
      put_lock({
        Lock#lock{
          graph = Graph
        },
        State
      });
    _->
      % Granted or left meanwhile
      State
  end.

%%-----------------------------------------------------------------
%%  The probe is addressed to a lock of this manager by its key. The
%%  closers are aborted by the ref: an abort may grant a later
%%  closer, which abort_waiter/3 then skips. The probe is forwarded
%%  on the graph after the aborts
%%-----------------------------------------------------------------
-spec handle_deadlock_probe(#deadlock_probe{}, #state{}) -> #state{}.
handle_deadlock_probe(
    #deadlock_probe{
      edge = Winner,
      target = {_Scope, Term, _Node}
    } = Probe,
    #state{
      locks = Locks
    } = State
)->
  case Locks of
    #{Term := #lock{graph = Graph0} = Lock}->
      LS =
        case elock_graph:probe(Probe, Graph0) of
          {forward, AbortRefs}->
            {#lock{graph = Graph}, _} = LS1 = lists:foldl(
              fun(Ref, Acc)->
                abort_waiter(Ref, Winner, Acc)
              end,
              {Lock, State},
              AbortRefs
            ),
            elock_graph:forward(Probe, Graph),
            LS1;
          stop->
            {Lock, State}
        end,
      put_lock(LS);
    _->
      % The waiters have left meanwhile: nothing to close, nothing to forward
      State
  end.

%%=================================================================
%%  Utilities
%%=================================================================
-spec add_client_request(pid(), term(), reference(), clients()) -> clients().
add_client_request(ClientPID, Term, Ref, Clients)->
  Client =
    case Clients of
      #{ClientPID := Client0}->
        #client{ locks = Locks} = Client0,
        ClientRequests = maps:get(Term, Locks, #{}),
        Client0#client{
          locks = Locks#{ Term => ClientRequests#{ Ref => true } }
        };
      _->
        #client{
          locks = #{ Term => #{ Ref => true } },
          monitor_ref = erlang:monitor(process, ClientPID)
        }
    end,
  Clients#{
    ClientPID => Client
  }.

-spec remove_client_request(pid(), term(), reference(), clients()) -> clients().
remove_client_request(ClientPID, Term, Ref, Clients0)->
  Client0 = maps:get(ClientPID, Clients0),
  #client{
    locks = Locks0,
    monitor_ref = MonRef
  } = Client0,

  Locks =
    case maps:remove(Ref, maps:get(Term, Locks0)) of
      ClientRequests when map_size(ClientRequests) =:= 0 ->
        maps:remove(Term, Locks0);
      ClientRequests->
        Locks0#{ Term => ClientRequests }
    end,
  if
    map_size(Locks) =:= 0 ->
      erlang:demonitor(MonRef),
      maps:remove(ClientPID, Clients0);
    true->
      Client = Client0#client{
        locks = Locks
      },
      Clients0#{
        ClientPID => Client
      }
  end.

%%-----------------------------------------------------------------
%%  ClientRequests are the requests of the client for the Term
%%-----------------------------------------------------------------
-spec client_holds_lock(client_requests(), requests()) -> boolean().
client_holds_lock(ClientRequests, Requests)->
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
%%  add_busy_request/2), so all its requests for the Term are holders
%%  except the barging one, counted in Waiting
%%-----------------------------------------------------------------
-spec only_holder(client_requests(), holders(), 0 | 1) -> boolean().
only_holder(ClientRequests, Holders, Waiting)->
  map_size(ClientRequests) - Waiting =:= map_size(Holders).

%%-----------------------------------------------------------------
%%  The #req{} of a waiter. Term is #lock.term, not the copy the
%%  request has brought. Seq is undefined for a barging request
%%-----------------------------------------------------------------
-spec start_waiting(#request{}, term(), pos_integer() | undefined) -> #req{}.
start_waiting(
    #request{
      client = ClientPID,
      ref = Ref,
      proxy = Proxy,
      tag = Tag,
      shared = Shared,
      held_count = HeldCount,
      timeout = Timeout
    } = Request,
    Term,
    Seq
)->
  notify_queued(Request),
  #req{
    client = ClientPID,
    ref = Ref,
    term = Term,
    seq = Seq,
    proxy = Proxy,
    tag = Tag,
    shared = Shared,
    held_count = HeldCount,
    has_lock = false,
    timer = start_timer(Ref, Timeout)
  }.

%%-----------------------------------------------------------------
%%  The held map is asked for only on a wait: most requests never
%%  wait. A single node request of a client that holds nothing can
%%  not be on a cycle
%%-----------------------------------------------------------------
-spec notify_queued(#request{}) -> ok.
notify_queued(#request{
  ref = Ref,
  proxy = Proxy,
  tag = Tag,
  held_count = HeldCount,
  nodes = Nodes
})->
  if
    HeldCount > 0; tl(Nodes) =/= []->
      Proxy ! ?reply(Tag, #queued{
        ref = Ref,
        manager = self(),
        node = node()
      });
    true ->
      ignore
  end,
  ok.

-spec start_timer(reference(), pos_integer() | undefined) -> reference() | undefined.
start_timer(_Ref, undefined)->
  undefined;
start_timer(Ref, Timeout)->
  erlang:start_timer(Timeout, self(), {timeout, Ref}).

-spec stop_timer(reference() | undefined) -> ok.
stop_timer(undefined)->
  ok;
stop_timer(Timer)->
  erlang:cancel_timer(Timer, [{async, true}, {info, false}]).

-spec can_share(holders()) -> boolean().
can_share(Holders)->
  can_share_loop( maps:next( maps:iterator(Holders) ) ).

-spec can_share_loop(none | {reference(), holder(),
                             maps:iterator(reference(), holder())}) -> boolean().
can_share_loop({_Ref, {_Shared = false, _ClientPID}, _Iterator})->
  false;
can_share_loop({_Ref, {_Shared, _ClientPID}, Iterator})->
  can_share_loop( maps:next(Iterator) );
can_share_loop(none)->
  true.
