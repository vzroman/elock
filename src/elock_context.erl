
%%=================================================================
%%  The logic behind the lock API of elock.erl: the context of the
%%  locks a client process holds (kept in its dictionary) and the
%%  client side of a request.
%%=================================================================
-module(elock_context).
-moduledoc false.

-include("elock.hrl").

%%=================================================================
%%	API
%%=================================================================
-export([
  lock/3, lock/4, lock/5,
  unlock/1
]).

-record(node,{
  manager :: pid(),
  holder :: pid() | undefined % worker PID; undefined for a single local request
}).

-type node_managers() :: #{node() => #node{}}.
-type queued_managers() :: #{node() => pid()}.
-type lock_counts() :: #{lock_key() => pos_integer()}.
-type pending_workers() :: #{node() => pid()}.
-type request_result() :: {ok, node_managers()} | {error, term()}.

-define(context,'$elock_context$').

-record(lock,{
  scope :: atom(),
  term :: term(),
  nodes :: node_managers()
}).

-record(context,{
  ref2lock :: #{reference() => #lock{}},
  % Sent to managers as is. The same Term in two scopes is two locks.
  locked :: held_locks(),
  counts :: lock_counts() % N >= 2: the re-entered keys only
}).

-type context() :: #context{} | undefined.

%%=================================================================
%%	API
%%=================================================================
-spec lock(atom(), term(), nonempty_list(node())) -> elock:lock_result().
lock(Scope, Term, Nodes)->
  lock(Scope, Term, Nodes, _Options = #{}).
% Deprecated boolean form, see lock/5
-spec lock(atom(), term(), boolean(), timeout()) ->
    {ok, fun(() -> ok)} | {error, term()};
  (atom(), term(), nonempty_list(node()), elock:lock_options()) -> elock:lock_result().
lock(Scope, Term, IsShared, Timeout) when is_boolean(IsShared)->
  lock(Scope, Term, IsShared, Timeout, [node()]);
lock(Scope, Term, Nodes, Options)->
  validate_nodes(Nodes),
  #{
    timeout := Timeout,
    is_shared := IsShared
  } = validate_options(Options),
  Ref = make_ref(),
  Context = get_context(),
  HeldLocks = held_locks(Context),
  Request = #request{
    ref = Ref,
    scope = Scope,
    term = Term,
    nodes = lists:usort(Nodes),
    held_count = held_count(Context),
    client = self(),
    timeout = Timeout,
    shared = IsShared
  },
  % Ref is passed as a plain argument for the receive marker optimization
  % (see wait_verdict/2)
  case run_request(Ref, Request, HeldLocks) of
    {ok, LockedNodes} ->
      locked(Request, LockedNodes, Context),
      {ok, Ref};
    Error ->
      Error
  end.

% Deprecated. UnlockFun() must be called by the locking process
-spec lock(atom(), term(), boolean(), timeout(), [node()]) ->
  {ok, fun(() -> ok)} | {error, term()}.
lock(_Scope, _Term, _IsShared, _Timeout, [])->
  {ok, fun()-> ok end};
lock(_Scope, _Term, _IsShared, 0, _Nodes)->
  {error, timeout};
lock(Scope, Term, IsShared, Timeout, Nodes)->
  Options = #{
    is_shared => IsShared,
    timeout => case Timeout of infinity -> undefined; _ -> Timeout end
  },
  case lock(Scope, Term, Nodes, Options) of
    {ok, Ref}->
      {ok, fun()-> unlock(Ref) end};
    {error, {deadlock, _Winner}}->
      {error, deadlock};
    Error->
      Error
  end.

-spec locked(#request{}, node_managers(), context()) -> context().
locked(
    #request{
      ref = Ref,
      scope = Scope,
      term = Term
    },
    Nodes,
    #context{
      ref2lock = Ref2Lock0,
      locked = Locked0,
      counts = Counts0
    } = Context0
)->
  Lock = #lock{
    scope = Scope,
    term = Term,
    nodes = Nodes
  },
  Ref2Lock = Ref2Lock0#{
    Ref => Lock
  },

  {Locked, Counts} = add_lock(Lock, {Locked0, Counts0}),

  Context = Context0#context{
    ref2lock = Ref2Lock,
    locked = Locked,
    counts = Counts
  },
  put_context(Context);
locked(Request, Nodes, _NoContext)->
  Context = #context{
    ref2lock = #{},
    locked = #{},
    counts = #{}
  },
  locked(Request, Nodes, Context).

-spec unlock(reference()) -> ok.
unlock(Ref)->
  unlock(Ref, erase_context()).
-spec unlock(reference(), context()) -> ok.
unlock(
    Ref,
    #context{
      ref2lock = Ref2Lock0,
      locked = Locked0,
      counts = Counts0
    } = Context
) when is_map_key(Ref, Ref2Lock0)->
  {Lock, Ref2Lock} = maps:take(Ref, Ref2Lock0),
  #lock{
    nodes = Nodes
  } = Lock,
  release(Nodes, Ref),
  if
    map_size(Ref2Lock) =:= 0 ->
      no_locks_remain;
    true ->
      {Locked, Counts} = remove_lock(Lock, {Locked0, Counts0}),
      put_context(Context#context{
        ref2lock = Ref2Lock,
        locked = Locked,
        counts = Counts
      })
  end,
  ok;
unlock(_UnexpectedRef, #context{} = Context)->
  put_context(Context),
  ok;
unlock(_Ref, _NoContext)->
  ok.

% {Locked, Counts} as in #context{}. A key held by another Manager is
% stale: its manager has died and a new one has taken the Term
-spec add_lock(#lock{}, {held_locks(), lock_counts()}) ->
  {held_locks(), lock_counts()}.
add_lock(
    #lock{
      scope = Scope,
      term = Term,
      nodes = Nodes
    },
    LockedCounts
)->
  maps:fold(
    fun(Node, #node{manager = Manager}, {Locked, Counts})->
      Key = {Scope, Term, Node},
      case Locked of
        #{Key := Manager}->
          Count = maps:get(Key, Counts, 1),
          {Locked, Counts#{ Key => Count + 1 }};
        #{Key := _StaleManager}->
          ?LOGWARNING("~p has stale lock: ~p, count: ~p",[self(), Key, maps:get(Key, Counts, 1)]),
          {Locked#{ Key => Manager }, maps:remove(Key, Counts)};
        _->
          {Locked#{ Key => Manager }, Counts}
      end
    end,
    LockedCounts,
    Nodes
  ).

-spec remove_lock(#lock{}, {held_locks(), lock_counts()}) ->
  {held_locks(), lock_counts()}.
remove_lock(
    #lock{
      scope = Scope,
      term = Term,
      nodes = Nodes
    },
    LockedCounts
)->
  maps:fold(
    fun(Node, #node{manager = Manager}, {Locked, Counts} = Acc)->
      Key = {Scope, Term, Node},
      case Locked of
        #{Key := Manager} ->
          case Counts of
            #{Key := 2}->
              {Locked, maps:remove(Key, Counts)};
            #{Key := Count}->
              {Locked, Counts#{ Key => Count - 1 }};
            _->
              {maps:remove(Key, Locked), Counts}
          end;
        #{Key := _NewManager}->
          ?LOGWARNING("~p unlocked stale lock ~p",[self(), Key]),
          Acc;
        _->
          Acc
      end
    end,
    LockedCounts,
    Nodes
  ).

-spec held_locks(context()) -> held_locks().
held_locks(#context{locked = Locked})->
  Locked;
held_locks(_NoContext)->
  #{}.

-spec held_count(context()) -> non_neg_integer().
held_count(Context)->
  map_size(held_locks(Context)).

%%=================================================================
%%	REQUEST
%%=================================================================
-record(waiting,{
  ref :: reference(),
  scope :: atom(),
  term :: term(),
  held :: held_locks(),         % the locks the client holds
  pending :: pending_workers(), % the workers that have not returned yet
  nodes :: node_managers(),     % the grants so far
  queued :: queued_managers()   % the managers the request waits at
}).

% The local node alone: the client is the proxy itself
-spec run_request(reference(), #request{}, held_locks()) -> request_result().
run_request(
    _Ref,
    #request{
      nodes = [Node]
    } = Request,
    HeldLocks
) when Node =:= node() ->
  case elock_manager:lock(Request, HeldLocks) of
    {ok, Manager} ->
      {ok, #{ Node => #node{manager = Manager} }};
    Error ->
      Error
  end;
% A worker per node sends its result tagged with Ref. A remote grant
% keeps the worker as its holder, monitoring the client locally until
% release. The client answers #queued{} meanwhile
run_request(
    Ref,
    #request{
      scope = Scope,
      term = Term,
      nodes = Nodes
    } = Request,
    HeldLocks
)->
  Client = self(),
  Pending =
    lists:foldl(
      fun(N, Acc)->
        Holder = spawn(fun()->
          Result = ecall_connection:call(N, elock_manager, lock, [Request]),
          Client ! {Ref, N, Result},
          case Result of
            {ok, {ok, Manager}} when N =/= node()->
              hold(Manager, Ref, Client);
            _->
              ok
          end
        end),
        Acc#{ N => Holder }
      end,
      #{},
      Nodes
    ),
  wait_verdict(Ref, #waiting{
    ref = Ref,
    scope = Scope,
    term = Term,
    held = HeldLocks,
    pending = Pending,
    nodes = #{},
    queued = #{}
  }).

% Workers reply with their node and result; remote grants keep a holder.
% Every clause matches Ref, a plain argument from make_ref/0 in lock/4:
% the receive skips the older messages. Ref taken from #waiting{} would
% break the optimization
-spec wait_verdict(reference(), #waiting{}) -> request_result().
wait_verdict(
    Ref,
    #waiting{
      scope = Scope,
      term = Term,
      held = Held0,
      pending = Pending0,
      queued = Queued0,
      nodes = Nodes0
    } = Waiting0
) when map_size(Pending0) > 0->
  receive
    {Ref, Node, NodeResult}->
      {Holder, Pending} = maps:take(Node, Pending0),
      case NodeResult of
        {ok, {ok,Manager}} ->
          % The queued managers already have the rest
          Queued = maps:remove(Node, Queued0),
          notify_queued(Queued, #{ {Scope, Term, Node} => Manager }, Ref),

          Nodes = Nodes0#{
            Node => #node{manager = Manager, holder = Holder}
          },
          Waiting = Waiting0#waiting{
            pending = Pending,
            queued = Queued,
            nodes = Nodes
          },
          wait_verdict(Ref, Waiting);
        Error ->
          release(Nodes0, Ref),
          % Otherwise they block their queues and probe with the released locks
          unlock_nodes(Queued0, Ref),
          % A local worker is also its proxy and withdrawal may kill it
          % before it can reply. Remote workers report their proxy's exit.
          LocalMonRef = case maps:find(node(), Pending) of
            {ok, LocalWorker}->
              erlang:monitor(process, LocalWorker, [{tag, {local, Ref}}]);
            error->
              undefined
          end,
          wait_unlock(Ref, Pending),
          % A result can finish the drain before the worker's DOWN arrives.
          case LocalMonRef of
            undefined-> ok;
            _-> erlang:demonitor(LocalMonRef, [flush])
          end,
          Error
      end;
    #queued{ref = Ref, manager = Manager, node = Node}->
      % The locks of the client and the grants so far
      Held =
        maps:fold(
          fun(N, #node{manager = M}, Acc)->
            Acc#{
              {Scope, Term, N} => M
            }
          end,
          Held0,
          Nodes0
        ),
      notify_queued(#{Node => Manager}, Held, Ref),
      Queued = Queued0#{
        Node => Manager
      },
      Waiting = Waiting0#waiting{
        queued = Queued
      },
      wait_verdict(Ref, Waiting)
  end;
wait_verdict(
    _Ref,
    #waiting{
      nodes = Nodes
    }
)->
  {ok, Nodes}.

% The request has failed: release the late grants and withdraw the
% request from the managers that report it queued
-spec wait_unlock(reference(), pending_workers()) -> ok.
wait_unlock(Ref, Pending0)
  when map_size(Pending0) > 0->
  receive
    {Ref, Node, NodeResult}->
      {Holder, Pending} = maps:take(Node, Pending0),
      case NodeResult of
        {ok, {ok, Manager}} ->
          ecall:send(Manager, #unlock{ref = Ref}),
          kill_holder(Holder);
        _->
          ignore
      end,
      wait_unlock(Ref, Pending);
    #queued{ref = Ref, manager = Manager}->
      ecall:send(Manager, #unlock{ref = Ref}),
      wait_unlock(Ref, Pending0);
    {{local, Ref}, _MonRef, process, _Worker, _Reason}->
      wait_unlock(Ref, maps:remove(node(), Pending0))
  end;
wait_unlock(_Ref, _Pending)->
  ok.

-spec notify_queued(queued_managers(), held_locks(), reference()) -> ok.
notify_queued(Queued, Held, Ref)
  when map_size(Queued) > 0, map_size(Held) > 0->
  Message = #add_held_locks{
    ref = Ref,
    held = Held
  },
  [ ecall:send(Manager, Message) || Manager <- maps:values(Queued) ],
  ok;
notify_queued(_Queued, _Held, _Ref)->
  ok.

%%=================================================================
%%  Remote holders
%%=================================================================
-spec hold(pid(), reference(), pid()) -> #unlock{}.
hold(Manager, Ref, Client)->
  MonRef = erlang:monitor(process, Client),
  holding(MonRef, Manager, Ref).

-spec holding(reference(), pid(), reference()) -> #unlock{}.
holding(MonRef, Manager, Ref)->
  receive
    {'DOWN', MonRef, process, _Client, _Reason}->
      ecall:send(Manager, #unlock{ref = Ref});
    Unexpected->
      ?LOGWARNING("unexpected message received: ~p",[Unexpected]),
      holding(MonRef, Manager, Ref)
  end.

-spec release(node_managers(), reference()) -> ok.
release(Nodes, Ref)->
  maps:foreach(
    fun(_Node, #node{manager = Manager, holder = Holder})->
      ecall:send(Manager, #unlock{ref = Ref}),
      kill_holder(Holder)
    end,
    Nodes
  ).

-spec kill_holder(pid() | undefined) -> ok | true.
kill_holder(undefined)-> ok;
kill_holder(Holder)-> exit(Holder, kill).

%%=================================================================
%%	UTILITIES
%%=================================================================
-spec validate_nodes(term()) -> ok.
validate_nodes(Nodes)->
  if
    is_list(Nodes), length(Nodes) > 0 -> ok;
    true -> throw({invalid_nodes, Nodes})
  end,
  lists:foreach(
    fun(N)->
      if
        is_atom(N) -> ok;
        true -> throw({invalid_node, N})
      end
    end,
    Nodes
  ).

-spec validate_options(term()) ->
  #{is_shared := boolean(), timeout := pos_integer() | undefined}.
validate_options(Options)->
  if
    is_map(Options) -> ok;
    true -> throw({invalid_options, Options})
  end,
  WithDefaults = maps:merge(#{
    is_shared => false,
    timeout => undefined
  }, Options),
  maps:foreach(fun validate_option/2, WithDefaults),
  WithDefaults.

-spec validate_option(term(), term()) -> boolean() | ok.
validate_option(is_shared, Value)->
  if
    is_boolean(Value) -> Value;
    true -> throw({invalid_is_shared, Value})
  end;
validate_option(timeout, Value)->
  if
    Value =:= undefined-> ok;
    is_integer(Value), Value > 0 -> ok;
    true -> throw({invalid_timeout, Value})
  end;
validate_option(Unexpected, _Value)->
  throw({invalid_option, Unexpected}).

% The process dictionary holds only this process's lock context.
-spec get_context() -> context().
get_context()->
  get(?context).
-spec put_context(#context{}) -> context().
put_context(Context)->
  put(?context, Context).
-spec erase_context() -> context().
erase_context()->
  erase(?context).

-spec unlock_nodes(queued_managers(), reference()) -> ok.
unlock_nodes(Nodes, Ref) when map_size(Nodes) > 0->
  [ ecall:send(Manager, #unlock{ref = Ref}) || Manager <- maps:values(Nodes) ],
  ok;
unlock_nodes(_Locked, _Ref)->
  ok.
