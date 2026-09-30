
-module(elock).

-include("elock.hrl").

%%=================================================================
%%	OTP API
%%=================================================================
-export([
  start_link/1
]).

%%=================================================================
%%	API
%%=================================================================
-export([
  lock/3, lock/4, lock/5,
  unlock/1,
  ready_nodes/1
]).

% lock/4 is not listed: only its boolean form is deprecated
-deprecated([{lock, 5, "Use lock/3 or lock/4 with nodes and options, then unlock/1"}]).

-define(context,'$elock_context$').
-define(pg_scope(Scope),list_to_atom(atom_to_list(Scope)++"_$pg$")).

-record(context,{
  ref2lock,   % #{ Ref => #lock{} }
  locked,     % #{ {Scope, Term, Node} => Manager } - the held map, sent to
              % the managers as is. Scope is in the key: the same Term in two
              % scopes is two locks
  counts      % #{ {Scope, Term, Node} => N }, N >= 2: the re-entered keys only
}).

-record(lock,{
  scope,
  term,
  nodes
}).

%%=================================================================
%%	OTP API
%%=================================================================
start_link(Scope)->
  {ok, spawn_link(fun()->

    % Prepare the storage for locks
    ets:new(Scope,[
      named_table,
      public,
      set,
      {read_concurrency, true},
      {write_concurrency, auto}
    ]),

    PgScope = ?pg_scope(Scope),
    case pg:start_link( PgScope ) of
      {ok,_} -> ok;
      {error,{already_started,_}}->ok;
      {error,Error}-> throw({pg_error, Error})
    end,
    pg:join(PgScope, {?MODULE,'$members$'}, self() ),

    timer:sleep(infinity)
  end)}.

%%=================================================================
%%	API
%%=================================================================
lock(Scope, Term, Nodes)->
  lock(Scope, Term, Nodes, _Options = #{}).
% Deprecated boolean form, see lock/5
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

unlock(Ref)->
  unlock(Ref, erase_context()).
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
  unlock_nodes(Nodes, Ref),
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
add_lock(
    #lock{
      scope = Scope,
      term = Term,
      nodes = Nodes
    },
    LockedCounts
)->
  maps:fold(
    fun(Node, Manager, {Locked, Counts})->
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

remove_lock(
    #lock{
      scope = Scope,
      term = Term,
      nodes = Nodes
    },
    LockedCounts
)->
  maps:fold(
    fun(Node, Manager, {Locked, Counts} = Acc)->
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

held_locks(#context{locked = Locked})->
  Locked;
held_locks(_NoContext)->
  #{}.

held_count(Context)->
  map_size(held_locks(Context)).

ready_nodes(Scope)->
  [node(PID)|| PID <- pg:get_members(?pg_scope(Scope), {?MODULE,'$members$'})].

%%=================================================================
%%	REQUEST
%%=================================================================
-record(waiting,{
  ref,
  scope,
  term,
  held,       % #{ {Scope, Term, Node} => Manager } - the locks the client holds
  pending,    % #{ MonRef => Node } - the workers that have not returned yet
  nodes,      % #{ Node => Manager } - the grants so far
  queued      % #{ Node => Manager } - the managers the request waits at
}).

% The local node alone: the client is the proxy itself
run_request(
    _Ref,
    #request{
      nodes = [Node]
    } = Request,
    HeldLocks
) when Node =:= node() ->
  case elock_manager:lock(Request, HeldLocks) of
    {ok, Manager} ->
      {ok, #{ Node => Manager }};
    Error ->
      Error
  end;
% A worker per node is the proxy, it exits with the result. The client
% answers #queued{} meanwhile. The monitor tag is Ref, so every message
% of the wait carries Ref
run_request(
    Ref,
    #request{
      scope = Scope,
      term = Term,
      nodes = Nodes
    } = Request,
    HeldLocks
)->
  Pending =
    lists:foldl(
      fun(N, Acc)->
        {_Pid, MonRef} = spawn_opt(
          fun()->
            exit( ecall_connection:call(N, elock_manager, lock, [Request]) )
          end,
          [{monitor, [{tag, Ref}]}]
        ),
        Acc#{ MonRef => N }
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

% Every clause matches Ref, a plain argument from make_ref/0 in lock/4:
% the receive skips the older messages. Ref taken from #waiting{} would
% break the optimization
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
    {Ref, MonRef, process, _Pid, NodeResult} when is_map_key(MonRef, Pending0)->
      {Node, Pending} = maps:take(MonRef, Pending0),
      case NodeResult of
        {ok, {ok,Manager}} ->
          % The queued managers already have the rest
          Queued = maps:remove(Node, Queued0),
          notify_queued(Queued, #{ {Scope, Term, Node} => Manager }, Ref),

          Nodes = Nodes0#{
            Node => Manager
          },
          Waiting = Waiting0#waiting{
            pending = Pending,
            queued = Queued,
            nodes = Nodes
          },
          wait_verdict(Ref, Waiting);
        Error ->
          unlock_nodes(Nodes0, Ref),
          % Otherwise they block their queues and probe with the released locks
          unlock_nodes(Queued0, Ref),
          wait_unlock(Ref, Pending),
          Error
      end;
    #queued{ref = Ref, manager = Manager, node = Node}->
      % The locks of the client and the grants so far
      Held =
        maps:fold(
          fun(N, M, Acc)->
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
wait_unlock(Ref, Pending0)
  when map_size(Pending0) > 0->
  receive
    {Ref, MonRef, process, _Pid, NodeResult} when is_map_key(MonRef, Pending0)->
      Pending = maps:remove(MonRef, Pending0),
      case NodeResult of
        {ok, {ok, Manager}} ->
          ecall:send(Manager, #unlock{ref = Ref});
        _->
          ignore
      end,
      wait_unlock(Ref, Pending);
    #queued{ref = Ref, manager = Manager}->
      ecall:send(Manager, #unlock{ref = Ref}),
      wait_unlock(Ref, Pending0)
  end;
wait_unlock(_Ref, _Pending)->
  ok.

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
%%	UTILITIES
%%=================================================================
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

get_context()->
  get(?context).
put_context(Context)->
  put(?context, Context).
erase_context()->
  erase(?context).

unlock_nodes(Nodes, Ref) when map_size(Nodes) > 0->
  [ ecall:send(Manager, #unlock{ref = Ref}) || Manager <- maps:values(Nodes) ],
  ok;
unlock_nodes(_Locked, _Ref)->
  ok.
