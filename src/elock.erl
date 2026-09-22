
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
  lock/3, lock/4,
  unlock/1,
  ready_nodes/1
]).

-define(context,'$elock_context$').
-record(context,{
  ref2lock,
  locked
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

    DeadLockScope = ?deadlock_scope(Scope),
    case pg:start_link( DeadLockScope ) of
      {ok,_} -> ok;
      {error,{already_started,_}}->ok;
      {error,Error}-> throw({pg_error, Error})
    end,
    pg:join(DeadLockScope, {?MODULE,'$members$'}, self() ),

    timer:sleep(infinity)
  end)}.

%%=================================================================
%%	API
%%=================================================================
lock(Scope, Term, Nodes)->
  lock(Scope, Term, Nodes, _Options = #{}).
lock(Scope, Term, Nodes, Options)->
  validate_nodes(Nodes),
  #{
    timeout := Timeout,
    is_shared := IsShared
  } = validate_options(Options),
  Ref = make_ref(),
  Context = get_context(),
  HeldLocks = held_locks(Scope, Context),
  Request = #request{
    ref = Ref,
    scope = Scope,
    term = Term,
    nodes = lists:usort(Nodes),
    held = HeldLocks,
    client = self(),
    timeout = Timeout,
    shared = IsShared
  },
  case run_request(Request) of
    {ok, Results} ->
      locked(Request, Results, Context),
      {ok, Ref};
    Error ->
      Error
  end.

locked(
    #request{
      ref = Ref,
      scope = Scope,
      term = Term
    },
    Results,
    #context{
      ref2lock = Ref2Lock0,
      locked = Locked0
    } = Context0
)->
  Lock = #lock{
    scope = Scope,
    term = Term,
    nodes = Results
  },
  Ref2Lock = Ref2Lock0#{
    Ref => Lock
  },
  ScopeLocked0 = maps:get(Scope, Locked0, #{}),
  ScopeLocked =
    lists:foldl(
      fun(Node, Acc)->
        Key = {Term, Node},
        Count = maps:get(Key, Acc, 0),
        Acc#{ Key => Count + 1 }
      end,
      ScopeLocked0,
      maps:keys(Results)
    ),
  Locked = Locked0#{
    Scope => ScopeLocked
  },
  Context = Context0#context{
    ref2lock = Ref2Lock,
    locked = Locked
  },
  put_context(Context);
locked(Request, Results, _NoContext)->
  Context = #context{
    ref2lock = #{},
    locked = #{}
  },
  locked(Request, Results, Context).

unlock(Ref)->
  unlock(Ref, erase_context()).
unlock(
    Ref,
    #context{
      ref2lock = Ref2Lock0,
      locked = Locked0
    } = Context
) when is_map_key(Ref, Ref2Lock0)->
  {Lock, Ref2Lock} = maps:take(Ref, Ref2Lock0),
  #lock{
    scope = Scope,
    term = Term,
    nodes = Nodes
  } = Lock,

  unlock_nodes(Nodes),
  if
    map_size(Ref2Lock) =:= 0 ->
      no_locks_remains;
    true ->
      ScopeLocked0 = maps:get(Scope, Locked0),
      ScopeLocked =
        lists:foldl(
          fun(Node, Acc)->
            Key = {Term, Node},
            Count = maps:get(Key, Acc),
            if
              Count =:= 1 ->
                maps:remove(Key, Acc);
              true ->
                Acc#{ Key => Count - 1 }
            end
          end,
          ScopeLocked0,
          maps:keys(Nodes)
        ),
      Locked =
        if
          map_size(ScopeLocked) > 0 ->
            Locked0#{
              Scope => ScopeLocked
            };
          true ->
            maps:remove(Scope, Locked0)
        end,
      put_context(Context#context{
        ref2lock = Ref2Lock,
        locked = Locked
      })
  end,
  ok;
unlock(_UnexpectedRef, #context{} = Context)->
  put_context(Context),
  ok;
unlock(_Ref, _NoContext)->
  ok.

ready_nodes(Scope)->
  [node(PID)|| PID <- pg:get_members(?deadlock_scope(Scope), {?MODULE,'$members$'})].

%%=================================================================
%%	REQUEST
%%=================================================================
run_request(#request{
  nodes = [Node]
} = Request) when Node =:= node() ->
  case elock_manager:lock(Request) of
    {ok, Unlock} ->
      {ok, #{ Node => Unlock }};
    Error ->
      Error
  end;
run_request(#request{
  nodes = [Node]
} = Request) ->
  case ecall:call(Node, elock_manager, lock, [Request]) of
    {ok, {ok, Unlock}} ->
      {ok, #{ Node => Unlock }};
    Error ->
      Error
  end;
run_request(#request{
  nodes = Nodes
} = Request) ->
  Calls =
    lists:foldl(
      fun(N, Acc)->
        {_Pid, MonRef} = spawn_monitor(
          fun()->
            exit( ecall_connection:call(N, elock_manager, lock, [Request]) )
          end
        ),
        Acc#{ MonRef => N }
      end,
      #{},
      Nodes
    ),
  wait_lock(Calls, _Results = #{}).

wait_lock(Calls, Results)
  when map_size(Calls) > 0->
  receive
    {'DOWN', MonRef, process, _P, NodeResult} when is_map_key(MonRef, Calls)->
      {Node, RestCalls} = maps:take(MonRef, Calls),
      case NodeResult of
        {ok, {ok,Unlock}} ->
          wait_lock(RestCalls, Results#{ Node => Unlock });
        Error ->
          unlock_nodes(Results),
          wait_unlock(RestCalls),
          Error
      end
  end;
wait_lock(_Calls, Results)->
  {ok, Results}.

wait_unlock(Calls)
  when map_size(Calls) > 0->
  receive
    {'DOWN', MonRef, process, _P, NodeResult} when is_map_key(MonRef, Calls)->
      {Node, RestCalls} = maps:take(MonRef, Calls),
      case NodeResult of
        {ok, {ok, Unlock}} ->
          ecall:cast(Node, elock_manager, unlock, [Unlock]);
        _->
          ignore
      end,
      wait_unlock(RestCalls)
  end;
wait_unlock(_Calls)->
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
  end.

get_context()->
  get(?context).
put_context(Context)->
  put(?context, Context).
erase_context()->
  erase(?context).

held_locks(
    Scope,
    #context{
      locked = Locked
    }
)->
  case Locked of
    #{ Scope := ScopeLocked }->
      maps:keys(ScopeLocked);
    _->
      []
  end;
held_locks(_Scope, _NoContext)->
  [].

unlock_nodes(Results)->
  maps:foreach(
    fun(Node, Unlock)->
      ecall:cast(Node, elock_manager, unlock, [Unlock])
    end,
    Results
  ).
