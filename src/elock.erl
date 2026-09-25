
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
-define(pg_scope(Scope),list_to_atom(atom_to_list(Scope)++"_$pg$")).

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
    {ok, Nodes} ->
      locked(Request, Nodes, Context),
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
    Nodes,
    #context{
      ref2lock = Ref2Lock0,
      locked = Locked0
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

  Locked = add_lock(Lock, Locked0),

  Context = Context0#context{
    ref2lock = Ref2Lock,
    locked = Locked
  },
  put_context(Context);
locked(Request, Nodes, _NoContext)->
  Context = #context{
    ref2lock = #{},
    locked = #{}
  },
  locked(Request, Nodes, Context).

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
    nodes = Nodes
  } = Lock,
  unlock_nodes(Nodes, Ref),
  if
    map_size(Ref2Lock) =:= 0 ->
      no_locks_remain;
    true ->
      Locked = remove_lock(Lock, Locked0),
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

% Locked structure:
% #{
%   {Term, Node} => {Manager, Count}
% }
add_lock(
    #lock{
      scope = Scope,
      term = Term,
      nodes = Nodes
    },
    Locked
)->
  ScopeLocked0 = maps:get(Scope, Locked, #{}),
  ScopeLocked =
    maps:fold(
      fun(Node, Manager, Acc)->
        Key = {Term, Node},
        case Acc of
          #{Key := {Manager, Count}}->
            Acc#{ Key => {Manager, Count + 1}};
          #{Key := {_StaleManager, StaleCount}}->
            ?LOGWARNING("~p has stale lock: ~p, count: ~p",[self(), Key, StaleCount]),
            Acc#{ Key => {Manager, 1}};
          _->
            Acc#{ Key => {Manager, 1}}
        end
      end,
      ScopeLocked0,
      Nodes
    ),
  Locked = Locked#{
    Scope => ScopeLocked
  }.

remove_lock(
    #lock{
      scope = Scope,
      term = Term,
      nodes = Nodes
    },
    Locked
)->
  ScopeLocked0 = maps:get(Scope, Locked),
  ScopeLocked =
    maps:fold(
      fun(Node, Manager, Acc)->
        Key = {Term, Node},
        case Acc of
          #{Key := {Manager, Count}} ->
            if
              Count =:= 1 ->
                maps:remove(Key, Acc);
              true ->
                Acc#{ Key => {Manager, Count - 1}}
            end;
          #{Key := {_NewManager, _Count}}->
            ?LOGWARNING("~p unlocked stale lock ~p",[self(), Key]),
            Acc;
          _->
            Acc
        end
      end,
      ScopeLocked0,
      Nodes
    ),
  Locked =
    if
      map_size(ScopeLocked) > 0 ->
        Locked#{
          Scope => ScopeLocked
        };
      true ->
        maps:remove(Scope, Locked)
    end.

held_locks(
    Scope,
    #context{
      locked = Locked
    }
)->
  case Locked of
    #{ Scope := ScopeLocked }->
      maps:fold(
        fun(Key, {Manager, _Count}, Acc)->
          Acc#{
            Key => Manager
          }
        end,
        #{},
        ScopeLocked
      );
    _->
      #{}
  end;
held_locks(_Scope, _NoContext)->
  #{}.

ready_nodes(Scope)->
  [node(PID)|| PID <- pg:get_members(?pg_scope(Scope), {?MODULE,'$members$'})].

%%=================================================================
%%	REQUEST
%%=================================================================
-record(waiting,{
  ref,
  term,
  pending,
  nodes,
  queued
}).

run_request(#request{
  nodes = [Node]
} = Request) when Node =:= node() ->
  case elock_manager:lock(Request) of
    {ok, Manager} ->
      {ok, #{ Node => Manager }};
    Error ->
      Error
  end;
run_request(#request{
  nodes = [Node]
} = Request) ->
  case ecall:call(Node, elock_manager, lock, [Request]) of
    {ok, {ok, Manager}} ->
      {ok, #{ Node => Manager }};
    Error ->
      Error
  end;
run_request(#request{
  ref = Ref,
  term = Term,
  nodes = Nodes
} = Request) ->
  Pending =
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
  wait_verdict(#waiting{
    ref = Ref,
    term = Term,
    pending = Pending,
    nodes = #{},
    queued = #{}
  }).

wait_verdict(#waiting{
  ref = Ref,
  term = Term,
  pending = Pending0,
  queued = Queued0,
  nodes = Nodes0
} = Waiting0)
  when map_size(Pending0) > 0->
  receive
    {'DOWN', MonRef, process, _P, NodeResult} when is_map_key(MonRef, Pending0)->
      {Node, Pending} = maps:take(MonRef, Pending0),
      case NodeResult of
        {ok, {ok,Manager}} ->

          Queued = maps:remove(Node, Queued0),
          notify_queued(Queued, #{Node => Manager}, Term, Ref),

          Nodes = Nodes0#{
            Node => Manager
          },
          Waiting = Waiting0#waiting{
            pending = Pending,
            queued = Queued,
            nodes = Nodes
          },
          wait_verdict(Waiting);
        Error ->
          unlock_nodes(Nodes0, Ref),
          wait_unlock(Pending, Ref),
          Error
      end;
    #queued{ref = Ref, node = Node} when is_map_key(Node, Nodes0)->
      % The grant overtook the notification: the node is held already
      % and there is nothing to notify. Should the request fail, the
      % granted nodes are unlocked through unlock_nodes/2 anyway
      wait_verdict(Waiting0);
    #queued{ref = Ref, manager = Manager, node = Node}->
      notify_queued(#{Node => Manager}, Nodes0, Term, Ref),
      Queued = Queued0#{
        Node => Manager
      },
      Waiting = Waiting0#waiting{
        queued = Queued
      },
      wait_verdict(Waiting)
  end;
wait_verdict(#waiting{
  nodes = Nodes
})->
  {ok, Nodes}.

wait_unlock(Pending0, Ref)
  when map_size(Pending0) > 0->
  receive
    {'DOWN', MonRef, process, _P, NodeResult} when is_map_key(MonRef, Pending0)->
      Pending = maps:remove(MonRef, Pending0),
      case NodeResult of
        {ok, {ok, Manager}} ->
          catch ecall:send(Manager, #unlock{ref = Ref});
        _->
          ignore
      end,
      wait_unlock(Pending, Ref);
    #queued{ref = Ref, manager = Manager}->
      catch ecall:send(Manager, #unlock{ref = Ref}),
      wait_unlock(Pending0, Ref)
  end;
wait_unlock(_Calls, _Ref)->
  ok.

notify_queued(Queued, Locked, Term, Ref)
  when map_size(Queued) > 0, map_size(Locked) > 0->
  Held =
    maps:fold(
      fun(Node, Manager, Acc)->
        Acc#{
          {Term, Node} => Manager
        }
      end,
      #{},
      Locked
    ),
  Message = #add_held_locks{
    ref = Ref,
    held = Held
  },
  [ catch ecall:send(Manager, Message) || Manager <- maps:values(Queued) ],
  ok;
notify_queued(_Queued, _Locked, _Term, _Ref)->
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

unlock_nodes(Nodes, Ref) when map_size(Nodes) > 0->
  [ecall:send(Manager, #unlock{ref = Ref}) || Manager <- maps:values(Nodes)],
  ok;
unlock_nodes(_Locked, _Ref)->
  ok.
