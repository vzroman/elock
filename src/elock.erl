
%%=================================================================
%%  The public API. The logic is in elock_scope.erl (the scope) and
%%  elock_context.erl (the locks)
%%=================================================================
-module(elock).

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

-export_type([lock_options/0, lock_result/0]).

-type lock_options() :: #{
  is_shared => boolean(),
  timeout => pos_integer() | undefined
}.
-type lock_result() :: {ok, reference()} | {error, term()}.

%%=================================================================
%%	OTP API
%%=================================================================
-spec start_link(atom()) -> {ok, pid()}.
start_link(Scope)->
  elock_scope:start_link(Scope).

%%=================================================================
%%	API
%%=================================================================
-spec lock(atom(), term(), nonempty_list(node())) -> lock_result().
lock(Scope, Term, Nodes)->
  elock_context:lock(Scope, Term, Nodes).

% The boolean form is deprecated, see lock/5
-spec lock(atom(), term(), boolean(), timeout()) ->
    {ok, fun(() -> ok)} | {error, term()};
  (atom(), term(), nonempty_list(node()), lock_options()) -> lock_result().
lock(Scope, Term, NodesOrIsShared, OptionsOrTimeout)->
  elock_context:lock(Scope, Term, NodesOrIsShared, OptionsOrTimeout).

% Deprecated. UnlockFun() must be called by the locking process
-spec lock(atom(), term(), boolean(), timeout(), [node()]) ->
  {ok, fun(() -> ok)} | {error, term()}.
lock(Scope, Term, IsShared, Timeout, Nodes)->
  elock_context:lock(Scope, Term, IsShared, Timeout, Nodes).

-spec unlock(reference()) -> ok.
unlock(Ref)->
  elock_context:unlock(Ref).

-spec ready_nodes(atom()) -> [node()].
ready_nodes(Scope)->
  elock_scope:ready_nodes(Scope).
