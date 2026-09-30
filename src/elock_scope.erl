
%%=================================================================
%%  The scope: the process that owns the ETS table of the locks and
%%  announces the node to the other nodes of the scope.
%%=================================================================
-module(elock_scope).
-moduledoc false.

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
  ready_nodes/1
]).

-define(pg_scope(Scope),list_to_atom(atom_to_list(Scope)++"_$pg$")).
% Named after the API module: the nodes look each other up by it
-define(pg_group,{elock,'$members$'}).

%%=================================================================
%%	OTP API
%%=================================================================
-spec start_link(atom()) -> {ok, pid()}.
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
    pg:join(PgScope, ?pg_group, self() ),

    timer:sleep(infinity)
  end)}.

%%=================================================================
%%	API
%%=================================================================
-spec ready_nodes(atom()) -> [node()].
ready_nodes(Scope)->
  [node(PID)|| PID <- pg:get_members(?pg_scope(Scope), ?pg_group)].
