
%%=================================================================
%%  The scope: the process that owns the pool of the managers and
%%  announces the node to the other nodes of the scope.
%%
%%  The pool is a tuple of the manager PIDs kept in a persistent
%%  term, a Term is hashed to its slot (see manager/2). The managers
%%  are linked to the scope and stop with it. The persistent term
%%  stays: the monitor of a client tells it that the manager is dead
%%  (see elock_manager:lock/2).
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
  manager/2,
  ready_nodes/1
]).

-define(pool(Scope),{?MODULE, Scope}).
-define(pg_scope(Scope),list_to_atom(atom_to_list(Scope)++"_$pg$")).
% Named after the API module: the nodes look each other up by it
-define(pg_group,{elock,'$members$'}).

%%=================================================================
%%	OTP API
%%=================================================================
%%-----------------------------------------------------------------
%%  Returns when the pool is published, or the name is taken
%%-----------------------------------------------------------------
-spec start_link(atom()) -> {ok, pid()} | {error, {already_started, pid()}}.
start_link(Scope)->
  Sup = self(),
  PID = spawn_link(fun()->init(Scope, Sup) end),
  receive
    {ready, PID}->
      {ok, PID};
    {error, PID, Error}->
      {error, Error}
  end.

-spec init(atom(), pid()) -> ok | no_return().
init(Scope, Sup)->
  try register(Scope, self()) of
    true->
      start_pool(Scope),

      PgScope = ?pg_scope(Scope),
      start_pg(PgScope),
      pg:join(PgScope, ?pg_group, self() ),

      Sup ! {ready, self()},

      timer:sleep(infinity)
  catch
    error:badarg->
      % The scope is already started on the node
      unlink(Sup),
      Sup ! {error, self(), {already_started, whereis(Scope)}},
      ok
  end.

%%-----------------------------------------------------------------
%%  A manager per scheduler.
%%  High priority: every client of a slot waits for its manager.
%%  Off heap mailbox: request bursts stay out of its garbage
%%  collection
%%-----------------------------------------------------------------
-spec start_pool(atom()) -> ok.
start_pool(Scope)->
  Size = erlang:system_info(schedulers_online),
  Pool = [
    spawn_opt(elock_manager, init, [Scope], [
      link,
      {priority, high},
      {message_queue_data, off_heap}
    ])
    || _ <- lists:seq(1, Size)
  ],
  persistent_term:put(?pool(Scope), list_to_tuple(Pool)).

%%-----------------------------------------------------------------
%%  already_started: the scope is restarted, the pg process of the
%%  previous scope process stops with it, but has not stopped yet
%%-----------------------------------------------------------------
-spec start_pg(atom()) -> ok.
start_pg(PgScope)->
  case pg:start_link( PgScope ) of
    {ok,_}->
      ok;
    {error,{already_started,PID}}->
      MonitorRef = erlang:monitor(process, PID),
      receive
        {'DOWN', MonitorRef, process, PID, _Reason}->
          start_pg(PgScope)
      end
  end.

%%=================================================================
%%	API
%%=================================================================
%%-----------------------------------------------------------------
%%  The manager of the Term. badarg if the scope has never been
%%  started on the node
%%-----------------------------------------------------------------
-spec manager(atom(), term()) -> pid().
manager(Scope, Term)->
  Pool = persistent_term:get(?pool(Scope)),
  element(erlang:phash2(Term, tuple_size(Pool)) + 1, Pool).

-spec ready_nodes(atom()) -> [node()].
ready_nodes(Scope)->
  [node(PID)|| PID <- pg:get_members(?pg_scope(Scope), ?pg_group)].
