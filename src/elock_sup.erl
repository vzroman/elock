
%%=================================================================
%%  The supervisor of the graph process (see elock_graph)
%%=================================================================
-module(elock_sup).
-moduledoc false.

-behaviour(supervisor).

%%=================================================================
%%	OTP API
%%=================================================================
-export([
  start_link/0,
  init/1
]).

%%=================================================================
%%	OTP API
%%=================================================================
-spec start_link() -> {ok, pid()}.
start_link()->
  supervisor:start_link(?MODULE, []).

%%-----------------------------------------------------------------
%%  One permanent worker, the defaults of a child spec
%%-----------------------------------------------------------------
-spec init([]) -> {ok, {supervisor:sup_flags(), [supervisor:child_spec()]}}.
init([])->
  {ok, {
    #{
      strategy => one_for_one
    },
    [
      #{
        id => elock_graph,
        start => {elock_graph, start_link, []}
      }
    ]
  }}.
