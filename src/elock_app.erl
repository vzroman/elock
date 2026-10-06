
%%=================================================================
%%  The application: one graph process per node, under elock_sup,
%%  started before the scopes of the user's application
%%=================================================================
-module(elock_app).
-moduledoc false.

-behaviour(application).

%%=================================================================
%%	OTP API
%%=================================================================
-export([
  start/2,
  stop/1
]).

%%=================================================================
%%	OTP API
%%=================================================================
-spec start(application:start_type(), term()) -> {ok, pid()}.
start(_StartType, _StartArgs)->
  elock_sup:start_link().

-spec stop(term()) -> ok.
stop(_State)->
  ok.
