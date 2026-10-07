
-ifndef(elock_trace).
-define(elock_trace,1).

%%-------------------------------------------------------------------------------
%% TRACING (see elock_trace.erl)
%%
%% A trace point is compiled in only with the ELOCK_TRACE macro (the
%% test profile of rebar.config) and writes only between
%% elock_trace:start/1 and elock_trace:stop/0. Data is evaluated only
%% then.
%%-------------------------------------------------------------------------------
-ifdef(ELOCK_TRACE).

-define(TRACE(Step, Id, Data),
  case persistent_term:get(elock_trace, false) of
    true -> elock_trace:event(Step, Id, Data);
    false -> ok
  end).

-else.

-define(TRACE(Step, Id, Data), ok).

-endif.

-endif.
