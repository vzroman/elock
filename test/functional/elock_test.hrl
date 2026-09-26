-ifndef(elock_test).
-define(elock_test, 1).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

%% The deadline of every wait, ms
-define(DEADLINE, 5000).

%% The window given to a "nothing must happen" assertion, ms
-define(QUIET, 150).

%% Poll Cond (an expression) every 10 ms until it is true, fail at the deadline
-define(WAIT(Cond), elock_test_utils:wait_until(fun()-> Cond end, ?DEADLINE)).

%% Receive a message matching Pattern (the message is the value of the
%% expression, the variables of the pattern are local) or fail naming the
%% pattern and what was in the mailbox instead
-define(RECEIVE(Pattern), ?RECEIVE(Pattern, ?DEADLINE)).
-define(RECEIVE(Pattern, Timeout),
  (fun()->
    receive
      Pattern = __Received__ -> __Received__
    after Timeout ->
      erlang:error({receive_timeout, ??Pattern, elock_test_utils:flush()})
    end
  end)()).

%% Nothing arrives within the quiet window
-define(NO_MESSAGE, elock_test_utils:no_message(?QUIET)).

-endif.
