%% Explicit-state workload generation. The state belongs to one numbered client;
%% backend retry randomness never touches it.
-module(performance_workload).
-export([plan/2, size/1]).

size(#{read := Read, update := Update, write := Write})->
  Read + Update + Write.

plan(#{objects_pool_size := Pool, transaction := Transaction, deadlocks := Deadlocks}, State0)->
  #{read := Read, update := Update, write := Write} = Transaction,
  {Keys, State1} = draw(?MODULE:size(Transaction), Pool, #{}, [], State0),
  Operations = lists:zip(Keys,
    lists:duplicate(Read, read) ++ lists:duplicate(Update, update) ++ lists:duplicate(Write, write)),
  case Deadlocks of
    false-> {lists:sort(Operations), State1};
    true-> shuffle(Operations, State1)
  end.

%% A sparse partial Fisher-Yates draw: O(number of operations), including when
%% the transaction uses the complete pool. Retain draw order for mode assignment.
draw(0, _Remaining, _Moved, Keys, State)->
  {lists:reverse(Keys), State};
draw(Count, Remaining, Moved, Keys, State0)->
  {Index, State1} = rand:uniform_s(Remaining, State0),
  Key = maps:get(Index, Moved, Index),
  Last = maps:get(Remaining, Moved, Remaining),
  draw(Count - 1, Remaining - 1, maps:remove(Remaining, Moved#{Index => Last}),
    [{object, Key} | Keys], State1).

shuffle(Operations, State0)->
  Count = length(Operations),
  {Indices, State1} = draw(Count, Count, #{}, [], State0),
  Tuple = list_to_tuple(Operations),
  {[element(I, Tuple) || {object, I} <- Indices], State1}.
