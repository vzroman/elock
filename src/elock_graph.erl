%%=================================================================
%%  The wait-for graph of a lock and the deadlock probes
%%
%%  A lock is {Scope, Term, Node}, its manager keeps a graph of its
%%  waiters. A held map spans all the scopes, so a cycle through
%%  several scopes is found like any other.
%%
%%  A waiting request joins the graph with its first hold: the held
%%  map comes in as #add_held_locks{} only after the request queues.
%%  A request that holds nothing can not be on a cycle.
%%
%%  Weight is #request.held_count. It is fixed for the life of the
%%  request, so every manager of a cycle picks the same loser: the
%%  lighter one, or the coin on a tie (see drop_coin/2).
%%
%%  Each new hold of a waiting request (the origin) is probed at the
%%  held lock (run_probe/4). A probe is addressed to a lock by its
%%  key (target) and goes to the mailbox of its manager, also when
%%  that is the sending manager itself. Every edge is probed as it
%%  appears, so the probe of the edge that closes a cycle finds the
%%  rest of the cycle in place. The waiters of the target lock depend
%%  on the origin: it holds their lock, or a forwarded probe has come
%%  through the holders:
%%    * a waiter that holds the origin's lock closes a cycle
%%      (probe/2). If a closer beats the origin, #deadlock{} goes to
%%      the origin manager and the probe stops. Otherwise every
%%      closer is aborted.
%%    * the probe goes on to the locks the remaining waiters hold
%%      (forward/2). sent_to keeps the flood finite.
%%
%%  Only the manager changes the graph. forward/2 runs after the
%%  aborts: an abort may grant the lock to a later waiter, and a
%%  holder does not depend on the origin.
%%=================================================================
-module(elock_graph).
-moduledoc false.

-include("elock.hrl").

-export_type([graph/0]).

-type edges() :: #{lock_key() => #{reference() => non_neg_integer()}}.
-type index() :: #{reference() => {non_neg_integer(), held_locks()}}.

% #graph{
%   edges = #{ {Scope, Term, Node} => #{ Ref => Weight } }, - the waiters holding the lock
%   index = #{ Ref => {Weight, HeldMap} }                  - HeldMap as in #add_held_locks{}
% }
% undefined while no waiter holds anything, so index is never empty
-record(graph,{
  edges :: edges(),
  index :: index()
}).

-opaque graph() :: #graph{}.

%%=================================================================
%%	API
%%=================================================================
-export([
  add_edges/5,
  remove_edges/2,
  probe/2,
  forward/2
]).

%%=================================================================
%%  The edges
%%=================================================================
%%-----------------------------------------------------------------
%%  Adds and probes the new holds of a waiting request. Edge is the
%%  lock of the graph. A request already in the graph keeps its weight.
%%  Update is never empty (see elock_context:notify_queued/3 and
%%  elock_manager:wait_verdict/4)
%%-----------------------------------------------------------------
-spec add_edges(reference(), lock_key(), held_locks(),
                non_neg_integer(), graph() | undefined) -> graph().
add_edges(
    Ref,
    Edge,
    Update,
    Weight,
    #graph{
      edges = Edges0,
      index = Index0
    } = Graph0
)->
  {Weight0, Held0} = maps:get(Ref, Index0, {Weight, #{}}),
  case new_held_locks(Update, Held0) of
    New when map_size(New) =:= 0->
      Graph0;
    New->
      run_probe(Ref, Edge, New, Weight0),

      Edges = add_holder(Ref, Weight0, maps:keys(New), Edges0),
      Index = Index0#{
        Ref => {Weight0, maps:merge(Held0, New)}
      },
      Graph0#graph{
        edges = Edges,
        index = Index
      }
  end;

add_edges(Ref, Edge, Update, Weight, _Graph)->
  add_edges(Ref, Edge, Update, Weight, #graph{
    edges = #{},
    index = #{}
  }).

%%-----------------------------------------------------------------
%%  A new key, or a new manager PID for a stale key
%%-----------------------------------------------------------------
-spec new_held_locks(held_locks(), held_locks()) -> held_locks().
new_held_locks(Update, Held) when map_size(Held) =:= 0->
  Update;
new_held_locks(Update, Held)->
  maps:filter(
    fun(Key, Manager)->
      case Held of
        #{Key := Manager}->
          false;
        _->
          true
      end
    end,
    Update
  ).

-spec add_holder(reference(), non_neg_integer(), [lock_key()], edges()) ->
  edges().
add_holder(Ref, Weight, Locks, Edges)->
  lists:foldl(
    fun(Edge, Acc)->
      EdgeAcc0 = maps:get(Edge, Acc, #{}),
      EdgeAcc = EdgeAcc0#{
        Ref => Weight
      },
      Acc#{
        Edge => EdgeAcc
      }
    end,
    Edges,
    Locks
  ).

-spec remove_edges(reference(), graph() | undefined) -> graph() | undefined.
remove_edges(
    Ref,
    #graph{
      edges = Edges0,
      index = Index0
    } = Graph0
)->
  case maps:take(Ref, Index0) of
    {{_Weight, _Held}, Index} when map_size(Index) =:= 0->
      undefined;
    {{_Weight, Held}, Index}->
      Edges =
        maps:fold(
          fun(Edge, _Manager, Acc)->
            EdgeAcc = maps:remove(Ref, maps:get(Edge, Acc)),
            if
              map_size(EdgeAcc) > 0 ->
                Acc#{ Edge => EdgeAcc };
              true ->
                maps:remove(Edge, Acc)
            end
          end,
          Edges0,
          Held
        ),
      Graph0#graph{
        edges = Edges,
        index = Index
      };
    error->
      % Holds nothing, or its holds have not come in yet
      Graph0
  end;
remove_edges(_Ref, Graph)->
  Graph.

%%=================================================================
%%  The probes
%%=================================================================
%%-----------------------------------------------------------------
%%  Returns the closers to abort, or stop if the origin loses: its
%%  abort breaks every cycle through it, the lighter closers stay.
%%  The target is the lock of the graph, the one the winner waits for
%%-----------------------------------------------------------------
-spec probe(#deadlock_probe{}, graph() | undefined) ->
  stop | {forward, [reference()]}.
probe(
    #deadlock_probe{
      ref = Ref,
      edge = Edge,
      target = Target,
      manager = Manager
    } = Probe,
    #graph{
      edges = Edges,
      index = Index
    }
)->
  case Edges of
    #{ Edge := Holders }->
      case check_cycles(maps:to_list(Holders), Probe, Index, []) of
        origin->
          ecall:send(Manager, #deadlock{ref = Ref, winner = Target}),
          stop;
        Closers->
          {forward, Closers}
      end;
    _->
      {forward, []}
  end;
probe(_Probe, _Graph)->
  {forward, []}.

%%-----------------------------------------------------------------
%%  Returns the losing closers, or origin as soon as a closer beats it.
%%  The origin itself is skipped: a multi node request waits at several
%%  managers, a barging one holds the term it waits for. On a tie it
%%  could lose to itself
%%-----------------------------------------------------------------
-spec check_cycles([{reference(), non_neg_integer()}], #deadlock_probe{},
                   index(), [reference()]) -> origin | [reference()].
check_cycles(
    [{Ref, _Weight}|Rest],
    #deadlock_probe{
      ref = Ref
    } = Probe,
    Index,
    Acc
)->
  check_cycles(Rest, Probe, Index, Acc);

%%-----------------------------------------------------------------
%%  Only a hold by the origin manager's PID closes a cycle. Another PID
%%  is a stale hold: the scope has restarted on that node
%%-----------------------------------------------------------------
check_cycles(
    [{CloserRef, CloserWeight}|Rest],
    #deadlock_probe{
      ref = Ref,
      edge = Edge,
      manager = Manager,
      weight = Weight
    } = Probe,
    Index,
    Acc
)->
  {_, Held} = maps:get(CloserRef, Index),
  case Held of
    #{ Edge := Manager }->
      if
        CloserWeight > Weight ->
          origin;
        CloserWeight < Weight ->
          check_cycles(Rest, Probe, Index, [CloserRef|Acc]);
        true ->
          case drop_coin(CloserRef, Ref) of
            CloserRef ->
              origin;
            _->
              check_cycles(Rest, Probe, Index, [CloserRef|Acc])
          end
      end;
    _->
      check_cycles(Rest, Probe, Index, Acc)
  end;

check_cycles([], _Probe, _Index, Acc)->
  Acc.

%%-----------------------------------------------------------------
%%  The pair is sorted before hashing, so every manager of a cycle
%%  picks the same winner whatever the argument order
%%-----------------------------------------------------------------
-spec drop_coin(reference(), reference()) -> reference().
drop_coin(Ref1, Ref2)->
  Tie =
    if
      Ref1 < Ref2 ->
        {Ref1, Ref2};
      true ->
        {Ref2, Ref1}
    end,
  Winner = erlang:phash2(Tie, 2) + 1,
  element(Winner, Tie).

%%-----------------------------------------------------------------
%%  Sends a probe to every new hold, except the lock the origin waits
%%  for: a barging request holds it
%%-----------------------------------------------------------------
-spec run_probe(reference(), lock_key(), held_locks(),
                non_neg_integer()) -> ok.
run_probe(Ref, Edge, Held, Weight)->
  Self = self(),
  SentTo = maps:from_keys([Edge|maps:keys(Held)], true),
  maps:foreach(
    fun
      (Key, Manager) when Key =/= Edge->
        ecall:send(Manager, #deadlock_probe{
          ref = Ref,
          edge = Edge,
          target = Key,
          manager = Self,
          weight = Weight,
          sent_to = SentTo
        });
      (_Edge, _Self)->
        ok
    end,
    Held
  ).

%%-----------------------------------------------------------------
%%  The waiters here depend on the origin, and so do the waiters of
%%  the locks they hold
%%-----------------------------------------------------------------
-spec forward(#deadlock_probe{}, graph() | undefined) -> ok.
forward(
    #deadlock_probe{
      sent_to = SentTo
    } = Probe0,
    #graph{
      index = Index
    }
)->
  Targets =
    #{ Key => Manager ||
      {_Weight, Held} <- maps:values(Index),
      Key := Manager <- Held,
      not is_map_key(Key, SentTo)
    },
  Probe = Probe0#deadlock_probe{
    sent_to = maps:merge(SentTo, maps:from_keys(maps:keys(Targets), true))
  },
  maps:foreach(
    fun(Key, Manager)->
      ecall:send(Manager, Probe#deadlock_probe{target = Key})
    end,
    Targets
  );

forward(_Probe, _Graph)->
  ok.
