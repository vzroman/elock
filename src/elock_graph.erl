%%=================================================================
%%  The wait-for graph of a manager and the deadlock probes
%%
%%  A lock is {Scope, Term, Node}. A held map spans all the scopes,
%%  so a cycle through several scopes is found like any other.
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
%%  manager of the held lock (run_probe/4). Every edge is probed as
%%  it appears, so the probe of the edge that closes a cycle finds
%%  the rest of the cycle in place. The waiters of the receiving
%%  manager depend on the origin: it holds their lock, or a forwarded
%%  probe has come through the holders:
%%    * a waiter that holds the origin's lock closes a cycle
%%      (probe/3). If a closer beats the origin, #deadlock{} goes to
%%      the origin manager and the probe stops. Otherwise every
%%      closer is aborted.
%%    * the probe goes on to the managers of the locks the remaining
%%      waiters hold (forward/2). sent_to keeps the flood finite.
%%
%%  Only the manager changes the graph. forward/2 runs after the
%%  aborts: an abort may grant the lock to a later waiter, and a
%%  holder does not depend on the origin.
%%=================================================================
-module(elock_graph).

-include("elock.hrl").

% #graph{
%   edges = #{ {Scope, Term, Node} => #{ Ref => Weight } }, - the waiters holding the lock
%   index = #{ Ref => {Weight, HeldMap} }                  - HeldMap as in #add_held_locks{}
% }
% undefined while no waiter holds anything, so index is never empty
-record(graph,{
  edges,
  index
}).

%%=================================================================
%%	API
%%=================================================================
-export([
  add_edges/5,
  remove_edges/2,
  probe/3,
  forward/2
]).

%%=================================================================
%%  The edges
%%=================================================================
%%-----------------------------------------------------------------
%%  Adds and probes the new holds of a waiting request. Edge is this
%%  manager's lock. A request already in the graph keeps its weight.
%%  Update is never empty (see elock:notify_queued/3 and
%%  elock_manager:wait_verdict/4)
%%-----------------------------------------------------------------
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
            case Acc of
              #{ Edge := EdgeAcc0 }->
                EdgeAcc = maps:remove(Ref, EdgeAcc0),
                if
                  map_size(EdgeAcc) > 0 ->
                    Acc#{ Edge => EdgeAcc };
                  true ->
                    maps:remove(Edge, Acc)
                end;
              _->
                Acc
            end
          end,
          Edges0,
          Held
        ),
      Graph0#graph{
        edges = Edges,
        index = Index
      };
    _->
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
%%  LocalEdge is this manager's lock, the one the winner waits for
%%-----------------------------------------------------------------
-spec probe(#deadlock_probe{}, {atom(), term(), node()}, #graph{} | undefined) ->
  stop | {forward, [reference()]}.
probe(
    #deadlock_probe{
      ref = Ref,
      edge = Edge,
      manager = Manager
    } = Probe,
    LocalEdge,
    #graph{
      edges = Edges,
      index = Index
    }
)->
  case Edges of
    #{ Edge := Holders }->
      case check_cycles(maps:to_list(Holders), Probe, Index, []) of
        origin->
          ecall:send(Manager, #deadlock{ref = Ref, winner = LocalEdge}),
          stop;
        Closers->
          {forward, Closers}
      end;
    _->
      {forward, []}
  end;
probe(_Probe, _LocalEdge, _Graph)->
  {forward, []}.

%%-----------------------------------------------------------------
%%  Returns the losing closers, or origin as soon as a closer beats it.
%%  The origin itself is skipped: a multi node request waits at several
%%  managers, a barging one holds the term it waits for. On a tie it
%%  could lose to itself
%%-----------------------------------------------------------------
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
%%  is a stale hold: that manager has died and a new one took the term
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
%%  Sends the probe to the managers of the new holds, except this one:
%%  a barging request holds the term it waits for
%%-----------------------------------------------------------------
run_probe(Ref, Edge, Held, Weight)->
  Self = self(),
  SentTo =
    maps:fold(
      fun(_Edge, Manager, Acc)->
        Acc#{ Manager => true }
      end,
      #{Self => true},
      Held
    ),
  Probe = #deadlock_probe{
    ref = Ref,
    edge = Edge,
    manager = Self,
    weight = Weight,
    sent_to = SentTo
  },
  maps:foreach(
    fun(Manager, _)->
      ecall:send(Manager, Probe)
    end,
    maps:remove(Self, SentTo)
  ).

%%-----------------------------------------------------------------
%%  The waiters here depend on the origin, and so do the waiters of
%%  the locks they hold
%%-----------------------------------------------------------------
forward(
    #deadlock_probe{
      sent_to = SentTo
    } = Probe0,
    #graph{
      index = Index
    }
)->
  Targets =
    maps:from_keys(
      [ Manager ||
        {_Weight, Held} <- maps:values(Index),
        Manager <- maps:values(Held),
        not is_map_key(Manager, SentTo)
      ],
      true
    ),
  Probe = Probe0#deadlock_probe{
    sent_to = maps:merge(SentTo, Targets)
  },
  maps:foreach(
    fun(Manager, _)->
      ecall:send(Manager, Probe)
    end,
    Targets
  );

forward(_Probe, _Graph)->
  ok.
