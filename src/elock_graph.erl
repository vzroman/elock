%%=================================================================
%%  Deadlock probes
%%
%%  A waiter depends on every holder of its Term, and a holder that
%%  is itself waiting for another Term carries the dependency on - a
%%  deadlock is a cycle of such dependencies. #req.held is the set of
%%  the locks the client held when it made the request, i.e. the
%%  terms the request holds while it waits. A multi node request is
%%  granted node by node: every grant the client gets while the
%%  request still waits here comes in as #add_held_locks{} and joins
%%  the held map.
%%
%%  There are no checker processes, the managers probe each other.
%%  When a request starts waiting here (the origin) the probe goes
%%  to the manager of every lock it holds, except this very Term (a
%%  barging request holds the term it waits for), and every hold it
%%  gains meanwhile is probed the same way: it adds wait-for edges
%%  and may be the one that closes a cycle. A request that holds
%%  nothing can not be on a cycle - no probe.
%%
%%  A manager that receives the probe looks at its own waiters:
%%    * a waiter that holds the origin's term closes a cycle. The
%%      lighter request loses (see #req.weight). If any closer is
%%      heavier than the origin then the origin loses: #deadlock{}
%%      goes back to the origin manager and the probe stops - the
%%      abort of the origin breaks every cycle through it. Otherwise
%%      every closer is aborted here.
%%    * the probe is passed on to the managers of the locks held by
%%      the waiters that are left: they depend on the holders of
%%      this Term, which depend on the origin. The managers that have
%%      already seen the probe are skipped, hence the flood is finite.
%%
%%  Every edge is probed as soon as it appears, hence the probe of
%%  the edge that completes a cycle finds the rest of the cycle
%%  already in place
%%=================================================================
%%-----------------------------------------------------------------
%%  A request starts waiting: the client of a multi node request is
%%  told where it queued up (it answers with #add_held_locks{} as
%%  the other nodes grant), and the locks the request holds are
%%  probed
%%-----------------------------------------------------------------

-module(elock_graph).

-include("elock.hrl").

-record(graph,{
  edges,
  index
}).
% Graph structure:
% #graph{
%   edges = #{
%     {Term, Node} => #{
%       Ref => HeldCount
%     }
%   },
%   index => #{
%     Ref => #{
%       {Term, Node} => Manager
%     }
%   }
% }

%%=================================================================
%%	API
%%=================================================================
-export([
  add_edges/2,
  remove_edges/2,
  probe/2
]).

%%=================================================================
%%  API
%%=================================================================
add_edges(
    #request{
      held = Held
    },
    Graph
) when map_size(Held) =:= 0 ->
  Graph;
add_edges(
    #request{
      ref = Ref,
      term = Term,
      held = Held
    },
    #graph{
      edges = Edges0,
      index = Index0
    } = Graph0
) when map_size(Held) > 0->

  Weight = map_size(Held),
  run_probe(Ref, Term, Held, Weight),

  Edges =
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
      Edges0,
      maps:keys(Held)
    ),
  Index = Index0#{
    Ref => Held
  },
  Graph0#graph{
    edges = Edges,
    index = Index
  };
add_edges(Request, _Graph)->
  add_edges(Request, #graph{
    edges = #{},
    index = #{}
  }).

remove_edges(
    Ref,
    #graph{
      edges = Edges0,
      index = Index0
    } = Graph0
)->
  case maps:take(Ref, Index0) of
    {_Held, Index} when map_size(Index) =:= 0->
      undefined;
    {Held, Index}->
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
      Graph0
  end;
remove_edges(_Ref, Graph)->
  Graph.

probe(
    #deadlock_probe{
      ref = Ref,
      weight = Weight,
      edge = Edge
    } =Probe,
    #graph{
      edges = Edges
    } =Graph0
)->
  case Edges of
    #{ Edge := Cycles } ->
      case check_cycles(maps:to_list(Cycles), Weight, Ref, []) of
        []->
          forward_probe(Probe, Graph0),
          {[], Graph0};
        AbortRefs->
          Graph = lists:foldl(fun remove_edges/2, Graph0, AbortRefs),
          {AbortRefs, Graph}
      end;
    _->
      forward_probe(Probe, Graph0),
      {[], Graph0}
  end;
probe(_Probe, Graph)->
  {[], Graph}.

check_cycles(
    [{MyRef,MyWeight}|Rest],
    ProbeWeight,
    ProbeRef,
    Acc
)->
  if
    MyWeight > ProbeWeight ->
      [];
    MyWeight < ProbeWeight ->
      check_cycles(Rest, ProbeWeight, ProbeRef, [MyRef|Acc]);
    true ->
      case drop_coin(MyRef, ProbeRef) of
        MyRef ->
          [];
        _->
          check_cycles(Rest, ProbeWeight, ProbeRef, [MyRef|Acc])
      end
  end;
check_cycles([], _ProbeWeight, _ProbeRef, Acc)->
  Acc.

drop_coin(Ref1, Ref2)->
  Tie =
    if
      Ref1 < Ref2 ->
        {Ref1, Ref2};
      true ->
        {Ref2, Ref1}
    end,
  Winner = phash2(Tie, 2) + 1,
  element(Winner, Tie).

run_probe(Ref, Term, Held, Weight)->
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
    edge = {Term, node()},
    manager = Self,
    weight = Weight,
    sent_to = SentTo
  },
  maps:foreach(
    fun(Manager, _)->
      catch ecall:send(Manager, Probe)
    end,
    maps:remove(Self, SentTo)
  ).

forward_probe(Probe, Graph)->
  todo.

