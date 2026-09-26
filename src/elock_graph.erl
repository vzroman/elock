%%=================================================================
%%  The wait-for graph of a manager and the deadlock probes
%%
%%  A waiter depends on every holder of its Term, and a holder that
%%  is itself waiting for another Term carries the dependency on - a
%%  deadlock is a cycle of such dependencies. #request.held is the
%%  set of the locks the client held when it made the request, i.e.
%%  the terms the request holds while it waits. A lock is
%%  {Scope, Term, Node}: the scopes are separate heaps of locks, but
%%  a cycle through several of them blocks like any other, hence the
%%  held map spans the scopes and the key carries the scope - the
%%  same Term in two scopes is two locks. A multi node request
%%  is granted node by node: every grant the client gets while the
%%  request still waits comes in as #add_held_locks{} and joins the
%%  held map (see add_held_locks/4).
%%
%%  There are no checker processes, the managers probe each other.
%%  When a request starts waiting (the origin) the probe goes to the
%%  manager of every lock it holds, except the origin manager itself
%%  (a barging request holds the term it waits for), and every hold
%%  it gains meanwhile is probed the same way: it adds wait-for
%%  edges and may be the one that closes a cycle. A request that
%%  holds nothing can not be on a cycle - it stays out of the graph,
%%  no probe.
%%
%%  A manager that receives the probe looks at its own waiters:
%%    * a waiter that holds the lock the origin waits for closes a
%%      cycle - the same incarnation of it, i.e. by the origin
%%      manager's PID, a hold by another PID is a stale one, not an
%%      edge. The origin itself is skipped: a multi node request
%%      waits at several managers and a barging one holds the term
%%      it waits for, a request can not close a cycle with itself.
%%      The lighter request loses, the coin settles the equal
%%      weights (see drop_coin/2). If any closer beats the origin
%%      then the origin loses: #deadlock{} goes back to the origin
%%      manager and the probe stops - the abort of the origin breaks
%%      every cycle through it, the lighter closers are left alone.
%%      Otherwise every closer loses, the manager aborts them.
%%    * the probe is passed on to the managers of the locks held by
%%      the waiters that are left: they depend on the holders of
%%      this Term, which depend on the origin. The managers that have
%%      already seen the probe are skipped, hence the flood is finite.
%%
%%  Every edge is probed as soon as it appears, hence the probe of
%%  the edge that completes a cycle finds the rest of the cycle
%%  already in place.
%%
%%  The graph is changed by the manager alone: a request joins it
%%  when it starts waiting and leaves it when it stops. probe/2 only
%%  names the closers to abort and forward/2 is called after the
%%  aborts, on the graph as it is then - an abort pushes the queue
%%  and may grant the lock to a later waiter, from then on it is a
%%  holder, it does not depend on the origin and is not forwarded
%%  for
%%=================================================================
-module(elock_graph).

-include("elock.hrl").

-record(graph,{
  edges,            % by the lock: the waiters that hold it and their weights
  index             % by the waiter: its frozen weight and its held map
}).
% Graph structure:
% #graph{
%   edges = #{
%     {Scope, Term, Node} => #{
%       Ref => Weight
%     }
%   },
%   index = #{
%     Ref => {Weight, #{
%       {Scope, Term, Node} => Manager
%     }}
%   }
% }

%%=================================================================
%%	API
%%=================================================================
-export([
  add_edges/2,
  add_held_locks/4,
  remove_edges/2,
  probe/2,
  forward/2
]).

%%=================================================================
%%  The edges
%%=================================================================
%%-----------------------------------------------------------------
%%  A request starts waiting
%%  the guard:
%%  * the request holds nothing - it can not be on a cycle, it stays
%%    out of the graph and there is nothing to probe
%%-----------------------------------------------------------------
add_edges(
    #request{
      held = Held
    },
    Graph
) when map_size(Held) =:= 0 ->
  Graph;

%%-----------------------------------------------------------------
%%  The request joins the graph with the locks it holds and they are
%%  probed. The weight is the number of them as of now and it stays
%%  so (see the header)
%%  the guard:
%%  * the request holds something (the clause above)
%%  * the graph exists
%%-----------------------------------------------------------------
add_edges(
    #request{
      ref = Ref,
      scope = Scope,
      term = Term,
      held = Held
    },
    #graph{
      edges = Edges0,
      index = Index0
    } = Graph0
) when map_size(Held) > 0->

  Weight = map_size(Held),
  run_probe(Ref, {Scope, Term, node()}, Held, Weight),

  Edges = add_holder(Ref, Weight, maps:keys(Held), Edges0),
  Index = Index0#{
    Ref => {Weight, Held}
  },
  Graph0#graph{
    edges = Edges,
    index = Index
  };

%%-----------------------------------------------------------------
%%  The first waiter that holds something - the graph starts with it
%%  the guard:
%%  * there is no graph yet (the clauses above)
%%-----------------------------------------------------------------
add_edges(Request, _Graph)->
  add_edges(Request, #graph{
    edges = #{},
    index = #{}
  }).

%%-----------------------------------------------------------------
%%  The entries of the update the held map does not have yet join it
%%  and are probed alone: they add wait-for edges and may be the very
%%  edge that closes a cycle, and only the request that gained them
%%  can see that. The weight stays as it was when the request asked
%%  (see the header), a request that held nothing then is not in the
%%  graph yet and joins with the weight 0. Edge is the lock of this
%%  manager, the one the request waits for
%%  the guard:
%%  * the update is not empty (the clause above)
%%  * the graph exists
%%-----------------------------------------------------------------
add_held_locks(
    Ref,
    Edge,
    Update,
    #graph{
      edges = Edges0,
      index = Index0
    } = Graph0
)->
  {Weight, Held0} = maps:get(Ref, Index0, {0, #{}}),
  case new_held_locks(Update, Held0) of
    New when map_size(New) =:= 0->
      Graph0;
    New->
      run_probe(Ref, Edge, New, Weight),

      Edges = add_holder(Ref, Weight, maps:keys(New), Edges0),
      Index = Index0#{
        Ref => {Weight, maps:merge(Held0, New)}
      },
      Graph0#graph{
        edges = Edges,
        index = Index
      }
  end;

%%-----------------------------------------------------------------
%%  The first waiter that holds something - the graph starts with it
%%  the guard:
%%  * there is no graph yet (the clauses above)
%%-----------------------------------------------------------------
add_held_locks(Ref, Edge, Update, _Graph)->
  add_held_locks(Ref, Edge, Update, #graph{
    edges = #{},
    index = #{}
  }).

%%-----------------------------------------------------------------
%%  The entries of the update the held map does not have yet: a new
%%  key, or a fresh PID for a key whose old one is stale
%%-----------------------------------------------------------------
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

%%-----------------------------------------------------------------
%%  The request joins the edges of the locks as their holder
%%-----------------------------------------------------------------
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

%%-----------------------------------------------------------------
%%  A request stops waiting: it leaves the index and the edges of the
%%  locks it holds, a lock nobody holds any more leaves the edges.
%%  The graph goes with the last waiter, hence a #graph{} never has
%%  an empty index
%%-----------------------------------------------------------------
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
      % the request held nothing, it has never been in the graph
      Graph0
  end;

%%-----------------------------------------------------------------
%%  Nobody waits here holding anything
%%  the guard:
%%  * there is no graph (the clause above)
%%-----------------------------------------------------------------
remove_edges(_Ref, Graph)->
  Graph.

%%=================================================================
%%  The probes
%%=================================================================
%%-----------------------------------------------------------------
%%  A probe from another manager. The waiters that hold the lock the
%%  origin waits for close a cycle with it and are weighed against
%%  it (see check_cycles/4):
%%  * a closer beats the origin - the origin loses. The verdict goes
%%    to the origin manager and the probe stops here: the abort of
%%    the origin breaks every cycle through it, the lighter closers
%%    are left alone
%%  * otherwise every closer loses. They are handed to the manager
%%    to abort, it passes the probe on after that (see forward/2)
%%  The graph is not changed here
%%-----------------------------------------------------------------
probe(
    #deadlock_probe{
      ref = Ref,
      edge = Edge,
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
          catch ecall:send(Manager, #deadlock{ref = Ref}),
          stop;
        Closers->
          {forward, Closers}
      end;
    _->
      % nobody here holds the lock the origin waits for
      {forward, []}
  end;

%%-----------------------------------------------------------------
%%  Nobody waits here holding anything - nothing closes a cycle
%%  the guard:
%%  * there is no graph (the clause above)
%%-----------------------------------------------------------------
probe(_Probe, _Graph)->
  {forward, []}.

%%-----------------------------------------------------------------
%%  Weigh the waiters that hold the origin's edge against the origin:
%%  the lighter one loses, the coin settles a tie. The result is the
%%  closers that lose, or origin as soon as one of them beats it.
%%  The origin itself is skipped: a multi node request waits at
%%  several managers and a barging one holds the term it waits for.
%%  A request can not close a cycle with itself, and the equal
%%  weights could make it lose to itself
%%  the guard:
%%  * the waiter is the origin
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
%%  A hold on the same incarnation of the lock, i.e. by the origin
%%  manager's PID, closes a cycle. Another PID is a stale hold - the
%%  manager died and a new one took the term - not an edge
%%  the guard:
%%  * the waiter is not the origin (the clause above)
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

%%-----------------------------------------------------------------
%%  No closer beats the origin
%%-----------------------------------------------------------------
check_cycles([], _Probe, _Index, Acc)->
  Acc.

%%-----------------------------------------------------------------
%%  The tie. The pair is sorted and hashed, hence both managers of a
%%  cycle come to the same winner whichever probe gets there first
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
%%  The probe of a waiting request for the locks it holds - the whole
%%  held map when it starts waiting, the new entries as it gains
%%  them. It goes to the manager of each of them, except this very
%%  manager: a barging request holds the term it waits for. Edge is
%%  the lock of this manager, the one the origin waits for. sent_to
%%  names every manager the probe has been sent to, this one
%%  included, before it goes
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
      catch ecall:send(Manager, Probe)
    end,
    maps:remove(Self, SentTo)
  ).

%%-----------------------------------------------------------------
%%  Pass the probe on: the waiters of this Term depend on its
%%  holders, which depend on the origin - so do the waiters of the
%%  locks they hold. It goes to the managers of every lock held by
%%  the waiters that are left, except those it has been sent to
%%  already, and they join sent_to before it goes. The manager calls
%%  this after the aborts of the closers (see probe/2), on the graph
%%  as it is then: an abort pushes the queue and may grant the lock
%%  to a later waiter, from then on it is a holder and does not
%%  depend on the origin
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
      catch ecall:send(Manager, Probe)
    end,
    Targets
  );

%%-----------------------------------------------------------------
%%  Nobody waits here holding anything - the probe stops
%%  the guard:
%%  * there is no graph (the clause above)
%%-----------------------------------------------------------------
forward(_Probe, _Graph)->
  ok.
