%%=================================================================
%%  Module tests of elock_graph: the wait-for graph of a manager and
%%  the deadlock probes. The test process is "this manager" (self()),
%%  the managers of the held locks are collectors that forward every
%%  message they get to the test process as {Collector, Message}
%%=================================================================
-module(elock_graph_SUITE).

-include("elock.hrl").
-include("elock_test.hrl").

%% API
-export([
  all/0,
  groups/0,
  suite/0,
  init_per_testcase/2,
  end_per_testcase/2,
  init_per_group/2,
  end_per_group/2,
  init_per_suite/1,
  end_per_suite/1
]).

-export([
  add_edges_holds_nothing_test/1,
  add_edges_new_graph_test/1,
  add_edges_self_held_test/1,
  add_edges_second_waiter_test/1,
  add_held_locks_new_request_test/1,
  add_held_locks_update_test/1,
  remove_edges_test/1,
  probe_no_graph_test/1,
  probe_edge_not_held_test/1,
  probe_origin_loses_test/1,
  probe_closer_loses_test/1,
  probe_tie_test/1,
  probe_stale_hold_test/1,
  probe_skips_origin_test/1,
  probe_multiple_closers_test/1,
  forward_test/1,
  drop_coin_test/1
]).

% mirrors elock_graph.erl
-record(graph,{
  edges,
  index
}).

% The lock of "this manager", the one its waiters wait for
-define(SCOPE, graph_test_scope).
-define(TERM, graph_test_term).
-define(EDGE, {?SCOPE, ?TERM, node()}).

% The lock a probed origin waits for, managed by the origin manager
-define(ORIGIN_EDGE, {origin_scope, origin_term, node()}).

all()->
  [
    {group, edges},
    {group, probes}
  ].

groups()->
  [
    {edges, [], [
      add_edges_holds_nothing_test,
      add_edges_new_graph_test,
      add_edges_self_held_test,
      add_edges_second_waiter_test,
      add_held_locks_new_request_test,
      add_held_locks_update_test,
      remove_edges_test
    ]},
    {probes, [], [
      probe_no_graph_test,
      probe_edge_not_held_test,
      probe_origin_loses_test,
      probe_closer_loses_test,
      probe_tie_test,
      probe_stale_hold_test,
      probe_skips_origin_test,
      probe_multiple_closers_test,
      forward_test,
      drop_coin_test
    ]}
  ].

suite()->
  [{timetrap, {minutes, 10}}].

init_per_suite(Config)->
  Config.

end_per_suite(_Config)->
  ok.

init_per_group(_Group, Config)->
  Config.

end_per_group(_Group, _Config)->
  ok.

init_per_testcase(_TestCase, Config)->
  Config.

end_per_testcase(_TestCase, _Config)->
  elock_test_utils:stop_collectors(),
  ok.

%%=================================================================
%%  The edges
%%=================================================================
%%-----------------------------------------------------------------
%%  A request that holds nothing stays out of the graph: undefined
%%  stays undefined, an existing graph is not changed, no probe
%%-----------------------------------------------------------------
add_edges_holds_nothing_test(_Config)->
  M1 = elock_test_utils:collector(),
  Request = request(make_ref(), #{}),

  ?assertEqual(undefined, elock_graph:add_edges(Request, undefined)),

  Ref = make_ref(),
  K1 = {s1, t1, node()},
  Graph = #graph{
    edges = #{ K1 => #{ Ref => 1 } },
    index = #{ Ref => {1, #{ K1 => M1 }} }
  },
  ?assertEqual(Graph, elock_graph:add_edges(Request, Graph)),

  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The first waiter that holds something starts the graph: the
%%  exact edges and index, and exactly one probe per held manager
%%  with every field asserted
%%-----------------------------------------------------------------
add_edges_new_graph_test(_Config)->
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  K1 = {s1, t1, node()},
  K2 = {s2, t2, 'other@node'},
  Held = #{ K1 => M1, K2 => M2 },
  Ref = make_ref(),

  Graph = elock_graph:add_edges(request(Ref, Held), undefined),

  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref => 2 },
      K2 => #{ Ref => 2 }
    },
    index = #{
      Ref => {2, Held}
    }
  }, Graph),

  Probe = #deadlock_probe{
    ref = Ref,
    edge = ?EDGE,
    manager = self(),
    weight = 2,
    sent_to = #{ self() => true, M1 => true, M2 => true }
  },
  ?assertEqual([Probe], elock_test_utils:collected(M1, 1)),
  ?assertEqual([Probe], elock_test_utils:collected(M2, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A held lock managed by this very manager (a barging request
%%  holds the term it waits for) is in the edges but gets no probe
%%-----------------------------------------------------------------
add_edges_self_held_test(_Config)->
  M2 = elock_test_utils:collector(),
  K1 = ?EDGE,
  K2 = {s2, t2, node()},
  Held = #{ K1 => self(), K2 => M2 },
  Ref = make_ref(),

  Graph = elock_graph:add_edges(request(Ref, Held), undefined),

  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref => 2 },
      K2 => #{ Ref => 2 }
    },
    index = #{
      Ref => {2, Held}
    }
  }, Graph),

  ?assertEqual([#deadlock_probe{
    ref = Ref,
    edge = ?EDGE,
    manager = self(),
    weight = 2,
    sent_to = #{ self() => true, M2 => true }
  }], elock_test_utils:collected(M2, 1)),
  ?NO_MESSAGE,

  % the only held lock is this manager's - the graph is built, nothing is sent
  Ref2 = make_ref(),
  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref => 2, Ref2 => 1 },
      K2 => #{ Ref => 2 }
    },
    index = #{
      Ref => {2, Held},
      Ref2 => {1, #{ K1 => self() }}
    }
  }, elock_graph:add_edges(request(Ref2, #{ K1 => self() }), Graph)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  Two waiters that hold the same lock share its edge, each with
%%  its own weight
%%-----------------------------------------------------------------
add_edges_second_waiter_test(_Config)->
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  K1 = {s1, t1, node()},
  K2 = {s1, t2, node()},
  Ref1 = make_ref(),
  Ref2 = make_ref(),

  Graph1 = elock_graph:add_edges(request(Ref1, #{ K1 => M1 }), undefined),
  [_Probe1] = elock_test_utils:collected(M1, 1),

  Graph2 = elock_graph:add_edges(request(Ref2, #{ K1 => M1, K2 => M2 }), Graph1),

  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref1 => 1, Ref2 => 2 },
      K2 => #{ Ref2 => 2 }
    },
    index = #{
      Ref1 => {1, #{ K1 => M1 }},
      Ref2 => {2, #{ K1 => M1, K2 => M2 }}
    }
  }, Graph2),

  Probe2 = #deadlock_probe{
    ref = Ref2,
    edge = ?EDGE,
    manager = self(),
    weight = 2,
    sent_to = #{ self() => true, M1 => true, M2 => true }
  },
  ?assertEqual([Probe2], elock_test_utils:collected(M1, 1)),
  ?assertEqual([Probe2], elock_test_utils:collected(M2, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A hold gained by a request that is not in the index yet (it
%%  held nothing when it asked): it joins with the weight 0, the
%%  probe carries the weight 0 and the given edge
%%-----------------------------------------------------------------
add_held_locks_new_request_test(_Config)->
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  K1 = {s1, t1, 'n1@host'},
  Ref = make_ref(),

  Graph = elock_graph:add_held_locks(Ref, ?EDGE, #{ K1 => M1 }, undefined),

  ?assertEqual(#graph{
    edges = #{ K1 => #{ Ref => 0 } },
    index = #{ Ref => {0, #{ K1 => M1 }} }
  }, Graph),
  ?assertEqual([#deadlock_probe{
    ref = Ref,
    edge = ?EDGE,
    manager = self(),
    weight = 0,
    sent_to = #{ self() => true, M1 => true }
  }], elock_test_utils:collected(M1, 1)),
  ?NO_MESSAGE,

  % the same for a ref unknown to an existing graph
  Ref2 = make_ref(),
  K2 = {s1, t1, 'n2@host'},
  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref => 0 },
      K2 => #{ Ref2 => 0 }
    },
    index = #{
      Ref => {0, #{ K1 => M1 }},
      Ref2 => {0, #{ K2 => M2 }}
    }
  }, elock_graph:add_held_locks(Ref2, ?EDGE, #{ K2 => M2 }, Graph)),
  ?assertEqual([#deadlock_probe{
    ref = Ref2,
    edge = ?EDGE,
    manager = self(),
    weight = 0,
    sent_to = #{ self() => true, M2 => true }
  }], elock_test_utils:collected(M2, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The update of a known waiter: the known entries are ignored (no
%%  probe, graph identical), a new key is probed alone, a known key
%%  with a fresh pid replaces the pid and is probed, an empty update
%%  changes nothing. The weight stays as it was
%%-----------------------------------------------------------------
add_held_locks_update_test(_Config)->
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  K1 = {s1, t1, node()},
  K2 = {s1, t2, node()},
  Ref = make_ref(),

  Graph0 = elock_graph:add_edges(request(Ref, #{ K1 => M1 }), undefined),
  [_Probe0] = elock_test_utils:collected(M1, 1),

  % known entry: identical, no probe
  ?assertEqual(Graph0, elock_graph:add_held_locks(Ref, ?EDGE, #{ K1 => M1 }, Graph0)),
  ?NO_MESSAGE,

  % empty update: identical
  ?assertEqual(Graph0, elock_graph:add_held_locks(Ref, ?EDGE, #{}, Graph0)),
  ?NO_MESSAGE,

  % a new key among known ones: only the new one is probed
  Graph1 = elock_graph:add_held_locks(Ref, ?EDGE, #{ K1 => M1, K2 => M2 }, Graph0),
  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref => 1 },
      K2 => #{ Ref => 1 }
    },
    index = #{
      Ref => {1, #{ K1 => M1, K2 => M2 }}
    }
  }, Graph1),
  ?assertEqual([#deadlock_probe{
    ref = Ref,
    edge = ?EDGE,
    manager = self(),
    weight = 1,
    sent_to = #{ self() => true, M2 => true }
  }], elock_test_utils:collected(M2, 1)),
  ?NO_MESSAGE,

  % a known key with a fresh pid: the pid is replaced and probed
  M1b = elock_test_utils:collector(),
  Graph2 = elock_graph:add_held_locks(Ref, ?EDGE, #{ K1 => M1b }, Graph1),
  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref => 1 },
      K2 => #{ Ref => 1 }
    },
    index = #{
      Ref => {1, #{ K1 => M1b, K2 => M2 }}
    }
  }, Graph2),
  ?assertEqual([#deadlock_probe{
    ref = Ref,
    edge = ?EDGE,
    manager = self(),
    weight = 1,
    sent_to = #{ self() => true, M1b => true }
  }], elock_test_utils:collected(M1b, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A request stops waiting: the last waiter takes the graph with it
%%  (undefined), one of two leaves the shared keys to the other and
%%  its own keys vanish, an unknown ref changes nothing, undefined
%%  stays undefined
%%-----------------------------------------------------------------
remove_edges_test(_Config)->
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  K1 = {s1, t1, node()},
  K2 = {s1, t2, node()},
  Ref1 = make_ref(),
  Ref2 = make_ref(),

  ?assertEqual(undefined, elock_graph:remove_edges(Ref1, undefined)),

  Graph1 = elock_graph:add_edges(request(Ref1, #{ K1 => M1 }), undefined),
  Graph2 = elock_graph:add_edges(request(Ref2, #{ K1 => M1, K2 => M2 }), Graph1),
  [_, _] = elock_test_utils:collected(M1, 2),
  [_] = elock_test_utils:collected(M2, 1),

  % unknown ref
  ?assertEqual(Graph2, elock_graph:remove_edges(make_ref(), Graph2)),

  % the second waiter leaves: K1 keeps the first, K2 vanishes
  ?assertEqual(Graph1, elock_graph:remove_edges(Ref2, Graph2)),
  ?assertEqual(#graph{
    edges = #{ K1 => #{ Ref1 => 1 } },
    index = #{ Ref1 => {1, #{ K1 => M1 }} }
  }, Graph1),

  % the first waiter leaves instead: the second keeps both keys
  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref2 => 2 },
      K2 => #{ Ref2 => 2 }
    },
    index = #{
      Ref2 => {2, #{ K1 => M1, K2 => M2 }}
    }
  }, elock_graph:remove_edges(Ref1, Graph2)),

  % the last waiter takes the graph with it
  ?assertEqual(undefined, elock_graph:remove_edges(Ref1, Graph1)),

  ?NO_MESSAGE.

%%=================================================================
%%  The probes
%%=================================================================
%%-----------------------------------------------------------------
%%  Without a graph nothing closes a cycle and nothing is forwarded
%%-----------------------------------------------------------------
probe_no_graph_test(_Config)->
  OM = elock_test_utils:collector(),
  Probe = probe(make_ref(), OM, 1),

  ?assertEqual({forward, []}, elock_graph:probe(Probe, undefined)),
  ?assertEqual(ok, elock_graph:forward(Probe, undefined)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  Nobody here holds the lock the origin waits for
%%-----------------------------------------------------------------
probe_edge_not_held_test(_Config)->
  OM = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  CRef = make_ref(),
  K1 = {s1, t1, node()},
  Graph = #graph{
    edges = #{ K1 => #{ CRef => 1 } },
    index = #{ CRef => {1, #{ K1 => M1 }} }
  },
  Probe = probe(make_ref(), OM, 1),

  ?assertEqual({forward, []}, elock_graph:probe(Probe, Graph)),
  ?NO_MESSAGE,

  % and the probe goes on to the managers of the locks the waiters hold
  ?assertEqual(ok, elock_graph:forward(Probe, Graph)),
  ?assertEqual([Probe#deadlock_probe{
    sent_to = #{ OM => true, self() => true, M1 => true }
  }], elock_test_utils:collected(M1, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A closer heavier than the origin: the origin loses - #deadlock{}
%%  with the origin's ref goes to the origin manager, the probe
%%  stops, the closer is left alone
%%-----------------------------------------------------------------
probe_origin_loses_test(_Config)->
  OM = elock_test_utils:collector(),
  ORef = make_ref(),
  CRef = make_ref(),
  Graph = #graph{
    edges = #{ ?ORIGIN_EDGE => #{ CRef => 2 } },
    index = #{ CRef => {2, #{ ?ORIGIN_EDGE => OM }} }
  },

  ?assertEqual(stop, elock_graph:probe(probe(ORef, OM, 1), Graph)),
  ?assertEqual([#deadlock{ref = ORef}], elock_test_utils:collected(OM, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A closer lighter than the origin loses: it is named to abort,
%%  nothing is sent by the graph
%%-----------------------------------------------------------------
probe_closer_loses_test(_Config)->
  OM = elock_test_utils:collector(),
  ORef = make_ref(),
  CRef = make_ref(),
  Graph = #graph{
    edges = #{ ?ORIGIN_EDGE => #{ CRef => 1 } },
    index = #{ CRef => {1, #{ ?ORIGIN_EDGE => OM }} }
  },

  ?assertEqual({forward, [CRef]}, elock_graph:probe(probe(ORef, OM, 2), Graph)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  Equal weights: the coin decides. A closer the coin favours makes
%%  the origin lose, a closer the coin rejects loses itself
%%-----------------------------------------------------------------
probe_tie_test(_Config)->
  OM = elock_test_utils:collector(),
  ORef = make_ref(),
  Winner = ref_until(fun(Ref)-> elock_graph:drop_coin(Ref, ORef) =:= Ref end),
  Loser = ref_until(fun(Ref)-> elock_graph:drop_coin(Ref, ORef) =:= ORef end),

  WinnerGraph = #graph{
    edges = #{ ?ORIGIN_EDGE => #{ Winner => 1 } },
    index = #{ Winner => {1, #{ ?ORIGIN_EDGE => OM }} }
  },
  ?assertEqual(stop, elock_graph:probe(probe(ORef, OM, 1), WinnerGraph)),
  ?assertEqual([#deadlock{ref = ORef}], elock_test_utils:collected(OM, 1)),
  ?NO_MESSAGE,

  LoserGraph = #graph{
    edges = #{ ?ORIGIN_EDGE => #{ Loser => 1 } },
    index = #{ Loser => {1, #{ ?ORIGIN_EDGE => OM }} }
  },
  ?assertEqual({forward, [Loser]}, elock_graph:probe(probe(ORef, OM, 1), LoserGraph)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A hold on the origin's edge by another manager pid is a stale
%%  one - not an edge, the waiter is not a closer
%%-----------------------------------------------------------------
probe_stale_hold_test(_Config)->
  OM = elock_test_utils:collector(),
  Stale = elock_test_utils:collector(),
  ORef = make_ref(),
  CRef = make_ref(),
  Graph = #graph{
    edges = #{ ?ORIGIN_EDGE => #{ CRef => 5 } },
    index = #{ CRef => {5, #{ ?ORIGIN_EDGE => Stale }} }
  },

  ?assertEqual({forward, []}, elock_graph:probe(probe(ORef, OM, 1), Graph)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The origin's own ref among the holders of its edge is skipped:
%%  a request can not close a cycle with itself
%%-----------------------------------------------------------------
probe_skips_origin_test(_Config)->
  OM = elock_test_utils:collector(),
  ORef = make_ref(),
  Graph = #graph{
    edges = #{ ?ORIGIN_EDGE => #{ ORef => 5 } },
    index = #{ ORef => {5, #{ ?ORIGIN_EDGE => OM }} }
  },

  ?assertEqual({forward, []}, elock_graph:probe(probe(ORef, OM, 1), Graph)),
  ?NO_MESSAGE,

  % the origin among real closers is skipped as well
  CRef = make_ref(),
  Graph2 = #graph{
    edges = #{ ?ORIGIN_EDGE => #{ ORef => 5, CRef => 1 } },
    index = #{
      ORef => {5, #{ ?ORIGIN_EDGE => OM }},
      CRef => {1, #{ ?ORIGIN_EDGE => OM }}
    }
  },
  ?assertEqual({forward, [CRef]}, elock_graph:probe(probe(ORef, OM, 2), Graph2)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  Several closers: every lighter one is named; a single heavier
%%  one among them makes the origin lose instead
%%-----------------------------------------------------------------
probe_multiple_closers_test(_Config)->
  OM = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  ORef = make_ref(),
  C1 = make_ref(),
  C2 = make_ref(),
  C3 = make_ref(),
  W = make_ref(),
  K3 = {s3, t3, node()},
  Graph = #graph{
    edges = #{
      ?ORIGIN_EDGE => #{ C1 => 1, C2 => 2 },
      K3 => #{ W => 4 }
    },
    index = #{
      C1 => {1, #{ ?ORIGIN_EDGE => OM }},
      C2 => {2, #{ ?ORIGIN_EDGE => OM }},
      W => {4, #{ K3 => M3 }}
    }
  },

  {forward, Closers} = elock_graph:probe(probe(ORef, OM, 3), Graph),
  ?assertEqual(lists:sort([C1, C2]), lists:sort(Closers)),
  ?NO_MESSAGE,

  Graph2 = #graph{
    edges = #{
      ?ORIGIN_EDGE => #{ C1 => 1, C2 => 2, C3 => 4 },
      K3 => #{ W => 4 }
    },
    index = #{
      C1 => {1, #{ ?ORIGIN_EDGE => OM }},
      C2 => {2, #{ ?ORIGIN_EDGE => OM }},
      C3 => {4, #{ ?ORIGIN_EDGE => OM }},
      W => {4, #{ K3 => M3 }}
    }
  },
  ?assertEqual(stop, elock_graph:probe(probe(ORef, OM, 3), Graph2)),
  ?assertEqual([#deadlock{ref = ORef}], elock_test_utils:collected(OM, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The probe goes on to every manager of every lock held by the
%%  waiters, except those in sent_to, once per manager even when
%%  several waiters hold its locks; sent_to of the forwarded probe
%%  is the old one plus the targets
%%-----------------------------------------------------------------
forward_test(_Config)->
  OM = elock_test_utils:collector(),
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  R1 = make_ref(),
  R2 = make_ref(),
  R3 = make_ref(),
  K1 = {s1, t1, node()},
  K2 = {s1, t2, node()},
  K2b = {s2, t2, node()},
  K3 = {s1, t3, 'n3@host'},
  Graph = #graph{
    edges = #{
      K1 => #{ R1 => 1 },
      K2 => #{ R2 => 2 },
      K2b => #{ R3 => 1 },
      K3 => #{ R2 => 2 }
    },
    index = #{
      R1 => {1, #{ K1 => M1 }},
      R2 => {2, #{ K2 => M2, K3 => M3 }},
      R3 => {1, #{ K2b => M2 }}
    }
  },
  Probe = #deadlock_probe{
    ref = make_ref(),
    edge = ?ORIGIN_EDGE,
    manager = OM,
    weight = 1,
    sent_to = #{ OM => true, self() => true, M1 => true }
  },

  ?assertEqual(ok, elock_graph:forward(Probe, Graph)),

  Forwarded = Probe#deadlock_probe{
    sent_to = #{ OM => true, self() => true, M1 => true, M2 => true, M3 => true }
  },
  ?assertEqual([Forwarded], elock_test_utils:collected(M2, 1)),
  ?assertEqual([Forwarded], elock_test_utils:collected(M3, 1)),
  % M1 and the origin manager have seen it already
  ?NO_MESSAGE,

  % everybody has seen it - nothing goes
  ?assertEqual(ok, elock_graph:forward(Forwarded, Graph)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The coin is symmetric, deterministic and returns one of the two
%%-----------------------------------------------------------------
drop_coin_test(_Config)->
  Pairs = [ {make_ref(), make_ref()} || _ <- lists:seq(1, 200) ],
  Outcomes =
    [ begin
        Winner = elock_graph:drop_coin(A, B),
        ?assert(Winner =:= A orelse Winner =:= B),
        ?assertEqual(Winner, elock_graph:drop_coin(B, A)),
        ?assertEqual(Winner, elock_graph:drop_coin(A, B)),
        Winner =:= min(A, B)
      end || {A, B} <- Pairs ],
  % both sides win now and then
  ?assert(lists:member(true, Outcomes)),
  ?assert(lists:member(false, Outcomes)).

%%=================================================================
%%  Utilities
%%=================================================================
% A request that waits at this manager holding Held
request(Ref, Held)->
  #request{
    ref = Ref,
    scope = ?SCOPE,
    term = ?TERM,
    client = self(),
    shared = false,
    held = Held,
    nodes = [node()],
    timeout = undefined
  }.

% A probe of the origin request Ref waiting at the origin manager OM
probe(Ref, OM, Weight)->
  #deadlock_probe{
    ref = Ref,
    edge = ?ORIGIN_EDGE,
    manager = OM,
    weight = Weight,
    sent_to = #{ OM => true, self() => true }
  }.

% A fresh reference satisfying Pred, bounded
ref_until(Pred)->
  ref_until(Pred, 10000).
ref_until(_Pred, 0)->
  erlang:error(no_such_ref);
ref_until(Pred, Attempts)->
  Ref = make_ref(),
  case Pred(Ref) of
    true-> Ref;
    false-> ref_until(Pred, Attempts - 1)
  end.
