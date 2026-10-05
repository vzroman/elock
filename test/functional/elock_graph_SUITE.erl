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
  probe_seen_test/1,
  probe_new_launch_test/1,
  probe_seen_lifecycle_test/1,
  launch_ids_test/1,
  forward_test/1,
  drop_coin_test/1
]).

% mirrors elock_graph.erl
-record(graph,{
  edges,
  index,
  seen = #{}
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
      probe_seen_test,
      probe_new_launch_test,
      probe_seen_lifecycle_test,
      launch_ids_test,
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
%%  The first waiter whose holds come in starts the graph: the
%%  exact edges and index with the weight that is passed, and
%%  exactly one probe per held manager with every field asserted
%%-----------------------------------------------------------------
add_edges_new_graph_test(_Config)->
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  K1 = {s1, t1, node()},
  K2 = {s2, t2, 'other@node'},
  Held = #{ K1 => M1, K2 => M2 },
  Ref = make_ref(),

  Graph = elock_graph:add_edges(Ref, ?EDGE, Held, 2, undefined),

  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref => 2 },
      K2 => #{ Ref => 2 }
    },
    index = #{
      Ref => {2, Held}
    }
  }, Graph),

  Probe = assert_probe(M1, #deadlock_probe{
    ref = Ref,
    edge = ?EDGE,
    manager = self(),
    weight = 2,
    sent_to = #{ self() => true, M1 => true, M2 => true }
  }),
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

  Graph = elock_graph:add_edges(Ref, ?EDGE, Held, 2, undefined),

  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref => 2 },
      K2 => #{ Ref => 2 }
    },
    index = #{
      Ref => {2, Held}
    }
  }, Graph),

  assert_probe(M2, #deadlock_probe{
    ref = Ref,
    edge = ?EDGE,
    manager = self(),
    weight = 2,
    sent_to = #{ self() => true, M2 => true }
  }),
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
  }, elock_graph:add_edges(Ref2, ?EDGE, #{ K1 => self() }, 1, Graph)),
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

  Graph1 = elock_graph:add_edges(Ref1, ?EDGE, #{ K1 => M1 }, 1, undefined),
  [_Probe1] = elock_test_utils:collected(M1, 1),

  Graph2 = elock_graph:add_edges(Ref2, ?EDGE, #{ K1 => M1, K2 => M2 }, 2, Graph1),

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

  Probe2 = assert_probe(M1, #deadlock_probe{
    ref = Ref2,
    edge = ?EDGE,
    manager = self(),
    weight = 2,
    sent_to = #{ self() => true, M1 => true, M2 => true }
  }),
  ?assertEqual([Probe2], elock_test_utils:collected(M2, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A request that is not in the index joins with the weight that
%%  is passed, whatever the size of the update: 0 (a multi node
%%  request whose client held nothing reports a grant) and a
%%  positive one (the client held three locks, here comes one), the
%%  probe carries the same weight and the given edge. Without a
%%  graph and in a graph of other requests alike
%%-----------------------------------------------------------------
add_held_locks_new_request_test(_Config)->
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  K1 = {s1, t1, 'n1@host'},
  Ref = make_ref(),

  Graph = elock_graph:add_edges(Ref, ?EDGE, #{ K1 => M1 }, 0, undefined),

  ?assertEqual(#graph{
    edges = #{ K1 => #{ Ref => 0 } },
    index = #{ Ref => {0, #{ K1 => M1 }} }
  }, Graph),
  assert_probe(M1, #deadlock_probe{
    ref = Ref,
    edge = ?EDGE,
    manager = self(),
    weight = 0,
    sent_to = #{ self() => true, M1 => true }
  }),
  ?NO_MESSAGE,

  % the same for a ref unknown to an existing graph
  Ref2 = make_ref(),
  K2 = {s1, t1, 'n2@host'},
  Graph2 = elock_graph:add_edges(Ref2, ?EDGE, #{ K2 => M2 }, 0, Graph),
  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref => 0 },
      K2 => #{ Ref2 => 0 }
    },
    index = #{
      Ref => {0, #{ K1 => M1 }},
      Ref2 => {0, #{ K2 => M2 }}
    }
  }, Graph2),
  assert_probe(M2, #deadlock_probe{
    ref = Ref2,
    edge = ?EDGE,
    manager = self(),
    weight = 0,
    sent_to = #{ self() => true, M2 => true }
  }),
  ?NO_MESSAGE,

  % a positive weight: the one passed, not the size of the update
  Ref3 = make_ref(),
  K3 = {s2, t3, node()},
  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref => 0 },
      K2 => #{ Ref2 => 0 },
      K3 => #{ Ref3 => 3 }
    },
    index = #{
      Ref => {0, #{ K1 => M1 }},
      Ref2 => {0, #{ K2 => M2 }},
      Ref3 => {3, #{ K3 => M3 }}
    }
  }, elock_graph:add_edges(Ref3, ?EDGE, #{ K3 => M3 }, 3, Graph2)),
  assert_probe(M3, #deadlock_probe{
    ref = Ref3,
    edge = ?EDGE,
    manager = self(),
    weight = 3,
    sent_to = #{ self() => true, M3 => true }
  }),
  ?NO_MESSAGE,

  % and without a graph: the update is larger than the weight (the
  % held lock of the client and the grants of two nodes in one answer)
  Ref4 = make_ref(),
  Held4 = #{ K1 => M1, K2 => M2, K3 => M3 },
  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref4 => 1 },
      K2 => #{ Ref4 => 1 },
      K3 => #{ Ref4 => 1 }
    },
    index = #{ Ref4 => {1, Held4} }
  }, elock_graph:add_edges(Ref4, ?EDGE, Held4, 1, undefined)),
  Probe4 = assert_probe(M1, #deadlock_probe{
    ref = Ref4,
    edge = ?EDGE,
    manager = self(),
    weight = 1,
    sent_to = #{ self() => true, M1 => true, M2 => true, M3 => true }
  }),
  ?assertEqual([Probe4], elock_test_utils:collected(M2, 1)),
  ?assertEqual([Probe4], elock_test_utils:collected(M3, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  The update of a known waiter. It keeps the weight it has joined
%%  with whatever weight is passed, in the index, in the edges and
%%  in the probes. An entry it has already leaves the graph
%%  identical and is not probed; of an update with known and new
%%  keys only the new ones are merged and probed; a known key with
%%  a fresh pid replaces the pid and is probed, alone
%%-----------------------------------------------------------------
add_held_locks_update_test(_Config)->
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  K1 = {s1, t1, node()},
  K2 = {s1, t2, node()},
  K3 = {s2, t1, 'n3@host'},
  Ref = make_ref(),

  Graph0 = elock_graph:add_edges(Ref, ?EDGE, #{ K1 => M1 }, 1, undefined),
  [_Probe0] = elock_test_utils:collected(M1, 1),

  % known entry: identical, no probe, with the same weight and with another one
  ?assertEqual(Graph0, elock_graph:add_edges(Ref, ?EDGE, #{ K1 => M1 }, 1, Graph0)),
  ?assertEqual(Graph0, elock_graph:add_edges(Ref, ?EDGE, #{ K1 => M1 }, 5, Graph0)),
  ?NO_MESSAGE,

  % new keys among known ones: only the new ones are merged and probed,
  % the weight stays 1 though 5 is passed
  Graph1 = elock_graph:add_edges(Ref, ?EDGE, #{ K1 => M1, K2 => M2, K3 => M3 }, 5, Graph0),
  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref => 1 },
      K2 => #{ Ref => 1 },
      K3 => #{ Ref => 1 }
    },
    index = #{
      Ref => {1, #{ K1 => M1, K2 => M2, K3 => M3 }}
    }
  }, Graph1),
  Probe1 = assert_probe(M2, #deadlock_probe{
    ref = Ref,
    edge = ?EDGE,
    manager = self(),
    weight = 1,
    sent_to = #{ self() => true, M2 => true, M3 => true }
  }),
  ?assertEqual([Probe1], elock_test_utils:collected(M3, 1)),
  ?NO_MESSAGE,

  % the whole held map once more: every entry is known by now
  ?assertEqual(Graph1, elock_graph:add_edges(Ref, ?EDGE, #{ K1 => M1, K2 => M2, K3 => M3 }, 3, Graph1)),
  ?NO_MESSAGE,

  % a known key with a fresh pid among known entries: the pid is
  % replaced and probed alone, a lighter weight is not taken either
  M1b = elock_test_utils:collector(),
  Graph2 = elock_graph:add_edges(Ref, ?EDGE, #{ K1 => M1b, K2 => M2 }, 0, Graph1),
  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref => 1 },
      K2 => #{ Ref => 1 },
      K3 => #{ Ref => 1 }
    },
    index = #{
      Ref => {1, #{ K1 => M1b, K2 => M2, K3 => M3 }}
    }
  }, Graph2),
  assert_probe(M1b, #deadlock_probe{
    ref = Ref,
    edge = ?EDGE,
    manager = self(),
    weight = 1,
    sent_to = #{ self() => true, M1b => true }
  }),
  ?NO_MESSAGE,

  % the other waiters are not touched by the update of this one
  Ref2 = make_ref(),
  Graph3 = elock_graph:add_edges(Ref2, ?EDGE, #{ K2 => M2 }, 4, Graph2),
  [_Probe2] = elock_test_utils:collected(M2, 1),
  ?assertEqual(Graph3, elock_graph:add_edges(Ref2, ?EDGE, #{ K2 => M2 }, 1, Graph3)),
  ?NO_MESSAGE,
  ?assertEqual(#graph{
    edges = #{
      K1 => #{ Ref => 1, Ref2 => 4 },
      K2 => #{ Ref => 1, Ref2 => 4 },
      K3 => #{ Ref => 1 }
    },
    index = #{
      Ref => {1, #{ K1 => M1b, K2 => M2, K3 => M3 }},
      Ref2 => {4, #{ K1 => M1b, K2 => M2 }}
    }
  }, elock_graph:add_edges(Ref2, ?EDGE, #{ K1 => M1b }, 1, Graph3)),
  assert_probe(M1b, #deadlock_probe{
    ref = Ref2,
    edge = ?EDGE,
    manager = self(),
    weight = 4,
    sent_to = #{ self() => true, M1b => true }
  }),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A request stops waiting: the last waiter takes the graph with it
%%  (undefined), one of two leaves the shared keys to the other and
%%  its own keys vanish, the keys that came in with a later update
%%  go as well, an unknown ref changes nothing, undefined stays
%%  undefined
%%-----------------------------------------------------------------
remove_edges_test(_Config)->
  M1 = elock_test_utils:collector(),
  M2 = elock_test_utils:collector(),
  M3 = elock_test_utils:collector(),
  K1 = {s1, t1, node()},
  K2 = {s1, t2, node()},
  K3 = {s1, t3, node()},
  Ref1 = make_ref(),
  Ref2 = make_ref(),

  ?assertEqual(undefined, elock_graph:remove_edges(Ref1, undefined)),

  Graph1 = elock_graph:add_edges(Ref1, ?EDGE, #{ K1 => M1 }, 1, undefined),
  Graph2 = elock_graph:add_edges(Ref2, ?EDGE, #{ K1 => M1, K2 => M2 }, 2, Graph1),
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

  % the keys of a later update leave with the waiter as well
  Graph3 = elock_graph:add_edges(Ref2, ?EDGE, #{ K3 => M3 }, 2, Graph2),
  [_] = elock_test_utils:collected(M3, 1),
  ?assertEqual(Graph1, elock_graph:remove_edges(Ref2, Graph3)),

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

  ?assertEqual(stop, elock_graph:probe(Probe, ?EDGE, undefined)),
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

  Graph1 = assert_forward(Probe, Graph, []),
  ?NO_MESSAGE,

  % and the probe goes on to the managers of the locks the waiters hold
  ?assertEqual(ok, elock_graph:forward(Probe, Graph1)),
  ?assertEqual([Probe#deadlock_probe{
    sent_to = #{ OM => true, self() => true, M1 => true }
  }], elock_test_utils:collected(M1, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A closer heavier than the origin: the origin loses - #deadlock{}
%%  with the origin's ref goes to the origin manager, the probe
%%  stops, the closer is left alone. The launch is not recorded, so
%%  the same copy gives the same verdict again
%%-----------------------------------------------------------------
probe_origin_loses_test(_Config)->
  OM = elock_test_utils:collector(),
  ORef = make_ref(),
  CRef = make_ref(),
  Graph = #graph{
    edges = #{ ?ORIGIN_EDGE => #{ CRef => 2 } },
    index = #{ CRef => {2, #{ ?ORIGIN_EDGE => OM }} }
  },

  Probe = probe(ORef, OM, 1),
  ?assertEqual(stop, elock_graph:probe(Probe, ?EDGE, Graph)),
  ?assertEqual([#deadlock{ref = ORef, winner = ?EDGE}], elock_test_utils:collected(OM, 1)),
  ?assertEqual(stop, elock_graph:probe(Probe, ?EDGE, Graph)),
  ?assertEqual([#deadlock{ref = ORef, winner = ?EDGE}], elock_test_utils:collected(OM, 1)),
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

  assert_forward(probe(ORef, OM, 2), Graph, [CRef]),
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
  ?assertEqual(stop, elock_graph:probe(probe(ORef, OM, 1), ?EDGE, WinnerGraph)),
  ?assertEqual([#deadlock{ref = ORef, winner = ?EDGE}], elock_test_utils:collected(OM, 1)),
  ?NO_MESSAGE,

  LoserGraph = #graph{
    edges = #{ ?ORIGIN_EDGE => #{ Loser => 1 } },
    index = #{ Loser => {1, #{ ?ORIGIN_EDGE => OM }} }
  },
  assert_forward(probe(ORef, OM, 1), LoserGraph, [Loser]),
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

  assert_forward(probe(ORef, OM, 1), Graph, []),
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

  assert_forward(probe(ORef, OM, 1), Graph, []),
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
  assert_forward(probe(ORef, OM, 2), Graph2, [CRef]),
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

  #deadlock_probe{id = Id} = Probe = probe(ORef, OM, 3),
  {forward, Closers, Graph1} = elock_graph:probe(Probe, ?EDGE, Graph),
  ?assertEqual(lists:sort([C1, C2]), lists:sort(Closers)),
  ?assertEqual(Graph#graph{seen = #{Id => true}}, Graph1),
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
  ?assertEqual(stop, elock_graph:probe(probe(ORef, OM, 3), ?EDGE, Graph2)),
  ?assertEqual([#deadlock{ref = ORef, winner = ?EDGE}], elock_test_utils:collected(OM, 1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A launch is handled once, even when another branch brings a copy
%%  with different sent_to. Its closers are not returned again
%%-----------------------------------------------------------------
probe_seen_test(_Config)->
  OM = elock_test_utils:collector(),
  Branch = elock_test_utils:collector(),
  CRef = make_ref(),
  Graph = #graph{
    edges = #{ ?ORIGIN_EDGE => #{ CRef => 1 } },
    index = #{ CRef => {1, #{ ?ORIGIN_EDGE => OM }} }
  },
  Probe = probe(make_ref(), OM, 2),
  Graph1 = assert_forward(Probe, Graph, [CRef]),
  ?assertEqual(stop, elock_graph:probe(Probe, ?EDGE, Graph1)),
  Copy = Probe#deadlock_probe{
    sent_to = #{ OM => true, self() => true, Branch => true }
  },
  ?assertEqual(stop, elock_graph:probe(Copy, ?EDGE, Graph1)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  A new launch of the same origin passes a manager that saw the old
%%  launch: only the id differs, both are remembered
%%-----------------------------------------------------------------
probe_new_launch_test(_Config)->
  OM = elock_test_utils:collector(),
  CRef = make_ref(),
  Graph = #graph{
    edges = #{ ?ORIGIN_EDGE => #{ CRef => 1 } },
    index = #{ CRef => {1, #{ ?ORIGIN_EDGE => OM }} }
  },
  Probe = probe(make_ref(), OM, 2),
  Graph1 = assert_forward(Probe, Graph, [CRef]),
  Next = Probe#deadlock_probe{id = make_ref()},
  Graph2 = assert_forward(Next, Graph1, [CRef]),
  ?assertEqual(2, map_size(Graph2#graph.seen)),
  ?assertEqual(stop, elock_graph:probe(Next, ?EDGE, Graph2)),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  Edge updates and removals retain seen launches until the last
%%  waiter leaves. A rebuilt graph handles an old copy again
%%-----------------------------------------------------------------
probe_seen_lifecycle_test(_Config)->
  OM = elock_test_utils:collector(),
  Ref1 = make_ref(),
  Ref2 = make_ref(),
  Held = #{ ?EDGE => self() },
  Graph0 = elock_graph:add_edges(Ref1, ?EDGE, Held, 1, undefined),
  ?assertEqual(#{}, Graph0#graph.seen),
  #deadlock_probe{id = Id} = Probe = probe(make_ref(), OM, 2),
  Graph1 = assert_forward(Probe, Graph0, []),
  ?assertEqual(Graph1, elock_graph:add_edges(Ref1, ?EDGE, Held, 1, Graph1)),
  Graph2 = elock_graph:add_edges(Ref2, ?EDGE, Held, 1, Graph1),
  ?assertEqual(#{Id => true}, Graph2#graph.seen),
  Graph3 = elock_graph:add_edges(Ref1, ?EDGE,
    #{ {s2, t2, node()} => self() }, 1, Graph2),
  ?assertEqual(#{Id => true}, Graph3#graph.seen),
  ?assertEqual(Graph3, elock_graph:remove_edges(make_ref(), Graph3)),
  Graph4 = elock_graph:remove_edges(Ref1, Graph3),
  ?assertEqual(#{Id => true}, Graph4#graph.seen),
  ?assertEqual(stop, elock_graph:probe(Probe, ?EDGE, Graph4)),
  ?assertEqual(undefined, elock_graph:remove_edges(Ref2, Graph4)),

  Rebuilt = elock_graph:add_edges(Ref1, ?EDGE, Held, 1, undefined),
  ?assertEqual(#{}, Rebuilt#graph.seen),
  assert_forward(Probe, Rebuilt, []),
  ?NO_MESSAGE.

%%-----------------------------------------------------------------
%%  Later holds of one request launch fresh ids, including after its
%%  graph is dropped; the request, edge and origin manager stay the same
%%-----------------------------------------------------------------
launch_ids_test(_Config)->
  M1 = elock_test_utils:collector(),
  Ref = make_ref(),
  Held = #{ {s1, t1, node()} => M1 },
  Expected = #deadlock_probe{
    ref = Ref,
    edge = ?EDGE,
    manager = self(),
    weight = 1,
    sent_to = #{ self() => true, M1 => true }
  },
  Graph1 = elock_graph:add_edges(Ref, ?EDGE, Held, 1, undefined),
  #deadlock_probe{id = Id1} = assert_probe(M1, Expected),
  Graph2 = elock_graph:add_edges(Ref, ?EDGE,
    #{ {s1, t2, node()} => M1 }, 1, Graph1),
  #deadlock_probe{id = Id2} = assert_probe(M1, Expected),
  ?assertNotEqual(Id1, Id2),
  undefined = elock_graph:remove_edges(Ref, Graph2),
  elock_graph:add_edges(Ref, ?EDGE, Held, 1, undefined),
  #deadlock_probe{id = Id3} = assert_probe(M1, Expected),
  ?assertNotEqual(Id1, Id3),
  ?assertNotEqual(Id2, Id3),
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
    id = make_ref(),
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
% The closers and the whole graph must match, with only this launch added.
assert_forward(#deadlock_probe{id = Id} = Probe, Graph, Closers)->
  Expected = Graph#graph{seen = (Graph#graph.seen)#{Id => true}},
  ?assertEqual({forward, Closers, Expected},
    elock_graph:probe(Probe, ?EDGE, Graph)),
  Expected.

% Checks every field of a launched probe, including its fresh reference.
% Returns the copy so other recipients can be checked against the same id.
assert_probe(Collector, Expected)->
  [#deadlock_probe{id = Id} = Probe] = elock_test_utils:collected(Collector, 1),
  ?assert(is_reference(Id)),
  ?assertEqual(Expected#deadlock_probe{id = Id}, Probe),
  Probe.

% A probe of the origin request Ref waiting at the origin manager OM
probe(Ref, OM, Weight)->
  #deadlock_probe{
    id = make_ref(),
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
