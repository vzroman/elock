%%=================================================================
%%  The peer node environment of the multi node suites. Adapted
%%  from ecall/test/performance/util/distributed_tests_utils.erl:
%%  the docker, ssh and metrics parts are dropped, the nodes are
%%  plain peer nodes on this host started with the code paths of
%%  the build, so that every node has elock, ecall and the compiled
%%  test modules.
%%
%%  start_nodes/1 makes the controller (the ct node) distributed on
%%  demand with a unique cookie per run, starts the peers, connects
%%  them pairwise, starts ecall on every one of them and establishes
%%  the ecall connections in both directions, verified. The
%%  controller itself does not run ecall: it only drives the clients
%%  on the peers through rpc. The nodes started so far live in a
%%  persistent_term, in the order they were started.
%%
%%  Every node name carries the OS pid of the controller and a
%%  unique integer: a leftover node of a crashed run can never clash
%%  in epmd and the name of a killed node is never reused.
%%
%%  The elock scopes are not the business of this module: the suites
%%  start them per node with elock_test_utils:start_scope/2
%%=================================================================
-module(distributed_tests_utils).

-compile({no_auto_import, [nodes/0]}).

%% API
-export([
  start_nodes/1,
  stop_nodes/1,
  start_node/1,
  stop_node/1,
  kill_node/1,
  nodes/0
]).

-define(STATE_KEY, {?MODULE, state}).
-define(HOST, "127.0.0.1").
-define(RPC_TIMEOUT, 30000).
-define(DEADLINE, 30000).

%%=================================================================
%%  API
%%=================================================================
%%-----------------------------------------------------------------
%%  Start the nodes of the configs ([#{name => atom()}]), connect
%%  them pairwise, start ecall on each and connect ecall both ways.
%%  The result is the node names in the order of the configs
%%-----------------------------------------------------------------
start_nodes(Configs)->
  Cookie = ensure_controller(),
  Nodes = [ start_peer(Config, Cookie) || Config <- Configs ],
  ok = connect_nodes(Nodes),
  ok = start_ecall(Nodes),
  ok = connect_ecall(Nodes),
  Nodes.

stop_nodes(Nodes)->
  [ ok = stop_node(Node) || Node <- Nodes ],
  ok.

%%-----------------------------------------------------------------
%%  One more node: connected to every node started so far, ecall
%%  started and connected both ways with each of them
%%-----------------------------------------------------------------
start_node(Config)->
  Cookie = ensure_controller(),
  Known = nodes(),
  Node = start_peer(Config, Cookie),
  Nodes = Known ++ [Node],
  ok = connect_nodes(Nodes),
  ok = start_ecall([Node]),
  ok = connect_ecall(Nodes),
  Node.

%%-----------------------------------------------------------------
%%  Graceful stop: peer:stop/1 (init:stop on the node, killed after
%%  the shutdown timeout). Returns once the node is gone from the
%%  view of the controller and of every survivor, and the survivors
%%  have dropped their ecall connection to it
%%-----------------------------------------------------------------
stop_node(Node)->
  Peer = take_node(Node),
  Survivors = nodes(),
  stop_peer(Peer),
  wait_gone(Node, Survivors),
  ok.

%%-----------------------------------------------------------------
%%  Abrupt stop: erlang:halt() on the node, no shutdown of the
%%  applications, no goodbye to the other nodes. Returns once the
%%  node is gone from the view of the controller and of every
%%  survivor, the survivors have dropped their ecall connection to
%%  it and still hold their connections to each other, and the peer
%%  control process is gone
%%-----------------------------------------------------------------
kill_node(Node)->
  Peer = take_node(Node),
  Survivors = nodes(),
  rpc:cast(Node, erlang, halt, []),
  wait_gone(Node, Survivors),
  stop_peer(Peer),
  ok.

%%-----------------------------------------------------------------
%%  The nodes started so far, in the order they were started
%%-----------------------------------------------------------------
nodes()->
  [ Node || {Node, _Peer} <- maps:get(nodes, state()) ].

%%=================================================================
%%  The controller
%%=================================================================
%%-----------------------------------------------------------------
%%  The ct node is not distributed (nonode@nohost): it becomes so
%%  on demand under a unique name, with a unique cookie per run.
%%  The cookie stays for the life of the controller, hence every
%%  node started later joins the same cluster
%%-----------------------------------------------------------------
ensure_controller()->
  case state() of
    #{cookie := Cookie}->
      Cookie;
    _->
      Cookie = unique_cookie(),
      case node() of
        nonode@nohost->
          start_distribution();
        _->
          ok
      end,
      true = erlang:set_cookie(node(), Cookie),
      put_state(#{cookie => Cookie, nodes => []}),
      Cookie
  end.

%%-----------------------------------------------------------------
%%  net_kernel needs epmd, which a non-distributed VM does not
%%  start by itself: on a failure the epmd of this release is
%%  started as a daemon and the start is retried once
%%-----------------------------------------------------------------
start_distribution()->
  Name = controller_name(),
  case net_kernel:start([Name, longnames]) of
    {ok, _}->
      ok;
    {error, _}->
      Epmd = filename:join([code:root_dir(), "erts-" ++ erlang:system_info(version), "bin", "epmd"]),
      _ = os:cmd(Epmd ++ " -daemon"),
      {ok, _} = net_kernel:start([Name, longnames]),
      ok
  end.

controller_name()->
  list_to_atom("elock_controller_" ++ os:getpid() ++ "@" ++ ?HOST).

unique_cookie()->
  list_to_atom("elock_" ++ os:getpid() ++ "_" ++ unique_suffix()).

unique_suffix()->
  integer_to_list(erlang:unique_integer([positive])).

%%=================================================================
%%  Startup
%%=================================================================
%%-----------------------------------------------------------------
%%  A peer node with the code paths of the build (the ebin of every
%%  dependency and the compiled test directories) and the cookie of
%%  the run. peer:start/1 returns once the node has booted, the
%%  ping proves the controller can talk to it. The node is recorded
%%  as the last one started
%%-----------------------------------------------------------------
start_peer(#{name := Name}, Cookie)->
  BuildPaths = [ P || P <- code:get_path(), string:find(P, "_build") =/= nomatch ],
  {ok, Peer, Node} = peer:start(#{
    name => node_name(Name),
    host => ?HOST,
    longnames => true,
    connection => standard_io,
    shutdown => 1000,
    args => ["-pa" | BuildPaths] ++ ["-setcookie", atom_to_list(Cookie)]
  }),
  pong = net_adm:ping(Node),
  add_node(Node, Peer),
  Node.

node_name(Name)->
  "elock_" ++ atom_to_list(Name) ++ "_" ++ os:getpid() ++ "_" ++ unique_suffix().

%%-----------------------------------------------------------------
%%  Every node connects to every other node and sees it connected
%%-----------------------------------------------------------------
connect_nodes(Nodes)->
  [ pong = net_adm:ping(Node) || Node <- Nodes ],
  [ connect_pair(From, To) || From <- Nodes, To <- Nodes, From =/= To ],
  ok.

connect_pair(From, To)->
  true = rpc(From, net_kernel, connect_node, [To]),
  wait_until(fun()-> lists:member(To, rpc(From, erlang, nodes, [connected])) end).

start_ecall(Nodes)->
  [ {ok, _} = rpc(Node, application, ensure_all_started, [ecall]) || Node <- Nodes ],
  ok.

%%-----------------------------------------------------------------
%%  The ecall connections are established from the peers (the
%%  controller does not run ecall) in both directions and verified:
%%  connection_info/1 must report the pair connected
%%-----------------------------------------------------------------
connect_ecall(Nodes)->
  [ connect_ecall_pair(From, To) || From <- Nodes, To <- Nodes, From =/= To ],
  ok.

connect_ecall_pair(From, To)->
  ok = rpc(From, ecall_connection, connect, [To]),
  wait_until(fun()-> ecall_connected(From, To) end).

ecall_connected(From, To)->
  case rpc(From, ecall_connection, connection_info, [To]) of
    {ok, #{status := connected}}-> true;
    Other-> {not_connected, From, To, Other}
  end.

%%=================================================================
%%  Shutdown
%%=================================================================
%%-----------------------------------------------------------------
%%  Stop the peer control process. After a kill it may be gone
%%  already (it exits when it loses the node), hence the catch and
%%  the wait for its death either way
%%-----------------------------------------------------------------
stop_peer(Peer)->
  MonRef = erlang:monitor(process, Peer),
  catch peer:stop(Peer),
  receive
    {'DOWN', MonRef, process, Peer, _Reason}->
      ok
  after ?DEADLINE->
    erlang:demonitor(MonRef, [flush]),
    erlang:error({peer_did_not_stop, Peer})
  end.

%%-----------------------------------------------------------------
%%  The node is gone for the controller and for every survivor, the
%%  survivors have dropped the ecall connection to it (ecall does
%%  that on the pg leave of the node) and keep the connections to
%%  each other
%%-----------------------------------------------------------------
wait_gone(Node, Survivors)->
  wait_until(
    fun()->
      case lists:member(Node, erlang:nodes()) orelse net_adm:ping(Node) =:= pong of
        true->
          {still_alive, Node};
        false->
          gone_for(Node, Survivors)
      end
    end
  ),
  [ ok = wait_until(fun()-> ecall_connected(From, To) end)
    || From <- Survivors, To <- Survivors, From =/= To ],
  ok.

gone_for(Node, Survivors)->
  lists:foldl(
    fun
      (Survivor, true)->
        case lists:member(Node, rpc(Survivor, erlang, nodes, [])) of
          true->
            {still_connected, Survivor, Node};
          false->
            case rpc(Survivor, ecall_connection, connection_info, [Node]) of
              {error, not_connected}-> true;
              Info-> {ecall_still_connected, Survivor, Node, Info}
            end
        end;
      (_Survivor, Problem)->
        Problem
    end,
    true,
    Survivors
  ).

%%=================================================================
%%  State
%%=================================================================
state()->
  persistent_term:get(?STATE_KEY, #{nodes => []}).

put_state(State)->
  persistent_term:put(?STATE_KEY, State).

add_node(Node, Peer)->
  #{nodes := Nodes} = State = state(),
  put_state(State#{nodes => Nodes ++ [{Node, Peer}]}).

%%-----------------------------------------------------------------
%%  The peer of a started node; the node leaves the state
%%-----------------------------------------------------------------
take_node(Node)->
  #{nodes := Nodes} = State = state(),
  case lists:keytake(Node, 1, Nodes) of
    {value, {Node, Peer}, Rest}->
      put_state(State#{nodes => Rest}),
      Peer;
    false->
      erlang:error({unknown_node, Node, [ N || {N, _} <- Nodes ]})
  end.

%%=================================================================
%%  Utilities
%%=================================================================
rpc(Node, Module, Function, Args)->
  case rpc:call(Node, Module, Function, Args, ?RPC_TIMEOUT) of
    {badrpc, Reason}->
      erlang:error({badrpc, Node, {Module, Function, Args}, Reason});
    Result->
      Result
  end.

wait_until(Fun)->
  elock_test_utils:wait_until(Fun, ?DEADLINE).
