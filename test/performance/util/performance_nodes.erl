%%=================================================================
%%  The nodes of the performance tests. Ported from
%%  ecall/test/performance/util/distributed_tests_utils.erl (the
%%  name distributed_tests_utils is taken by the functional tests,
%%  both are compiled into the test profile).
%%
%%  Every node is a peer node in a docker container of the image
%%  ?IMAGE: a local node in a container on this host, a remote node
%%  in a container on its host, started over ssh (sshpass with the
%%  password of the config). The image is built from the project
%%  directory with test/performance/Dockerfile, so it holds the
%%  beams of the current build. Dockerfile.dockerignore next to it
%%  keeps out of the build context what the nodes never read: the
%%  node_modules and dist of performance_report, _build/test/logs
%%  and .git. With ?PREBUILT_IMAGE_ENV=true the image found in the
%%  local docker store is used as it is. A remote host gets the
%%  local image unless it has the same image already.
%%
%%  start_nodes/1 makes the controller (the ct node) distributed on
%%  demand with a unique cookie per run, starts the nodes, connects
%%  them pairwise, starts ecall on every one of them and establishes
%%  the ecall connections in both directions. The controller listens
%%  on ?DIST_PORT_BASE, the nodes on ?DIST_PORT_BASE + their index
%%  in the sorted order of the role names. The started nodes live in
%%  a persistent_term until stop_nodes/1
%%=================================================================
-module(performance_nodes).

%% API
-export([
  start_nodes/1,
  stop_nodes/1,
  wait_until/1
]).

-define(STATE_KEY, {?MODULE, state}).
-define(IMAGE, "elock-performance:otp27").
-define(BASE_IMAGE, "erlang:27.2.2").
-define(PREBUILT_IMAGE_ENV, "ELOCK_PERFORMANCE_PREBUILT_IMAGE").
-define(COMMAND_TIMEOUT, 30000).
-define(BUILD_TIMEOUT, 300000).
-define(REMOTE_TIMEOUT, 300000).
-define(RPC_TIMEOUT, 30000).
-define(POLL, 100).
-define(PROCESS_LIMIT, 134217727).
-define(PORT_LIMIT, 1048576).
-define(ETS_LIMIT, 262144).
-define(LOCAL_NODE_HOST, "127.0.0.1").
-define(DIST_PORT_BASE, 4443).
-define(SSH_PORT, "22").

%%=================================================================
%%  API
%%=================================================================
%%-----------------------------------------------------------------
%%  Start the nodes of the config (#{Name => local | Remote}),
%%  connect them pairwise, start ecall on each and connect ecall
%%  both ways. The result is #{Name => Node}
%%-----------------------------------------------------------------
start_nodes(Configs)->
  ensure_local_image(project_dir()),
  EnvSettings = ct:get_config(env_settings),
  Cookie = ensure_controller(Configs),
  Nodes = #{
    Name => start_node(Name, Location, ?DIST_PORT_BASE + Index, Cookie, EnvSettings)
    || {Index, {Name, Location}} <- lists:enumerate(lists:sort(maps:to_list(Configs)))
  },
  NodeList = maps:values(Nodes),
  ok = connect_nodes(NodeList),
  ok = start_ecall(NodeList, EnvSettings),
  ok = connect_ecall(NodeList),
  Nodes.

stop_nodes(Nodes)->
  [ stop_node(Node) || Node <- Nodes ],
  remove_password_files().

%%-----------------------------------------------------------------
%%  Poll Predicate every ?POLL ms until it returns true, exit at
%%  the deadline naming what it has captured
%%-----------------------------------------------------------------
wait_until(Predicate)->
  wait_until(Predicate, erlang:monotonic_time(millisecond) + ?RPC_TIMEOUT).

wait_until(Predicate, Deadline)->
  case Predicate() of
    true->
      ok;
    false->
      case erlang:monotonic_time(millisecond) >= Deadline of
        false->
          timer:sleep(?POLL),
          wait_until(Predicate, Deadline);
        true->
          exit({wait_timeout, erlang:fun_info(Predicate, env)})
      end
  end.

%%=================================================================
%%  Startup
%%=================================================================
start_node(Name, local, DistPort, Cookie, EnvSettings)->
  Container = unique_name(atom_to_list(Name)),
  Node = list_to_atom(atom_to_list(Name) ++ "@" ++ ?LOCAL_NODE_HOST),
  Exec = {os:find_executable("docker"), docker_run_args(Container)},
  start_peer(Node, Exec, Container, local, DistPort, Cookie, EnvSettings);
start_node(Name, #{node := Node} = Location, DistPort, Cookie, EnvSettings)->
  ensure_remote_image(Location),
  Container = unique_name(atom_to_list(Name)),
  Exec = remote_exec(Location, ["docker" | docker_run_args(Container)]),
  start_peer(list_to_atom(Node), Exec, Container, Location, DistPort, Cookie, EnvSettings).

start_peer(Node, Exec, Container, Location, DistPort, Cookie, EnvSettings)->
  {ok, Peer, Node} = peer:start(#{
    name => Node,
    longnames => true,
    connection => standard_io,
    shutdown => 1000,
    exec => Exec,
    args => erl_args(Cookie, DistPort, EnvSettings)
  }),
  put_node(Node, #{peer => Peer, container => Container, location => Location}),
  Node.

erl_args(Cookie, DistPort, EnvSettings)->
  BusyKiB = integer_to_list(maps:get(distribution_busy_limit_kib, EnvSettings)),
  ["-pa" | container_code_paths()] ++ [
    "-setcookie", Cookie,
    "+zdbbl", BusyKiB,
    "+P", integer_to_list(?PROCESS_LIMIT),
    "+Q", integer_to_list(?PORT_LIMIT),
    "+e", integer_to_list(?ETS_LIMIT),
    "-kernel",
    "inet_dist_listen_min", integer_to_list(DistPort),
    "inet_dist_listen_max", integer_to_list(DistPort)
  ].

%%-----------------------------------------------------------------
%%  The beams of the build in the image. ecall is taken from the
%%  default profile: _build/test/lib/ecall is a symlink to it with
%%  an absolute path of this host, dangling in the container. The
%%  util modules are compiled next to the suites
%%-----------------------------------------------------------------
container_code_paths()->
  [
    "/opt/elock/_build/test/lib/elock/ebin",
    "/opt/elock/_build/default/lib/ecall/ebin",
    "/opt/elock/_build/test/lib/elock/test/performance"
  ].

docker_run_args(Container)->
  [
    "run", "--rm",
    "--name", Container,
    "--network", "host",
    "--ulimit", "nofile=1048576:1048576",
    "-i",
    ?IMAGE
  ].

%%=================================================================
%%  Readiness
%%=================================================================
connect_nodes(Nodes)->
  [ pong = net_adm:ping(Node) || Node <- Nodes ],
  [ true = rpc:call(From, net_kernel, connect_node, [To], ?RPC_TIMEOUT) || From <- Nodes, To <- Nodes, From =/= To ],
  ok.

start_ecall(Nodes, EnvSettings)->
  BatchSize = maps:get(ecall_batch_size, EnvSettings),
  [ begin
      ok = rpc:call(Node, application, set_env, [ecall, batch_size, BatchSize], ?RPC_TIMEOUT),
      {ok, _Started} = rpc:call(Node, application, ensure_all_started, [ecall], ?RPC_TIMEOUT)
    end || Node <- Nodes ],
  ok.

%%-----------------------------------------------------------------
%%  The ecall connections are established from the nodes (the
%%  controller does not run ecall) in both directions. A connection
%%  comes up after connect/1 has returned
%%-----------------------------------------------------------------
connect_ecall(Nodes)->
  [ connect_ecall_pair(From, To) || From <- Nodes, To <- Nodes, From =/= To ],
  ok.

connect_ecall_pair(From, To)->
  ok = rpc:call(From, ecall_connection, connect, [To], ?RPC_TIMEOUT),
  ok = wait_until(
    fun()->
      case rpc:call(From, ecall_connection, connection_info, [To], ?RPC_TIMEOUT) of
        {ok, #{status := connected}}-> true;
        _NotYet-> false
      end
    end
  ).

%%=================================================================
%%  Shutdown
%%=================================================================
%%-----------------------------------------------------------------
%%  The container goes with its node (docker run --rm). A node that
%%  outlives the shutdown timeout of the peer is left by
%%  peer:stop/1 with its container: docker rm -f removes it. For a
%%  container that is gone already the command fails, ignored
%%-----------------------------------------------------------------
stop_node(Node)->
  #{nodes := Nodes} = State = state(),
  {#{peer := Peer, container := Container, location := Location}, Rest} = maps:take(Node, Nodes),
  stop_peer(Peer),
  remove_container(Location, Container),
  put_state(State#{nodes => Rest}).

stop_peer(Peer)->
  try peer:stop(Peer)
  catch
    exit:noproc->
      % The peer has stopped with its node: the node has died during the run
      ok
  end.

remove_container(local, Container)->
  _ = run("docker", ["rm", "-f", Container], ?COMMAND_TIMEOUT),
  ok;
remove_container(Location, Container)->
  _ = remote_run(Location, ["docker", "rm", "-f", Container], ?COMMAND_TIMEOUT),
  ok.

%%=================================================================
%%  State
%%=================================================================
state()->
  persistent_term:get(?STATE_KEY, #{nodes => #{}, password_files => #{}}).

put_state(State)->
  persistent_term:put(?STATE_KEY, State).

put_node(Node, NodeState)->
  #{nodes := Nodes} = State = state(),
  put_state(State#{nodes => Nodes#{Node => NodeState}}).

%%=================================================================
%%  Docker and remote commands
%%=================================================================
ensure_local_image(ProjectDir)->
  case os:getenv(?PREBUILT_IMAGE_ENV) of
    "true"->
      ct:pal("Using prebuilt performance image ~s", [?IMAGE]),
      _ImageId = local_image_id(),
      ok;
    _BuildLocally->
      build_local_image(ProjectDir)
  end.

build_local_image(ProjectDir)->
  ok = ensure_base_image(),
  Dockerfile = filename:join([ProjectDir, "test", "performance", "Dockerfile"]),
  ct:pal("Rebuilding performance image ~s from current source", [?IMAGE]),
  _Output = command_ok(
    "docker build",
    "docker",
    [
      "build",
      "--file", Dockerfile,
      "--build-arg", "BASE_IMAGE=" ++ ?BASE_IMAGE,
      "--tag", ?IMAGE,
      ProjectDir
    ],
    ?BUILD_TIMEOUT
  ),
  ok.

ensure_base_image()->
  case run("docker", ["image", "inspect", "--format={{.Id}}", ?BASE_IMAGE], ?COMMAND_TIMEOUT) of
    {0, _ImageId}->
      ok;
    _Missing->
      ct:fail({base_image_missing, ?BASE_IMAGE,
        "the base image is not in the local Docker image store and the "
        "performance hosts have no registry access; load it there first"})
  end.

ensure_remote_image(#{host := Host} = Location)->
  LocalImageId = local_image_id(),
  case remote_image_id(Location) of
    {ok, LocalImageId}->
      ct:pal("Reusing performance image ~s on ~s", [?IMAGE, Host]);
    _Other->
      transfer_image(Location)
  end.

local_image_id()->
  Output = command_ok(
    "inspect local docker image",
    "docker",
    ["image", "inspect", "--format={{.Id}}", ?IMAGE],
    ?COMMAND_TIMEOUT
  ),
  string:trim(binary_to_list(Output)).

remote_image_id(Location)->
  case remote_run(Location, ["docker", "image", "inspect", "--format={{.Id}}", ?IMAGE], ?COMMAND_TIMEOUT) of
    {0, Output}->
      {ok, string:trim(binary_to_list(Output))};
    _NotThere->
      not_found
  end.

transfer_image(#{host := Host} = Location)->
  ct:pal("Transferring performance image ~s to ~s", [?IMAGE, Host]),
  RemoteLoad = remote_command_string(Location, ["docker", "load"]),
  Command = "docker save " ++ shell_quote(?IMAGE) ++ " | gzip -1 | " ++ RemoteLoad,
  _Output = command_ok("remote docker load", "sh", ["-c", Command], ?REMOTE_TIMEOUT),
  ok.

remote_run(Location, RemoteArgs, Timeout)->
  {Exec, Args} = remote_exec_parts(Location, RemoteArgs),
  run(Exec, Args, Timeout).

remote_exec(Location, RemoteArgs)->
  {Exec, Args} = remote_exec_parts(Location, RemoteArgs),
  {os:find_executable(Exec), Args}.

remote_exec_parts(Location, RemoteArgs)->
  {"sshpass", ["-f", password_file(Location), os:find_executable("ssh") | ssh_args(Location) ++ RemoteArgs]}.

remote_command_string(Location, RemoteArgs)->
  {Exec, Args} = remote_exec_parts(Location, RemoteArgs),
  string:join([ shell_quote(Arg) || Arg <- [Exec | Args] ], " ").

ssh_args(#{user := User, host := Host})->
  [
    "-p", ?SSH_PORT,
    "-o", "StrictHostKeyChecking=accept-new",
    "-o", "ConnectTimeout=10",
    User ++ "@" ++ Host
  ].

%%=================================================================
%%  The controller
%%=================================================================
%%-----------------------------------------------------------------
%%  The ct node is not distributed (nonode@nohost) unless ct was
%%  started with a name: it becomes so on ?DIST_PORT_BASE, on the
%%  host the remote nodes see it at. The cookie is unique per run
%%-----------------------------------------------------------------
ensure_controller(Configs)->
  Cookie = "cookie_" ++ unique_suffix(),
  case node() of
    nonode@nohost->
      start_distribution(controller_name(Configs));
    _Distributed->
      ok
  end,
  true = erlang:set_cookie(node(), list_to_atom(Cookie)),
  Cookie.

%%-----------------------------------------------------------------
%%  net_kernel needs epmd, which a non-distributed VM does not
%%  start by itself: on a failure the epmd of this release is
%%  started as a daemon and the start is retried once
%%-----------------------------------------------------------------
start_distribution(Name)->
  ok = application:set_env(kernel, inet_dist_listen_min, ?DIST_PORT_BASE),
  ok = application:set_env(kernel, inet_dist_listen_max, ?DIST_PORT_BASE),
  case net_kernel:start([Name, longnames]) of
    {ok, _}->
      ok;
    {error, _NoEpmd}->
      Epmd = filename:join([code:root_dir(), "erts-" ++ erlang:system_info(version), "bin", "epmd"]),
      _ = os:cmd(Epmd ++ " -daemon"),
      {ok, _} = net_kernel:start([Name, longnames]),
      ok
  end.

controller_name(Configs)->
  list_to_atom("elock_performance_controller_" ++ os:getpid() ++ "@" ++ controller_host(Configs)).

controller_host(Configs)->
  case [ Location || Location <- maps:values(Configs), Location =/= local ] of
    []->
      ?LOCAL_NODE_HOST;
    [Location | _Rest]->
      {0, Output} = remote_run(Location, ["printenv", "SSH_CLIENT"], ?COMMAND_TIMEOUT),
      [Host | _] = string:tokens(string:trim(binary_to_list(Output)), " \t"),
      Host
  end.

%%=================================================================
%%  Command helpers
%%=================================================================
%%-----------------------------------------------------------------
%%  The password of a remote host in a file for sshpass -f, one
%%  file per host and user for the run
%%-----------------------------------------------------------------
password_file(#{host := Host, user := User, password := Password})->
  #{password_files := Files} = State = state(),
  case Files of
    #{{Host, User} := Path}->
      Path;
    _->
      Path = filename:join("/tmp", unique_name("sshpass")),
      ok = file:write_file(Path, Password),
      ok = file:change_mode(Path, 8#600),
      put_state(State#{password_files => Files#{{Host, User} => Path}}),
      Path
  end.

remove_password_files()->
  #{password_files := Files} = State = state(),
  [ ok = file:delete(Path) || Path <- maps:values(Files) ],
  put_state(State#{password_files => #{}}),
  ok.

command_ok(Label, Exec, Args, Timeout)->
  case run(Exec, Args, Timeout) of
    {0, Output}->
      Output;
    {Status, Output} when is_integer(Status)->
      ct:fail({command_failed, Label, Status, lists:sublist(binary_to_list(Output), 4000)});
    {error, Reason}->
      ct:fail({command_failed, Label, Reason})
  end.

run(Exec, Args, Timeout)->
  Port = open_port(
    {spawn_executable, os:find_executable(Exec)},
    [{args, Args}, binary, exit_status, stderr_to_stdout]
  ),
  collect_port(Port, Timeout, []).

collect_port(Port, Timeout, Acc)->
  receive
    {Port, {data, Data}}->
      collect_port(Port, Timeout, [Data | Acc]);
    {Port, {exit_status, Status}}->
      {Status, iolist_to_binary(lists:reverse(Acc))}
  after Timeout->
    try port_close(Port)
    catch
      error:badarg->
        % The command has exited after the timeout had fired
        ok
    end,
    {error, timeout}
  end.

%%=================================================================
%%  Paths and names
%%=================================================================
%%-----------------------------------------------------------------
%%  The project directory: the application directory of the test
%%  profile is <project>/_build/test/lib/elock
%%-----------------------------------------------------------------
project_dir()->
  filename:dirname(filename:dirname(filename:dirname(filename:dirname(code:lib_dir(elock))))).

unique_name(Prefix)->
  "elock-performance-" ++ Prefix ++ "-" ++ os:getpid() ++ "-" ++ unique_suffix().

unique_suffix()->
  integer_to_list(erlang:unique_integer([monotonic, positive]), 36).

shell_quote(Value)->
  "'" ++ shell_quote_chars(Value) ++ "'".

shell_quote_chars([])->
  [];
shell_quote_chars([$' | Rest])->
  "'\\''" ++ shell_quote_chars(Rest);
shell_quote_chars([Char | Rest])->
  [Char | shell_quote_chars(Rest)].
