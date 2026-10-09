%% Seeded lock-and-delay transactions.
-module(performance_transaction_SUITE).

-include_lib("common_test/include/ct.hrl").

%% Common Test API
-export([
  all/0,
  init_per_suite/1,
  end_per_suite/1,
  init_per_testcase/2,
  end_per_testcase/2
]).

%% Test cases
-export([
  transactions_test/1
]).

-define(TAG, ?MODULE).
-define(SCOPE, performance_scope).
-define(TABLE, performance_table).
-define(LOCK_US, '$performance_lock_us$').
-define(RUNS, '$performance_runs$').
-define(RPC_TIMEOUT, 30000).
-define(TABLE_TIMEOUT, 10000).

% The nodes, unless the nodes config is given
-define(NODES, #{
  node1 => local,
  node2 => local,
  node3 => local
}).

% The measurements of a transaction, summed per client, node and point
-record(sums, {
  transaction_us = 0,
  lock_us = 0,
  restarts = 0
}).

%%=================================================================
%%  Common Test API
%%=================================================================
all()->
  [transactions_test].

%%-----------------------------------------------------------------
%%  The nodes of the nodes config (#{Name => Node}), started in
%%  docker, connected, ecall running and connected both ways
%%  between every pair
%%-----------------------------------------------------------------
init_per_suite(Config)->
  Locations = ct:get_config(nodes, ?NODES),
  [
    {nodes, performance_nodes:start_nodes(Locations)},
    {locations, #{ Name => location(Location) || Name := Location <- Locations }},
    {performance, performance()}
    | Config
  ].

end_per_suite(Config)->
  performance_nodes:stop_nodes(node_list(Config)).

%%-----------------------------------------------------------------
%%  What the configured paths need, up for the whole case
%%-----------------------------------------------------------------
init_per_testcase(transactions_test, Config)->
  #{paths := Paths} = ?config(performance, Config),
  lists:foldl(fun start_path/2, Config, Paths).

end_per_testcase(transactions_test, Config)->
  ok = performance_metrics:stopped(Config),
  #{paths := Paths} = ?config(performance, Config),
  [ ok = stop_path(Path, Config) || Path <- Paths ],
  ok.

%%-----------------------------------------------------------------
%%  elock: the application on every node (the graph process), then
%%  the scope on every node, linked to a holder that lives for the
%%  test case, ready on all the nodes.
%%  mnesia: a ram schema on every node, joined to the first one,
%%  the table with ram_copies on all the nodes.
%%  global: nothing
%%-----------------------------------------------------------------
start_path(elock, Config)->
  Nodes = node_list(Config),
  [ {ok, _Started} = rpc(Node, application, ensure_all_started, [elock]) || Node <- Nodes ],
  Holders = [ spawn(Node, fun scope_holder/0) || Node <- Nodes ],
  ok = performance_nodes:wait_until(fun()-> scope_ready(Nodes) end),
  [{holders, Holders} | Config];
start_path(mnesia, Config)->
  [First | Rest] = Nodes = node_list(Config),
  [ ok = rpc(Node, application, set_env, [mnesia, schema_location, ram]) || Node <- Nodes ],
  [ ok = rpc(Node, mnesia, start, []) || Node <- Nodes ],
  [ {ok, _Connected} = rpc(Node, mnesia, change_config, [extra_db_nodes, [First]]) || Node <- Rest ],
  {atomic, ok} = rpc(First, mnesia, create_table, [?TABLE, [{ram_copies, Nodes}]]),
  [ ok = rpc(Node, mnesia, wait_for_tables, [[?TABLE], ?TABLE_TIMEOUT]) || Node <- Nodes ],
  Config;
start_path(global, Config)->
  Config.

stop_path(elock, Config)->
  [ exit(Holder, kill) || Holder <- ?config(holders, Config) ],
  [ ok = rpc(Node, application, stop, [elock]) || Node <- node_list(Config) ],
  ok;
stop_path(mnesia, Config)->
  [ stopped = rpc(Node, mnesia, stop, []) || Node <- node_list(Config) ],
  ok;
stop_path(global, _Config)->
  ok.

%%=================================================================
%%  Test cases
%%=================================================================
transactions_test(Config)->
  run_points(Config).

%%=================================================================
%%  One fixed workload, selected paths run sequentially
%%=================================================================
run_points(Config)->
  _ = ct:timetrap(infinity),
  Performance = ?config(performance, Config),
  Locations = ?config(locations, Config),
  Points = points(Performance, Locations),
  Total = length(Points),
  [ok = run_point(Point, Index, Total, Config) || {Index, Point} <- lists:enumerate(Points)],
  ok.

points(#{paths := Paths, clients_per_node := Clients, transaction := Transaction,
    deadlocks := Deadlocks, global_max_locks := GlobalMaxLocks} = Performance, Locations)->
  Held = map_size(Locations) * Clients * performance_workload:size(Transaction),
  Skipped = [Path || Path <- Paths, not runs(Path, Transaction, Deadlocks, Held, GlobalMaxLocks)],
  case Skipped of
    []-> ok;
    _-> ct:pal("Skipped global: requires read = 0, deadlocks = false, and "
      "nodes * clients_per_node * (read + update + write) =< ~w (this workload: ~w)",
      [GlobalMaxLocks, Held])
  end,
  Workload = maps:without([paths, global_max_locks], Performance),
  [Workload#{path => Path, nodes => Locations, skipped_paths => Skipped}
    || Path <- Paths, not lists:member(Path, Skipped)].

runs(global, #{read := Read}, Deadlocks, Held, GlobalMaxLocks)->
  Read =:= 0 andalso Deadlocks =:= false andalso Held =< GlobalMaxLocks;
runs(_Path, _Transaction, _Deadlocks, _Held, _GlobalMaxLocks)->
  true.

run_point(#{clients_per_node := Clients, transactions_per_client := PerClient} = Point,
    Index, Total, Config)->
  ok = performance_metrics:running(Config, Point#{index => Index, total => Total}),
  Nodes = ?config(nodes, Config),
  Client = Point#{nodes => node_list(Config)},
  RunRef = make_ref(),
  Controller = self(),
  Runners = maps:from_list([
    start_runner(Name, Node, Controller, RunRef, Clients, Client#{node_index => NodeIndex})
    || {NodeIndex, {Name, Node}} <- lists:enumerate(lists:sort(maps:to_list(Nodes)))
  ]),
  ok = await_runners_ready(map_size(Runners), RunRef, Runners),
  [Runner ! {?TAG, RunRef, start} || Runner := _ <- Runners],
  StartedAt = erlang:monotonic_time(microsecond),
  {Sums, Metrics} = await_runners(RunRef, Runners, #sums{}, #{}),
  ElapsedUs = erlang:monotonic_time(microsecond) - StartedAt,
  Transactions = map_size(Nodes) * Clients * PerClient,
  Result = result(Point, ElapsedUs, Transactions, Sums, Metrics),
  performance_metrics:point(Config, Result).

start_runner(Name, Node, Controller, RunRef, Clients, Client)->
  {Runner, MonRef} = spawn_monitor(Node, fun()-> runner(Controller, RunRef, Clients, Client) end),
  {Runner, {Name, MonRef}}.

%%-----------------------------------------------------------------
%%  A runner exits normally only after it has reported, so every
%%  'DOWN' of a runner that has not reported yet is a failure
%%-----------------------------------------------------------------
await_runners_ready(0, _RunRef, _Runners)->
  ok;
await_runners_ready(Count, RunRef, Runners)->
  receive
    {?TAG, RunRef, ready}->
      await_runners_ready(Count - 1, RunRef, Runners);
    {'DOWN', _MonRef, process, Runner, Reason}->
      runner_failed(Runner, Reason, Runners)
  end.

await_runners(_RunRef, Runners, Sums, Metrics) when map_size(Runners) =:= 0->
  {Sums, Metrics};
await_runners(RunRef, Runners, Sums, Metrics)->
  receive
    {?TAG, RunRef, completed, Runner, RunnerSums, NodeMetrics}->
      {{Name, MonRef}, Rest} = maps:take(Runner, Runners),
      erlang:demonitor(MonRef, [flush]),
      await_runners(RunRef, Rest, add(Sums, RunnerSums), Metrics#{Name => NodeMetrics});
    {'DOWN', _MonRef, process, Runner, Reason}->
      runner_failed(Runner, Reason, Runners)
  end.

%%-----------------------------------------------------------------
%%  A runner has failed: a client of it has failed and taken it
%%  down, or its node is gone. The other runners are killed, their
%%  clients and collectors are linked to them and go with them
%%-----------------------------------------------------------------
runner_failed(Runner, Reason, Runners)->
  {Name, _MonRef} = maps:get(Runner, Runners),
  [ exit(Pid, kill) || Pid := _ <- Runners ],
  exit({runner_failed, Name, Reason}).

result(#{transaction := Transaction} = Point, ElapsedUs, Transactions, #sums{
  transaction_us = TransactionUs, lock_us = LockUs, restarts = Restarts
}, Metrics)->
  Locks = performance_workload:size(Transaction),
  Point#{
    elapsed_ms => ElapsedUs div 1000,
    transactions => Transactions,
    transactions_per_second => Transactions * 1000000 / ElapsedUs,
    locks => Transactions * Locks,
    locks_per_second => Transactions * Locks * 1000000 / ElapsedUs,
    lock_time_percent => 100 * LockUs / TransactionUs,
    restarts => Restarts,
    metrics => Metrics
  }.

%%=================================================================
%%  The point runner on a node
%%=================================================================
runner(Controller, RunRef, Count, #{nodes := Nodes} = Client)->
  Runner = self(),
  Clients = [start_client(Runner, RunRef, Client#{client_index => I}) || I <- lists:seq(1, Count)],
  ok = await_clients_ready(Count, RunRef),
  Controller ! {?TAG, RunRef, ready},
  receive
    {?TAG, RunRef, start}->
      ok
  end,
  Collector = performance_metrics:start(Nodes -- [node()]),
  lists:foreach(fun(Pid)-> Pid ! {?TAG, RunRef, start} end, Clients),
  Sums = await_clients(Count, RunRef, #sums{}),
  Controller ! {?TAG, RunRef, completed, Runner, Sums, performance_metrics:finish(Collector)}.

start_client(Runner, RunRef, Client)->
  spawn_link(fun()-> client(Runner, RunRef, Client) end).

await_clients_ready(0, _RunRef)->
  ok;
await_clients_ready(Count, RunRef)->
  receive
    {?TAG, RunRef, ready}->
      await_clients_ready(Count - 1, RunRef)
  end.

%%-----------------------------------------------------------------
%%  A client reports its sums as its last act. A client that fails
%%  takes the runner down through the link
%%-----------------------------------------------------------------
await_clients(0, _RunRef, Sums)->
  Sums;
await_clients(Count, RunRef, Sums)->
  receive
    {?TAG, RunRef, completed, ClientSums}->
      await_clients(Count - 1, RunRef, add(Sums, ClientSums))
  end.

add(
  #sums{transaction_us = TransactionUs1, lock_us = LockUs1, restarts = Restarts1},
  #sums{transaction_us = TransactionUs2, lock_us = LockUs2, restarts = Restarts2}
)->
  #sums{
    transaction_us = TransactionUs1 + TransactionUs2,
    lock_us = LockUs1 + LockUs2,
    restarts = Restarts1 + Restarts2
  }.

%%=================================================================
%%  The client
%%=================================================================
client(Runner, RunRef, #{transactions_per_client := Count, seed := Seed,
    node_index := NodeIndex, client_index := ClientIndex} = Client)->
  State = rand:seed_s(exsss, {Seed, NodeIndex, ClientIndex}),
  Runner ! {?TAG, RunRef, ready},
  receive
    {?TAG, RunRef, start}->
      Runner ! {?TAG, RunRef, completed, transactions(Count, Client, State, #sums{})}
  end.

transactions(0, _Client, _State, Sums)->
  Sums;
transactions(Count, #{think_ms := ThinkMs} = Client, State0, Sums)->
  {Plan, State1} = performance_workload:plan(Client, State0),
  Measured = transaction(Client, Plan),
  %% The transaction has completed and released its locks. Pause only when
  %% another plan follows, outside transaction measurements but inside the run.
  case Count > 1 of
    true-> delay(ThinkMs);
    false-> ok
  end,
  transactions(Count - 1, Client, State1, add(Sums, Measured)).

%% The clock and accumulators cover all attempts of the same generated plan.
transaction(#{path := Path} = Client, Plan)->
  init_measurements(),
  StartedAt = erlang:monotonic_time(microsecond),
  case Path of
    mnesia-> mnesia_retry(fun()-> mnesia_operations(Plan, Client), commit(Client) end);
    _-> attempts(Client, Plan)
  end,
  #sums{transaction_us = erlang:monotonic_time(microsecond) - StartedAt,
    lock_us = erase(?LOCK_US), restarts = erase(?RUNS) - 1}.

init_measurements()->
  put(?LOCK_US, 0),
  put(?RUNS, 0),
  ok.

%% after executes also for Mnesia's abort exits; process dictionary measurements
%% survive native callback reruns and returned-abort harness retries.
timed_lock(Fun)->
  StartedAt = erlang:monotonic_time(microsecond),
  try Fun()
  after put(?LOCK_US, get(?LOCK_US) + erlang:monotonic_time(microsecond) - StartedAt)
  end.

attempts(#{path := Path, restart_ms := RestartMs} = Client, Plan)->
  put(?RUNS, get(?RUNS) + 1),
  Attempt = case Path of elock-> []; global-> start_global_owner(maps:get(nodes, Client)) end,
  case operations(Plan, Client, Attempt) of
    {ok, Held}->
      commit(Client),
      release(Path, Held);
    {abort, Held}->
      cancel(Path, Held),
      delay(RestartMs),
      attempts(Client, Plan)
  end.

operations([], _Client, Held)->
  {ok, Held};
operations([{Key, Operation} | Rest], #{path := Path} = Client, Held)->
  case acquire(Path, Key, Operation, Client, Held) of
    {ok, Next}->
      read(Operation, Client),
      operations(Rest, Client, Next);
    abort-> {abort, Held}
  end.

acquire(elock, Key, Operation, #{nodes := Nodes, timeout := Timeout}, Refs)->
  {LockNodes, Options} = case Operation of
    read-> {[node()], #{is_shared => true, timeout => Timeout}};
    _-> {Nodes, #{timeout => Timeout}}
  end,
  case timed_lock(fun()-> elock:lock(?SCOPE, Key, LockNodes, Options) end) of
    {ok, Ref}-> {ok, [Ref | Refs]};
    {error, timeout}-> abort;
    {error, {deadlock, _}}-> abort
  end;
acquire(global, Key, _Operation, #{timeout := Timeout}, Owner)->
  case timed_lock(fun()-> global_acquire(Owner, Key, Timeout) end) of
    ok-> {ok, Owner};
    timeout-> abort
  end.

release(elock, Refs)-> lists:foreach(fun elock:unlock/1, Refs);
release(global, Owner)-> finish_global_owner(Owner).

cancel(elock, Refs)-> release(elock, Refs);
cancel(global, Owner)-> cancel_global_owner(Owner).

read(write, _Client)-> ok;
read(_Operation, #{read_ms := ReadMs})->
  delay(ReadMs).

commit(#{transaction := #{update := Update, write := Write}, write_ms := WriteMs})->
  delay((Update + Write) * WriteMs).

delay(0)-> ok;
delay(Milliseconds)-> timer:sleep(Milliseconds).

mnesia_retry(Fun)->
  Before = get(?RUNS),
  Result = mnesia:transaction(fun()->
    put(?RUNS, get(?RUNS) + 1),
    Fun()
  end),
  %% A returned abort before invoking the callback is still an attempt.
  case get(?RUNS) of Before-> put(?RUNS, Before + 1); _-> ok end,
  case Result of
    {atomic, _}-> ok;
    {aborted, _}-> mnesia_retry(Fun)
  end.

mnesia_operations([], _Client)-> ok;
mnesia_operations([{Key, Operation} | Rest], Client)->
  Kind = case Operation of read-> read; _-> write end,
  _ = timed_lock(fun()-> mnesia:lock({record, ?TABLE, Key}, Kind) end),
  read(Operation, Client),
  mnesia_operations(Rest, Client).

%% The process calling global must own the locks until normal unlock. A linked
%% watcher also kills it when its client exits normally, even during set_lock.
start_global_owner(Nodes)->
  Client = self(),
  spawn_monitor(fun()->
    Owner = self(),
    spawn_link(fun()-> watch_global_client(Client, Owner) end),
    global_owner(Client, Nodes, make_ref(), [])
  end).

watch_global_client(Client, Owner)->
  ClientRef = monitor(process, Client),
  OwnerRef = monitor(process, Owner),
  receive
    {'DOWN', ClientRef, process, Client, _}-> exit(client_down);
    {'DOWN', OwnerRef, process, Owner, _}-> ok
  end.

global_owner(Client, Nodes, Requester, Held)->
  receive
    {?TAG, acquire, Ref, Key}->
      true = global:set_lock({Key, Requester}, Nodes, infinity),
      Client ! {?TAG, self(), Ref, acquired},
      global_owner(Client, Nodes, Requester, [Key | Held]);
    {?TAG, finish, Ref}->
      [true = global:del_lock({Key, Requester}, Nodes) || Key <- Held],
      Client ! {?TAG, self(), Ref, released}
  end.

global_acquire({Owner, MonRef}, Key, Timeout)->
  Ref = make_ref(),
  Owner ! {?TAG, acquire, Ref, Key},
  Wait = case Timeout of undefined-> infinity; _-> Timeout end,
  receive
    {?TAG, Owner, Ref, acquired}-> ok;
    {'DOWN', MonRef, process, Owner, Reason}-> exit({global_owner_failed, Reason})
  after Wait-> timeout
  end.

finish_global_owner({Owner, MonRef})->
  Ref = make_ref(),
  Owner ! {?TAG, finish, Ref},
  receive
    {?TAG, Owner, Ref, released}->
      receive {'DOWN', MonRef, process, Owner, normal}-> ok end;
    {'DOWN', MonRef, process, Owner, Reason}-> exit({global_owner_failed, Reason})
  end.

cancel_global_owner({Owner, MonRef})->
  exit(Owner, kill),
  receive {'DOWN', MonRef, process, Owner, _}-> ok end,
  flush_global_replies(Owner).

flush_global_replies(Owner)->
  receive {?TAG, Owner, _Ref, _Reply}-> flush_global_replies(Owner)
  after 0-> ok
  end.

%%=================================================================
%%  Utilities
%%=================================================================
% Complete settings are supplied by the caller; no defaults or validation.
performance()->
  ct:get_config(performance).

node_list(Config)->
  lists:sort(maps:values(?config(nodes, Config))).

location(local)->
  <<"local">>;
location(#{user := User, host := Host})->
  iolist_to_binary([User, $@, Host]).

scope_holder()->
  {ok, _} = elock:start_link(?SCOPE),
  timer:sleep(infinity).

%%-----------------------------------------------------------------
%%  Every node sees the scope ready on all the nodes
%%-----------------------------------------------------------------
scope_ready(Nodes)->
  lists:all(fun(Node)-> lists:sort(rpc(Node, elock, ready_nodes, [?SCOPE])) =:= Nodes end, Nodes).

rpc(Node, Module, Function, Args)->
  rpc:call(Node, Module, Function, Args, ?RPC_TIMEOUT).
