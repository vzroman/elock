%%=================================================================
%%  Database transactions on elock, mnesia and global.
%%
%%  A client imitates database transactions in a loop: 1) it
%%  acquires the locks of the transaction one after another and
%%  waits read_cost after every one of them, the imitated read under
%%  the lock just taken, 2) waits write_ms, the imitated write to the
%%  database (timer:sleep/1), 3) releases the locks;
%%  transactions_per_client times. clients_per_node clients run on
%%  every node of the nodes config.
%%
%%  The locks of a transaction are built before its clock starts:
%%  - N (locks_per_transaction) terms. K of them are keys of the
%%    shared pool, the other N - K are private: a fresh reference
%%    each, in every transaction. K is N * P / 100 (P:
%%    intersect_percent) rounded at random so that the share is
%%    exact on average: with Q = N * P, K = Q div 100, plus 1 with
%%    the probability (Q rem 100) / 100. N = 1, P = 20: one
%%    transaction in five takes a key of the pool.
%%  - The shared pool is virtual, nothing is stored: its keys are
%%    {shared, I}, I = 1..S, and a transaction takes K distinct
%%    random ones. S is the number of all the clients (the nodes *
%%    clients_per_node) * N. All the clients together want
%%    clients * N * P / 100 keys of the pool at once, so at every
%%    point of the matrix a key is wanted by P / 100 clients on
%%    average: P is the only contention knob, the clients and the
%%    locks dimensions keep the contention per key constant.
%%  - A lock is exclusive with the probability E / 100 (E:
%%    exclusive_percent), shared otherwise. A shared lock is taken
%%    on the local node only, an exclusive one on all the
%%    participating nodes.
%%  - The order follows the deadlocks setting of the run. false:
%%    every path takes the locks in the sorted order of the terms,
%%    no deadlock can come from the order of the locks. true: a
%%    random order, deadlocks happen, elock and mnesia restart.
%%
%%  The paths:
%%  - elock: elock:lock/4 for one lock after another. A request
%%    refused by wait-die ({error, abort}) has the locks
%%    taken in the attempt released and the transaction starts over
%%    with the same locks in the same order and a fresh lock context:
%%    a restart. Wait-die can refuse a request in either lock order.
%%    The caller retries immediately, with no added backoff.
%%  - mnesia: an imitation without writes. A transaction is
%%    mnesia:transaction/1 of a fun that takes the locks with
%%    mnesia:lock/2 on the records of a table with ram_copies on all
%%    the nodes - read for a shared lock (taken on the local
%%    replica), write for an exclusive one (on all the replicas) -
%%    and then waits write_ms. Mnesia restarts the fun by itself,
%%    every extra run of it is a restart.
%%  - global: global:set_lock/3 on all the nodes without a retry
%%    limit, released with global:del_lock/2. global has no shared
%%    locks and does not detect deadlocks, a random order would hang
%%    it: it runs only at E = 100 and only in a run with deadlocks =
%%    false. Its retries are internal, no restarts are counted. On
%%    every lock and unlock global copies the map of all the lock
%%    monitors of the node, so its cost per lock grows with the
%%    locks held at once: it runs a point only if the locks its
%%    clients hold at once (the nodes * clients_per_node * N) are
%%    not above global_max_locks.
%%
%%  The points come from the performance config (performance.config
%%  merged over ?DEFAULTS). The nodes, transactions_per_client,
%%  write_ms, read_cost and deadlocks are fixed per run, the lists nest from
%%  the outermost to the innermost: clients_per_node,
%%  locks_per_transaction, intersect_percents, exclusive_percents,
%%  paths. So at every (clients, locks, intersect, exclusive) the
%%  configured paths run back to back, global only in a run with
%%  deadlocks = false, where E = 100 and the point is within
%%  global_max_locks. The single test case, transactions_test, runs
%%  them all: its init starts what the configured paths need - the
%%  elock scope on every node, ready on all of them, for elock;
%%  mnesia with a ram schema and the table on all the nodes for
%%  mnesia; nothing for global - and both stay up for the whole
%%  case.
%%
%%  A point runs one path. The controller (the ct node) spawns a
%%  point runner on every node, a runner spawns its clients and
%%  reports ready when all of them are. When all the runners are
%%  ready the controller starts them and its clock. Every runner
%%  opens the metrics window of its node (performance_metrics),
%%  starts its clients, waits for every one of them to complete,
%%  closes the window and reports the sums of its node. The clock
%%  stops when the last runner has reported. A client is linked to
%%  its runner: a client that fails takes its runner down, then the
%%  controller kills the other runners (their clients and collectors
%%  go with them) and fails the point.
%%
%%  A client measures every transaction: the transaction time from
%%  the start of the acquisition (restarts included) to the end of
%%  the release (for mnesia: around mnesia:transaction/1), the write
%%  time, the measured sleep of the committed attempt, and its read
%%  time, the measured sleeps of the reads of the committed attempt.
%%  The reads of an attempt thrown away by a restart are lost with
%%  it: they are the price of the restart.
%%
%%  Before a point starts the controller writes the marker of the
%%  running point for the performance report (its inputs, its index
%%  among the points of the run and their total, see
%%  performance_metrics:running/2); the end of the test case deletes
%%  it.
%%
%%  The result of a point, logged and written as JSON (see
%%  performance_metrics:point/2):
%%  - the inputs: path, nodes, clients_per_node,
%%    transactions_per_client, locks_per_transaction,
%%    exclusive_percent, intersect_percent, deadlocks, write_ms,
%%    read_cost; elapsed_ms;
%%  - transactions and transactions_per_second, locks
%%    (transactions * N) and locks_per_second, over the elapsed
%%    time;
%%  - lock_time_percent, the share of the locks in the transaction
%%    time: 100 * (transaction time - write time - read time) /
%%    transaction time, summed over all the transactions. The reads
%%    of the thrown away attempts stay in it;
%%  - restarts (elock and mnesia);
%%  - metrics, per node by role name: the maximum memory, the
%%    scheduler utilization, the maximum run queue and the octets
%%    sent over the distribution to the other participating nodes.
%%
%%  With trace => true of the performance config every point is
%%  traced (performance_trace): the trace points of elock, of the
%%  mnesia copies in test/performance/mnesia and of the clients below
%%  (?TRACE, the steps of a transaction) write the events of every
%%  node, the controller saves them next to performance_data and
%%  writes the report of the point. Lock-level details require the
%%  production trace points; when these are absent, the report says
%%  they are unavailable and the regular point metrics are retained.
%%  trace is true or the limit of the events
%%  a node keeps: at the limit the trace stops and the report covers
%%  the point up to there (see performance_trace for the memory it
%%  takes).
%%=================================================================
-module(performance_transaction_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("elock/include/elock_trace.hrl").

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
-define(RUNS, '$performance_runs$').
-define(RPC_TIMEOUT, 30000).
-define(TABLE_TIMEOUT, 10000).

% The nodes, unless the nodes config is given
-define(NODES, #{
  node1 => local,
  node2 => local,
  node3 => local
}).

% The points, overridden by the performance map of performance.config.
% transactions_per_client, write_ms, read_cost (ms of the imitated read
% under every lock just taken, 0: none) and deadlocks (false: the locks
% are taken in the sorted order, true: in a random order) are fixed per run,
% the lists nest from the outermost to the innermost in this order.
% global_max_locks: global skips the points with more locks held at
% once (the nodes * clients_per_node * locks_per_transaction).
% trace: false, true or the limit of the events per node - the
% points are traced and reported step by step (see performance_trace)
-define(DEFAULTS, #{
  transactions_per_client => 1000,
  write_ms => 10,
  read_cost => 0,
  deadlocks => false,
  clients_per_node => [1000, 10000, 100000],
  locks_per_transaction => [1, 10, 100, 1000],
  intersect_percents => [0, 20, 50, 100],
  exclusive_percents => [0, 20, 50, 100],
  paths => [elock, mnesia, global],
  global_max_locks => 1000,
  trace => false
}).

% What a client runs
-record(client, {
  path,
  nodes,
  locks,
  exclusive,
  intersect,
  deadlocks,
  pool,
  write_ms,
  read_cost,
  transactions
}).

% The measurements of a transaction, summed per client, node and point
-record(sums, {
  transaction_us = 0,
  write_us = 0,
  read_us = 0,
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
%%  elock: the application on every node, then
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
%%  Test matrix
%%=================================================================
%%-----------------------------------------------------------------
%%  From the outermost to the innermost: clients, locks, intersect,
%%  exclusive, path. Held is the locks all the clients of a point
%%  hold at once: the size of its shared pool and what
%%  global_max_locks limits. The points are listed first: a point
%%  runs knowing its index and their total
%%-----------------------------------------------------------------
run_points(Config)->
  _ = ct:timetrap(infinity),
  #{
    transactions_per_client := Transactions,
    write_ms := WriteMs,
    read_cost := ReadCost,
    deadlocks := Deadlocks,
    clients_per_node := ClientCounts,
    locks_per_transaction := LockCounts,
    intersect_percents := IntersectPercents,
    exclusive_percents := ExclusivePercents,
    paths := Paths,
    global_max_locks := GlobalMaxLocks
  } = ?config(performance, Config),
  Locations = ?config(locations, Config),
  NodeCount = map_size(Locations),
  log_global(Paths, ExclusivePercents, Deadlocks, GlobalMaxLocks),
  Points = [
    {#{
      path => Path,
      nodes => Locations,
      clients_per_node => Clients,
      transactions_per_client => Transactions,
      locks_per_transaction => Locks,
      exclusive_percent => Exclusive,
      intersect_percent => Intersect,
      deadlocks => Deadlocks,
      write_ms => WriteMs,
      read_cost => ReadCost
    }, Held}
    || Clients <- ClientCounts,
       Locks <- LockCounts,
       Held <- [NodeCount * Clients * Locks],
       Intersect <- IntersectPercents,
       Exclusive <- ExclusivePercents,
       Path <- Paths,
       runs(Path, Exclusive, Deadlocks, Held, GlobalMaxLocks) ],
  Total = length(Points),
  [ ok = run_point(Point, Held, Index, Total, Config) || {Index, {Point, Held}} <- lists:enumerate(Points) ],
  ok.

%%-----------------------------------------------------------------
%%  Does the path run the point? global has no shared locks, does
%%  not detect deadlocks and its cost per lock grows with the locks
%%  held at once: it runs only at E = 100, in a run with deadlocks =
%%  false and within global_max_locks
%%-----------------------------------------------------------------
runs(global, Exclusive, Deadlocks, Held, GlobalMaxLocks)->
  Exclusive =:= 100 andalso Deadlocks =:= false andalso Held =< GlobalMaxLocks;
runs(_Path, _Exclusive, _Deadlocks, _Held, _GlobalMaxLocks)->
  true.

log_global(Paths, ExclusivePercents, Deadlocks, GlobalMaxLocks)->
  case {lists:member(global, Paths), Deadlocks} of
    {true, false}->
      ct:pal(
        "global has no shared locks, it skips the points with exclusive_percent ~w; "
        "its cost per lock grows with the locks held at once, it skips the points with more than ~w of them "
        "(nodes * clients_per_node * locks_per_transaction)",
        [[ E || E <- ExclusivePercents, E =/= 100 ], GlobalMaxLocks]);
    {true, true}->
      ct:pal("global does not detect deadlocks, a random order would hang it: "
        "with deadlocks = true it runs no point of the run");
    {false, _Deadlocks}->
      ok
  end.

%%=================================================================
%%  The point on the controller
%%=================================================================
run_point(#{
  path := Path,
  clients_per_node := Clients,
  transactions_per_client := PerClient,
  locks_per_transaction := Locks,
  exclusive_percent := Exclusive,
  intersect_percent := Intersect,
  deadlocks := Deadlocks,
  write_ms := WriteMs,
  read_cost := ReadCost
} = Point, Held, Index, Total, Config)->
  ok = performance_metrics:running(Config, Point#{index => Index, total => Total}),
  Nodes = ?config(nodes, Config),
  Client = #client{
    path = Path,
    nodes = node_list(Config),
    locks = Locks,
    exclusive = Exclusive,
    intersect = Intersect,
    deadlocks = Deadlocks,
    % The size of the shared pool: the locks all the clients hold at once
    pool = Held,
    write_ms = WriteMs,
    read_cost = ReadCost,
    transactions = PerClient
  },
  RunRef = make_ref(),
  Controller = self(),
  Runners = maps:from_list([
    start_runner(Name, Node, Controller, RunRef, Clients, Client) || Name := Node <- Nodes
  ]),
  ok = await_runners_ready(map_size(Runners), RunRef, Runners),
  #{trace := Trace0} = ?config(performance, Config),
  % global is not traced: it writes none of the steps of a transaction
  % and the report has no model of it
  Trace = case Path of global-> false; _-> Trace0 end,
  ok = performance_trace:start(Trace, Nodes),
  [ Runner ! {?TAG, RunRef, start} || Runner := _ <- Runners ],
  StartedAt = erlang:monotonic_time(microsecond),
  {Sums, Metrics} = await_runners(RunRef, Runners, #sums{}, #{}),
  ElapsedUs = erlang:monotonic_time(microsecond) - StartedAt,
  Transactions = map_size(Nodes) * Clients * PerClient,
  Result = result(Point, ElapsedUs, Transactions, Sums, Metrics),
  ok = performance_metrics:point(Config, Result),
  performance_trace:finish(Trace, Nodes, Config, Result).

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

result(#{
  path := Path,
  locks_per_transaction := Locks
} = Point, ElapsedUs, Transactions, #sums{
  transaction_us = TransactionUs,
  write_us = WriteUs,
  read_us = ReadUs,
  restarts = Restarts
}, Metrics)->
  Result = Point#{
    elapsed_ms => ElapsedUs div 1000,
    transactions => Transactions,
    transactions_per_second => Transactions * 1000000 / ElapsedUs,
    locks => Transactions * Locks,
    locks_per_second => Transactions * Locks * 1000000 / ElapsedUs,
    lock_time_percent => 100 * (TransactionUs - WriteUs - ReadUs) / TransactionUs,
    metrics => Metrics
  },
  case Path of
    global->
      % The retries of global are internal
      Result;
    _->
      Result#{restarts => Restarts}
  end.

%%=================================================================
%%  The point runner on a node
%%=================================================================
runner(Controller, RunRef, Count, #client{nodes = Nodes} = Client)->
  Runner = self(),
  Clients = [ start_client(Runner, RunRef, Client) || _ <- lists:seq(1, Count) ],
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
  #sums{transaction_us = TransactionUs1, write_us = WriteUs1, read_us = ReadUs1, restarts = Restarts1},
  #sums{transaction_us = TransactionUs2, write_us = WriteUs2, read_us = ReadUs2, restarts = Restarts2}
)->
  #sums{
    transaction_us = TransactionUs1 + TransactionUs2,
    write_us = WriteUs1 + WriteUs2,
    read_us = ReadUs1 + ReadUs2,
    restarts = Restarts1 + Restarts2
  }.

%%=================================================================
%%  The client
%%=================================================================
client(Runner, RunRef, #client{transactions = Count} = Client)->
  Runner ! {?TAG, RunRef, ready},
  receive
    {?TAG, RunRef, start}->
      Runner ! {?TAG, RunRef, completed, transactions(Count, Client, #sums{})}
  end.

transactions(0, _Client, Sums)->
  Sums;
transactions(Count, Client, Sums)->
  transactions(Count - 1, Client, add(Sums, transaction(Client))).

%%-----------------------------------------------------------------
%%  A transaction of the path: its locks are built before the clock
%%  starts. Every lock taken is followed by its read. elock: an
%%  abort has the locks of the attempt released and starts the
%%  transaction over. mnesia: every run of the fun counts, the result
%%  is the reads and the write of the committed run
%%-----------------------------------------------------------------
transaction(#client{path = elock, write_ms = WriteMs, read_cost = ReadCost} = Client)->
  Locks = elock_locks(Client),
  StartedAt = erlang:monotonic_time(microsecond),
  ?TRACE(tx_begin, self(), elock),
  {Refs, ReadUs, Restarts} = elock_lock(Locks, Locks, [], 0, 0, ReadCost),
  ?TRACE(tx_locked, self(), Restarts),
  WriteUs = write(WriteMs),
  ?TRACE(tx_written, self(), []),
  lists:foreach(fun elock:unlock/1, Refs),
  ?TRACE(tx_end, self(), []),
  #sums{
    transaction_us = erlang:monotonic_time(microsecond) - StartedAt,
    write_us = WriteUs,
    read_us = ReadUs,
    restarts = Restarts
  };
transaction(#client{path = mnesia, write_ms = WriteMs, read_cost = ReadCost} = Client)->
  Locks = mnesia_locks(Client),
  put(?RUNS, 0),
  StartedAt = erlang:monotonic_time(microsecond),
  ?TRACE(tx_begin, self(), mnesia),
  {atomic, {ReadUs, WriteUs}} = mnesia:transaction(
    fun()->
      put(?RUNS, get(?RUNS) + 1),
      ?TRACE(tx_run, self(), get(?RUNS)),
      Read = mnesia_lock(Locks, ReadCost, 0),
      ?TRACE(tx_locked, self(), get(?RUNS) - 1),
      Written = write(WriteMs),
      ?TRACE(tx_written, self(), []),
      {Read, Written}
    end
  ),
  ?TRACE(tx_end, self(), []),
  #sums{
    transaction_us = erlang:monotonic_time(microsecond) - StartedAt,
    write_us = WriteUs,
    read_us = ReadUs,
    restarts = get(?RUNS) - 1
  };
transaction(#client{path = global, nodes = Nodes, write_ms = WriteMs, read_cost = ReadCost} = Client)->
  % global runs only at deadlocks = false
  Terms = order(terms(Client), false),
  StartedAt = erlang:monotonic_time(microsecond),
  ReadUs = global_lock(Terms, Nodes, ReadCost, 0),
  WriteUs = write(WriteMs),
  lists:foreach(fun(Term)-> true = global:del_lock({Term, self()}, Nodes) end, Terms),
  #sums{
    transaction_us = erlang:monotonic_time(microsecond) - StartedAt,
    write_us = WriteUs,
    read_us = ReadUs
  }.

% The imitated write, its measured time
write(WriteMs)->
  work(WriteMs).

% The imitated read under the lock just taken, its measured time.
% read_cost 0: no read
read(0)->
  0;
read(ReadCost)->
  ?TRACE(tx_read, self(), []),
  work(ReadCost).

work(Ms)->
  StartedAt = erlang:monotonic_time(microsecond),
  timer:sleep(Ms),
  erlang:monotonic_time(microsecond) - StartedAt.

% {Refs, ReadUs, Restarts} of the committed attempt
elock_lock([{Term, Nodes, Options} | Rest], Locks, Refs, ReadUs, Restarts, ReadCost)->
  case elock:lock(?SCOPE, Term, Nodes, Options) of
    {ok, Ref}->
      elock_lock(Rest, Locks, [Ref | Refs], ReadUs + read(ReadCost), Restarts, ReadCost);
    {error, abort}->
      ?TRACE(tx_restart, self(), length(Refs)),
      lists:foreach(fun elock:unlock/1, Refs),
      ?TRACE(tx_released, self(), []),
      % The reads of the attempt are lost with it
      elock_lock(Locks, Locks, [], 0, Restarts + 1, ReadCost)
  end;
elock_lock([], _Locks, Refs, ReadUs, Restarts, _ReadCost)->
  {Refs, ReadUs, Restarts}.

% ReadUs of the run. A run that restarts is left by mnesia:lock/2
mnesia_lock([{Item, Kind} | Rest], ReadCost, ReadUs)->
  _ = mnesia:lock(Item, Kind),
  mnesia_lock(Rest, ReadCost, ReadUs + read(ReadCost));
mnesia_lock([], _ReadCost, ReadUs)->
  ReadUs.

% ReadUs
global_lock([Term | Rest], Nodes, ReadCost, ReadUs)->
  true = global:set_lock({Term, self()}, Nodes, infinity),
  global_lock(Rest, Nodes, ReadCost, ReadUs + read(ReadCost));
global_lock([], _Nodes, _ReadCost, ReadUs)->
  ReadUs.

%%=================================================================
%%  The locks of a transaction
%%=================================================================
%%-----------------------------------------------------------------
%%  K keys of the shared pool and N - K private terms, a fresh
%%  reference each. K is N * P / 100 rounded at random, exact on
%%  average
%%-----------------------------------------------------------------
terms(#client{locks = N, intersect = P, pool = S})->
  Q = N * P,
  K =
    case chance(Q rem 100) of
      true-> Q div 100 + 1;
      false-> Q div 100
    end,
  pool_keys(K, S, #{}) ++ [ make_ref() || _ <- lists:seq(1, N - K) ].

%%-----------------------------------------------------------------
%%  K distinct random keys of the pool of S (K =< N =< S). An index
%%  drawn twice is in the map already: the map stays as it is and
%%  the draw is repeated
%%-----------------------------------------------------------------
pool_keys(K, _S, Indices) when map_size(Indices) =:= K->
  [ {shared, I} || I := _ <- Indices ];
pool_keys(K, S, Indices)->
  pool_keys(K, S, Indices#{rand:uniform(S) => []}).

% The arguments of elock:lock/4: {Term, Nodes, Options}
elock_locks(#client{nodes = Nodes, exclusive = E, deadlocks = Deadlocks} = Client)->
  order([ elock_mode(Term, Nodes, E) || Term <- terms(Client) ], Deadlocks).

elock_mode(Term, Nodes, E)->
  case chance(E) of
    true-> {Term, Nodes, #{}};
    false-> {Term, [node()], #{is_shared => true}}
  end.

% The arguments of mnesia:lock/2: {LockItem, LockKind}
mnesia_locks(#client{exclusive = E, deadlocks = Deadlocks} = Client)->
  order([ mnesia_mode(Term, E) || Term <- terms(Client) ], Deadlocks).

mnesia_mode(Term, E)->
  case chance(E) of
    true-> {{record, ?TABLE, Term}, write};
    false-> {{record, ?TABLE, Term}, read}
  end.

%%-----------------------------------------------------------------
%%  The order of the deadlocks setting. false: the sorted order
%%  of the terms. The locks of every path sort by their term: it
%%  leads the tuple of elock and the lock item of mnesia, the lock
%%  of global is the term itself; the terms of a transaction are
%%  distinct, so the modes never decide. true: a random order
%%-----------------------------------------------------------------
order(Locks, false)->
  lists:sort(Locks);
order(Locks, true)->
  [ Lock || {_, Lock} <- lists:sort([ {rand:uniform(), Lock} || Lock <- Locks ]) ].

% true with the probability Percent / 100
chance(Percent)->
  rand:uniform(100) =< Percent.

%%=================================================================
%%  Utilities
%%=================================================================
% The performance map of performance.config merged over ?DEFAULTS
performance()->
  maps:merge(?DEFAULTS, ct:get_config(performance, #{})).

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
