# elock performance report

The report reads completed Common Test points from `_build/test/logs`.

```bash
cd performance_report
npm install
npm run dev
```

Development mode serves the application at `http://localhost:5173`. It starts
the data backend on port 3000 and refreshes the page data every five seconds.

For a production-style local build:

```bash
npm run build
npm start
```

The built application is served at `http://localhost:3000`. From the project
root, `make performance_report` installs, builds, starts it and opens the page.

The performance suite runs one fixed workload from
`test/performance/performance.config`. Supply all fields; workload settings are
scalars and are not merged with defaults. Node roles and `env_settings` retain
their existing meaning. From the repository root:

```bash
./rebar3 ct --spec test/performance/test.spec
```

`transaction => #{read => R, update => U, write => W}` gives every transaction
`R + U + W` operations on distinct keys. With `unique_keys => false` the keys are
drawn from `objects_pool_size`, shared across every client and node. With
`unique_keys => true` every operation locks a fresh reference: no key is ever
locked twice, by any client or on any path, so there is no contention, no
deadlock and no restart on any path. `objects_pool_size` and `deadlocks` do not
apply then, the seeded state is not consumed, and the references differ between
paths, which is immaterial since nothing shares them.
Reads take shared locks; updates and writes take exclusive locks. Reads and
updates pay `read_ms` immediately under their lock. After all acquisitions,
`(U + W) * write_ms` is paid once while all locks are held, then locks release.
The workload simulates costs without storing or updating records.

For each path, each numbered client initializes `rand:seed_s(exsss,
{Seed, NodeIndex, ClientIndex})`. Nodes are numbered by sorted configured role;
clients are numbered before spawning. The same seed replays the same plans on
all paths on the same OTP runtime. Retries reuse the current plan and do not
consume workload random state. Scheduling and restart counts are not replayed.

`timeout` is a positive number of milliseconds or `undefined` (no limit).
Elock uses its native per-node queue timeout; global times one complete
acquisition from the client. Failed elock/global attempts release or cancel
locks, sleep `restart_ms` once, and retry the full plan without a cap. Mnesia
uses native transactions, retries and backoff; **configured timeout and restart
delay do not apply to Mnesia**, including retries after a returned abort.

`pacing_ms` is the fixed length of a transaction cycle on every path, including
Mnesia. A cycle starts before plan generation and covers every attempt of the
plan, the commit and the lock release; the client then idles for the rest of
the cycle and starts the next one. A transaction that overruns its cycle leaves
no rest, and the next cycle starts at once. A client completing `N` transactions
idles at most `N - 1` times; it never idles after its last transaction. Set
`pacing_ms => 0` to start every transaction immediately. The pacing bounds the
offered load at `nodes * clients_per_node * 1000 / pacing_ms` transactions per
second; the report shows this bound next to the run constants.

Global runs only for zero reads, sorted order (`deadlocks => false`), and when
`node_count * clients_per_node * (R + U + W) <= global_max_locks`. Skips appear
in the Common Test log and in executed paths' report data.

Each executed path writes `performance_data/<path>.json` in the suite's
`log_private` directory. The running marker carries the same fixed workload.
The report compares paths directly and displays operation counts, pool, costs,
timeout, restart delay, pacing, seed and order. Historical result formats are not
converted or supported.

Lock time share is `100 * sum(lock durations) / sum(transaction durations)`.
The lock duration of a transaction is its transaction duration less its work.
The work is the measured duration of the reads and the commit delay of the
attempt that committed; an aborted attempt does no work. Everything else is
lock time: the lock calls, the aborted attempts with their reads, the releases,
the restart delay, and for Mnesia its own restart backoff and transaction
engine overhead. Transaction time starts after plan generation and ends after
release. The idle rest of a cycle is outside both transaction and lock
measurements. Throughput is over the elapsed time of the point: on the node
that takes longest, from the start of its first client to the last transaction
of its latest client, which includes plan generation and the idle rests but
not the reporting of the clients. `run_ms` is the controller's clock around
the point, with the start and the reporting of the clients. Throughput per
client is the mean over the clients of a client's own transactions per second,
over its own span from its start to its last transaction. The span holds one
pacing less than transactions, as a client never idles after its last one, so
the pacing bounds it at `1000 / pacing_ms` only up to the factor
`N / (N - 1)` for `N` transactions per client. Resource
measurements cover each node from the start of its clients to their last
report.

Report checks:

```bash
npm test --prefix performance_report
npm run build --prefix performance_report
```
