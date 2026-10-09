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

`transaction => #{read => R, update => U, write => W}` selects exactly `R + U + W`
distinct keys from `objects_pool_size`, shared across every client and node.
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

Global runs only for zero reads, sorted order (`deadlocks => false`), and when
`node_count * clients_per_node * (R + U + W) <= global_max_locks`. Skips appear
in the Common Test log and in executed paths' report data.

Each executed path writes `performance_data/<path>.json` in the suite's
`log_private` directory. The running marker carries the same fixed workload.
The report compares paths directly and displays operation counts, pool, costs,
timeout, restart delay, seed and order. Historical result formats are not
converted or supported.

Lock time share is `100 * sum(acquisition durations) / sum(transaction durations)`.
Acquisition time includes failed calls and every attempt. Transaction time starts
after plan generation and includes reads, commit delay, release, and retry work.
Throughput retains the overall run window, which includes plan generation.

Report checks:

```bash
npm test --prefix performance_report
npm run build --prefix performance_report
```
