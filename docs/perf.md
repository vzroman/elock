# elock against mnesia in the performance tests — report

Status: measured 2026-10-06 on branch `graph` at `1531188`, with the trace of §3 in the working tree. The possible solutions of §7 are proposals: none of them is implemented.

The report answers one question: where does the time of a transaction go when elock loses to mnesia in `test/performance`, and where does mnesia win it. Every number below is measured. Where a statement is a reading or an expectation, it says so.

## 1. Summary

1. A lock call that does not have to wait costs the same or less in elock: 0.010 ms local and 0.42 ms on two nodes, against 0.16 ms (read) and 0.41 ms (write) in mnesia. The chance that a call meets a conflicting holder is the same in both, 11 to 12% for a shared call and 19 to 21% for an exclusive one.
2. elock loses in the price of a conflict. Its request waits, 121 ms on average at the first losing point. mnesia's request waits 7 ms if its transaction is older than the holder, and otherwise is refused at once: the transaction releases everything, sleeps 16 ms on average and starts again.
3. The waits of elock are long because the holders wait themselves. A lock is held 51 ms for a 10 ms write, and for 77% of that time its holder is queued at another lock. The chain from a waiter to a holder that runs is 6 deep on average, 16 with 100 locks per transaction, 26 with 1000 clients per node. In mnesia it is 1.0 to 1.4: the waiter stands right behind a running holder.
4. The same loss is there on one node, with no distribution and a deadlock found in 0.3 ms: 1113 against 3006 transactions per second. The distribution link and the detector are not what loses the point with 10 locks and 100 clients.
5. With 100 locks per transaction, or with 1000 clients per node, deadlocks become the second cost. 0.8 to 0.9 deadlocks are alive at a moment on average, one lives 14 to 28 ms on two nodes (3 ms on one), and with chains that deep it stands at the head of 27 to 61% of all the waiting.
6. In the sorted order elock wins on one node and on two. mnesia restarts there as well, 2.6 times per transaction, and its single locker process is the limit.
7. A wait limit in the client closes the gap with elock as it is. Every lock call gets `timeout => 20`, a timeout or a lost deadlock releases the locks of the attempt, the client sleeps as mnesia does and starts again. That takes elock from 1786 to 5759 transactions per second at 10 locks (mnesia 5830 to 5957), from 32 to 368 at 100 locks (mnesia 417), from 1297 to 6392 at 1000 clients (mnesia 7199).

## 2. The workload and the results of the suite

`performance_transaction_SUITE` runs clients that repeat a transaction: take N locks one after another, sleep 10 ms (the write), release them.

- P = 50: half of the locks of a transaction are keys of a shared pool, the others are private (a fresh reference, never contended). The pool has as many keys as all the clients hold at once, so a pool key is wanted by 0.5 clients on average at every point.
- E = 50: half of the locks are exclusive, taken on all the nodes. The others are shared, taken on the local node.
- The order of the locks is sorted or random per run. In the random order elock answers a lost deadlock by releasing the attempt and starting over at once; mnesia restarts by itself.

The runs of the suite on docker nodes, 100 transactions per client, transactions per second:

| nodes | clients per node | locks | order | elock | mnesia | elock restarts per tx | mnesia restarts per tx | elock MB sent per node | mnesia MB sent per node |
|---|---|---|---|---|---|---|---|---|---|
| 2 | 100 | 10 | random | 1530 | 6015 | 0.01 | 0.91 | 44 | 13 |
| 2 | 1000 | 10 | random | 1175 | 6724 | 0.01 | 1.15 | 3335 | 128 |
| 2 | 100 | 100 | random | 25.4 | 534 | 2.00 | 4.09 | 17051 | 195 |
| 2 | 100 | 100 | sorted | 708 | 443 | 0.01 | 2.86 | 919 | 228 |
| 1 | 100 | 100 | sorted | 661 | 624 | 0 | 2.76 | – | – |

## 3. How it was measured

### 3.1 The trace

Trace points write an event `{Id, Step, Time, PID, Data}` to an ETS table of the node at every step of a lock call. `Time` is the OS clock in microseconds, which the nodes of one host share, so the events of several nodes are joined by the request and compared in time.

| Where | What |
|---|---|
| `include/elock_trace.hrl` | `?TRACE(Step, Id, Data)`. Compiled in only with `ELOCK_TRACE`, which the test profile of `rebar.config` defines. It writes only while a trace is started |
| `src/elock_trace.erl` | The table, `start/1` with the limit of the events, `stop/0`, `dump/0`. Its header lists the steps of elock |
| `src/elock_context.erl`, `elock_manager.erl`, `elock_graph.erl` | The trace points: the client, the proxy, the manager, the graph process |
| `test/performance/mnesia/` | Copies of `mnesia_locker.erl` and `mnesia_tm.erl` of mnesia-4.23.3 with trace points. Compiled next to the suite, they replace the modules of OTP on the test nodes |
| `performance_transaction_SUITE.erl` | The steps of a transaction, and the trace of a point with `trace => true` of `performance.config` |
| `test/performance/util/performance_trace.erl` | Starts the trace of a point and saves the events of every node |
| `test/performance/util/performance_trace_report.erl` | The report of a point. Its header lists the steps of mnesia |

The report of a point is `<point>.report.txt` in `performance_trace`, next to `performance_data`. The raw events are saved beside it and can be read again without a run.

A node keeps at most 250000 events with `trace => true`, then its trace stops and the report covers the point up to there. A number instead of `true` is that limit. The report needs about 2 GB on the ct node per million events of all the nodes; a lock call writes 5 to 10 events per node.

### 3.2 The runs

The points of the suite were run by the suite's own `run_points/1` on peer nodes of one host, without docker, with 3 to 30 transactions per client. Without the trace this setup gives what the docker runs give:

| point | docker, 100 tx per client | peer nodes |
|---|---|---|
| 2 nodes, 100 clients, 10 locks, random: elock | 1530 | 1435 to 1786 in six runs |
| the same: mnesia | 6015 | 5791 to 5957 in five runs |
| 2 nodes, 1000 clients, 10 locks, random: elock | 1175 | 1297 (30 tx per client) |
| the same: mnesia | 6724 | 7199 (30 tx per client) |
| 2 nodes, 100 clients, 100 locks, random: elock | 25.4 | 32.2 (3 tx per client) |
| the same: mnesia | 534 | 417 (3 tx per client) |

The trace does not change the throughput of elock at these points (1516 to 1629 traced). It slows mnesia by 12 to 16%, whose locker process writes several events per request. The numbers of §4 are from traced runs unless they say otherwise.

### 3.3 Terms

- **Queued**: the request waits in the queue of a lock, from the moment its manager (elock) or the locker (mnesia) queues it to the grant or to the failure.
- **Not queued**: the client is inside a lock call that waits in no queue: the request or its answer is on the way.
- **Chain depth**: at every moment of a wait the lock has a holder that will leave last. If that holder is queued itself, the chain goes on to the lock it waits for. Depth 1 is a waiter right behind a holder that is not queued. The mean is weighted by the time of the waits.
- **Thrown away**: the time of an attempt from its start to the restart that ended it.
- **A cycle**: the chain comes back to one of its clients. That is a deadlock that exists at that moment.

## 4. Where the time goes

### 4.1 A call that is not queued

2 nodes, 100 clients per node, 10 locks, random order. Mean microseconds per call.

| elock, local shared call | us |
|---|---|
| take the ticket (`ets:update_counter`) | 4.1 |
| find the manager, send | 0.3 |
| the manager takes the request | 0.6 |
| the verdict to the proxy | 0.5 |
| `lock/4` returns | 6.2 |
| **all** | **11.6** |

| elock, exclusive call on 2 nodes | us |
|---|---|
| spawn the workers | 25.3 |
| the worker to its node (`ecall_connection:call`) | 190.6 |
| take the ticket | 4.2 |
| the node back to its worker | 177.1 |
| the worker to the client | 11.6 |
| `lock/4` returns | 7.8 |
| **all** | **416.5** |

| mnesia | read, us | write, us |
|---|---|---|
| in the client | 3.9 | 5.8 |
| to the local locker: its mailbox | 141.2 | 139.5 |
| in the local locker | 7.3 | 6.8 |
| the answer of the local locker | 8.5 | 8.2 |
| to the remote locker | – | 187.9 |
| in the remote locker | – | 7.3 |
| the answer of the remote locker | – | 54.3 |
| **all** | **161.0** | **409.8** |

The share of the calls on a pool key that meet a conflict is the same: 12.0% (shared) and 19.3% (exclusive) in elock, 11.0% (read) and 20.8% (write) in mnesia.

A call of elock that waits spends its time in one step. For a local call of 124.2 ms: ticket 4 us, finding the manager 6 us, the manager's mailbox 10 us, queueing 3 us, **queued at the lock 124169 us**, the verdict to the proxy 14 us, return 9 us. Queued time is 97.8% of the time of all the lock calls of the point. Requests that arrive ahead of a missing ticket are rare, 0 to 26 per point among 30 000 to 200 000 calls, and the postpone timer never fired at any point.

### 4.2 Where the time of a transaction goes

Milliseconds per transaction, by what the client does.

| point | path | all | queued at a lock | in calls, not queued | restart: release and sleep | write |
|---|---|---|---|---|---|---|
| 2 nodes, 100 clients, 10 locks, random | elock | 108.3 | 95.4 | 2.1 | 0 | 10.7 |
| | mnesia | 31.4 | 2.3 | 4.0 | 14.4 | 10.6 |
| 2 nodes, 100 clients, 100 locks, random | elock | 5929 | 5876 | 40.8 | 0.4 | 10.7 |
| | mnesia | 440 | 48.4 | 113.2 | 266.6 | 10.7 |
| 2 nodes, 1000 clients, 10 locks, random | elock | 648 | 603.7 | 33.8 | 0 | 10.8 |
| | mnesia | 343 | 34.5 | 257.9 | 39.5 | 10.8 |
| 1 node, 100 clients, 10 locks, random | elock | 79.5 | 68.1 | 0.4 | 0 | 10.8 |
| | mnesia | 27.1 | 1.9 | 0.9 | 13.7 | 10.6 |
| 1 node, 100 clients, 100 locks, random | elock | 826.7 | 811.3 | 2.8 | 0.4 | 10.8 |
| | mnesia | 161.5 | 14.1 | 18.1 | 118.0 | 10.6 |
| 2 nodes, 100 clients, 10 locks, sorted | elock | 26.6 | 13.3 | 2.5 | 0 | 10.7 |
| | mnesia | 33.1 | 2.2 | 8.0 | 12.1 | 10.7 |
| 2 nodes, 100 clients, 100 locks, sorted | elock | 219.2 | 166.7 | 40.6 | 0 | 10.9 |
| | mnesia | 505.8 | 50.6 | 342.3 | 101.1 | 10.8 |

The row of 1000 clients is a run of 10 transactions per client and includes the start of the point, see §4.6.

At the first point 155 of the 200 clients of elock are queued at a moment on average and 17 are writing. In mnesia 12 are queued, 74 sleep before a restart and 54 are writing.

### 4.3 Waits, holds and chains

| point | path | calls that wait | mean wait, ms | chain depth | a pool lock is held, ms | restarts per tx | thrown away per restart, ms |
|---|---|---|---|---|---|---|---|
| 2 nodes, 100 clients, 10 locks, random | elock | 10.7% | 121 | 6.2 | 51.3 | 0.010 | 159 |
| | mnesia | 2.2% | 7.3 | 1.04 | 10.3 | 0.88 | 1.6 |
| 2 nodes, 100 clients, 100 locks, random | elock | 6.8% | 714 | 16.4 | 1048 | 1.62 | 2162 |
| | mnesia | 0.8% | 25 | 1.42 | 30.6 | 4.00 | 23.8 |
| 2 nodes, 1000 clients, 10 locks, random | elock | 10.0% | 832 | 25.7 | 287 | 0.012 | 849 |
| | mnesia | 2.6% | 95 | 1.14 | 113 | 1.09 | 71 |
| 1 node, 100 clients, 10 locks, random | elock | 8.2% | 83 | 4.7 | 36.8 | 0.014 | 173 |
| | mnesia | 2.0% | 6.7 | 1.03 | 9.0 | 0.86 | 0.5 |
| 1 node, 100 clients, 100 locks, random | elock | 4.7% | 119 | 8.1 | 174 | 1.04 | 419 |
| | mnesia | 0.6% | 11.6 | 1.35 | 11.4 | 2.76 | 6.0 |
| 2 nodes, 100 clients, 100 locks, sorted | elock | 3.5% | 66 | 2.3 | 38.7 | 0.004 | – |
| | mnesia | 0.5% | 36 | 1.19 | 39.3 | 2.61 | 90.5 |

The first point in detail.

What the holder of a pool lock does while it holds it:

| | elock, ms | share | mnesia, ms | share |
|---|---|---|---|---|
| queued at another lock | 39.5 | 77.1% | 0.7 | 6.9% |
| writes | 10.6 | 20.7% | 8.1 | 78.7% |
| in a lock call, not queued | 1.1 | 2.1% | 1.4 | 13.3% |
| **the hold** | **51.3** | | **10.3** | |

A private key, which nobody else wants, is held 58 ms in elock for the same reason.

The time of all the waits by the depth of the chain:

| depth | 1 | 2 | 3 | 4 | 5 to 8 | 9 to 16 | 17 and more |
|---|---|---|---|---|---|---|---|
| elock | 11.1% | 10.0% | 9.9% | 9.8% | 32.4% | 24.8% | 2.0% |
| mnesia | 95.8% | 4.1% | 0.1% | 0 | 0 | 0 | 0 |

In both, the holder at the end of the chain is writing for 91% of the wait time. The difference is how many waiting holders stand in between.

The time of a transaction, traced, 2 nodes, 100 clients:

| locks | path | mean, ms | p50 | p90 | p99 | max |
|---|---|---|---|---|---|---|
| 10 | elock | 108.3 | 22.0 | 332 | 681 | 1308 |
| 10 | mnesia | 31.4 | 19.0 | 66 | 136 | 239 |
| 100 | elock | 5929 | 4552 | 13851 | 19951 | 20583 |
| 100 | mnesia | 440 | 379 | 913 | 1323 | 1563 |

### 4.4 Deadlocks and the detector

| point (elock) | wait time behind a cycle | age of a cycle when its loser leaves, mean / p90, ms | cycles alive at a moment | a hop on the way, ms | the graph process busy, per node |
|---|---|---|---|---|---|
| 1 node, 100 clients, 10 locks | 0.4% | 0.33 / 0.68 | 0.01 | – | 3% |
| 2 nodes, 100 clients, 10 locks | 1.6% | 1.8 / 5.1 | 0.04 | 0.30 | 4% |
| 1 node, 100 clients, 100 locks | 30.7% | 3.0 / 5.2 | 0.30 | – | 35% |
| 2 nodes, 100 clients, 100 locks | 61.0% | 13.7 / 30.8 | 0.80 | 3.9 | 17% |
| 2 nodes, 1000 clients, 10 locks | 26.9% | 27.7 / 46.3 | 0.88 | 6.8 | 12% |

The age is a lower bound: the first cycle found through the loser is taken, another one may be older.

The steps of a detection, mean microseconds:

| | 2 nodes, 10 locks | 2 nodes, 100 locks | 1 node, 100 locks | 2 nodes, 1000 clients |
|---|---|---|---|---|
| the manager to the graph process (its mailbox) | 36 | 673 | 1021 | 422 |
| a walk of new edges | 24 | 149 | 546 | 22 |
| a hop on the way to the other node | 298 | 3866 | – | 6754 |
| a walk of a hop | 16 | 369 | – | 61 |
| the edges at the manager to their verdict | 1548 | 17391 | 2873 | 19081 |
| the verdict to the manager, same node / other node | 21 / 676 | 80 / 1234 | 45 / – | 47 / 7090 |
| the verdict from the manager to the return of `lock/4`, same / other | 138 / 749 | 285 / 477 | 32 / – | 203 / 9111 |

What a hop carries:

| | 2 nodes, 10 locks | 2 nodes, 100 locks, random | 2 nodes, 100 locks, sorted | 2 nodes, 1000 clients |
|---|---|---|---|---|
| hops per transaction | 1.2 | 26.4 | 5.2 | 1.2 |
| locks to expand, mean | 8.2 | 202 | 82 | 25 |
| visited locks carried, mean / max | 42 / 1335 | 1914 / 9698 | 299 / 5527 | 361 / 14228 |
| the term of a hop, mean / max, KB | 4.3 / 115 | 174 / 847 | 35 / 551 | 32 / 1202 |

On a link measured apart, a visited lock of the shared pool takes 16 to 17 bytes and a private one 29 to 32. `erlang:external_size/1`, the size of the term above, counts the atoms of the scope and of the node in every key, and the link sends them as cache references: the link carries about a quarter of the term size.

More facts of the detector:

- With 100 locks on 2 nodes 1326 verdicts were issued for 600 transactions. 346 of them (26%) reached a request that waited no more. Of the 1304 waits that ended without the lock, 96 (7%) were on no cycle a microsecond before the end.
- In the sorted order with 100 locks the graph process is 40% busy on each node, its mailbox is 12 to 14 long, and it issued 7 verdicts for 1000 transactions: the deadlocks of two requests for the same term that are granted one node each.

### 4.5 One node

One node takes the distribution out: every call is local (10 us), there are no hops.

| 1 node, 100 clients, traced | elock tx/s | mnesia tx/s |
|---|---|---|
| 10 locks, random | 1113 | 3006 |
| 10 locks, sorted | 3410 | 3004 |
| 100 locks, random | 98 | 380 |
| 100 locks, sorted | 611 | 413 |
| 1000 clients, 10 locks, random (10 tx per client, with the start) | 3855 | 5215 |

The loss in the random order is there without the link, and with 10 locks without the detector as well: a cycle is 0.33 ms old when it is broken and 0.4% of the wait time is behind one. The chains are 4.7 deep and a pool lock is held 36.8 ms.

With 100 locks the two nodes add to the one: per client the throughput is 0.98 transactions per second on one node and 0.145 on two, the deadlocks live 3.0 ms against 13.7, and stand at the head of 31% of the waiting against 61%.

### 4.6 1000 clients in time

The run of 10 transactions per client in ten equal parts, elock:

| from, s | tx/s | clients queued | in calls, not queued | writing | a 2-node call not queued, ms | hops, KB of the term | a hop on the way, ms | a verdict after its edges, ms |
|---|---|---|---|---|---|---|---|---|
| 0.00 | 7734 | 1114 | 786 | 84 | 18.2 | 2.1 | 9.6 | 10.7 |
| 0.79 | 3040 | 1905 | 37 | 33 | 2.5 | 37.8 | 9.8 | 37.9 |
| 1.58 | 1631 | 1902 | 6 | 18 | 0.80 | 63.8 | 7.6 | 46.3 |
| 2.38 | 876 | 1859 | 2 | 9 | 0.45 | 75.3 | 3.8 | 42.9 |
| 3.17 | 1241 | 1805 | 2 | 13 | 0.38 | 51.6 | 2.4 | 20.2 |
| 3.96 | 459 | 1753 | 2 | 5 | 0.71 | 172.5 | 5.5 | 80.4 |
| 4.75 | 1251 | 1679 | 2 | 14 | 0.38 | 60.2 | 3.0 | 54.6 |
| 5.55 | 2544 | 1510 | 5 | 27 | 0.38 | 28.6 | 2.6 | 20.5 |
| 6.34 | 1779 | 1237 | 3 | 20 | 0.38 | 70.3 | 2.9 | 19.8 |
| 7.13 | 4686 | 475 | 7 | 49 | 0.33 | 10.5 | 0.4 | 5.5 |

- In the first 0.8 s 786 clients are inside calls at once and a 2-node call takes 18 ms: the link is the limit there.
- Then 1900 of the 2000 clients are queued and the point runs at 460 to 1630 transactions per second, the level of the full run (1175). A short run misses this: 5 transactions per client gave 4790 transactions per second.
- The last parts speed up because the clients finish.

mnesia at this point is bound by its locker: the mailbox of a locker is 800 to 900 requests long, a read call takes 13.5 ms and a write call 28 ms without being queued at a lock, the locker is 72% busy.

### 4.7 The bytes on the link

KB sent per transaction per node, the counter of the distribution port:

| point | elock | mnesia | visited locks carried by the hops, per tx |
|---|---|---|---|
| 2 nodes, 100 clients, 10 locks, random | 1.9 | 0.6 | 52 |
| 2 nodes, 100 clients, 100 locks, random | 646 | 9.1 | 50463 |
| 2 nodes, 100 clients, 100 locks, sorted | 38.6 | 10.3 | 1544 |
| 2 nodes, 1000 clients, 10 locks, random, full run of the suite | 16.4 | 0.6 | – |

At 16 to 32 bytes per visited lock the hops of the second row are 400 to 800 KB of its 646 KB per node: the visited sets are most of what elock sends there.

## 5. What mnesia does differently

The code is `mnesia_locker.erl` and `mnesia_tm.erl` of mnesia-4.23.3.

1. **A request waits only for a younger transaction.** `can_lock/4` compares the requester with every holder through `allowed_to_be_queued/2`, which is true only if the holder's `#tid{}` is greater, that is the holder is younger. Otherwise the locker answers `{not_granted, #cyclic{}}` at once. The decision takes 7 us in the locker and needs no graph. At the first point 74% of the conflicts are refused and 26% are queued.
2. **A refused transaction releases everything and keeps its age.** `mnesia_tm:restart/9` sends the release to the lockers, sleeps, and runs the fun again under the same `#tid{}`. It gets older against the others with every restart, so it is refused less and less.
3. **The sleep grows with the attempt.** `mnesia_lib:random_time/2`: 2 to 10 ms after the first attempt, 5 to 41 after the second, 10 to 85 after the third. Measured means: 6.0, 22.7, 47.6, 75.2 ms.

What it buys: a lock is held for the write and little more (10.3 ms), a waiter stands right behind a running holder (96% of the wait time), an attempt that fails is 1.6 ms old.

What it costs:

- The sleep is 46 to 61% of the time of a transaction in the random order with 100 clients per node.
- It restarts in the sorted order too, where no deadlock can come from the order: 2.6 restarts per transaction with 100 locks, each throwing away 90 ms, the eighth attempt sleeping 251 ms.
- One locker process per node takes every request. 100 clients, 10 locks: 54% busy, 0.14 ms of every call is its mailbox. 100 locks, sorted: mailbox 52 long, 0.75 ms per read call and 1.9 ms per write call. 1000 clients: see §4.6.

## 6. What is not established

- The traced runs are 3 to 30 transactions per client. The full-size points of the suite were not traced.
- A hop of 174 KB is 3.9 ms on the way. How that time divides between the ecall proxy, the link and the receiving side is not traced: ecall has no trace points.
- Everything is two nodes of one host. Three nodes and real network latency were not run.
- Of §7 only the options marked measured were run, and those as probes of 3 to 30 transactions per client.

## 7. Possible solutions

The facts name two costs and two smaller ones:

- **A. The chains** (§4.3): a request waits behind holders that wait. This is the whole loss with 10 locks and 100 clients, on one node as well.
- **B. The life of a deadlock** (§4.4): 14 to 28 ms on two nodes, most of it the hops, and with deep chains one deadlock holds up most of the waiting.
- **C. The 2-node call** (§4.1): 0.4 to 0.9 ms. It is 2% of a transaction where elock loses and 18% where it wins.
- **D. The detector without deadlocks** (§4.4): 40% of a core per node in the sorted order for 7 verdicts.

### 7.1 A wait limit, a restart and a sleep in the caller — measured

Answers A, and B with it.

**The change.** None in elock. The caller gives every request a `timeout`. On `{error, timeout}` or `{error, {deadlock, _}}` it releases the locks of the attempt, sleeps a random time that grows with the attempt, and takes the locks again. The client of the probes is in §8.

**Measured.** Probes with the client of the suite changed and elock as it is, transactions per second, not traced:

| point | elock as is | limit 20 ms | limit 20 ms and mnesia's sleep | mnesia |
|---|---|---|---|---|
| 2 nodes, 100 clients, 10 locks, random | 1786 | 5433 | **5759** | 5830 to 5957 |
| 2 nodes, 100 clients, 100 locks, random | 32.2 | 169 | **368** | 417 |
| 2 nodes, 1000 clients, 10 locks, random | 1297 | 4634 | **6392** | 7199 |
| 1 node, 100 clients, 10 locks, random | 1324 | – | **3185** | 2816 |
| 1 node, 100 clients, 100 locks, random | 97.8 | – | **363** | 334 |

The limit and the sleep, 2 nodes, 100 clients, with restarts per transaction:

| | 10 locks | restarts | 100 locks | restarts |
|---|---|---|---|---|
| no limit | 1786 | 0.01 | 32.2 | 1.58 |
| limit 5 ms | 4498 | 1.61 | 68.8 | 64.1 |
| limit 20 ms | 5433 | 0.26 | 169 | 18.1 |
| limit 50 ms | 4752 | 0.09 | – | – |
| limit 5 ms and the sleep | 4616 | 0.56 | 291 | 3.27 |
| limit 20 ms and the sleep | 5759 | 0.18 | 368 | 2.82 |

What the trace of the 20 ms limit with the sleep shows, against §4:

| 2 nodes, 100 clients | 10 locks: as is | with the limit | 100 locks: as is | with the limit |
|---|---|---|---|---|
| ms per transaction | 108.3 | 29.2 | 5929 | 354.6 |
| queued, granted later | 95.1 | 8.8 | 4877 | 28.7 |
| queued, ends without the lock | 0.4 | 4.0 | 999 | 55.2 |
| restart: release and sleep | 0 | 2.5 | 0.4 | 173.9 |
| 2-node calls, not queued | 2.0 | 2.9 | 39.9 | 83.7 |
| chain depth | 6.2 | 1.5 | 16.4 | 2.0 |
| a pool lock is held, ms | 51.3 | 16.3 | 1048 | 35.9 |
| deadlock verdicts delivered per 1000 tx | 10 | 5 | 1632 | 60 |
| visited locks per hop | 42 | 10 | 1914 | 100 |
| KB sent per transaction per node | 1.9 | 1.6 | 646 | 36 |
| transaction time p99 / max, ms | 681 / 1308 | 140 / 525 | 19951 / 20583 | 1507 / 1746 |

The limit of 20 ms is twice the write. A waiter right behind a writing holder is never cut by it, a waiter behind a chain is.

**Costs and open questions.**

- The limit has to fit the hold of a running holder. 5 ms, under the write, restarts 64 times per transaction with 100 locks; 50 ms gives 12% less than 20 ms with 10 locks. With the sleep the result depends on it much less.
- Without the sleep the limit alone gives 169 instead of 368 with 100 locks.
- A restarted transaction has no priority. The longest transaction was 525 ms against 239 in mnesia with 10 locks, 1746 against 1563 with 100. Starvation was not seen and is not excluded.
- Every restart by timeout first waits the limit out: 4.0 of 29.2 ms and 55 of 355 ms per transaction.
- It is the caller's code. elock can document the pattern in the "Deadlocks" section of `elock.erl`, or carry it as a function of the API that takes a list of locks; the second is a new interface and was not designed here.

### 7.2 An age rule in the manager — not measured

Answers A, B and D for the requests that use it. This is the rule of mnesia (§5) in `elock_manager`.

**The change.** A request carries the age of its transaction, an option of `lock/4` that the caller takes once and keeps over its restarts. In `add_busy_request/2`, where a request is queued today, the manager compares it with the holders and with the requests queued ahead. Older than all of them: it is queued as today. Otherwise the manager answers at once with a new verdict, the request fails as a whole (`{error, busy}` beside `timeout` and `deadlock`), and the caller releases, sleeps and repeats with the same age.

**What it adds to 7.1**, as expectations from the facts, not as measurements:

- No wait before a refusal. That wait is 14 to 16% of a transaction in the probes of 7.1. In mnesia the refusal takes 7 us in the locker.
- The oldest transaction is never refused, so every transaction ends.
- Requests under the rule need no edges in the graph. Every wait goes from an older request to a younger one, so no cycle closes through them. With "no age" read as "older than everyone", a request with an age never waits behind one without, and cycles stay among the requests without an age, which keep their edges as today. This argument has to be checked in a spec, with the upgrade (`try_barging/2`) and the requests for several nodes.

**Costs and open questions.**

- It must stay an option per request. In the sorted order waiting wins (692 against 334 transactions per second) and the age rule is what makes mnesia restart there.
- A new option, a new error, a field in `#request{}` and in `#req{}`, a comparison on the path of a request that has to wait. The path of a request that is granted at once does not change.
- The ages of two nodes must compare: the caller's clock, node and a unique integer, for example.
- The sleep stays the caller's.

### 7.3 Hops that carry the locks to expand and nothing else — not measured

Answers B for the callers that wait without a limit, and most of the bytes of §4.7.

**The facts.** A hop names 202 locks to expand and carries 1914 visited ones with 100 locks; 25 and 361 with 1000 clients. A hop of 4 KB is 0.30 ms on the way, a hop of 174 KB is 3.9 ms. A deadlock lives 13.7 ms on two nodes and 3.0 ms on one.

**The change.** Today `elock_graph:verdict/2` sends the visited set of the whole walk with every hop, and `handle_probe/1` walks on from it, so the graph process keeps nothing between two hops. A node expands only its own locks, so what it must remember to expand none of them twice is its own part of the set. The graph process would keep that part per walk and a hop would carry `expand` alone; the receiving node drops from it what it has expanded for this walk already.

**Costs and open questions.**

- The graph process gets a state per walk and has to drop it. Today it "keeps nothing clean" (`docs/graph.md` §3). A walk has no end that every node sees: the remove of the origin reaches the graph of its own node only. A bound in time or in size is new machinery; expanding a lock again after a drop is extra work, never a wrong verdict.
- A node no longer knows what the other node has expanded, so it sends hops for locks that are dropped on arrival. More small hops instead of fewer large ones: the balance has to be measured.
- The upper bound of the gain is the one node figure of §4.5, which still loses to mnesia in the random order. 7.3 shortens the deadlocks, it does not shorten the chains.

### 7.4 Walks outside the one process — not measured

Answers a part of B and D.

**The facts.** The manager's edges wait 0.7 to 2.4 ms in the mailbox of the graph process when it is 17 to 40% busy. On one node with 100 locks that is 1.0 ms of a deadlock's 3.0 ms; the walk itself is 0.55 ms.

**The change.** `docs/graph.md` §9 names it: a walker per launch over a public table, the process as the address of the hops only.

**Costs.** The table is read while the process writes it, so the walks lose the snapshot they have today. On two nodes the hops are the larger part of a deadlock's life, so 7.3 comes first.

### 7.5 The 2-node call — not measured here

Answers C.

**The facts.** 0.42 ms not queued, of which 0.19 ms is the way to the node and 0.18 ms the way back; 0.9 ms with 100 locks in the sorted order, where it is 18% of a transaction; 18 ms while 786 clients are in calls at once (§4.6). With the wait limit of 7.1 it becomes the second item with 100 locks: 84 of 355 ms.

**Where the time is.** `elock_context:run_request/3` spawns a worker per node per request. The worker calls its node through `ecall_connection:call/4`, and `ecall_receive` spawns a process per call there. The proxy of ecall sends what is in its mailbox the moment it wakes (`collect_requests/2`, `after 0`), so clients that each wait for one answer are not batched.

**The change.** Fewer and larger packets on the link: a proxy that waits a moment for more requests, or a request and its answer that need no process of their own on the other node. Both are changes of ecall or of the worker protocol and were not tried in this report.

### 7.6 No detection for the callers that lock in order — not measured

Answers D.

**The facts.** In the sorted order the graph process is 40% busy per node, the hops carry 1544 visited locks per transaction, and all of it yields 7 verdicts per 1000 transactions. Those are two requests for the same term on two nodes, granted one node each. elock wins this point all the same.

**The change, two parts.** A request for several nodes takes its nodes one after another in one order for all the clients, so two requests for a term can not hold a node each. Then a caller that locks in a global order can ask for no detection: its waiting requests send no held map and the graph never sees them.

**Costs.** A request for N nodes costs the sum of N - 1 remote calls in place of the slowest one. With two nodes that is the same call as today. A caller that asks for no detection and breaks the order hangs, unless it also sets a timeout.

### 7.7 What the measurements speak against

- **Releasing only the lock the winner waits for**, in place of the whole attempt, on a lost deadlock. Measured: 1479 against 1786 with 10 locks, 28.4 against 32.2 with 100. The thrown away attempt is not the cost, the chains are.
- **A limit without a sleep** with many locks: 169 against 368, at 18 restarts per transaction.
- **A limit under the write time**: 64 restarts per transaction with 100 locks.
- **Launching the walk after a delay**, to walk less. With 100 locks 0.8 deadlocks are alive at a moment already and the delay adds to the life of each. Not measured.
- **Tuning the detector for 10 locks and 100 clients.** 1.6% of the wait time is behind a cycle there and the graph process is 4% busy.

### 7.8 The order

A reading of the facts, not a measurement: 7.1 is the only option that is measured, it needs no change of elock and it takes every losing point to the level of mnesia. 7.2 is the same policy owned by the library, without the wait before a restart and without the graph for its requests. 7.3 is for the callers that keep waiting without a limit, and its ceiling is the one node figure. 7.5 is what is left at the top of a transaction once the chains are gone.

## 8. Reproducing

**A traced point.** Set `trace => true` in `test/performance/performance.config`, keep `transactions_per_client` low and run `make performance_tests`. The report of every point is in `performance_trace` under the priv_dir of the suite. For a baseline without the mnesia copies, remove `test/performance/mnesia`; for one without the trace points, remove `{d, 'ELOCK_TRACE'}` from the test profile of `rebar.config`.

**The client of 7.1.** The elock client of the suite with these changes:

```erlang
% Every lock call: Options#{timeout => 20}
elock_lock([{Term, Nodes, Options} = Lock | Rest], Locks, Held, Restarts)->
  case elock:lock(?SCOPE, Term, Nodes, Options) of
    {ok, Ref}->
      elock_lock(Rest, Locks, [Ref | Held], Restarts);
    {error, _TimeoutOrDeadlock}->
      lists:foreach(fun elock:unlock/1, Held),
      sleep(Restarts + 1),
      elock_lock(Locks, Locks, [], Restarts + 1)
  end;
elock_lock([], _Locks, Held, Restarts)->
  {Held, Restarts}.

% mnesia_lib:random_time/2
sleep(Attempt)->
  Dup = Attempt * Attempt,
  timer:sleep(Dup + rand:uniform(trunc(500 * (1 - 50 / (Dup + 50))))).
```

The probes ran 30 transactions per client (3 with 100 locks) on peer nodes, elock unchanged.
