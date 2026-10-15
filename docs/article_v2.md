# elock: scalable distributed locks with deadlock detection, 5.5 million locks per second on two nodes

*Locks are cheap, waiting for them is not. Take the waiting out and your only limit is your cores: that is how our production system came to 5.5 million locks per second on two nodes. [elock](https://github.com/vzroman/elock) is a lock library for Erlang: locks on any term, exclusive or shared, on one node or on several at once, with deadlocks detected across nodes, and a happy path that waits for nobody.*

![Locks per second on two nodes, 10,000 clients per node, ten shared local locks per transaction with no contention: elock 5.57 million per second with the schedulers 99.8 percent busy, mnesia 211 thousand per second at 11 percent busy.](figures/happy-path.svg)

The runs behind every number here: two hosts with two Xeon Gold 6342 sockets each, 48 cores, hyper-threading off, a 10 GbE bond between them, one Erlang 27.2.2 node per host in a Docker container, driven by Common Test from a third machine. 10,000 clients per node unless said otherwise, over a pool of 5 million objects, 20 million locks per point. A transaction is a plan of keys from a seeded generator, the locks for them, and the release. There is no simulated write, and a failed attempt is repeated at no cost. mnesia is in the tables as the lock engine every Erlang programmer has, global as the other one; neither is a competitor on the same design. The suite is in the repository under `test/performance`.

## What elock does

The library is small, and the part that decides the numbers is smaller.

**A lock is an ETS row, and a ticket is the only synchronization.** A scope is a public ETS table. A lock on a term is a row in it: the term, the pid of its manager, and a counter. To ask for the lock, the caller increments the counter:

```erlang
lock(#request{scope = Scope, term = Term} = Request, OnQueued) ->
  case ets:update_counter(Scope, Term, {3, 1}, {Term, 0, 0}) of
    1 ->
      % the row was free: the caller holds the lock from this moment
      Manager = start_manager(Request),
      {ok, Manager};
    Ticket ->
      % somebody holds it: queue behind them, in ticket order
      Manager = get_manager(Scope, Term, Ticket),
      Manager ! Request#request{ticket = Ticket, proxy = self()},
      wait_verdict(Manager, OnQueued, Request)
  end.
```

Ticket 1 means the row was free: the caller holds the lock the moment the counter returns and spawns a manager for the term. Any later ticket means somebody holds it: the caller sends its request to the manager and waits for a verdict, `locked`, `timeout` or `deadlock`. The uncontended path is one atomic counter update in the caller, about 17 µs of scheduler time, and it is why both nodes were at 99.8 percent in the figure above: every core was taking locks, because nothing stood in the way. mnesia's 11 percent in the same figure is its locker, one process per node, that every request calls whether the key is free or not.

**A manager per locked term, for as long as it is locked.** The manager keeps the queue in ticket order and grants in that order. A shared request joins the shared holders at once only while nobody is waiting, so a stream of readers cannot starve a writer. When the last holder leaves and nobody waits, the manager deletes the row and exits. Contention on a hot term is one process on one scheduler; two hot terms are two processes on two schedulers.

**A request for several nodes is one worker per node, over ecall.** Each worker asks its node through [ecall](https://github.com/vzroman/ecall), the batched transport of the [previous article](https://erlangforums.com/t/ecall-batched-pooled-send-cast-call-over-erlang-distribution-scales-past-the-single-output-queue-per-node-pair/5940). All nodes are asked at once; the request holds the nodes that have granted it while it waits for the rest, and if any node fails the granted ones are released, so the result is all or nothing. A remote worker stays alive after the grant as the lock's representative on the client's node: it monitors the client and sends the release when the client exits.

**Deadlock detection is off the happy path.** Only a request that has to wait while its process already holds something can deadlock, and only such a request pays for detection. It gets a graph worker, spawned when the manager reports it queued and never before. The worker publishes the process's holds into a wait-for graph that every node keeps in a public ETS table, and every new edge starts a walk from it, expanding locks on their own node and hopping to the other nodes with one cast per node. A walk that comes back to its origin has found a cycle, and the request of the younger context loses: it gets `{error, {deadlock, Lock}}`, keeps what it holds, and the caller decides, typically release and repeat. A walk has a limit on the locks it visits, past which the origin loses with `probe_limit` rather than let the walk become the cost.

**Locks die with their holder.** A local holder is monitored by its manager, a remote one by its worker, and every node drops the locks of the clients of a node that has gone down. A process that crashes while holding locks leaves no lock behind. That is a correctness property in a lock library; in our system it turned out to be a feature.

## Distributed locks

Now every write takes an exclusive lock on its key on both nodes, writes, releases: 10,000 clients per node with 1,000 transactions each, then 100,000 clients per node with 100 each, the same 20 million locks.

![Locks per second for one exclusive lock on both nodes per write, with the demand of 200,000 per second marked. At 10,000 clients per node: elock 95.6 thousand, mnesia 21.7 thousand, global 1.3 thousand. At 100,000 clients per node: elock 307 thousand, mnesia 15.8 thousand; global was not run.](figures/distributed.svg)

mnesia has three limits in its way, one behind the other: the locker, one process per node that every request goes through whether the key is free or not; the distribution queue, one per node pair, that every cross-node signal enters one at a time; and the wire. Ten times the clients gave it a quarter less throughput, 12 second transactions and 10 GB of memory per node. global's `set_lock` is a cluster-wide protocol with retries: 16 seconds per lock, 4.4 hours for the run, and we did not run it with ten times the clients.

elock has no locker, and its cross-node traffic rides ecall, which takes the distribution queue out of the way: ten times the clients gave three times the locks, and the batches got cheaper per lock as they grew. What is left is the wire. A cross-node lock is a round trip, 208 ms at 10,000 clients per node, and a client spends its time waiting for it while the nodes sit three quarters idle. More clients means more round trips in flight, not a shorter one.

elock can take the waiting out entirely, or cut it down a lot, but in the end that depends on the design of your system. The rest of this article is how we came to the 5.5 million figure in our production system.

## Our production system and its demand

The system is the SCADA/EMS we built for KEGOC, Kazakhstan's national electricity grid operator, described in the [discussion](https://erlangforums.com/t/ecall-batched-pooled-send-cast-call-over-erlang-distribution-scales-past-the-single-output-queue-per-node-pair/5940/10) under the previous article.

The unit of information is a tag, much like a classic SCADA tag. Tags are data, not processes: records with a schema in a database engine that supports subscriptions, to a tag or to a query. The processes are the actors, the runtime services that read and write tags:

- **A device connection.** One process per connection to a field device, a PLC, an energy meter, any smart device. It receives updates from the device and writes them into the tags, and writes some points back, setpoints, PID settings and the like.
- **A user script.** It subscribes to its input fields, recalculates when they change, and writes the results to its output fields.
- **An archive.** It subscribes to its source tags and hands the values to a pool of writers in front of a time-sharded RocksDB store.
- **A user session.** One process per browser connection, subscribed to the tags on the open screen. A writer only when the user changes a setpoint or a setting.
- **High-level services.** The state estimation engine, prediction, the electricity grid graph, AI agents, the dispatcher assistant among them. Mainly consumers, with occasional writes back.

Never a process per tag. A node runs a large population of such actors, each quiet until its device, its inputs or its user has something new, and most of them are both producers and consumers. A write does not stop at the node where it happened: it is shipped to the neighbouring nodes, and on every node the engine fans it out to the local subscribers. Actors have to synchronise over the tags they write, so there are locks, and every lock that spans nodes crosses the same channel as the writes.

The demand: a pool of 5 million real-time objects, about 10,000 actors over them, each writing about ten objects per second. That is 200,000 keys per second across the cluster, and the number every design below is measured against.

## Design 1: one exclusive lock on both nodes, per write

The simplest thing that is correct. Each actor, for each key it writes, takes an exclusive lock on both nodes, writes, releases. 10,000 clients per node, 1,000 transactions each, one key per transaction: 20 million locks.

| path | keys/s | avg transaction | restarted | scheduler busy | peak memory |
|---|--:|--:|--:|--:|--:|
| elock | 95,642 | 208 ms | 0.15 % | 24 % | 2.6 GB |
| mnesia | 21,671 | 897 ms | 1.3 % | 54 % | 4.2 GB |
| global | 1,265 | 15.8 s | 0 | 2 % | 3.7 GB |

Under half the demand with elock, a tenth of it with mnesia, and global took 4.4 hours for the run. The nodes are idle in every row: the clients are waiting for round trips, 20,000 of them at a time, and the throughput is how many of those round trips the link completes per second.

## Design 2: ten times the actors

If the limit is the round trip, put more clients on it. 100,000 clients per node, 100 transactions each, the same 20 million locks.

| path | keys/s | avg transaction | restarted | scheduler busy | peak memory | max run queue |
|---|--:|--:|--:|--:|--:|--:|
| elock | 307,275 | 616 ms | 1.5 % | 77 % | 5.5 GB | 34,630 |
| mnesia | 15,791 | 12.5 s | 7.7 % | 62 % | 10.6 GB | 19,033 |

elock met the demand with half as much again to spare; mnesia went backwards. We did not take it. Ten times the actors is not a parameter, it is a rewrite of the system: the device connections, scripts and archives would be split into pieces that have no reason to exist except to lock in parallel. And the schedulers were already at 77 percent with a run queue of 34,000, on a demand that is a floor. There was not much left for the system to do its actual work with.

## Design 3: batches, in random order

A transaction of ten keys is cheaper to write than ten transactions of one, which for the store is true. So, back at 10,000 clients, each locks its ten keys exclusively on both nodes, then writes, then releases. A real actor does not know in advance which keys it will need, so it takes them in the order it comes to need them, which for the benchmark is random. 100 transactions per client, the same 20 million locks.

| path | keys/s | tx/s | avg transaction | restarted | scheduler busy |
|---|--:|--:|--:|--:|--:|
| elock | 88,287 | 8,829 | 2.2 s | 1.0 % | 17 % |
| mnesia | 79,714 | 7,971 | 2.5 s | 29.8 % | 4 % |

elock's lock rate held, eight percent below design 1. The transaction did not: 2.2 seconds on average against 208 ms for a single lock, eleven seconds at the maximum. That is the chain. A transaction with six of its ten locks waiting for the seventh holds six keys that other transactions wait for, and they hold theirs. One percent of transactions closed a cycle and were restarted; the rest paid for the chains, with the schedulers idle at 17 percent.

mnesia sped up, and stayed behind. Its locker prevents deadlocks instead of detecting them: a younger transaction asking for a lock held by an older one is restarted. Under random-order batches that rule fires constantly, and it restarted 30 percent of all transactions at a scheduler cost of 4 percent, because a restarted transaction does nothing but wait and try again. The harness makes a restart free. A production system does not: a restarted transaction has done its reads, perhaps its computation, and does them again.

This is where we stopped believing in a better lock engine. Design 3 builds chains by construction, and a system that does that is not helped by an engine, however scalable.

## Design 4: lock the piece of work, not the keys

The way out was in the shape of the traffic. About 99 percent of the writes come from the heavy runtime services, the device connections, the calculations, the user scripts, each producing derived values for a set of tags that is known in advance and does not overlap with any other service's set. The rest comes from the occasional writers: a user changing a setpoint, the state estimation engine, the grid graph, an AI agent. Those can touch any key, including the keys a heavy service owns, so the two kinds of writer still have to synchronise. Heavy services do not have to synchronise with each other at all.

So the lock that has to cross the link is not the lock on a key. It is the lock on a piece of work.

**Ownership by lock.** Every node starts every configured service. On start, each service asks for an exclusive lock on its service object, the PLC connection object, the script object, on every node of the scope. The service that gets the lock keeps it and does the work for its set of tags. The service that does not get it waits inside the request, holding nothing. The request is a loop: when it returns an error, because a node went away or the scope on it stopped, the service re-reads the ready nodes of the scope and asks again. One cross-node exclusive lock per service, once, at start.

**Heavy writes under shared local locks, in batches.** A service writes its tags under a shared lock on each key, on its own node only. Shared, because the only conflict a heavy write can have is with an occasional writer, and shared holders never wait for each other. Local, because that is where the occasional writer's exclusive request will arrive. Between occasional writes, which is nearly always, the service's shared requests are granted at once, by the caller itself.

**Occasional writes under exclusive locks on every node.** An occasional writer takes an exclusive lock on each key it writes, on every node. On the heavy service's node it queues behind the shared holders of that key and is granted when they release; the service's next shared request on that key queues behind it. That is the whole synchronisation between the two kinds of writer, and it is paid by the rare one.

The benchmark of the heavy path: 10,000 clients per node, 100 transactions each, ten shared local locks per transaction on keys nobody else touches, which is what disjoint sets mean. The same 20 million locks.

| path | keys/s | tx/s | avg transaction | scheduler busy | peak memory | network |
|---|--:|--:|--:|--:|--:|--:|
| elock | 5,573,802 | 557,380 | 30 ms | 99.8 % | 2.8 GB | none |
| mnesia | 210,543 | 21,054 | 936 ms | 11 % | 2.6 GB | none |

20 million locks in 3.6 seconds, 28 times the demand, with no traffic on the link and both nodes fully busy with the work the benchmark asked for. The 30 ms of a transaction is 10,000 runnable clients sharing 48 cores, not the locks. mnesia took 95 seconds at 11 percent busy: 10,000 clients queued behind its locker. The occasional path is design 1, 95,000 exclusive two-node locks per second, which for one percent of the traffic is a ceiling we will not see.

Two things came for free. The load is balanced: which node runs which service is decided by who wins the lock, and a node that is added takes the services whose locks it gets. And every service has a standby: when a service dies, or its node does, its locks are released, the partner on the neighbouring node that has been waiting inside its request gets the lock and continues the work. The retry loop and the property that locks die with their holder are the whole mechanism; there is no failover code.

A third node, by the same reasoning, adds its own heavy capacity, since every heavy lock is local. What crosses the link is the occasional writers, which lock on every node and get slower by a node, and the service locks at start. We have not measured it, and it is the claim we would test first.

## Takeaways

- **Waiting is lost time.** A granted lock costs microseconds. A waiting one costs the wait, and everything its holder holds waits with it.
- **Not all locks cost the same.** Shared local, exclusive local, exclusive across nodes, in that order. Put the common path on the lowest rung.
- **Make the cross-node exclusive lock rare.** Lock the piece of work once, at start. Lock its keys locally, shared, on every write.
- **Keep the chains short, and count the restarts.** Holding while waiting is the multiplier, on one node as on two. Batches in random order build chains and close them into deadlocks, and a restart is free only in a harness.
- **The engine should get out of the way when nothing waits.** No locker process; the caller takes the row itself. One path used every core, the other used one.
- **A lock that dies with its holder is a standby.** Ownership through a lock gives balance and failover without a line of failover code.

The library, with its performance suite, is at [github.com/vzroman/elock](https://github.com/vzroman/elock).
