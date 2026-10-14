The task is to prepare an article for the project. Take a look at https://erlangforums.com/t/ecall-batched-pooled-send-cast-call-over-erlang-distribution-scales-past-the-single-output-queue-per-node-pair/5940. It's my previous article and it's an example of a well-shaped article. Notice my latest comment on it, it mirrors some concepts in the ongoing article. Learn carefully the narrative docs/narrative.md. Find and analyze the performance tests results in /home/roman/WORKTEMP/202610/elock_tests/2nodes. We'll use them. 

It's a brainstorming step. No changes. Suggest a shape of the article. 

the design that wins is the one that takes the cross-node lock once per owner instead of once per key. The key is not actually a key to write to. it's the key of a piece of work like PLC connection that writes to predefined set of keys.

Lede, italic. A cross-node lock is three signals on the one queue per node pair from the last article, and the caller waits for them. Four designs of the same write path, two nodes, one lock library. No, it's a different story about a different problem. Deadlocks and long wait chains are very expensive aven on a single node stand. The lede should be about expensive waits that prevents useful work progress. Propoose another one.

Where we left off. We need to mention that it was SCADA EMS for the national electricity grid operator.

The problem we all know: a lock is a round trip. Short, first principles. What a local lock costs (a few ETS operations) versus a lock on two nodes (request, grant, release, each crossing the link). Then the two stock engines and their single points: mnesia's locker is one process per node, global's set_lock is a cluster-wide protocol with retries. No the actual problem is waiting instead of working. And it's cost can change significantly for local only, shared and multi-node exclusive locks. Suggest another shape for this part.

What it costs. Two nodes only, one run per point, zero write cost, free restarts, global reported without a number, a lock on a departed node is lost without notifying the holder, the age verdict can lose a request to a cycle that has dissolved, ecall's backpressure caveat is inherited. No need to mention ecall's backpressure here.

Takeaways. Count the round trips. A lock engine's ceiling is latency times concurrency. Batching plus random order is a deadlock generator. Lock the owner once, the keys locally. Locks that die with the holder are a free standby. Link to the repository and its suite. We need to reshape it a little. The main idea: waiting is a lost time. Local only and/or shared locks are much cheaper, avoid expensive multi-nodes exclusive locks where possible or make the as reare as possible. Avoid long lock chains and potential deadlocks as they are very expensive even for a single node. What else?

Headroom in design 2. Accept yours

Two design-1 elock runs. Take with the higher throughput only, don't mention the .

Ready to scale adding more nodes. Keep it but without real numbers. Base on the assumption that as all locks are local only then we extend it.

Failover wording. Let's describe it describe it as the retry loop the service runs.

Title. Let's try to wrap it around expensive waiting on locks. `5.5M locks per second on two nodes` also deserves mentioning. 



`and locks those keys locally and shared on every write`. Because it still needs sync with the ocasianol rare writers.

Lede, three candidates. 2 has good statements but 1 has numbers. Try to combine them based on 2. 

Title, three candidates in the forum's style. Accept 2

The third rung. The data has no exclusive local point, so the price list shows two rungs. A single-node run of design 1's workload would give the middle rung, and it is a run of yours. Otherwise the list stays at two.
Single-node evidence for chains. The traced single-node runs from October 6 put the latency in queues behind chains of 6 to 26 and deadlocks at the root of 27 to 61 percent of the waiting at 100 locks per transaction. One sentence of that would carry the "even on one node" claim in the problem section, if you want material from outside the 2nodes set used.
No need to mention, it will overload the article

Prepare the article draft in docs/article.html. With numbers and diagrams. Take a look at /home/roman/PROJECTS/SOURCES/ecall/docs/article.md
