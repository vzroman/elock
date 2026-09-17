# Deadlock detection review (draft)

Review of `src/elock_deadlock.erl` and its wiring in `src/elock.erl`, with the
focus on multi-node requests. Findings are from code reading only: the current
test suite never starts a checker (every test holds nothing and locks a single
node, so it hits the `can_not_have_deadlocks` shortcut), and there is no
multi-node test.

## What holds up

- Neighbour subscription order: join `{holder,H,{T,N}}`, wait for the join to be
  visible on `N` (`wait_local`), then query `registered_locks` on `N`. Any
  registration before the query is found; any after it is notified. No gap.
- A cycle that completes on an *acquisition* (not on a new wait) is caught:
  `register_lock` sends `add_held_lock`, the checker monitors the wait group of
  the new term and probes its members.
- The holder-based wait-for model is sound for shared queues: a blocked shared
  waiter always transitively depends on every current holder, and every queue
  neighbour it waits for also depends on those holders.
- `pg:monitor/2` returns the member list and subscribes atomically, so there is
  no window between the initial member list and the first join notification.

## Bugs

### 1. Victim selection is not antisymmetric

`src/elock_deadlock.erl:113-132`, clause `{deadlock_detected, From, Locks}`.

When the opponent holds strictly more locks the code falls through to the hash
comparison instead of yielding. In a two-party cycle both checkers detect and
both send `deadlock_detected`, so:

- heavier side: `length(Held) > length(Locks)` → "you yield";
- lighter side: falls into `phash2` comparison → tells the heavier side to
  yield about half of the time.

Result: both requests abort roughly half the time. With identical held sets
(the shared→exclusive upgrade deadlock) both sides always yield. A `phash2`
collision has the same effect. Held sets can also change between the two
decisions while an `add_held_lock` is in flight, so even a corrected length
comparison can disagree.

Fix: carry the holder pid and the request-time held count in
`check_deadlock` / `deadlock_detected`, and compare the immutable pair
`{Count, Holder}` on both sides. Drop `phash2`.

### 2. Transitive wait memberships are never left → false deadlocks

`src/elock_deadlock.erl:90`: `pg:join(Scope, ?wait(ItsWaitTerm), self())`.
Nothing ever leaves the group; it lives as long as the checker.

```
1. A holds X, waits Y.   B holds Y, waits Z.
   CB probes CA  →  CA joins wait(Z)          (A transitively waits for Z)
2. B gets Z, CB stops.  B releases Z, keeps Y. A still waits for Y.
   CA is still in wait(Z) — stale.
3. D takes Z, requests X (held by A).
   CD monitors wait(Z) → finds CA → probes it with wait term X.
   CA: X ∈ held  →  "deadlock"  →  A or D gets {error, deadlock}.

   Real graph: D → A → B, B waits for nothing. No cycle.
```

Nested lock/unlock patterns (`lock(Y) … lock(Z) … unlock(Z) … unlock(Y)`)
make this ordinary.

Fix: remember which checker induced each transitive join (`From`), monitor it,
and leave the group when it goes down and no other inducer remains. Alive
inducers keep the membership valid; a stopped inducer means its holder is no
longer waiting, so the transitive edge is gone.

### 3. A yield releases nothing until every node of the request has returned

`src/elock.erl:70`: `ecall:call_all_wait` waits for all nodes; the acquired
lockers are unlocked only after it returns.

```
H  holds {T,n1}, waits {T,n2} (held by H2), waits {T,n3} (held by H3, unrelated)
H2 holds {T,n2}, waits {T,n1}
```

H loses, its n2 locker yields and `do_lock` on n2 returns `{error,deadlock}`,
but H keeps `{T,n1}` until the n3 call returns. With an infinite timeout H2
stays blocked for as long as H3 holds `{T,n3}`. Detection did its job and
nothing changed.

Fix: on the first failed node cancel the still-pending lockers. The caller
does not know their pids today; in the gen_statem port key them by the request
`Ref` (pg group or ETS) so the caller can reach them, then unlock the acquired
ones immediately.

### 4. The checker keeps deciding after its locker stopped waiting

`src/elock.erl:357-360`: `claim_next` sends `wait_share`, then `claim_wait`
spawns the checker. A shared request that is granted immediately via
`take_share` still probes. If a `deadlock_detected` reaches the checker before
the locker's `{stop, Locker}`, it may tell a genuinely waiting opponent to
yield. The same window exists after `{timeout, Ref}` and holder `DOWN`. The
locker's own state (`{deadlock, Deadlock}` handled only in `wait_lock`) guards
the self-yield case, not the opponent.

Low probability (needs a loaded scheduler), but real.

Fix in the port: the checker forwards the detection, the locker makes the final
decision as an event handled only in `wait_lock`; a checker whose locker has
left `wait_lock` must not send `{yield}`.

### 5. `remove_held_lock` is dropped, `add_held_lock` can arrive twice

- `src/elock_deadlock.erl:180` sends `{remove_held_lock, Term}`; the loop's
  catch-all at line 136 drops it. Today a neighbour only unlocks while the
  checker is still waiting on request-failure paths, so it is harmless, but the
  port should handle it: remove the term and `pg:demonitor` its wait group
  (keep the refs returned by `pg:monitor`, currently discarded).
- Duplicate: when the neighbour registers after the checker's join it appears
  both in the `registered_locks` result and as an `add_held_lock` message
  (`register_lock` reads pg members after the ETS insert). The second
  `add_held_lock/2` creates a second `pg:monitor`, so every later join is
  probed twice. Skip terms already in `held_locks`.

### 6. Silent checker failure

`src/elock_deadlock.erl:26`: the checker is `spawn`ed, the locker never monitors
it. If it crashes (pg scope down, `pg:join` failure) the locker waits with no
detection. Monitor it (or link + trap in the port).

## Limitations (by design, keep in mind)

- N-cycles: each detecting pair picks its victim independently, so up to N
  requests abort where one would do.
- Cross-scope cycles are invisible: each `Locks` scope has its own pg scope and
  `held` is filtered per scope.
- The upgrade path (`src/elock.erl:160-167`) has no checker; a holder blocked
  there is invisible to detection.
- Rolling upgrade: remote calls target `elock_deadlock` now. A node on old code
  fails `wait_local` / `registered_locks` and is treated as "not holding".

## Suggested next step

A two-node common test that reproduces items 1 and 2 before the gen_statem
port, so the fixes can be verified rather than reasoned about.
