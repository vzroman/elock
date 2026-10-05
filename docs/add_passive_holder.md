# Passive holders — spec

Status: draft, 2026-10-01. Not implemented.
Consumer: zaya two-phase commit (see `docs/add_passive_holder.md` in the zaya repository).

## 1. Purpose

Today a lock belongs to the process that took it. When that process unlocks or exits, the manager releases the lock at once.

Some callers need the lock to outlive its owner. In zaya's two-phase commit, each participating node writes uncommitted data before the decision is known. If the owner dies in that window, its locks are released while the data is still in doubt, and another transaction can read and overwrite it.

A **passive holder** is a second process that keeps one granted lock alive on one node. The lock is released on that node only when its owner has released it **and** every passive holder has exited.

## 2. API

Two new functions in `elock.erl`.

### 2.1 `elock:get_managers(Ref)`

```erlang
-spec get_managers(reference()) -> #{node() => pid()}.
```

Returns the manager of the lock `Ref` on each node it was taken on.

- Must be called by the process that took the lock. The map is read from that process's lock context (`#lock.nodes` in `elock_context`).
- Returns `#{}` for a reference that is unknown, already unlocked, or taken by another process.
- The map is the one recorded when the lock was granted. A manager in it may have died since.

### 2.2 `elock:add_passive_holder(Manager, Ref)`

```erlang
-spec add_passive_holder(pid(), reference()) -> ok | {error, not_held}.
```

Makes the calling process a passive holder of the lock `Ref` at `Manager`.

- `ok`: the manager now keeps the lock until the caller exits, whatever the owner does.
- `{error, not_held}`: `Ref` does not hold the lock at this manager, or the manager is dead. Nothing was changed.
- The call is synchronous. It returns only after the manager has answered or died. There is no timeout.
- Calling it again for the same `Ref` returns `ok` and changes nothing.
- A passive holder stops holding only by exiting. There is no call to remove it.

`Manager` and `Ref` are passed to the passive holder by the owner. A passive holder is expected to run on the manager's node. It works from another node as well, but then a lost connection ends the hold (see §3).

## 3. Semantics

1. **What can be held.** Only a granted request. A request that is still waiting, or a reference the manager does not know, gets `{error, not_held}`.
2. **One node at a time.** A passive hold is registered at one manager, so it covers one node. A lock taken on several nodes needs a passive holder at each manager where it must survive.
3. **Release rule.** The manager releases the request when both are true:
   - the owner has released it, by `unlock/1` or by exiting;
   - it has no passive holders left.
4. **While passively held, nothing else changes.** The request stays a holder in the same mode, shared or exclusive. It admits and blocks other requests exactly as before.
5. **The owner's side is unchanged.** `unlock/1` returns `ok` and the owner's context forgets the lock, as today. From then on the owner is an ordinary client of that term: a new request from it for the same term waits like anyone else's.
6. **A passive holder leaves by exiting.** Any exit reason counts. If the holder is on another node and the connection is lost, the manager sees it as an exit.
7. **Several passive holders** may hold the same request. The owner may add itself.
8. **An owner-released request can still take new passive holders.** The lock is still held, so the call returns `ok`.

## 4. Manager changes (`elock_manager.erl`)

### 4.1 Protocol

A new message from the passive holder to the manager. It is not subject to ticket ordering and is handled as it arrives.

```erlang
-record(add_passive_holder,{
  ref :: reference(),
  holder :: pid(),
  tag :: reference() % the holder's monitor of the manager, tags the reply
}).
```

The manager answers `?reply(Tag, ok)` or `?reply(Tag, {error, not_held})`.

The calling side, in the same module as the rest of the client–manager protocol:

```erlang
add_passive_holder(Manager, Ref)->
  MonitorRef = erlang:monitor(process, Manager),
  ecall:send(Manager, #add_passive_holder{ref = Ref, holder = self(), tag = MonitorRef}),
  receive
    ?reply(MonitorRef, Result)->
      erlang:demonitor(MonitorRef, [flush]),
      Result;
    {'DOWN', MonitorRef, process, Manager, _Reason}->
      {error, not_held}
  end.
```

### 4.2 State

Two new fields in `#req{}`, both with defaults so `init/1` and `new_req/1` need no change:

```erlang
passive_holders = #{} :: #{pid() => true},
owner_released = false :: boolean() % the owner has unlocked or exited
```

A request with `owner_released = true` is called **orphaned** below. It is in `#state.holders` and `#state.requests`, and in no `#client.requests`.

### 4.3 Monitor

One monitor per passive holder per request, tagged so the exit is routed without a lookup:

```erlang
erlang:monitor(process, Holder, [{tag, {passive, Ref, Holder}}])
```

The exit arrives as `{{passive, Ref, Holder}, MonitorRef, process, Pid, Reason}`. It needs its own clause in `loop/1`, placed before the catch-all. The existing `{'DOWN', ...}` clause does not match it.

These monitors are never removed explicitly. A request is released only after all its passive holders have exited, so none is left behind.

### 4.4 Transitions

| Event | Condition | Action |
|---|---|---|
| `#add_passive_holder{}` | `Ref` is in `requests` with `has_lock = true`, holder not registered | Monitor the holder, add it to `passive_holders`, reply `ok` |
| | Same, holder already registered | Reply `ok`, no second monitor |
| | Anything else | Reply `{error, not_held}` |
| Owner release: `#unlock{}` or the client's `'DOWN'` | Holding request, `passive_holders` empty | As today |
| | Holding request, `passive_holders` not empty | Orphan the request (§4.5) |
| | Waiting request | As today |
| `#unlock{}` | Request already orphaned | Ignore |
| Passive holder exit | Other passive holders remain, or the owner has not released | Remove the holder from `passive_holders` |
| | Last passive holder of an orphaned request | Final release (§4.6) |
| | `Ref` not in `requests` | Ignore |

### 4.5 Orphaning a request

- Remove `Ref` from the owner's `#client.requests` with `remove_client_request/3`. This drops the client entry and its monitor when it was the client's last request.
- Set `owner_released = true` and store the request back in `requests`.
- Leave `holders` and `can_share` as they are.

No waiter can be granted by this step, because the set of holders did not change.

### 4.6 Final release of an orphaned request

- Remove `Ref` from `holders` and `requests`.
- Recompute `can_share` as `unlocked/2` does.
- Do no client bookkeeping: the owner's entry is already gone.
- Call `next/1`. It grants the waiters, or ends the round through `try_unlock/1` when nobody is left.

### 4.7 Code that assumes a holder has a live client entry

These paths must be made aware of passive holders:

- **`handle_unlock/2`, the fast path.** The clause for "the only holder, no queue" calls `try_unlock/1` directly. It must not be taken when the request has passive holders.
- **`handle_down/2`.** It removes every request of the dead client. A holding request with passive holders must be orphaned instead.
- **`remove_request/2` and `unlocked/2`.** `unlocked/2` calls `remove_client_request/3`, which does `maps:get(ClientPID, Clients)`. For an orphaned request that key is gone, so the final release must not go through it unchanged.
- **`leave_lock/2`.** An `#unlock{}` for an orphaned request must be ignored. Otherwise a repeated unlock would release a lock that is still passively held.

Code that needs no change, and why:

- `only_holder/3` and `client_holds_lock/2` count a client's own requests against `holders`. An orphaned request is in `holders` and belongs to no client, so it is counted as someone else's hold. An upgrade waits for it, which is correct.
- `try_unlock/1` resets the state only when there are no holders, so no passive holder exists at that point.
- `elock_graph` tracks waiting requests only. Passive holds add no edges.

## 5. Client side changes

- `elock_context:get_managers/1`: read the context without changing it and return `#lock.nodes` for `Ref`, or `#{}`.
- `elock.erl`: export both functions with `-doc` and `-spec`.

## 6. Limitations

- **Release by exit only.** The lock stays as long as a passive holder is alive. A holder that never exits keeps the lock forever, and requests without a timeout wait forever. Bounding the holder's lifetime is the caller's responsibility.
- **Deadlock detection does not see passive holds.** They are not in any process's held map. A passive holder that itself waits for a lock can be part of a cycle that is not detected. A passive holder should not take locks.
- **A crashed manager loses the lock**, as today, and the passive holders are not notified.
- **A passive holder on another node** loses the hold when the connection to the manager's node is lost.

## 7. Performance

Requests that have no passive holders pay one extra field check on release.

Cost of the new calls, measured on one machine (OTP 27) with stand-in manager processes that use the manager's spawn options. Treat the figures as order of magnitude.

| Operation | Cost per lock |
|---|---|
| Local `elock:lock/3`, for comparison | 4–10 µs |
| `add_passive_holder/2`, one call at a time | 2–3 µs |
| Same calls pipelined (send all, then collect) | about 1 µs |
| Release when the passive holder exits | about 1 µs |

The API in this spec makes one call at a time. A batch form that pipelines the calls is a possible later addition; it is not part of this spec.

Per passively held lock the manager handles two more messages (the request and the holder's exit) and keeps one more monitor.

## 8. Tests

In `test/functional`, single node unless noted.

Holding:
- A passive holder keeps an exclusive lock after the owner unlocks; a second client waits and is granted when the holder exits.
- The same after the owner is killed instead of unlocking.
- The same for a shared lock: other shared requests are still granted, an exclusive one waits.
- The passive holder exits first, the owner unlocks later: the lock is released on the unlock.
- Two passive holders: the lock is released after the second one exits.
- The owner is its own passive holder and exits: the lock is released.

Refusals:
- Unknown reference, an already unlocked reference, a waiting request, and a dead manager each return `{error, not_held}`.
- A manager that has started a new round for the same term refuses a reference of the previous round.

Bookkeeping:
- A repeated `add_passive_holder/2` returns `ok`, and one exit of the holder releases the lock.
- A repeated `unlock/1` of an orphaned request does not release it.
- After orphaning, the owner's new request for the same term waits and is granted when the passive holder exits.
- An upgrade by another shared holder waits for an orphaned shared request and is granted when it leaves.
- The manager exits and the scope's ETS entry is removed after the final release of the last holder.
- `get_managers/1` returns the managers for a local and for a multi-node lock, and `#{}` for an unknown reference.

Multi-node:
- A lock on two nodes, a passive holder on one of them, the owner's node is stopped: the lock stays on the holder's node until the holder exits.

Regression: the existing suites pass unchanged.

## 9. Documentation

Update the module documentation of `elock.erl`:

- The sentence "A lock belongs to the process that took it, and only that process can release it. The locks of a process are released ... when it exits" gains the passive holder exception.
- Add a "Passive holders" section with the rules of §3 and the limitations of §6.
