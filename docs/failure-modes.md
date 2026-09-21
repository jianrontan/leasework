# Failure modes

Every point where a worker can die, what recovers it, and what the user observes.

The governing rule: **every failure must produce a repeat, a delay, or nothing — never a
loss.** Losses are silent and undebuggable; repeats are visible and can be neutralised. Where
this document says a failure is "accepted", it means accepted deliberately, with the mechanism
that bounds it named.

Design rationale for the mechanisms themselves lives in [design-decisions.md](design-decisions.md).

---

## The worker loop

```
1. receive message        (partition P, offset N)
2. claim lease in Redis   -> fence token F
3. stamp Postgres          status=running, fence=F, lease_expires_at
4. execute handler         (heartbeat renews the lease at TTL/3)
5. fenced write            UPDATE jobs SET status=$3 WHERE id=$1 AND fence=$2
6. commit offset N+1
```

Two properties of this ordering carry most of the safety:

- **The offset is committed last.** Committing before the durable write would turn a crash
  into a silent loss. Committing after turns it into a redelivery, which step 5 makes
  harmless.
- **The terminal write is conditional.** `AND fence = $2` collapses "check I still own this"
  and "write the result" into one atomic statement, so there is no window between them.

---

## Crash points

### A — Before the claim (between 1 and 2)

Nothing has happened anywhere: no lease, no Postgres change, no commit.

Kafka redelivers offset N to whichever worker next owns partition P. The job runs once,
normally.

**Effect: no-op.** Indistinguishable from the message never having been read.

### B — Claimed but not stamped (between 2 and 3)

Redis holds a lease with fence F, but the `jobs` row still says `queued` and knows nothing
about it.

Only one of the two recovery paths covers this, which is worth noticing:

- The **reaper** looks for `status='running' AND lease_expires_at < now()`. This row is still
  `queued`, so the reaper never sees it.
- The **offset was never committed**, so Kafka redelivers regardless.

The orphaned Redis lease expires on its own TTL, so the job is not stuck locked. The
redelivered message is claimed fresh with a new fence.

**Effect: one clean execution.**

> Kafka redelivery and the lease reaper cover overlapping but different ground. Neither alone
> is sufficient, which is why both exist.

### C — Mid-execution (during 4)

Postgres says `running` with fence F and a lease expiry. The heartbeat stops.

Both safety nets fire. The lease expires; the reaper finds `running` plus an expired lease and
requeues. The uncommitted offset also causes redelivery. The job is re-executed from the
beginning under a new fence.

**Effect: retry.** Any external side effect completed before the crash has already happened —
see [Handler contract](#handler-contract).

### D — Work done, result not written (between 4 and 5)

The job genuinely ran and there is no record of it.

From the system's perspective this is indistinguishable from C: Postgres still says `running`,
the lease still expires, the offset is still uncommitted. It is retried, and the work happens
**twice**.

**Effect: duplicate execution. Not solved — bounded.**

This is irreducible. The handler's effect and the database write are two separate operations
across a process boundary and cannot be made atomic. It is the reason this project claims
at-least-once delivery rather than exactly-once, and the reason handlers must be idempotent.

Fencing does **not** help here. Fencing protects the database row; it cannot un-send an email.

### E — Written but not committed (between 5 and 6)

Kafka still points at offset N and redelivers it.

The redelivered message hits a dedup check: the worker reads the job row, sees a terminal
status, skips execution and commits the offset.

**Effect: duplicate delivery, zero duplicate execution.**

> Why check-then-act is safe *here* but not for the lease: a terminal status is final.
> `succeeded` can never revert to `running`. The value moves in one direction only, so there is
> no race to lose. Lease ownership *can* change underneath a reader, which is exactly why that
> case needs a fenced write instead of a check.

### F — Rebalance mid-job

Not a crash. The worker is healthy, which is what makes this the interesting case.

A consumer must call `poll()` at least every `max.poll.interval.ms`. A handler that runs longer
blocks the poll loop, Kafka concludes the worker is gone and **evicts it from the group while
it is still working**. The partition is reassigned, the new owner reads offset N, and both
copies run concurrently. When the original finishes, its commit is rejected — it is no longer a
group member.

Its fenced write lands on a row whose fence has since advanced, affects **0 rows**, and
`leasework_fence_rejections_total` increments.

**Effect: concurrent duplicate execution, exactly one recorded outcome.**

This is a genuine duplicate produced by ordinary configuration rather than an artificial kill,
which is why `make demo-fence` reproduces it this way. A manufactured duplicate — calling the
handler twice in a loop — proves nothing.

### G — Redis unavailable

No claims can be granted, so workers stall. Nothing is lost: Postgres holds the truth and Kafka
holds the messages. Work resumes on recovery.

**Effect: delay.**

One real hazard: the `fence` counter lives in Redis. If Redis returned **empty**, `INCR` would
restart at 1 and reissue tokens already in use. Fence values would regress and stop being a
reliable "who is newer" signal, which is the single property fencing depends on.

This is the concrete reason Redis runs with `--appendonly yes`. It is not generic durability
hygiene; it protects the one piece of Redis state that must never go backwards.

### H — Scheduler down

The outbox relay, due-job promoter and lease reaper all stop.

New submissions are still durably accepted — the API writes the job and outbox rows in one
transaction and returns. They simply are not published until the scheduler returns. In-flight
work continues; expired leases are not reaped until it returns.

**Effect: delay, with a growing outbox backlog.** Detected by
`leasework_scheduler_last_tick_timestamp_seconds` going stale and by `OutboxBacklog`.

---

## Summary

| # | Crash point | Recovered by | Net effect |
|---|---|---|---|
| A | Before claim | Kafka redelivery | No-op |
| B | Claimed, not stamped | Kafka redelivery (reaper cannot see it) | One execution |
| C | Mid-execution | Reaper **and** redelivery | Retry |
| D | Done, not written | Reaper and redelivery | **Runs twice** |
| E | Written, not committed | Redelivery + status dedup | Duplicate delivery only |
| F | Rebalance mid-job | Fence rejects the stale write | Duplicate run, one result |
| G | Redis down | Stall, resume; AOF protects the counter | Delay |
| H | Scheduler down | Resumes on restart | Delay |

Reading down the last column: repeats, delays, and no-ops. **No losses.**

---

## Why heartbeating does not remove the need for fencing

A recurring and reasonable objection to F: if a long job heartbeats while it runs, a frozen
worker stops heartbeating and is detected quickly, so why can it still be evicted wrongly?

Kafka already implements exactly that idea, and then adds a second timeout anyway:

| Setting | Default | Runs on | Proves |
|---|---|---|---|
| `heartbeat.interval.ms` / `session.timeout.ms` | 3s / 45s | background thread | the process is alive and reachable |
| `max.poll.interval.ms` | 5 min | main processing loop | the handler is actually returning |

The background heartbeat is independent of handler duration. The second timeout exists because
**a heartbeat from a background thread proves the wrong thing**: it proves the process is
scheduled and the network works, not that the job is progressing. A deadlocked handler
heartbeats happily forever while its partition never advances.

Three cases survive any heartbeat frequency:

1. **Heartbeat on the wrong thread** — above. Moving it onto the job thread fixes that but
   reintroduces a deadline for any single uninterruptible operation.
2. **Network partition** — the worker is healthy and progressing, but the path to Kafka is cut.
   Heartbeats are sent and never arrive. Kafka evicts; the new owner starts the job; the
   original keeps working, unaware. Heartbeating cannot fix this because the heartbeat is
   precisely what is broken.
3. **Whole-process pause** — stop-the-world GC, VM migration, `SIGSTOP`, a closed laptop lid.
   The heartbeat thread pauses too. The process resumes mid-job with no idea time has passed.

Underneath all three: **over an unreliable network, a crashed process is indistinguishable from
a slow or unreachable one.** The absence of a message is ambiguous, always. Every failure
detector is therefore a guess with a false-positive rate.

Raising the timeout does not remove the tradeoff, it slides along it:

```
short timeout  ->  evicts healthy slow workers  ->  duplicates
long timeout   ->  tolerates genuinely dead workers  ->  silent stalls
```

Hence the division of labour:

- **Heartbeating is an optimisation.** It makes duplicates rare.
- **Fencing is a correctness mechanism.** It makes duplicates harmless.

They are not substitutes. Heartbeat only, and the bad case is rare — which at volume means
daily. Fence only, and every slow job triggers an eviction and constant double work.

---

## Handler contract

What the platform guarantees, and where the handler must carry its own weight.

**Guaranteed:** every job is delivered at least once; exactly one terminal outcome is recorded
per job, even under concurrent execution; a lost worker is detected and its work requeued.

**Not guaranteed:** that a handler body runs only once (D and F), or that external side effects
are deduplicated.

So handlers must be **idempotent**, which shows up in two distinct places:

- **Inbound** — a client retrying `POST /jobs` after a timeout. Handled by the
  `Idempotency-Key` header and a unique constraint: the same key returns the existing job id;
  the same key with a different body returns 409.
- **Outbound** — a handler retrying a call to a third party. The handler must supply that
  API's own idempotency key. The platform cannot do this on its behalf.

Prefer naturally idempotent operations (`SET balance = 100` over `balance = balance + 10`)
wherever the choice exists.

---

## Rejected mitigations

**Falling back to Postgres when Redis is down.** Under a *partial* Redis outage some workers
would fail over while others did not, producing **two independent fence counters** with no
ordering between them. Monotonicity is global or it is nothing, and losing it is a correctness
bug, not a degradation. The fallback path would also be the least-exercised code in the system
at precisely the moment it matters. The design fails closed instead: no claims, work stalls,
nothing is lost.

Legitimate alternatives, both honest to state:

- Redis Sentinel or Cluster for failover — currently a [known gap](design-decisions.md#known-gaps).
- Move the fence counter to a Postgres `SEQUENCE`, which is durable and monotonic by
  construction, keeping only the lease in Redis. This deletes the counter-reset hazard entirely
  at the cost of more load on Postgres.

**Raising `max.poll.interval.ms` far enough to never evict.** Converts a visible duplicate into
an invisible stall; see above.
