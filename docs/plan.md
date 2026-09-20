# leasework — implementation plan

Self-contained: a fresh session should be able to work from this file alone.

---

## 0. Identity

| | |
|---|---|
| **Name** | `leasework` |
| **Module path** | `github.com/jianrontan/leasework` |
| **Subtitle** | Distributed job scheduler on Kafka, Redis and Postgres |

The name describes the core mechanism: work is claimed under a lease, and the lease is fenced.

The locked design decisions live in [design-decisions.md](design-decisions.md). Read that
first — this file is the sequence, that file is the reasoning.

---

## 1. Architecture

**Three binaries:**

| Binary | Replicas | Job |
|---|---|---|
| `cmd/api` | N (stateless) | HTTP: submit, get, list, cancel. Writes job + outbox in one tx. |
| `cmd/worker` | N (scales) | Kafka consumer. Claim → heartbeat → execute → fenced write → commit. |
| `cmd/scheduler` | **1** | Three singleton loops: outbox relay, due-job promoter, lease reaper. |

**Flow:**

```
POST /jobs ──tx──> [jobs row] + [outbox row]
                        |
                   scheduler (relay loop, LISTEN/NOTIFY + 1s fallback tick)
                        |
                   Kafka jobs.ready (12 partitions, RF=3)
                        |
                   worker: claim.lua -> fence token -> heartbeat @ TTL/3
                        |                           -> execute under context.WithTimeout
                        |
                   UPDATE jobs SET status=$3 WHERE id=$1 AND fence=$2
                        |
                   commit offset
```

The offset is committed **last**, on purpose. Committing before the durable write would turn a
crash into a silent loss; committing after turns it into a redelivery, which the fence makes
harmless.

Delayed jobs skip the outbox: they are written with `run_at` in the future and status
`scheduled`, and the promoter loop moves them into the outbox when due.

**Redis keys:**

| Key | Type | Purpose |
|---|---|---|
| `lease:{job_id}` | string + TTL | Current owner |
| `fence` | counter | Global `INCR`, monotonic |
| `due` | ZSET (score = `run_at` ms) | Hot index of the due-soon window, rebuildable |
| `ratelimit:{tenant}` | token bucket | Per-tenant API quota |

**Lua scripts:** `claim.lua`, `renew.lua`, `release.lua`, `pop_due.lua`. All four need both a
`miniredis` unit test and a real-Redis integration test — their Lua semantics differ.

**Postgres:**

- `jobs` — id, type, payload jsonb, status, run_at, attempt, max_attempts, timeout_seconds,
  **fence**, lease_owner, lease_expires_at, idempotency_key UNIQUE, timestamps, last_error.
- `outbox` — id, job_id, topic, key, payload, created_at, published_at NULL, with a partial
  index on `published_at IS NULL`.

Status machine, written down as an enum plus allowed transitions:

```
pending | scheduled -> queued -> running -> succeeded
                                         -> failed -> (retry) queued
                                                   -> dead
                                         -> cancelled
```

---

## 2. Infrastructure choices

**Kafka: 3 brokers in KRaft mode, RF=3, `min.insync.replicas=2`.** The highest-value deviation
from the reference project, which runs one broker at RF=1 and lists that as a known gap. Three
brokers cost a few lines of Compose and turn "I have never seen a broker fail" into a chaos
demo.

**`jobs.ready` gets 12 partitions.** The KEDA demo scales workers 1 to 6 or 1 to 8, and
parallelism is capped at partition count. At 6 partitions the throughput graph plateaus and
looks like a bug in the screenshot. `jobs.dlq` gets 3.

**Partition key is `job_id`**, with an optional client-supplied `ordering_key`. Never key by
job type: it is low-cardinality, so you get hot partitions and one popular type serialising
behind itself.

**kind with 3 nodes**, not the default 1. One config line, and it makes drains, `cordon`,
rescheduling and anti-affinity real instead of invisible.

**Libraries:** `franz-go` (pure Go, no cgo — `confluent-kafka-go` needs librdkafka and is
painful on Windows), `go-redis` v9, `pgx` + `goose`, stdlib `net/http`,
`prometheus/client_golang`, `log/slog`, `testcontainers-go` + `miniredis`.

---

## 3. Phases

Each phase ends runnable. The ordering exists so you are never debugging more than one new
thing at a time.

**Phase 0 — Scaffold.** Module init, Compose (3 Kafka brokers, Redis with `--appendonly yes`,
Postgres), env config, `log/slog` JSON, Makefile. Every binary gets `/healthz`, `/readyz`,
`/metrics` and SIGTERM handling from the first commit — not Phase 7. Prometheus scrapes and
Grafana provisions from files, day one.

**Phase 1 — Walking skeleton with the outbox.** `POST /jobs` into one transaction writing job +
outbox, then the relay loop (`LISTEN/NOTIFY` for immediacy, 1s tick as fallback), produce,
worker consumes, no-op handler, mark succeeded, manual offset commit. No lease, no delay, no
retry, no DLQ. One Grafana panel showing jobs/sec.
**Exit: `make up && make smoke` green.** Everything after this diffs against a working baseline.

**Phase 2 — Durable job model.** Full schema, goose migrations, the status state machine as
code. Idempotency-Key backed by the unique constraint: replay returns the existing job id,
same key with a different body returns 409. `GET /jobs/{id}`, list with pagination.

**Phase 3 — Claim, lease, fence.** `claim.lua` (claim if unheld, set TTL, `INCR` fence, return
token), `renew.lua` (extend only if token matches), `release.lua`. Worker loop: claim,
heartbeat goroutine at TTL/3, execute under `context.WithTimeout`, fenced terminal write,
commit. Graceful shutdown deepens here: SIGTERM, stop polling, finish in-flight, release
lease, commit offsets.

**Phase 4 — Reaper and the proof.** Reaper requeues rows where `lease_expires_at < now()` and
status is `running`. Then `make demo-fence` produces the README's two numbers: duplicate
executions with fencing off versus on. The duplicate is real, not manufactured — a handler
running longer than `max.poll.interval.ms` gets its consumer evicted, the job is redelivered,
and both copies run. Write `docs/failure-modes.md` while it is fresh: crash before claim,
after claim, mid-execute, after execute before the write, after the write before the commit,
during rebalance. One row each.

**Phase 5 — Delayed execution.** `run_at` in Postgres is truth. Scheduler loads the due-soon
window into the `due` ZSET on boot, ticks every second, pops atomically (`ZRANGEBYSCORE` plus
`ZREM` in one script), writes to the outbox, marks promoted. A reconciliation loop re-adds
anything in Postgres that is overdue and unpromoted — that is what makes the
**flush-Redis-and-jobs-still-run** test pass. Add `scheduler_last_tick_timestamp` with a
staleness alert.

**Phase 6 — Retry, DLQ, cancel.** Exponential backoff with jitter; attempt count and
next-attempt time in Postgres, not Kafka. Exhausted jobs go to `jobs.dlq` with diagnostic
headers. Poison payloads (unmarshalable, unknown type) go straight to the DLQ; transient
failures retry. Cancel sets status in Postgres — you cannot unsend a Kafka message — and the
worker checks before executing and on each heartbeat, cancelling the handler's context
cooperatively.

**Phase 7 — Observability and SLO.** Full metric set:

```
leasework_jobs_submitted_total{type}
leasework_jobs_executed_total{type,outcome}
leasework_job_execution_duration_seconds{type}     histogram
leasework_scheduling_delay_seconds{type}           histogram   <- the SLI
leasework_lease_expirations_total
leasework_fence_rejections_total                   <- the star metric
leasework_retries_total / leasework_dlq_total
leasework_outbox_pending                           gauge
leasework_scheduler_last_tick_timestamp_seconds    gauge
leasework_consumer_lag{partition}                  gauge
leasework_jobs_inflight                            gauge
```

`fence_rejections_total` is the best metric in the project: non-zero is proof the mechanism
did its job. **Never label anything by `job_id`** — and spend an hour deliberately doing it
once, watching Prometheus suffer, then reverting. Best cardinality lesson there is.

SLI is `started_at - run_at`. Pick histogram buckets deliberately
(0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300) — you cannot re-bucket retroactively.
SLO: **99% of jobs start within 5s of their scheduled time.**

Then alert rules — `SchedulerStalled`, `OutboxBacklog`, `ConsumerLagHigh`, `SLOBurn`,
`DLQNonZero` — and `docs/runbook.md` with one section per alert. **This is the tier that beats
the reference project, which has dashboards and no alerting. Do not let it slip for time.**

**Phase 8 — Load, chaos, numbers.** Throughput at 1/3/6 workers; p50/p95/p99 scheduling delay;
backlog drain rate. Chaos, each with recovery behaviour and measured time: kill a worker
mid-job, kill Redis, **kill one Kafka broker** (meaningful at RF=3), stop the scheduler. README
gets real numbers and dashboard screenshots.

**Phase 9 — Kubernetes.** 3-node kind. Deployments with probes and resource limits, PDB,
`preStop` plus `terminationGracePeriodSeconds` wired to the Phase 3 graceful drain, scheduler
pinned to one replica and explicitly excluded from autoscaling. KEDA scales workers on
consumer lag: load up, lag up, replicas up, lag drains. Then drain a node and watch pods
reschedule. That demo is the entire reason this phase exists.

---

## 4. Answers to have ready

1. **"Why not `SKIP LOCKED`?"** — See design-decisions.md; Kafka is justified, not necessary.
2. **"Isn't a lease enough?"** — No. A stalled worker does not know it lost the claim. Fence
   tokens close that, and `fence_rejections_total` proves it fires.
3. **"Exactly once?"** — At-least-once delivery, at-most-once fenced side effects,
   effectively-once outcomes.
4. **"What's the cost of a log here?"** — Head-of-line blocking per partition. A pull queue
   does not have it.
5. **"Have you operated Kafka?"** — "As a client, at depth. I have run three brokers at RF=3
   and killed one. I have not run a production cluster." Bluffing is not the move.
