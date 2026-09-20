# leasework

**Distributed job scheduler on Kafka, Redis and Postgres.**

Clients submit jobs — run now, run later, retry on failure. Workers execute them under a
**fenced lease**, so a job that is delivered twice still takes effect once. Operators get
metrics, an SLO and alerts that say whether the system is keeping its promise.

> **Status: Phase 0 — scaffold.** The stack comes up, every process is healthy and scraped,
> and no job logic exists yet. Nothing below is claimed as working until its phase lands. See
> [docs/plan.md](docs/plan.md) for the sequence.

---

## The idea in one paragraph

Workers die mid-job. Processes freeze and come back believing no time has passed. Brokers
restart. The guarantee this project makes is that **every job runs, and no job takes effect
twice** — and that both halves are provable, not asserted.

Getting there means pushing every failure into "this might happen twice" and never into "this
might not happen at all", then making "twice" harmless. Two mechanisms do that work:

**A transactional outbox.** The job row and a "publish this" row are written in one Postgres
transaction. A relay loop reads the outbox and produces to Kafka. The alternative — insert,
then publish as a separate step — loses a job silently if the process dies in between, which
is exactly the failure this project claims to prevent.

**A fenced lease.** A worker claims a job for a bounded time and heartbeats to hold it. If it
dies, the claim expires on its own with nothing to clean up. But a TTL alone is not enough: a
worker that is merely *stalled* can wake up after its lease expired, still believing it owns
the job, and execute it a second time. So each claim also carries a monotonically increasing
**fence token**, and the terminal write is conditioned on it:

```sql
UPDATE jobs SET status = $3 WHERE id = $1 AND fence = $2
```

The stale worker is not stopped — it cannot reliably be. Its write simply matches zero rows.

This is **not** exactly-once. It is at-least-once delivery, at-most-once side effects for
fenced handlers, effectively-once outcomes.

---

## Architecture

```
POST /jobs --tx--> [jobs row] + [outbox row]
                        |
                   scheduler (relay loop)
                        |
                   Kafka jobs.ready (12 partitions, RF=3)
                        |
                   worker: claim -> fence token -> heartbeat -> execute
                        |
                   fenced write to Postgres
                        |
                   commit offset   <- last, deliberately
```

| Binary | Replicas | Role |
|---|---|---|
| `cmd/api` | N, stateless | HTTP: submit, get, list, cancel. Job + outbox in one transaction. |
| `cmd/worker` | N, scales | Kafka consumer. Claim, heartbeat, execute, fenced write, commit. |
| `cmd/scheduler` | **1** | Outbox relay, due-job promoter, lease reaper. |

**Postgres is the only source of truth.** Wipe Redis and the system stays correct, just slower
for a few seconds. Wipe Kafka and you replay from Postgres. Wipe Postgres and work is lost.

---

## Running it

Requires Docker. Go is only needed if you want to build outside a container — the `make`
targets shell out to a `golang` image, so a local toolchain is optional.

```bash
make up
```

```bash
make topics
```

```bash
make smoke
```

| Service | URL |
|---|---|
| API | http://localhost:8080 |
| Prometheus | http://localhost:9090 |
| Grafana | http://localhost:3000 |

`make help` lists every target. `make down` stops the stack; `make clean` also drops volumes.

Each binary serves `/healthz`, `/readyz` and `/metrics` — api on `8080`, worker on `9101`,
scheduler on `9102`. These exist from the first commit rather than being retrofitted later,
because readiness semantics are the thing graceful shutdown depends on.

## Configuration

Environment variables, all prefixed `LEASEWORK_`:

| Variable | Default |
|---|---|
| `LEASEWORK_LOG_LEVEL` | `info` |
| `LEASEWORK_OPS_ADDR` | per service: `:8080` / `:9101` / `:9102` |
| `LEASEWORK_POSTGRES_DSN` | local Postgres |
| `LEASEWORK_REDIS_ADDR` | `localhost:6379` |
| `LEASEWORK_KAFKA_BROKERS` | `localhost:19092,localhost:29092,localhost:39092` |
| `LEASEWORK_SHUTDOWN_TIMEOUT` | `20s` |

---

## Documentation

- [docs/design-decisions.md](docs/design-decisions.md) — what was decided, what it beat, and
  what it costs.
- [docs/plan.md](docs/plan.md) — the phase sequence, from scaffold to Kubernetes autoscaling.

## Known gaps

Listed deliberately. Naming them is what makes the rest credible.

- Single `scheduler` replica, no leader election.
- Single Redis instance — no Sentinel, no Cluster.
- No Kafka transactions / EOS.
- No RBAC beyond API keys.
- `kind` is not a real cluster: no CNI, storage or upgrade experience.
- No recurring schedules.
