# Design decisions

Decisions locked before the first line of code, recorded while deciding rather than
reconstructed afterwards. Each one names the alternative it beat and the cost it carries.

---

## Postgres is the only source of truth

Redis and Kafka are both derived state.

| Wipe | Consequence |
|---|---|
| Redis | System stays correct. Slower for a few seconds while the due-window reloads from Postgres. |
| Kafka | Replay from Postgres via the outbox. |
| Postgres | Work is lost. |

Every piece of state has exactly one home, and that home is a durable, transactional one.
Redis is a cache and a coordination primitive; Kafka is a transport. Neither is ever asked a
question whose answer must be right.

## A transactional outbox bridges Postgres and Kafka

The job row and an outbox row are written in a single transaction. A relay loop reads
unpublished outbox rows and produces them to Kafka.

The alternative is a dual write: insert the job, then produce to Kafka as a separate step. A
crash in between leaves a job that exists but will never run, and nothing anywhere reports an
error. That silent loss is precisely the failure this project claims to prevent, so accepting
it in the submit path would make the entire premise dishonest.

Cost: one extra table, one extra loop, and end-to-end latency now includes a relay hop
(mitigated with `LISTEN/NOTIFY`, with a 1s tick as the fallback).

## Leases are fenced

A lease TTL alone does not prevent double execution. It narrows the window, and it leaves the
stalled worker unaware that it lost its claim.

The failure it misses: worker A claims a job and stalls (GC pause, suspended VM, partitioned
network). The TTL expires. Worker B claims the job and executes it. Worker A resumes, still
believing it holds the claim, and executes it again. Nothing about a timeout stops this.

So `claim.lua` also hands out a **fence token** from a monotonic Redis `INCR`, and every
terminal write is conditioned on it:

```sql
UPDATE jobs SET status = $3 WHERE id = $1 AND fence = $2
```

A stale worker's write affects zero rows and the worker learns it lost. The stalled process is
not stopped (it cannot reliably be); its output is made powerless at the last gate.

`leasework_fence_rejections_total` is the observable proof: non-zero means the mechanism
caught a real double-execution.

## The claim is not "exactly once"

It is **at-least-once delivery, at-most-once side effects for fenced handlers, effectively-once
outcomes**. Stated that way in the README, in interviews, and anywhere else the question comes
up. "Exactly once" is a claim this architecture does not support and nobody should make
casually.

## Kafka is justified, not necessary

At this scale `SELECT ... FOR UPDATE SKIP LOCKED` against Postgres would work. River does
exactly that in Go with no broker at all. Kafka earns its place on three counts:

1. Workers scale out without every replica polling Postgres.
2. Consumer lag is a single number meaning "falling behind", which drives both alerting and
   KEDA autoscaling.
3. Other consumers can read the job stream later without touching the worker path.

The cost is **head-of-line blocking per partition**: one slow job delays everything behind it
in its partition, which a pull queue does not do. Lead with this tradeoff rather than letting
an interviewer find it.

## Kafka and Kubernetes are a single decision

The KEDA autoscaling demo needs a lag signal, and the lag signal comes from Kafka. Dropping
the Kubernetes tier removes the best argument for Kafka being here at all. They stand or fall
together.

## The three control loops share one binary

The outbox relay, the due-job promoter and the lease reaper all live in `cmd/scheduler`, which
runs at exactly one replica.

All three are singleton Postgres tickers. Bundling them means **one** leader-election problem
instead of three. Splitting them into three binaries would triple the coordination surface for
no operational gain at this size. "Leader election via a Redis lease" is then honest future
work rather than a hole.

## Priority is out of v1

A partitioned log has no notion of priority. Doing it properly means a topic per priority
class plus starvation handling, and it triples the surface area of the autoscaling demo. It is
removed from the API surface entirely rather than left as a README bullet with nothing behind
it. Good first post-v1 extension.

---

## Deliberately cut

| Cut | Why |
|---|---|
| nginx | ingress-nginx in the Kubernetes tier tells the same load-balancing story for free. |
| Terraform + cloud deploy | READMEs get read; live URLs do not. A running Kafka cluster is a recurring bill. |
| Cron / recurring schedules | Misfire policy plus DST is its own subsystem. Ship one-shot delays. |
| Priority | See above. |

## Known gaps

Named here deliberately. Naming them is what makes everything else credible.

- Single `scheduler` replica, no leader election.
- Single Redis instance: no Sentinel, no Cluster.
- No Kafka transactions / EOS.
- No RBAC beyond API keys (hashed at rest).
- `kind` is not a real cluster: no CNI, storage or upgrade experience.
- No recurring schedules.
