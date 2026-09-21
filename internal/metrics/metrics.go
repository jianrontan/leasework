// Package metrics defines leasework's Prometheus collectors. Each is
// registered on the default registry at package init time via promauto, so
// the ops server's existing GET /metrics endpoint (backed by
// promhttp.Handler, which serves the default registry) exposes them without
// any further wiring.
//
// This is deliberately the full metric set for Phase 1, not a starting
// point: the complete inventory of counters, histograms and gauges leasework
// eventually needs is Phase 7's job. Adding metrics now that nothing
// populates would just be noise on the /metrics output.
package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

// JobsSubmitted counts jobs accepted by the API's POST /jobs handler.
var JobsSubmitted = promauto.NewCounter(prometheus.CounterOpts{
	Name: "leasework_jobs_submitted_total",
	Help: "Total number of jobs accepted by the API.",
})

// JobsExecuted counts job executions performed by workers, labeled by
// outcome: "succeeded" or "failed".
var JobsExecuted = promauto.NewCounterVec(prometheus.CounterOpts{
	Name: "leasework_jobs_executed_total",
	Help: "Total number of job executions, labeled by outcome.",
}, []string{"outcome"})

// OutboxPublished counts outbox rows the scheduler's relay loop has
// successfully handed to Kafka.
var OutboxPublished = promauto.NewCounter(prometheus.CounterOpts{
	Name: "leasework_outbox_published_total",
	Help: "Total number of outbox messages published to Kafka.",
})
