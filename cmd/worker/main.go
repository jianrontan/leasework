// Command worker is the leasework worker binary: the tier that scales, run
// as N replicas. As a Kafka consumer it claims a lease, heartbeats,
// executes the job, performs a fenced terminal write and commits its
// offset. Phase 1 wires up the consume loop with a no-op job handler; the
// lease and fence token are added in Phase 3, real job handlers in Phase 6.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jianrontan/leasework/internal/config"
	"github.com/jianrontan/leasework/internal/kafka"
	"github.com/jianrontan/leasework/internal/logging"
	"github.com/jianrontan/leasework/internal/metrics"
	"github.com/jianrontan/leasework/internal/opsserver"
	"github.com/jianrontan/leasework/internal/store"
)

const serviceName = "worker"

// jobsReadyTopic must match the topic the outbox relay republishes onto
// (see internal/api/handlers.go's jobsReadyTopic), since that is the only
// place records on it are produced.
const jobsReadyTopic = "jobs.ready"

// workerConsumerGroup is the Kafka consumer group all worker replicas
// share, so each job record is delivered to exactly one of them.
const workerConsumerGroup = "leasework-workers"

// readinessPollInterval is how often the background goroutine re-checks
// Postgres and Kafka connectivity to keep /readyz current between requests.
const readinessPollInterval = 5 * time.Second

func main() {
	cfg, err := config.Load(serviceName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading config: %v\n", err)
		os.Exit(1)
	}

	logger := logging.New(cfg.LogLevel).With("service", serviceName)
	logger.Info("starting",
		"ops_addr", cfg.OpsAddr,
		"kafka_brokers", cfg.KafkaBrokers,
		"log_level", cfg.LogLevel,
		"postgres_configured", cfg.PostgresDSN != "",
	)

	connectCtx, cancelConnect := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	st, err := store.New(connectCtx, cfg.PostgresDSN)
	cancelConnect()
	if err != nil {
		logger.Error("connecting to postgres", "error", err)
		os.Exit(1)
	}

	consumer, err := kafka.NewConsumer(cfg.KafkaBrokers, workerConsumerGroup, []string{jobsReadyTopic}, logger)
	if err != nil {
		logger.Error("creating kafka consumer", "error", err)
		st.Close()
		os.Exit(1)
	}

	pingCtx, cancelPing := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	pingErr := consumer.Ping(pingCtx)
	cancelPing()
	if pingErr != nil {
		logger.Error("pinging kafka", "error", pingErr)
		consumer.Close()
		st.Close()
		os.Exit(1)
	}

	ops := opsserver.New(cfg.OpsAddr, logger)
	opsErr := ops.Start()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Readiness means both dependencies are reachable: this worker cannot
	// consume without Kafka or record results without Postgres. The poller
	// runs until ctx is cancelled at shutdown, mirroring the api binary's
	// pattern.
	readinessDone := make(chan struct{})
	go runReadinessPoller(ctx, st, consumer, ops, logger, readinessDone)

	consumeDone := make(chan struct{})
	consumeErr := make(chan error, 1)
	go runConsumeLoop(ctx, st, consumer, logger, consumeDone, consumeErr)

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-opsErr:
		if err != nil {
			logger.Error("ops server failed", "error", err)
			os.Exit(1)
		}
	case err := <-consumeErr:
		// Exit non-zero so the container restart policy replays from the
		// last committed offset. Logging and shutting down cleanly would
		// make a stalled consumer indistinguishable from a normal stop.
		logger.Error("consume loop failed", "error", err)
		consumer.Close()
		st.Close()
		os.Exit(1)
	}

	ops.SetReady(false)
	stop()
	<-readinessDone
	<-consumeDone

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := ops.Shutdown(shutdownCtx); err != nil {
		logger.Error("ops server shutdown failed", "error", err)
	}

	consumer.Close()
	st.Close()

	logger.Info("shutdown complete")
}

// runReadinessPoller pings st and consumer every readinessPollInterval and
// mirrors the combined result onto ops' readiness flag, so a Postgres or
// Kafka outage shows up on /readyz instead of staying invisible. It marks
// ready immediately on entry, matching the successful connect and ping
// already performed in main before this goroutine starts, then stops
// cleanly when ctx is cancelled.
func runReadinessPoller(ctx context.Context, st *store.Store, consumer *kafka.Consumer, ops *opsserver.Server, logger *slog.Logger, done chan<- struct{}) {
	defer close(done)

	ops.SetReady(true)

	ticker := time.NewTicker(readinessPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			pgErr := st.Ping(ctx)
			kafkaErr := consumer.Ping(ctx)
			if pgErr != nil || kafkaErr != nil {
				logger.Warn("readiness check failed", "postgres_error", pgErr, "kafka_error", kafkaErr)
				ops.SetReady(false)
				continue
			}
			ops.SetReady(true)
		}
	}
}

// runConsumeLoop runs the Kafka consume loop until ctx is cancelled, then
// closes done. A non-nil error after ctx has already been cancelled is the
// expected shutdown path, not a failure, so it is only logged when ctx is
// still live.
func runConsumeLoop(ctx context.Context, st *store.Store, consumer *kafka.Consumer, logger *slog.Logger, done chan<- struct{}, errCh chan<- error) {
	defer close(done)

	if err := consumer.Run(ctx, handleJob(st, logger)); err != nil && ctx.Err() == nil {
		errCh <- err
	}
}

// handleJob returns the per-record handler passed to Consumer.Run. The job
// id travels as the Kafka record key (see EnqueueJob and the outbox relay,
// which both key by job id), not the value, so every record for one job
// lands on the same partition and this handler never needs to parse the
// payload to find it.
func handleJob(st *store.Store, logger *slog.Logger) func(ctx context.Context, key, value []byte) error {
	return func(ctx context.Context, key, value []byte) error {
		jobID := string(key)

		if err := st.SetJobStatus(ctx, jobID, store.StatusRunning); err != nil {
			return fmt.Errorf("setting job %s running: %w", jobID, err)
		}

		// Phase 1's handler is a deliberate no-op: it exists to prove the
		// whole pipe end to end. Phase 3 adds the lease and fence token
		// around this point, Phase 6 adds real job handlers.
		logger.Info("executing", "job_id", jobID)

		if err := st.SetJobStatus(ctx, jobID, store.StatusSucceeded); err != nil {
			return fmt.Errorf("setting job %s succeeded: %w", jobID, err)
		}
		metrics.JobsExecuted.WithLabelValues("succeeded").Inc()

		// Returning nil here is what lets Consumer.Run commit this record's
		// offset, and it does so only after the Postgres write above has
		// already landed. A crash in between leaves the offset uncommitted,
		// so this record is redelivered on restart rather than lost. Phase
		// 3's fence token is what makes that redelivery harmless rather
		// than merely safe-by-luck (redoing a no-op is harmless either way,
		// but a real handler needs the fence to avoid double-executing
		// side effects).
		return nil
	}
}
