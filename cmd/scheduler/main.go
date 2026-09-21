// Command scheduler is the leasework scheduler binary: exactly ONE replica,
// running three singleton loops: the outbox relay, the due-job promoter
// and the lease reaper. Phase 1 adds the first of those, the outbox relay,
// which republishes durably-written outbox rows onto Kafka; the promoter
// and reaper are added in later phases.
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

const serviceName = "scheduler"

// readinessPollInterval is how often the background goroutine re-checks
// Postgres and Kafka connectivity to keep /readyz current between requests.
const readinessPollInterval = 5 * time.Second

// relayBatchSize is the maximum number of outbox rows fetched per relay
// pass.
const relayBatchSize = 100

// relayPollInterval bounds how long the relay loop waits for an
// outbox_new notification before it falls back to fetching anyway.
const relayPollInterval = time.Second

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

	producer, err := kafka.NewProducer(cfg.KafkaBrokers, logger)
	if err != nil {
		logger.Error("creating kafka producer", "error", err)
		st.Close()
		os.Exit(1)
	}

	pingCtx, cancelPing := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	pingErr := producer.Ping(pingCtx)
	cancelPing()
	if pingErr != nil {
		logger.Error("pinging kafka", "error", pingErr)
		producer.Close()
		st.Close()
		os.Exit(1)
	}

	ops := opsserver.New(cfg.OpsAddr, logger)
	opsErr := ops.Start()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Readiness means both dependencies are reachable: the relay cannot make
	// progress without either. The poller runs until ctx is cancelled at
	// shutdown, mirroring the api binary's pattern.
	readinessDone := make(chan struct{})
	go runReadinessPoller(ctx, st, producer, ops, logger, readinessDone)

	relayDone := make(chan struct{})
	go runRelayLoop(ctx, st, producer, logger, relayDone)

	select {
	case <-ctx.Done():
		logger.Info("shutdown signal received")
	case err := <-opsErr:
		if err != nil {
			logger.Error("ops server failed", "error", err)
			os.Exit(1)
		}
	}

	ops.SetReady(false)
	stop()
	<-readinessDone
	<-relayDone

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := ops.Shutdown(shutdownCtx); err != nil {
		logger.Error("ops server shutdown failed", "error", err)
	}

	producer.Close()
	st.Close()

	logger.Info("shutdown complete")
}

// runReadinessPoller pings st and producer every readinessPollInterval and
// mirrors the combined result onto ops' readiness flag, so a Postgres or
// Kafka outage shows up on /readyz instead of staying invisible. It marks
// ready immediately on entry, matching the successful connect and ping
// already performed in main before this goroutine starts, then stops
// cleanly when ctx is cancelled.
func runReadinessPoller(ctx context.Context, st *store.Store, producer *kafka.Producer, ops *opsserver.Server, logger *slog.Logger, done chan<- struct{}) {
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
			kafkaErr := producer.Ping(ctx)
			if pgErr != nil || kafkaErr != nil {
				logger.Warn("readiness check failed", "postgres_error", pgErr, "kafka_error", kafkaErr)
				ops.SetReady(false)
				continue
			}
			ops.SetReady(true)
		}
	}
}

// runRelayLoop repeatedly republishes durably-written outbox rows onto
// Kafka until ctx is cancelled, then closes done.
func runRelayLoop(ctx context.Context, st *store.Store, producer *kafka.Producer, logger *slog.Logger, done chan<- struct{}) {
	defer close(done)

	for {
		relayBatch(ctx, st, producer, logger)

		if ctx.Err() != nil {
			return
		}

		waitForNextRelayPass(ctx, st)

		if ctx.Err() != nil {
			return
		}
	}
}

// relayBatch fetches up to relayBatchSize unpublished outbox rows and
// produces each one to Kafka, in order, so that all events for a job stay
// ordered on the partition their key hashes to.
//
// If a produce fails, relayBatch logs it and returns immediately, leaving
// that row (and everything after it in the batch) unpublished. The next
// pass will fetch the same rows again and retry from the front, which is
// why this is safe to abandon mid-batch rather than needing to resume from
// where it stopped.
func relayBatch(ctx context.Context, st *store.Store, producer *kafka.Producer, logger *slog.Logger) {
	messages, err := st.FetchUnpublished(ctx, relayBatchSize)
	if err != nil {
		logger.Error("fetching unpublished outbox messages", "error", err)
		return
	}

	for _, msg := range messages {
		if err := producer.Produce(ctx, msg.Topic, msg.Key, msg.Payload); err != nil {
			logger.Error("producing outbox message, leaving unpublished for retry",
				"error", err, "outbox_id", msg.ID, "job_id", msg.JobID)
			return
		}

		// A crash, or any error, between the produce above and this write
		// republishes the same message on restart, since it is still
		// marked unpublished: an accepted duplicate, not a lost job.
		// Downstream consumers dedupe by job id (the record key), which is
		// what absorbs the duplicate.
		if err := st.MarkPublished(ctx, msg.ID); err != nil {
			logger.Error("marking outbox message published, leaving unpublished for retry",
				"error", err, "outbox_id", msg.ID, "job_id", msg.JobID)
			return
		}

		metrics.OutboxPublished.Inc()
	}
}

// waitForNextRelayPass blocks until whichever comes first: an outbox_new
// notification, relayPollInterval elapsing, or ctx being cancelled. Any
// error from WaitForOutboxNotification, including the timeout expiring, is
// intentionally ignored: the notification only exists to wake the relay
// early, so every outcome here just means "run the next pass now".
func waitForNextRelayPass(ctx context.Context, st *store.Store) {
	waitCtx, cancel := context.WithTimeout(ctx, relayPollInterval)
	defer cancel()

	_ = st.WaitForOutboxNotification(waitCtx)
}
