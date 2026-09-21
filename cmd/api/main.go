// Command api is the leasework API binary: a stateless service, run as N
// replicas, exposing HTTP endpoints to submit, get, list and cancel jobs.
// Each submission writes the job row and an outbox row in a single Postgres
// transaction. Phase 1 wires up POST /jobs and GET /jobs/{id}; list and
// cancel are added in a later phase.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jianrontan/leasework/internal/api"
	"github.com/jianrontan/leasework/internal/config"
	"github.com/jianrontan/leasework/internal/logging"
	"github.com/jianrontan/leasework/internal/opsserver"
	"github.com/jianrontan/leasework/internal/store"
)

const serviceName = "api"

// readinessPollInterval is how often the background goroutine re-checks
// Postgres connectivity to keep /readyz current between requests.
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

	ops := opsserver.New(cfg.OpsAddr, logger)
	handler := api.NewHandler(st, logger)
	handler.Register(ops.Mux())
	opsErr := ops.Start()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Readiness now means something: the API is not ready to serve traffic
	// until Postgres answers, and a later outage must be reflected in
	// /readyz rather than staying invisible. The poller runs until ctx is
	// cancelled at shutdown.
	readinessDone := make(chan struct{})
	go runReadinessPoller(ctx, st, ops, logger, readinessDone)

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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := ops.Shutdown(shutdownCtx); err != nil {
		logger.Error("ops server shutdown failed", "error", err)
	}

	// Closed after the HTTP server has finished draining in-flight
	// requests, so a request that started before shutdown can still reach
	// Postgres.
	st.Close()

	logger.Info("shutdown complete")
}

// runReadinessPoller pings st every readinessPollInterval and mirrors the
// result onto ops' readiness flag, so a Postgres outage shows up on
// /readyz instead of being silently invisible. It marks ready immediately
// on entry, matching the successful Ping already performed in main before
// this goroutine starts, then stops cleanly when ctx is cancelled.
func runReadinessPoller(ctx context.Context, st *store.Store, ops *opsserver.Server, logger *slog.Logger, done chan<- struct{}) {
	defer close(done)

	ops.SetReady(true)

	ticker := time.NewTicker(readinessPollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := st.Ping(ctx); err != nil {
				logger.Warn("postgres readiness check failed", "error", err)
				ops.SetReady(false)
				continue
			}
			ops.SetReady(true)
		}
	}
}
