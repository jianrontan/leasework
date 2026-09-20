// Command worker is the leasework worker binary: the tier that scales, run
// as N replicas. As a Kafka consumer it will claim a lease, heartbeat,
// execute the job, perform a fenced terminal write and commit its offset.
// Phase 0 only stands up the process skeleton and ops server; the consumer
// loop itself is added in a later phase.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"syscall"

	"github.com/jianrontan/leasework/internal/config"
	"github.com/jianrontan/leasework/internal/logging"
	"github.com/jianrontan/leasework/internal/opsserver"
)

const serviceName = "worker"

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

	ops := opsserver.New(cfg.OpsAddr, logger)
	opsErr := ops.Start()

	// Phase 0 has no dependencies to wait for. Later phases gate readiness
	// on Kafka, Redis and Postgres connectivity.
	ops.SetReady(true)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

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

	shutdownCtx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer cancel()

	if err := ops.Shutdown(shutdownCtx); err != nil {
		logger.Error("ops server shutdown failed", "error", err)
	}

	logger.Info("shutdown complete")
}
