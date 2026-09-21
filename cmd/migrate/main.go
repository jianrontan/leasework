// Command migrate applies leasework's database schema and exits. Migrations
// are a separate one-shot step rather than something the API does at boot,
// so exactly one process owns schema changes and every long-running service
// (api, worker, scheduler) can assume the schema already exists by the time
// it starts.
package main

import (
	"context"
	"fmt"
	"os"

	"github.com/jianrontan/leasework/internal/config"
	"github.com/jianrontan/leasework/internal/logging"
	"github.com/jianrontan/leasework/internal/store"
)

const serviceName = "migrate"

func main() {
	cfg, err := config.Load(serviceName)
	if err != nil {
		fmt.Fprintf(os.Stderr, "loading config: %v\n", err)
		os.Exit(1)
	}

	logger := logging.New(cfg.LogLevel).With("service", serviceName)
	logger.Info("starting",
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
	defer st.Close()

	migrateCtx, cancelMigrate := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	migrateErr := st.Migrate(migrateCtx)
	cancelMigrate()
	if migrateErr != nil {
		logger.Error("running migrations", "error", migrateErr)
		os.Exit(1)
	}

	logger.Info("migrations applied")
}
