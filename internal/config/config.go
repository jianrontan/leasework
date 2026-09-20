// Package config loads leasework's runtime configuration from environment
// variables. In Phase 0 nothing connects to the referenced services; the
// values are parsed and logged only, so later phases can use them directly.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"
)

// Config holds runtime configuration shared by all leasework binaries.
type Config struct {
	LogLevel        string
	OpsAddr         string
	PostgresDSN     string
	RedisAddr       string
	KafkaBrokers    []string
	ShutdownTimeout time.Duration
}

// Load reads configuration for the named service ("api", "worker" or
// "scheduler") from environment variables, applying defaults for anything
// unset. service only affects the default ops address.
func Load(service string) (Config, error) {
	cfg := Config{
		LogLevel:    getenv("LEASEWORK_LOG_LEVEL", "info"),
		OpsAddr:     getenv("LEASEWORK_OPS_ADDR", defaultOpsAddr(service)),
		PostgresDSN: getenv("LEASEWORK_POSTGRES_DSN", "postgres://leasework:leasework@localhost:5432/leasework?sslmode=disable"),
		RedisAddr:   getenv("LEASEWORK_REDIS_ADDR", "localhost:6379"),
		KafkaBrokers: parseBrokers(getenv("LEASEWORK_KAFKA_BROKERS",
			"localhost:19092,localhost:29092,localhost:39092")),
	}

	timeoutStr := getenv("LEASEWORK_SHUTDOWN_TIMEOUT", "20s")
	timeout, err := time.ParseDuration(timeoutStr)
	if err != nil {
		return Config{}, fmt.Errorf("parsing LEASEWORK_SHUTDOWN_TIMEOUT %q: %w", timeoutStr, err)
	}
	cfg.ShutdownTimeout = timeout

	return cfg, nil
}

// defaultOpsAddr returns the default ops-server listen address for a given
// service name: api defaults to :8080, worker to :9101, scheduler to :9102.
func defaultOpsAddr(service string) string {
	switch service {
	case "worker":
		return ":9101"
	case "scheduler":
		return ":9102"
	default:
		return ":8080"
	}
}

// parseBrokers splits a comma-separated broker list, trimming whitespace and
// dropping empty entries.
func parseBrokers(raw string) []string {
	parts := strings.Split(raw, ",")
	brokers := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			brokers = append(brokers, p)
		}
	}
	return brokers
}

// getenv returns the environment variable named key, or fallback if unset.
func getenv(key, fallback string) string {
	if v, ok := os.LookupEnv(key); ok {
		return v
	}
	return fallback
}
