package config

import (
	"testing"
	"time"
)

// TestLoadDefaults verifies that Load applies the documented defaults when
// none of the LEASEWORK_* environment variables are set.
func TestLoadDefaults(t *testing.T) {
	cfg, err := Load("api")
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}

	if got, want := cfg.LogLevel, "info"; got != want {
		t.Errorf("LogLevel = %q, want %q", got, want)
	}
	if got, want := cfg.OpsAddr, ":8080"; got != want {
		t.Errorf("OpsAddr = %q, want %q", got, want)
	}
	if got, want := cfg.PostgresDSN, "postgres://leasework:leasework@localhost:5432/leasework?sslmode=disable"; got != want {
		t.Errorf("PostgresDSN = %q, want %q", got, want)
	}
	if got, want := cfg.RedisAddr, "localhost:6379"; got != want {
		t.Errorf("RedisAddr = %q, want %q", got, want)
	}
	wantBrokers := []string{"localhost:19092", "localhost:29092", "localhost:39092"}
	if !stringSlicesEqual(cfg.KafkaBrokers, wantBrokers) {
		t.Errorf("KafkaBrokers = %v, want %v", cfg.KafkaBrokers, wantBrokers)
	}
	if got, want := cfg.ShutdownTimeout, 20*time.Second; got != want {
		t.Errorf("ShutdownTimeout = %v, want %v", got, want)
	}
}

// TestDefaultOpsAddr verifies the per-service ops-address defaults.
func TestDefaultOpsAddr(t *testing.T) {
	tests := []struct {
		name    string
		service string
		want    string
	}{
		{"api", "api", ":8080"},
		{"worker", "worker", ":9101"},
		{"scheduler", "scheduler", ":9102"},
		{"unknown falls back to api default", "some-unknown-service", ":8080"},
		{"empty falls back to api default", "", ":8080"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := defaultOpsAddr(tt.service); got != tt.want {
				t.Errorf("defaultOpsAddr(%q) = %q, want %q", tt.service, got, tt.want)
			}
		})
	}
}

// TestLoadOpsAddrOverride verifies that LEASEWORK_OPS_ADDR overrides the
// per-service default.
func TestLoadOpsAddrOverride(t *testing.T) {
	t.Setenv("LEASEWORK_OPS_ADDR", ":9999")

	cfg, err := Load("worker")
	if err != nil {
		t.Fatalf("Load returned unexpected error: %v", err)
	}
	if got, want := cfg.OpsAddr, ":9999"; got != want {
		t.Errorf("OpsAddr = %q, want %q (override should take precedence over service default)", got, want)
	}
}

// TestParseBrokers exercises parseBrokers with a table of inputs.
func TestParseBrokers(t *testing.T) {
	tests := []struct {
		name string
		raw  string
		want []string
	}{
		{
			name: "normal list",
			raw:  "broker1:9092,broker2:9092,broker3:9092",
			want: []string{"broker1:9092", "broker2:9092", "broker3:9092"},
		},
		{
			name: "surrounding whitespace",
			raw:  "  broker1:9092 , broker2:9092  ,broker3:9092 ",
			want: []string{"broker1:9092", "broker2:9092", "broker3:9092"},
		},
		{
			name: "empty entries dropped",
			raw:  "broker1:9092,,broker2:9092,",
			want: []string{"broker1:9092", "broker2:9092"},
		},
		{
			name: "empty string yields empty slice",
			raw:  "",
			want: []string{},
		},
		{
			name: "single broker",
			raw:  "broker1:9092",
			want: []string{"broker1:9092"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := parseBrokers(tt.raw)
			if got == nil {
				t.Fatalf("parseBrokers(%q) returned nil, want a non-nil slice", tt.raw)
			}
			if !stringSlicesEqual(got, tt.want) {
				t.Errorf("parseBrokers(%q) = %v, want %v", tt.raw, got, tt.want)
			}
		})
	}
}

// TestLoadShutdownTimeout verifies that an unparseable
// LEASEWORK_SHUTDOWN_TIMEOUT causes Load to return an error, and that a
// valid one parses to the expected duration.
func TestLoadShutdownTimeout(t *testing.T) {
	t.Run("invalid returns error", func(t *testing.T) {
		t.Setenv("LEASEWORK_SHUTDOWN_TIMEOUT", "not-a-duration")

		_, err := Load("api")
		if err == nil {
			t.Fatalf("Load returned nil error, want an error for unparseable LEASEWORK_SHUTDOWN_TIMEOUT")
		}
	})

	t.Run("valid parses correctly", func(t *testing.T) {
		t.Setenv("LEASEWORK_SHUTDOWN_TIMEOUT", "45s")

		cfg, err := Load("api")
		if err != nil {
			t.Fatalf("Load returned unexpected error: %v", err)
		}
		if got, want := cfg.ShutdownTimeout, 45*time.Second; got != want {
			t.Errorf("ShutdownTimeout = %v, want %v", got, want)
		}
	})
}

// stringSlicesEqual reports whether two string slices contain the same
// elements in the same order.
func stringSlicesEqual(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
