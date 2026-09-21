package store

import (
	"context"
	"errors"
	"fmt"
	"testing"
)

// TestStatusConstants verifies the status constants have the exact string
// values other services (and any Postgres CHECK constraint added later)
// will compare against.
func TestStatusConstants(t *testing.T) {
	tests := []struct {
		name string
		got  string
		want string
	}{
		{"pending", StatusPending, "pending"},
		{"queued", StatusQueued, "queued"},
		{"running", StatusRunning, "running"},
		{"succeeded", StatusSucceeded, "succeeded"},
		{"failed", StatusFailed, "failed"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.got != tt.want {
				t.Errorf("got %q, want %q", tt.got, tt.want)
			}
		})
	}
}

// TestNewInvalidDSN verifies that New reports an error for a DSN pgxpool
// cannot even parse, rather than deferring the failure to first use. This
// does not require a live database: pgxpool.New validates the connection
// string before attempting to connect.
func TestNewInvalidDSN(t *testing.T) {
	_, err := New(context.Background(), "not-a-valid-dsn://[::::]")
	if err == nil {
		t.Fatal("New returned nil error for an invalid DSN, want an error")
	}
}

// TestErrJobNotFoundIsDistinct verifies ErrJobNotFound is a distinct
// sentinel that survives fmt.Errorf(%w) wrapping, since GetJob and
// SetJobStatus both wrap it that way for callers to match with errors.Is.
func TestErrJobNotFoundIsDistinct(t *testing.T) {
	wrapped := fmt.Errorf("getting job x: %w", ErrJobNotFound)
	if !errors.Is(wrapped, ErrJobNotFound) {
		t.Error("wrapped error does not satisfy errors.Is(err, ErrJobNotFound)")
	}

	other := errors.New("some other failure")
	if errors.Is(other, ErrJobNotFound) {
		t.Error("unrelated error unexpectedly satisfies errors.Is(err, ErrJobNotFound)")
	}
}
