package logging

import (
	"context"
	"log/slog"
	"testing"
)

// TestNewLevelFiltering verifies that New parses the requested level
// case-insensitively and configures the logger's enablement accordingly,
// falling back to info for unrecognised input.
func TestNewLevelFiltering(t *testing.T) {
	tests := []struct {
		name  string
		level string
		want  slog.Level
	}{
		{"debug", "debug", slog.LevelDebug},
		{"INFO uppercase", "INFO", slog.LevelInfo},
		{"warn", "warn", slog.LevelWarn},
		{"error", "error", slog.LevelError},
		{"garbage falls back to info", "not-a-real-level", slog.LevelInfo},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			logger := New(tt.level)
			if logger == nil {
				t.Fatalf("New(%q) returned nil logger", tt.level)
			}

			assertEnabledFrom(t, logger, tt.want)
		})
	}
}

// assertEnabledFrom checks that logger is enabled for every level from
// minEnabled upward, and disabled for every standard level below it.
func assertEnabledFrom(t *testing.T, logger *slog.Logger, minEnabled slog.Level) {
	t.Helper()

	ctx := context.Background()
	levels := []slog.Level{slog.LevelDebug, slog.LevelInfo, slog.LevelWarn, slog.LevelError}

	for _, lvl := range levels {
		want := lvl >= minEnabled
		got := logger.Enabled(ctx, lvl)
		if got != want {
			t.Errorf("Enabled(%v) = %v, want %v (min enabled level %v)", lvl, got, want, minEnabled)
		}
	}
}
