// Package logging provides the shared structured logger for all leasework
// binaries.
package logging

import (
	"log/slog"
	"os"
	"strings"
)

// New returns a *slog.Logger backed by a JSON handler writing to stderr.
// level is parsed case-insensitively as one of "debug", "info", "warn" or
// "error"; anything unrecognised defaults to info.
func New(level string) *slog.Logger {
	var lvl slog.Level
	switch strings.ToLower(level) {
	case "debug":
		lvl = slog.LevelDebug
	case "warn":
		lvl = slog.LevelWarn
	case "error":
		lvl = slog.LevelError
	default:
		lvl = slog.LevelInfo
	}

	handler := slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: lvl})
	return slog.New(handler)
}
