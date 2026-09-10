// Package observability holds the logging and metrics wiring.
package observability

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// NewLogger builds the process logger: JSON, at the configured level, tagged
// with the venue this process serves.
//
// Nothing logs per message, at any level. Three channels at one update a second
// across 200 instruments is 600 lines a second, which buries the one line that
// mattered. Log lifecycle events and status transitions, not data.
func NewLogger(writer io.Writer, level, venue string) (*slog.Logger, error) {
	parsedLevel, err := ParseLevel(level)
	if err != nil {
		return nil, err
	}
	handler := slog.NewJSONHandler(writer, &slog.HandlerOptions{Level: parsedLevel})
	return slog.New(handler).With("venue", venue), nil
}

// ParseLevel maps a config log_level onto a slog level.
func ParseLevel(s string) (slog.Level, error) {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn", "warning":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return 0, fmt.Errorf("unknown log level %q", s)
	}
}
