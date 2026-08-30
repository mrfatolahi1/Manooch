// Package obs holds Manooch's logging and metrics wiring.
package obs

import (
	"fmt"
	"io"
	"log/slog"
	"strings"
)

// NewLogger builds the process logger: JSON, at the configured level, tagged
// with the venue this process serves.
//
// Nothing in Manooch logs per message, at any level, ever. A book stream at
// 10 updates a second across 200 instruments is 2,000 lines a second, which is
// not a log — it is a denial of service against the disk, and against anyone
// trying to find the one line that mattered. Lifecycle events and status
// transitions are logged. Data is not.
func NewLogger(w io.Writer, level, venue string) (*slog.Logger, error) {
	lvl, err := ParseLevel(level)
	if err != nil {
		return nil, err
	}
	h := slog.NewJSONHandler(w, &slog.HandlerOptions{Level: lvl})
	return slog.New(h).With("venue", venue), nil
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
