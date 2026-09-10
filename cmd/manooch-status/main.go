// Command manooch-status reads the Redis keys and prints one row per stream.
//
// It reads; it never subscribes. The last-value cache is the state of the
// world, so one pass over the keys is the whole picture, and a stream whose key
// has expired simply is not there.
//
// The health keys are read alongside the data keys, which is where the restart
// count comes from and what the venue row is: socket state, clock skew and
// leaked goroutines belong to the connection rather than to any one stream.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"text/tabwriter"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/publish"
)

// ANSI colours, used only when stdout is a terminal.
const (
	ansiReset  = "\033[0m"
	ansiRed    = "\033[31m"
	ansiYellow = "\033[33m"
	ansiDim    = "\033[2m"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "manooch-status: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		venue    = flag.String("venue", "", "restrict to one venue (default: all)")
		address  = flag.String("redis", "127.0.0.1:6379", "Redis address")
		database = flag.Int("db", 0, "Redis database")
		noColor  = flag.Bool("no-color", false, "never colourise output")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisClient := redis.NewClient(&redis.Options{Addr: *address, DB: *database})
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis %s: %w", *address, err)
	}

	keys, err := scanKeys(ctx, redisClient, publish.MatchPattern(*venue))
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		fmt.Printf("no streams matching %s\n", publish.MatchPattern(*venue))
		return nil
	}

	rows, err := readRows(ctx, redisClient, keys)
	if err != nil {
		return err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].less(rows[j]) })

	print(rows, colourEnabled(*noColor))
	return nil
}

// scanKeys uses SCAN, never KEYS: KEYS walks the whole keyspace in one blocking
// call and stalls every publisher behind it.
func scanKeys(ctx context.Context, redisClient *redis.Client, pattern string) ([]string, error) {
	var (
		keys   []string
		cursor uint64
	)
	for {
		batch, next, err := redisClient.Scan(ctx, cursor, pattern, 500).Result()
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", pattern, err)
		}
		keys = append(keys, batch...)
		if next == 0 {
			return keys, nil
		}
		cursor = next
	}
}

type row struct {
	key             string
	venue           string
	marketType      string
	symbol          string
	channel         string
	status          manoochv1.Status
	statusText      string
	age             time.Duration
	source          string
	timeToLive      time.Duration
	ttlText         string
	publishSequence uint64
	restarts        uint32
	reason          string

	// venueScoped marks a Manooch:{VENUE}:venue:{subject} row, which describes
	// the connection rather than any one instrument.
	venueScoped bool
	// health is set on any row carrying a Health payload, which is what the
	// restart counts are attached from.
	health *manoochv1.Health
}

func (current row) less(other row) bool {
	if current.venue != other.venue {
		return current.venue < other.venue
	}
	// The connection-level row first: everything under it is conditional on
	// the socket being up, so reading it second is reading it backwards.
	if current.venueScoped != other.venueScoped {
		return current.venueScoped
	}
	if current.marketType != other.marketType {
		return current.marketType < other.marketType
	}
	if current.symbol != other.symbol {
		return current.symbol < other.symbol
	}
	return current.channel < other.channel
}

// readRows fetches every value and its remaining TTL in one round trip.
func readRows(ctx context.Context, redisClient *redis.Client, keys []string) ([]row, error) {
	pipe := redisClient.Pipeline()
	getCommands := make([]*redis.StringCmd, len(keys))
	timeToLives := make([]*redis.DurationCmd, len(keys))
	for i, k := range keys {
		getCommands[i] = pipe.Get(ctx, k)
		timeToLives[i] = pipe.PTTL(ctx, k)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read: %w", err)
	}

	now := time.Now()
	rows := make([]row, 0, len(keys))
	for i, k := range keys {
		row := row{key: k, venue: "?", marketType: "?", symbol: "?", channel: "?", ttlText: "-", source: "-", statusText: "-"}

		parts, err := publish.ParseKey(k)
		if err != nil {
			row.reason = "unparseable key"
			rows = append(rows, row)
			continue
		}
		row.venue = parts.Venue
		row.venueScoped = parts.VenueScoped
		if parts.VenueScoped {
			// Nothing about a connection belongs to one instrument.
			row.marketType, row.channel, row.symbol = publish.VenueScope, parts.Subject, "-"
		} else {
			row.marketType = core.MarketTypeName(parts.MarketType)
			row.symbol = parts.Symbol
			row.channel = core.ChannelName(parts.Channel)
		}

		// -1 is a key with no expiry; -2 is one that vanished since the SCAN.
		switch duration, err := timeToLives[i].Result(); {
		case err != nil:
			row.ttlText = "?"
		case duration == -1:
			row.ttlText = "none"
		case duration < 0:
			row.ttlText = "expired"
		default:
			row.timeToLive, row.ttlText = duration, compactDuration(duration)
		}

		payload, err := getCommands[i].Bytes()
		if err != nil {
			row.reason = "expired between scan and read"
			rows = append(rows, row)
			continue
		}

		channel := parts.Channel
		if parts.VenueScoped {
			subjectChannel, ok := publish.ChannelForSubject(parts.Subject)
			if !ok {
				row.reason = fmt.Sprintf("%d bytes", len(payload))
				rows = append(rows, row)
				continue
			}
			channel = subjectChannel
		}

		message, envelope, err := publish.Decode(channel, payload)
		if err != nil {
			row.reason = "decode: " + err.Error()
			rows = append(rows, row)
			continue
		}
		row.status = envelope.Status
		row.statusText = core.StatusName(envelope.Status)
		row.age = now.Sub(time.Unix(0, envelope.PublishTimeNs))
		row.source = "-" // health carries no source of its own
		if envelope.Source != manoochv1.Source_SOURCE_UNSPECIFIED {
			row.source = core.SourceName(envelope.Source)
		}
		row.publishSequence = envelope.PublishSeq
		row.reason = envelope.StatusReason
		switch m := message.(type) {
		case *manoochv1.Health:
			row.health = m
			row.restarts = m.StreamRestartCount
		case *manoochv1.RateLimit:
			// Advisory: what this process has spent of the venue's budget.
			// Spelled out here because no other row carries it.
			row.reason = strings.TrimSpace(row.reason + " " + budgetSummary(m))
		}
		rows = append(rows, row)
	}

	attachHealth(rows)
	return rows, nil
}

// attachHealth copies each instrument's restart count onto its data rows, and
// spells the connection-level numbers out on the venue row.
//
// The restart count is per instrument, not per channel: the health key is one
// per instrument, and a channel's own status and reason already ride inside its
// data key. What the column answers is "has this instrument been churning",
// which is the question worth asking from a table.
func attachHealth(rows []row) {
	restarts := map[string]uint32{}
	for _, row := range rows {
		if row.health != nil && !row.venueScoped {
			restarts[row.venue+"|"+row.marketType+"|"+row.symbol] = row.restarts
		}
	}

	for i := range rows {
		row := &rows[i]
		if row.venueScoped {
			row.reason = venueReason(row)
			continue
		}
		if row.health == nil {
			row.restarts = restarts[row.venue+"|"+row.marketType+"|"+row.symbol]
		}
	}
}

// budgetSummary renders the rate-limit budgets as "rest_weight=3/3000".
func budgetSummary(rateLimit *manoochv1.RateLimit) string {
	parts := make([]string, 0, len(rateLimit.Budgets))
	for _, budget := range rateLimit.Budgets {
		parts = append(parts, fmt.Sprintf("%s=%d/%d", budget.Kind, budget.Used, budget.Capacity))
	}
	return strings.Join(parts, " ")
}

// venueReason spells out what only the connection-level row knows.
func venueReason(row *row) string {
	if row.health == nil {
		return row.reason
	}
	parts := []string{}
	if row.reason != "" {
		parts = append(parts, row.reason)
	}
	parts = append(parts,
		fmt.Sprintf("skew=%dms", row.health.ClockSkewMs),
		fmt.Sprintf("reconnects=%d", row.health.ReconnectCount))
	if row.health.LeakedGoroutines > 0 {
		parts = append(parts, fmt.Sprintf("leaked=%d", row.health.LeakedGoroutines))
	}
	return strings.Join(parts, " ")
}

func print(rows []row, colour bool) {
	writer := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(writer, "VENUE\tMARKET TYPE\tSYMBOL\tCHANNEL\tSTATUS\tAGE\tSOURCE\tTTL\tRESTARTS\tPUBLISH SEQ\tREASON\t")

	counts := map[string]int{}
	for _, row := range rows {
		counts[row.statusText]++

		line := fmt.Sprintf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s",
			row.venue, row.marketType, row.symbol, row.channel,
			marker(row.status)+row.statusText, compactDuration(row.age), row.source, row.ttlText,
			row.restarts, row.publishSequence, row.reason)

		if colour {
			if c := colourFor(row.status); c != "" {
				line = c + line + ansiReset
			}
		}
		fmt.Fprintln(writer, line)
	}
	writer.Flush()

	// Keys, not streams: the health keys are rows too, and calling them
	// streams would make the count disagree with the statuses beside it.
	summary := fmt.Sprintf("%d keys", len(rows))
	for _, s := range []string{"HEALTHY", "DEGRADED", "STALE", "UNSPECIFIED"} {
		if n := counts[s]; n > 0 {
			summary += fmt.Sprintf(", %d %s", n, strings.ToLower(s))
		}
	}
	fmt.Println("\n" + summary)
}

// marker prefixes anything not healthy, so the row stands out through a pipe or
// a terminal with no colour.
func marker(status manoochv1.Status) string {
	switch status {
	case manoochv1.Status_STATUS_DEGRADED:
		return "! "
	case manoochv1.Status_STATUS_STALE:
		return "!! "
	default:
		return ""
	}
}

func colourFor(status manoochv1.Status) string {
	switch status {
	case manoochv1.Status_STATUS_DEGRADED:
		return ansiYellow
	case manoochv1.Status_STATUS_STALE:
		return ansiRed
	case manoochv1.Status_STATUS_UNSPECIFIED:
		return ansiDim
	default:
		return ""
	}
}

func colourEnabled(noColor bool) bool {
	if noColor || os.Getenv("NO_COLOR") != "" {
		return false
	}
	fileInfo, err := os.Stdout.Stat()
	return err == nil && fileInfo.Mode()&os.ModeCharDevice != 0
}

func compactDuration(duration time.Duration) string {
	switch {
	case duration < 0:
		return "-" + compactDuration(-duration)
	case duration < time.Millisecond:
		return fmt.Sprintf("%dus", duration.Microseconds())
	case duration < time.Second:
		return fmt.Sprintf("%dms", duration.Milliseconds())
	case duration < time.Minute:
		return fmt.Sprintf("%.1fs", duration.Seconds())
	default:
		return duration.Truncate(time.Second).String()
	}
}
