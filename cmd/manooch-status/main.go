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
	pb "github.com/you/manooch/gen/manoochv1"
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
		venue   = flag.String("venue", "", "restrict to one venue (default: all)")
		addr    = flag.String("redis", "127.0.0.1:6379", "Redis address")
		db      = flag.Int("db", 0, "Redis database")
		noColor = flag.Bool("no-color", false, "never colourise output")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	rdb := redis.NewClient(&redis.Options{Addr: *addr, DB: *db})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis %s: %w", *addr, err)
	}

	keys, err := scanKeys(ctx, rdb, publish.MatchPattern(*venue))
	if err != nil {
		return err
	}
	if len(keys) == 0 {
		fmt.Printf("no streams matching %s\n", publish.MatchPattern(*venue))
		return nil
	}

	rows, err := readRows(ctx, rdb, keys)
	if err != nil {
		return err
	}
	sort.Slice(rows, func(i, j int) bool { return rows[i].less(rows[j]) })

	print(rows, colourEnabled(*noColor))
	return nil
}

// scanKeys uses SCAN, never KEYS: KEYS walks the whole keyspace in one blocking
// call and stalls every publisher behind it.
func scanKeys(ctx context.Context, rdb *redis.Client, pattern string) ([]string, error) {
	var (
		keys   []string
		cursor uint64
	)
	for {
		batch, next, err := rdb.Scan(ctx, cursor, pattern, 500).Result()
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
	key        string
	venue      string
	marketType string
	symbol     string
	channel    string
	status     pb.Status
	statusText string
	age        time.Duration
	source     string
	ttl        time.Duration
	ttlText    string
	publishSeq uint64
	restarts   uint32
	reason     string

	// venueScoped marks Manooch:{VENUE}:venue:health, the connection-level row.
	venueScoped bool
	// health is set on any row carrying a Health payload, which is what the
	// restart counts are attached from.
	health *pb.Health
}

func (r row) less(o row) bool {
	if r.venue != o.venue {
		return r.venue < o.venue
	}
	// The connection-level row first: everything under it is conditional on
	// the socket being up, so reading it second is reading it backwards.
	if r.venueScoped != o.venueScoped {
		return r.venueScoped
	}
	if r.marketType != o.marketType {
		return r.marketType < o.marketType
	}
	if r.symbol != o.symbol {
		return r.symbol < o.symbol
	}
	return r.channel < o.channel
}

// readRows fetches every value and its remaining TTL in one round trip.
func readRows(ctx context.Context, rdb *redis.Client, keys []string) ([]row, error) {
	pipe := rdb.Pipeline()
	gets := make([]*redis.StringCmd, len(keys))
	ttls := make([]*redis.DurationCmd, len(keys))
	for i, k := range keys {
		gets[i] = pipe.Get(ctx, k)
		ttls[i] = pipe.PTTL(ctx, k)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		return nil, fmt.Errorf("read: %w", err)
	}

	now := time.Now()
	rows := make([]row, 0, len(keys))
	for i, k := range keys {
		r := row{key: k, venue: "?", marketType: "?", symbol: "?", channel: "?", ttlText: "-", source: "-", statusText: "-"}

		parts, err := publish.ParseKey(k)
		if err != nil {
			r.reason = "unparseable key"
			rows = append(rows, r)
			continue
		}
		r.venue = parts.Venue
		r.venueScoped = parts.VenueScoped
		if parts.VenueScoped {
			// Nothing about a connection belongs to one instrument.
			r.marketType, r.channel, r.symbol = publish.VenueScope, parts.Subject, "-"
		} else {
			r.marketType = core.MarketTypeName(parts.MarketType)
			r.symbol = parts.Symbol
			r.channel = core.ChannelName(parts.Channel)
		}

		// -1 is a key with no expiry; -2 is one that vanished since the SCAN.
		switch d, err := ttls[i].Result(); {
		case err != nil:
			r.ttlText = "?"
		case d == -1:
			r.ttlText = "none"
		case d < 0:
			r.ttlText = "expired"
		default:
			r.ttl, r.ttlText = d, compactDuration(d)
		}

		payload, err := gets[i].Bytes()
		if err != nil {
			r.reason = "expired between scan and read"
			rows = append(rows, r)
			continue
		}

		ch := parts.Channel
		if parts.VenueScoped {
			if parts.Subject != publish.SubjectHealth {
				r.reason = fmt.Sprintf("%d bytes", len(payload))
				rows = append(rows, r)
				continue
			}
			ch = pb.Channel_CHANNEL_HEALTH
		}

		msg, env, err := publish.Decode(ch, payload)
		if err != nil {
			r.reason = "decode: " + err.Error()
			rows = append(rows, r)
			continue
		}
		r.status = env.Status
		r.statusText = core.StatusName(env.Status)
		r.age = now.Sub(time.Unix(0, env.PublishTimeNs))
		r.source = "-" // health carries no source of its own
		if env.Source != pb.Source_SOURCE_UNSPECIFIED {
			r.source = core.SourceName(env.Source)
		}
		r.publishSeq = env.PublishSeq
		r.reason = env.StatusReason
		if h, ok := msg.(*pb.Health); ok {
			r.health = h
			r.restarts = h.StreamRestartCount
		}
		rows = append(rows, r)
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
	for _, r := range rows {
		if r.health != nil && !r.venueScoped {
			restarts[r.venue+"|"+r.marketType+"|"+r.symbol] = r.restarts
		}
	}

	for i := range rows {
		r := &rows[i]
		if r.venueScoped {
			r.reason = venueReason(r)
			continue
		}
		if r.health == nil {
			r.restarts = restarts[r.venue+"|"+r.marketType+"|"+r.symbol]
		}
	}
}

// venueReason spells out what only the connection-level row knows.
func venueReason(r *row) string {
	if r.health == nil {
		return r.reason
	}
	parts := []string{}
	if r.reason != "" {
		parts = append(parts, r.reason)
	}
	parts = append(parts,
		fmt.Sprintf("skew=%dms", r.health.ClockSkewMs),
		fmt.Sprintf("reconnects=%d", r.health.ReconnectCount))
	if r.health.LeakedGoroutines > 0 {
		parts = append(parts, fmt.Sprintf("leaked=%d", r.health.LeakedGoroutines))
	}
	return strings.Join(parts, " ")
}

func print(rows []row, colour bool) {
	w := tabwriter.NewWriter(os.Stdout, 0, 0, 2, ' ', 0)
	fmt.Fprintln(w, "VENUE\tMARKET TYPE\tSYMBOL\tCHANNEL\tSTATUS\tAGE\tSOURCE\tTTL\tRESTARTS\tPUBLISH SEQ\tREASON\t")

	counts := map[string]int{}
	for _, r := range rows {
		counts[r.statusText]++

		line := fmt.Sprintf("%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%d\t%d\t%s",
			r.venue, r.marketType, r.symbol, r.channel,
			marker(r.status)+r.statusText, compactDuration(r.age), r.source, r.ttlText,
			r.restarts, r.publishSeq, r.reason)

		if colour {
			if c := colourFor(r.status); c != "" {
				line = c + line + ansiReset
			}
		}
		fmt.Fprintln(w, line)
	}
	w.Flush()

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
func marker(s pb.Status) string {
	switch s {
	case pb.Status_STATUS_DEGRADED:
		return "! "
	case pb.Status_STATUS_STALE:
		return "!! "
	default:
		return ""
	}
}

func colourFor(s pb.Status) string {
	switch s {
	case pb.Status_STATUS_DEGRADED:
		return ansiYellow
	case pb.Status_STATUS_STALE:
		return ansiRed
	case pb.Status_STATUS_UNSPECIFIED:
		return ansiDim
	default:
		return ""
	}
}

func colourEnabled(noColor bool) bool {
	if noColor || os.Getenv("NO_COLOR") != "" {
		return false
	}
	fi, err := os.Stdout.Stat()
	return err == nil && fi.Mode()&os.ModeCharDevice != 0
}

func compactDuration(d time.Duration) string {
	switch {
	case d < 0:
		return "-" + compactDuration(-d)
	case d < time.Millisecond:
		return fmt.Sprintf("%dus", d.Microseconds())
	case d < time.Second:
		return fmt.Sprintf("%dms", d.Milliseconds())
	case d < time.Minute:
		return fmt.Sprintf("%.1fs", d.Seconds())
	default:
		return d.Truncate(time.Second).String()
	}
}
