// Command manooch-tap subscribes to the Pub/Sub channels and prints what goes
// past. The wire format is protobuf, so redis-cli psubscribe shows only binary.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/pkg/price"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	if err := run(); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintf(os.Stderr, "manooch-tap: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	var (
		pattern  = flag.String("pattern", publish.MatchPattern(""), "Pub/Sub pattern to subscribe to")
		address  = flag.String("redis", "127.0.0.1:6379", "Redis address")
		database = flag.Int("db", 0, "Redis database")
		asJSON   = flag.Bool("json", false, "print each message as JSON")
		raw      = flag.Bool("raw", false, "write raw message bytes to --out, for building test fixtures")
		out      = flag.String("out", filepath.Join("testdata", "raw"), "directory for --raw output")
	)
	flag.Parse()

	if *raw {
		if err := os.MkdirAll(*out, 0o755); err != nil {
			return err
		}
		fmt.Fprintf(os.Stderr, "writing raw messages to %s\n", *out)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	redisClient := redis.NewClient(&redis.Options{Addr: *address, DB: *database})
	defer redisClient.Close()
	if err := redisClient.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis %s: %w", *address, err)
	}

	subscription := redisClient.PSubscribe(ctx, *pattern)
	defer subscription.Close()
	if _, err := subscription.Receive(ctx); err != nil {
		return fmt.Errorf("subscribe %s: %w", *pattern, err)
	}
	fmt.Fprintf(os.Stderr, "subscribed to %s\n", *pattern)

	tap := &tap{seen: map[string]seen{}, rawDir: *out}
	for {
		select {
		case <-ctx.Done():
			return nil
		case message, ok := <-subscription.Channel():
			if !ok {
				return nil
			}
			tap.handle(message.Channel, []byte(message.Payload), *asJSON, *raw)
		}
	}
}

// seen is the last message on a topic, so gaps are spotted as they go past.
type seen struct {
	publishSequence uint64
	instanceID      string
}

type tap struct {
	seen   map[string]seen
	rawDir string
	n      int
}

func (tap *tap) handle(key string, payload []byte, asJSON, raw bool) {
	parts, err := publish.ParseKey(key)
	if err != nil {
		fmt.Printf("%s  ?? %v\n", stamp(time.Now()), err)
		return
	}
	channel := parts.Channel
	if parts.VenueScoped {
		subjectChannel, ok := publish.ChannelForSubject(parts.Subject)
		if !ok {
			fmt.Printf("%s  %s  %d bytes\n", stamp(time.Now()), key, len(payload))
			return
		}
		channel = subjectChannel
	}

	message, envelope, err := publish.Decode(channel, payload)
	if err != nil {
		fmt.Printf("%s  %s  decode: %v\n", stamp(time.Now()), key, err)
		return
	}

	if raw {
		tap.writeRaw(key, envelope.PublishSeq, payload)
	}

	// Pub/Sub is fire and forget: a subscriber that fell behind, or one Redis
	// dropped for overrunning its output buffer, misses messages with no error
	// anywhere. A publish_seq jump is the only evidence; instance_id separates
	// a drop from a restart.
	if prev, ok := tap.seen[key]; ok {
		switch {
		case prev.instanceID != envelope.InstanceId:
			fmt.Printf("%s  %s  !! feed restarted: instance %s -> %s\n",
				stamp(time.Now()), key, short(prev.instanceID), short(envelope.InstanceId))
		case envelope.PublishSeq > prev.publishSequence+1:
			fmt.Printf("%s  %s  !! dropped %d message(s): publish_seq %d -> %d\n",
				stamp(time.Now()), key, envelope.PublishSeq-prev.publishSequence-1, prev.publishSequence, envelope.PublishSeq)
		}
	}
	tap.seen[key] = seen{publishSequence: envelope.PublishSeq, instanceID: envelope.InstanceId}

	if asJSON {
		body, err := protojson.MarshalOptions{}.Marshal(message)
		if err != nil {
			fmt.Printf("%s  %s  json: %v\n", stamp(time.Now()), key, err)
			return
		}
		fmt.Printf("{\"key\":%q,\"message\":%s}\n", key, body)
		return
	}

	fmt.Printf("%s  %-50s seq=%-6d %-8s %s\n",
		stamp(time.Unix(0, envelope.PublishTimeNs)), key, envelope.PublishSeq,
		core.StatusName(envelope.Status), summarize(message))
}

func (tap *tap) writeRaw(key string, sequence uint64, payload []byte) {
	name := fmt.Sprintf("%s-%06d.bin", strings.ReplaceAll(key, ":", "_"), sequence)
	if err := os.WriteFile(filepath.Join(tap.rawDir, name), payload, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "raw write: %v\n", err)
		return
	}
	tap.n++
}

// summarize is the few numbers per message worth reading at speed.
func summarize(message any) string {
	switch m := message.(type) {
	case *manoochv1.MarkPrice:
		return "mark=" + price.Price(m.MarkPrice).String()

	case *manoochv1.IndexPrice:
		return "index=" + price.Price(m.IndexPrice).String()

	case *manoochv1.Funding:
		// A zero next-funding time means the venue did not supply one, which is
		// not the same as a funding settlement in 1970.
		next := "-"
		if m.NextFundingTimeNs > 0 {
			next = time.Unix(0, m.NextFundingTimeNs).UTC().Format(time.RFC3339)
		}
		interval := "-"
		if m.IntervalSeconds > 0 {
			interval = fmt.Sprintf("%ds", m.IntervalSeconds)
		}
		return fmt.Sprintf("rate=%s next=%s interval=%s", price.Rate(m.FundingRate), next, interval)

	case *manoochv1.InstrumentMeta:
		return fmt.Sprintf("tick=%s lot=%s min=%s active=%v",
			price.Price(m.TickSize), price.Size(m.LotSize), price.Size(m.MinSize), m.Active)

	case *manoochv1.RateLimit:
		parts := make([]string, 0, len(m.Budgets))
		for _, budget := range m.Budgets {
			parts = append(parts, fmt.Sprintf("%s=%d/%d", budget.Kind, budget.Used, budget.Capacity))
		}
		return strings.Join(parts, " ")

	case *manoochv1.Health:
		s := fmt.Sprintf("status=%s age=%dms reconnects=%d skew=%dms",
			core.StatusName(m.Status), m.LastMessageAgeMs, m.ReconnectCount, m.ClockSkewMs)
		if m.Reason != "" {
			s += " reason=" + m.Reason
		}
		return s
	}
	return ""
}

func stamp(t time.Time) string { return t.UTC().Format("15:04:05.000") }

func short(id string) string {
	if len(id) > 8 {
		return id[:8]
	}
	return id
}
