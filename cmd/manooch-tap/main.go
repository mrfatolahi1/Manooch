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
	pb "github.com/you/manooch/gen/manoochv1"
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
		pattern = flag.String("pattern", publish.MatchPattern(""), "Pub/Sub pattern to subscribe to")
		addr    = flag.String("redis", "127.0.0.1:6379", "Redis address")
		db      = flag.Int("db", 0, "Redis database")
		asJSON  = flag.Bool("json", false, "print each message as JSON")
		raw     = flag.Bool("raw", false, "write raw message bytes to --out, for building test fixtures")
		out     = flag.String("out", filepath.Join("testdata", "raw"), "directory for --raw output")
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

	rdb := redis.NewClient(&redis.Options{Addr: *addr, DB: *db})
	defer rdb.Close()
	if err := rdb.Ping(ctx).Err(); err != nil {
		return fmt.Errorf("redis %s: %w", *addr, err)
	}

	sub := rdb.PSubscribe(ctx, *pattern)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		return fmt.Errorf("subscribe %s: %w", *pattern, err)
	}
	fmt.Fprintf(os.Stderr, "subscribed to %s\n", *pattern)

	t := &tap{seen: map[string]seen{}, rawDir: *out}
	for {
		select {
		case <-ctx.Done():
			return nil
		case m, ok := <-sub.Channel():
			if !ok {
				return nil
			}
			t.handle(m.Channel, []byte(m.Payload), *asJSON, *raw)
		}
	}
}

// seen is the last message on a topic, so gaps are spotted as they go past.
type seen struct {
	publishSeq uint64
	instanceID string
}

type tap struct {
	seen   map[string]seen
	rawDir string
	n      int
}

func (t *tap) handle(key string, payload []byte, asJSON, raw bool) {
	parts, err := publish.ParseKey(key)
	if err != nil {
		fmt.Printf("%s  ?? %v\n", stamp(time.Now()), err)
		return
	}
	ch := parts.Channel
	if parts.VenueScoped {
		if parts.Subject != publish.SubjectHealth {
			fmt.Printf("%s  %s  %d bytes\n", stamp(time.Now()), key, len(payload))
			return
		}
		ch = pb.Channel_CHANNEL_HEALTH
	}

	msg, env, err := publish.Decode(ch, payload)
	if err != nil {
		fmt.Printf("%s  %s  decode: %v\n", stamp(time.Now()), key, err)
		return
	}

	if raw {
		t.writeRaw(key, env.PublishSeq, payload)
	}

	// Pub/Sub is fire and forget: a subscriber that fell behind, or one Redis
	// dropped for overrunning its output buffer, misses messages with no error
	// anywhere. A publish_seq jump is the only evidence; instance_id separates
	// a drop from a restart.
	if prev, ok := t.seen[key]; ok {
		switch {
		case prev.instanceID != env.InstanceId:
			fmt.Printf("%s  %s  !! feed restarted: instance %s -> %s\n",
				stamp(time.Now()), key, short(prev.instanceID), short(env.InstanceId))
		case env.PublishSeq > prev.publishSeq+1:
			fmt.Printf("%s  %s  !! dropped %d message(s): publish_seq %d -> %d\n",
				stamp(time.Now()), key, env.PublishSeq-prev.publishSeq-1, prev.publishSeq, env.PublishSeq)
		}
	}
	t.seen[key] = seen{publishSeq: env.PublishSeq, instanceID: env.InstanceId}

	if asJSON {
		body, err := protojson.MarshalOptions{}.Marshal(msg)
		if err != nil {
			fmt.Printf("%s  %s  json: %v\n", stamp(time.Now()), key, err)
			return
		}
		fmt.Printf("{\"key\":%q,\"message\":%s}\n", key, body)
		return
	}

	fmt.Printf("%s  %-50s seq=%-6d %-8s %s\n",
		stamp(time.Unix(0, env.PublishTimeNs)), key, env.PublishSeq,
		core.StatusName(env.Status), summarize(msg))
}

func (t *tap) writeRaw(key string, seq uint64, payload []byte) {
	name := fmt.Sprintf("%s-%06d.bin", strings.ReplaceAll(key, ":", "_"), seq)
	if err := os.WriteFile(filepath.Join(t.rawDir, name), payload, 0o644); err != nil {
		fmt.Fprintf(os.Stderr, "raw write: %v\n", err)
		return
	}
	t.n++
}

// summarize is the few numbers per message worth reading at speed.
func summarize(msg any) string {
	switch m := msg.(type) {
	case *pb.MarkPrice:
		return "mark=" + price.Price(m.MarkPrice).String()

	case *pb.IndexPrice:
		return "index=" + price.Price(m.IndexPrice).String()

	case *pb.Funding:
		return fmt.Sprintf("rate=%s next=%s interval=%ds",
			price.Rate(m.FundingRate), time.Unix(0, m.NextFundingTimeNs).UTC().Format(time.RFC3339),
			m.IntervalSeconds)

	case *pb.InstrumentMeta:
		return fmt.Sprintf("tick=%s lot=%s min=%s active=%v",
			price.Price(m.TickSize), price.Size(m.LotSize), price.Size(m.MinSize), m.Active)

	case *pb.Health:
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
