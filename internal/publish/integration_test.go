//go:build integration

// Integration tests for the publisher, against a real Redis in a container.
//
// Not miniredis: these exercise TTL expiry timing, keyspace notifications and
// Pub/Sub buffer behaviour, which are the parts of Redis a reimplementation
// approximates rather than reproduces.
//
//	go test -tags=integration ./...
package publish_test

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/redis/go-redis/v9"
	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/obs"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/pkg/price"
	"google.golang.org/protobuf/proto"
)

const (
	testDB    = 0
	waitShort = 5 * time.Second
)

var redisAddr string

func TestMain(m *testing.M) {
	pool, err := dockertest.NewPool("")
	if err != nil {
		fmt.Fprintf(os.Stderr, "docker: %v\n", err)
		os.Exit(1)
	}
	pool.MaxWait = 90 * time.Second
	if err := pool.Client.Ping(); err != nil {
		fmt.Fprintf(os.Stderr, "docker not reachable: %v\n", err)
		os.Exit(1)
	}

	// The settings from deploy/redis.conf, which are what these tests exercise.
	res, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "redis",
		Tag:        "8-alpine",
		Cmd: []string{
			"redis-server",
			"--appendonly", "no",
			"--save", "",
			"--maxmemory-policy", "noeviction",
			"--notify-keyspace-events", "Ex",
		},
	}, func(cfg *docker.HostConfig) {
		cfg.AutoRemove = true
		cfg.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start redis: %v\n", err)
		os.Exit(1)
	}
	_ = res.Expire(600) // never outlive the test run by more than 10 minutes

	redisAddr = "127.0.0.1:" + res.GetPort("6379/tcp")
	if err := pool.Retry(func() error {
		c := redis.NewClient(&redis.Options{Addr: redisAddr})
		defer c.Close()
		return c.Ping(context.Background()).Err()
	}); err != nil {
		fmt.Fprintf(os.Stderr, "redis never became ready: %v\n", err)
		_ = pool.Purge(res)
		os.Exit(1)
	}

	code := m.Run()
	_ = pool.Purge(res)
	os.Exit(code)
}

// ---------- helpers ----------

func newPublisher(t *testing.T) *publish.RedisPublisher {
	t.Helper()
	p, err := publish.NewRedis(context.Background(), publish.Options{
		Addr:          redisAddr,
		DB:            testDB,
		DialTimeout:   2 * time.Second,
		ReadTimeout:   2 * time.Second,
		PoolSize:      8,
		Venue:         "TESTVENUE",
		InstanceID:    fmt.Sprintf("instance-%d", time.Now().UnixNano()),
		SchemaVersion: 1,
		Metrics:       obs.NewMetrics(),
		Logger:        slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	})
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	t.Cleanup(func() { p.Close() })
	return p
}

func newClient(t *testing.T) *redis.Client {
	t.Helper()
	c := redis.NewClient(&redis.Options{Addr: redisAddr, DB: testDB})
	t.Cleanup(func() { c.Close() })
	return c
}

func testKey(t *testing.T, ch pb.Channel) string {
	t.Helper()
	// One key per test, so tests cannot see each other's sequence numbers.
	sym := strings.ToUpper(strings.NewReplacer("/", "_", "-", "_").Replace(t.Name()))
	sym = strings.Map(func(r rune) rune {
		if (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') {
			return r
		}
		return -1
	}, sym)
	return publish.Key("TESTVENUE", pb.MarketType_MARKET_TYPE_PERP_LINEAR, sym+"_USDT", ch)
}

func mark(t *testing.T, symbol string) *pb.MarkPrice {
	t.Helper()
	ref, err := core.ParseCanonical(symbol, pb.MarketType_MARKET_TYPE_PERP_LINEAR)
	if err != nil {
		t.Fatal(err)
	}
	p, _ := price.ParsePrice("68432.15")
	now := time.Now()
	return &pb.MarkPrice{
		Env: &pb.Envelope{
			Venue:          "TESTVENUE",
			Instrument:     ref.Proto("TESTUSDT"),
			Channel:        pb.Channel_CHANNEL_MARK_PRICE,
			ExchangeTimeNs: now.Add(-10 * time.Millisecond).UnixNano(),
			RecvTimeNs:     now.UnixNano(),
			Source:         pb.Source_SOURCE_WEBSOCKET,
			Status:         pb.Status_STATUS_HEALTHY,
		},
		MarkPrice: int64(p),
	}
}

// ---------- tests ----------

// TestPublishWritesKeyAndChannel: one round trip leaves both a readable last
// value and a delivered notification.
func TestPublishWritesKeyAndChannel(t *testing.T) {
	ctx := context.Background()
	pub := newPublisher(t)
	rdb := newClient(t)
	key := testKey(t, pb.Channel_CHANNEL_MARK_PRICE)

	sub := rdb.Subscribe(ctx, key)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	msg := mark(t, "BTC_USDT")
	if err := pub.Publish(ctx, key, msg, 5*time.Second); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// The publisher owns these; the caller must not have to set them.
	env := msg.GetEnv()
	if env.PublishSeq != 1 {
		t.Errorf("publish_seq = %d, want 1", env.PublishSeq)
	}
	if env.InstanceId == "" {
		t.Error("instance_id not set")
	}
	if env.SchemaVersion != 1 {
		t.Errorf("schema_version = %d, want 1", env.SchemaVersion)
	}
	if env.PublishTimeNs < env.RecvTimeNs {
		t.Errorf("publish_time_ns %d precedes recv_time_ns %d", env.PublishTimeNs, env.RecvTimeNs)
	}

	// Delivered over Pub/Sub.
	select {
	case m := <-sub.Channel():
		var got pb.MarkPrice
		if err := proto.Unmarshal([]byte(m.Payload), &got); err != nil {
			t.Fatalf("unmarshal published: %v", err)
		}
		if !proto.Equal(&got, msg) {
			t.Errorf("published message differs from what was sent")
		}
	case <-time.After(waitShort):
		t.Fatal("no message delivered on the Pub/Sub channel")
	}

	// And readable as the last value, which is what a cold consumer gets.
	b, err := rdb.Get(ctx, key).Bytes()
	if err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	var cached pb.MarkPrice
	if err := proto.Unmarshal(b, &cached); err != nil {
		t.Fatalf("unmarshal cached: %v", err)
	}
	if !proto.Equal(&cached, msg) {
		t.Errorf("cached message differs from what was sent")
	}
}

// TestKeyExpiresAndNotifies: the key going away is itself the event, which is
// what M2's REST fallback will be triggered by.
func TestKeyExpiresAndNotifies(t *testing.T) {
	ctx := context.Background()
	pub := newPublisher(t)
	rdb := newClient(t)
	key := testKey(t, pb.Channel_CHANNEL_MARK_PRICE)

	events := rdb.PSubscribe(ctx, fmt.Sprintf("__keyevent@%d__:expired", testDB))
	defer events.Close()
	if _, err := events.Receive(ctx); err != nil {
		t.Fatalf("subscribe to keyspace events: %v", err)
	}

	const ttl = 300 * time.Millisecond
	if err := pub.Publish(ctx, key, mark(t, "BTC_USDT"), ttl); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	pttl, err := rdb.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}
	if pttl <= 0 || pttl > ttl {
		t.Errorf("PTTL = %v, want (0, %v]", pttl, ttl)
	}

	deadline := time.After(waitShort)
	for {
		select {
		case m := <-events.Channel():
			if m.Payload == key {
				return // expired, and said so
			}
		case <-deadline:
			t.Fatalf("no expired event for %s within %v", key, waitShort)
		}
	}
}

// TestZeroTTLPersists covers a channel with no cadence of its own, where an
// expiring key would call a working stream dead.
func TestZeroTTLPersists(t *testing.T) {
	ctx := context.Background()
	pub := newPublisher(t)
	rdb := newClient(t)
	key := testKey(t, pb.Channel_CHANNEL_METADATA)

	if err := pub.Publish(ctx, key, mark(t, "BTC_USDT"), 0); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	t.Cleanup(func() { rdb.Del(context.Background(), key) })

	pttl, err := rdb.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}
	// -1 is go-redis for "exists, no expiry".
	if pttl != -1 {
		t.Errorf("PTTL = %v, want -1 (no expiry)", pttl)
	}
}

// TestPublishSeqIsGapFree covers what a consumer relies on to detect bus-side
// drops: without it, a slow subscriber's missing messages are indistinguishable
// from a quiet market.
func TestPublishSeqIsGapFree(t *testing.T) {
	ctx := context.Background()
	pub := newPublisher(t)
	rdb := newClient(t)
	key := testKey(t, pb.Channel_CHANNEL_MARK_PRICE)

	const n = 10_000
	sub := rdb.Subscribe(ctx, key)
	defer sub.Close()
	if _, err := sub.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	ch := sub.ChannelSize(n + 1000)

	msg := mark(t, "BTC_USDT")
	for i := 1; i <= n; i++ {
		if err := pub.Publish(ctx, key, msg, time.Minute); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
		if got := msg.GetEnv().PublishSeq; got != uint64(i) {
			t.Fatalf("publish %d was assigned publish_seq %d", i, got)
		}
	}

	// And the same sequence arrives on the wire, in order and complete.
	var last uint64
	deadline := time.After(30 * time.Second)
	for last < n {
		select {
		case m := <-ch:
			var got pb.MarkPrice
			if err := proto.Unmarshal([]byte(m.Payload), &got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if seq := got.GetEnv().PublishSeq; seq != last+1 {
				t.Fatalf("publish_seq jumped from %d to %d", last, seq)
			}
			last++
		case <-deadline:
			t.Fatalf("only %d of %d messages delivered", last, n)
		}
	}
}

// TestInstanceIDDistinguishesRestartFromDrop: publish_seq restarts at zero on
// every process start, so instance_id is the only thing separating a restart
// from ten thousand missed messages.
func TestInstanceIDDistinguishesRestartFromDrop(t *testing.T) {
	ctx := context.Background()
	key := testKey(t, pb.Channel_CHANNEL_MARK_PRICE)

	first := newPublisher(t)
	msg := mark(t, "BTC_USDT")
	for i := 1; i <= 3; i++ {
		if err := first.Publish(ctx, key, msg, time.Minute); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	firstInstance := msg.GetEnv().InstanceId
	if msg.GetEnv().PublishSeq != 3 {
		t.Fatalf("publish_seq = %d, want 3", msg.GetEnv().PublishSeq)
	}

	second := newPublisher(t)
	msg2 := mark(t, "BTC_USDT")
	if err := second.Publish(ctx, key, msg2, time.Minute); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := msg2.GetEnv().PublishSeq; got != 1 {
		t.Errorf("publish_seq after restart = %d, want 1", got)
	}
	if got := msg2.GetEnv().InstanceId; got == firstInstance {
		t.Errorf("instance_id unchanged across publishers: %q", got)
	}
}

// TestNoEvictionSurfacesWriteErrors is why deploy/redis.conf sets
// maxmemory-policy to noeviction: under any eviction policy a full instance
// drops last-value keys and working streams read as stale.
func TestNoEvictionSurfacesWriteErrors(t *testing.T) {
	ctx := context.Background()
	pub := newPublisher(t)
	rdb := newClient(t)
	key := testKey(t, pb.Channel_CHANNEL_MARK_PRICE)

	t.Cleanup(func() {
		bg := context.Background()
		rdb.ConfigSet(bg, "maxmemory", "0")
		rdb.Del(bg, "filler")
	})

	// Fill past the cap we are about to impose.
	filler := strings.Repeat("x", 64*1024)
	for i := range 100 {
		if err := rdb.Set(ctx, fmt.Sprintf("filler:%d", i), filler, time.Minute).Err(); err != nil {
			t.Fatalf("filling: %v", err)
		}
	}
	t.Cleanup(func() {
		bg := context.Background()
		for i := range 100 {
			rdb.Del(bg, fmt.Sprintf("filler:%d", i))
		}
	})

	if err := rdb.ConfigSet(ctx, "maxmemory", "2mb").Err(); err != nil {
		t.Fatalf("CONFIG SET maxmemory: %v", err)
	}

	err := pub.Publish(ctx, key, mark(t, "BTC_USDT"), time.Minute)
	if err == nil {
		t.Fatal("Publish succeeded against an exhausted noeviction instance")
	}
	if !strings.Contains(strings.ToUpper(err.Error()), "OOM") {
		t.Errorf("error does not look like a Redis OOM refusal: %v", err)
	}
}
