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
	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/observability"
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
	resource, err := pool.RunWithOptions(&dockertest.RunOptions{
		Repository: "redis",
		Tag:        "8-alpine",
		Cmd: []string{
			"redis-server",
			"--appendonly", "no",
			"--save", "",
			"--maxmemory-policy", "noeviction",
			"--notify-keyspace-events", "Ex",
		},
	}, func(configuration *docker.HostConfig) {
		configuration.AutoRemove = true
		configuration.RestartPolicy = docker.RestartPolicy{Name: "no"}
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "start redis: %v\n", err)
		os.Exit(1)
	}
	_ = resource.Expire(600) // never outlive the test run by more than 10 minutes

	redisAddr = "127.0.0.1:" + resource.GetPort("6379/tcp")
	if err := pool.Retry(func() error {
		redisClient := redis.NewClient(&redis.Options{Addr: redisAddr})
		defer redisClient.Close()
		return redisClient.Ping(context.Background()).Err()
	}); err != nil {
		fmt.Fprintf(os.Stderr, "redis never became ready: %v\n", err)
		_ = pool.Purge(resource)
		os.Exit(1)
	}

	code := m.Run()
	_ = pool.Purge(resource)
	os.Exit(code)
}

// ---------- helpers ----------

func newPublisher(t *testing.T) *publish.RedisPublisher {
	t.Helper()
	publisher, err := publish.NewRedis(context.Background(), publish.Options{
		Addr:          redisAddr,
		Database:      testDB,
		DialTimeout:   2 * time.Second,
		ReadTimeout:   2 * time.Second,
		PoolSize:      8,
		Venue:         "TESTVENUE",
		InstanceID:    fmt.Sprintf("instance-%d", time.Now().UnixNano()),
		SchemaVersion: 2,
		Metrics:       observability.NewMetrics(),
		Logger:        slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelError})),
	})
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	t.Cleanup(func() { publisher.Close() })
	return publisher
}

func newClient(t *testing.T) *redis.Client {
	t.Helper()
	redisClient := redis.NewClient(&redis.Options{Addr: redisAddr, DB: testDB})
	t.Cleanup(func() { redisClient.Close() })
	return redisClient
}

func testKey(t *testing.T, channel manoochv1.Channel) string {
	t.Helper()
	// One key per test, so tests cannot see each other's sequence numbers.
	symbol := strings.ToUpper(strings.NewReplacer("/", "_", "-", "_").Replace(t.Name()))
	symbol = strings.Map(func(letter rune) rune {
		if (letter >= 'A' && letter <= 'Z') || (letter >= '0' && letter <= '9') {
			return letter
		}
		return -1
	}, symbol)
	return publish.Key("TESTVENUE", manoochv1.MarketType_MARKET_TYPE_PERP_LINEAR, symbol+"_USDT", channel)
}

func mark(t *testing.T, symbol string) *manoochv1.MarkPrice {
	t.Helper()
	ref, err := core.ParseCanonical(symbol, manoochv1.MarketType_MARKET_TYPE_PERP_LINEAR)
	if err != nil {
		t.Fatal(err)
	}
	price, _ := price.ParsePrice("68432.15")
	now := time.Now()
	return &manoochv1.MarkPrice{
		Env: &manoochv1.Envelope{
			Venue:          "TESTVENUE",
			Instrument:     ref.Proto("TESTUSDT"),
			Channel:        manoochv1.Channel_CHANNEL_MARK_PRICE,
			ExchangeTimeNs: now.Add(-10 * time.Millisecond).UnixNano(),
			RecvTimeNs:     now.UnixNano(),
			Source:         manoochv1.Source_SOURCE_WEBSOCKET,
			Status:         manoochv1.Status_STATUS_HEALTHY,
		},
		MarkPrice: int64(price),
	}
}

// ---------- tests ----------

// TestPublishWritesKeyAndChannel: one round trip leaves both a readable last
// value and a delivered notification.
func TestPublishWritesKeyAndChannel(t *testing.T) {
	ctx := context.Background()
	publisher := newPublisher(t)
	redisClient := newClient(t)
	key := testKey(t, manoochv1.Channel_CHANNEL_MARK_PRICE)

	subscription := redisClient.Subscribe(ctx, key)
	defer subscription.Close()
	if _, err := subscription.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}

	markPrice := mark(t, "BTC_USDT")
	if err := publisher.Publish(ctx, key, markPrice, 5*time.Second); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	// The publisher owns these; the caller must not have to set them.
	envelope := markPrice.GetEnv()
	if envelope.PublishSeq != 1 {
		t.Errorf("publish_seq = %d, want 1", envelope.PublishSeq)
	}
	if envelope.InstanceId == "" {
		t.Error("instance_id not set")
	}
	if envelope.SchemaVersion != 2 {
		t.Errorf("schema_version = %d, want 2", envelope.SchemaVersion)
	}
	if envelope.PublishTimeNs < envelope.RecvTimeNs {
		t.Errorf("publish_time_ns %d precedes recv_time_ns %d", envelope.PublishTimeNs, envelope.RecvTimeNs)
	}

	// Delivered over Pub/Sub.
	select {
	case message := <-subscription.Channel():
		var got manoochv1.MarkPrice
		if err := proto.Unmarshal([]byte(message.Payload), &got); err != nil {
			t.Fatalf("unmarshal published: %v", err)
		}
		if !proto.Equal(&got, markPrice) {
			t.Errorf("published message differs from what was sent")
		}
	case <-time.After(waitShort):
		t.Fatal("no message delivered on the Pub/Sub channel")
	}

	// And readable as the last value, which is what a cold consumer gets.
	data, err := redisClient.Get(ctx, key).Bytes()
	if err != nil {
		t.Fatalf("GET %s: %v", key, err)
	}
	var cached manoochv1.MarkPrice
	if err := proto.Unmarshal(data, &cached); err != nil {
		t.Fatalf("unmarshal cached: %v", err)
	}
	if !proto.Equal(&cached, markPrice) {
		t.Errorf("cached message differs from what was sent")
	}
}

// TestKeyExpiresAndNotifies: the key going away is itself the event, which is
// what M2's REST fallback will be triggered by.
func TestKeyExpiresAndNotifies(t *testing.T) {
	ctx := context.Background()
	publisher := newPublisher(t)
	redisClient := newClient(t)
	key := testKey(t, manoochv1.Channel_CHANNEL_MARK_PRICE)

	subscription := redisClient.PSubscribe(ctx, fmt.Sprintf("__keyevent@%d__:expired", testDB))
	defer subscription.Close()
	if _, err := subscription.Receive(ctx); err != nil {
		t.Fatalf("subscribe to keyspace events: %v", err)
	}

	const timeToLive = 300 * time.Millisecond
	if err := publisher.Publish(ctx, key, mark(t, "BTC_USDT"), timeToLive); err != nil {
		t.Fatalf("Publish: %v", err)
	}

	pttl, err := redisClient.PTTL(ctx, key).Result()
	if err != nil {
		t.Fatalf("PTTL: %v", err)
	}
	if pttl <= 0 || pttl > timeToLive {
		t.Errorf("PTTL = %v, want (0, %v]", pttl, timeToLive)
	}

	deadline := time.After(waitShort)
	for {
		select {
		case message := <-subscription.Channel():
			if message.Payload == key {
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
	publisher := newPublisher(t)
	redisClient := newClient(t)
	key := testKey(t, manoochv1.Channel_CHANNEL_METADATA)

	if err := publisher.Publish(ctx, key, mark(t, "BTC_USDT"), 0); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	t.Cleanup(func() { redisClient.Del(context.Background(), key) })

	pttl, err := redisClient.PTTL(ctx, key).Result()
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
	publisher := newPublisher(t)
	redisClient := newClient(t)
	key := testKey(t, manoochv1.Channel_CHANNEL_MARK_PRICE)

	const messageCount = 10_000
	subscription := redisClient.Subscribe(ctx, key)
	defer subscription.Close()
	if _, err := subscription.Receive(ctx); err != nil {
		t.Fatalf("subscribe: %v", err)
	}
	messages := subscription.ChannelSize(messageCount + 1000)

	markPrice := mark(t, "BTC_USDT")
	for i := 1; i <= messageCount; i++ {
		if err := publisher.Publish(ctx, key, markPrice, time.Minute); err != nil {
			t.Fatalf("Publish %d: %v", i, err)
		}
		if got := markPrice.GetEnv().PublishSeq; got != uint64(i) {
			t.Fatalf("publish %d was assigned publish_seq %d", i, got)
		}
	}

	// And the same sequence arrives on the wire, in order and complete.
	var last uint64
	deadline := time.After(30 * time.Second)
	for last < messageCount {
		select {
		case message := <-messages:
			var got manoochv1.MarkPrice
			if err := proto.Unmarshal([]byte(message.Payload), &got); err != nil {
				t.Fatalf("unmarshal: %v", err)
			}
			if sequence := got.GetEnv().PublishSeq; sequence != last+1 {
				t.Fatalf("publish_seq jumped from %d to %d", last, sequence)
			}
			last++
		case <-deadline:
			t.Fatalf("only %d of %d messages delivered", last, messageCount)
		}
	}
}

// TestInstanceIDDistinguishesRestartFromDrop: publish_seq restarts at zero on
// every process start, so instance_id is the only thing separating a restart
// from ten thousand missed messages.
func TestInstanceIDDistinguishesRestartFromDrop(t *testing.T) {
	ctx := context.Background()
	key := testKey(t, manoochv1.Channel_CHANNEL_MARK_PRICE)

	first := newPublisher(t)
	message := mark(t, "BTC_USDT")
	for i := 1; i <= 3; i++ {
		if err := first.Publish(ctx, key, message, time.Minute); err != nil {
			t.Fatalf("Publish: %v", err)
		}
	}
	firstInstance := message.GetEnv().InstanceId
	if message.GetEnv().PublishSeq != 3 {
		t.Fatalf("publish_seq = %d, want 3", message.GetEnv().PublishSeq)
	}

	second := newPublisher(t)
	secondMessage := mark(t, "BTC_USDT")
	if err := second.Publish(ctx, key, secondMessage, time.Minute); err != nil {
		t.Fatalf("Publish: %v", err)
	}
	if got := secondMessage.GetEnv().PublishSeq; got != 1 {
		t.Errorf("publish_seq after restart = %d, want 1", got)
	}
	if got := secondMessage.GetEnv().InstanceId; got == firstInstance {
		t.Errorf("instance_id unchanged across publishers: %q", got)
	}
}

// TestNoEvictionSurfacesWriteErrors is why deploy/redis.conf sets
// maxmemory-policy to noeviction: under any eviction policy a full instance
// drops last-value keys and working streams read as stale.
func TestNoEvictionSurfacesWriteErrors(t *testing.T) {
	ctx := context.Background()
	publisher := newPublisher(t)
	redisClient := newClient(t)
	key := testKey(t, manoochv1.Channel_CHANNEL_MARK_PRICE)

	t.Cleanup(func() {
		background := context.Background()
		redisClient.ConfigSet(background, "maxmemory", "0")
		redisClient.Del(background, "filler")
	})

	// Fill past the cap we are about to impose.
	filler := strings.Repeat("x", 64*1024)
	for i := range 100 {
		if err := redisClient.Set(ctx, fmt.Sprintf("filler:%d", i), filler, time.Minute).Err(); err != nil {
			t.Fatalf("filling: %v", err)
		}
	}
	t.Cleanup(func() {
		background := context.Background()
		for i := range 100 {
			redisClient.Del(background, fmt.Sprintf("filler:%d", i))
		}
	})

	if err := redisClient.ConfigSet(ctx, "maxmemory", "2mb").Err(); err != nil {
		t.Fatalf("CONFIG SET maxmemory: %v", err)
	}

	err := publisher.Publish(ctx, key, mark(t, "BTC_USDT"), time.Minute)
	if err == nil {
		t.Fatal("Publish succeeded against an exhausted noeviction instance")
	}
	if !strings.Contains(strings.ToUpper(err.Error()), "OOM") {
		t.Errorf("error does not look like a Redis OOM refusal: %v", err)
	}
}
