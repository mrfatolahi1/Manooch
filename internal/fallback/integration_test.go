//go:build integration

// Integration tests for the M2 reliability path, against a real Redis in a
// container.
//
// Not miniredis: this phase rests on TTL expiry timing, keyspace notifications
// and Pub/Sub delivery, which are the parts of Redis a reimplementation
// approximates rather than reproduces — and approximating them would test the
// approximation.
//
//	go test -tags=integration ./...
package fallback_test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/redis/go-redis/v9"
	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/core/coretest"
	"github.com/you/manooch/internal/fallback"
	"github.com/you/manooch/internal/health"
	"github.com/you/manooch/internal/observability"
	"github.com/you/manooch/internal/publish"
)

const testDB = 0

// markOnly narrows a test to one channel per instrument.
var markOnly = []manoochv1.Channel{manoochv1.Channel_CHANNEL_MARK_PRICE}

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

	// The settings from deploy/redis.conf, which are what these exercise.
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
	_ = resource.Expire(600)

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

// ---------- harness ----------

type live struct {
	publisher      *publish.RedisPublisher
	redisClient    *redis.Client
	adapter        *coretest.Adapter
	tracker        *health.Tracker
	watcher        *fallback.Watcher
	specifications []core.StreamSpec

	expiries chan core.StreamSpec
}

type options struct {
	symbols []string
	// channels narrows the streams under test. A key that has never been
	// written is indistinguishable from one that expired — which is correct,
	// and makes a single-channel assertion noisy unless the others are simply
	// not configured.
	channels      []manoochv1.Channel
	timeToLive    time.Duration
	heartbeat     time.Duration
	maxConcurrent int
	maxDuration   time.Duration
	sweepInterval time.Duration
	pollInterval  time.Duration
	// subscribe false leaves the watcher's Run unstarted, so only what the
	// caller drives by hand happens.
	run bool
}

func newLive(t *testing.T, options options) *live {
	t.Helper()

	if len(options.symbols) == 0 {
		options.symbols = []string{"BTC_USDT"}
	}
	if options.timeToLive == 0 {
		options.timeToLive = 300 * time.Millisecond
	}
	if options.heartbeat == 0 {
		options.heartbeat = 200 * time.Millisecond
	}
	if options.maxConcurrent == 0 {
		options.maxConcurrent = 4
	}
	if options.maxDuration == 0 {
		options.maxDuration = 5 * time.Minute
	}
	if options.sweepInterval == 0 {
		options.sweepInterval = 100 * time.Millisecond
	}
	if options.pollInterval == 0 {
		options.pollInterval = 50 * time.Millisecond
	}

	specifications, err := coretest.Specifications(options.symbols...)
	if err != nil {
		t.Fatal(err)
	}
	if len(options.channels) > 0 {
		var narrowed []core.StreamSpec
		for _, specification := range specifications {
			for _, channel := range options.channels {
				if specification.Channel == channel {
					narrowed = append(narrowed, specification)
				}
			}
		}
		specifications = narrowed
	}

	publisher, err := publish.NewRedis(context.Background(), publish.Options{
		Addr:          redisAddr,
		Database:      testDB,
		DialTimeout:   2 * time.Second,
		ReadTimeout:   2 * time.Second,
		PoolSize:      8,
		Venue:         coretest.Venue,
		InstanceID:    fmt.Sprintf("instance-%d", time.Now().UnixNano()),
		SchemaVersion: 2,
		Metrics:       observability.NewMetrics(),
		Logger:        quiet(),
	})
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	t.Cleanup(func() { publisher.Close() })

	harness := &live{
		publisher:      publisher,
		redisClient:    publisher.Redis(),
		adapter:        &coretest.Adapter{TimeToLive: options.timeToLive, Channels: options.channels},
		specifications: specifications,
		expiries:       make(chan core.StreamSpec, 64),
	}

	// Every test starts from a clean keyspace, or one test's leftovers are
	// another's "already expired".
	harness.clear(t)
	t.Cleanup(func() { harness.clear(t) })

	harness.tracker, err = health.New(health.Options{
		Venue:               coretest.Venue,
		Publisher:           publisher,
		Metrics:             observability.NewMetrics(),
		Log:                 quiet(),
		HeartbeatInterval:   options.heartbeat,
		ClockSkewDegradedMS: 2000,
		ClockSkewStaleMS:    10000,
		FallbackMaxDuration: options.maxDuration,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, specification := range specifications {
		symbol, _ := harness.adapter.VenueSymbol(specification.Instrument)
		harness.tracker.Register(specification, symbol, "test-0")
	}
	harness.tracker.SocketState("test-0", health.SocketConnected, "")

	harness.watcher, err = fallback.New(fallback.Options{
		Venue:              coretest.Venue,
		Adapter:            harness.adapter,
		Publisher:          publisher,
		Redis:              harness.redisClient,
		Database:           testDB,
		Health:             harness.tracker,
		Metrics:            observability.NewMetrics(),
		Log:                quiet(),
		Specifications:     specifications,
		MaxConcurrentPolls: options.maxConcurrent,
		PollInterval:       options.pollInterval,
		SweepInterval:      options.sweepInterval,
		MaxDuration:        options.maxDuration,
		OnExpired: func(specification core.StreamSpec) {
			select {
			case harness.expiries <- specification:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if options.run {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); harness.watcher.Run(ctx) }()
		t.Cleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(settle):
				t.Error("watcher Run did not return")
			}
		})
	}
	return harness
}

// clear removes every key this venue owns.
func (harness *live) clear(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	iter := harness.redisClient.Scan(ctx, 0, publish.MatchPattern(coretest.Venue), 500).Iterator()
	for iter.Next(ctx) {
		harness.redisClient.Del(ctx, iter.Val())
	}
}

// socketMessage publishes a stream the way the supervisor would, which is what
// makes its key exist and start counting down.
func (harness *live) socketMessage(t *testing.T, specification core.StreamSpec) {
	t.Helper()
	harness.watcher.Note(specification)
	harness.tracker.Received(specification)

	message := harness.adapter.Message(specification, time.Now().UnixNano(), manoochv1.Source_SOURCE_WEBSOCKET)
	envelope := message.Proto.(interface{ GetEnv() *manoochv1.Envelope }).GetEnv()
	envelope.Status, envelope.StatusReason = harness.tracker.Status(specification)

	if err := harness.publisher.Publish(context.Background(), message.Key, message.Proto, message.TimeToLive); err != nil {
		t.Fatalf("publish %s: %v", message.Key, err)
	}
}

// envelope reads a key back the way a consumer would.
func (harness *live) envelope(t *testing.T, key string, channel manoochv1.Channel) *manoochv1.Envelope {
	t.Helper()
	data, err := harness.redisClient.Get(context.Background(), key).Bytes()
	if err != nil {
		return nil
	}
	_, envelope, err := publish.Decode(channel, data)
	if err != nil {
		t.Fatalf("decode %s: %v", key, err)
	}
	return envelope
}

func (harness *live) exists(key string) bool {
	count, err := harness.redisClient.Exists(context.Background(), key).Result()
	return err == nil && count > 0
}

// ---------- tests ----------

// TestKeyExpiryIsTheTrigger: the TTL is the freshness signal, and the key going
// away is the event everything downstream reacts to.
func TestKeyExpiryIsTheTrigger(t *testing.T) {
	harness := newLive(t, options{run: true, timeToLive: 300 * time.Millisecond, channels: markOnly})
	specification := harness.specifications[0]
	streamKey := key(specification)

	harness.socketMessage(t, specification)
	if !harness.exists(streamKey) {
		t.Fatalf("%s does not exist after publishing it", streamKey)
	}
	if pttl, err := harness.redisClient.PTTL(context.Background(), streamKey).Result(); err != nil || pttl <= 0 || pttl > 300*time.Millisecond {
		t.Fatalf("PTTL = %v (%v), want (0, 300ms]", pttl, err)
	}

	select {
	case got := <-harness.expiries:
		if got != specification {
			t.Errorf("expiry reported for %s, want %s", got, specification)
		}
	case <-time.After(settle):
		t.Fatalf("no expiry reported for %s", streamKey)
	}
}

// TestSweepFindsWhatTheNotificationMissed: expiry events are Pub/Sub, so they
// are fire-and-forget and can simply not arrive. Without the backstop a missed
// notification is a stream that is never served again.
func TestSweepFindsWhatTheNotificationMissed(t *testing.T) {
	ctx := context.Background()

	// Notifications off entirely, which is the strongest form of "the event
	// did not arrive": nothing is published to the keyspace channel at all.
	harness := newLive(t, options{timeToLive: 200 * time.Millisecond, sweepInterval: 100 * time.Millisecond, channels: markOnly})
	if err := harness.redisClient.ConfigSet(ctx, "notify-keyspace-events", "").Err(); err != nil {
		t.Fatalf("disabling keyspace events: %v", err)
	}
	t.Cleanup(func() { harness.redisClient.ConfigSet(context.Background(), "notify-keyspace-events", "Ex") })

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); harness.watcher.Run(runCtx) }()
	t.Cleanup(func() { cancel(); <-done })

	specification := harness.specifications[0]
	harness.socketMessage(t, specification)

	select {
	case got := <-harness.expiries:
		if got != specification {
			t.Errorf("expiry reported for %s, want %s", got, specification)
		}
	case <-time.After(settle):
		t.Fatal("the sweep did not find an expired key with notifications disabled")
	}
}

// TestFallbackEngagesAndDisengages is the acceptance criterion end to end: the
// key expires, REST takes over and says so, and a websocket message hands it
// back with no restart anywhere.
func TestFallbackEngagesAndDisengages(t *testing.T) {
	harness := newLive(t, options{run: true, timeToLive: 200 * time.Millisecond, channels: markOnly})
	specification := harness.specifications[0]
	streamKey := key(specification)

	harness.socketMessage(t, specification)

	// The key comes back, written by REST and labelled as such.
	eventually(t, "fallback to republish the key", func() bool {
		envelope := harness.envelope(t, streamKey, specification.Channel)
		return envelope != nil && envelope.Source == manoochv1.Source_SOURCE_REST
	})
	envelope := harness.envelope(t, streamKey, specification.Channel)
	if envelope.Status != manoochv1.Status_STATUS_DEGRADED {
		t.Errorf("status = %s, want DEGRADED", core.StatusName(envelope.Status))
	}
	if envelope.StatusReason == "" {
		t.Error("DEGRADED published with no reason")
	}
	if harness.watcher.Active() != 1 {
		t.Errorf("%d pollers active, want 1", harness.watcher.Active())
	}

	// The socket comes back. Nothing restarts; the source simply returns.
	harness.socketMessage(t, specification)
	if harness.watcher.Active() != 0 {
		t.Errorf("%d pollers still active after a websocket message", harness.watcher.Active())
	}
	envelope = harness.envelope(t, streamKey, specification.Channel)
	if envelope.Source != manoochv1.Source_SOURCE_WEBSOCKET {
		t.Errorf("source = %s, want WEBSOCKET", core.SourceName(envelope.Source))
	}
	if envelope.Status != manoochv1.Status_STATUS_HEALTHY {
		t.Errorf("status = %s (%q), want HEALTHY", core.StatusName(envelope.Status), envelope.StatusReason)
	}
}

// TestFallbackPastMaxDurationGoesStale: long-running fallback is a failure, not
// a steady state, and the value on the wire has to say so.
func TestFallbackPastMaxDurationGoesStale(t *testing.T) {
	harness := newLive(t, options{run: true, timeToLive: 200 * time.Millisecond, maxDuration: 500 * time.Millisecond, channels: markOnly})
	specification := harness.specifications[0]
	streamKey := key(specification)

	harness.socketMessage(t, specification)

	eventually(t, "fallback to escalate to stale", func() bool {
		envelope := harness.envelope(t, streamKey, specification.Channel)
		return envelope != nil && envelope.Status == manoochv1.Status_STATUS_STALE
	})

	// Still REST, still being published: giving up entirely would leave a
	// consumer with no value rather than one labelled not to trade on.
	envelope := harness.envelope(t, streamKey, specification.Channel)
	if envelope.Source != manoochv1.Source_SOURCE_REST {
		t.Errorf("source = %s, want REST", core.SourceName(envelope.Source))
	}
}

// TestConcurrencyCapLeavesTheExcessStale: past the cap a stream is turned away
// rather than queued, because a queued poll arrives after it stopped being
// worth having and is published as though it were current.
func TestConcurrencyCapLeavesTheExcessStale(t *testing.T) {
	harness := newLive(t, options{run: true, timeToLive: 200 * time.Millisecond, maxConcurrent: 2})

	for _, specification := range harness.specifications {
		harness.socketMessage(t, specification)
	}

	eventually(t, "the cap to be reached", func() bool { return harness.watcher.Active() == 2 })

	eventually(t, "the excess stream to go stale", func() bool {
		stale := 0
		for _, specification := range harness.specifications {
			if status, reason := harness.tracker.Status(specification); status == manoochv1.Status_STATUS_STALE && reason == "fallback at capacity" {
				stale++
			}
		}
		return stale == 1
	})

	if got := harness.watcher.Active(); got != 2 {
		t.Errorf("%d pollers active, want the cap of 2; the excess was queued rather than refused", got)
	}
}

// TestHealthPublishesOnTransitionAndHeartbeat, and the health key expires when
// the publisher stops. Without both, "healthy and quiet" and "the health
// publisher is dead" are the same observation.
func TestHealthPublishesOnTransitionAndHeartbeat(t *testing.T) {
	const heartbeat = 200 * time.Millisecond
	harness := newLive(t, options{heartbeat: heartbeat, timeToLive: time.Minute, channels: markOnly})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); harness.tracker.Run(ctx) }()

	specification := harness.specifications[0]
	instrumentKey := publish.Key(coretest.Venue, specification.Instrument.MarketType, specification.Instrument.Canonical(), manoochv1.Channel_CHANNEL_HEALTH)
	venueKey := publish.VenueKey(coretest.Venue, publish.SubjectHealth)

	eventually(t, "the health keys to appear", func() bool {
		return harness.exists(instrumentKey) && harness.exists(venueKey)
	})

	// The TTL is three heartbeats, so a stopped publisher is visible rather
	// than looking like the last state forever.
	pttl, err := harness.redisClient.PTTL(context.Background(), instrumentKey).Result()
	if err != nil || pttl <= 0 || pttl > 3*heartbeat {
		t.Fatalf("health key PTTL = %v (%v), want (0, %v]", pttl, err, 3*heartbeat)
	}

	// The heartbeat republishes with nothing changed.
	first := harness.envelope(t, instrumentKey, manoochv1.Channel_CHANNEL_HEALTH).PublishSeq
	eventually(t, "a heartbeat with nothing changed", func() bool {
		envelope := harness.envelope(t, instrumentKey, manoochv1.Channel_CHANNEL_HEALTH)
		return envelope != nil && envelope.PublishSeq > first
	})

	// A transition does not wait for the next tick.
	harness.tracker.Leaked(2)
	eventually(t, "the transition to reach the venue key", func() bool {
		envelope := harness.envelope(t, venueKey, manoochv1.Channel_CHANNEL_HEALTH)
		return envelope != nil && envelope.Status == manoochv1.Status_STATUS_DEGRADED && envelope.StatusReason == "leaked goroutines: 2"
	})

	// And the channel is detectably dead once the publisher stops.
	cancel()
	<-done
	eventually(t, "the health key to expire after the publisher stopped", func() bool {
		return !harness.exists(instrumentKey) && !harness.exists(venueKey)
	})
}
