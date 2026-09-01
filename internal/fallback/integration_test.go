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
	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/core/coretest"
	"github.com/you/manooch/internal/fallback"
	"github.com/you/manooch/internal/health"
	"github.com/you/manooch/internal/obs"
	"github.com/you/manooch/internal/publish"
)

const testDB = 0

// markOnly narrows a test to one channel per instrument.
var markOnly = []pb.Channel{pb.Channel_CHANNEL_MARK_PRICE}

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
	_ = res.Expire(600)

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

// ---------- harness ----------

type live struct {
	pub     *publish.RedisPublisher
	rdb     *redis.Client
	adapter *coretest.Adapter
	tracker *health.Tracker
	watcher *fallback.Watcher
	specs   []core.StreamSpec

	expiries chan core.StreamSpec
}

type liveOptions struct {
	symbols []string
	// channels narrows the streams under test. A key that has never been
	// written is indistinguishable from one that expired — which is correct,
	// and makes a single-channel assertion noisy unless the others are simply
	// not configured.
	channels      []pb.Channel
	ttl           time.Duration
	heartbeat     time.Duration
	maxConcurrent int
	maxDuration   time.Duration
	sweepInterval time.Duration
	pollInterval  time.Duration
	// subscribe false leaves the watcher's Run unstarted, so only what the
	// caller drives by hand happens.
	run bool
}

func newLive(t *testing.T, o liveOptions) *live {
	t.Helper()

	if len(o.symbols) == 0 {
		o.symbols = []string{"BTC_USDT"}
	}
	if o.ttl == 0 {
		o.ttl = 300 * time.Millisecond
	}
	if o.heartbeat == 0 {
		o.heartbeat = 200 * time.Millisecond
	}
	if o.maxConcurrent == 0 {
		o.maxConcurrent = 4
	}
	if o.maxDuration == 0 {
		o.maxDuration = 5 * time.Minute
	}
	if o.sweepInterval == 0 {
		o.sweepInterval = 100 * time.Millisecond
	}
	if o.pollInterval == 0 {
		o.pollInterval = 50 * time.Millisecond
	}

	specs, err := coretest.Specs(o.symbols...)
	if err != nil {
		t.Fatal(err)
	}
	if len(o.channels) > 0 {
		var narrowed []core.StreamSpec
		for _, spec := range specs {
			for _, ch := range o.channels {
				if spec.Channel == ch {
					narrowed = append(narrowed, spec)
				}
			}
		}
		specs = narrowed
	}

	pub, err := publish.NewRedis(context.Background(), publish.Options{
		Addr:          redisAddr,
		DB:            testDB,
		DialTimeout:   2 * time.Second,
		ReadTimeout:   2 * time.Second,
		PoolSize:      8,
		Venue:         coretest.Venue,
		InstanceID:    fmt.Sprintf("instance-%d", time.Now().UnixNano()),
		SchemaVersion: 2,
		Metrics:       obs.NewMetrics(),
		Logger:        quiet(),
	})
	if err != nil {
		t.Fatalf("NewRedis: %v", err)
	}
	t.Cleanup(func() { pub.Close() })

	l := &live{
		pub:      pub,
		rdb:      pub.Redis(),
		adapter:  &coretest.Adapter{TTL: o.ttl, Channels: o.channels},
		specs:    specs,
		expiries: make(chan core.StreamSpec, 64),
	}

	// Every test starts from a clean keyspace, or one test's leftovers are
	// another's "already expired".
	l.clear(t)
	t.Cleanup(func() { l.clear(t) })

	l.tracker, err = health.New(health.Options{
		Venue:               coretest.Venue,
		Publisher:           pub,
		Metrics:             obs.NewMetrics(),
		Log:                 quiet(),
		HeartbeatInterval:   o.heartbeat,
		ClockSkewDegradedMS: 2000,
		ClockSkewStaleMS:    10000,
		FallbackMaxDuration: o.maxDuration,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range specs {
		sym, _ := l.adapter.VenueSymbol(spec.Instrument)
		l.tracker.Register(spec, sym, "test-0")
	}
	l.tracker.SocketState("test-0", health.SocketConnected, "")

	l.watcher, err = fallback.New(fallback.Options{
		Venue:              coretest.Venue,
		Adapter:            l.adapter,
		Publisher:          pub,
		Redis:              l.rdb,
		DB:                 testDB,
		Health:             l.tracker,
		Metrics:            obs.NewMetrics(),
		Log:                quiet(),
		Specs:              specs,
		MaxConcurrentPolls: o.maxConcurrent,
		PollInterval:       o.pollInterval,
		SweepInterval:      o.sweepInterval,
		MaxDuration:        o.maxDuration,
		OnExpired: func(spec core.StreamSpec) {
			select {
			case l.expiries <- spec:
			default:
			}
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if o.run {
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan struct{})
		go func() { defer close(done); l.watcher.Run(ctx) }()
		t.Cleanup(func() {
			cancel()
			select {
			case <-done:
			case <-time.After(settle):
				t.Error("watcher Run did not return")
			}
		})
	}
	return l
}

// clear removes every key this venue owns.
func (l *live) clear(t *testing.T) {
	t.Helper()
	ctx := context.Background()
	iter := l.rdb.Scan(ctx, 0, publish.MatchPattern(coretest.Venue), 500).Iterator()
	for iter.Next(ctx) {
		l.rdb.Del(ctx, iter.Val())
	}
}

// socketMessage publishes a stream the way the supervisor would, which is what
// makes its key exist and start counting down.
func (l *live) socketMessage(t *testing.T, spec core.StreamSpec) {
	t.Helper()
	l.watcher.Note(spec)
	l.tracker.Received(spec)

	m := l.adapter.Message(spec, time.Now().UnixNano(), pb.Source_SOURCE_WEBSOCKET)
	env := m.Proto.(interface{ GetEnv() *pb.Envelope }).GetEnv()
	env.Status, env.StatusReason = l.tracker.Status(spec)

	if err := l.pub.Publish(context.Background(), m.Key, m.Proto, m.TTL); err != nil {
		t.Fatalf("publish %s: %v", m.Key, err)
	}
}

// envelope reads a key back the way a consumer would.
func (l *live) envelope(t *testing.T, key string, ch pb.Channel) *pb.Envelope {
	t.Helper()
	b, err := l.rdb.Get(context.Background(), key).Bytes()
	if err != nil {
		return nil
	}
	_, env, err := publish.Decode(ch, b)
	if err != nil {
		t.Fatalf("decode %s: %v", key, err)
	}
	return env
}

func (l *live) exists(key string) bool {
	n, err := l.rdb.Exists(context.Background(), key).Result()
	return err == nil && n > 0
}

// ---------- tests ----------

// TestKeyExpiryIsTheTrigger: the TTL is the freshness signal, and the key going
// away is the event everything downstream reacts to.
func TestKeyExpiryIsTheTrigger(t *testing.T) {
	l := newLive(t, liveOptions{run: true, ttl: 300 * time.Millisecond, channels: markOnly})
	spec := l.specs[0]
	k := key(spec)

	l.socketMessage(t, spec)
	if !l.exists(k) {
		t.Fatalf("%s does not exist after publishing it", k)
	}
	if pttl, err := l.rdb.PTTL(context.Background(), k).Result(); err != nil || pttl <= 0 || pttl > 300*time.Millisecond {
		t.Fatalf("PTTL = %v (%v), want (0, 300ms]", pttl, err)
	}

	select {
	case got := <-l.expiries:
		if got != spec {
			t.Errorf("expiry reported for %s, want %s", got, spec)
		}
	case <-time.After(settle):
		t.Fatalf("no expiry reported for %s", k)
	}
}

// TestSweepFindsWhatTheNotificationMissed: expiry events are Pub/Sub, so they
// are fire-and-forget and can simply not arrive. Without the backstop a missed
// notification is a stream that is never served again.
func TestSweepFindsWhatTheNotificationMissed(t *testing.T) {
	ctx := context.Background()

	// Notifications off entirely, which is the strongest form of "the event
	// did not arrive": nothing is published to the keyspace channel at all.
	l := newLive(t, liveOptions{ttl: 200 * time.Millisecond, sweepInterval: 100 * time.Millisecond, channels: markOnly})
	if err := l.rdb.ConfigSet(ctx, "notify-keyspace-events", "").Err(); err != nil {
		t.Fatalf("disabling keyspace events: %v", err)
	}
	t.Cleanup(func() { l.rdb.ConfigSet(context.Background(), "notify-keyspace-events", "Ex") })

	runCtx, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	go func() { defer close(done); l.watcher.Run(runCtx) }()
	t.Cleanup(func() { cancel(); <-done })

	spec := l.specs[0]
	l.socketMessage(t, spec)

	select {
	case got := <-l.expiries:
		if got != spec {
			t.Errorf("expiry reported for %s, want %s", got, spec)
		}
	case <-time.After(settle):
		t.Fatal("the sweep did not find an expired key with notifications disabled")
	}
}

// TestFallbackEngagesAndDisengages is the acceptance criterion end to end: the
// key expires, REST takes over and says so, and a websocket message hands it
// back with no restart anywhere.
func TestFallbackEngagesAndDisengages(t *testing.T) {
	l := newLive(t, liveOptions{run: true, ttl: 200 * time.Millisecond, channels: markOnly})
	spec := l.specs[0]
	k := key(spec)

	l.socketMessage(t, spec)

	// The key comes back, written by REST and labelled as such.
	eventually(t, "fallback to republish the key", func() bool {
		env := l.envelope(t, k, spec.Channel)
		return env != nil && env.Source == pb.Source_SOURCE_REST
	})
	env := l.envelope(t, k, spec.Channel)
	if env.Status != pb.Status_STATUS_DEGRADED {
		t.Errorf("status = %s, want DEGRADED", core.StatusName(env.Status))
	}
	if env.StatusReason == "" {
		t.Error("DEGRADED published with no reason")
	}
	if l.watcher.Active() != 1 {
		t.Errorf("%d pollers active, want 1", l.watcher.Active())
	}

	// The socket comes back. Nothing restarts; the source simply returns.
	l.socketMessage(t, spec)
	if l.watcher.Active() != 0 {
		t.Errorf("%d pollers still active after a websocket message", l.watcher.Active())
	}
	env = l.envelope(t, k, spec.Channel)
	if env.Source != pb.Source_SOURCE_WEBSOCKET {
		t.Errorf("source = %s, want WEBSOCKET", core.SourceName(env.Source))
	}
	if env.Status != pb.Status_STATUS_HEALTHY {
		t.Errorf("status = %s (%q), want HEALTHY", core.StatusName(env.Status), env.StatusReason)
	}
}

// TestFallbackPastMaxDurationGoesStale: long-running fallback is a failure, not
// a steady state, and the value on the wire has to say so.
func TestFallbackPastMaxDurationGoesStale(t *testing.T) {
	l := newLive(t, liveOptions{run: true, ttl: 200 * time.Millisecond, maxDuration: 500 * time.Millisecond, channels: markOnly})
	spec := l.specs[0]
	k := key(spec)

	l.socketMessage(t, spec)

	eventually(t, "fallback to escalate to stale", func() bool {
		env := l.envelope(t, k, spec.Channel)
		return env != nil && env.Status == pb.Status_STATUS_STALE
	})

	// Still REST, still being published: giving up entirely would leave a
	// consumer with no value rather than one labelled not to trade on.
	env := l.envelope(t, k, spec.Channel)
	if env.Source != pb.Source_SOURCE_REST {
		t.Errorf("source = %s, want REST", core.SourceName(env.Source))
	}
}

// TestConcurrencyCapLeavesTheExcessStale: past the cap a stream is turned away
// rather than queued, because a queued poll arrives after it stopped being
// worth having and is published as though it were current.
func TestConcurrencyCapLeavesTheExcessStale(t *testing.T) {
	l := newLive(t, liveOptions{run: true, ttl: 200 * time.Millisecond, maxConcurrent: 2})

	for _, spec := range l.specs {
		l.socketMessage(t, spec)
	}

	eventually(t, "the cap to be reached", func() bool { return l.watcher.Active() == 2 })

	eventually(t, "the excess stream to go stale", func() bool {
		stale := 0
		for _, spec := range l.specs {
			if st, reason := l.tracker.Status(spec); st == pb.Status_STATUS_STALE && reason == "fallback at capacity" {
				stale++
			}
		}
		return stale == 1
	})

	if got := l.watcher.Active(); got != 2 {
		t.Errorf("%d pollers active, want the cap of 2; the excess was queued rather than refused", got)
	}
}

// TestHealthPublishesOnTransitionAndHeartbeat, and the health key expires when
// the publisher stops. Without both, "healthy and quiet" and "the health
// publisher is dead" are the same observation.
func TestHealthPublishesOnTransitionAndHeartbeat(t *testing.T) {
	const heartbeat = 200 * time.Millisecond
	l := newLive(t, liveOptions{heartbeat: heartbeat, ttl: time.Minute, channels: markOnly})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); l.tracker.Run(ctx) }()

	spec := l.specs[0]
	instrumentKey := publish.Key(coretest.Venue, spec.Instrument.MarketType, spec.Instrument.Canonical(), pb.Channel_CHANNEL_HEALTH)
	venueKey := publish.VenueKey(coretest.Venue, publish.SubjectHealth)

	eventually(t, "the health keys to appear", func() bool {
		return l.exists(instrumentKey) && l.exists(venueKey)
	})

	// The TTL is three heartbeats, so a stopped publisher is visible rather
	// than looking like the last state forever.
	pttl, err := l.rdb.PTTL(context.Background(), instrumentKey).Result()
	if err != nil || pttl <= 0 || pttl > 3*heartbeat {
		t.Fatalf("health key PTTL = %v (%v), want (0, %v]", pttl, err, 3*heartbeat)
	}

	// The heartbeat republishes with nothing changed.
	first := l.envelope(t, instrumentKey, pb.Channel_CHANNEL_HEALTH).PublishSeq
	eventually(t, "a heartbeat with nothing changed", func() bool {
		env := l.envelope(t, instrumentKey, pb.Channel_CHANNEL_HEALTH)
		return env != nil && env.PublishSeq > first
	})

	// A transition does not wait for the next tick.
	l.tracker.Leaked(2)
	eventually(t, "the transition to reach the venue key", func() bool {
		env := l.envelope(t, venueKey, pb.Channel_CHANNEL_HEALTH)
		return env != nil && env.Status == pb.Status_STATUS_DEGRADED && env.StatusReason == "leaked goroutines: 2"
	})

	// And the channel is detectably dead once the publisher stops.
	cancel()
	<-done
	eventually(t, "the health key to expire after the publisher stopped", func() bool {
		return !l.exists(instrumentKey) && !l.exists(venueKey)
	})
}
