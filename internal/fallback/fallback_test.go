package fallback_test

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/core/coretest"
	"github.com/you/manooch/internal/fallback"
	"github.com/you/manooch/internal/health"
	"github.com/you/manooch/internal/obs"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/internal/ratelimit"
	"google.golang.org/protobuf/proto"
)

const settle = 5 * time.Second

// clock is a hand-wound time source: the escalation past max_duration is a
// duration comparison, and sleeping through five real minutes is not a test.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Unix(1_700_000_000, 0)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// recorder keeps the last envelope written to each key.
type recorder struct {
	mu     sync.Mutex
	counts map[string]int
	last   map[string]*pb.Envelope
	err    error
}

func newRecorder() *recorder {
	return &recorder{counts: map[string]int{}, last: map[string]*pb.Envelope{}}
}

func (r *recorder) Publish(_ context.Context, key string, msg proto.Message, _ time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.counts[key]++
	if e, ok := msg.(interface{ GetEnv() *pb.Envelope }); ok {
		r.last[key] = proto.Clone(e.GetEnv()).(*pb.Envelope)
	}
	return nil
}

func (r *recorder) Close() error { return nil }

func (r *recorder) count(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[key]
}

func (r *recorder) envelope(key string) *pb.Envelope {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last[key]
}

func (r *recorder) fail(err error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.err = err
}

func quiet() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(settle)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// ---------- harness ----------

type harness struct {
	adapter *coretest.Adapter
	pub     *recorder
	tracker *health.Tracker
	watcher *fallback.Watcher
	specs   []core.StreamSpec
	clock   *clock
}

// newHarness wires a watcher with no Redis behind it. The client is never
// dialled: these cover engagement, the cap and disengagement, which touch the
// adapter and the publisher only. The trigger paths that do need Redis — the
// expiry event and the sweep — are in the integration suite.
func newHarness(t *testing.T, maxConcurrent int, symbols ...string) *harness {
	t.Helper()
	if len(symbols) == 0 {
		symbols = []string{"BTC_USDT"}
	}
	specs, err := coretest.Specs(symbols...)
	if err != nil {
		t.Fatal(err)
	}

	h := &harness{adapter: &coretest.Adapter{}, pub: newRecorder(), specs: specs, clock: newClock()}

	h.tracker, err = health.New(health.Options{
		Venue:               coretest.Venue,
		Publisher:           h.pub,
		Metrics:             obs.NewMetrics(),
		Log:                 quiet(),
		HeartbeatInterval:   time.Second,
		ClockSkewDegradedMS: 2000,
		ClockSkewStaleMS:    10000,
		FallbackMaxDuration: 5 * time.Minute,
		Now:                 h.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, spec := range specs {
		sym, _ := h.adapter.VenueSymbol(spec.Instrument)
		h.tracker.Register(spec, sym, "test-0")
	}
	h.tracker.SocketState("test-0", health.SocketConnected, "")

	h.watcher, err = fallback.New(fallback.Options{
		Venue:              coretest.Venue,
		Adapter:            h.adapter,
		Publisher:          h.pub,
		Redis:              redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}),
		Health:             h.tracker,
		Metrics:            obs.NewMetrics(),
		Log:                quiet(),
		Specs:              specs,
		MaxConcurrentPolls: maxConcurrent,
		PollInterval:       5 * time.Millisecond,
		SweepInterval:      time.Second,
		MaxDuration:        5 * time.Minute,
		Now:                h.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func key(spec core.StreamSpec) string {
	return publish.Key(coretest.Venue, spec.Instrument.MarketType, spec.Instrument.Canonical(), spec.Channel)
}

// ---------- tests ----------

// TestFallbackPublishesAsRestAndDegraded: same channel, same key, so consumers
// have one code path — and the difference is visible to anyone who looks.
func TestFallbackPublishesAsRestAndDegraded(t *testing.T) {
	h := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	spec := h.specs[0]
	h.watcher.Expired(ctx, spec)
	t.Cleanup(func() { h.watcher.Note(spec) })

	k := key(spec)
	eventually(t, "the first poll", func() bool { return h.pub.count(k) > 0 })

	env := h.pub.envelope(k)
	if env.Source != pb.Source_SOURCE_REST {
		t.Errorf("source = %s, want REST", core.SourceName(env.Source))
	}
	if env.Status != pb.Status_STATUS_DEGRADED {
		t.Errorf("status = %s, want DEGRADED", core.StatusName(env.Status))
	}
	if env.StatusReason == "" {
		t.Error("DEGRADED published with no reason")
	}
}

// TestFallbackPublishesOnlyTheExpiredChannel: the endpoint answers all three,
// but republishing the other two would reset their TTL — the one signal saying
// they are fine — from a source nobody asked for.
func TestFallbackPublishesOnlyTheExpiredChannel(t *testing.T) {
	h := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A venue whose REST call answers every channel at once, as Binance's does.
	h.adapter.FetchFunc = func(_ context.Context, spec core.StreamSpec) ([]core.Message, error) {
		var out []core.Message
		for _, ch := range []pb.Channel{pb.Channel_CHANNEL_MARK_PRICE, pb.Channel_CHANNEL_INDEX_PRICE, pb.Channel_CHANNEL_FUNDING} {
			out = append(out, h.adapter.Message(core.StreamSpec{Instrument: spec.Instrument, Channel: ch},
				time.Now().UnixNano(), pb.Source_SOURCE_REST))
		}
		return out, nil
	}

	mark := h.specs[0]
	h.watcher.Expired(ctx, mark)
	t.Cleanup(func() { h.watcher.Note(mark) })

	eventually(t, "the first poll", func() bool { return h.pub.count(key(mark)) > 0 })

	for _, other := range h.specs[1:] {
		if n := h.pub.count(key(other)); n != 0 {
			t.Errorf("%s was republished %d times; only the expired channel may be", key(other), n)
		}
	}
}

// TestConcurrencyCapGoesStaleRatherThanQueueing: a queued poll is a value that
// arrives after it stopped being worth having, published as though current.
func TestConcurrencyCapGoesStaleRatherThanQueueing(t *testing.T) {
	h := newHarness(t, 2, "BTC_USDT")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, spec := range h.specs {
		h.watcher.Expired(ctx, spec)
	}
	t.Cleanup(func() {
		for _, spec := range h.specs {
			h.watcher.Note(spec)
		}
	})

	if got := h.watcher.Active(); got != 2 {
		t.Errorf("%d pollers active, want the cap of 2", got)
	}

	// The two that got a poller have to have polled before their statuses mean
	// anything: until the first value lands they are still STALE for having
	// expired, which is a different failure from being turned away.
	eventually(t, "the engaged streams to poll", func() bool {
		return h.pub.count(key(h.specs[0])) > 0 && h.pub.count(key(h.specs[1])) > 0
	})

	// Exactly one of the three was turned away, and it says so.
	stale := 0
	for _, spec := range h.specs {
		if st, reason := h.tracker.Status(spec); st == pb.Status_STATUS_STALE {
			stale++
			if reason != "fallback at capacity" {
				t.Errorf("reason = %q, want %q", reason, "fallback at capacity")
			}
		}
	}
	if stale != 1 {
		t.Errorf("%d streams STALE, want 1 past a cap of 2", stale)
	}
}

// TestWebsocketMessageDisengagesFallback, with no restart: the acceptance
// criterion is that restoring the venue is enough on its own.
func TestWebsocketMessageDisengagesFallback(t *testing.T) {
	h := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	spec := h.specs[0]
	h.watcher.Expired(ctx, spec)
	eventually(t, "the first poll", func() bool { return h.pub.count(key(spec)) > 0 })

	h.watcher.Note(spec)
	h.tracker.Received(spec)

	if got := h.watcher.Active(); got != 0 {
		t.Errorf("%d pollers still active after a websocket message", got)
	}
	if st, reason := h.tracker.Status(spec); st != pb.Status_STATUS_HEALTHY {
		t.Errorf("status = %s (%q), want HEALTHY", core.StatusName(st), reason)
	}

	// And the polling actually stopped rather than merely being deregistered.
	settled := h.pub.count(key(spec))
	time.Sleep(50 * time.Millisecond)
	if got := h.pub.count(key(spec)); got != settled {
		t.Errorf("%d further polls after disengaging", got-settled)
	}
}

// TestFailedPollIsStale: a fallback that quietly stops is exactly the failure
// this service exists to prevent.
func TestFailedPollIsStale(t *testing.T) {
	h := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.adapter.FetchFunc = func(context.Context, core.StreamSpec) ([]core.Message, error) {
		return nil, errors.New("503 Service Unavailable")
	}

	spec := h.specs[0]
	h.watcher.Expired(ctx, spec)
	t.Cleanup(func() { h.watcher.Note(spec) })

	eventually(t, "the poll failure to reach the status", func() bool {
		st, reason := h.tracker.Status(spec)
		return st == pb.Status_STATUS_STALE && reason == "rest poll failed"
	})
}

// TestLimiterDenialIsStale: a poll the rate limiter refused did not happen, so
// the key it would have refreshed stays absent. Reporting that as anything
// other than STALE would be the silent skip this service exists to prevent —
// the consumer would see no data and nothing anywhere would say why.
func TestLimiterDenialIsStale(t *testing.T) {
	h := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.adapter.FetchFunc = func(context.Context, core.StreamSpec) ([]core.Message, error) {
		return nil, fmt.Errorf("fetch: %w", ratelimit.ErrBudgetExhausted)
	}

	spec := h.specs[0]
	h.watcher.Expired(ctx, spec)
	t.Cleanup(func() { h.watcher.Note(spec) })

	eventually(t, "the refused poll to reach the status", func() bool {
		st, reason := h.tracker.Status(spec)
		return st == pb.Status_STATUS_STALE && reason == "rest poll failed"
	})
	if got := h.pub.count(key(spec)); got != 0 {
		t.Errorf("%d values published from polls that never happened", got)
	}
}

// TestEmptyAnswerIsStale: the venue answered, but not with this value. Missing
// data is not a zero, and it must not read as a working fallback.
func TestEmptyAnswerIsStale(t *testing.T) {
	h := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.adapter.FetchFunc = func(context.Context, core.StreamSpec) ([]core.Message, error) {
		return nil, nil
	}

	spec := h.specs[0]
	h.watcher.Expired(ctx, spec)
	t.Cleanup(func() { h.watcher.Note(spec) })

	eventually(t, "the empty answer to reach the status", func() bool {
		st, reason := h.tracker.Status(spec)
		return st == pb.Status_STATUS_STALE && reason == "rest returned no value"
	})
	if n := h.pub.count(key(spec)); n != 0 {
		t.Errorf("%d messages published for an answer that carried no value", n)
	}
}

// TestFallbackPastMaxDurationPublishesStale: long-running fallback is a
// failure, not a steady state, and the value on the wire has to say so.
func TestFallbackPastMaxDurationPublishesStale(t *testing.T) {
	h := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	spec := h.specs[0]
	h.watcher.Expired(ctx, spec)
	t.Cleanup(func() { h.watcher.Note(spec) })

	k := key(spec)
	eventually(t, "the first poll", func() bool { return h.pub.count(k) > 0 })
	h.clock.advance(5 * time.Minute)

	eventually(t, "a poll published as stale", func() bool {
		env := h.pub.envelope(k)
		return env != nil && env.Status == pb.Status_STATUS_STALE
	})

	// Still polling, though: giving up entirely would leave the consumer with
	// no value at all rather than one clearly labelled as not to be traded on.
	settled := h.pub.count(k)
	eventually(t, "polling to continue past max_duration", func() bool { return h.pub.count(k) > settled })

	env := h.pub.envelope(k)
	if env.Source != pb.Source_SOURCE_REST {
		t.Errorf("source = %s, want REST", core.SourceName(env.Source))
	}
}

// TestFailedPublishIsStale: the write not landing is the same failure as the
// poll not answering, and must not be quieter.
func TestFailedPublishIsStale(t *testing.T) {
	h := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	h.pub.fail(errors.New("OOM command not allowed"))

	spec := h.specs[0]
	h.watcher.Expired(ctx, spec)
	t.Cleanup(func() { h.watcher.Note(spec) })

	eventually(t, "the failed write to reach the status", func() bool {
		st, reason := h.tracker.Status(spec)
		return st == pb.Status_STATUS_STALE && reason == "rest poll failed"
	})
}

// TestExpiryIsReportedOncePerOutage: the sweep re-finds a key that is still
// missing every interval, and counting each as a fresh expiry would turn one
// dead stream into a restart every sweep.
func TestExpiryIsReportedOncePerOutage(t *testing.T) {
	h := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var escalations int
	var mu sync.Mutex

	w, err := fallback.New(fallback.Options{
		Venue:              coretest.Venue,
		Adapter:            h.adapter,
		Publisher:          h.pub,
		Redis:              redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}),
		Health:             h.tracker,
		Metrics:            obs.NewMetrics(),
		Log:                quiet(),
		Specs:              h.specs,
		MaxConcurrentPolls: 4,
		PollInterval:       time.Hour, // one poll, then quiet
		SweepInterval:      time.Second,
		MaxDuration:        5 * time.Minute,
		OnExpired:          func(core.StreamSpec) { mu.Lock(); escalations++; mu.Unlock() },
		Now:                h.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}

	spec := h.specs[0]
	for range 10 {
		w.Expired(ctx, spec)
	}
	t.Cleanup(func() { w.Note(spec) })

	mu.Lock()
	got := escalations
	mu.Unlock()
	if got != 1 {
		t.Errorf("%d escalations for one outage seen ten times, want 1", got)
	}

	// A recovery, then a second outage, is a second escalation.
	w.Note(spec)
	w.Expired(ctx, spec)
	t.Cleanup(func() { w.Note(spec) })

	mu.Lock()
	got = escalations
	mu.Unlock()
	if got != 2 {
		t.Errorf("%d escalations after a second outage, want 2", got)
	}
}
