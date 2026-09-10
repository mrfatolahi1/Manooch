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
	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/core/coretest"
	"github.com/you/manooch/internal/fallback"
	"github.com/you/manooch/internal/health"
	"github.com/you/manooch/internal/observability"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/internal/ratelimit"
	"google.golang.org/protobuf/proto"
)

const settle = 5 * time.Second

// clock is a hand-wound time source: the escalation past max_duration is a
// duration comparison, and sleeping through five real minutes is not a test.
type clock struct {
	mutex sync.Mutex
	t     time.Time
}

func newClock() *clock { return &clock{t: time.Unix(1_700_000_000, 0)} }

func (clock *clock) Now() time.Time {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	return clock.t
}

func (clock *clock) advance(duration time.Duration) {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	clock.t = clock.t.Add(duration)
}

// recorder keeps the last envelope written to each key.
type recorder struct {
	mutex  sync.Mutex
	counts map[string]int
	last   map[string]*manoochv1.Envelope
	err    error
}

func newRecorder() *recorder {
	return &recorder{counts: map[string]int{}, last: map[string]*manoochv1.Envelope{}}
}

func (recorder *recorder) Publish(_ context.Context, key string, message proto.Message, _ time.Duration) error {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	if recorder.err != nil {
		return recorder.err
	}
	recorder.counts[key]++
	if enveloped, ok := message.(interface{ GetEnv() *manoochv1.Envelope }); ok {
		recorder.last[key] = proto.Clone(enveloped.GetEnv()).(*manoochv1.Envelope)
	}
	return nil
}

func (recorder *recorder) Close() error { return nil }

func (recorder *recorder) count(key string) int {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return recorder.counts[key]
}

func (recorder *recorder) envelope(key string) *manoochv1.Envelope {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return recorder.last[key]
}

func (recorder *recorder) fail(err error) {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	recorder.err = err
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
	adapter        *coretest.Adapter
	recorder       *recorder
	tracker        *health.Tracker
	watcher        *fallback.Watcher
	specifications []core.StreamSpec
	clock          *clock
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
	specifications, err := coretest.Specifications(symbols...)
	if err != nil {
		t.Fatal(err)
	}

	harness := &harness{adapter: &coretest.Adapter{}, recorder: newRecorder(), specifications: specifications, clock: newClock()}

	harness.tracker, err = health.New(health.Options{
		Venue:               coretest.Venue,
		Publisher:           harness.recorder,
		Metrics:             observability.NewMetrics(),
		Log:                 quiet(),
		HeartbeatInterval:   time.Second,
		ClockSkewDegradedMS: 2000,
		ClockSkewStaleMS:    10000,
		FallbackMaxDuration: 5 * time.Minute,
		Now:                 harness.clock.Now,
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
		Publisher:          harness.recorder,
		Redis:              redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}),
		Health:             harness.tracker,
		Metrics:            observability.NewMetrics(),
		Log:                quiet(),
		Specifications:     specifications,
		MaxConcurrentPolls: maxConcurrent,
		PollInterval:       5 * time.Millisecond,
		SweepInterval:      time.Second,
		MaxDuration:        5 * time.Minute,
		Now:                harness.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	return harness
}

func key(specification core.StreamSpec) string {
	return publish.Key(coretest.Venue, specification.Instrument.MarketType, specification.Instrument.Canonical(), specification.Channel)
}

// ---------- tests ----------

// TestFallbackPublishesAsRestAndDegraded: same channel, same key, so consumers
// have one code path — and the difference is visible to anyone who looks.
func TestFallbackPublishesAsRestAndDegraded(t *testing.T) {
	harness := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	specification := harness.specifications[0]
	harness.watcher.Expired(ctx, specification)
	t.Cleanup(func() { harness.watcher.Note(specification) })

	k := key(specification)
	eventually(t, "the first poll", func() bool { return harness.recorder.count(k) > 0 })

	envelope := harness.recorder.envelope(k)
	if envelope.Source != manoochv1.Source_SOURCE_REST {
		t.Errorf("source = %s, want REST", core.SourceName(envelope.Source))
	}
	if envelope.Status != manoochv1.Status_STATUS_DEGRADED {
		t.Errorf("status = %s, want DEGRADED", core.StatusName(envelope.Status))
	}
	if envelope.StatusReason == "" {
		t.Error("DEGRADED published with no reason")
	}
}

// TestFallbackPublishesOnlyTheExpiredChannel: the endpoint answers all three,
// but republishing the other two would reset their TTL — the one signal saying
// they are fine — from a source nobody asked for.
func TestFallbackPublishesOnlyTheExpiredChannel(t *testing.T) {
	harness := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// A venue whose REST call answers every channel at once, as Binance's does.
	harness.adapter.FetchFunc = func(_ context.Context, specification core.StreamSpec) ([]core.Message, error) {
		var out []core.Message
		for _, channel := range []manoochv1.Channel{manoochv1.Channel_CHANNEL_MARK_PRICE, manoochv1.Channel_CHANNEL_INDEX_PRICE, manoochv1.Channel_CHANNEL_FUNDING} {
			out = append(out, harness.adapter.Message(core.StreamSpec{Instrument: specification.Instrument, Channel: channel},
				time.Now().UnixNano(), manoochv1.Source_SOURCE_REST))
		}
		return out, nil
	}

	mark := harness.specifications[0]
	harness.watcher.Expired(ctx, mark)
	t.Cleanup(func() { harness.watcher.Note(mark) })

	eventually(t, "the first poll", func() bool { return harness.recorder.count(key(mark)) > 0 })

	for _, other := range harness.specifications[1:] {
		if n := harness.recorder.count(key(other)); n != 0 {
			t.Errorf("%s was republished %d times; only the expired channel may be", key(other), n)
		}
	}
}

// TestConcurrencyCapGoesStaleRatherThanQueueing: a queued poll is a value that
// arrives after it stopped being worth having, published as though current.
func TestConcurrencyCapGoesStaleRatherThanQueueing(t *testing.T) {
	harness := newHarness(t, 2, "BTC_USDT")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	for _, specification := range harness.specifications {
		harness.watcher.Expired(ctx, specification)
	}
	t.Cleanup(func() {
		for _, specification := range harness.specifications {
			harness.watcher.Note(specification)
		}
	})

	if got := harness.watcher.Active(); got != 2 {
		t.Errorf("%d pollers active, want the cap of 2", got)
	}

	// The two that got a poller have to have polled before their statuses mean
	// anything: until the first value lands they are still STALE for having
	// expired, which is a different failure from being turned away.
	eventually(t, "the engaged streams to poll", func() bool {
		return harness.recorder.count(key(harness.specifications[0])) > 0 && harness.recorder.count(key(harness.specifications[1])) > 0
	})

	// Exactly one of the three was turned away, and it says so.
	stale := 0
	for _, specification := range harness.specifications {
		if status, reason := harness.tracker.Status(specification); status == manoochv1.Status_STATUS_STALE {
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
	harness := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	specification := harness.specifications[0]
	harness.watcher.Expired(ctx, specification)
	eventually(t, "the first poll", func() bool { return harness.recorder.count(key(specification)) > 0 })

	harness.watcher.Note(specification)
	harness.tracker.Received(specification)

	if got := harness.watcher.Active(); got != 0 {
		t.Errorf("%d pollers still active after a websocket message", got)
	}
	if status, reason := harness.tracker.Status(specification); status != manoochv1.Status_STATUS_HEALTHY {
		t.Errorf("status = %s (%q), want HEALTHY", core.StatusName(status), reason)
	}

	// And the polling actually stopped rather than merely being deregistered.
	settled := harness.recorder.count(key(specification))
	time.Sleep(50 * time.Millisecond)
	if got := harness.recorder.count(key(specification)); got != settled {
		t.Errorf("%d further polls after disengaging", got-settled)
	}
}

// TestFailedPollIsStale: a fallback that quietly stops is exactly the failure
// this service exists to prevent.
func TestFailedPollIsStale(t *testing.T) {
	harness := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness.adapter.FetchFunc = func(context.Context, core.StreamSpec) ([]core.Message, error) {
		return nil, errors.New("503 Service Unavailable")
	}

	specification := harness.specifications[0]
	harness.watcher.Expired(ctx, specification)
	t.Cleanup(func() { harness.watcher.Note(specification) })

	eventually(t, "the poll failure to reach the status", func() bool {
		status, reason := harness.tracker.Status(specification)
		return status == manoochv1.Status_STATUS_STALE && reason == "rest poll failed"
	})
}

// TestLimiterDenialIsStale: a poll the rate limiter refused did not happen, so
// the key it would have refreshed stays absent. Reporting that as anything
// other than STALE would be the silent skip this service exists to prevent —
// the consumer would see no data and nothing anywhere would say why.
func TestLimiterDenialIsStale(t *testing.T) {
	harness := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness.adapter.FetchFunc = func(context.Context, core.StreamSpec) ([]core.Message, error) {
		return nil, fmt.Errorf("fetch: %w", ratelimit.ErrBudgetExhausted)
	}

	specification := harness.specifications[0]
	harness.watcher.Expired(ctx, specification)
	t.Cleanup(func() { harness.watcher.Note(specification) })

	eventually(t, "the refused poll to reach the status", func() bool {
		status, reason := harness.tracker.Status(specification)
		return status == manoochv1.Status_STATUS_STALE && reason == "rest poll failed"
	})
	if got := harness.recorder.count(key(specification)); got != 0 {
		t.Errorf("%d values published from polls that never happened", got)
	}
}

// TestEmptyAnswerIsStale: the venue answered, but not with this value. Missing
// data is not a zero, and it must not read as a working fallback.
func TestEmptyAnswerIsStale(t *testing.T) {
	harness := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness.adapter.FetchFunc = func(context.Context, core.StreamSpec) ([]core.Message, error) {
		return nil, nil
	}

	specification := harness.specifications[0]
	harness.watcher.Expired(ctx, specification)
	t.Cleanup(func() { harness.watcher.Note(specification) })

	eventually(t, "the empty answer to reach the status", func() bool {
		status, reason := harness.tracker.Status(specification)
		return status == manoochv1.Status_STATUS_STALE && reason == "rest returned no value"
	})
	if n := harness.recorder.count(key(specification)); n != 0 {
		t.Errorf("%d messages published for an answer that carried no value", n)
	}
}

// TestFallbackPastMaxDurationPublishesStale: long-running fallback is a
// failure, not a steady state, and the value on the wire has to say so.
func TestFallbackPastMaxDurationPublishesStale(t *testing.T) {
	harness := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	specification := harness.specifications[0]
	harness.watcher.Expired(ctx, specification)
	t.Cleanup(func() { harness.watcher.Note(specification) })

	k := key(specification)
	eventually(t, "the first poll", func() bool { return harness.recorder.count(k) > 0 })
	harness.clock.advance(5 * time.Minute)

	eventually(t, "a poll published as stale", func() bool {
		envelope := harness.recorder.envelope(k)
		return envelope != nil && envelope.Status == manoochv1.Status_STATUS_STALE
	})

	// Still polling, though: giving up entirely would leave the consumer with
	// no value at all rather than one clearly labelled as not to be traded on.
	settled := harness.recorder.count(k)
	eventually(t, "polling to continue past max_duration", func() bool { return harness.recorder.count(k) > settled })

	envelope := harness.recorder.envelope(k)
	if envelope.Source != manoochv1.Source_SOURCE_REST {
		t.Errorf("source = %s, want REST", core.SourceName(envelope.Source))
	}
}

// TestFailedPublishIsStale: the write not landing is the same failure as the
// poll not answering, and must not be quieter.
func TestFailedPublishIsStale(t *testing.T) {
	harness := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	harness.recorder.fail(errors.New("OOM command not allowed"))

	specification := harness.specifications[0]
	harness.watcher.Expired(ctx, specification)
	t.Cleanup(func() { harness.watcher.Note(specification) })

	eventually(t, "the failed write to reach the status", func() bool {
		status, reason := harness.tracker.Status(specification)
		return status == manoochv1.Status_STATUS_STALE && reason == "rest poll failed"
	})
}

// TestExpiryIsReportedOncePerOutage: the sweep re-finds a key that is still
// missing every interval, and counting each as a fresh expiry would turn one
// dead stream into a restart every sweep.
func TestExpiryIsReportedOncePerOutage(t *testing.T) {
	harness := newHarness(t, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var escalations int
	var mutex sync.Mutex

	watcher, err := fallback.New(fallback.Options{
		Venue:              coretest.Venue,
		Adapter:            harness.adapter,
		Publisher:          harness.recorder,
		Redis:              redis.NewClient(&redis.Options{Addr: "127.0.0.1:1"}),
		Health:             harness.tracker,
		Metrics:            observability.NewMetrics(),
		Log:                quiet(),
		Specifications:     harness.specifications,
		MaxConcurrentPolls: 4,
		PollInterval:       time.Hour, // one poll, then quiet
		SweepInterval:      time.Second,
		MaxDuration:        5 * time.Minute,
		OnExpired:          func(core.StreamSpec) { mutex.Lock(); escalations++; mutex.Unlock() },
		Now:                harness.clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}

	specification := harness.specifications[0]
	for range 10 {
		watcher.Expired(ctx, specification)
	}
	t.Cleanup(func() { watcher.Note(specification) })

	mutex.Lock()
	got := escalations
	mutex.Unlock()
	if got != 1 {
		t.Errorf("%d escalations for one outage seen ten times, want 1", got)
	}

	// A recovery, then a second outage, is a second escalation.
	watcher.Note(specification)
	watcher.Expired(ctx, specification)
	t.Cleanup(func() { watcher.Note(specification) })

	mutex.Lock()
	got = escalations
	mutex.Unlock()
	if got != 2 {
		t.Errorf("%d escalations after a second outage, want 2", got)
	}
}
