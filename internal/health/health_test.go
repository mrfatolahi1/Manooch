package health_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/health"
	"github.com/you/manooch/internal/observability"
	"google.golang.org/protobuf/proto"
)

const socketID = "test-0"

// clock is a hand-wound time source: the fallback escalation is a duration
// comparison, and sleeping through five real minutes to assert it is not a
// test.
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

// recorder is a Publisher that keeps what it was handed.
type recorder struct {
	mutex    sync.Mutex
	messages []recorded
}

type recorded struct {
	key        string
	health     *manoochv1.Health
	timeToLive time.Duration
}

func (recorder *recorder) Publish(_ context.Context, key string, message proto.Message, timeToLive time.Duration) error {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	health, _ := proto.Clone(message).(*manoochv1.Health)
	recorder.messages = append(recorder.messages, recorded{key: key, health: health, timeToLive: timeToLive})
	return nil
}

func (recorder *recorder) Close() error { return nil }

func (recorder *recorder) all() []recorded {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return append([]recorded(nil), recorder.messages...)
}

func (recorder *recorder) reset() {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	recorder.messages = nil
}

func specifications(t *testing.T) []core.StreamSpec {
	t.Helper()
	reference, err := core.ParseCanonical("BTC_USDT", manoochv1.MarketType_MARKET_TYPE_PERP_LINEAR)
	if err != nil {
		t.Fatal(err)
	}
	return []core.StreamSpec{
		{Instrument: reference, Channel: manoochv1.Channel_CHANNEL_MARK_PRICE},
		{Instrument: reference, Channel: manoochv1.Channel_CHANNEL_INDEX_PRICE},
		{Instrument: reference, Channel: manoochv1.Channel_CHANNEL_FUNDING},
	}
}

func newTracker(t *testing.T, clock *clock, recorder *recorder) *health.Tracker {
	t.Helper()
	tracker, err := health.New(health.Options{
		Venue:               "TESTVENUE",
		Publisher:           recorder,
		Metrics:             observability.NewMetrics(),
		Log:                 slog.New(slog.NewJSONHandler(io.Discard, nil)),
		HeartbeatInterval:   time.Second,
		ClockSkewDegradedMS: 2000,
		ClockSkewStaleMS:    10000,
		FallbackMaxDuration: 5 * time.Minute,
		Now:                 clock.Now,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, registered := range specifications(t) {
		tracker.Register(registered, "BTCUSDT", socketID)
	}
	tracker.SocketState(socketID, health.SocketConnected, "")
	return tracker
}

func wantStatus(t *testing.T, tracker *health.Tracker, specification core.StreamSpec, status manoochv1.Status, reason string) {
	t.Helper()
	got, gotReason := tracker.Status(specification)
	if got != status {
		t.Errorf("status = %s (%q), want %s", core.StatusName(got), gotReason, core.StatusName(status))
	}
	if reason != "" && gotReason != reason {
		t.Errorf("reason = %q, want %q", gotReason, reason)
	}
}

// TestHealthyWhenConnectedAndReceiving is the baseline every other case moves
// away from.
func TestHealthyWhenConnectedAndReceiving(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	specification := specifications(t)[0]

	tracker.Received(specification)
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_HEALTHY, "")
}

// TestFallbackIsDegradedThenStale: REST is usable data a consumer should know
// about; REST for longer than max_duration is a failure, not a steady state.
func TestFallbackIsDegradedThenStale(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	specification := specifications(t)[0]
	tracker.Received(specification)

	tracker.FallbackEngaged(specification)
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_DEGRADED, "rest fallback")

	clock.advance(5 * time.Minute)
	got, reason := tracker.Status(specification)
	if got != manoochv1.Status_STATUS_STALE {
		t.Errorf("status after max_duration on fallback = %s, want STALE", core.StatusName(got))
	}
	if reason == "" {
		t.Error("STALE with no reason")
	}

	// And a websocket message ends it without any restart.
	tracker.FallbackDisengaged(specification)
	tracker.Received(specification)
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_HEALTHY, "")
}

// TestFailedFallbackIsStale: the poll erroring, the venue answering nothing, or
// the concurrency cap all mean nobody is serving this key. It must never read
// as merely degraded.
func TestFailedFallbackIsStale(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	specification := specifications(t)[0]
	tracker.Received(specification)
	tracker.FallbackEngaged(specification)

	tracker.FallbackFailed(specification, "fallback at capacity")
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_STALE, "fallback at capacity")

	// A poll that then works clears it back to the ordinary fallback state.
	tracker.Polled(specification)
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_DEGRADED, "rest fallback")
}

// TestExpiredKeyIsStale: past the TTL with nothing serving it, a consumer is
// holding a price older than the venue's own cadence allows.
func TestExpiredKeyIsStale(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	specification := specifications(t)[0]
	tracker.Received(specification)

	tracker.KeyExpired(specification)
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_STALE, "key expired")

	tracker.Received(specification)
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_HEALTHY, "")
}

// TestCircuitOpenIsStale, and it outranks everything else: no connection
// attempt is being made at all, so nothing will arrive for the open duration
// however good the rest of the state looks.
func TestCircuitOpenIsStale(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	specification := specifications(t)[0]
	tracker.Received(specification)

	tracker.SocketState(socketID, health.SocketCircuitOpen, "10 consecutive failures")
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_STALE, "circuit open")

	if status, _ := tracker.VenueStatus(); status != manoochv1.Status_STATUS_STALE {
		t.Errorf("venue status = %s, want STALE", core.StatusName(status))
	}
}

// TestReconnectingIsDegraded: the key may still be inside its TTL, so the data
// is usable — but a consumer must be told the socket is not up.
func TestReconnectingIsDegraded(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	specification := specifications(t)[0]
	tracker.Received(specification)

	tracker.SocketState(socketID, health.SocketDialing, "read: connection reset")
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_DEGRADED, "read: connection reset")
}

// TestClockSkewCrossesBothThresholds, in both directions: the sign says which
// clock is ahead and must not decide whether the threshold is crossed.
func TestClockSkewCrossesBothThresholds(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	specification := specifications(t)[0]
	tracker.Received(specification)

	for _, sign := range []int64{1, -1} {
		tracker.ClockSkew(sign * 500)
		wantStatus(t, tracker, specification, manoochv1.Status_STATUS_HEALTHY, "")

		tracker.ClockSkew(sign * 3000)
		if status, _ := tracker.Status(specification); status != manoochv1.Status_STATUS_DEGRADED {
			t.Errorf("skew %dms: status = %s, want DEGRADED", sign*3000, core.StatusName(status))
		}

		tracker.ClockSkew(sign * 20000)
		if status, _ := tracker.Status(specification); status != manoochv1.Status_STATUS_STALE {
			t.Errorf("skew %dms: status = %s, want STALE", sign*20000, core.StatusName(status))
		}
	}
	tracker.ClockSkew(0)
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_HEALTHY, "")
}

// TestRejectedFrameIsDegraded, and clears on the next frame that parses. A
// venue sending shapes we cannot read is worth saying while the keys are still
// inside their TTL.
func TestRejectedFrameIsDegraded(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	specification := specifications(t)[0]
	tracker.Received(specification)

	tracker.FrameRejected(socketID)
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_DEGRADED, "frame rejected")

	tracker.Received(specification)
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_HEALTHY, "")
}

// TestLeakedGoroutinesDegradeTheVenue: there is no self-kill, so a leak
// accumulates silently unless something holds it visible.
func TestLeakedGoroutinesDegradeTheVenue(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	specification := specifications(t)[0]
	tracker.Received(specification)

	tracker.Leaked(2)

	status, reason := tracker.VenueStatus()
	if status != manoochv1.Status_STATUS_DEGRADED {
		t.Errorf("venue status = %s, want DEGRADED", core.StatusName(status))
	}
	if reason != "leaked goroutines: 2" {
		t.Errorf("venue reason = %q", reason)
	}
	// A venue-level leak is not a reason to call any individual price stale.
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_HEALTHY, "")
}

// TestUnregisteredStreamIsStale: the alternative publishes data as healthy on
// the strength of knowing nothing about it.
func TestUnregisteredStreamIsStale(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)

	reference, err := core.ParseCanonical("SOL_USDT", manoochv1.MarketType_MARKET_TYPE_PERP_LINEAR)
	if err != nil {
		t.Fatal(err)
	}
	status, reason := tracker.Status(core.StreamSpec{Instrument: reference, Channel: manoochv1.Channel_CHANNEL_MARK_PRICE})
	if status != manoochv1.Status_STATUS_STALE {
		t.Errorf("status = %s, want STALE", core.StatusName(status))
	}
	if reason == "" {
		t.Error("STALE with no reason")
	}
}

// TestStaleOutranksDegraded: a stream that qualifies for both must report the
// one that says do not trade.
func TestStaleOutranksDegraded(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	specification := specifications(t)[0]
	tracker.Received(specification)

	tracker.SocketState(socketID, health.SocketDialing, "read: connection reset") // degraded
	tracker.KeyExpired(specification)                                             // stale
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_STALE, "key expired")
}

// TestMetadataGatesEverything: without instrument metadata a price is a number
// nobody can size an order against, so every stream is STALE and stays STALE
// until the first fetch lands — whatever else is going right.
func TestMetadataGatesEverything(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker, err := health.New(health.Options{
		Venue:               "TESTVENUE",
		Publisher:           recorder,
		Metrics:             observability.NewMetrics(),
		Log:                 slog.New(slog.NewJSONHandler(io.Discard, nil)),
		HeartbeatInterval:   time.Second,
		ClockSkewDegradedMS: 2000,
		ClockSkewStaleMS:    10000,
		FallbackMaxDuration: 5 * time.Minute,
		MetadataRequired:    true,
		Now:                 clock.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	specification := specifications(t)[0]
	for _, registered := range specifications(t) {
		tracker.Register(registered, "BTCUSDT", socketID)
	}
	tracker.SocketState(socketID, health.SocketConnected, "")

	// Connected, receiving, and still STALE: the reason names what is missing.
	tracker.Received(specification)
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_STALE, "metadata unavailable")

	if status, reason := tracker.VenueStatus(); status != manoochv1.Status_STATUS_STALE || reason != "metadata unavailable" {
		t.Errorf("venue status = %s (%q), want STALE", core.StatusName(status), reason)
	}

	tracker.MetadataState(true, "")
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_HEALTHY, "")

	// It can go back: a refresher that starts failing again says so.
	tracker.MetadataState(false, "metadata unavailable")
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_STALE, "metadata unavailable")
}

// TestMetadataNotRequiredIsHealthyFromTheStart: a venue file that does not make
// metadata a startup dependency must not be held at STALE by one.
func TestMetadataNotRequiredIsHealthyFromTheStart(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	specification := specifications(t)[0]

	tracker.Received(specification)
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_HEALTHY, "")
}

// TestMetadataStateIsIgnoredWhenNotRequired: metadata is a gate or it is not.
// A venue file that opted out must not have its streams taken STALE by a
// refresh that happens to be failing in the background.
func TestMetadataStateIsIgnoredWhenNotRequired(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder) // MetadataRequired is false here
	specification := specifications(t)[0]
	tracker.Received(specification)

	tracker.MetadataState(false, "metadata unavailable")
	wantStatus(t, tracker, specification, manoochv1.Status_STATUS_HEALTHY, "")
	if status, _ := tracker.VenueStatus(); status != manoochv1.Status_STATUS_HEALTHY {
		t.Errorf("venue status = %s, want HEALTHY", core.StatusName(status))
	}
}
