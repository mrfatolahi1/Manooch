package health_test

import (
	"context"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/health"
	"github.com/you/manooch/internal/obs"
	"google.golang.org/protobuf/proto"
)

const socketID = "test-0"

// clock is a hand-wound time source: the fallback escalation is a duration
// comparison, and sleeping through five real minutes to assert it is not a test.
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

// recorder is a Publisher that keeps what it was handed.
type recorder struct {
	mu   sync.Mutex
	msgs []recorded
}

type recorded struct {
	key    string
	health *pb.Health
	ttl    time.Duration
}

func (r *recorder) Publish(_ context.Context, key string, msg proto.Message, ttl time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	h, _ := proto.Clone(msg).(*pb.Health)
	r.msgs = append(r.msgs, recorded{key: key, health: h, ttl: ttl})
	return nil
}

func (r *recorder) Close() error { return nil }

func (r *recorder) all() []recorded {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]recorded(nil), r.msgs...)
}

func (r *recorder) reset() {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.msgs = nil
}

func specs(t *testing.T) []core.StreamSpec {
	t.Helper()
	ref, err := core.ParseCanonical("BTC_USDT", pb.MarketType_MARKET_TYPE_PERP_LINEAR)
	if err != nil {
		t.Fatal(err)
	}
	return []core.StreamSpec{
		{Instrument: ref, Channel: pb.Channel_CHANNEL_MARK_PRICE},
		{Instrument: ref, Channel: pb.Channel_CHANNEL_INDEX_PRICE},
		{Instrument: ref, Channel: pb.Channel_CHANNEL_FUNDING},
	}
}

func newTracker(t *testing.T, c *clock, pub *recorder) *health.Tracker {
	t.Helper()
	tr, err := health.New(health.Options{
		Venue:               "TESTVENUE",
		Publisher:           pub,
		Metrics:             obs.NewMetrics(),
		Log:                 slog.New(slog.NewJSONHandler(io.Discard, nil)),
		HeartbeatInterval:   time.Second,
		ClockSkewDegradedMS: 2000,
		ClockSkewStaleMS:    10000,
		FallbackMaxDuration: 5 * time.Minute,
		Now:                 c.Now,
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	for _, s := range specs(t) {
		tr.Register(s, "BTCUSDT", socketID)
	}
	tr.SocketState(socketID, health.SocketConnected, "")
	return tr
}

func wantStatus(t *testing.T, tr *health.Tracker, spec core.StreamSpec, status pb.Status, reason string) {
	t.Helper()
	got, gotReason := tr.Status(spec)
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
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	spec := specs(t)[0]

	tr.Received(spec)
	wantStatus(t, tr, spec, pb.Status_STATUS_HEALTHY, "")
}

// TestFallbackIsDegradedThenStale: REST is usable data a consumer should know
// about; REST for longer than max_duration is a failure, not a steady state.
func TestFallbackIsDegradedThenStale(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	spec := specs(t)[0]
	tr.Received(spec)

	tr.FallbackEngaged(spec)
	wantStatus(t, tr, spec, pb.Status_STATUS_DEGRADED, "rest fallback")

	c.advance(5 * time.Minute)
	got, reason := tr.Status(spec)
	if got != pb.Status_STATUS_STALE {
		t.Errorf("status after max_duration on fallback = %s, want STALE", core.StatusName(got))
	}
	if reason == "" {
		t.Error("STALE with no reason")
	}

	// And a websocket message ends it without any restart.
	tr.FallbackDisengaged(spec)
	tr.Received(spec)
	wantStatus(t, tr, spec, pb.Status_STATUS_HEALTHY, "")
}

// TestFailedFallbackIsStale: the poll erroring, the venue answering nothing, or
// the concurrency cap all mean nobody is serving this key. It must never read
// as merely degraded.
func TestFailedFallbackIsStale(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	spec := specs(t)[0]
	tr.Received(spec)
	tr.FallbackEngaged(spec)

	tr.FallbackFailed(spec, "fallback at capacity")
	wantStatus(t, tr, spec, pb.Status_STATUS_STALE, "fallback at capacity")

	// A poll that then works clears it back to the ordinary fallback state.
	tr.Polled(spec)
	wantStatus(t, tr, spec, pb.Status_STATUS_DEGRADED, "rest fallback")
}

// TestExpiredKeyIsStale: past the TTL with nothing serving it, a consumer is
// holding a price older than the venue's own cadence allows.
func TestExpiredKeyIsStale(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	spec := specs(t)[0]
	tr.Received(spec)

	tr.KeyExpired(spec)
	wantStatus(t, tr, spec, pb.Status_STATUS_STALE, "key expired")

	tr.Received(spec)
	wantStatus(t, tr, spec, pb.Status_STATUS_HEALTHY, "")
}

// TestCircuitOpenIsStale, and it outranks everything else: no connection
// attempt is being made at all, so nothing will arrive for the open duration
// however good the rest of the state looks.
func TestCircuitOpenIsStale(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	spec := specs(t)[0]
	tr.Received(spec)

	tr.SocketState(socketID, health.SocketCircuitOpen, "10 consecutive failures")
	wantStatus(t, tr, spec, pb.Status_STATUS_STALE, "circuit open")

	if st, _ := tr.VenueStatus(); st != pb.Status_STATUS_STALE {
		t.Errorf("venue status = %s, want STALE", core.StatusName(st))
	}
}

// TestReconnectingIsDegraded: the key may still be inside its TTL, so the data
// is usable — but a consumer must be told the socket is not up.
func TestReconnectingIsDegraded(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	spec := specs(t)[0]
	tr.Received(spec)

	tr.SocketState(socketID, health.SocketDialing, "read: connection reset")
	wantStatus(t, tr, spec, pb.Status_STATUS_DEGRADED, "read: connection reset")
}

// TestClockSkewCrossesBothThresholds, in both directions: the sign says which
// clock is ahead and must not decide whether the threshold is crossed.
func TestClockSkewCrossesBothThresholds(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	spec := specs(t)[0]
	tr.Received(spec)

	for _, sign := range []int64{1, -1} {
		tr.ClockSkew(sign * 500)
		wantStatus(t, tr, spec, pb.Status_STATUS_HEALTHY, "")

		tr.ClockSkew(sign * 3000)
		if st, _ := tr.Status(spec); st != pb.Status_STATUS_DEGRADED {
			t.Errorf("skew %dms: status = %s, want DEGRADED", sign*3000, core.StatusName(st))
		}

		tr.ClockSkew(sign * 20000)
		if st, _ := tr.Status(spec); st != pb.Status_STATUS_STALE {
			t.Errorf("skew %dms: status = %s, want STALE", sign*20000, core.StatusName(st))
		}
	}
	tr.ClockSkew(0)
	wantStatus(t, tr, spec, pb.Status_STATUS_HEALTHY, "")
}

// TestRejectedFrameIsDegraded, and clears on the next frame that parses. A
// venue sending shapes we cannot read is worth saying while the keys are still
// inside their TTL.
func TestRejectedFrameIsDegraded(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	spec := specs(t)[0]
	tr.Received(spec)

	tr.FrameRejected(socketID)
	wantStatus(t, tr, spec, pb.Status_STATUS_DEGRADED, "frame rejected")

	tr.Received(spec)
	wantStatus(t, tr, spec, pb.Status_STATUS_HEALTHY, "")
}

// TestLeakedGoroutinesDegradeTheVenue: there is no self-kill, so a leak
// accumulates silently unless something holds it visible.
func TestLeakedGoroutinesDegradeTheVenue(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	spec := specs(t)[0]
	tr.Received(spec)

	tr.Leaked(2)

	st, reason := tr.VenueStatus()
	if st != pb.Status_STATUS_DEGRADED {
		t.Errorf("venue status = %s, want DEGRADED", core.StatusName(st))
	}
	if reason != "leaked goroutines: 2" {
		t.Errorf("venue reason = %q", reason)
	}
	// A venue-level leak is not a reason to call any individual price stale.
	wantStatus(t, tr, spec, pb.Status_STATUS_HEALTHY, "")
}

// TestUnregisteredStreamIsStale: the alternative publishes data as healthy on
// the strength of knowing nothing about it.
func TestUnregisteredStreamIsStale(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)

	ref, err := core.ParseCanonical("SOL_USDT", pb.MarketType_MARKET_TYPE_PERP_LINEAR)
	if err != nil {
		t.Fatal(err)
	}
	st, reason := tr.Status(core.StreamSpec{Instrument: ref, Channel: pb.Channel_CHANNEL_MARK_PRICE})
	if st != pb.Status_STATUS_STALE {
		t.Errorf("status = %s, want STALE", core.StatusName(st))
	}
	if reason == "" {
		t.Error("STALE with no reason")
	}
}

// TestStaleOutranksDegraded: a stream that qualifies for both must report the
// one that says do not trade.
func TestStaleOutranksDegraded(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	spec := specs(t)[0]
	tr.Received(spec)

	tr.SocketState(socketID, health.SocketDialing, "read: connection reset") // degraded
	tr.KeyExpired(spec)                                                      // stale
	wantStatus(t, tr, spec, pb.Status_STATUS_STALE, "key expired")
}

// TestMetadataGatesEverything: without instrument metadata a price is a number
// nobody can size an order against, so every stream is STALE and stays STALE
// until the first fetch lands — whatever else is going right.
func TestMetadataGatesEverything(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr, err := health.New(health.Options{
		Venue:               "TESTVENUE",
		Publisher:           pub,
		Metrics:             obs.NewMetrics(),
		Log:                 slog.New(slog.NewJSONHandler(io.Discard, nil)),
		HeartbeatInterval:   time.Second,
		ClockSkewDegradedMS: 2000,
		ClockSkewStaleMS:    10000,
		FallbackMaxDuration: 5 * time.Minute,
		MetadataRequired:    true,
		Now:                 c.Now,
	})
	if err != nil {
		t.Fatal(err)
	}
	spec := specs(t)[0]
	for _, s := range specs(t) {
		tr.Register(s, "BTCUSDT", socketID)
	}
	tr.SocketState(socketID, health.SocketConnected, "")

	// Connected, receiving, and still STALE: the reason names what is missing.
	tr.Received(spec)
	wantStatus(t, tr, spec, pb.Status_STATUS_STALE, "metadata unavailable")

	if status, reason := tr.VenueStatus(); status != pb.Status_STATUS_STALE || reason != "metadata unavailable" {
		t.Errorf("venue status = %s (%q), want STALE", core.StatusName(status), reason)
	}

	tr.MetadataState(true, "")
	wantStatus(t, tr, spec, pb.Status_STATUS_HEALTHY, "")

	// It can go back: a refresher that starts failing again says so.
	tr.MetadataState(false, "metadata unavailable")
	wantStatus(t, tr, spec, pb.Status_STATUS_STALE, "metadata unavailable")
}

// TestMetadataNotRequiredIsHealthyFromTheStart: a venue file that does not make
// metadata a startup dependency must not be held at STALE by one.
func TestMetadataNotRequiredIsHealthyFromTheStart(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	spec := specs(t)[0]

	tr.Received(spec)
	wantStatus(t, tr, spec, pb.Status_STATUS_HEALTHY, "")
}
