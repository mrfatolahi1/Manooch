package health_test

import (
	"context"
	"testing"
	"time"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/publish"
)

func instrumentKey() string {
	return publish.Key("TESTVENUE", manoochv1.MarketType_MARKET_TYPE_PERP_LINEAR, "BTC_USDT", manoochv1.Channel_CHANNEL_HEALTH)
}

// find returns the last message published to key.
func find(messages []recorded, key string) *recorded {
	for i := len(messages) - 1; i >= 0; i-- {
		if messages[i].key == key {
			return &messages[i]
		}
	}
	return nil
}

// TestPublishesOnTransition: a consumer that learns about a transition one
// heartbeat late is a consumer that traded through it.
func TestPublishesOnTransition(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	specification := specifications(t)[0]
	tracker.Received(specification)
	recorder.reset()

	tracker.FallbackEngaged(specification)

	got := find(recorder.all(), instrumentKey())
	if got == nil {
		t.Fatalf("no health message published for %s; got %v", instrumentKey(), recorder.all())
	}
	if got.health.Status != manoochv1.Status_STATUS_DEGRADED {
		t.Errorf("status = %s, want DEGRADED", core.StatusName(got.health.Status))
	}
	if !got.health.FallbackActive {
		t.Error("fallback_active is false while the stream is on REST")
	}
	if got.health.Env.Source != manoochv1.Source_SOURCE_REST {
		t.Errorf("source = %s, want REST", core.SourceName(got.health.Env.Source))
	}
}

// TestNoPublishWithoutATransition: the heartbeat is the only thing that
// republishes an unchanged status, or every message would carry one.
func TestNoPublishWithoutATransition(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	specification := specifications(t)[0]
	tracker.Received(specification)
	recorder.reset()

	for range 100 {
		tracker.Received(specification)
	}
	if n := len(recorder.all()); n != 0 {
		t.Errorf("%d health messages published for 100 unchanged updates", n)
	}
}

// TestVenueKeyCarriesConnectionState: socket state, skew and leaks belong to no
// single stream, so they need a key of their own.
func TestVenueKeyCarriesConnectionState(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	tracker.Received(specifications(t)[0])
	recorder.reset()

	tracker.Leaked(3)

	key := publish.VenueKey("TESTVENUE", publish.SubjectHealth)
	got := find(recorder.all(), key)
	if got == nil {
		t.Fatalf("no venue health message published to %s", key)
	}
	if got.health.LeakedGoroutines != 3 {
		t.Errorf("leaked_goroutines = %d, want 3", got.health.LeakedGoroutines)
	}
	if got.health.Status != manoochv1.Status_STATUS_DEGRADED {
		t.Errorf("venue status = %s, want DEGRADED", core.StatusName(got.health.Status))
	}
}

// TestHeartbeatPublishesUnchangedState is why the heartbeat exists: Pub/Sub is
// fire-and-forget, so without it "healthy and quiet" and "the health publisher
// is dead" are the same observation.
func TestHeartbeatPublishesUnchangedState(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	tracker.Received(specifications(t)[0])

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); tracker.Run(ctx) }()

	// Three heartbeats at the 1s interval the tracker was built with would take
	// three seconds; one immediate beat plus one tick is enough to prove the
	// ticker republishes with nothing changed.
	deadline := time.After(5 * time.Second)
	for {
		messages := recorder.all()
		if countKey(messages, instrumentKey()) >= 2 && countKey(messages, publish.VenueKey("TESTVENUE", publish.SubjectHealth)) >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("fewer than two heartbeats in 5s: %d messages", len(messages))
		case <-time.After(20 * time.Millisecond):
		}
	}

	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after its context was cancelled")
	}
}

// TestHealthKeyExpiresIfThePublisherStops: the health channel has to be
// detectably dead itself, or the last message ever published sits in Redis
// looking current forever.
func TestHealthKeyExpiresIfThePublisherStops(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	tracker.Received(specifications(t)[0])
	recorder.reset()

	tracker.KeyExpired(specifications(t)[0])

	got := find(recorder.all(), instrumentKey())
	if got == nil {
		t.Fatal("no health message published")
	}
	// heartbeat_interval 1s × 3.
	if want := 3 * time.Second; got.timeToLive != want {
		t.Errorf("health key TTL = %v, want %v", got.timeToLive, want)
	}
}

// TestUnknownStatusIsNotPublished: a key whose presence claims the publisher is
// alive while its content says nothing is worse than no key.
func TestUnknownStatusIsNotPublished(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	_ = tracker

	for _, recorded := range recorder.all() {
		if recorded.health.Status == manoochv1.Status_STATUS_UNSPECIFIED {
			t.Errorf("published %s with an unspecified status", recorded.key)
		}
		if recorded.health.Env.Status == manoochv1.Status_STATUS_UNSPECIFIED {
			t.Errorf("published %s with an unspecified envelope status", recorded.key)
		}
	}
}

// TestRestartsAndAgeReachTheKey, which is what manooch-status reads for its
// RESTARTS column.
func TestRestartsAndAgeReachTheKey(t *testing.T) {
	clock, recorder := newClock(), &recorder{}
	tracker := newTracker(t, clock, recorder)
	all := specifications(t)
	for _, specification := range all {
		tracker.Received(specification)
	}

	tracker.StreamRestarted(all[0])
	tracker.StreamRestarted(all[1])
	clock.advance(1500 * time.Millisecond)
	recorder.reset()
	tracker.KeyExpired(all[2]) // any transition, to force a publish

	got := find(recorder.all(), instrumentKey())
	if got == nil {
		t.Fatal("no health message published")
	}
	if got.health.StreamRestartCount != 2 {
		t.Errorf("stream_restart_count = %d, want 2", got.health.StreamRestartCount)
	}
	if got.health.LastMessageAgeMs != 1500 {
		t.Errorf("last_message_age_ms = %d, want 1500", got.health.LastMessageAgeMs)
	}
}

func countKey(messages []recorded, key string) int {
	n := 0
	for _, recorded := range messages {
		if recorded.key == key {
			n++
		}
	}
	return n
}
