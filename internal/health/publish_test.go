package health_test

import (
	"context"
	"testing"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/publish"
)

func instrumentKey() string {
	return publish.Key("TESTVENUE", pb.MarketType_MARKET_TYPE_PERP_LINEAR, "BTC_USDT", pb.Channel_CHANNEL_HEALTH)
}

// find returns the last message published to key.
func find(msgs []recorded, key string) *recorded {
	for i := len(msgs) - 1; i >= 0; i-- {
		if msgs[i].key == key {
			return &msgs[i]
		}
	}
	return nil
}

// TestPublishesOnTransition: a consumer that learns about a transition one
// heartbeat late is a consumer that traded through it.
func TestPublishesOnTransition(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	spec := specs(t)[0]
	tr.Received(spec)
	pub.reset()

	tr.FallbackEngaged(spec)

	got := find(pub.all(), instrumentKey())
	if got == nil {
		t.Fatalf("no health message published for %s; got %v", instrumentKey(), pub.all())
	}
	if got.health.Status != pb.Status_STATUS_DEGRADED {
		t.Errorf("status = %s, want DEGRADED", core.StatusName(got.health.Status))
	}
	if !got.health.FallbackActive {
		t.Error("fallback_active is false while the stream is on REST")
	}
	if got.health.Env.Source != pb.Source_SOURCE_REST {
		t.Errorf("source = %s, want REST", core.SourceName(got.health.Env.Source))
	}
}

// TestNoPublishWithoutATransition: the heartbeat is the only thing that
// republishes an unchanged status, or every message would carry one.
func TestNoPublishWithoutATransition(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	spec := specs(t)[0]
	tr.Received(spec)
	pub.reset()

	for range 100 {
		tr.Received(spec)
	}
	if n := len(pub.all()); n != 0 {
		t.Errorf("%d health messages published for 100 unchanged updates", n)
	}
}

// TestVenueKeyCarriesConnectionState: socket state, skew and leaks belong to no
// single stream, so they need a key of their own.
func TestVenueKeyCarriesConnectionState(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	tr.Received(specs(t)[0])
	pub.reset()

	tr.Leaked(3)

	key := publish.VenueKey("TESTVENUE", publish.SubjectHealth)
	got := find(pub.all(), key)
	if got == nil {
		t.Fatalf("no venue health message published to %s", key)
	}
	if got.health.LeakedGoroutines != 3 {
		t.Errorf("leaked_goroutines = %d, want 3", got.health.LeakedGoroutines)
	}
	if got.health.Status != pb.Status_STATUS_DEGRADED {
		t.Errorf("venue status = %s, want DEGRADED", core.StatusName(got.health.Status))
	}
}

// TestHeartbeatPublishesUnchangedState is why the heartbeat exists: Pub/Sub is
// fire-and-forget, so without it "healthy and quiet" and "the health publisher
// is dead" are the same observation.
func TestHeartbeatPublishesUnchangedState(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	tr.Received(specs(t)[0])

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	done := make(chan struct{})
	go func() { defer close(done); tr.Run(ctx) }()

	// Three heartbeats at the 1s interval the tracker was built with would take
	// three seconds; one immediate beat plus one tick is enough to prove the
	// ticker republishes with nothing changed.
	deadline := time.After(5 * time.Second)
	for {
		msgs := pub.all()
		if countKey(msgs, instrumentKey()) >= 2 && countKey(msgs, publish.VenueKey("TESTVENUE", publish.SubjectHealth)) >= 2 {
			break
		}
		select {
		case <-deadline:
			t.Fatalf("fewer than two heartbeats in 5s: %d messages", len(msgs))
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
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	tr.Received(specs(t)[0])
	pub.reset()

	tr.KeyExpired(specs(t)[0])

	got := find(pub.all(), instrumentKey())
	if got == nil {
		t.Fatal("no health message published")
	}
	// heartbeat_interval 1s × 3.
	if want := 3 * time.Second; got.ttl != want {
		t.Errorf("health key TTL = %v, want %v", got.ttl, want)
	}
}

// TestUnknownStatusIsNotPublished: a key whose presence claims the publisher is
// alive while its content says nothing is worse than no key.
func TestUnknownStatusIsNotPublished(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	_ = tr

	for _, m := range pub.all() {
		if m.health.Status == pb.Status_STATUS_UNSPECIFIED {
			t.Errorf("published %s with an unspecified status", m.key)
		}
		if m.health.Env.Status == pb.Status_STATUS_UNSPECIFIED {
			t.Errorf("published %s with an unspecified envelope status", m.key)
		}
	}
}

// TestRestartsAndAgeReachTheKey, which is what manooch-status reads for its
// RESTARTS column.
func TestRestartsAndAgeReachTheKey(t *testing.T) {
	c, pub := newClock(), &recorder{}
	tr := newTracker(t, c, pub)
	all := specs(t)
	for _, s := range all {
		tr.Received(s)
	}

	tr.StreamRestarted(all[0])
	tr.StreamRestarted(all[1])
	c.advance(1500 * time.Millisecond)
	pub.reset()
	tr.KeyExpired(all[2]) // any transition, to force a publish

	got := find(pub.all(), instrumentKey())
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

func countKey(msgs []recorded, key string) int {
	n := 0
	for _, m := range msgs {
		if m.key == key {
			n++
		}
	}
	return n
}
