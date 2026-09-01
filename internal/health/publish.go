package health

import (
	"context"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/publish"
)

// healthTTLMultiple is how many heartbeats a health key survives without one.
//
// The health channel has to be detectably dead itself. If the publisher stops,
// its own key expires and a consumer sees that; without a TTL here, the last
// health message ever published would sit in Redis looking current forever.
const healthTTLMultiple = 3

// Run publishes health until ctx ends.
//
// It publishes on a ticker whatever else happens, because Redis Pub/Sub is
// fire-and-forget: without a heartbeat, "healthy and quiet" and "the health
// publisher is dead" are the same observation. Silence must never be ambiguous.
//
// The tick also recomputes every stream, which is how the purely time-based
// transitions happen — fallback crossing its maximum duration is one no event
// fires for.
func (t *Tracker) Run(ctx context.Context) {
	tick := time.NewTicker(t.opts.HeartbeatInterval)
	defer tick.Stop()

	t.beat(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			t.beat(ctx)
		}
	}
}

// beat refreshes every stream and publishes the whole set.
func (t *Tracker) beat(ctx context.Context) {
	t.mu.Lock()
	// The transitions refresh returns are discarded: snapshotAll republishes
	// every key anyway, and the heartbeat is what proves the publisher alive.
	t.refresh(t.order)
	msgs := t.snapshotAll()
	t.mu.Unlock()

	t.write(ctx, msgs)
}

// ---------- snapshots ----------

// A snapshot is one health message ready to publish, taken under the mutex so
// the Redis write happens outside it.
type snapshot struct {
	key string
	msg *pb.Health
}

// snapshotAll is every instrument plus the venue. Callers hold the mutex.
func (t *Tracker) snapshotAll() []snapshot {
	out := make([]snapshot, 0, len(t.order)+1)
	for _, in := range t.order {
		out = append(out, t.snapshotInstrument(in))
	}
	return append(out, t.snapshotVenue())
}

// snapshotInstrument builds one instrument's health message. Callers hold the
// mutex.
//
// The message carries the worst of the instrument's channels. Per-channel
// status is not lost by that: every data key's own envelope carries its own
// status and reason, which is what a consumer reading a price sees first.
func (t *Tracker) snapshotInstrument(in *instrument) snapshot {
	var (
		oldest      time.Time
		restarts    uint32
		onFallback  bool
		anyReceived bool
	)
	for _, s := range in.streams {
		restarts += s.restarts
		if !s.fallbackSince.IsZero() {
			onFallback = true
		}
		if s.lastMessage.IsZero() {
			continue
		}
		if !anyReceived || s.lastMessage.Before(oldest) {
			oldest, anyReceived = s.lastMessage, true
		}
	}

	// Age is the oldest of the instrument's channels: the freshest one says
	// nothing about whether the other two are still arriving. Minus one is
	// "nothing has ever arrived", which is not the same as "arrived just now".
	var ageMS int64 = -1
	if anyReceived {
		ageMS = t.now().Sub(oldest).Milliseconds()
	}

	env := &pb.Envelope{
		Venue:      t.opts.Venue,
		Instrument: in.ref.Proto(in.venueSymbol),
		Channel:    pb.Channel_CHANNEL_HEALTH,
		// No exchange time: nothing here came from the venue.
		RecvTimeNs:   oldest.UnixNano(),
		Source:       pb.Source_SOURCE_WEBSOCKET,
		Status:       in.status,
		StatusReason: in.reason,
	}
	if onFallback {
		env.Source = pb.Source_SOURCE_REST
	}
	if !anyReceived {
		env.RecvTimeNs = 0
	}

	return snapshot{key: in.key, msg: &pb.Health{
		Env:                env,
		Status:             in.status,
		Reason:             in.reason,
		LastMessageAgeMs:   ageMS,
		ReconnectCount:     t.reconnects,
		StreamRestartCount: restarts,
		FallbackActive:     onFallback,
		ClockSkewMs:        t.skewMS,
		LeakedGoroutines:   uint32(t.leaked),
	}}
}

// snapshotVenue builds the connection-level message: socket state, clock skew
// and leaked goroutines, none of which belong to any one stream. Callers hold
// the mutex.
func (t *Tracker) snapshotVenue() snapshot {
	return snapshot{
		key: publish.VenueKey(t.opts.Venue, publish.SubjectHealth),
		msg: &pb.Health{
			Env: &pb.Envelope{
				Venue:        t.opts.Venue,
				Channel:      pb.Channel_CHANNEL_HEALTH,
				Status:       t.venueStatus,
				StatusReason: t.venueReason,
			},
			Status:           t.venueStatus,
			Reason:           t.venueReason,
			LastMessageAgeMs: -1,
			ReconnectCount:   t.reconnects,
			ClockSkewMs:      t.skewMS,
			LeakedGoroutines: uint32(t.leaked),
		},
	}
}

// write publishes a batch of snapshots. A failed health write is already
// counted and rate-limit logged by the publisher; there is nothing useful to do
// about it here, and the key expiring is itself the signal.
func (t *Tracker) write(ctx context.Context, msgs []snapshot) {
	ttl := t.opts.HeartbeatInterval * healthTTLMultiple
	for _, m := range msgs {
		if m.msg.Env.Status == pb.Status_STATUS_UNSPECIFIED {
			// Nothing has reported yet. Publishing a status of "unknown" is
			// worse than publishing nothing: the key's presence would claim
			// the publisher is alive and its content would say nothing.
			continue
		}
		// The snapshot was built fresh under the mutex, so the publisher owns
		// the envelope it is about to stamp and nothing else holds it.
		_ = t.opts.Publisher.Publish(ctx, m.key, m.msg, ttl)
	}
}
