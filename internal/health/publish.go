package health

import (
	"context"
	"time"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/publish"
)

// healthTimeToLiveMultiple is how many heartbeats a health key survives
// without one.
//
// The health channel has to be detectably dead itself. If the publisher stops,
// its own key expires and a consumer sees that; without a TTL here, the last
// health message ever published would sit in Redis looking current forever.
const healthTimeToLiveMultiple = 3

// Run publishes health until ctx ends.
//
// It publishes on a ticker whatever else happens, because Redis Pub/Sub is
// fire-and-forget: without a heartbeat, "healthy and quiet" and "the health
// publisher is dead" are the same observation. Silence must never be ambiguous.
//
// The tick also recomputes every stream, which is how the purely time-based
// transitions happen — fallback crossing its maximum duration is one no event
// fires for.
func (tracker *Tracker) Run(ctx context.Context) {
	tick := time.NewTicker(tracker.options.HeartbeatInterval)
	defer tick.Stop()

	tracker.beat(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			tracker.beat(ctx)
		}
	}
}

// beat refreshes every stream and publishes the whole set.
func (tracker *Tracker) beat(ctx context.Context) {
	tracker.mutex.Lock()
	// The transitions refresh returns are discarded: snapshotAll republishes
	// every key anyway, and the heartbeat is what proves the publisher alive.
	tracker.refresh(tracker.order)
	messages := tracker.snapshotAll()
	tracker.mutex.Unlock()

	tracker.write(ctx, messages)
}

// ---------- snapshots ----------

// A snapshot is one health message ready to publish, taken under the mutex so
// the Redis write happens outside it.
type snapshot struct {
	key     string
	message *manoochv1.Health
}

// snapshotAll is every instrument plus the venue. Callers hold the mutex.
func (tracker *Tracker) snapshotAll() []snapshot {
	out := make([]snapshot, 0, len(tracker.order)+1)
	for _, instrument := range tracker.order {
		out = append(out, tracker.snapshotInstrument(instrument))
	}
	return append(out, tracker.snapshotVenue())
}

// snapshotInstrument builds one instrument's health message. Callers hold the
// mutex.
//
// The message carries the worst of the instrument's channels. Per-channel
// status is not lost by that: every data key's own envelope carries its own
// status and reason, which is what a consumer reading a price sees first.
func (tracker *Tracker) snapshotInstrument(instrument *instrument) snapshot {
	var (
		oldest      time.Time
		restarts    uint32
		onFallback  bool
		anyReceived bool
	)
	for _, stream := range instrument.streams {
		restarts += stream.restarts
		if !stream.fallbackSince.IsZero() {
			onFallback = true
		}
		if stream.lastMessage.IsZero() {
			continue
		}
		if !anyReceived || stream.lastMessage.Before(oldest) {
			oldest, anyReceived = stream.lastMessage, true
		}
	}

	// Age is the oldest of the instrument's channels: the freshest one says
	// nothing about whether the other two are still arriving. Minus one is
	// "nothing has ever arrived", which is not the same as "arrived just now".
	var ageMilliseconds int64 = -1
	if anyReceived {
		ageMilliseconds = tracker.now().Sub(oldest).Milliseconds()
	}

	envelope := &manoochv1.Envelope{
		Venue:      tracker.options.Venue,
		Instrument: instrument.reference.Proto(instrument.venueSymbol),
		Channel:    manoochv1.Channel_CHANNEL_HEALTH,
		// No exchange time: nothing here came from the venue.
		RecvTimeNs:   oldest.UnixNano(),
		Source:       manoochv1.Source_SOURCE_WEBSOCKET,
		Status:       instrument.status,
		StatusReason: instrument.reason,
	}
	if onFallback {
		envelope.Source = manoochv1.Source_SOURCE_REST
	}
	if !anyReceived {
		envelope.RecvTimeNs = 0
	}

	return snapshot{key: instrument.key, message: &manoochv1.Health{
		Env:                envelope,
		Status:             instrument.status,
		Reason:             instrument.reason,
		LastMessageAgeMs:   ageMilliseconds,
		ReconnectCount:     tracker.reconnects,
		StreamRestartCount: restarts,
		FallbackActive:     onFallback,
		ClockSkewMs:        tracker.skewMS,
		LeakedGoroutines:   uint32(tracker.leaked),
	}}
}

// snapshotVenue builds the connection-level message: socket state, clock skew
// and leaked goroutines, none of which belong to any one stream. Callers hold
// the mutex.
func (tracker *Tracker) snapshotVenue() snapshot {
	return snapshot{
		key: publish.VenueKey(tracker.options.Venue, publish.SubjectHealth),
		message: &manoochv1.Health{
			Env: &manoochv1.Envelope{
				Venue:        tracker.options.Venue,
				Channel:      manoochv1.Channel_CHANNEL_HEALTH,
				Status:       tracker.venueStatus,
				StatusReason: tracker.venueReason,
			},
			Status:           tracker.venueStatus,
			Reason:           tracker.venueReason,
			LastMessageAgeMs: -1,
			ReconnectCount:   tracker.reconnects,
			ClockSkewMs:      tracker.skewMS,
			LeakedGoroutines: uint32(tracker.leaked),
		},
	}
}

// write publishes a batch of snapshots. A failed health write is already
// counted and rate-limit logged by the publisher; there is nothing useful to do
// about it here, and the key expiring is itself the signal.
func (tracker *Tracker) write(ctx context.Context, messages []snapshot) {
	timeToLive := tracker.options.HeartbeatInterval * healthTimeToLiveMultiple
	for _, snapshot := range messages {
		if snapshot.message.Env.Status == manoochv1.Status_STATUS_UNSPECIFIED {
			// Nothing has reported yet. Publishing a status of "unknown" is
			// worse than publishing nothing: the key's presence would claim
			// the publisher is alive and its content would say nothing.
			continue
		}
		// The snapshot was built fresh under the mutex, so the publisher owns
		// the envelope it is about to stamp and nothing else holds it.
		_ = tracker.options.Publisher.Publish(ctx, snapshot.key, snapshot.message, timeToLive)
	}
}
