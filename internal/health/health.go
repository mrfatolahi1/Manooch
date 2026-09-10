// Package health owns what a consumer reads to decide whether to trust a price.
//
// Freshness itself is not kept here: it is the Redis key's TTL, so the
// last-value cache and the liveness signal are one object and cannot drift
// apart. What this package adds is the part a TTL cannot express — that a key
// is fresh but currently sourced from REST, that a socket is reconnecting,
// that the venue's clock and ours disagree — which is exactly what a strategy
// needs to know while the data is still within TTL.
//
// Every channel in scope is a scheduled stream: the venue promises a cadence,
// so silence is unambiguous and a TTL always means something. There is no
// machinery here for an event-driven stream that legitimately goes quiet.
package health

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/observability"
	"github.com/you/manooch/internal/publish"
)

// Socket states, in order of how bad they are.
const (
	// SocketConnected is dialled and delivering.
	SocketConnected = "connected"
	// SocketDialing is a connection attempt in flight, or the backoff before
	// one. Streams on it are DEGRADED: the data may still be inside its TTL.
	SocketDialing = "dialing"
	// SocketCircuitOpen is the breaker refusing attempts. Streams on it are
	// STALE: nothing will arrive for at least the open duration.
	SocketCircuitOpen = "circuit_open"
)

// Options configures a Tracker.
type Options struct {
	// Venue is the canonical upper-case venue name.
	Venue string

	// Publisher writes the health keys. Required.
	Publisher publish.Publisher

	Metrics *observability.Metrics
	Log     *slog.Logger

	// HeartbeatInterval is how often health republishes with nothing changed.
	// The health key's own TTL is three times this, so a health publisher that
	// stops is itself detectable.
	HeartbeatInterval time.Duration

	// ClockSkewDegradedMS and ClockSkewStaleMS are the thresholds on the gap
	// between the venue's clock and ours.
	ClockSkewDegradedMS int64
	ClockSkewStaleMS    int64

	// FallbackMaxDuration is how long a stream may be served by REST before it
	// stops being a degradation and becomes a failure.
	FallbackMaxDuration time.Duration

	// MetadataRequired holds every stream at STALE until MetadataState says
	// the instrument metadata has arrived. Without tick size, lot size and the
	// contract multiplier a consumer cannot size an order against the price we
	// are publishing, so a feed that streams before metadata lands is a feed
	// nobody can act on.
	MetadataRequired bool

	// Now is swappable for tests. Zero means time.Now.
	Now func() time.Time
}

// A Tracker holds every stream's state and computes its status from that state
// plus the configured thresholds. It is safe for concurrent use: one goroutine
// per stream reports into it, and the heartbeat reads all of them.
type Tracker struct {
	options Options
	now     func() time.Time

	mutex   sync.Mutex
	streams map[core.StreamSpec]*stream
	// order is registration order, so the heartbeat publishes the same
	// instruments in the same sequence every tick.
	order      []*instrument
	sockets    map[string]*socket
	skewMS     int64
	leaked     int
	reconnects uint32

	metadataOK     bool
	metadataReason string

	venueStatus manoochv1.Status
	venueReason string
}

// A stream is one (instrument, channel): exactly one Redis key.
type stream struct {
	specification core.StreamSpec
	instrument    *instrument
	socketID      string

	lastMessage time.Time
	source      manoochv1.Source
	restarts    uint32

	// expired is set when the key reached its TTL and cleared by the next
	// message. It is not a timestamp comparison: Redis told us.
	expired bool
	// rejected is set when a frame on this stream's socket could not be
	// parsed, and cleared by the next message that could.
	rejected bool

	fallbackSince time.Time
	fallbackFail  string

	status manoochv1.Status
	reason string
}

// An instrument groups the channels that share one health key.
type instrument struct {
	reference   core.InstrumentRef
	venueSymbol string
	key         string
	streams     []*stream

	status manoochv1.Status
	reason string
}

type socket struct {
	id     string
	state  string
	reason string
}

// New builds a tracker. It publishes nothing until Run or an event says so.
func New(options Options) (*Tracker, error) {
	if options.Venue == "" {
		return nil, fmt.Errorf("health: no venue")
	}
	if options.Publisher == nil {
		return nil, fmt.Errorf("health: no publisher")
	}
	if options.Log == nil {
		return nil, fmt.Errorf("health: no logger")
	}
	if options.HeartbeatInterval <= 0 {
		return nil, fmt.Errorf("health: heartbeat interval is %v", options.HeartbeatInterval)
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &Tracker{
		options:        options,
		now:            options.Now,
		streams:        map[core.StreamSpec]*stream{},
		sockets:        map[string]*socket{},
		metadataOK:     !options.MetadataRequired,
		metadataReason: "metadata unavailable",
		venueStatus:    manoochv1.Status_STATUS_UNSPECIFIED,
	}, nil
}

// Register declares a stream before anything reports on it. socketID names the
// connection that carries it, so a socket-level event reaches the right
// streams. An unregistered specification is ignored everywhere else: a stream
// nobody declared has no key, and inventing one would publish a status for a
// stream that does not exist.
func (tracker *Tracker) Register(specification core.StreamSpec, venueSymbol, socketID string) {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()

	if _, duplicate := tracker.streams[specification]; duplicate {
		return
	}
	tracked := tracker.instrumentFor(specification.Instrument, venueSymbol)
	newStream := &stream{
		specification: specification,
		instrument:    tracked,
		socketID:      socketID,
		source:        manoochv1.Source_SOURCE_WEBSOCKET,
		status:        manoochv1.Status_STATUS_UNSPECIFIED,
	}
	tracked.streams = append(tracked.streams, newStream)
	tracker.streams[specification] = newStream

	if _, ok := tracker.sockets[socketID]; !ok && socketID != "" {
		tracker.sockets[socketID] = &socket{id: socketID, state: SocketDialing, reason: "not yet connected"}
	}
	// Seeded now rather than on the first event, so a stream that never
	// receives anything still has a status to publish.
	tracker.refresh([]*instrument{tracked})
}

// instrumentFor finds or creates the instrument a stream belongs to. Callers
// hold the mutex.
func (tracker *Tracker) instrumentFor(reference core.InstrumentRef, venueSymbol string) *instrument {
	for _, instrument := range tracker.order {
		if instrument.reference == reference {
			return instrument
		}
	}
	instrument := &instrument{
		reference:   reference,
		venueSymbol: venueSymbol,
		key:         publish.Key(tracker.options.Venue, reference.MarketType, reference.Canonical(), manoochv1.Channel_CHANNEL_HEALTH),
		status:      manoochv1.Status_STATUS_UNSPECIFIED,
	}
	tracker.order = append(tracker.order, instrument)
	return instrument
}

// Specifications is every registered stream, in registration order. The
// fallback watcher needs the set to sweep.
func (tracker *Tracker) Specifications() []core.StreamSpec {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()

	out := make([]core.StreamSpec, 0, len(tracker.streams))
	for _, instrument := range tracker.order {
		for _, stream := range instrument.streams {
			out = append(out, stream.specification)
		}
	}
	return out
}

// ---------- events ----------

// Received records a websocket message for a stream. It is called before the
// message is stamped and published, so the status that goes onto the wire
// already reflects the arrival rather than the state it recovered from.
func (tracker *Tracker) Received(specification core.StreamSpec) {
	tracker.update(func() []*instrument {
		stream := tracker.streams[specification]
		if stream == nil {
			return nil
		}
		stream.lastMessage = tracker.now()
		stream.source = manoochv1.Source_SOURCE_WEBSOCKET
		stream.expired = false
		stream.rejected = false
		return []*instrument{stream.instrument}
	})
}

// Polled records a successful REST fallback poll. The stream stays on fallback
// — only a websocket message ends that — but the value is current again, so a
// previous poll failure is cleared.
func (tracker *Tracker) Polled(specification core.StreamSpec) {
	tracker.update(func() []*instrument {
		stream := tracker.streams[specification]
		if stream == nil {
			return nil
		}
		stream.lastMessage = tracker.now()
		stream.source = manoochv1.Source_SOURCE_REST
		stream.expired = false
		stream.fallbackFail = ""
		return []*instrument{stream.instrument}
	})
}

// KeyExpired records that a stream's Redis key reached its TTL.
func (tracker *Tracker) KeyExpired(specification core.StreamSpec) {
	tracker.update(func() []*instrument {
		stream := tracker.streams[specification]
		if stream == nil {
			return nil
		}
		if tracker.options.Metrics != nil && !stream.expired {
			tracker.options.Metrics.KeyExpired.WithLabelValues(tracker.options.Venue, core.ChannelName(specification.Channel)).Inc()
		}
		stream.expired = true
		return []*instrument{stream.instrument}
	})
}

// FrameRejected records a frame on a socket that could not be parsed. It marks
// every stream the socket carries, because a frame that did not parse names no
// channel: attributing it to none of them would leave a venue sending shapes we
// cannot read looking perfectly healthy for as long as the keys stay inside
// their TTL.
func (tracker *Tracker) FrameRejected(socketID string) {
	tracker.update(func() []*instrument {
		var touched []*instrument
		for _, instrument := range tracker.order {
			marked := false
			for _, stream := range instrument.streams {
				if stream.socketID == socketID && !stream.rejected {
					stream.rejected = true
					marked = true
				}
			}
			if marked {
				touched = append(touched, instrument)
			}
		}
		return touched
	})
}

// StreamRestarted counts a tier-1 restart of one stream's goroutine.
func (tracker *Tracker) StreamRestarted(specification core.StreamSpec) {
	tracker.update(func() []*instrument {
		stream := tracker.streams[specification]
		if stream == nil {
			return nil
		}
		stream.restarts++
		if tracker.options.Metrics != nil {
			tracker.options.Metrics.StreamRestarts.WithLabelValues(
				tracker.options.Venue, core.MarketTypeName(stream.specification.Instrument.MarketType),
				stream.specification.Instrument.Canonical(), core.ChannelName(stream.specification.Channel)).Inc()
		}
		return []*instrument{stream.instrument}
	})
}

// FallbackEngaged records that a stream is now served by REST polling.
func (tracker *Tracker) FallbackEngaged(specification core.StreamSpec) {
	tracker.update(func() []*instrument {
		stream := tracker.streams[specification]
		if stream == nil || !stream.fallbackSince.IsZero() {
			return nil
		}
		stream.fallbackSince = tracker.now()
		stream.source = manoochv1.Source_SOURCE_REST
		tracker.setFallbackMetric(stream, 1)
		return []*instrument{stream.instrument}
	})
}

// FallbackFailed records that fallback could not serve a stream — the poll
// errored, the venue answered nothing usable, or the concurrency cap was
// reached. The stream goes STALE. A fallback that quietly stops is the failure
// this whole service exists to prevent.
func (tracker *Tracker) FallbackFailed(specification core.StreamSpec, reason string) {
	tracker.update(func() []*instrument {
		stream := tracker.streams[specification]
		if stream == nil || stream.fallbackFail == reason {
			return nil
		}
		stream.fallbackFail = reason
		return []*instrument{stream.instrument}
	})
}

// FallbackDisengaged records that a stream is back on its socket.
func (tracker *Tracker) FallbackDisengaged(specification core.StreamSpec) {
	tracker.update(func() []*instrument {
		stream := tracker.streams[specification]
		if stream == nil || (stream.fallbackSince.IsZero() && stream.fallbackFail == "") {
			return nil
		}
		stream.fallbackSince = time.Time{}
		stream.fallbackFail = ""
		stream.source = manoochv1.Source_SOURCE_WEBSOCKET
		tracker.setFallbackMetric(stream, 0)
		return []*instrument{stream.instrument}
	})
}

// MetadataState records whether the venue's instrument metadata has been
// fetched. Until it has, every stream on the venue is STALE and nothing
// streams: publishing a price with unknown precision is publishing a number
// nobody can size an order from.
//
// It does nothing at all when the venue file did not make metadata a startup
// requirement. Metadata is a gate or it is not, and a venue that opted out must
// not be taken STALE by a refresh that happens to be failing.
//
// reason is ignored when ok is true.
func (tracker *Tracker) MetadataState(ok bool, reason string) {
	if !tracker.options.MetadataRequired {
		return
	}
	tracker.update(func() []*instrument {
		if tracker.metadataOK == ok && (ok || tracker.metadataReason == reason) {
			return nil
		}
		tracker.metadataOK = ok
		if !ok {
			tracker.metadataReason = reason
		}
		return tracker.order
	})
}

// SocketState records what one connection is doing. state is one of
// SocketConnected, SocketDialing or SocketCircuitOpen.
func (tracker *Tracker) SocketState(socketID, state, reason string) {
	tracker.update(func() []*instrument {
		tracked := tracker.sockets[socketID]
		if tracked == nil {
			tracked = &socket{id: socketID}
			tracker.sockets[socketID] = tracked
		}
		if tracked.state == state && tracked.reason == reason {
			return nil
		}
		tracked.state, tracked.reason = state, reason
		return tracker.instrumentsOn(socketID)
	})
}

// Reconnected counts one completed reconnection of a socket.
func (tracker *Tracker) Reconnected(socketID string) {
	tracker.update(func() []*instrument {
		tracker.reconnects++
		if tracker.options.Metrics != nil {
			tracker.options.Metrics.Reconnects.WithLabelValues(tracker.options.Venue, socketID).Inc()
		}
		return nil
	})
}

// ClockSkew records the gap between the venue's clock and ours, in
// milliseconds, signed: a venue clock ahead of ours reads positive. The sign is
// kept because losing it hides which way the two disagree, and every freshness
// number depends on the answer.
func (tracker *Tracker) ClockSkew(milliseconds int64) {
	tracker.update(func() []*instrument {
		if tracker.skewMS == milliseconds {
			return nil
		}
		tracker.skewMS = milliseconds
		if tracker.options.Metrics != nil {
			tracker.options.Metrics.ClockSkewMS.WithLabelValues(tracker.options.Venue).Set(float64(milliseconds))
		}
		return tracker.order
	})
}

// Leaked records how many goroutines failed to return within the leak timeout.
//
// There is no self-kill, so leaks accumulate; making them visible is the price
// of never restarting the process. Any value above zero holds the venue at
// DEGRADED until an operator does something about it.
func (tracker *Tracker) Leaked(n int) {
	tracker.update(func() []*instrument {
		if n == tracker.leaked {
			return nil
		}
		if n > tracker.leaked {
			tracker.options.Log.Error("goroutines leaked", "count", n)
		}
		tracker.leaked = n
		if tracker.options.Metrics != nil {
			tracker.options.Metrics.LeakedGoroutines.WithLabelValues(tracker.options.Venue).Set(float64(n))
		}
		return nil
	})
}

// ---------- status ----------

// Status is a stream's current status and, when it is not healthy, why. It is
// what a producer stamps into the envelope immediately before publishing.
func (tracker *Tracker) Status(specification core.StreamSpec) (manoochv1.Status, string) {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()

	stream := tracker.streams[specification]
	if stream == nil {
		// A stream nobody registered has no state to judge. STALE is the only
		// safe answer: the alternative publishes data as healthy on the
		// strength of knowing nothing about it.
		return manoochv1.Status_STATUS_STALE, "stream not registered"
	}
	return tracker.compute(stream)
}

// VenueStatus is the connection-level status: socket state, clock skew and
// leaked goroutines, none of which belong to any one stream.
func (tracker *Tracker) VenueStatus() (manoochv1.Status, string) {
	tracker.mutex.Lock()
	defer tracker.mutex.Unlock()
	return tracker.computeVenue()
}

// compute derives a stream's status from its state. Callers hold the mutex.
//
// STALE is tested before DEGRADED throughout: the two differ by whether a
// consumer may trade on the data, so a stream that qualifies for both must
// report the one that says stop.
func (tracker *Tracker) compute(stream *stream) (manoochv1.Status, string) {
	socket := tracker.sockets[stream.socketID]

	// --- STALE: do not trade this venue.
	if !tracker.metadataOK {
		// First, and above everything else: without metadata there is no
		// price being published at all, so no other reason has happened yet.
		return manoochv1.Status_STATUS_STALE, tracker.metadataReason
	}
	if socket != nil && socket.state == SocketCircuitOpen {
		return manoochv1.Status_STATUS_STALE, "circuit open"
	}
	if stream.fallbackFail != "" {
		return manoochv1.Status_STATUS_STALE, stream.fallbackFail
	}
	if !stream.fallbackSince.IsZero() && tracker.options.FallbackMaxDuration > 0 &&
		tracker.now().Sub(stream.fallbackSince) >= tracker.options.FallbackMaxDuration {
		// Long-running fallback is a failure, not a steady state.
		return manoochv1.Status_STATUS_STALE, "rest fallback for " + tracker.now().Sub(stream.fallbackSince).Truncate(time.Second).String()
	}
	if tracker.options.ClockSkewStaleMS > 0 && absolute(tracker.skewMS) >= tracker.options.ClockSkewStaleMS {
		return manoochv1.Status_STATUS_STALE, fmt.Sprintf("clock skew %dms", tracker.skewMS)
	}
	if stream.expired {
		// The key reached its TTL and no fallback picked it up. Whatever a
		// consumer holds is older than the venue's own cadence allows.
		return manoochv1.Status_STATUS_STALE, "key expired"
	}

	// --- DEGRADED: usable, but you should know.
	if !stream.fallbackSince.IsZero() {
		return manoochv1.Status_STATUS_DEGRADED, "rest fallback"
	}
	if socket != nil && socket.state != SocketConnected {
		return manoochv1.Status_STATUS_DEGRADED, socket.reason
	}
	if tracker.options.ClockSkewDegradedMS > 0 && absolute(tracker.skewMS) >= tracker.options.ClockSkewDegradedMS {
		return manoochv1.Status_STATUS_DEGRADED, fmt.Sprintf("clock skew %dms", tracker.skewMS)
	}
	if stream.rejected {
		return manoochv1.Status_STATUS_DEGRADED, "frame rejected"
	}
	return manoochv1.Status_STATUS_HEALTHY, ""
}

// computeVenue derives the connection-level status. Callers hold the mutex.
func (tracker *Tracker) computeVenue() (manoochv1.Status, string) {
	if !tracker.metadataOK {
		return manoochv1.Status_STATUS_STALE, tracker.metadataReason
	}
	ids := tracker.socketIDs()
	for _, id := range ids {
		if tracker.sockets[id].state == SocketCircuitOpen {
			return manoochv1.Status_STATUS_STALE, "circuit open: " + id
		}
	}
	if tracker.options.ClockSkewStaleMS > 0 && absolute(tracker.skewMS) >= tracker.options.ClockSkewStaleMS {
		return manoochv1.Status_STATUS_STALE, fmt.Sprintf("clock skew %dms", tracker.skewMS)
	}
	if tracker.leaked > 0 {
		return manoochv1.Status_STATUS_DEGRADED, fmt.Sprintf("leaked goroutines: %d", tracker.leaked)
	}
	for _, id := range ids {
		if socket := tracker.sockets[id]; socket.state != SocketConnected {
			return manoochv1.Status_STATUS_DEGRADED, fmt.Sprintf("socket %s %s: %s", id, socket.state, socket.reason)
		}
	}
	if tracker.options.ClockSkewDegradedMS > 0 && absolute(tracker.skewMS) >= tracker.options.ClockSkewDegradedMS {
		return manoochv1.Status_STATUS_DEGRADED, fmt.Sprintf("clock skew %dms", tracker.skewMS)
	}
	return manoochv1.Status_STATUS_HEALTHY, ""
}

// worst folds an instrument's channels into the one status its health key
// carries: the worst of them, named by the channel it came from. Callers hold
// the mutex.
func (instrument *instrument) worst() (manoochv1.Status, string) {
	status, reason := manoochv1.Status_STATUS_HEALTHY, ""
	for _, stream := range instrument.streams {
		if stream.status > status {
			status, reason = stream.status, core.ChannelName(stream.specification.Channel)+": "+stream.reason
		}
	}
	return status, reason
}

// instrumentsOn is every instrument with a stream on one socket. Callers hold
// the mutex.
func (tracker *Tracker) instrumentsOn(socketID string) []*instrument {
	var out []*instrument
	for _, instrument := range tracker.order {
		for _, stream := range instrument.streams {
			if stream.socketID == socketID {
				out = append(out, instrument)
				break
			}
		}
	}
	return out
}

// socketIDs is the socket set in registration order, so a status reason names
// the same socket run after run rather than whichever the map yielded first.
func (tracker *Tracker) socketIDs() []string {
	seen := map[string]bool{}
	var out []string
	for _, instrument := range tracker.order {
		for _, stream := range instrument.streams {
			if stream.socketID != "" && !seen[stream.socketID] {
				seen[stream.socketID] = true
				out = append(out, stream.socketID)
			}
		}
	}
	return out
}

// setFallbackMetric records whether a stream is on REST. Callers hold the
// mutex.
func (tracker *Tracker) setFallbackMetric(stream *stream, v float64) {
	if tracker.options.Metrics == nil {
		return
	}
	tracker.options.Metrics.FallbackActive.WithLabelValues(
		tracker.options.Venue, core.MarketTypeName(stream.specification.Instrument.MarketType),
		stream.specification.Instrument.Canonical(), core.ChannelName(stream.specification.Channel)).Set(v)
}

func absolute(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// ---------- transitions ----------

// update applies one state change under the mutex, recomputes the status of
// every stream it touched, and publishes whatever moved.
//
// mutate returns the instruments whose streams may have moved; returning nil
// means the change was venue-level, which is recomputed on every call anyway.
//
// Publishing happens outside the mutex and on the caller's goroutine, because a
// transition a consumer learns about one heartbeat late is a transition they
// traded through. There are a handful an hour, so the Redis round trip is not
// on any hot path.
func (tracker *Tracker) update(mutate func() []*instrument) {
	tracker.mutex.Lock()
	moved := tracker.refresh(mutate())
	tracker.mutex.Unlock()

	// Background rather than a request context: these are called from whatever
	// goroutine noticed the change, including one that is shutting down, and
	// the last transition before an exit is the one worth having. The Redis
	// client's own write timeout bounds it.
	tracker.write(context.Background(), moved)
}

// refresh recomputes status for the given instruments and returns a snapshot
// for every health key whose contents changed. Callers hold the mutex.
//
// Passing t.order refreshes everything, which the heartbeat does: some
// transitions are purely the passage of time — fallback crossing its maximum
// duration is the one that matters — and no event fires for those.
func (tracker *Tracker) refresh(touched []*instrument) []snapshot {
	// A venue-level change moves every stream, so it is recomputed on every
	// update rather than only when an event says to.
	prevStatus, prevReason := tracker.venueStatus, tracker.venueReason
	tracker.venueStatus, tracker.venueReason = tracker.computeVenue()

	var moved []snapshot
	for _, instrument := range touched {
		for _, stream := range instrument.streams {
			status, reason := tracker.compute(stream)
			if status == stream.status && reason == stream.reason {
				continue
			}
			prev := stream.status
			stream.status, stream.reason = status, reason
			tracker.exportStatus(stream)
			tracker.logTransition(stream, prev)
		}
		status, reason := instrument.worst()
		if status == instrument.status && reason == instrument.reason {
			continue
		}
		instrument.status, instrument.reason = status, reason
		moved = append(moved, tracker.snapshotInstrument(instrument))
	}

	if tracker.venueStatus != prevStatus || tracker.venueReason != prevReason {
		if prevStatus != manoochv1.Status_STATUS_UNSPECIFIED {
			tracker.options.Log.Info("venue status",
				"from", core.StatusName(prevStatus),
				"to", core.StatusName(tracker.venueStatus),
				"reason", tracker.venueReason)
		}
		moved = append(moved, tracker.snapshotVenue())
	}
	return moved
}

// exportStatus writes the stream status gauge. Callers hold the mutex.
func (tracker *Tracker) exportStatus(stream *stream) {
	if tracker.options.Metrics == nil {
		return
	}
	v := observability.StreamStatusHealthy
	switch stream.status {
	case manoochv1.Status_STATUS_DEGRADED:
		v = observability.StreamStatusDegraded
	case manoochv1.Status_STATUS_STALE:
		v = observability.StreamStatusStale
	}
	tracker.options.Metrics.StreamStatus.WithLabelValues(
		tracker.options.Venue, core.MarketTypeName(stream.specification.Instrument.MarketType),
		stream.specification.Instrument.Canonical(), core.ChannelName(stream.specification.Channel)).Set(float64(v))
}

// logTransition writes one line per status change. Callers hold the mutex.
//
// Transitions are the one thing in the data path that is worth a log line:
// there are a handful an hour, against six hundred messages a second, and they
// are what an operator reconstructs an incident from.
func (tracker *Tracker) logTransition(stream *stream, prev manoochv1.Status) {
	if prev == manoochv1.Status_STATUS_UNSPECIFIED && stream.status == manoochv1.Status_STATUS_DEGRADED {
		return // the startup state; the socket log line already says this
	}
	arguments := []any{
		"stream", stream.specification.String(),
		"from", core.StatusName(prev),
		"to", core.StatusName(stream.status),
		"reason", stream.reason,
		"source", core.SourceName(stream.source),
	}
	if stream.status == manoochv1.Status_STATUS_STALE {
		tracker.options.Log.Error("stream status", arguments...)
		return
	}
	tracker.options.Log.Info("stream status", arguments...)
}
