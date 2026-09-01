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

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/obs"
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

	Metrics *obs.Metrics
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
	opts Options
	now  func() time.Time

	mu      sync.Mutex
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

	venueStatus pb.Status
	venueReason string
}

// A stream is one (instrument, channel): exactly one Redis key.
type stream struct {
	spec     core.StreamSpec
	inst     *instrument
	socketID string

	lastMessage time.Time
	source      pb.Source
	restarts    uint32

	// expired is set when the key reached its TTL and cleared by the next
	// message. It is not a timestamp comparison: Redis told us.
	expired bool
	// rejected is set when a frame on this stream's socket could not be
	// parsed, and cleared by the next message that could.
	rejected bool

	fallbackSince time.Time
	fallbackFail  string

	status pb.Status
	reason string
}

// An instrument groups the channels that share one health key.
type instrument struct {
	ref         core.InstrumentRef
	venueSymbol string
	key         string
	streams     []*stream

	status pb.Status
	reason string
}

type socket struct {
	id     string
	state  string
	reason string
}

// New builds a tracker. It publishes nothing until Run or an event says so.
func New(opts Options) (*Tracker, error) {
	if opts.Venue == "" {
		return nil, fmt.Errorf("health: no venue")
	}
	if opts.Publisher == nil {
		return nil, fmt.Errorf("health: no publisher")
	}
	if opts.Log == nil {
		return nil, fmt.Errorf("health: no logger")
	}
	if opts.HeartbeatInterval <= 0 {
		return nil, fmt.Errorf("health: heartbeat interval is %v", opts.HeartbeatInterval)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Tracker{
		opts:           opts,
		now:            opts.Now,
		streams:        map[core.StreamSpec]*stream{},
		sockets:        map[string]*socket{},
		metadataOK:     !opts.MetadataRequired,
		metadataReason: "metadata unavailable",
		venueStatus:    pb.Status_STATUS_UNSPECIFIED,
	}, nil
}

// Register declares a stream before anything reports on it. socketID names the
// connection that carries it, so a socket-level event reaches the right
// streams. An unregistered spec is ignored everywhere else: a stream nobody
// declared has no key, and inventing one would publish a status for a stream
// that does not exist.
func (t *Tracker) Register(spec core.StreamSpec, venueSymbol, socketID string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	if _, dup := t.streams[spec]; dup {
		return
	}
	inst := t.instrumentFor(spec.Instrument, venueSymbol)
	s := &stream{
		spec:     spec,
		inst:     inst,
		socketID: socketID,
		source:   pb.Source_SOURCE_WEBSOCKET,
		status:   pb.Status_STATUS_UNSPECIFIED,
	}
	inst.streams = append(inst.streams, s)
	t.streams[spec] = s

	if _, ok := t.sockets[socketID]; !ok && socketID != "" {
		t.sockets[socketID] = &socket{id: socketID, state: SocketDialing, reason: "not yet connected"}
	}
	// Seeded now rather than on the first event, so a stream that never
	// receives anything still has a status to publish.
	t.refresh([]*instrument{inst})
}

// instrumentFor finds or creates the instrument a stream belongs to. Callers
// hold the mutex.
func (t *Tracker) instrumentFor(ref core.InstrumentRef, venueSymbol string) *instrument {
	for _, in := range t.order {
		if in.ref == ref {
			return in
		}
	}
	in := &instrument{
		ref:         ref,
		venueSymbol: venueSymbol,
		key:         publish.Key(t.opts.Venue, ref.MarketType, ref.Canonical(), pb.Channel_CHANNEL_HEALTH),
		status:      pb.Status_STATUS_UNSPECIFIED,
	}
	t.order = append(t.order, in)
	return in
}

// Specs is every registered stream, in registration order. The fallback watcher
// needs the set to sweep.
func (t *Tracker) Specs() []core.StreamSpec {
	t.mu.Lock()
	defer t.mu.Unlock()

	out := make([]core.StreamSpec, 0, len(t.streams))
	for _, in := range t.order {
		for _, s := range in.streams {
			out = append(out, s.spec)
		}
	}
	return out
}

// ---------- events ----------

// Received records a websocket message for a stream. It is called before the
// message is stamped and published, so the status that goes onto the wire
// already reflects the arrival rather than the state it recovered from.
func (t *Tracker) Received(spec core.StreamSpec) {
	t.update(func() []*instrument {
		s := t.streams[spec]
		if s == nil {
			return nil
		}
		s.lastMessage = t.now()
		s.source = pb.Source_SOURCE_WEBSOCKET
		s.expired = false
		s.rejected = false
		return []*instrument{s.inst}
	})
}

// Polled records a successful REST fallback poll. The stream stays on fallback
// — only a websocket message ends that — but the value is current again, so a
// previous poll failure is cleared.
func (t *Tracker) Polled(spec core.StreamSpec) {
	t.update(func() []*instrument {
		s := t.streams[spec]
		if s == nil {
			return nil
		}
		s.lastMessage = t.now()
		s.source = pb.Source_SOURCE_REST
		s.expired = false
		s.fallbackFail = ""
		return []*instrument{s.inst}
	})
}

// KeyExpired records that a stream's Redis key reached its TTL.
func (t *Tracker) KeyExpired(spec core.StreamSpec) {
	t.update(func() []*instrument {
		s := t.streams[spec]
		if s == nil {
			return nil
		}
		if t.opts.Metrics != nil && !s.expired {
			t.opts.Metrics.KeyExpired.WithLabelValues(t.opts.Venue, core.ChannelName(spec.Channel)).Inc()
		}
		s.expired = true
		return []*instrument{s.inst}
	})
}

// FrameRejected records a frame on a socket that could not be parsed. It marks
// every stream the socket carries, because a frame that did not parse names no
// channel: attributing it to none of them would leave a venue sending shapes we
// cannot read looking perfectly healthy for as long as the keys stay inside
// their TTL.
func (t *Tracker) FrameRejected(socketID string) {
	t.update(func() []*instrument {
		var touched []*instrument
		for _, in := range t.order {
			marked := false
			for _, s := range in.streams {
				if s.socketID == socketID && !s.rejected {
					s.rejected = true
					marked = true
				}
			}
			if marked {
				touched = append(touched, in)
			}
		}
		return touched
	})
}

// StreamRestarted counts a tier-1 restart of one stream's goroutine.
func (t *Tracker) StreamRestarted(spec core.StreamSpec) {
	t.update(func() []*instrument {
		s := t.streams[spec]
		if s == nil {
			return nil
		}
		s.restarts++
		if t.opts.Metrics != nil {
			t.opts.Metrics.StreamRestarts.WithLabelValues(
				t.opts.Venue, core.MarketTypeName(s.spec.Instrument.MarketType),
				s.spec.Instrument.Canonical(), core.ChannelName(s.spec.Channel)).Inc()
		}
		return []*instrument{s.inst}
	})
}

// FallbackEngaged records that a stream is now served by REST polling.
func (t *Tracker) FallbackEngaged(spec core.StreamSpec) {
	t.update(func() []*instrument {
		s := t.streams[spec]
		if s == nil || !s.fallbackSince.IsZero() {
			return nil
		}
		s.fallbackSince = t.now()
		s.source = pb.Source_SOURCE_REST
		t.setFallbackMetric(s, 1)
		return []*instrument{s.inst}
	})
}

// FallbackFailed records that fallback could not serve a stream — the poll
// errored, the venue answered nothing usable, or the concurrency cap was
// reached. The stream goes STALE. A fallback that quietly stops is the failure
// this whole service exists to prevent.
func (t *Tracker) FallbackFailed(spec core.StreamSpec, reason string) {
	t.update(func() []*instrument {
		s := t.streams[spec]
		if s == nil || s.fallbackFail == reason {
			return nil
		}
		s.fallbackFail = reason
		return []*instrument{s.inst}
	})
}

// FallbackDisengaged records that a stream is back on its socket.
func (t *Tracker) FallbackDisengaged(spec core.StreamSpec) {
	t.update(func() []*instrument {
		s := t.streams[spec]
		if s == nil || (s.fallbackSince.IsZero() && s.fallbackFail == "") {
			return nil
		}
		s.fallbackSince = time.Time{}
		s.fallbackFail = ""
		s.source = pb.Source_SOURCE_WEBSOCKET
		t.setFallbackMetric(s, 0)
		return []*instrument{s.inst}
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
func (t *Tracker) MetadataState(ok bool, reason string) {
	if !t.opts.MetadataRequired {
		return
	}
	t.update(func() []*instrument {
		if t.metadataOK == ok && (ok || t.metadataReason == reason) {
			return nil
		}
		t.metadataOK = ok
		if !ok {
			t.metadataReason = reason
		}
		return t.order
	})
}

// SocketState records what one connection is doing. state is one of
// SocketConnected, SocketDialing or SocketCircuitOpen.
func (t *Tracker) SocketState(socketID, state, reason string) {
	t.update(func() []*instrument {
		sock := t.sockets[socketID]
		if sock == nil {
			sock = &socket{id: socketID}
			t.sockets[socketID] = sock
		}
		if sock.state == state && sock.reason == reason {
			return nil
		}
		sock.state, sock.reason = state, reason
		return t.instrumentsOn(socketID)
	})
}

// Reconnected counts one completed reconnection of a socket.
func (t *Tracker) Reconnected(socketID string) {
	t.update(func() []*instrument {
		t.reconnects++
		if t.opts.Metrics != nil {
			t.opts.Metrics.Reconnects.WithLabelValues(t.opts.Venue, socketID).Inc()
		}
		return nil
	})
}

// ClockSkew records the gap between the venue's clock and ours, in
// milliseconds, signed: a venue clock ahead of ours reads positive. The sign is
// kept because losing it hides which way the two disagree, and every freshness
// number depends on the answer.
func (t *Tracker) ClockSkew(ms int64) {
	t.update(func() []*instrument {
		if t.skewMS == ms {
			return nil
		}
		t.skewMS = ms
		if t.opts.Metrics != nil {
			t.opts.Metrics.ClockSkewMS.WithLabelValues(t.opts.Venue).Set(float64(ms))
		}
		return t.order
	})
}

// Leaked records how many goroutines failed to return within the leak timeout.
//
// There is no self-kill, so leaks accumulate; making them visible is the price
// of never restarting the process. Any value above zero holds the venue at
// DEGRADED until an operator does something about it.
func (t *Tracker) Leaked(n int) {
	t.update(func() []*instrument {
		if n == t.leaked {
			return nil
		}
		if n > t.leaked {
			t.opts.Log.Error("goroutines leaked", "count", n)
		}
		t.leaked = n
		if t.opts.Metrics != nil {
			t.opts.Metrics.LeakedGoroutines.WithLabelValues(t.opts.Venue).Set(float64(n))
		}
		return nil
	})
}

// ---------- status ----------

// Status is a stream's current status and, when it is not healthy, why. It is
// what a producer stamps into the envelope immediately before publishing.
func (t *Tracker) Status(spec core.StreamSpec) (pb.Status, string) {
	t.mu.Lock()
	defer t.mu.Unlock()

	s := t.streams[spec]
	if s == nil {
		// A stream nobody registered has no state to judge. STALE is the only
		// safe answer: the alternative publishes data as healthy on the
		// strength of knowing nothing about it.
		return pb.Status_STATUS_STALE, "stream not registered"
	}
	return t.compute(s)
}

// VenueStatus is the connection-level status: socket state, clock skew and
// leaked goroutines, none of which belong to any one stream.
func (t *Tracker) VenueStatus() (pb.Status, string) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.computeVenue()
}

// compute derives a stream's status from its state. Callers hold the mutex.
//
// STALE is tested before DEGRADED throughout: the two differ by whether a
// consumer may trade on the data, so a stream that qualifies for both must
// report the one that says stop.
func (t *Tracker) compute(s *stream) (pb.Status, string) {
	sock := t.sockets[s.socketID]

	// --- STALE: do not trade this venue.
	if !t.metadataOK {
		// First, and above everything else: without metadata there is no
		// price being published at all, so no other reason has happened yet.
		return pb.Status_STATUS_STALE, t.metadataReason
	}
	if sock != nil && sock.state == SocketCircuitOpen {
		return pb.Status_STATUS_STALE, "circuit open"
	}
	if s.fallbackFail != "" {
		return pb.Status_STATUS_STALE, s.fallbackFail
	}
	if !s.fallbackSince.IsZero() && t.opts.FallbackMaxDuration > 0 &&
		t.now().Sub(s.fallbackSince) >= t.opts.FallbackMaxDuration {
		// Long-running fallback is a failure, not a steady state.
		return pb.Status_STATUS_STALE, "rest fallback for " + t.now().Sub(s.fallbackSince).Truncate(time.Second).String()
	}
	if t.opts.ClockSkewStaleMS > 0 && abs(t.skewMS) >= t.opts.ClockSkewStaleMS {
		return pb.Status_STATUS_STALE, fmt.Sprintf("clock skew %dms", t.skewMS)
	}
	if s.expired {
		// The key reached its TTL and no fallback picked it up. Whatever a
		// consumer holds is older than the venue's own cadence allows.
		return pb.Status_STATUS_STALE, "key expired"
	}

	// --- DEGRADED: usable, but you should know.
	if !s.fallbackSince.IsZero() {
		return pb.Status_STATUS_DEGRADED, "rest fallback"
	}
	if sock != nil && sock.state != SocketConnected {
		return pb.Status_STATUS_DEGRADED, sock.reason
	}
	if t.opts.ClockSkewDegradedMS > 0 && abs(t.skewMS) >= t.opts.ClockSkewDegradedMS {
		return pb.Status_STATUS_DEGRADED, fmt.Sprintf("clock skew %dms", t.skewMS)
	}
	if s.rejected {
		return pb.Status_STATUS_DEGRADED, "frame rejected"
	}
	return pb.Status_STATUS_HEALTHY, ""
}

// computeVenue derives the connection-level status. Callers hold the mutex.
func (t *Tracker) computeVenue() (pb.Status, string) {
	if !t.metadataOK {
		return pb.Status_STATUS_STALE, t.metadataReason
	}
	ids := t.socketIDs()
	for _, id := range ids {
		if t.sockets[id].state == SocketCircuitOpen {
			return pb.Status_STATUS_STALE, "circuit open: " + id
		}
	}
	if t.opts.ClockSkewStaleMS > 0 && abs(t.skewMS) >= t.opts.ClockSkewStaleMS {
		return pb.Status_STATUS_STALE, fmt.Sprintf("clock skew %dms", t.skewMS)
	}
	if t.leaked > 0 {
		return pb.Status_STATUS_DEGRADED, fmt.Sprintf("leaked goroutines: %d", t.leaked)
	}
	for _, id := range ids {
		if sock := t.sockets[id]; sock.state != SocketConnected {
			return pb.Status_STATUS_DEGRADED, fmt.Sprintf("socket %s %s: %s", id, sock.state, sock.reason)
		}
	}
	if t.opts.ClockSkewDegradedMS > 0 && abs(t.skewMS) >= t.opts.ClockSkewDegradedMS {
		return pb.Status_STATUS_DEGRADED, fmt.Sprintf("clock skew %dms", t.skewMS)
	}
	return pb.Status_STATUS_HEALTHY, ""
}

// worst folds an instrument's channels into the one status its health key
// carries: the worst of them, named by the channel it came from. Callers hold
// the mutex.
func (in *instrument) worst() (pb.Status, string) {
	status, reason := pb.Status_STATUS_HEALTHY, ""
	for _, s := range in.streams {
		if s.status > status {
			status, reason = s.status, core.ChannelName(s.spec.Channel)+": "+s.reason
		}
	}
	return status, reason
}

// instrumentsOn is every instrument with a stream on one socket. Callers hold
// the mutex.
func (t *Tracker) instrumentsOn(socketID string) []*instrument {
	var out []*instrument
	for _, in := range t.order {
		for _, s := range in.streams {
			if s.socketID == socketID {
				out = append(out, in)
				break
			}
		}
	}
	return out
}

// socketIDs is the socket set in registration order, so a status reason names
// the same socket run after run rather than whichever the map yielded first.
func (t *Tracker) socketIDs() []string {
	seen := map[string]bool{}
	var out []string
	for _, in := range t.order {
		for _, s := range in.streams {
			if s.socketID != "" && !seen[s.socketID] {
				seen[s.socketID] = true
				out = append(out, s.socketID)
			}
		}
	}
	return out
}

// setFallbackMetric records whether a stream is on REST. Callers hold the mutex.
func (t *Tracker) setFallbackMetric(s *stream, v float64) {
	if t.opts.Metrics == nil {
		return
	}
	t.opts.Metrics.FallbackActive.WithLabelValues(
		t.opts.Venue, core.MarketTypeName(s.spec.Instrument.MarketType),
		s.spec.Instrument.Canonical(), core.ChannelName(s.spec.Channel)).Set(v)
}

func abs(v int64) int64 {
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
func (t *Tracker) update(mutate func() []*instrument) {
	t.mu.Lock()
	moved := t.refresh(mutate())
	t.mu.Unlock()

	// Background rather than a request context: these are called from whatever
	// goroutine noticed the change, including one that is shutting down, and
	// the last transition before an exit is the one worth having. The Redis
	// client's own write timeout bounds it.
	t.write(context.Background(), moved)
}

// refresh recomputes status for the given instruments and returns a snapshot
// for every health key whose contents changed. Callers hold the mutex.
//
// Passing t.order refreshes everything, which the heartbeat does: some
// transitions are purely the passage of time — fallback crossing its maximum
// duration is the one that matters — and no event fires for those.
func (t *Tracker) refresh(touched []*instrument) []snapshot {
	// A venue-level change moves every stream, so it is recomputed on every
	// update rather than only when an event says to.
	prevStatus, prevReason := t.venueStatus, t.venueReason
	t.venueStatus, t.venueReason = t.computeVenue()

	var moved []snapshot
	for _, in := range touched {
		for _, s := range in.streams {
			status, reason := t.compute(s)
			if status == s.status && reason == s.reason {
				continue
			}
			prev := s.status
			s.status, s.reason = status, reason
			t.exportStatus(s)
			t.logTransition(s, prev)
		}
		status, reason := in.worst()
		if status == in.status && reason == in.reason {
			continue
		}
		in.status, in.reason = status, reason
		moved = append(moved, t.snapshotInstrument(in))
	}

	if t.venueStatus != prevStatus || t.venueReason != prevReason {
		if prevStatus != pb.Status_STATUS_UNSPECIFIED {
			t.opts.Log.Info("venue status",
				"from", core.StatusName(prevStatus),
				"to", core.StatusName(t.venueStatus),
				"reason", t.venueReason)
		}
		moved = append(moved, t.snapshotVenue())
	}
	return moved
}

// exportStatus writes the stream status gauge. Callers hold the mutex.
func (t *Tracker) exportStatus(s *stream) {
	if t.opts.Metrics == nil {
		return
	}
	v := obs.StreamStatusHealthy
	switch s.status {
	case pb.Status_STATUS_DEGRADED:
		v = obs.StreamStatusDegraded
	case pb.Status_STATUS_STALE:
		v = obs.StreamStatusStale
	}
	t.opts.Metrics.StreamStatus.WithLabelValues(
		t.opts.Venue, core.MarketTypeName(s.spec.Instrument.MarketType),
		s.spec.Instrument.Canonical(), core.ChannelName(s.spec.Channel)).Set(float64(v))
}

// logTransition writes one line per status change. Callers hold the mutex.
//
// Transitions are the one thing in the data path that is worth a log line:
// there are a handful an hour, against six hundred messages a second, and they
// are what an operator reconstructs an incident from.
func (t *Tracker) logTransition(s *stream, prev pb.Status) {
	if prev == pb.Status_STATUS_UNSPECIFIED && s.status == pb.Status_STATUS_DEGRADED {
		return // the startup state; the socket log line already says this
	}
	args := []any{
		"stream", s.spec.String(),
		"from", core.StatusName(prev),
		"to", core.StatusName(s.status),
		"reason", s.reason,
		"source", core.SourceName(s.source),
	}
	if s.status == pb.Status_STATUS_STALE {
		t.opts.Log.Error("stream status", args...)
		return
	}
	t.opts.Log.Info("stream status", args...)
}
