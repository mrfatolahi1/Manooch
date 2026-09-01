package fallback

import (
	"context"
	"sync"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
)

// Poll results, the "result" label on manooch_fallback_polls_total. A closed
// set rather than free text, so the metric can be summed.
const (
	resultOK       = "ok"
	resultError    = "error"
	resultEmpty    = "empty"
	resultCapacity = "capacity"
)

// Reasons a stream on fallback is STALE rather than DEGRADED. They reach a
// consumer as status_reason, so they are worded for someone reading a table.
const (
	reasonCapacity = "fallback at capacity"
	reasonPoll     = "rest poll failed"
	reasonEmpty    = "rest returned no value"
)

// A poller serves one stream over REST until it is stopped.
type poller struct {
	w    *Watcher
	spec core.StreamSpec
	stop chan struct{}
	done chan struct{}
	once sync.Once
}

// engage starts polling a stream, or marks it STALE if it cannot.
//
// Scope is per channel and surgical: only the expired key is polled. The
// endpoint answers all three channels, but the other two are still fresh from
// the socket, and republishing them from REST would reset their TTL — the one
// signal saying they are fine — on the strength of a poll nobody asked for.
func (w *Watcher) engage(ctx context.Context, spec core.StreamSpec) {
	w.mu.Lock()
	if _, on := w.active[spec]; on {
		w.mu.Unlock()
		return
	}
	if len(w.active) >= w.opts.MaxConcurrentPolls {
		w.mu.Unlock()
		// Past the cap the stream goes STALE rather than into a queue. A
		// queued poll is a value that arrives after it stopped being worth
		// having, published as though it were current.
		w.count(spec, resultCapacity)
		w.opts.Health.FallbackFailed(spec, reasonCapacity)
		return
	}
	p := &poller{w: w, spec: spec, stop: make(chan struct{}), done: make(chan struct{})}
	w.active[spec] = p
	w.mu.Unlock()

	w.opts.Health.FallbackEngaged(spec)
	w.opts.Log.Warn("rest fallback engaged", "stream", spec.String())
	go p.run(ctx)
}

// Note records that a websocket message arrived for a stream, which is the only
// thing that ends fallback. It is called on the publish path for every message,
// so it does nothing at all in the ordinary case.
func (w *Watcher) Note(spec core.StreamSpec) {
	w.mu.Lock()
	p := w.active[spec]
	if p != nil {
		delete(w.active, spec)
	}
	hadExpiry := w.expired[spec]
	delete(w.expired, spec)
	w.mu.Unlock()

	if p == nil {
		if hadExpiry {
			w.opts.Health.FallbackDisengaged(spec)
		}
		return
	}

	// Signalled, not waited for. This runs on the publish path of a stream
	// that is working again, and a poll parked in an HTTP call to a venue
	// having a bad day would otherwise stall the socket behind it.
	p.signal()
	w.opts.Health.FallbackDisengaged(spec)
	w.setActiveMetric(spec, 0)
	w.opts.Log.Info("rest fallback disengaged", "stream", spec.String())
}

// Active is how many streams are currently being served over REST.
func (w *Watcher) Active() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.active)
}

// stop ends every poller, on the way out.
func (w *Watcher) stop() {
	w.mu.Lock()
	pollers := make([]*poller, 0, len(w.active))
	for spec, p := range w.active {
		pollers = append(pollers, p)
		delete(w.active, spec)
	}
	w.mu.Unlock()

	for _, p := range pollers {
		p.halt()
	}
}

// run polls until stopped. The first poll is immediate: waiting a full interval
// would leave the key absent for that long having already noticed it was gone.
func (p *poller) run(ctx context.Context) {
	defer close(p.done)

	tick := time.NewTicker(p.w.opts.PollInterval)
	defer tick.Stop()

	p.poll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-p.stop:
			return
		case <-tick.C:
			p.poll(ctx)
		}
	}
}

// signal tells the poller to stop, without waiting for it.
func (p *poller) signal() {
	p.once.Do(func() { close(p.stop) })
}

// halt stops the poller and waits for its goroutine, for shutdown.
func (p *poller) halt() {
	p.signal()
	<-p.done
}

// poll fetches one value and publishes it.
//
// Every failure path ends in STALE. A fallback that quietly skips a poll is
// precisely the failure this service exists to prevent: the key stays absent,
// the consumer sees nothing, and nothing anywhere says why.
func (p *poller) poll(ctx context.Context) {
	w := p.w
	spec := p.spec

	msgs, err := w.opts.Adapter.FetchOnce(ctx, spec)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		w.count(spec, resultError)
		w.opts.Health.FallbackFailed(spec, reasonPoll)
		w.logError("rest fallback poll failed", err)
		return
	}

	published := false
	for _, m := range msgs {
		// The adapter may answer more than was asked for; only the expired
		// channel is republished.
		if m.Channel != spec.Channel {
			continue
		}
		if !p.publish(ctx, m) {
			return
		}
		published = true
	}

	if !published {
		// The venue answered, but not with this value. That is missing data,
		// not a zero, and it must not read as a working fallback.
		w.count(spec, resultEmpty)
		w.opts.Health.FallbackFailed(spec, reasonEmpty)
	}
}

// publish writes one polled message, reporting whether it landed.
func (p *poller) publish(ctx context.Context, m core.Message) bool {
	w := p.w

	// A poll that was already in flight when the socket recovered must not
	// land: it would put SOURCE_REST and DEGRADED back onto a stream that is
	// healthy again, and reset the TTL from a source nobody is using.
	if !w.owns(p) {
		return false
	}

	env, ok := m.Proto.(interface{ GetEnv() *pb.Envelope })
	if !ok || env.GetEnv() == nil {
		w.count(p.spec, resultError)
		w.opts.Health.FallbackFailed(p.spec, reasonEmpty)
		return false
	}

	// The value is current again, which clears an earlier failure but does not
	// end fallback: only a websocket message does that.
	w.opts.Health.Polled(p.spec)

	e := env.GetEnv()
	// Same channel, same key, so a consumer has one code path. What differs is
	// visible to anyone who looks: the source says REST and the status says
	// this is not the socket.
	e.Source = pb.Source_SOURCE_REST
	e.Status, e.StatusReason = w.opts.Health.Status(p.spec)

	if err := w.opts.Publisher.Publish(ctx, m.Key, m.Proto, m.TTL); err != nil {
		if ctx.Err() != nil {
			return false
		}
		w.count(p.spec, resultError)
		w.opts.Health.FallbackFailed(p.spec, reasonPoll)
		return false
	}

	w.count(p.spec, resultOK)
	w.setActiveMetric(p.spec, 1)
	return true
}

// owns reports whether a poller is still the one serving its stream.
func (w *Watcher) owns(p *poller) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.active[p.spec] == p
}

// count records one poll result.
func (w *Watcher) count(spec core.StreamSpec, result string) {
	w.opts.Metrics.FallbackPolls.WithLabelValues(w.opts.Venue, core.ChannelName(spec.Channel), result).Inc()
}

// setActiveMetric records whether a stream is on REST.
func (w *Watcher) setActiveMetric(spec core.StreamSpec, v float64) {
	w.opts.Metrics.FallbackActive.WithLabelValues(
		w.opts.Venue,
		core.MarketTypeName(spec.Instrument.MarketType),
		spec.Instrument.Canonical(),
		core.ChannelName(spec.Channel)).Set(v)
}
