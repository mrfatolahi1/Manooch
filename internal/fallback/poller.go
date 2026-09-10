package fallback

import (
	"context"
	"sync"
	"time"

	"github.com/you/manooch/gen/manoochv1"
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
	watcher       *Watcher
	specification core.StreamSpec
	stop          chan struct{}
	done          chan struct{}
	once          sync.Once
}

// engage starts polling a stream, or marks it STALE if it cannot.
//
// Scope is per channel and surgical: only the expired key is polled. The
// endpoint answers all three channels, but the other two are still fresh from
// the socket, and republishing them from REST would reset their TTL — the one
// signal saying they are fine — on the strength of a poll nobody asked for.
func (watcher *Watcher) engage(ctx context.Context, specification core.StreamSpec) {
	watcher.mutex.Lock()
	if _, on := watcher.active[specification]; on {
		watcher.mutex.Unlock()
		return
	}
	if len(watcher.active) >= watcher.options.MaxConcurrentPolls {
		watcher.mutex.Unlock()
		// Past the cap the stream goes STALE rather than into a queue. A
		// queued poll is a value that arrives after it stopped being worth
		// having, published as though it were current.
		watcher.count(specification, resultCapacity)
		watcher.options.Health.FallbackFailed(specification, reasonCapacity)
		return
	}
	poller := &poller{watcher: watcher, specification: specification, stop: make(chan struct{}), done: make(chan struct{})}
	watcher.active[specification] = poller
	watcher.mutex.Unlock()

	watcher.options.Health.FallbackEngaged(specification)
	watcher.options.Log.Warn("rest fallback engaged", "stream", specification.String())
	go poller.run(ctx)
}

// Note records that a websocket message arrived for a stream, which is the only
// thing that ends fallback. It is called on the publish path for every message,
// so it does nothing at all in the ordinary case.
func (watcher *Watcher) Note(specification core.StreamSpec) {
	watcher.mutex.Lock()
	poller := watcher.active[specification]
	if poller != nil {
		delete(watcher.active, specification)
	}
	hadExpiry := watcher.expired[specification]
	delete(watcher.expired, specification)
	watcher.mutex.Unlock()

	if poller == nil {
		if hadExpiry {
			watcher.options.Health.FallbackDisengaged(specification)
		}
		return
	}

	// Signalled, not waited for. This runs on the publish path of a stream
	// that is working again, and a poll parked in an HTTP call to a venue
	// having a bad day would otherwise stall the socket behind it.
	poller.signal()
	watcher.options.Health.FallbackDisengaged(specification)
	watcher.setActiveMetric(specification, 0)
	watcher.options.Log.Info("rest fallback disengaged", "stream", specification.String())
}

// Active is how many streams are currently being served over REST.
func (watcher *Watcher) Active() int {
	watcher.mutex.Lock()
	defer watcher.mutex.Unlock()
	return len(watcher.active)
}

// stop ends every poller, on the way out.
func (watcher *Watcher) stop() {
	watcher.mutex.Lock()
	pollers := make([]*poller, 0, len(watcher.active))
	for specification, poller := range watcher.active {
		pollers = append(pollers, poller)
		delete(watcher.active, specification)
	}
	watcher.mutex.Unlock()

	for _, poller := range pollers {
		poller.halt()
	}
}

// run polls until stopped. The first poll is immediate: waiting a full interval
// would leave the key absent for that long having already noticed it was gone.
func (poller *poller) run(ctx context.Context) {
	defer close(poller.done)

	tick := time.NewTicker(poller.watcher.options.PollInterval)
	defer tick.Stop()

	poller.poll(ctx)
	for {
		select {
		case <-ctx.Done():
			return
		case <-poller.stop:
			return
		case <-tick.C:
			poller.poll(ctx)
		}
	}
}

// signal tells the poller to stop, without waiting for it.
func (poller *poller) signal() {
	poller.once.Do(func() { close(poller.stop) })
}

// halt stops the poller and waits for its goroutine, for shutdown.
func (poller *poller) halt() {
	poller.signal()
	<-poller.done
}

// poll fetches one value and publishes it.
//
// Every failure path ends in STALE. A fallback that quietly skips a poll is
// precisely the failure this service exists to prevent: the key stays absent,
// the consumer sees nothing, and nothing anywhere says why.
func (poller *poller) poll(ctx context.Context) {
	watcher := poller.watcher
	specification := poller.specification

	messages, err := watcher.options.Adapter.FetchOnce(ctx, specification)
	if err != nil {
		if ctx.Err() != nil {
			return
		}
		watcher.count(specification, resultError)
		watcher.options.Health.FallbackFailed(specification, reasonPoll)
		watcher.logError("rest fallback poll failed", err)
		return
	}

	published := false
	for _, message := range messages {
		// The adapter may answer more than was asked for; only the expired
		// channel is republished.
		if message.Channel != specification.Channel {
			continue
		}
		if !poller.publish(ctx, message) {
			return
		}
		published = true
	}

	if !published {
		// The venue answered, but not with this value. That is missing data,
		// not a zero, and it must not read as a working fallback.
		watcher.count(specification, resultEmpty)
		watcher.options.Health.FallbackFailed(specification, reasonEmpty)
	}
}

// publish writes one polled message, reporting whether it landed.
func (poller *poller) publish(ctx context.Context, message core.Message) bool {
	watcher := poller.watcher

	// A poll that was already in flight when the socket recovered must not
	// land: it would put SOURCE_REST and DEGRADED back onto a stream that is
	// healthy again, and reset the TTL from a source nobody is using.
	if !watcher.owns(poller) {
		return false
	}

	enveloped, ok := message.Proto.(interface{ GetEnv() *manoochv1.Envelope })
	if !ok || enveloped.GetEnv() == nil {
		watcher.count(poller.specification, resultError)
		watcher.options.Health.FallbackFailed(poller.specification, reasonEmpty)
		return false
	}

	// The value is current again, which clears an earlier failure but does not
	// end fallback: only a websocket message does that.
	watcher.options.Health.Polled(poller.specification)

	envelope := enveloped.GetEnv()
	// Same channel, same key, so a consumer has one code path. What differs is
	// visible to anyone who looks: the source says REST and the status says
	// this is not the socket.
	envelope.Source = manoochv1.Source_SOURCE_REST
	envelope.Status, envelope.StatusReason = watcher.options.Health.Status(poller.specification)

	if err := watcher.options.Publisher.Publish(ctx, message.Key, message.Proto, message.TimeToLive); err != nil {
		if ctx.Err() != nil {
			return false
		}
		watcher.count(poller.specification, resultError)
		watcher.options.Health.FallbackFailed(poller.specification, reasonPoll)
		return false
	}

	watcher.count(poller.specification, resultOK)
	watcher.setActiveMetric(poller.specification, 1)
	return true
}

// owns reports whether a poller is still the one serving its stream.
func (watcher *Watcher) owns(poller *poller) bool {
	watcher.mutex.Lock()
	defer watcher.mutex.Unlock()
	return watcher.active[poller.specification] == poller
}

// count records one poll result.
func (watcher *Watcher) count(specification core.StreamSpec, result string) {
	watcher.options.Metrics.FallbackPolls.WithLabelValues(watcher.options.Venue, core.ChannelName(specification.Channel), result).Inc()
}

// setActiveMetric records whether a stream is on REST.
func (watcher *Watcher) setActiveMetric(specification core.StreamSpec, v float64) {
	watcher.options.Metrics.FallbackActive.WithLabelValues(
		watcher.options.Venue,
		core.MarketTypeName(specification.Instrument.MarketType),
		specification.Instrument.Canonical(),
		core.ChannelName(specification.Channel)).Set(v)
}
