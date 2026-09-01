// Package metadata keeps the venue's instrument definitions — tick size, lot
// size, minimum notional, contract multiplier — in Redis beside the prices.
//
// It is a startup dependency, not a background nicety. A price with unknown
// precision is a price nobody can size an order against, and a missing contract
// multiplier is a silently wrong order size on every venue that trades in
// contracts rather than base units. Until the first fetch succeeds the venue
// reports STALE and publishes no market data at all.
//
// Refresh is an interval poll and nothing else: no diff events, no on-demand
// trigger. The venue publishes no change feed, so a poll is the only honest
// mechanism, and one that pretends otherwise would be a second answer to
// "when did this change" that eventually disagrees with the first.
package metadata

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/internal/transport"
	"github.com/you/manooch/pkg/price"
)

// ttlMultiple is how many refresh intervals a metadata key survives without
// one. Two, so a single missed cycle is not an outage and two are: the key
// going absent is the only signal that nobody is maintaining it.
const ttlMultiple = 2

// unavailable is the status_reason every stream carries until the first fetch
// lands. It is worded for someone reading a status table, not a stack trace.
const unavailable = "metadata unavailable"

// A Reporter is told whether metadata is available. health.Tracker implements
// it; the interface is here so this package does not have to import the
// tracker to say one thing to it.
type Reporter interface {
	MetadataState(ok bool, reason string)
}

// Options configures a Refresher.
type Options struct {
	// Venue is the canonical upper-case venue name.
	Venue string

	// Adapter reads the venue's public instrument endpoint.
	Adapter core.Adapter

	// Publisher writes the metadata keys.
	Publisher publish.Publisher

	// Health is told when metadata is missing and when it arrives.
	Health Reporter

	Log *slog.Logger

	// Instruments is what to publish: the configured set, not everything the
	// venue lists. A venue's whole contract list is hundreds of keys nobody
	// subscribed to.
	Instruments []core.InstrumentRef

	// MarketType is the market to fetch. One per process, like everything else.
	MarketType pb.MarketType

	// Interval is how often metadata is refetched. The key's TTL is twice it.
	Interval time.Duration

	// FetchTimeout bounds one call to the venue.
	FetchTimeout time.Duration

	// Required holds the venue at STALE until the first fetch succeeds. When
	// false the refresher still runs, but nothing waits for it.
	Required bool

	// Backoff is the wait between failed initial fetches.
	Backoff transport.Policy

	// Now is swappable for tests. Zero means time.Now.
	Now func() time.Time
}

// A Refresher fetches and republishes instrument metadata on an interval.
type Refresher struct {
	opts Options
	now  func() time.Time

	// ready closes once the first fetch has succeeded. It is a channel rather
	// than a flag because the daemon parks on it before it starts streaming.
	ready     chan struct{}
	readyOnce sync.Once

	mu   sync.Mutex
	last map[string]*pb.InstrumentMeta // canonical symbol -> last published
}

// New builds a refresher. It fetches nothing until Run.
func New(opts Options) (*Refresher, error) {
	switch {
	case opts.Venue == "":
		return nil, errors.New("metadata: no venue")
	case opts.Adapter == nil:
		return nil, errors.New("metadata: no adapter")
	case opts.Publisher == nil:
		return nil, errors.New("metadata: no publisher")
	case opts.Log == nil:
		return nil, errors.New("metadata: no logger")
	case len(opts.Instruments) == 0:
		return nil, errors.New("metadata: no instruments")
	case opts.Interval <= 0:
		return nil, fmt.Errorf("metadata: refresh interval is %v", opts.Interval)
	case opts.FetchTimeout <= 0:
		return nil, fmt.Errorf("metadata: fetch timeout is %v", opts.FetchTimeout)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Refresher{
		opts:  opts,
		now:   opts.Now,
		ready: make(chan struct{}),
		last:  map[string]*pb.InstrumentMeta{},
	}, nil
}

// Ready closes once the first fetch has succeeded.
func (r *Refresher) Ready() <-chan struct{} { return r.ready }

// WaitReady blocks until metadata has arrived, reporting false if ctx ended
// first. It returns immediately when metadata is not a startup requirement.
func (r *Refresher) WaitReady(ctx context.Context) bool {
	if !r.opts.Required {
		return ctx.Err() == nil
	}
	select {
	case <-r.ready:
		return true
	case <-ctx.Done():
		return false
	}
}

// Run fetches until the first success, then refreshes on the interval.
//
// The initial fetch retries on backoff and reports STALE throughout, because
// the alternative — starting the streams anyway — publishes prices at unknown
// precision, which is worse than publishing nothing and saying so.
//
// A later failure is not fatal and does not republish what we already have:
// resetting a key's TTL from a fetch that did not happen would claim a
// freshness nobody has. The key expires after two missed cycles and that
// absence is the signal.
func (r *Refresher) Run(ctx context.Context) {
	r.report(false, unavailable)

	for attempt := 0; ctx.Err() == nil; attempt++ {
		if err := r.refresh(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			r.opts.Log.Error("metadata unavailable, the venue is publishing nothing",
				"attempt", attempt+1, "error", err.Error())
			r.report(false, unavailable)
			if !r.opts.Backoff.Sleep(ctx, attempt) {
				return
			}
			continue
		}
		break
	}
	if ctx.Err() != nil {
		return
	}

	r.report(true, "")
	r.readyOnce.Do(func() { close(r.ready) })

	tick := time.NewTicker(r.opts.Interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := r.refresh(ctx); err != nil && ctx.Err() == nil {
				// Held, not escalated: the prices are still arriving and what
				// we published last cycle is still inside its TTL.
				r.opts.Log.Error("metadata refresh failed, keeping the last values until they expire",
					"error", err.Error())
			}
		}
	}
}

// refresh fetches once, logs what moved and republishes everything.
//
// The whole set goes out every cycle rather than only what changed: Pub/Sub is
// fire-and-forget, and a consumer that missed the one message announcing a tick
// size change would never hear about it again.
func (r *Refresher) refresh(ctx context.Context) error {
	fetchCtx, cancel := context.WithTimeout(ctx, r.opts.FetchTimeout)
	defer cancel()

	metas, err := r.opts.Adapter.FetchMetadata(fetchCtx, r.opts.MarketType)
	if err != nil {
		return err
	}

	byCanonical := make(map[string]*pb.InstrumentMeta, len(metas))
	for _, m := range metas {
		if m.GetEnv().GetInstrument() == nil {
			continue
		}
		byCanonical[m.Env.Instrument.Canonical] = m
	}

	var (
		missing []string
		found   []*pb.InstrumentMeta
	)
	for _, ref := range r.opts.Instruments {
		m, ok := byCanonical[ref.Canonical()]
		if !ok {
			missing = append(missing, ref.Canonical())
			continue
		}
		found = append(found, m)
	}
	if len(found) == 0 {
		return fmt.Errorf("metadata: the venue lists none of the %d configured instruments", len(r.opts.Instruments))
	}
	if len(missing) > 0 {
		// Not fatal, but it means those streams will never produce data, and
		// nothing else in the service would say so.
		r.opts.Log.Warn("metadata: the venue does not list these instruments",
			"symbols", missing, "market_type", core.MarketTypeName(r.opts.MarketType))
	}

	ttl := r.opts.Interval * ttlMultiple
	for _, m := range found {
		canonical := m.Env.Instrument.Canonical
		r.logChanges(canonical, m)

		key := publish.Key(r.opts.Venue, m.Env.Instrument.MarketType, canonical, pb.Channel_CHANNEL_METADATA)
		if err := r.opts.Publisher.Publish(ctx, key, m, ttl); err != nil {
			return err
		}
		r.remember(canonical, m)
	}
	return nil
}

// logChanges compares what arrived against what we published last cycle.
//
// Exchanges change these without warning and announce it nowhere a program can
// read, so this line is the only record that it happened. It is cheap: both
// values are already in hand.
//
// A tick size change does not reinterpret any value already published. Scaling
// is global — 1e-11 for price, 1e-8 for size — so tick size is a fact about the
// instrument rather than the exponent anything was encoded at. That is the
// whole reason the global scale was chosen over a per-instrument one.
func (r *Refresher) logChanges(canonical string, next *pb.InstrumentMeta) {
	prev := r.previous(canonical)
	if prev == nil {
		return
	}
	for _, f := range []struct {
		name       string
		prev, next int64
		render     func(int64) string
	}{
		{"tick_size", prev.TickSize, next.TickSize, renderPrice},
		{"lot_size", prev.LotSize, next.LotSize, renderSize},
		{"min_notional", prev.MinNotional, next.MinNotional, renderPrice},
		{"contract_multiplier", prev.ContractMultiplier, next.ContractMultiplier, renderSize},
	} {
		if f.prev == f.next {
			continue
		}
		r.opts.Log.Warn("instrument metadata changed",
			"symbol", canonical, "field", f.name,
			"from", f.render(f.prev), "to", f.render(f.next))
	}
	if prev.Active != next.Active {
		r.opts.Log.Warn("instrument metadata changed",
			"symbol", canonical, "field", "active", "from", prev.Active, "to", next.Active)
	}
}

// renderPrice and renderSize print a scaled integer as the decimal an operator
// reading the log recognises. Display only.
func renderPrice(v int64) string { return price.Price(v).String() }
func renderSize(v int64) string  { return price.Size(v).String() }

func (r *Refresher) previous(canonical string) *pb.InstrumentMeta {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last[canonical]
}

func (r *Refresher) remember(canonical string, m *pb.InstrumentMeta) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.last[canonical] = m
}

// report tells health whether metadata is available, when there is a health
// tracker to tell.
func (r *Refresher) report(ok bool, reason string) {
	if r.opts.Health == nil {
		return
	}
	r.opts.Health.MetadataState(ok, reason)
}
