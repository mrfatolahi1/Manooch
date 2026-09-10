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

	"github.com/you/manooch/gen/manoochv1"
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
	MarketType manoochv1.MarketType

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
	options Options
	now     func() time.Time

	// ready closes once the first fetch has succeeded. It is a channel rather
	// than a flag because the daemon parks on it before it starts streaming.
	ready     chan struct{}
	readyOnce sync.Once

	mutex sync.Mutex
	last  map[string]*manoochv1.InstrumentMeta // canonical symbol -> last published
}

// New builds a refresher. It fetches nothing until Run.
func New(options Options) (*Refresher, error) {
	switch {
	case options.Venue == "":
		return nil, errors.New("metadata: no venue")
	case options.Adapter == nil:
		return nil, errors.New("metadata: no adapter")
	case options.Publisher == nil:
		return nil, errors.New("metadata: no publisher")
	case options.Log == nil:
		return nil, errors.New("metadata: no logger")
	case len(options.Instruments) == 0:
		return nil, errors.New("metadata: no instruments")
	case options.Interval <= 0:
		return nil, fmt.Errorf("metadata: refresh interval is %v", options.Interval)
	case options.FetchTimeout <= 0:
		return nil, fmt.Errorf("metadata: fetch timeout is %v", options.FetchTimeout)
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	return &Refresher{
		options: options,
		now:     options.Now,
		ready:   make(chan struct{}),
		last:    map[string]*manoochv1.InstrumentMeta{},
	}, nil
}

// Ready closes once the first fetch has succeeded.
func (refresher *Refresher) Ready() <-chan struct{} { return refresher.ready }

// WaitReady blocks until metadata has arrived, reporting false if ctx ended
// first. It returns immediately when metadata is not a startup requirement.
func (refresher *Refresher) WaitReady(ctx context.Context) bool {
	if !refresher.options.Required {
		return ctx.Err() == nil
	}
	select {
	case <-refresher.ready:
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
func (refresher *Refresher) Run(ctx context.Context) {
	refresher.report(false, unavailable)

	for attempt := 0; ctx.Err() == nil; attempt++ {
		if err := refresher.refresh(ctx); err != nil {
			if ctx.Err() != nil {
				return
			}
			refresher.options.Log.Error("metadata unavailable, the venue is publishing nothing",
				"attempt", attempt+1, "error", err.Error())
			refresher.report(false, unavailable)
			if !refresher.options.Backoff.Sleep(ctx, attempt) {
				return
			}
			continue
		}
		break
	}
	if ctx.Err() != nil {
		return
	}

	refresher.report(true, "")
	refresher.readyOnce.Do(func() { close(refresher.ready) })

	tick := time.NewTicker(refresher.options.Interval)
	defer tick.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
			if err := refresher.refresh(ctx); err != nil && ctx.Err() == nil {
				// Held, not escalated: the prices are still arriving and what
				// we published last cycle is still inside its TTL.
				refresher.options.Log.Error("metadata refresh failed, keeping the last values until they expire",
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
func (refresher *Refresher) refresh(ctx context.Context) error {
	fetchCtx, cancel := context.WithTimeout(ctx, refresher.options.FetchTimeout)
	defer cancel()

	metadataList, err := refresher.options.Adapter.FetchMetadata(fetchCtx, refresher.options.MarketType)
	if err != nil {
		return err
	}

	byCanonical := make(map[string]*manoochv1.InstrumentMeta, len(metadataList))
	for _, metadata := range metadataList {
		if metadata.GetEnv().GetInstrument() == nil {
			continue
		}
		byCanonical[metadata.Env.Instrument.Canonical] = metadata
	}

	var (
		missing []string
		found   []*manoochv1.InstrumentMeta
	)
	for _, reference := range refresher.options.Instruments {
		metadata, ok := byCanonical[reference.Canonical()]
		if !ok {
			missing = append(missing, reference.Canonical())
			continue
		}
		found = append(found, metadata)
	}
	if len(found) == 0 {
		return fmt.Errorf("metadata: the venue lists none of the %d configured instruments", len(refresher.options.Instruments))
	}
	if len(missing) > 0 {
		// Not fatal, but it means those streams will never produce data, and
		// nothing else in the service would say so.
		refresher.options.Log.Warn("metadata: the venue does not list these instruments",
			"symbols", missing, "market_type", core.MarketTypeName(refresher.options.MarketType))
	}

	timeToLive := refresher.options.Interval * ttlMultiple
	for _, metadata := range found {
		canonical := metadata.Env.Instrument.Canonical
		refresher.logChanges(canonical, metadata)

		key := publish.Key(refresher.options.Venue, metadata.Env.Instrument.MarketType, canonical, manoochv1.Channel_CHANNEL_METADATA)
		if err := refresher.options.Publisher.Publish(ctx, key, metadata, timeToLive); err != nil {
			return err
		}
		refresher.remember(canonical, metadata)
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
func (refresher *Refresher) logChanges(canonical string, next *manoochv1.InstrumentMeta) {
	prev := refresher.previous(canonical)
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
		refresher.options.Log.Warn("instrument metadata changed",
			"symbol", canonical, "field", f.name,
			"from", f.render(f.prev), "to", f.render(f.next))
	}
	if prev.Active != next.Active {
		refresher.options.Log.Warn("instrument metadata changed",
			"symbol", canonical, "field", "active", "from", prev.Active, "to", next.Active)
	}
}

// renderPrice and renderSize print a scaled integer as the decimal an operator
// reading the log recognises. Display only.
func renderPrice(v int64) string { return price.Price(v).String() }
func renderSize(v int64) string  { return price.Size(v).String() }

func (refresher *Refresher) previous(canonical string) *manoochv1.InstrumentMeta {
	refresher.mutex.Lock()
	defer refresher.mutex.Unlock()
	return refresher.last[canonical]
}

func (refresher *Refresher) remember(canonical string, metadata *manoochv1.InstrumentMeta) {
	refresher.mutex.Lock()
	defer refresher.mutex.Unlock()
	refresher.last[canonical] = metadata
}

// report tells health whether metadata is available, when there is a health
// tracker to tell.
func (refresher *Refresher) report(ok bool, reason string) {
	if refresher.options.Health == nil {
		return
	}
	refresher.options.Health.MetadataState(ok, reason)
}
