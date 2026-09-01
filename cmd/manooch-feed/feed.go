package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/you/manooch/internal/adapter"
	"github.com/you/manooch/internal/config"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/fallback"
	"github.com/you/manooch/internal/health"
	"github.com/you/manooch/internal/metadata"
	"github.com/you/manooch/internal/obs"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/internal/ratelimit"
	"github.com/you/manooch/internal/supervisor"
	"github.com/you/manooch/internal/transport"
)

// producers is what will feed the publisher once Redis is up: one venue
// adapter's sockets, the metadata refresher, and the health and fallback
// machinery around them.
type producers struct {
	adapter core.Adapter
	plans   []core.SocketPlan
	limiter *ratelimit.LocalLimiter
}

// planProducers resolves everything the config can get wrong. It opens
// nothing — no socket, no REST call — so an unknown venue or a stream this
// venue cannot serve fails at startup rather than becoming a key nobody ever
// writes, which reads exactly like a venue that went quiet.
func planProducers(cfg *config.Config, log *slog.Logger) (*producers, error) {
	limiter, err := newLimiter(cfg, log)
	if err != nil {
		return nil, err
	}
	a, err := adapter.New(cfg, adapter.Deps{Limiter: limiter})
	if err != nil {
		return nil, err
	}
	specs, err := adapter.Specs(cfg)
	if err != nil {
		return nil, err
	}
	plans, err := a.PlanSubscriptions(specs)
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return nil, errors.New("config declares no streams")
	}
	return &producers{adapter: a, plans: plans, limiter: limiter}, nil
}

// newLimiter translates the venue's published limits into the budget this
// process will spend.
//
// Every capacity is a fraction of what the venue allows, never all of it: the
// limiter is blind to the order service, which shares this host's IP and spends
// against the same limits. The subscription budget is derived rather than
// configured — as many subscribe frames as the connect budget could possibly
// need — because the venue's subscription limit is per connection, which
// PlanSubscriptions already respects, and inventing a rate for it would be a
// number nobody chose.
func newLimiter(cfg *config.Config, log *slog.Logger) (*ratelimit.LocalLimiter, error) {
	rest := ratelimit.Bucket{
		Capacity: cfg.RateLimit.RESTWeightPerMinute,
		Window:   time.Minute,
	}.Fraction(cfg.RateLimit.MaxWeightFraction)

	connect := ratelimit.Bucket{
		Capacity: cfg.RateLimit.WSConnectPer5Min,
		Window:   5 * time.Minute,
	}.Fraction(cfg.RateLimit.WSConnectFraction)

	subscriptions := ratelimit.Bucket{
		Capacity: cfg.RateLimit.SubscriptionsPerConnection * connect.Capacity,
		Window:   connect.Window,
	}

	return ratelimit.New(ratelimit.Options{
		Venue: cfg.Venue,
		Buckets: map[ratelimit.LimitKind]ratelimit.Bucket{
			ratelimit.LimitRESTWeight:    rest,
			ratelimit.LimitWSConnect:     connect,
			ratelimit.LimitSubscriptions: subscriptions,
		},
		Log: log,
	})
}

// start launches the supervision tree and returns a group that closes once
// every part of it has stopped.
//
// Three goroutines sit above the sockets: the health heartbeat, the fallback
// watcher and the socket supervisor. None of them ends the process. A dead
// socket redials, a dead stream relaunches, and a goroutine that will not come
// back is counted and reported rather than escalated into a restart.
func (p *producers) start(ctx context.Context, cfg *config.Config, pub *publish.RedisPublisher, metrics *obs.Metrics, log *slog.Logger) (*sync.WaitGroup, error) {
	var wg sync.WaitGroup

	// The limiter was built before Redis was dialled, because the adapter needed
	// it. Now there is somewhere to write the advisory key.
	p.limiter.AttachPublisher(pub)

	tracker, err := health.New(health.Options{
		Venue:               cfg.Venue,
		Publisher:           pub,
		Metrics:             metrics,
		Log:                 log,
		HeartbeatInterval:   cfg.Health.HeartbeatInterval.Std(),
		ClockSkewDegradedMS: cfg.Health.ClockSkewDegradedMS,
		ClockSkewStaleMS:    cfg.Health.ClockSkewStaleMS,
		FallbackMaxDuration: cfg.Fallback.MaxDuration.Std(),
		MetadataRequired:    cfg.Metadata.StartupRequired,
	})
	if err != nil {
		return nil, err
	}

	specs, err := registerStreams(tracker, p.adapter, p.plans)
	if err != nil {
		return nil, err
	}

	refresher, err := metadata.New(metadata.Options{
		Venue:        cfg.Venue,
		Adapter:      p.adapter,
		Publisher:    pub,
		Health:       tracker,
		Log:          log,
		Instruments:  instrumentsOf(p.plans),
		MarketType:   p.plans[0].Specs[0].Instrument.MarketType,
		Interval:     cfg.Metadata.RefreshInterval.Std(),
		FetchTimeout: cfg.Metadata.FetchTimeout.Std(),
		Required:     cfg.Metadata.StartupRequired,
		Backoff:      backoffPolicy(cfg.Supervisor.SocketReconnectBackoff),
	})
	if err != nil {
		return nil, err
	}

	// The watcher and the supervisor each need the other: an expired key
	// escalates into the supervisor, and a websocket message ends fallback.
	// Both are assigned before anything starts, so neither closure can be
	// called against a nil.
	var (
		proc    *supervisor.Process
		watcher *fallback.Watcher
	)

	if cfg.Fallback.Enabled {
		watcher, err = fallback.New(fallback.Options{
			Venue:              cfg.Venue,
			Adapter:            p.adapter,
			Publisher:          pub,
			Redis:              pub.Redis(),
			DB:                 cfg.Redis.DB,
			Health:             tracker,
			Metrics:            metrics,
			Log:                log,
			Specs:              specs,
			MaxConcurrentPolls: cfg.Fallback.MaxConcurrentPolls,
			PollInterval:       cfg.Fallback.PollInterval.Std(),
			SweepInterval:      cfg.Fallback.SweepInterval.Std(),
			MaxDuration:        cfg.Fallback.MaxDuration.Std(),
			OnExpired:          func(spec core.StreamSpec) { proc.KeyExpired(spec) },
		})
		if err != nil {
			return nil, err
		}
	} else {
		log.Warn("rest fallback disabled: an expired key will stay expired")
	}

	onMessage := func(core.StreamSpec) {}
	if watcher != nil {
		onMessage = watcher.Note
	}

	proc, err = supervisor.New(supervisor.Options{
		Venue:         cfg.Venue,
		Adapter:       p.adapter,
		Plans:         p.plans,
		Publisher:     pub,
		Health:        tracker,
		Metrics:       metrics,
		Log:           log,
		StreamBackoff: backoffPolicy(cfg.Supervisor.StreamRestartBackoff),
		SocketBackoff: backoffPolicy(cfg.Supervisor.SocketReconnectBackoff),
		Breaker: transport.BreakerOptions{
			ConsecutiveFailures: cfg.Supervisor.CircuitBreaker.ConsecutiveFailures,
			OpenDuration:        cfg.Supervisor.CircuitBreaker.OpenDuration.Std(),
		},
		LeakTimeout: cfg.Supervisor.GoroutineLeakTimeout.Std(),
		ConnMaxAge:  cfg.Connection.MaxAge.Std(),
		OnMessage:   onMessage,
	})
	if err != nil {
		return nil, err
	}

	log.Info("venue adapter ready",
		"sockets", len(p.plans), "streams", len(specs),
		"rate_limits", p.limiter.KindNames())

	run := func(fn func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(ctx)
		}()
	}
	// Health first and on its own: it is what publishes STALE while the
	// metadata fetch is still failing, so it has to be running before anything
	// waits on that fetch.
	run(tracker.Run)
	run(refresher.Run)

	// Nothing streams until metadata has landed. A price published at unknown
	// precision, with no contract multiplier, is a number a consumer would size
	// an order from and get wrong.
	run(func(ctx context.Context) {
		if !refresher.WaitReady(ctx) {
			log.Warn("shutting down before instrument metadata arrived; no market data was published")
			return
		}
		var streams sync.WaitGroup
		if watcher != nil {
			streams.Add(1)
			go func() {
				defer streams.Done()
				watcher.Run(ctx)
			}()
		}
		streams.Add(1)
		go func() {
			defer streams.Done()
			proc.Run(ctx)
		}()
		streams.Wait()
	})

	return &wg, nil
}

// instrumentsOf is the distinct instruments the plans cover, in plan order, so
// the metadata refresher asks about exactly what this process streams.
func instrumentsOf(plans []core.SocketPlan) []core.InstrumentRef {
	var (
		out  []core.InstrumentRef
		seen = map[core.InstrumentRef]bool{}
	)
	for _, plan := range plans {
		for _, spec := range plan.Specs {
			if seen[spec.Instrument] {
				continue
			}
			seen[spec.Instrument] = true
			out = append(out, spec.Instrument)
		}
	}
	return out
}

// registerStreams declares every stream to the health tracker before anything
// runs, so a stream that never receives a byte still has a status to publish
// rather than being indistinguishable from one nobody configured.
func registerStreams(tracker *health.Tracker, a core.Adapter, plans []core.SocketPlan) ([]core.StreamSpec, error) {
	for _, plan := range plans {
		for _, spec := range plan.Specs {
			venueSymbol, err := a.VenueSymbol(spec.Instrument)
			if err != nil {
				return nil, err
			}
			tracker.Register(spec, venueSymbol, plan.ID)
		}
	}
	return tracker.Specs(), nil
}

// backoffPolicy translates one configured backoff block. The transport package
// is handed values rather than reading config itself, so a test can build a
// policy without a YAML file.
func backoffPolicy(c config.BackoffConfig) transport.Policy {
	return transport.Policy{
		Initial:    c.Initial.Std(),
		Max:        c.Max.Std(),
		Multiplier: c.Multiplier,
		Jitter:     c.Jitter,
	}
}
