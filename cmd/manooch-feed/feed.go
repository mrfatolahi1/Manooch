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
	"github.com/you/manooch/internal/observability"
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
func planProducers(configuration *config.Config, log *slog.Logger) (*producers, error) {
	limiter, err := newLimiter(configuration, log)
	if err != nil {
		return nil, err
	}
	venueAdapter, err := adapter.New(configuration, adapter.Dependencies{Limiter: limiter})
	if err != nil {
		return nil, err
	}
	specifications, err := adapter.Specifications(configuration)
	if err != nil {
		return nil, err
	}
	plans, err := venueAdapter.PlanSubscriptions(specifications)
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return nil, errors.New("config declares no streams")
	}
	return &producers{adapter: venueAdapter, plans: plans, limiter: limiter}, nil
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
func newLimiter(configuration *config.Config, log *slog.Logger) (*ratelimit.LocalLimiter, error) {
	rest := ratelimit.Bucket{
		Capacity: configuration.RateLimit.RESTWeightPerMinute,
		Window:   time.Minute,
	}.Fraction(configuration.RateLimit.MaxWeightFraction)

	connect := ratelimit.Bucket{
		Capacity: configuration.RateLimit.WebSocketConnectPer5Min,
		Window:   5 * time.Minute,
	}.Fraction(configuration.RateLimit.WebSocketConnectFraction)

	subscriptions := ratelimit.Bucket{
		Capacity: configuration.RateLimit.SubscriptionsPerConnection * connect.Capacity,
		Window:   connect.Window,
	}

	return ratelimit.New(ratelimit.Options{
		Venue: configuration.Venue,
		Buckets: map[ratelimit.LimitKind]ratelimit.Bucket{
			ratelimit.LimitRESTWeight:       rest,
			ratelimit.LimitWebSocketConnect: connect,
			ratelimit.LimitSubscriptions:    subscriptions,
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
func (producers *producers) start(ctx context.Context, configuration *config.Config, publisher *publish.RedisPublisher, metrics *observability.Metrics, log *slog.Logger) (*sync.WaitGroup, error) {
	var waitGroup sync.WaitGroup

	// The limiter was built before Redis was dialled, because the adapter needed
	// it. Now there is somewhere to write the advisory key.
	producers.limiter.AttachPublisher(publisher)

	tracker, err := health.New(health.Options{
		Venue:               configuration.Venue,
		Publisher:           publisher,
		Metrics:             metrics,
		Log:                 log,
		HeartbeatInterval:   configuration.Health.HeartbeatInterval.Standard(),
		ClockSkewDegradedMS: configuration.Health.ClockSkewDegradedMS,
		ClockSkewStaleMS:    configuration.Health.ClockSkewStaleMS,
		FallbackMaxDuration: configuration.Fallback.MaxDuration.Standard(),
		MetadataRequired:    configuration.Metadata.StartupRequired,
	})
	if err != nil {
		return nil, err
	}

	specifications, err := registerStreams(tracker, producers.adapter, producers.plans)
	if err != nil {
		return nil, err
	}

	refresher, err := metadata.New(metadata.Options{
		Venue:        configuration.Venue,
		Adapter:      producers.adapter,
		Publisher:    publisher,
		Health:       tracker,
		Log:          log,
		Instruments:  instrumentsOf(producers.plans),
		MarketType:   producers.plans[0].Specifications[0].Instrument.MarketType,
		Interval:     configuration.Metadata.RefreshInterval.Standard(),
		FetchTimeout: configuration.Metadata.FetchTimeout.Standard(),
		Required:     configuration.Metadata.StartupRequired,
		Backoff:      backoffPolicy(configuration.Supervisor.SocketReconnectBackoff),
	})
	if err != nil {
		return nil, err
	}

	// The watcher and the supervisor each need the other: an expired key
	// escalates into the supervisor, and a websocket message ends fallback.
	// Both are assigned before anything starts, so neither closure can be
	// called against a nil.
	var (
		process *supervisor.Process
		watcher *fallback.Watcher
	)

	if configuration.Fallback.Enabled {
		watcher, err = fallback.New(fallback.Options{
			Venue:              configuration.Venue,
			Adapter:            producers.adapter,
			Publisher:          publisher,
			Redis:              publisher.Redis(),
			Database:           configuration.Redis.Database,
			Health:             tracker,
			Metrics:            metrics,
			Log:                log,
			Specifications:     specifications,
			MaxConcurrentPolls: configuration.Fallback.MaxConcurrentPolls,
			PollInterval:       configuration.Fallback.PollInterval.Standard(),
			SweepInterval:      configuration.Fallback.SweepInterval.Standard(),
			MaxDuration:        configuration.Fallback.MaxDuration.Standard(),
			OnExpired:          func(specification core.StreamSpec) { process.KeyExpired(specification) },
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

	process, err = supervisor.New(supervisor.Options{
		Venue:         configuration.Venue,
		Adapter:       producers.adapter,
		Plans:         producers.plans,
		Publisher:     publisher,
		Health:        tracker,
		Metrics:       metrics,
		Log:           log,
		StreamBackoff: backoffPolicy(configuration.Supervisor.StreamRestartBackoff),
		SocketBackoff: backoffPolicy(configuration.Supervisor.SocketReconnectBackoff),
		Breaker: transport.BreakerOptions{
			ConsecutiveFailures: configuration.Supervisor.CircuitBreaker.ConsecutiveFailures,
			OpenDuration:        configuration.Supervisor.CircuitBreaker.OpenDuration.Standard(),
		},
		LeakTimeout: configuration.Supervisor.GoroutineLeakTimeout.Standard(),
		ConnMaxAge:  configuration.Connection.MaxAge.Standard(),
		OnMessage:   onMessage,
	})
	if err != nil {
		return nil, err
	}

	log.Info("venue adapter ready",
		"sockets", len(producers.plans), "streams", len(specifications),
		"rate_limits", producers.limiter.KindNames())

	run := func(callback func(context.Context)) {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			callback(ctx)
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
			process.Run(ctx)
		}()
		streams.Wait()
	})

	return &waitGroup, nil
}

// instrumentsOf is the distinct instruments the plans cover, in plan order, so
// the metadata refresher asks about exactly what this process streams.
func instrumentsOf(plans []core.SocketPlan) []core.InstrumentRef {
	var (
		out  []core.InstrumentRef
		seen = map[core.InstrumentRef]bool{}
	)
	for _, plan := range plans {
		for _, specification := range plan.Specifications {
			if seen[specification.Instrument] {
				continue
			}
			seen[specification.Instrument] = true
			out = append(out, specification.Instrument)
		}
	}
	return out
}

// registerStreams declares every stream to the health tracker before anything
// runs, so a stream that never receives a byte still has a status to publish
// rather than being indistinguishable from one nobody configured.
func registerStreams(tracker *health.Tracker, adapter core.Adapter, plans []core.SocketPlan) ([]core.StreamSpec, error) {
	for _, plan := range plans {
		for _, specification := range plan.Specifications {
			venueSymbol, err := adapter.VenueSymbol(specification.Instrument)
			if err != nil {
				return nil, err
			}
			tracker.Register(specification, venueSymbol, plan.ID)
		}
	}
	return tracker.Specifications(), nil
}

// backoffPolicy translates one configured backoff block. The transport package
// is handed values rather than reading config itself, so a test can build a
// policy without a YAML file.
func backoffPolicy(backoff config.BackoffConfig) transport.Policy {
	return transport.Policy{
		Initial:    backoff.Initial.Standard(),
		Max:        backoff.Max.Standard(),
		Multiplier: backoff.Multiplier,
		Jitter:     backoff.Jitter,
	}
}
