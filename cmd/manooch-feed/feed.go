package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"

	"github.com/you/manooch/internal/adapter"
	"github.com/you/manooch/internal/config"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/fallback"
	"github.com/you/manooch/internal/health"
	"github.com/you/manooch/internal/obs"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/internal/supervisor"
	"github.com/you/manooch/internal/synth"
	"github.com/you/manooch/internal/transport"
)

// producers is what will feed the publisher once Redis is up: either the
// synthetic generator or a venue adapter's sockets.
type producers struct {
	synthetic bool
	adapter   core.Adapter
	plans     []core.SocketPlan
}

// planProducers resolves everything the config can get wrong. It opens
// nothing — no socket, no REST call — so an unknown venue or a stream this
// venue cannot serve fails at startup rather than becoming a key nobody ever
// writes, which reads exactly like a venue that went quiet.
func planProducers(f flags, cfg *config.Config) (*producers, error) {
	if f.synthetic {
		return &producers{synthetic: true}, nil
	}

	a, err := adapter.New(cfg)
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
	return &producers{adapter: a, plans: plans}, nil
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

	if p.synthetic {
		log.Warn("synthetic mode: publishing generated data, not venue data")
		gen := synth.New(cfg, pub, log)
		wg.Add(1)
		go func() {
			defer wg.Done()
			gen.Run(ctx)
		}()
		return &wg, nil
	}

	tracker, err := health.New(health.Options{
		Venue:               cfg.Venue,
		Publisher:           pub,
		Metrics:             metrics,
		Log:                 log,
		HeartbeatInterval:   cfg.Health.HeartbeatInterval.Std(),
		ClockSkewDegradedMS: cfg.Health.ClockSkewDegradedMS,
		ClockSkewStaleMS:    cfg.Health.ClockSkewStaleMS,
		FallbackMaxDuration: cfg.Fallback.MaxDuration.Std(),
	})
	if err != nil {
		return nil, err
	}

	specs, err := registerStreams(tracker, p.adapter, p.plans)
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

	log.Info("venue adapter ready", "sockets", len(p.plans), "streams", len(specs))

	run := func(fn func(context.Context)) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			fn(ctx)
		}()
	}
	run(tracker.Run)
	if watcher != nil {
		run(watcher.Run)
	}
	run(proc.Run)

	return &wg, nil
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
