// Package fallback notices when a stream's Redis key expires and serves that
// stream over REST until its socket comes back.
//
// The key expiring is the whole trigger. There is no separate staleness
// timer to keep in step with the TTL, because two mechanisms that are supposed
// to agree about freshness eventually will not.
package fallback

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/health"
	"github.com/you/manooch/internal/obs"
	"github.com/you/manooch/internal/publish"
)

// expiredEvent is the keyspace notification Redis emits when a key reaches its
// TTL. deploy/redis.conf enables it with notify-keyspace-events Ex.
const expiredEvent = "__keyevent@%d__:expired"

// errLogInterval caps the error log rate. Redis being unreachable is one fact,
// not one fact per sweep.
const errLogInterval = 10 * time.Second

// Options configures a Watcher.
type Options struct {
	// Venue is the canonical upper-case venue name.
	Venue string

	// Adapter serves the REST poll. Required.
	Adapter core.Adapter

	// Publisher writes the polled values, on the same keys and channels the
	// socket would have used.
	Publisher publish.Publisher

	// Redis is read from, never written to: the expiry subscription and the
	// sweep. All writes go through Publisher.
	Redis *redis.Client
	// DB is the Redis database the keyspace events come from.
	DB int

	Health  *health.Tracker
	Metrics *obs.Metrics
	Log     *slog.Logger

	// Specs is every stream to watch.
	Specs []core.StreamSpec

	// MaxConcurrentPolls caps how many streams are on REST at once. Streams
	// past the cap go STALE rather than queueing behind the ones ahead.
	MaxConcurrentPolls int

	// PollInterval is how often an engaged poller calls the venue.
	PollInterval time.Duration

	// SweepInterval is how often the EXISTS backstop runs.
	SweepInterval time.Duration

	// MaxDuration is how long a stream may be served by REST before it stops
	// being a degradation and becomes a failure.
	MaxDuration time.Duration

	// OnExpired is called once each time a key is newly found expired, for the
	// supervisor's escalation tiers.
	OnExpired func(core.StreamSpec)

	// Now is swappable for tests. Zero means time.Now.
	Now func() time.Time
}

// A Watcher turns expired keys into REST polls.
type Watcher struct {
	opts Options
	now  func() time.Time

	// keys and order are fixed after New: the stream set comes from config and
	// does not change under a running process.
	keys  map[string]core.StreamSpec
	order []string

	mu      sync.Mutex
	active  map[core.StreamSpec]*poller
	expired map[core.StreamSpec]bool
	lastLog time.Time
}

// New builds a watcher. It subscribes to nothing until Run.
func New(opts Options) (*Watcher, error) {
	switch {
	case opts.Venue == "":
		return nil, errors.New("fallback: no venue")
	case opts.Adapter == nil:
		return nil, errors.New("fallback: no adapter")
	case opts.Publisher == nil:
		return nil, errors.New("fallback: no publisher")
	case opts.Redis == nil:
		return nil, errors.New("fallback: no redis client")
	case opts.Health == nil:
		return nil, errors.New("fallback: no health tracker")
	case opts.Metrics == nil:
		return nil, errors.New("fallback: no metrics")
	case opts.Log == nil:
		return nil, errors.New("fallback: no logger")
	case opts.SweepInterval <= 0:
		return nil, fmt.Errorf("fallback: sweep interval is %v", opts.SweepInterval)
	case opts.PollInterval <= 0:
		return nil, fmt.Errorf("fallback: poll interval is %v", opts.PollInterval)
	case opts.MaxConcurrentPolls < 1:
		return nil, fmt.Errorf("fallback: max concurrent polls is %d", opts.MaxConcurrentPolls)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}

	w := &Watcher{
		opts:    opts,
		now:     opts.Now,
		keys:    make(map[string]core.StreamSpec, len(opts.Specs)),
		active:  map[core.StreamSpec]*poller{},
		expired: map[core.StreamSpec]bool{},
	}
	for _, spec := range opts.Specs {
		k := publish.Key(opts.Venue, spec.Instrument.MarketType, spec.Instrument.Canonical(), spec.Channel)
		if _, dup := w.keys[k]; dup {
			continue
		}
		w.keys[k] = spec
		w.order = append(w.order, k)
	}
	return w, nil
}

// Run watches for expired keys until ctx ends.
//
// Two triggers, and the second is not optional. Expiry events are Pub/Sub, so
// they are fire-and-forget and can simply not arrive; Redis also only emits one
// when the key is actually reclaimed, which is not the instant it expired. The
// sweep is what makes a missed notification a five-second delay rather than a
// stream that is never served.
func (w *Watcher) Run(ctx context.Context) {
	sub := w.opts.Redis.Subscribe(ctx, fmt.Sprintf(expiredEvent, w.opts.DB))
	defer sub.Close()
	events := sub.Channel()

	tick := time.NewTicker(w.opts.SweepInterval)
	defer tick.Stop()

	w.opts.Log.Info("fallback watching",
		"keys", len(w.order),
		"sweep_interval", w.opts.SweepInterval.String(),
		"poll_interval", w.opts.PollInterval.String(),
		"max_concurrent", w.opts.MaxConcurrentPolls)

	for {
		select {
		case <-ctx.Done():
			w.stop()
			return
		case m, ok := <-events:
			if !ok {
				w.stop()
				return
			}
			w.onExpired(ctx, m.Payload)
		case <-tick.C:
			w.sweep(ctx)
		}
	}
}

// sweep is the backstop: one pipelined EXISTS over the whole key set.
//
// One round trip, never one call per key: a per-key poll over two hundred
// instruments is six hundred round trips every sweep interval, which is a load
// pattern that makes the outage worse at exactly the wrong moment.
func (w *Watcher) sweep(ctx context.Context) {
	pipe := w.opts.Redis.Pipeline()
	cmds := make([]*redis.IntCmd, len(w.order))
	for i, k := range w.order {
		cmds[i] = pipe.Exists(ctx, k)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		if ctx.Err() == nil {
			w.logError("sweep failed", err)
		}
		return
	}

	for i, cmd := range cmds {
		if n, err := cmd.Result(); err == nil && n == 0 {
			w.onExpired(ctx, w.order[i])
		}
	}
}

// onExpired reacts to one key that is gone.
func (w *Watcher) onExpired(ctx context.Context, key string) {
	if spec, ok := w.keys[key]; ok {
		w.Expired(ctx, spec)
	}
	// Anything else is another venue's key, or not ours at all.
}

// Expired reacts to a stream whose key is known to have gone: it reports the
// expiry once and starts serving the stream over REST.
func (w *Watcher) Expired(ctx context.Context, spec core.StreamSpec) {
	// Reported once per outage, not once per sweep: the sweep re-finds a key
	// that is still missing every interval, and counting each of those as a
	// fresh expiry would turn one dead stream into a restart every five
	// seconds and a metric nobody can read.
	if w.markExpired(spec) {
		w.opts.Health.KeyExpired(spec)
		w.opts.Log.Warn("key expired", "stream", spec.String())
		if w.opts.OnExpired != nil {
			w.opts.OnExpired(spec)
		}
	}
	// Retried on every sweep, not only on the first sighting: a stream turned
	// away by the concurrency cap has to get another chance when one frees up.
	w.engage(ctx, spec)
}

// markExpired records a stream as expired, reporting whether that is new.
func (w *Watcher) markExpired(spec core.StreamSpec) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.expired[spec] {
		return false
	}
	w.expired[spec] = true
	return true
}

// clearExpired forgets a stream's expiry, reporting whether it had one.
func (w *Watcher) clearExpired(spec core.StreamSpec) bool {
	w.mu.Lock()
	defer w.mu.Unlock()
	if !w.expired[spec] {
		return false
	}
	delete(w.expired, spec)
	return true
}

// logError rate-limits a repeated failure to one line per interval.
func (w *Watcher) logError(msg string, err error) {
	now := w.now()

	w.mu.Lock()
	log := now.Sub(w.lastLog) >= errLogInterval
	if log {
		w.lastLog = now
	}
	w.mu.Unlock()

	if log {
		w.opts.Log.Error(msg, "error", err.Error())
	}
}
