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
	"github.com/you/manooch/internal/observability"
	"github.com/you/manooch/internal/publish"
)

// expiredEvent is the keyspace notification Redis emits when a key reaches its
// TTL. deploy/redis.conf enables it with notify-keyspace-events Ex.
const expiredEvent = "__keyevent@%d__:expired"

// errorLogInterval caps the error log rate. Redis being unreachable is one
// fact, not one fact per sweep.
const errorLogInterval = 10 * time.Second

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
	// Database is the Redis database the keyspace events come from.
	Database int

	Health  *health.Tracker
	Metrics *observability.Metrics
	Log     *slog.Logger

	// Specifications is every stream to watch.
	Specifications []core.StreamSpec

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
	options Options
	now     func() time.Time

	// keys and order are fixed after New: the stream set comes from config and
	// does not change under a running process.
	keys  map[string]core.StreamSpec
	order []string

	mutex   sync.Mutex
	active  map[core.StreamSpec]*poller
	expired map[core.StreamSpec]bool
	lastLog time.Time
}

// New builds a watcher. It subscribes to nothing until Run.
func New(options Options) (*Watcher, error) {
	switch {
	case options.Venue == "":
		return nil, errors.New("fallback: no venue")
	case options.Adapter == nil:
		return nil, errors.New("fallback: no adapter")
	case options.Publisher == nil:
		return nil, errors.New("fallback: no publisher")
	case options.Redis == nil:
		return nil, errors.New("fallback: no redis client")
	case options.Health == nil:
		return nil, errors.New("fallback: no health tracker")
	case options.Metrics == nil:
		return nil, errors.New("fallback: no metrics")
	case options.Log == nil:
		return nil, errors.New("fallback: no logger")
	case options.SweepInterval <= 0:
		return nil, fmt.Errorf("fallback: sweep interval is %v", options.SweepInterval)
	case options.PollInterval <= 0:
		return nil, fmt.Errorf("fallback: poll interval is %v", options.PollInterval)
	case options.MaxConcurrentPolls < 1:
		return nil, fmt.Errorf("fallback: max concurrent polls is %d", options.MaxConcurrentPolls)
	}
	if options.Now == nil {
		options.Now = time.Now
	}

	watcher := &Watcher{
		options: options,
		now:     options.Now,
		keys:    make(map[string]core.StreamSpec, len(options.Specifications)),
		active:  map[core.StreamSpec]*poller{},
		expired: map[core.StreamSpec]bool{},
	}
	for _, specification := range options.Specifications {
		k := publish.Key(options.Venue, specification.Instrument.MarketType, specification.Instrument.Canonical(), specification.Channel)
		if _, duplicate := watcher.keys[k]; duplicate {
			continue
		}
		watcher.keys[k] = specification
		watcher.order = append(watcher.order, k)
	}
	return watcher, nil
}

// Run watches for expired keys until ctx ends.
//
// Two triggers, and the second is not optional. Expiry events are Pub/Sub, so
// they are fire-and-forget and can simply not arrive; Redis also only emits one
// when the key is actually reclaimed, which is not the instant it expired. The
// sweep is what makes a missed notification a five-second delay rather than a
// stream that is never served.
func (watcher *Watcher) Run(ctx context.Context) {
	subscription := watcher.options.Redis.Subscribe(ctx, fmt.Sprintf(expiredEvent, watcher.options.Database))
	defer subscription.Close()
	events := subscription.Channel()

	tick := time.NewTicker(watcher.options.SweepInterval)
	defer tick.Stop()

	watcher.options.Log.Info("fallback watching",
		"keys", len(watcher.order),
		"sweep_interval", watcher.options.SweepInterval.String(),
		"poll_interval", watcher.options.PollInterval.String(),
		"max_concurrent", watcher.options.MaxConcurrentPolls)

	for {
		select {
		case <-ctx.Done():
			watcher.stop()
			return
		case message, ok := <-events:
			if !ok {
				watcher.stop()
				return
			}
			watcher.onExpired(ctx, message.Payload)
		case <-tick.C:
			watcher.sweep(ctx)
		}
	}
}

// sweep is the backstop: one pipelined EXISTS over the whole key set.
//
// One round trip, never one call per key: a per-key poll over two hundred
// instruments is six hundred round trips every sweep interval, which is a load
// pattern that makes the outage worse at exactly the wrong moment.
func (watcher *Watcher) sweep(ctx context.Context) {
	pipe := watcher.options.Redis.Pipeline()
	commands := make([]*redis.IntCmd, len(watcher.order))
	for i, k := range watcher.order {
		commands[i] = pipe.Exists(ctx, k)
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, redis.Nil) {
		if ctx.Err() == nil {
			watcher.logError("sweep failed", err)
		}
		return
	}

	for i, command := range commands {
		if n, err := command.Result(); err == nil && n == 0 {
			watcher.onExpired(ctx, watcher.order[i])
		}
	}
}

// onExpired reacts to one key that is gone.
func (watcher *Watcher) onExpired(ctx context.Context, key string) {
	if specification, ok := watcher.keys[key]; ok {
		watcher.Expired(ctx, specification)
	}
	// Anything else is another venue's key, or not ours at all.
}

// Expired reacts to a stream whose key is known to have gone: it reports the
// expiry once and starts serving the stream over REST.
func (watcher *Watcher) Expired(ctx context.Context, specification core.StreamSpec) {
	// Reported once per outage, not once per sweep: the sweep re-finds a key
	// that is still missing every interval, and counting each of those as a
	// fresh expiry would turn one dead stream into a restart every five
	// seconds and a metric nobody can read.
	if watcher.markExpired(specification) {
		watcher.options.Health.KeyExpired(specification)
		watcher.options.Log.Warn("key expired", "stream", specification.String())
		if watcher.options.OnExpired != nil {
			watcher.options.OnExpired(specification)
		}
	}
	// Retried on every sweep, not only on the first sighting: a stream turned
	// away by the concurrency cap has to get another chance when one frees up.
	watcher.engage(ctx, specification)
}

// markExpired records a stream as expired, reporting whether that is new.
func (watcher *Watcher) markExpired(specification core.StreamSpec) bool {
	watcher.mutex.Lock()
	defer watcher.mutex.Unlock()
	if watcher.expired[specification] {
		return false
	}
	watcher.expired[specification] = true
	return true
}

// logError rate-limits a repeated failure to one line per interval.
func (watcher *Watcher) logError(message string, err error) {
	now := watcher.now()

	watcher.mutex.Lock()
	log := now.Sub(watcher.lastLog) >= errorLogInterval
	if log {
		watcher.lastLog = now
	}
	watcher.mutex.Unlock()

	if log {
		watcher.options.Log.Error(message, "error", err.Error())
	}
}
