package ratelimit

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/observability"
	"github.com/you/manooch/internal/publish"
)

// Options configures a LocalLimiter.
type Options struct {
	// Venue is the canonical upper-case venue name. One process serves one
	// venue, so it is also the only venue Allow will ever be asked about; a
	// call naming another is a bug and is refused rather than budgeted.
	Venue string

	// Buckets is the budget per kind. A kind absent from the map is
	// unbudgeted: Allow permits it and Used answers (0, 0). Nothing is
	// invented for a limit the venue does not publish.
	Buckets map[LimitKind]Bucket

	// Publisher writes the advisory key. Optional, and settable later with
	// AttachPublisher: it is data on Redis, not a dependency, and the limiter
	// works without one.
	Publisher publish.Publisher

	Metrics *observability.Metrics
	Log     *slog.Logger

	// Now and Sleep are swappable for tests. Zero means the real clock.
	Now   func() time.Time
	Sleep func(ctx context.Context, duration time.Duration) bool
}

// A LocalLimiter is an in-process token bucket per (venue, kind), implemented
// as GCRA: one theoretical-arrival-time per bucket rather than a counter and a
// refill ticker, so there is no goroutine and no window boundary for a burst
// to straddle.
//
// It is deliberately blind to every other process on this host. The order
// service shares the IP and spends against the same venue budget, and
// coordinating with it would make this service a dependency of that one.
// rate_limit.max_weight_fraction is the compensation: we use a share of the
// published limit and leave the rest.
type LocalLimiter struct {
	options Options
	now     func() time.Time
	sleep   func(ctx context.Context, duration time.Duration) bool

	mutex                  sync.Mutex
	publisher              publish.Publisher
	theoreticalArrivalTime map[LimitKind]time.Time
}

var _ Limiter = (*LocalLimiter)(nil)

// New builds a limiter. It publishes nothing until the first Allow.
func New(options Options) (*LocalLimiter, error) {
	if options.Venue == "" {
		return nil, fmt.Errorf("ratelimit: no venue")
	}
	if options.Log == nil {
		return nil, fmt.Errorf("ratelimit: no logger")
	}
	for _, kind := range Kinds {
		bucket, ok := options.Buckets[kind]
		if !ok {
			continue
		}
		if err := bucket.Validate(kind); err != nil {
			return nil, err
		}
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.Sleep == nil {
		options.Sleep = sleep
	}
	return &LocalLimiter{
		options:                options,
		now:                    options.Now,
		sleep:                  options.Sleep,
		publisher:              options.Publisher,
		theoreticalArrivalTime: map[LimitKind]time.Time{},
	}, nil
}

// AttachPublisher supplies the publisher for the advisory key after
// construction.
//
// The daemon builds the limiter before it dials Redis, because the adapter
// needs one and resolving the adapter is what proves the config servable. The
// advisory key cannot be written until there is a Redis to write it to, so the
// publisher arrives second.
func (limiter *LocalLimiter) AttachPublisher(publisher publish.Publisher) {
	limiter.mutex.Lock()
	defer limiter.mutex.Unlock()
	limiter.publisher = publisher
}

// attachedPublisher is whatever has been attached. Callers must not hold the
// mutex.
func (limiter *LocalLimiter) attachedPublisher() publish.Publisher {
	limiter.mutex.Lock()
	defer limiter.mutex.Unlock()
	return limiter.publisher
}

// Allow blocks until cost units of budget are available, or refuses.
//
// It refuses rather than waiting when the wait would outrun the caller's
// deadline, and immediately when cost is larger than the whole bucket: a wait
// that can never end is not a wait. Either way the caller does not make the
// request, which is what "fail closed" means here — the alternative is
// proceeding on the assumption that it is probably fine, and discovering
// otherwise as an IP ban.
func (limiter *LocalLimiter) Allow(ctx context.Context, venue string, kind LimitKind, cost int) error {
	if venue != limiter.options.Venue {
		return fmt.Errorf("ratelimit: venue %q is not %q", venue, limiter.options.Venue)
	}
	if cost <= 0 {
		return nil
	}
	bucket, ok := limiter.options.Buckets[kind]
	if !ok {
		return nil // unbudgeted: the venue publishes no limit for this kind
	}
	if cost > bucket.Capacity {
		limiter.deny(kind)
		return fmt.Errorf("%w: %s costs %d of a %d budget", ErrBudgetExhausted, kind, cost, bucket.Capacity)
	}

	wait, ok := limiter.reserve(kind, bucket, cost, deadline(ctx))
	if !ok {
		used, capacity := limiter.Used(venue, kind)
		limiter.deny(kind)
		return fmt.Errorf("%w: %s, %d in use of %d", ErrBudgetExhausted, kind, used, capacity)
	}

	limiter.report(ctx)
	if wait <= 0 {
		return nil
	}
	limiter.options.Log.Warn("rate limit: waiting for budget",
		"kind", kind.String(), "cost", cost, "wait", wait.Truncate(time.Millisecond).String())
	if !limiter.sleep(ctx, wait) {
		// The slot stays spent. Handing it back would let a cancelled caller
		// and its retry both spend it, which is the one direction a limiter
		// must never be wrong in.
		return ctx.Err()
	}
	return nil
}

// reserve advances the bucket's theoretical arrival time by cost, returning how
// long the caller must wait for the slot it just took. It reports false, having
// changed nothing, when that wait would pass the caller's deadline.
func (limiter *LocalLimiter) reserve(kind LimitKind, bucket Bucket, cost int, deadline time.Time) (time.Duration, bool) {
	limiter.mutex.Lock()
	defer limiter.mutex.Unlock()

	now := limiter.now()
	interval := bucket.interval()
	duration := interval * time.Duration(bucket.Capacity)

	theoreticalArrivalTime := limiter.theoreticalArrivalTime[kind]
	if theoreticalArrivalTime.Before(now) {
		theoreticalArrivalTime = now
	}
	next := theoreticalArrivalTime.Add(interval * time.Duration(cost))

	// The slot opens once the new arrival time is within one full bucket of
	// now: that is the burst allowance, and it is what makes a cold bucket
	// serve Capacity operations at once rather than one per interval.
	wait := next.Add(-duration).Sub(now)
	if wait < 0 {
		wait = 0
	}
	if wait > 0 && !deadline.IsZero() && now.Add(wait).After(deadline) {
		return 0, false
	}

	limiter.theoreticalArrivalTime[kind] = next
	return wait, true
}

// Used is the budget spent and the budget available for one kind.
func (limiter *LocalLimiter) Used(venue string, kind LimitKind) (int, int) {
	bucket, ok := limiter.options.Buckets[kind]
	if !ok || venue != limiter.options.Venue {
		return 0, 0
	}
	limiter.mutex.Lock()
	defer limiter.mutex.Unlock()
	return limiter.used(kind, bucket), bucket.Capacity
}

// used is how much of a bucket is spent right now: the distance the arrival
// time has been pushed past the present, in whole units. Callers hold the
// mutex.
func (limiter *LocalLimiter) used(kind LimitKind, bucket Bucket) int {
	duration := limiter.theoreticalArrivalTime[kind].Sub(limiter.now())
	if duration <= 0 {
		return 0
	}
	interval := bucket.interval()
	// Round up: a partly-spent unit is spent.
	n := int((duration + interval - 1) / interval)
	return min(n, bucket.Capacity)
}

// deny counts a refusal. It is a counter rather than a log line per refusal
// because a venue we are backing off from produces a lot of them, and the fact
// is one fact.
func (limiter *LocalLimiter) deny(kind LimitKind) {
	if limiter.options.Metrics != nil {
		limiter.options.Metrics.RateLimitDenied.WithLabelValues(limiter.options.Venue, kind.String()).Inc()
	}
	limiter.options.Log.Warn("rate limit: refusing the operation", "kind", kind.String())
}

// deadline is the caller's deadline, or the zero time when it has none.
func deadline(ctx context.Context) time.Time {
	t, ok := ctx.Deadline()
	if !ok {
		return time.Time{}
	}
	return t
}

// sleep waits for d, reporting false if ctx ended first.
func sleep(ctx context.Context, duration time.Duration) bool {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

// ---------- advisory publication ----------

// Snapshot is every budgeted kind's current usage, in Kinds order.
func (limiter *LocalLimiter) Snapshot() []*manoochv1.RateLimitBudget {
	out := make([]*manoochv1.RateLimitBudget, 0, len(limiter.options.Buckets))
	for _, kind := range Kinds {
		bucket, ok := limiter.options.Buckets[kind]
		if !ok {
			continue
		}
		used, capacity := limiter.Used(limiter.options.Venue, kind)
		out = append(out, &manoochv1.RateLimitBudget{
			Kind:     kind.String(),
			Used:     int64(used),
			Capacity: int64(capacity),
			WindowMs: bucket.Window.Milliseconds(),
		})
	}
	return out
}

// report writes the usage gauge and the advisory key.
//
// The key is data on Redis, not a dependency: the order service shares this
// host's IP and may read it to decide how much budget is left for its own
// calls, and nothing here breaks if it never does.
func (limiter *LocalLimiter) report(ctx context.Context) {
	budgets := limiter.Snapshot()

	if limiter.options.Metrics != nil {
		for _, budget := range budgets {
			if budget.Capacity > 0 {
				limiter.options.Metrics.RateLimitUsed.WithLabelValues(limiter.options.Venue, budget.Kind).
					Set(float64(budget.Used) / float64(budget.Capacity))
			}
		}
	}
	publisher := limiter.attachedPublisher()
	if publisher == nil || len(budgets) == 0 {
		return
	}

	// A bucket at capacity is not an error — it is the limiter working — but a
	// consumer reading this key wants to know it is happening.
	status := manoochv1.Status_STATUS_HEALTHY
	reason := ""
	for _, budget := range budgets {
		if budget.Used >= budget.Capacity {
			status, reason = manoochv1.Status_STATUS_DEGRADED, budget.Kind+" budget exhausted"
			break
		}
	}

	message := &manoochv1.RateLimit{
		Env: &manoochv1.Envelope{
			Venue:      limiter.options.Venue,
			Channel:    manoochv1.Channel_CHANNEL_RATELIMIT,
			RecvTimeNs: limiter.now().UnixNano(),
			// No source: nothing here came from the venue. It is what this
			// process has spent, not what the venue told us it has.
			Status:       status,
			StatusReason: reason,
		},
		Budgets: budgets,
	}
	// A failed write is not worth reacting to: the key expiring is itself the
	// signal that nobody is maintaining it.
	_ = publisher.Publish(ctx, publish.VenueKey(limiter.options.Venue, publish.SubjectRateLimit), message, limiter.timeToLive())
}

// timeToLive is twice the longest window, so the key outlives a quiet period
// without outliving the process that writes it.
func (limiter *LocalLimiter) timeToLive() time.Duration {
	var longest time.Duration
	for _, bucket := range limiter.options.Buckets {
		longest = max(longest, bucket.Window)
	}
	return longest * 2
}

// KindNames is every budgeted kind's name, sorted, for logs at startup.
func (limiter *LocalLimiter) KindNames() []string {
	out := make([]string, 0, len(limiter.options.Buckets))
	for kind := range limiter.options.Buckets {
		out = append(out, kind.String())
	}
	sort.Strings(out)
	return out
}
