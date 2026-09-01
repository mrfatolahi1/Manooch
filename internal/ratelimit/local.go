package ratelimit

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/obs"
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

	Metrics *obs.Metrics
	Log     *slog.Logger

	// Now and Sleep are swappable for tests. Zero means the real clock.
	Now   func() time.Time
	Sleep func(ctx context.Context, d time.Duration) bool
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
	opts  Options
	now   func() time.Time
	sleep func(ctx context.Context, d time.Duration) bool

	mu  sync.Mutex
	pub publish.Publisher
	tat map[LimitKind]time.Time
}

var _ Limiter = (*LocalLimiter)(nil)

// New builds a limiter. It publishes nothing until the first Allow.
func New(opts Options) (*LocalLimiter, error) {
	if opts.Venue == "" {
		return nil, fmt.Errorf("ratelimit: no venue")
	}
	if opts.Log == nil {
		return nil, fmt.Errorf("ratelimit: no logger")
	}
	for _, kind := range Kinds {
		b, ok := opts.Buckets[kind]
		if !ok {
			continue
		}
		if err := b.Validate(kind); err != nil {
			return nil, err
		}
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Sleep == nil {
		opts.Sleep = sleep
	}
	return &LocalLimiter{
		opts:  opts,
		now:   opts.Now,
		sleep: opts.Sleep,
		pub:   opts.Publisher,
		tat:   map[LimitKind]time.Time{},
	}, nil
}

// AttachPublisher supplies the publisher for the advisory key after
// construction.
//
// The daemon builds the limiter before it dials Redis, because the adapter
// needs one and resolving the adapter is what proves the config servable. The
// advisory key cannot be written until there is a Redis to write it to, so the
// publisher arrives second.
func (l *LocalLimiter) AttachPublisher(p publish.Publisher) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pub = p
}

// publisher is whatever has been attached. Callers must not hold the mutex.
func (l *LocalLimiter) publisher() publish.Publisher {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.pub
}

// Allow blocks until cost units of budget are available, or refuses.
//
// It refuses rather than waiting when the wait would outrun the caller's
// deadline, and immediately when cost is larger than the whole bucket: a wait
// that can never end is not a wait. Either way the caller does not make the
// request, which is what "fail closed" means here — the alternative is
// proceeding on the assumption that it is probably fine, and discovering
// otherwise as an IP ban.
func (l *LocalLimiter) Allow(ctx context.Context, venue string, kind LimitKind, cost int) error {
	if venue != l.opts.Venue {
		return fmt.Errorf("ratelimit: venue %q is not %q", venue, l.opts.Venue)
	}
	if cost <= 0 {
		return nil
	}
	bucket, ok := l.opts.Buckets[kind]
	if !ok {
		return nil // unbudgeted: the venue publishes no limit for this kind
	}
	if cost > bucket.Capacity {
		l.deny(kind)
		return fmt.Errorf("%w: %s costs %d of a %d budget", ErrBudgetExhausted, kind, cost, bucket.Capacity)
	}

	wait, ok := l.reserve(kind, bucket, cost, deadline(ctx))
	if !ok {
		used, capacity := l.Used(venue, kind)
		l.deny(kind)
		return fmt.Errorf("%w: %s, %d in use of %d", ErrBudgetExhausted, kind, used, capacity)
	}

	l.report(ctx)
	if wait <= 0 {
		return nil
	}
	l.opts.Log.Warn("rate limit: waiting for budget",
		"kind", kind.String(), "cost", cost, "wait", wait.Truncate(time.Millisecond).String())
	if !l.sleep(ctx, wait) {
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
func (l *LocalLimiter) reserve(kind LimitKind, bucket Bucket, cost int, deadline time.Time) (time.Duration, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := l.now()
	interval := bucket.interval()
	burst := interval * time.Duration(bucket.Capacity)

	tat := l.tat[kind]
	if tat.Before(now) {
		tat = now
	}
	next := tat.Add(interval * time.Duration(cost))

	// The slot opens once the new arrival time is within one full bucket of
	// now: that is the burst allowance, and it is what makes a cold bucket
	// serve Capacity operations at once rather than one per interval.
	wait := next.Add(-burst).Sub(now)
	if wait < 0 {
		wait = 0
	}
	if wait > 0 && !deadline.IsZero() && now.Add(wait).After(deadline) {
		return 0, false
	}

	l.tat[kind] = next
	return wait, true
}

// Used is the budget spent and the budget available for one kind.
func (l *LocalLimiter) Used(venue string, kind LimitKind) (int, int) {
	bucket, ok := l.opts.Buckets[kind]
	if !ok || venue != l.opts.Venue {
		return 0, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.used(kind, bucket), bucket.Capacity
}

// used is how much of a bucket is spent right now: the distance the arrival
// time has been pushed past the present, in whole units. Callers hold the mutex.
func (l *LocalLimiter) used(kind LimitKind, bucket Bucket) int {
	ahead := l.tat[kind].Sub(l.now())
	if ahead <= 0 {
		return 0
	}
	interval := bucket.interval()
	// Round up: a partly-spent unit is spent.
	n := int((ahead + interval - 1) / interval)
	return min(n, bucket.Capacity)
}

// deny counts a refusal. It is a counter rather than a log line per refusal
// because a venue we are backing off from produces a lot of them, and the fact
// is one fact.
func (l *LocalLimiter) deny(kind LimitKind) {
	if l.opts.Metrics != nil {
		l.opts.Metrics.RateLimitDenied.WithLabelValues(l.opts.Venue, kind.String()).Inc()
	}
	l.opts.Log.Warn("rate limit: refusing the operation", "kind", kind.String())
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
func sleep(ctx context.Context, d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-t.C:
		return true
	}
}

// ---------- advisory publication ----------

// Snapshot is every budgeted kind's current usage, in Kinds order.
func (l *LocalLimiter) Snapshot() []*pb.RateLimitBudget {
	out := make([]*pb.RateLimitBudget, 0, len(l.opts.Buckets))
	for _, kind := range Kinds {
		bucket, ok := l.opts.Buckets[kind]
		if !ok {
			continue
		}
		used, capacity := l.Used(l.opts.Venue, kind)
		out = append(out, &pb.RateLimitBudget{
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
func (l *LocalLimiter) report(ctx context.Context) {
	budgets := l.Snapshot()

	if l.opts.Metrics != nil {
		for _, b := range budgets {
			if b.Capacity > 0 {
				l.opts.Metrics.RateLimitUsed.WithLabelValues(l.opts.Venue, b.Kind).
					Set(float64(b.Used) / float64(b.Capacity))
			}
		}
	}
	pub := l.publisher()
	if pub == nil || len(budgets) == 0 {
		return
	}

	// A bucket at capacity is not an error — it is the limiter working — but a
	// consumer reading this key wants to know it is happening.
	status := pb.Status_STATUS_HEALTHY
	reason := ""
	for _, b := range budgets {
		if b.Used >= b.Capacity {
			status, reason = pb.Status_STATUS_DEGRADED, b.Kind+" budget exhausted"
			break
		}
	}

	msg := &pb.RateLimit{
		Env: &pb.Envelope{
			Venue:      l.opts.Venue,
			Channel:    pb.Channel_CHANNEL_RATELIMIT,
			RecvTimeNs: l.now().UnixNano(),
			// No source: nothing here came from the venue. It is what this
			// process has spent, not what the venue told us it has.
			Status:       status,
			StatusReason: reason,
		},
		Budgets: budgets,
	}
	// A failed write is not worth reacting to: the key expiring is itself the
	// signal that nobody is maintaining it.
	_ = pub.Publish(ctx, publish.VenueKey(l.opts.Venue, publish.SubjectRateLimit), msg, l.ttl())
}

// ttl is twice the longest window, so the key outlives a quiet period without
// outliving the process that writes it.
func (l *LocalLimiter) ttl() time.Duration {
	var longest time.Duration
	for _, b := range l.opts.Buckets {
		longest = max(longest, b.Window)
	}
	return longest * 2
}

// KindNames is every budgeted kind's name, sorted, for logs at startup.
func (l *LocalLimiter) KindNames() []string {
	out := make([]string, 0, len(l.opts.Buckets))
	for kind := range l.opts.Buckets {
		out = append(out, kind.String())
	}
	sort.Strings(out)
	return out
}
