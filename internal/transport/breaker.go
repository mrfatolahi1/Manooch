package transport

import (
	"sync"
	"time"
)

// BreakerOptions configures a Breaker.
type BreakerOptions struct {
	// ConsecutiveFailures is how many failures in a row open the breaker.
	ConsecutiveFailures int
	// OpenDuration is how long it stays open. One attempt is allowed when it
	// elapses; failing that attempt reopens for the full duration again.
	OpenDuration time.Duration
	// Now is swappable for tests. Zero means time.Now.
	Now func() time.Time
}

// A Breaker stops connection attempts entirely after repeated failure.
//
// While open, no attempt is made at all — not a slow one, not a probe. Backoff
// alone still dials a venue that is rate-limiting us every minute forever; the
// breaker is what makes the client stop knocking, which is what a venue is
// asking for when it starts refusing.
//
// One supervisor owns one Breaker and calls it from that goroutine. The mutex
// is there because Open and Failures are read by the health tracker.
type Breaker struct {
	threshold    int
	openDuration time.Duration
	now          func() time.Time

	mu       sync.Mutex
	failures int
	openedAt time.Time
	// probing is the single attempt handed out when the open period elapses.
	// Its failure reopens for the full duration rather than for one more
	// interval of backoff, so a venue that is still refusing is left alone.
	probing bool
}

// NewBreaker builds a breaker. A threshold below 1 or a non-positive duration
// disables it: it then allows every attempt, which is what a config with the
// section removed should mean.
func NewBreaker(opts BreakerOptions) *Breaker {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	return &Breaker{
		threshold:    opts.ConsecutiveFailures,
		openDuration: opts.OpenDuration,
		now:          opts.Now,
	}
}

// Retry reports how long to wait before the next connection attempt. Zero means
// one may be made now.
//
// Calling it is what hands out the post-expiry probe, so call it once per
// attempt and act on the answer.
func (b *Breaker) Retry() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.openedAt.IsZero() {
		return 0
	}
	if d := b.openDuration - b.now().Sub(b.openedAt); d > 0 {
		return d
	}
	b.openedAt = time.Time{}
	b.probing = true
	return 0
}

// Fail records a failed attempt, opening the breaker at the threshold and
// reopening it when the probe was the thing that failed.
func (b *Breaker) Fail() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.failures++
	if !b.enabled() {
		return
	}
	if b.probing || b.failures >= b.threshold {
		b.probing = false
		b.openedAt = b.now()
	}
}

// Succeed records a working connection and closes the breaker.
func (b *Breaker) Succeed() {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.failures = 0
	b.probing = false
	b.openedAt = time.Time{}
}

// Open reports whether the breaker is currently refusing attempts. It is the
// status reason a stream on the affected socket publishes.
func (b *Breaker) Open() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return !b.openedAt.IsZero() && b.now().Sub(b.openedAt) < b.openDuration
}

// Failures is the consecutive failure count, for logs.
func (b *Breaker) Failures() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.failures
}

func (b *Breaker) enabled() bool { return b.threshold >= 1 && b.openDuration > 0 }
