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

	mutex    sync.Mutex
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
func NewBreaker(options BreakerOptions) *Breaker {
	if options.Now == nil {
		options.Now = time.Now
	}
	return &Breaker{
		threshold:    options.ConsecutiveFailures,
		openDuration: options.OpenDuration,
		now:          options.Now,
	}
}

// Retry reports how long to wait before the next connection attempt. Zero means
// one may be made now.
//
// Calling it is what hands out the post-expiry probe, so call it once per
// attempt and act on the answer.
func (breaker *Breaker) Retry() time.Duration {
	breaker.mutex.Lock()
	defer breaker.mutex.Unlock()

	if breaker.openedAt.IsZero() {
		return 0
	}
	if duration := breaker.openDuration - breaker.now().Sub(breaker.openedAt); duration > 0 {
		return duration
	}
	breaker.openedAt = time.Time{}
	breaker.probing = true
	return 0
}

// Fail records a failed attempt, opening the breaker at the threshold and
// reopening it when the probe was the thing that failed.
func (breaker *Breaker) Fail() {
	breaker.mutex.Lock()
	defer breaker.mutex.Unlock()

	breaker.failures++
	if !breaker.enabled() {
		return
	}
	if breaker.probing || breaker.failures >= breaker.threshold {
		breaker.probing = false
		breaker.openedAt = breaker.now()
	}
}

// Succeed records a working connection and closes the breaker.
func (breaker *Breaker) Succeed() {
	breaker.mutex.Lock()
	defer breaker.mutex.Unlock()

	breaker.failures = 0
	breaker.probing = false
	breaker.openedAt = time.Time{}
}

// Open reports whether the breaker is currently refusing attempts. It is the
// status reason a stream on the affected socket publishes.
func (breaker *Breaker) Open() bool {
	breaker.mutex.Lock()
	defer breaker.mutex.Unlock()
	return !breaker.openedAt.IsZero() && breaker.now().Sub(breaker.openedAt) < breaker.openDuration
}

// Failures is the consecutive failure count, for logs.
func (breaker *Breaker) Failures() int {
	breaker.mutex.Lock()
	defer breaker.mutex.Unlock()
	return breaker.failures
}

func (breaker *Breaker) enabled() bool { return breaker.threshold >= 1 && breaker.openDuration > 0 }
