package transport

import (
	"context"
	"math"
	"math/rand/v2"
	"time"
)

// Jitter modes, named as they are written in a supervisor backoff block.
const (
	// JitterNone is the raw exponential ceiling. Only ever right in a test:
	// several streams failing at once retry in lockstep.
	JitterNone = "none"
	// JitterFull is random(0, ceiling), the default and the only mode the
	// service configures.
	JitterFull = "full"
	// JitterEqual is ceiling/2 + random(0, ceiling/2). Half the spread of
	// full, which is half the storm protection.
	JitterEqual = "equal"
)

// A Policy is exponential backoff between retries.
//
// This matters more than any rate limiter. A venue ban is almost always earned
// by a reconnect storm rather than by steady-state polling, and a storm is what
// several streams failing at the same instant become when they all wait the
// same deterministic interval and retry together.
type Policy struct {
	// Initial is the ceiling for attempt 0.
	Initial time.Duration
	// Max caps the ceiling however many attempts have failed.
	Max time.Duration
	// Multiplier is the growth per attempt, normally 2.
	Multiplier float64
	// Jitter is JitterNone, JitterFull or JitterEqual. Anything else, empty
	// included, is treated as JitterFull: the safe direction to guess in.
	Jitter string

	// Rand returns a float in [0,1). Zero means rand.Float64. A test
	// substitutes to make one delay predictable; nothing else should.
	Rand func() float64
}

// Delay is how long to wait before the retry after attempt failures, counting
// from zero: Delay(0) is the wait after the first failure.
func (p Policy) Delay(attempt int) time.Duration {
	ceiling := p.ceiling(attempt)
	if ceiling <= 0 {
		return 0
	}
	switch p.Jitter {
	case JitterNone:
		return ceiling
	case JitterEqual:
		half := ceiling / 2
		return half + p.jitter(half)
	default:
		// Full jitter. The whole interval is in play, so two streams that
		// failed on the same frame do not come back on the same frame either.
		return p.jitter(ceiling)
	}
}

// ceiling is initial × multiplier^attempt, capped at Max.
//
// The arithmetic is in float64 and clamped rather than done in time.Duration:
// a socket that has been failing for a day reaches an attempt count where
// int64 nanoseconds overflow into a negative delay, which is a retry with no
// wait at all — the storm this exists to prevent.
func (p Policy) ceiling(attempt int) time.Duration {
	if p.Initial <= 0 {
		return 0
	}
	if attempt < 0 {
		attempt = 0
	}
	mult := p.Multiplier
	if mult < 1 {
		mult = 1
	}
	max := float64(p.Max)
	if max <= 0 {
		max = math.MaxFloat64
	}

	c := float64(p.Initial) * math.Pow(mult, float64(attempt))
	if math.IsNaN(c) || c > max {
		c = max
	}
	if c > float64(math.MaxInt64) {
		c = float64(math.MaxInt64)
	}
	return time.Duration(c)
}

// jitter returns a uniform duration in [0, d).
func (p Policy) jitter(d time.Duration) time.Duration {
	if d <= 0 {
		return 0
	}
	f := p.Rand
	if f == nil {
		f = rand.Float64
	}
	return time.Duration(f() * float64(d))
}

// Sleep waits for Delay(attempt) or until ctx ends, reporting whether the wait
// completed. A false return means the caller is shutting down and must not
// retry.
func (p Policy) Sleep(ctx context.Context, attempt int) bool {
	return Wait(ctx, p.Delay(attempt))
}

// Wait sleeps for d or until ctx ends, reporting whether the wait completed.
//
// It is here rather than inline at each call site because time.Sleep in a
// supervision loop is a shutdown that hangs for the length of the backoff.
func Wait(ctx context.Context, d time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	if d <= 0 {
		return true
	}
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-ctx.Done():
		return false
	}
}
