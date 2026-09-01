package transport_test

import (
	"sync"
	"testing"
	"time"

	"github.com/you/manooch/internal/transport"
)

// clock is a hand-wound time source, so the tests below assert the breaker's
// arithmetic rather than the scheduler's.
type clock struct {
	mu sync.Mutex
	t  time.Time
}

func newClock() *clock { return &clock{t: time.Unix(1_700_000_000, 0)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

func newBreaker(now func() time.Time) *transport.Breaker {
	return transport.NewBreaker(transport.BreakerOptions{
		ConsecutiveFailures: 10,
		OpenDuration:        5 * time.Minute,
		Now:                 now,
	})
}

// TestBreakerOpensAtThreshold: nine failures still allow an attempt, the tenth
// does not.
func TestBreakerOpensAtThreshold(t *testing.T) {
	c := newClock()
	b := newBreaker(c.Now)

	for i := 1; i <= 9; i++ {
		b.Fail()
		if d := b.Retry(); d != 0 {
			t.Fatalf("after %d failures Retry = %v, want 0", i, d)
		}
	}

	b.Fail()
	if !b.Open() {
		t.Fatal("breaker is not open after 10 consecutive failures")
	}
	if d := b.Retry(); d != 5*time.Minute {
		t.Errorf("Retry = %v, want the full open duration", d)
	}
}

// TestBreakerMakesNoAttemptWhileOpen is the acceptance criterion: not a slow
// attempt, not a probe, none at all.
func TestBreakerMakesNoAttemptWhileOpen(t *testing.T) {
	c := newClock()
	b := newBreaker(c.Now)
	for range 10 {
		b.Fail()
	}

	var elapsed time.Duration
	for _, step := range []time.Duration{0, time.Second, time.Minute - time.Second, 3*time.Minute + 59*time.Second} {
		c.advance(step)
		elapsed += step
		if d := b.Retry(); d <= 0 {
			t.Fatalf("%v into the open period Retry = %v, want a positive wait", elapsed, d)
		}
	}
}

// TestBreakerAllowsOneAttemptOnExpiry: exactly one, and its failure reopens for
// the full duration rather than for one more backoff interval.
func TestBreakerAllowsOneAttemptOnExpiry(t *testing.T) {
	c := newClock()
	b := newBreaker(c.Now)
	for range 10 {
		b.Fail()
	}
	c.advance(5 * time.Minute)

	if d := b.Retry(); d != 0 {
		t.Fatalf("Retry after the open period = %v, want 0", d)
	}

	b.Fail() // the probe failed
	if !b.Open() {
		t.Fatal("breaker did not reopen after the probe failed")
	}
	if d := b.Retry(); d != 5*time.Minute {
		t.Errorf("Retry = %v, want the full open duration again", d)
	}
}

// TestBreakerClosesOnSuccess: a working connection resets everything, so the
// next outage starts its count from zero.
func TestBreakerClosesOnSuccess(t *testing.T) {
	c := newClock()
	b := newBreaker(c.Now)
	for range 10 {
		b.Fail()
	}
	c.advance(5 * time.Minute)

	if d := b.Retry(); d != 0 {
		t.Fatalf("Retry = %v, want the probe", d)
	}
	b.Succeed()

	if b.Open() {
		t.Error("breaker still open after a successful connection")
	}
	if got := b.Failures(); got != 0 {
		t.Errorf("Failures = %d after success, want 0", got)
	}
	for range 9 {
		b.Fail()
	}
	if b.Open() {
		t.Error("breaker opened on 9 failures; the count was not reset by Succeed")
	}
}

// TestBreakerCountsConsecutively: a success between failures resets the run, so
// a socket that flaps once an hour never opens the breaker.
func TestBreakerCountsConsecutively(t *testing.T) {
	c := newClock()
	b := newBreaker(c.Now)

	for range 100 {
		for range 9 {
			b.Fail()
		}
		b.Succeed()
	}
	if b.Open() {
		t.Error("breaker opened on failures that were never consecutive")
	}
}

// TestBreakerDisabled: a zero threshold or duration must allow every attempt
// rather than open on the first failure.
func TestBreakerDisabled(t *testing.T) {
	b := transport.NewBreaker(transport.BreakerOptions{})
	for range 100 {
		b.Fail()
		if d := b.Retry(); d != 0 {
			t.Fatalf("disabled breaker returned Retry = %v", d)
		}
	}
	if b.Open() {
		t.Error("disabled breaker reports open")
	}
}
