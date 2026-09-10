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
	mutex sync.Mutex
	t     time.Time
}

func newClock() *clock { return &clock{t: time.Unix(1_700_000_000, 0)} }

func (clock *clock) Now() time.Time {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	return clock.t
}

func (clock *clock) advance(duration time.Duration) {
	clock.mutex.Lock()
	defer clock.mutex.Unlock()
	clock.t = clock.t.Add(duration)
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
	clock := newClock()
	breaker := newBreaker(clock.Now)

	for i := 1; i <= 9; i++ {
		breaker.Fail()
		if duration := breaker.Retry(); duration != 0 {
			t.Fatalf("after %d failures Retry = %v, want 0", i, duration)
		}
	}

	breaker.Fail()
	if !breaker.Open() {
		t.Fatal("breaker is not open after 10 consecutive failures")
	}
	if duration := breaker.Retry(); duration != 5*time.Minute {
		t.Errorf("Retry = %v, want the full open duration", duration)
	}
}

// TestBreakerMakesNoAttemptWhileOpen is the acceptance criterion: not a slow
// attempt, not a probe, none at all.
func TestBreakerMakesNoAttemptWhileOpen(t *testing.T) {
	clock := newClock()
	breaker := newBreaker(clock.Now)
	for range 10 {
		breaker.Fail()
	}

	var elapsed time.Duration
	for _, duration := range []time.Duration{0, time.Second, time.Minute - time.Second, 3*time.Minute + 59*time.Second} {
		clock.advance(duration)
		elapsed += duration
		if wait := breaker.Retry(); wait <= 0 {
			t.Fatalf("%v into the open period Retry = %v, want a positive wait", elapsed, wait)
		}
	}
}

// TestBreakerAllowsOneAttemptOnExpiry: exactly one, and its failure reopens for
// the full duration rather than for one more backoff interval.
func TestBreakerAllowsOneAttemptOnExpiry(t *testing.T) {
	clock := newClock()
	breaker := newBreaker(clock.Now)
	for range 10 {
		breaker.Fail()
	}
	clock.advance(5 * time.Minute)

	if duration := breaker.Retry(); duration != 0 {
		t.Fatalf("Retry after the open period = %v, want 0", duration)
	}

	breaker.Fail() // the probe failed
	if !breaker.Open() {
		t.Fatal("breaker did not reopen after the probe failed")
	}
	if duration := breaker.Retry(); duration != 5*time.Minute {
		t.Errorf("Retry = %v, want the full open duration again", duration)
	}
}

// TestBreakerClosesOnSuccess: a working connection resets everything, so the
// next outage starts its count from zero.
func TestBreakerClosesOnSuccess(t *testing.T) {
	clock := newClock()
	breaker := newBreaker(clock.Now)
	for range 10 {
		breaker.Fail()
	}
	clock.advance(5 * time.Minute)

	if duration := breaker.Retry(); duration != 0 {
		t.Fatalf("Retry = %v, want the probe", duration)
	}
	breaker.Succeed()

	if breaker.Open() {
		t.Error("breaker still open after a successful connection")
	}
	if got := breaker.Failures(); got != 0 {
		t.Errorf("Failures = %d after success, want 0", got)
	}
	for range 9 {
		breaker.Fail()
	}
	if breaker.Open() {
		t.Error("breaker opened on 9 failures; the count was not reset by Succeed")
	}
}

// TestBreakerCountsConsecutively: a success between failures resets the run, so
// a socket that flaps once an hour never opens the breaker.
func TestBreakerCountsConsecutively(t *testing.T) {
	clock := newClock()
	breaker := newBreaker(clock.Now)

	for range 100 {
		for range 9 {
			breaker.Fail()
		}
		breaker.Succeed()
	}
	if breaker.Open() {
		t.Error("breaker opened on failures that were never consecutive")
	}
}

// TestBreakerDisabled: a zero threshold or duration must allow every attempt
// rather than open on the first failure.
func TestBreakerDisabled(t *testing.T) {
	breaker := transport.NewBreaker(transport.BreakerOptions{})
	for range 100 {
		breaker.Fail()
		if duration := breaker.Retry(); duration != 0 {
			t.Fatalf("disabled breaker returned Retry = %v", duration)
		}
	}
	if breaker.Open() {
		t.Error("disabled breaker reports open")
	}
}
