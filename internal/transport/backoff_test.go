package transport_test

import (
	"context"
	"math"
	"testing"
	"time"

	"github.com/you/manooch/internal/transport"
)

func policy() transport.Policy {
	return transport.Policy{
		Initial:    500 * time.Millisecond,
		Max:        60 * time.Second,
		Multiplier: 2,
		Jitter:     transport.JitterFull,
	}
}

// TestDelayGrowsAndCaps: the ceiling doubles per attempt and stops at Max.
func TestDelayGrowsAndCaps(t *testing.T) {
	p := policy()
	p.Jitter = transport.JitterNone

	for attempt, want := range map[int]time.Duration{
		0: 500 * time.Millisecond,
		1: time.Second,
		2: 2 * time.Second,
		7: 64 * time.Second, // past Max
	} {
		got := p.Delay(attempt)
		if want > p.Max {
			want = p.Max
		}
		if got != want {
			t.Errorf("Delay(%d) = %v, want %v", attempt, got, want)
		}
	}
}

// TestDelayNeverOverflows covers a socket that has been failing long enough for
// initial × 2^attempt to wrap int64 nanoseconds. A negative delay is a retry
// with no wait, which is the storm the whole policy exists to prevent.
func TestDelayNeverOverflows(t *testing.T) {
	p := policy()
	p.Jitter = transport.JitterNone

	for _, attempt := range []int{63, 64, 1000, math.MaxInt32} {
		if got := p.Delay(attempt); got != p.Max {
			t.Errorf("Delay(%d) = %v, want %v", attempt, got, p.Max)
		}
	}
}

// TestFullJitterStaysInRange: every delay is in [0, ceiling].
func TestFullJitterStaysInRange(t *testing.T) {
	p := policy()
	ceiling := 2 * time.Second // attempt 2

	for range 10_000 {
		d := p.Delay(2)
		if d < 0 || d > ceiling {
			t.Fatalf("Delay(2) = %v, want within [0, %v]", d, ceiling)
		}
	}
}

// TestFullJitterSpreadsConcurrentFailures is the property the mode exists for.
// Deterministic backoff makes every stream that failed on the same frame retry
// on the same frame, which is a reconnect storm and the usual way an IP gets
// banned.
func TestFullJitterSpreadsConcurrentFailures(t *testing.T) {
	p := policy()

	const streams = 200
	seen := make(map[time.Duration]int, streams)
	for range streams {
		seen[p.Delay(4)]++
	}
	// Well under 200 to leave room for coincidence, but far above the 1 that
	// no jitter or a shared seed would produce.
	if len(seen) < streams/2 {
		t.Errorf("%d distinct delays across %d concurrent failures; jitter is not being applied", len(seen), streams)
	}

	// And they actually spread across the interval rather than clustering.
	var lo, hi time.Duration = time.Hour, 0
	for d := range seen {
		lo, hi = min(lo, d), max(hi, d)
	}
	if ceiling := 8 * time.Second; hi-lo < ceiling/2 {
		t.Errorf("delays span only %v of a %v interval", hi-lo, ceiling)
	}
}

// TestEqualJitterKeepsHalf: the mode is offered because config allows it, so
// its bound is asserted rather than assumed.
func TestEqualJitterKeepsHalf(t *testing.T) {
	p := policy()
	p.Jitter = transport.JitterEqual
	ceiling := 2 * time.Second

	for range 1000 {
		d := p.Delay(2)
		if d < ceiling/2 || d > ceiling {
			t.Fatalf("Delay(2) = %v, want within [%v, %v]", d, ceiling/2, ceiling)
		}
	}
}

// TestUnknownJitterIsFull: an unset or misspelt mode must not silently become
// no jitter, which is the one setting that causes the failure being avoided.
func TestUnknownJitterIsFull(t *testing.T) {
	p := policy()
	p.Jitter = ""
	p.Rand = func() float64 { return 0.25 }

	if got, want := p.Delay(1), 250*time.Millisecond; got != want {
		t.Errorf("Delay(1) with no jitter mode = %v, want %v (full jitter)", got, want)
	}
}

// TestZeroPolicyDoesNotSleep: a Policy nobody configured must retry immediately
// rather than block forever on a zero Initial.
func TestZeroPolicyDoesNotSleep(t *testing.T) {
	var p transport.Policy
	if got := p.Delay(5); got != 0 {
		t.Errorf("Delay(5) on a zero policy = %v, want 0", got)
	}
}

// TestWaitReturnsOnCancel: a supervision loop that used time.Sleep would hang
// shutdown for the length of the backoff.
func TestWaitReturnsOnCancel(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan bool, 1)
	go func() { done <- transport.Wait(ctx, time.Minute) }()

	time.Sleep(10 * time.Millisecond)
	cancel()

	select {
	case ok := <-done:
		if ok {
			t.Error("Wait reported the sleep completed after cancellation")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Wait did not return after its context was cancelled")
	}
}

// TestWaitOnCancelledContext: no retry may start once shutdown has begun.
func TestWaitOnCancelledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if transport.Wait(ctx, 0) {
		t.Error("Wait reported success on an already-cancelled context")
	}
}
