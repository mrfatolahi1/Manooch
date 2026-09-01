package ratelimit_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/ratelimit"
	"google.golang.org/protobuf/proto"
)

const venue = "KUCOIN"

// clock is a manual clock: every wait in these tests is a decision the limiter
// made, not a duration the test sat through.
type clock struct {
	mu   sync.Mutex
	now  time.Time
	naps []time.Duration
}

func newClock() *clock { return &clock{now: time.Unix(1_700_000_000, 0)} }

func (c *clock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *clock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(d)
}

// sleep records the wait and moves the clock over it, so a caller that was
// asked to wait resumes in the state it was told to wait for.
func (c *clock) sleep(ctx context.Context, d time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	c.mu.Lock()
	c.naps = append(c.naps, d)
	c.now = c.now.Add(d)
	c.mu.Unlock()
	return true
}

func (c *clock) waits() []time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]time.Duration(nil), c.naps...)
}

// recorder captures the advisory publication instead of dialing Redis.
type recorder struct {
	mu   sync.Mutex
	last *pb.RateLimit
	key  string
	ttl  time.Duration
	n    int
}

func (r *recorder) Publish(_ context.Context, key string, msg proto.Message, ttl time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.key, r.ttl, r.n = key, ttl, r.n+1
	r.last = proto.Clone(msg).(*pb.RateLimit)
	return nil
}

func (r *recorder) Close() error { return nil }

func (r *recorder) snapshot() (*pb.RateLimit, string, time.Duration, int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last, r.key, r.ttl, r.n
}

func newLimiter(t *testing.T, c *clock, pub *recorder, buckets map[ratelimit.LimitKind]ratelimit.Bucket) *ratelimit.LocalLimiter {
	t.Helper()

	opts := ratelimit.Options{
		Venue:   venue,
		Buckets: buckets,
		Log:     slog.New(slog.NewTextHandler(io.Discard, nil)),
		Now:     c.Now,
		Sleep:   c.sleep,
	}
	if pub != nil {
		opts.Publisher = pub
	}
	l, err := ratelimit.New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return l
}

// TestBurstThenRefill: a cold bucket serves a whole capacity at once — that is
// what makes a reconnect of every socket possible — and the next call waits
// exactly one emission interval.
func TestBurstThenRefill(t *testing.T) {
	c := newClock()
	l := newLimiter(t, c, nil, map[ratelimit.LimitKind]ratelimit.Bucket{
		ratelimit.LimitWSConnect: {Capacity: 10, Window: time.Minute},
	})
	ctx := context.Background()

	for i := range 10 {
		if err := l.Allow(ctx, venue, ratelimit.LimitWSConnect, 1); err != nil {
			t.Fatalf("connect %d: %v", i, err)
		}
	}
	if got := c.waits(); len(got) != 0 {
		t.Errorf("a cold bucket made the caller wait: %v", got)
	}
	if used, capacity := l.Used(venue, ratelimit.LimitWSConnect); used != 10 || capacity != 10 {
		t.Errorf("used = %d/%d, want 10/10", used, capacity)
	}

	if err := l.Allow(ctx, venue, ratelimit.LimitWSConnect, 1); err != nil {
		t.Fatalf("the eleventh connect: %v", err)
	}
	got := c.waits()
	if len(got) != 1 || got[0] != 6*time.Second {
		t.Errorf("waits = %v, want one wait of 6s (a minute over ten)", got)
	}
}

// TestDeniesRatherThanWaitingPastTheDeadline is the whole point of the
// interface: a caller with two seconds does not sit for thirty and then make
// the call anyway.
func TestDeniesRatherThanWaitingPastTheDeadline(t *testing.T) {
	c := newClock()
	l := newLimiter(t, c, nil, map[ratelimit.LimitKind]ratelimit.Bucket{
		ratelimit.LimitRESTWeight: {Capacity: 2, Window: time.Minute},
	})

	ctx, cancel := context.WithDeadline(context.Background(), c.Now().Add(2*time.Second))
	defer cancel()

	for i := range 2 {
		if err := l.Allow(ctx, venue, ratelimit.LimitRESTWeight, 1); err != nil {
			t.Fatalf("call %d: %v", i, err)
		}
	}

	err := l.Allow(ctx, venue, ratelimit.LimitRESTWeight, 1)
	if !errors.Is(err, ratelimit.ErrBudgetExhausted) {
		t.Fatalf("Allow = %v, want ErrBudgetExhausted", err)
	}
	if got := c.waits(); len(got) != 0 {
		t.Errorf("the limiter waited before refusing: %v", got)
	}

	// A refusal must not spend budget: the operation did not happen.
	c.advance(30 * time.Second)
	if err := l.Allow(context.Background(), venue, ratelimit.LimitRESTWeight, 1); err != nil {
		t.Errorf("after a refusal and a refill: %v", err)
	}
}

// TestCostLargerThanTheBucketIsRefusedImmediately: a wait that can never end is
// not a wait.
func TestCostLargerThanTheBucketIsRefusedImmediately(t *testing.T) {
	c := newClock()
	l := newLimiter(t, c, nil, map[ratelimit.LimitKind]ratelimit.Bucket{
		ratelimit.LimitRESTWeight: {Capacity: 5, Window: time.Minute},
	})

	err := l.Allow(context.Background(), venue, ratelimit.LimitRESTWeight, 6)
	if !errors.Is(err, ratelimit.ErrBudgetExhausted) {
		t.Fatalf("Allow = %v, want ErrBudgetExhausted", err)
	}
	if used, _ := l.Used(venue, ratelimit.LimitRESTWeight); used != 0 {
		t.Errorf("a refused call spent %d units", used)
	}
}

// TestKindsAreIndependent: spending REST weight must never stop a reconnect.
// The venue counts them separately and so do we.
func TestKindsAreIndependent(t *testing.T) {
	c := newClock()
	l := newLimiter(t, c, nil, map[ratelimit.LimitKind]ratelimit.Bucket{
		ratelimit.LimitRESTWeight: {Capacity: 1, Window: time.Minute},
		ratelimit.LimitWSConnect:  {Capacity: 1, Window: time.Minute},
	})
	ctx := context.Background()

	if err := l.Allow(ctx, venue, ratelimit.LimitRESTWeight, 1); err != nil {
		t.Fatal(err)
	}
	if err := l.Allow(ctx, venue, ratelimit.LimitWSConnect, 1); err != nil {
		t.Errorf("a spent REST budget blocked a websocket connect: %v", err)
	}
	if got := c.waits(); len(got) != 0 {
		t.Errorf("waits = %v", got)
	}
}

// TestUnbudgetedKindIsAllowed: nothing is invented for a limit the venue does
// not publish, and Used says so with a zero capacity.
func TestUnbudgetedKindIsAllowed(t *testing.T) {
	c := newClock()
	l := newLimiter(t, c, nil, map[ratelimit.LimitKind]ratelimit.Bucket{
		ratelimit.LimitRESTWeight: {Capacity: 1, Window: time.Minute},
	})

	for range 100 {
		if err := l.Allow(context.Background(), venue, ratelimit.LimitSubscriptions, 10); err != nil {
			t.Fatalf("unbudgeted kind: %v", err)
		}
	}
	if used, capacity := l.Used(venue, ratelimit.LimitSubscriptions); used != 0 || capacity != 0 {
		t.Errorf("Used = %d/%d, want 0/0 for an unbudgeted kind", used, capacity)
	}
}

// TestFractionLeavesHeadroom: the limiter is blind to the order service, which
// shares the IP, so it enforces a share of the published limit.
func TestFractionLeavesHeadroom(t *testing.T) {
	b := ratelimit.Bucket{Capacity: 6000, Window: time.Minute}.Fraction(0.5)
	if b.Capacity != 3000 {
		t.Errorf("capacity = %d, want 3000", b.Capacity)
	}
	if got := (ratelimit.Bucket{Capacity: 1, Window: time.Minute}).Fraction(0.01); got.Capacity != 1 {
		t.Errorf("capacity = %d, want at least 1", got.Capacity)
	}
}

// TestAdvisoryPublication: the order service may read this key and nothing
// breaks if it never does, but what is there has to be right.
func TestAdvisoryPublication(t *testing.T) {
	c := newClock()
	pub := &recorder{}
	l := newLimiter(t, c, pub, map[ratelimit.LimitKind]ratelimit.Bucket{
		ratelimit.LimitRESTWeight: {Capacity: 4, Window: time.Minute},
		ratelimit.LimitWSConnect:  {Capacity: 2, Window: 5 * time.Minute},
	})

	if err := l.Allow(context.Background(), venue, ratelimit.LimitRESTWeight, 2); err != nil {
		t.Fatal(err)
	}

	msg, key, ttl, n := pub.snapshot()
	if n != 1 {
		t.Errorf("published %d times, want 1", n)
	}
	if key != "Manooch:KUCOIN:venue:ratelimit" {
		t.Errorf("key = %q", key)
	}
	if ttl != 10*time.Minute {
		t.Errorf("ttl = %v, want twice the longest window", ttl)
	}
	if msg.Env.Status != pb.Status_STATUS_HEALTHY {
		t.Errorf("status = %v, want HEALTHY below capacity", msg.Env.Status)
	}
	if len(msg.Budgets) != 2 {
		t.Fatalf("budgets = %d, want 2", len(msg.Budgets))
	}
	if got := msg.Budgets[0]; got.Kind != "rest_weight" || got.Used != 2 || got.Capacity != 4 || got.WindowMs != 60_000 {
		t.Errorf("rest_weight budget = %+v", got)
	}
	if got := msg.Budgets[1]; got.Kind != "ws_connect" || got.Used != 0 || got.Capacity != 2 {
		t.Errorf("ws_connect budget = %+v", got)
	}

	// At capacity the key says so, which is the state worth reading it for.
	if err := l.Allow(context.Background(), venue, ratelimit.LimitRESTWeight, 2); err != nil {
		t.Fatal(err)
	}
	msg, _, _, _ = pub.snapshot()
	if msg.Env.Status != pb.Status_STATUS_DEGRADED {
		t.Errorf("status = %v at capacity, want DEGRADED", msg.Env.Status)
	}
	if msg.Env.StatusReason == "" {
		t.Error("no status_reason at capacity")
	}
}

// TestUsedDecaysWithTime: budget comes back on its own, without a refill
// goroutine and without a window boundary for a burst to straddle.
func TestUsedDecaysWithTime(t *testing.T) {
	c := newClock()
	l := newLimiter(t, c, nil, map[ratelimit.LimitKind]ratelimit.Bucket{
		ratelimit.LimitRESTWeight: {Capacity: 60, Window: time.Minute},
	})

	for range 60 {
		if err := l.Allow(context.Background(), venue, ratelimit.LimitRESTWeight, 1); err != nil {
			t.Fatal(err)
		}
	}
	if used, _ := l.Used(venue, ratelimit.LimitRESTWeight); used != 60 {
		t.Fatalf("used = %d, want 60", used)
	}

	c.advance(30 * time.Second)
	if used, _ := l.Used(venue, ratelimit.LimitRESTWeight); used != 30 {
		t.Errorf("used after 30s = %d, want 30", used)
	}
	c.advance(30 * time.Second)
	if used, _ := l.Used(venue, ratelimit.LimitRESTWeight); used != 0 {
		t.Errorf("used after a full window = %d, want 0", used)
	}
}

// TestRejectsAnotherVenue: one process serves one venue, so a call naming
// another is a bug rather than a budget to open.
func TestRejectsAnotherVenue(t *testing.T) {
	c := newClock()
	l := newLimiter(t, c, nil, map[ratelimit.LimitKind]ratelimit.Bucket{
		ratelimit.LimitRESTWeight: {Capacity: 1, Window: time.Minute},
	})
	if err := l.Allow(context.Background(), "BINANCE", ratelimit.LimitRESTWeight, 1); err == nil {
		t.Error("Allow accepted a venue this limiter does not serve")
	}
}

// TestUnlimitedPermitsEverything: what an adapter falls back to in a test, so
// it must never be the thing that fails one.
func TestUnlimitedPermitsEverything(t *testing.T) {
	var l ratelimit.Limiter = ratelimit.Unlimited{}
	if err := l.Allow(context.Background(), venue, ratelimit.LimitRESTWeight, 1_000_000); err != nil {
		t.Errorf("Unlimited.Allow: %v", err)
	}
	if used, capacity := l.Used(venue, ratelimit.LimitRESTWeight); used != 0 || capacity != 0 {
		t.Errorf("Used = %d/%d, want 0/0", used, capacity)
	}
}

func TestNewRejectsUnenforceableBuckets(t *testing.T) {
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	cases := map[string]ratelimit.Bucket{
		"zero capacity":     {Capacity: 0, Window: time.Minute},
		"negative capacity": {Capacity: -1, Window: time.Minute},
		"zero window":       {Capacity: 10, Window: 0},
		"window too short":  {Capacity: 1_000_000_000_000, Window: time.Nanosecond},
	}
	for name, b := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := ratelimit.New(ratelimit.Options{
				Venue:   venue,
				Log:     log,
				Buckets: map[ratelimit.LimitKind]ratelimit.Bucket{ratelimit.LimitRESTWeight: b},
			})
			if err == nil {
				t.Errorf("New accepted %+v", b)
			}
		})
	}
	if _, err := ratelimit.New(ratelimit.Options{Log: log}); err == nil {
		t.Error("New accepted an empty venue")
	}
}

func TestKindNames(t *testing.T) {
	for kind, want := range map[ratelimit.LimitKind]string{
		ratelimit.LimitRESTWeight:    "rest_weight",
		ratelimit.LimitWSConnect:     "ws_connect",
		ratelimit.LimitSubscriptions: "subscriptions",
		ratelimit.LimitKind(99):      "unspecified",
	} {
		if got := kind.String(); got != want {
			t.Errorf("String() = %q, want %q", got, want)
		}
	}
}
