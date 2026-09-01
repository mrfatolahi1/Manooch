package supervisor_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/core/coretest"
	"github.com/you/manooch/internal/health"
	"github.com/you/manooch/internal/obs"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/internal/supervisor"
	"github.com/you/manooch/internal/transport"
	"google.golang.org/protobuf/proto"
)

const (
	settle      = 5 * time.Second
	leakTimeout = 100 * time.Millisecond
)

// recorder counts publishes per key.
type recorder struct {
	mu     sync.Mutex
	counts map[string]int
	last   map[string]*pb.Envelope
	err    error
}

func newRecorder() *recorder {
	return &recorder{counts: map[string]int{}, last: map[string]*pb.Envelope{}}
}

func (r *recorder) Publish(_ context.Context, key string, msg proto.Message, _ time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.counts[key]++
	if e, ok := msg.(interface{ GetEnv() *pb.Envelope }); ok {
		r.last[key] = proto.Clone(e.GetEnv()).(*pb.Envelope)
	}
	return r.err
}

func (r *recorder) Close() error { return nil }

func (r *recorder) count(key string) int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.counts[key]
}

func (r *recorder) envelope(key string) *pb.Envelope {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.last[key]
}

func quiet() *slog.Logger {
	return slog.New(slog.NewJSONHandler(io.Discard, &slog.HandlerOptions{Level: slog.LevelError}))
}

func fastBackoff() transport.Policy {
	return transport.Policy{Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2, Jitter: transport.JitterFull}
}

// eventually polls until cond holds, which beats sleeping for a duration
// somebody guessed.
func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(settle)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// ---------- harness ----------

type harness struct {
	adapter *coretest.Adapter
	pub     *recorder
	tracker *health.Tracker
	proc    *supervisor.Process
	specs   []core.StreamSpec
	metrics *obs.Metrics

	mu    sync.Mutex
	conns []*coretest.Conn
	dial  func() (core.Conn, error)
}

func newHarness(t *testing.T, symbols ...string) *harness {
	t.Helper()
	if len(symbols) == 0 {
		symbols = []string{"BTC_USDT"}
	}

	specs, err := coretest.Specs(symbols...)
	if err != nil {
		t.Fatal(err)
	}

	h := &harness{pub: newRecorder(), specs: specs, metrics: obs.NewMetrics()}
	h.adapter = &coretest.Adapter{}
	h.adapter.DialFunc = func(context.Context, core.SocketPlan) (core.Conn, error) {
		h.mu.Lock()
		dial := h.dial
		h.mu.Unlock()
		if dial != nil {
			return dial()
		}
		return h.newConn(), nil
	}

	h.tracker, err = health.New(health.Options{
		Venue:               coretest.Venue,
		Publisher:           h.pub,
		Metrics:             h.metrics,
		Log:                 quiet(),
		HeartbeatInterval:   time.Second,
		ClockSkewDegradedMS: 2000,
		ClockSkewStaleMS:    10000,
		FallbackMaxDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	plans, err := h.adapter.PlanSubscriptions(specs)
	if err != nil {
		t.Fatal(err)
	}
	for _, plan := range plans {
		for _, spec := range plan.Specs {
			sym, _ := h.adapter.VenueSymbol(spec.Instrument)
			h.tracker.Register(spec, sym, plan.ID)
		}
	}

	h.proc, err = supervisor.New(supervisor.Options{
		Venue:         coretest.Venue,
		Adapter:       h.adapter,
		Plans:         plans,
		Publisher:     h.pub,
		Health:        h.tracker,
		Metrics:       h.metrics,
		Log:           quiet(),
		StreamBackoff: fastBackoff(),
		SocketBackoff: fastBackoff(),
		Breaker:       transport.BreakerOptions{ConsecutiveFailures: 10, OpenDuration: time.Hour},
		LeakTimeout:   leakTimeout,
		ExpiryWindow:  time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func (h *harness) newConn() *coretest.Conn {
	c := coretest.NewConn()
	h.mu.Lock()
	h.conns = append(h.conns, c)
	h.mu.Unlock()
	return c
}

// conn returns the nth connection handed out, waiting for it to exist.
func (h *harness) conn(t *testing.T, n int) *coretest.Conn {
	t.Helper()
	eventually(t, "connection %d", func() bool {
		h.mu.Lock()
		defer h.mu.Unlock()
		return len(h.conns) > n
	})
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.conns[n]
}

func (h *harness) run(t *testing.T) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); h.proc.Run(ctx) }()
	stopped := func() {
		cancel()
		select {
		case <-done:
		case <-time.After(settle):
			t.Error("Run did not return after its context was cancelled")
		}
	}
	t.Cleanup(stopped)
	return cancel
}

func key(symbol string, ch pb.Channel) string {
	return publish.Key(coretest.Venue, coretest.MarketType, symbol, ch)
}

// ---------- tests ----------

// TestPublishesWhatComesOffTheSocket is the baseline the failure cases move
// away from: one frame becomes one message per channel, each on its own key.
func TestPublishesWhatComesOffTheSocket(t *testing.T) {
	h := newHarness(t)
	h.run(t)

	h.conn(t, 0).Push([]byte("BTC_USDT"))

	for _, ch := range []pb.Channel{pb.Channel_CHANNEL_MARK_PRICE, pb.Channel_CHANNEL_INDEX_PRICE, pb.Channel_CHANNEL_FUNDING} {
		k := key("BTC_USDT", ch)
		eventually(t, k, func() bool { return h.pub.count(k) > 0 })
		if got := h.pub.envelope(k).Status; got != pb.Status_STATUS_HEALTHY {
			t.Errorf("%s status = %s, want HEALTHY", k, core.StatusName(got))
		}
	}
}

// TestSocketReconnectsAfterReadError: M1 exited the process here. Everything
// this phase is for starts with not doing that.
func TestSocketReconnectsAfterReadError(t *testing.T) {
	h := newHarness(t)
	h.run(t)

	first := h.conn(t, 0)
	first.Push([]byte("BTC_USDT"))
	k := key("BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE)
	eventually(t, "first publish", func() bool { return h.pub.count(k) > 0 })

	first.PushError(errors.New("connection reset by peer"))

	second := h.conn(t, 1)
	before := h.pub.count(k)
	second.Push([]byte("BTC_USDT"))
	eventually(t, "publishing again after reconnect", func() bool { return h.pub.count(k) > before })
}

// TestIdleSocketIsADisconnect: TCP holds a half-open connection open forever,
// so a socket that is connected and silent has to be treated as dead. The read
// deadline is the only thing that notices.
func TestIdleSocketIsADisconnect(t *testing.T) {
	h := newHarness(t)

	first := true
	h.mu.Lock()
	h.dial = func() (core.Conn, error) {
		c := h.newConn()
		if first {
			first = false
			c.Silent(20*time.Millisecond, transport.ErrIdle)
		}
		return c, nil
	}
	h.mu.Unlock()
	h.run(t)

	// The second connection only exists because the first was given up on.
	second := h.conn(t, 1)
	second.Push([]byte("BTC_USDT"))

	k := key("BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE)
	eventually(t, "publishing after the idle socket was replaced", func() bool { return h.pub.count(k) > 0 })
}

// TestCircuitBreakerStopsDialling is the acceptance criterion: ten consecutive
// failures, then no connection attempt at all while it is open.
func TestCircuitBreakerStopsDialling(t *testing.T) {
	h := newHarness(t)
	h.mu.Lock()
	h.dial = func() (core.Conn, error) { return nil, errors.New("connection refused") }
	h.mu.Unlock()
	h.run(t)

	eventually(t, "the breaker to open", func() bool { return h.adapter.Dials() >= 10 })

	// Whatever it reached when the breaker opened, it stops there. The open
	// duration is an hour, so any further dial is a real violation.
	settled := h.adapter.Dials()
	time.Sleep(200 * time.Millisecond)
	if got := h.adapter.Dials(); got != settled {
		t.Errorf("%d dials while the circuit was open, want none", got-settled)
	}

	// And the streams say so rather than looking merely degraded.
	st, reason := h.tracker.Status(h.specs[0])
	if st != pb.Status_STATUS_STALE {
		t.Errorf("status = %s (%q), want STALE", core.StatusName(st), reason)
	}
	if reason != "circuit open" {
		t.Errorf("reason = %q, want %q", reason, "circuit open")
	}
}

// TestOneStreamRestartLeavesTheOthersAlone: recovery is stream-level, so a
// single expired key must not interrupt the streams beside it.
func TestOneStreamRestartLeavesTheOthersAlone(t *testing.T) {
	h := newHarness(t)
	h.run(t)

	conn := h.conn(t, 0)
	conn.Push([]byte("BTC_USDT"))

	mark := key("BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE)
	index := key("BTC_USDT", pb.Channel_CHANNEL_INDEX_PRICE)
	eventually(t, "first publish", func() bool { return h.pub.count(mark) > 0 && h.pub.count(index) > 0 })

	h.proc.KeyExpired(h.specs[0]) // one key of six: tier 1

	// The socket is untouched: no redial, and the other streams keep going.
	beforeIndex := h.pub.count(index)
	conn.Push([]byte("BTC_USDT"))
	eventually(t, "the other streams still publishing", func() bool { return h.pub.count(index) > beforeIndex })

	h.mu.Lock()
	conns := len(h.conns)
	h.mu.Unlock()
	if conns != 1 {
		t.Errorf("%d connections opened; one expired key must not redial the socket", conns)
	}
	if conn.IsClosed() {
		t.Error("the socket was closed for a single expired key")
	}
}

// TestQuorumOfExpiriesRedialsTheSocket: enough of one socket's keys expiring
// together is the socket's failure, not the streams'.
func TestQuorumOfExpiriesRedialsTheSocket(t *testing.T) {
	h := newHarness(t)
	h.run(t)

	first := h.conn(t, 0)
	first.Push([]byte("BTC_USDT"))
	mark := key("BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE)
	eventually(t, "first publish", func() bool { return h.pub.count(mark) > 0 })

	// Three of three streams on this socket.
	for _, spec := range h.specs {
		h.proc.KeyExpired(spec)
	}

	eventually(t, "the socket to be closed", func() bool { return first.IsClosed() })

	second := h.conn(t, 1)
	before := h.pub.count(mark)
	second.Push([]byte("BTC_USDT"))
	eventually(t, "the replacement socket to publish", func() bool { return h.pub.count(mark) > before })
}

// TestWedgedSocketIsAbandonedAndCounted: Go cannot kill a goroutine. A read
// that never returns can only be given up on, and the whole point of doing it
// this way is that giving up is visible instead of silent.
func TestWedgedSocketIsAbandonedAndCounted(t *testing.T) {
	h := newHarness(t)

	wedged := true
	h.mu.Lock()
	h.dial = func() (core.Conn, error) {
		c := h.newConn()
		if wedged {
			wedged = false
			c.Wedge() // Close will not unblock its Read
		}
		return c, nil
	}
	h.mu.Unlock()
	h.run(t)

	first := h.conn(t, 0)
	first.Push([]byte("BTC_USDT"))
	mark := key("BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE)
	eventually(t, "first publish", func() bool { return h.pub.count(mark) > 0 })

	for _, spec := range h.specs {
		h.proc.KeyExpired(spec)
	}

	eventually(t, "the leak to be counted", func() bool { return h.proc.Leaked() > 0 })

	st, reason := h.tracker.VenueStatus()
	if st != pb.Status_STATUS_DEGRADED {
		t.Errorf("venue status = %s (%q), want DEGRADED", core.StatusName(st), reason)
	}

	// And it carried on: a leaked goroutine is not a reason to stop serving.
	second := h.conn(t, 1)
	before := h.pub.count(mark)
	second.Push([]byte("BTC_USDT"))
	eventually(t, "the replacement socket to publish", func() bool { return h.pub.count(mark) > before })
}

// TestShutdownClosesTheConnection: cancelling a context does not free a
// goroutine parked in Read, so the session has to close the socket underneath
// it. Without this, every shutdown waits out the leak timeout.
func TestShutdownClosesTheConnection(t *testing.T) {
	h := newHarness(t)
	cancel := h.run(t)

	conn := h.conn(t, 0)
	conn.Push([]byte("BTC_USDT"))
	eventually(t, "first publish", func() bool { return h.pub.count(key("BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE)) > 0 })

	start := time.Now()
	cancel()
	eventually(t, "the connection to be closed", conn.IsClosed)

	if elapsed := time.Since(start); elapsed >= leakTimeout {
		t.Errorf("shutdown took %v, past the %v leak timeout: the read was not unblocked by Close", elapsed, leakTimeout)
	}
	if h.proc.Leaked() != 0 {
		t.Errorf("%d goroutines leaked on a clean shutdown", h.proc.Leaked())
	}
}

// TestRejectedFrameDoesNotStopTheStream: one bad frame is not a reason to go
// dark on every other stream, and the keys it would have refreshed expire on
// their own and report themselves stale.
func TestRejectedFrameDoesNotStopTheStream(t *testing.T) {
	h := newHarness(t)
	h.run(t)

	conn := h.conn(t, 0)
	conn.Push([]byte("not a symbol at all"))

	eventually(t, "the venue to be marked degraded", func() bool {
		st, _ := h.tracker.Status(h.specs[0])
		return st == pb.Status_STATUS_DEGRADED
	})

	conn.Push([]byte("BTC_USDT"))
	mark := key("BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE)
	eventually(t, "the stream to recover", func() bool {
		st, _ := h.tracker.Status(h.specs[0])
		return h.pub.count(mark) > 0 && st == pb.Status_STATUS_HEALTHY
	})

	h.mu.Lock()
	conns := len(h.conns)
	h.mu.Unlock()
	if conns != 1 {
		t.Errorf("%d connections opened; a malformed frame must not redial", conns)
	}
}

// TestStatusIsStampedBySupervisorNotAdapter: the adapter knows the frame
// parsed, not whether the socket behind it is healthy, so its optimistic
// default must never reach the wire unchallenged.
func TestStatusIsStampedBySupervisorNotAdapter(t *testing.T) {
	h := newHarness(t)

	// The venue's clock 20 seconds ahead of ours, which is past the stale
	// threshold. The adapter still reports every frame as HEALTHY.
	h.adapter.ParseFunc = func(frame []byte, recvNs int64) ([]core.Message, error) {
		ref, err := core.ParseCanonical(string(frame), coretest.MarketType)
		if err != nil {
			return nil, err
		}
		m := h.adapter.Message(core.StreamSpec{Instrument: ref, Channel: pb.Channel_CHANNEL_MARK_PRICE}, recvNs, pb.Source_SOURCE_WEBSOCKET)
		m.Proto.(*pb.MarkPrice).Env.ExchangeTimeNs = recvNs + int64(20*time.Second)
		return []core.Message{m}, nil
	}
	h.run(t)

	conn := h.conn(t, 0)
	mark := key("BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE)

	// The first frame is what teaches the tracker about the skew, so the
	// status it stamps lands on the second.
	conn.Push([]byte("BTC_USDT"))
	eventually(t, "first publish", func() bool { return h.pub.count(mark) > 0 })
	before := h.pub.count(mark)
	conn.Push([]byte("BTC_USDT"))
	eventually(t, "the next publish", func() bool { return h.pub.count(mark) > before })

	env := h.pub.envelope(mark)
	if env.Status != pb.Status_STATUS_STALE {
		t.Errorf("status = %s, want STALE; the adapter's HEALTHY reached the wire", core.StatusName(env.Status))
	}
	if env.StatusReason == "" {
		t.Error("STALE published with no reason")
	}
}
