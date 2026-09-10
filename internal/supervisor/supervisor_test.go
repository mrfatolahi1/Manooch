package supervisor_test

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/core/coretest"
	"github.com/you/manooch/internal/health"
	"github.com/you/manooch/internal/observability"
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
	mutex  sync.Mutex
	counts map[string]int
	last   map[string]*manoochv1.Envelope
	err    error
}

func newRecorder() *recorder {
	return &recorder{counts: map[string]int{}, last: map[string]*manoochv1.Envelope{}}
}

func (recorder *recorder) Publish(_ context.Context, key string, message proto.Message, _ time.Duration) error {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	recorder.counts[key]++
	if enveloped, ok := message.(interface{ GetEnv() *manoochv1.Envelope }); ok {
		recorder.last[key] = proto.Clone(enveloped.GetEnv()).(*manoochv1.Envelope)
	}
	return recorder.err
}

func (recorder *recorder) Close() error { return nil }

func (recorder *recorder) count(key string) int {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return recorder.counts[key]
}

func (recorder *recorder) envelope(key string) *manoochv1.Envelope {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return recorder.last[key]
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
	adapter        *coretest.Adapter
	recorder       *recorder
	tracker        *health.Tracker
	process        *supervisor.Process
	specifications []core.StreamSpec
	metrics        *observability.Metrics

	mutex sync.Mutex
	conns []*coretest.Conn
	dial  func() (core.Conn, error)
}

func newHarness(t *testing.T, symbols ...string) *harness {
	t.Helper()
	if len(symbols) == 0 {
		symbols = []string{"BTC_USDT"}
	}

	specifications, err := coretest.Specifications(symbols...)
	if err != nil {
		t.Fatal(err)
	}

	harness := &harness{recorder: newRecorder(), specifications: specifications, metrics: observability.NewMetrics()}
	harness.adapter = &coretest.Adapter{}
	harness.adapter.DialFunc = func(context.Context, core.SocketPlan) (core.Conn, error) {
		harness.mutex.Lock()
		dial := harness.dial
		harness.mutex.Unlock()
		if dial != nil {
			return dial()
		}
		return harness.newConn(), nil
	}

	harness.tracker, err = health.New(health.Options{
		Venue:               coretest.Venue,
		Publisher:           harness.recorder,
		Metrics:             harness.metrics,
		Log:                 quiet(),
		HeartbeatInterval:   time.Second,
		ClockSkewDegradedMS: 2000,
		ClockSkewStaleMS:    10000,
		FallbackMaxDuration: 5 * time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}

	plans, err := harness.adapter.PlanSubscriptions(specifications)
	if err != nil {
		t.Fatal(err)
	}
	for _, plan := range plans {
		for _, specification := range plan.Specifications {
			symbol, _ := harness.adapter.VenueSymbol(specification.Instrument)
			harness.tracker.Register(specification, symbol, plan.ID)
		}
	}

	harness.process, err = supervisor.New(supervisor.Options{
		Venue:         coretest.Venue,
		Adapter:       harness.adapter,
		Plans:         plans,
		Publisher:     harness.recorder,
		Health:        harness.tracker,
		Metrics:       harness.metrics,
		Log:           quiet(),
		StreamBackoff: fastBackoff(),
		SocketBackoff: fastBackoff(),
		Breaker:       transport.BreakerOptions{ConsecutiveFailures: 10, OpenDuration: time.Hour},
		LeakTimeout:   leakTimeout,
		ExpiryWindow:  time.Minute,
		// The grace after a connect is what stops a reconnect's own expiry
		// burst from redialling again; these tests expire keys deliberately,
		// after a publish has already proved the socket up, so it is set out of
		// the way rather than waited through.
		ConnectGrace: time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	return harness
}

// withMaxAge rebuilds the process with a connection age limit, which is only
// settable at construction.
func (harness *harness) withMaxAge(t *testing.T, duration time.Duration) {
	t.Helper()
	plans, err := harness.adapter.PlanSubscriptions(harness.specifications)
	if err != nil {
		t.Fatal(err)
	}
	harness.process, err = supervisor.New(supervisor.Options{
		Venue:         coretest.Venue,
		Adapter:       harness.adapter,
		Plans:         plans,
		Publisher:     harness.recorder,
		Health:        harness.tracker,
		Metrics:       harness.metrics,
		Log:           quiet(),
		StreamBackoff: fastBackoff(),
		SocketBackoff: fastBackoff(),
		Breaker:       transport.BreakerOptions{ConsecutiveFailures: 10, OpenDuration: time.Hour},
		LeakTimeout:   leakTimeout,
		ExpiryWindow:  time.Minute,
		// The grace after a connect is what stops a reconnect's own expiry
		// burst from redialling again; these tests expire keys deliberately,
		// after a publish has already proved the socket up, so it is set out of
		// the way rather than waited through.
		ConnectGrace: time.Millisecond,
		ConnMaxAge:   duration,
	})
	if err != nil {
		t.Fatal(err)
	}
}

func (harness *harness) newConn() *coretest.Conn {
	connection := coretest.NewConn()
	harness.mutex.Lock()
	harness.conns = append(harness.conns, connection)
	harness.mutex.Unlock()
	return connection
}

// connection returns the nth connection handed out, waiting for it to exist.
func (harness *harness) connection(t *testing.T, n int) *coretest.Conn {
	t.Helper()
	eventually(t, "connection %d", func() bool {
		harness.mutex.Lock()
		defer harness.mutex.Unlock()
		return len(harness.conns) > n
	})
	harness.mutex.Lock()
	defer harness.mutex.Unlock()
	return harness.conns[n]
}

func (harness *harness) run(t *testing.T) context.CancelFunc {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { defer close(done); harness.process.Run(ctx) }()
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

func key(symbol string, channel manoochv1.Channel) string {
	return publish.Key(coretest.Venue, coretest.MarketType, symbol, channel)
}

// ---------- tests ----------

// TestPublishesWhatComesOffTheSocket is the baseline the failure cases move
// away from: one frame becomes one message per channel, each on its own key.
func TestPublishesWhatComesOffTheSocket(t *testing.T) {
	harness := newHarness(t)
	harness.run(t)

	harness.connection(t, 0).Push([]byte("BTC_USDT"))

	for _, channel := range []manoochv1.Channel{manoochv1.Channel_CHANNEL_MARK_PRICE, manoochv1.Channel_CHANNEL_INDEX_PRICE, manoochv1.Channel_CHANNEL_FUNDING} {
		k := key("BTC_USDT", channel)
		eventually(t, k, func() bool { return harness.recorder.count(k) > 0 })
		if got := harness.recorder.envelope(k).Status; got != manoochv1.Status_STATUS_HEALTHY {
			t.Errorf("%s status = %s, want HEALTHY", k, core.StatusName(got))
		}
	}
}

// TestSocketReconnectsAfterReadError: M1 exited the process here. Everything
// this phase is for starts with not doing that.
func TestSocketReconnectsAfterReadError(t *testing.T) {
	harness := newHarness(t)
	harness.run(t)

	first := harness.connection(t, 0)
	first.Push([]byte("BTC_USDT"))
	k := key("BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE)
	eventually(t, "first publish", func() bool { return harness.recorder.count(k) > 0 })

	first.PushError(errors.New("connection reset by peer"))

	second := harness.connection(t, 1)
	before := harness.recorder.count(k)
	second.Push([]byte("BTC_USDT"))
	eventually(t, "publishing again after reconnect", func() bool { return harness.recorder.count(k) > before })
}

// TestIdleSocketIsADisconnect: TCP holds a half-open connection open forever,
// so a socket that is connected and silent has to be treated as dead. The read
// deadline is the only thing that notices.
func TestIdleSocketIsADisconnect(t *testing.T) {
	harness := newHarness(t)

	first := true
	harness.mutex.Lock()
	harness.dial = func() (core.Conn, error) {
		connection := harness.newConn()
		if first {
			first = false
			connection.Silent(20*time.Millisecond, transport.ErrIdle)
		}
		return connection, nil
	}
	harness.mutex.Unlock()
	harness.run(t)

	// The second connection only exists because the first was given up on.
	second := harness.connection(t, 1)
	second.Push([]byte("BTC_USDT"))

	k := key("BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE)
	eventually(t, "publishing after the idle socket was replaced", func() bool { return harness.recorder.count(k) > 0 })
}

// TestCircuitBreakerStopsDialling is the acceptance criterion: ten consecutive
// failures, then no connection attempt at all while it is open.
func TestCircuitBreakerStopsDialling(t *testing.T) {
	harness := newHarness(t)
	harness.mutex.Lock()
	harness.dial = func() (core.Conn, error) { return nil, errors.New("connection refused") }
	harness.mutex.Unlock()
	harness.run(t)

	eventually(t, "the breaker to open", func() bool { return harness.adapter.Dials() >= 10 })

	// Whatever it reached when the breaker opened, it stops there. The open
	// duration is an hour, so any further dial is a real violation.
	settled := harness.adapter.Dials()
	time.Sleep(200 * time.Millisecond)
	if got := harness.adapter.Dials(); got != settled {
		t.Errorf("%d dials while the circuit was open, want none", got-settled)
	}

	// And the streams say so rather than looking merely degraded.
	status, reason := harness.tracker.Status(harness.specifications[0])
	if status != manoochv1.Status_STATUS_STALE {
		t.Errorf("status = %s (%q), want STALE", core.StatusName(status), reason)
	}
	if reason != "circuit open" {
		t.Errorf("reason = %q, want %q", reason, "circuit open")
	}
}

// TestOneStreamRestartLeavesTheOthersAlone: recovery is stream-level, so a
// single expired key must not interrupt the streams beside it.
func TestOneStreamRestartLeavesTheOthersAlone(t *testing.T) {
	harness := newHarness(t)
	harness.run(t)

	connection := harness.connection(t, 0)
	connection.Push([]byte("BTC_USDT"))

	mark := key("BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE)
	index := key("BTC_USDT", manoochv1.Channel_CHANNEL_INDEX_PRICE)
	eventually(t, "first publish", func() bool { return harness.recorder.count(mark) > 0 && harness.recorder.count(index) > 0 })

	harness.process.KeyExpired(harness.specifications[0]) // one key of six: tier 1

	// The socket is untouched: no redial, and the other streams keep going.
	beforeIndex := harness.recorder.count(index)
	connection.Push([]byte("BTC_USDT"))
	eventually(t, "the other streams still publishing", func() bool { return harness.recorder.count(index) > beforeIndex })

	harness.mutex.Lock()
	conns := len(harness.conns)
	harness.mutex.Unlock()
	if conns != 1 {
		t.Errorf("%d connections opened; one expired key must not redial the socket", conns)
	}
	if connection.IsClosed() {
		t.Error("the socket was closed for a single expired key")
	}
}

// TestQuorumOfExpiriesRedialsTheSocket: enough of one socket's keys expiring
// together is the socket's failure, not the streams'.
func TestQuorumOfExpiriesRedialsTheSocket(t *testing.T) {
	harness := newHarness(t)
	harness.run(t)

	first := harness.connection(t, 0)
	first.Push([]byte("BTC_USDT"))
	mark := key("BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE)
	eventually(t, "first publish", func() bool { return harness.recorder.count(mark) > 0 })

	// Three of three streams on this socket.
	for _, specification := range harness.specifications {
		harness.process.KeyExpired(specification)
	}

	eventually(t, "the socket to be closed", func() bool { return first.IsClosed() })

	second := harness.connection(t, 1)
	before := harness.recorder.count(mark)
	second.Push([]byte("BTC_USDT"))
	eventually(t, "the replacement socket to publish", func() bool { return harness.recorder.count(mark) > before })
}

// TestWedgedSocketIsAbandonedAndCounted: Go cannot kill a goroutine. A read
// that never returns can only be given up on, and the whole point of doing it
// this way is that giving up is visible instead of silent.
func TestWedgedSocketIsAbandonedAndCounted(t *testing.T) {
	harness := newHarness(t)

	wedged := true
	harness.mutex.Lock()
	harness.dial = func() (core.Conn, error) {
		connection := harness.newConn()
		if wedged {
			wedged = false
			connection.Wedge() // Close will not unblock its Read
		}
		return connection, nil
	}
	harness.mutex.Unlock()
	harness.run(t)

	first := harness.connection(t, 0)
	first.Push([]byte("BTC_USDT"))
	mark := key("BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE)
	eventually(t, "first publish", func() bool { return harness.recorder.count(mark) > 0 })

	for _, specification := range harness.specifications {
		harness.process.KeyExpired(specification)
	}

	eventually(t, "the leak to be counted", func() bool { return harness.process.Leaked() > 0 })

	status, reason := harness.tracker.VenueStatus()
	if status != manoochv1.Status_STATUS_DEGRADED {
		t.Errorf("venue status = %s (%q), want DEGRADED", core.StatusName(status), reason)
	}

	// And it carried on: a leaked goroutine is not a reason to stop serving.
	second := harness.connection(t, 1)
	before := harness.recorder.count(mark)
	second.Push([]byte("BTC_USDT"))
	eventually(t, "the replacement socket to publish", func() bool { return harness.recorder.count(mark) > before })
}

// TestShutdownClosesTheConnection: cancelling a context does not free a
// goroutine parked in Read, so the session has to close the socket underneath
// it. Without this, every shutdown waits out the leak timeout.
func TestShutdownClosesTheConnection(t *testing.T) {
	harness := newHarness(t)
	cancel := harness.run(t)

	connection := harness.connection(t, 0)
	connection.Push([]byte("BTC_USDT"))
	eventually(t, "first publish", func() bool { return harness.recorder.count(key("BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE)) > 0 })

	start := time.Now()
	cancel()
	eventually(t, "the connection to be closed", connection.IsClosed)

	if elapsed := time.Since(start); elapsed >= leakTimeout {
		t.Errorf("shutdown took %v, past the %v leak timeout: the read was not unblocked by Close", elapsed, leakTimeout)
	}
	if harness.process.Leaked() != 0 {
		t.Errorf("%d goroutines leaked on a clean shutdown", harness.process.Leaked())
	}
}

// TestRejectedFrameDoesNotStopTheStream: one bad frame is not a reason to go
// dark on every other stream, and the keys it would have refreshed expire on
// their own and report themselves stale.
func TestRejectedFrameDoesNotStopTheStream(t *testing.T) {
	harness := newHarness(t)
	harness.run(t)

	connection := harness.connection(t, 0)
	connection.Push([]byte("not a symbol at all"))

	eventually(t, "the venue to be marked degraded", func() bool {
		status, _ := harness.tracker.Status(harness.specifications[0])
		return status == manoochv1.Status_STATUS_DEGRADED
	})

	connection.Push([]byte("BTC_USDT"))
	mark := key("BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE)
	eventually(t, "the stream to recover", func() bool {
		status, _ := harness.tracker.Status(harness.specifications[0])
		return harness.recorder.count(mark) > 0 && status == manoochv1.Status_STATUS_HEALTHY
	})

	harness.mutex.Lock()
	conns := len(harness.conns)
	harness.mutex.Unlock()
	if conns != 1 {
		t.Errorf("%d connections opened; a malformed frame must not redial", conns)
	}
}

// TestStatusIsStampedBySupervisorNotAdapter: the adapter knows the frame
// parsed, not whether the socket behind it is healthy, so its optimistic
// default must never reach the wire unchallenged.
func TestStatusIsStampedBySupervisorNotAdapter(t *testing.T) {
	harness := newHarness(t)

	// The venue's clock 20 seconds ahead of ours, which is past the stale
	// threshold. The adapter still reports every frame as HEALTHY.
	harness.adapter.ParseFunc = func(frame []byte, receivedNs int64) ([]core.Message, error) {
		reference, err := core.ParseCanonical(string(frame), coretest.MarketType)
		if err != nil {
			return nil, err
		}
		message := harness.adapter.Message(core.StreamSpec{Instrument: reference, Channel: manoochv1.Channel_CHANNEL_MARK_PRICE}, receivedNs, manoochv1.Source_SOURCE_WEBSOCKET)
		message.Proto.(*manoochv1.MarkPrice).Env.ExchangeTimeNs = receivedNs + int64(20*time.Second)
		return []core.Message{message}, nil
	}
	harness.run(t)

	connection := harness.connection(t, 0)
	mark := key("BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE)

	// The first frame is what teaches the tracker about the skew, so the
	// status it stamps lands on the second.
	connection.Push([]byte("BTC_USDT"))
	eventually(t, "first publish", func() bool { return harness.recorder.count(mark) > 0 })
	before := harness.recorder.count(mark)
	connection.Push([]byte("BTC_USDT"))
	eventually(t, "the next publish", func() bool { return harness.recorder.count(mark) > before })

	envelope := harness.recorder.envelope(mark)
	if envelope.Status != manoochv1.Status_STATUS_STALE {
		t.Errorf("status = %s, want STALE; the adapter's HEALTHY reached the wire", core.StatusName(envelope.Status))
	}
	if envelope.StatusReason == "" {
		t.Error("STALE published with no reason")
	}
}

// TestProactiveReconnectAtMaxAge: Binance drops a futures socket after twenty-
// four hours. Going first turns that from a gap into a handover — the streams
// stay inside their TTL across it and nobody finds out by not being sent data.
func TestProactiveReconnectAtMaxAge(t *testing.T) {
	harness := newHarness(t)
	harness.withMaxAge(t, 30*time.Millisecond)
	harness.run(t)

	first := harness.connection(t, 0)
	first.Push([]byte("BTC_USDT"))
	mark := key("BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE)
	eventually(t, "first publish", func() bool { return harness.recorder.count(mark) > 0 })

	// Nothing failed: the socket is healthy and still gets replaced.
	eventually(t, "the aged socket to be closed", first.IsClosed)

	second := harness.connection(t, 1)
	before := harness.recorder.count(mark)
	second.Push([]byte("BTC_USDT"))
	eventually(t, "the replacement socket to publish", func() bool { return harness.recorder.count(mark) > before })
}

// TestSkewIsOnlyMeasuredFromASendTime: a venue that stamps a value with the
// instant it became effective, rather than the instant it sent it, is not
// reporting a clock. KuCoin does exactly that on funding — the timestamp is the
// settlement time, hours old on arrival — and differencing it against arrival
// would take every stream on the venue to STALE once a minute.
func TestSkewIsOnlyMeasuredFromASendTime(t *testing.T) {
	harness := newHarness(t)

	// The frame carries one message: a funding rate stamped with a settlement
	// four hours ago, exactly as KuCoin's funding.rate subject arrives.
	const fourHours = 4 * time.Hour
	harness.adapter.ParseFunc = func(frame []byte, receivedNs int64) ([]core.Message, error) {
		funding := harness.adapter.Message(harness.specifications[2], receivedNs, manoochv1.Source_SOURCE_WEBSOCKET)
		envelope := funding.Proto.(interface{ GetEnv() *manoochv1.Envelope }).GetEnv()
		envelope.ExchangeTimeNs = receivedNs - int64(fourHours)
		envelope.ExchangeTimeIsSendTime = false
		return []core.Message{funding}, nil
	}
	harness.run(t)

	connection := harness.connection(t, 0)
	connection.Push([]byte("BTC_USDT"))

	k := key("BTC_USDT", manoochv1.Channel_CHANNEL_FUNDING)
	eventually(t, "the funding message to be published", func() bool { return harness.recorder.count(k) > 0 })

	// The mark price is a send time and reports a skew of zero; the funding
	// message is ignored for this purpose rather than overwriting it.
	if status, reason := harness.tracker.VenueStatus(); status != manoochv1.Status_STATUS_HEALTHY {
		t.Errorf("venue status = %s (%q); an event time was mistaken for a clock reading",
			core.StatusName(status), reason)
	}
	if status, reason := harness.tracker.Status(harness.specifications[2]); status != manoochv1.Status_STATUS_HEALTHY {
		t.Errorf("funding status = %s (%q), want HEALTHY", core.StatusName(status), reason)
	}
}

// TestReconnectDoesNotRedialItself: the keys a socket feeds go stale while it
// is down, and Redis reports them when it reclaims them — after the reconnect,
// not when the TTL ran out. Counting those against the connection that has just
// fixed the problem is a loop with no exit on any venue whose dial takes longer
// than the shortest TTL, which is every venue that bootstraps over REST.
func TestReconnectDoesNotRedialItself(t *testing.T) {
	harness := newHarness(t)
	harness.process = nil // rebuilt below with the real grace period

	specifications, err := coretest.Specifications("BTC_USDT")
	if err != nil {
		t.Fatal(err)
	}
	plans, err := harness.adapter.PlanSubscriptions(specifications)
	if err != nil {
		t.Fatal(err)
	}
	harness.process, err = supervisor.New(supervisor.Options{
		Venue:         coretest.Venue,
		Adapter:       harness.adapter,
		Plans:         plans,
		Publisher:     harness.recorder,
		Health:        harness.tracker,
		Metrics:       harness.metrics,
		Log:           quiet(),
		StreamBackoff: fastBackoff(),
		SocketBackoff: fastBackoff(),
		Breaker:       transport.BreakerOptions{ConsecutiveFailures: 10, OpenDuration: time.Hour},
		LeakTimeout:   leakTimeout,
		ExpiryWindow:  time.Minute,
		ConnectGrace:  2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	harness.run(t)

	first := harness.connection(t, 0)
	mark := key("BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE)
	first.Push([]byte("BTC_USDT"))
	eventually(t, "first publish", func() bool { return harness.recorder.count(mark) > 0 })

	// Every key on the socket expires at once, which is exactly the burst a
	// reconnect produces. Inside the grace it must change nothing.
	for _, specification := range harness.specifications {
		harness.process.KeyExpired(specification)
	}

	time.Sleep(200 * time.Millisecond)
	if first.IsClosed() {
		t.Error("the socket was redialled by the expiries its own reconnect caused")
	}

	// And it is still delivering on the same connection.
	before := harness.recorder.count(mark)
	first.Push([]byte("BTC_USDT"))
	eventually(t, "the socket to keep publishing", func() bool { return harness.recorder.count(mark) > before })
}
