package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/health"
	"github.com/you/manooch/internal/observability"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/internal/transport"
	"google.golang.org/protobuf/proto"
)

// parseErrorLogInterval caps the parse-error log rate. A venue sending frames
// we cannot read is one fact, not one fact per frame.
const parseErrorLogInterval = time.Second

// defaultExpiryWindow is how long an expired key counts towards the quorum that
// redials a socket. Wide enough that keys on the same cadence expiring in
// sequence are seen together, short enough that yesterday's outage does not.
const defaultExpiryWindow = 10 * time.Second

// defaultConnectGrace is how long after a connection comes up an expired key is
// still attributed to the outage before it rather than to the new connection.
//
// It has to cover two things: the first message on each stream arriving, and
// Redis finishing its reports of the keys that lapsed while the socket was
// down — it reports an expiry when it reclaims the key, which is after the
// reconnect, not when the TTL ran out. Five seconds covers both on every
// channel in scope.
const defaultConnectGrace = 5 * time.Second

// Options configures a Process.
type Options struct {
	// Venue is the canonical upper-case venue name.
	Venue string

	// Adapter and Plans come from the venue package: the adapter decides how
	// streams are grouped onto sockets, this package decides when to dial.
	Adapter core.Adapter
	Plans   []core.SocketPlan

	Publisher publish.Publisher
	Health    *health.Tracker
	Metrics   *observability.Metrics
	Log       *slog.Logger

	// StreamBackoff is the wait before relaunching one stream goroutine;
	// SocketBackoff the wait before redialling a socket.
	StreamBackoff transport.Policy
	SocketBackoff transport.Policy

	// Breaker stops connection attempts entirely after repeated failure.
	Breaker transport.BreakerOptions

	// LeakTimeout bounds the wait for a stopped goroutine to return.
	LeakTimeout time.Duration

	// ConnMaxAge is how old a connection may get before it is redialled on
	// purpose. Zero disables the timer.
	ConnMaxAge time.Duration

	// ExpiryWindow is how long an expired key counts towards the quorum that
	// escalates from restarting one stream to redialling the socket. Zero
	// means defaultExpiryWindow.
	ExpiryWindow time.Duration

	// ConnectGrace is how long after a socket connects an expired key is
	// attributed to the outage before it rather than to the new connection.
	// Zero means defaultConnectGrace.
	ConnectGrace time.Duration

	// OnMessage is called for each stream a websocket message arrives on,
	// before it is published. The fallback watcher disengages through it.
	OnMessage func(core.StreamSpec)

	// Now is swappable for tests. Zero means time.Now.
	Now func() time.Time
}

// A Process runs one venue's sockets. It is the supervision tree's root and it
// never exits on a stream or socket failure: the only thing that stops it is
// its context.
type Process struct {
	options Options
	now     func() time.Time

	sockets  []*socketRunner
	byStream map[core.StreamSpec]*socketRunner

	mutex        sync.Mutex
	leaked       int
	lastParseLog time.Time
}

// New builds the supervision tree. It opens nothing.
func New(options Options) (*Process, error) {
	switch {
	case options.Venue == "":
		return nil, errors.New("supervisor: no venue")
	case options.Adapter == nil:
		return nil, errors.New("supervisor: no adapter")
	case len(options.Plans) == 0:
		return nil, errors.New("supervisor: no socket plans")
	case options.Publisher == nil:
		return nil, errors.New("supervisor: no publisher")
	case options.Health == nil:
		return nil, errors.New("supervisor: no health tracker")
	case options.Metrics == nil:
		return nil, errors.New("supervisor: no metrics")
	case options.Log == nil:
		return nil, errors.New("supervisor: no logger")
	case options.LeakTimeout <= 0:
		return nil, fmt.Errorf("supervisor: goroutine leak timeout is %v", options.LeakTimeout)
	}
	if options.Now == nil {
		options.Now = time.Now
	}
	if options.ExpiryWindow <= 0 {
		options.ExpiryWindow = defaultExpiryWindow
	}
	if options.ConnectGrace <= 0 {
		options.ConnectGrace = defaultConnectGrace
	}

	process := &Process{options: options, now: options.Now, byStream: map[core.StreamSpec]*socketRunner{}}
	for _, plan := range options.Plans {
		runner := newSocketRunner(process, plan)
		process.sockets = append(process.sockets, runner)
		for _, specification := range plan.Specifications {
			process.byStream[specification] = runner
		}
	}
	return process, nil
}

// Run supervises every socket until ctx ends.
func (process *Process) Run(ctx context.Context) {
	var waitGroup sync.WaitGroup
	for _, runner := range process.sockets {
		waitGroup.Add(1)
		go func() {
			defer waitGroup.Done()
			runner.run(ctx)
		}()
	}
	waitGroup.Wait()
}

// KeyExpired escalates a stream whose Redis key reached its TTL.
//
// One key is tier 1: restart that stream's goroutine and nothing else. Enough
// of a socket's keys expiring together is tier 2: the socket is delivering for
// nobody, so redial it. There is no tier 3 — the process does not exit.
func (process *Process) KeyExpired(specification core.StreamSpec) {
	runner := process.byStream[specification]
	if runner == nil {
		return
	}
	runner.noteExpiry(specification)
}

// ---------- socket ----------

// A socketRunner owns one websocket: dialling it, reading it, and the stream
// goroutines that publish what comes off it.
type socketRunner struct {
	process *Process
	plan    core.SocketPlan
	breaker *transport.Breaker
	// quorum is how many of this socket's streams must expire together before
	// the failure is treated as the socket's rather than the streams'.
	quorum int

	streams map[core.StreamSpec]*streamRunner
	// order keeps stream startup deterministic, for logs and tests.
	order []*streamRunner

	// redial ends the current session when closing the connection is not
	// enough on its own — a socket whose read never returns still has to be
	// replaced, and the old goroutine abandoned.
	redial chan struct{}

	mutex      sync.Mutex
	connection core.Conn
	// connectedAt is when the current connection came up, for the grace period
	// in noteExpiry.
	connectedAt time.Time
	expiredAt   map[core.StreamSpec]time.Time
}

func newSocketRunner(process *Process, plan core.SocketPlan) *socketRunner {
	runner := &socketRunner{
		process:   process,
		plan:      plan,
		breaker:   transport.NewBreaker(process.options.Breaker),
		streams:   make(map[core.StreamSpec]*streamRunner, len(plan.Specifications)),
		expiredAt: map[core.StreamSpec]time.Time{},
		redial:    make(chan struct{}, 1),
	}
	// Half the socket's streams, never fewer than two: one key expiring is a
	// stream problem, most of them expiring is a connection problem.
	runner.quorum = max(2, len(plan.Specifications)/2)

	for _, specification := range plan.Specifications {
		if _, duplicate := runner.streams[specification]; duplicate {
			continue
		}
		stream := &streamRunner{socket: runner, specification: specification, wake: make(chan struct{}, 1)}
		runner.streams[specification] = stream
		runner.order = append(runner.order, stream)
	}
	return runner
}

// run dials and reads the socket until ctx ends, redialling with backoff.
func (runner *socketRunner) run(ctx context.Context) {
	log := runner.process.options.Log.With("socket", runner.plan.ID)

	for attempt := 0; ctx.Err() == nil; {
		// The breaker is asked before every attempt. While it is open no
		// connection is made at all — not a slow one, not a probe — because a
		// venue that is refusing us is asking to be left alone, and a client
		// that keeps knocking is how an IP ban is earned.
		if wait := runner.breaker.Retry(); wait > 0 {
			runner.process.options.Health.SocketState(runner.plan.ID, health.SocketCircuitOpen,
				fmt.Sprintf("%d consecutive failures", runner.breaker.Failures()))
			log.Error("circuit open, making no connection attempt",
				"failures", runner.breaker.Failures(), "retry_in", wait.String())
			if !transport.Wait(ctx, wait) {
				return
			}
			continue
		}

		runner.process.options.Health.SocketState(runner.plan.ID, health.SocketDialing, "dialing")
		connection, err := runner.process.options.Adapter.Dial(ctx, runner.plan)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			runner.breaker.Fail()
			log.Error("dial failed", "error", err.Error(), "failures", runner.breaker.Failures())
			if !runner.process.options.SocketBackoff.Sleep(ctx, attempt) {
				return
			}
			attempt++
			continue
		}

		runner.breaker.Succeed()
		attempt = 0
		runner.setConn(connection)
		runner.process.options.Health.SocketState(runner.plan.ID, health.SocketConnected, "")
		log.Info("socket connected", "streams", len(runner.plan.Specifications))

		reason := runner.session(ctx)

		runner.closeConn()
		if ctx.Err() != nil {
			log.Info("socket closed")
			return
		}
		runner.process.options.Health.SocketState(runner.plan.ID, health.SocketDialing, reason)
		runner.process.options.Health.Reconnected(runner.plan.ID)
		log.Error("socket down, reconnecting", "reason", reason)

		if !runner.process.options.SocketBackoff.Sleep(ctx, attempt) {
			return
		}
		attempt++
	}
}

// session runs the read loop with the stream goroutines behind it, returning
// why it ended.
//
// The read loop gets a goroutine of its own rather than running here, because
// it is the one that parks in Conn.Read: cancelling a context cannot free it,
// and waiting for it without a bound would hang shutdown behind a socket whose
// read never returns.
func (runner *socketRunner) session(ctx context.Context) string {
	sessionCtx, cancel := context.WithCancel(ctx)

	for _, stream := range runner.order {
		stream.start(sessionCtx)
	}
	defer runner.stopStreams()

	// Buffered, and never read after the select below: an abandoned read loop
	// that finally returns must not block forever on the send.
	reasons := make(chan string, 1)
	exit := make(chan error, 1)
	connection := runner.currentConn()
	go func() {
		reasons <- runner.readLoop(ctx, connection)
		exit <- nil
	}()

	runner.drainRedial()

	// Venues drop long-lived sockets on a schedule of their own — Binance at
	// twenty-four hours. Going first is the difference between a handover and
	// a gap: we choose the moment, the streams stay inside their TTL across
	// it, and nobody has to discover the disconnect by not being sent data.
	var aged <-chan time.Time
	if runner.process.options.ConnMaxAge > 0 {
		timer := time.NewTimer(runner.process.options.ConnMaxAge)
		defer timer.Stop()
		aged = timer.C
	}

	var reason string
	select {
	case reason = <-reasons:
	case <-aged:
		reason = "planned reconnect at max age " + runner.process.options.ConnMaxAge.String()
		runner.process.options.Log.Info("reconnecting before the venue disconnects us",
			"socket", runner.plan.ID, "max_age", runner.process.options.ConnMaxAge.String())
	case <-runner.redial:
		reason = "streams expired together"
	case <-sessionCtx.Done():
		reason = "shutting down"
	}

	// Cancel, close, then wait — in that order, because a goroutine parked in
	// Read never sees the cancel and closing is the only thing that frees it.
	if err := StopGoroutine(cancel, runner.closeConn, exit, runner.process.options.LeakTimeout); errors.Is(err, ErrLeaked) {
		runner.process.options.Log.Error("socket read loop did not return, abandoning it",
			"socket", runner.plan.ID, "timeout", runner.process.options.LeakTimeout.String())
		runner.process.countLeak()
	}
	return reason
}

// readLoop reads frames until one fails, returning why.
func (runner *socketRunner) readLoop(ctx context.Context, connection core.Conn) string {
	if connection == nil {
		return "connection closed"
	}
	for {
		frame, receivedNs, err := connection.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return "shutting down"
			}
			return "read: " + err.Error()
		}
		runner.handleFrame(frame, receivedNs)
	}
}

// drainRedial clears a stale escalation left over from the previous session.
func (runner *socketRunner) drainRedial() {
	select {
	case <-runner.redial:
	default:
	}
}

// handleFrame turns one frame into published messages.
//
// A frame that will not parse is counted, rate-limit logged and skipped. One
// malformed frame is not a reason to go dark on every other stream, and the
// keys it would have refreshed expire on their own and report themselves stale.
func (runner *socketRunner) handleFrame(frame []byte, receivedNs int64) {
	messages, err := runner.process.options.Adapter.Parse(frame, receivedNs)
	if err != nil {
		runner.process.countParseError(runner.plan.ID, err)
		return
	}
	if len(messages) == 0 {
		return // an ack, a pong or a heartbeat
	}

	// Recorded before publishing, so it is present even when Redis is refusing
	// writes — which is exactly when it is worth reading. Every message is
	// offered: the first is not necessarily the one carrying a send time.
	for _, message := range messages {
		runner.process.observeSkew(message)
	}

	counted := make(map[manoochv1.Channel]bool, len(messages))
	for _, message := range messages {
		if !counted[message.Channel] {
			counted[message.Channel] = true
			runner.process.options.Metrics.WebSocketFramesReceived.WithLabelValues(
				runner.process.options.Venue,
				core.MarketTypeName(message.Specification.Instrument.MarketType),
				core.ChannelName(message.Channel)).Inc()
		}
		if stream := runner.streams[message.Specification]; stream != nil {
			stream.deliver(message)
		}
	}
}

// noteExpiry records one expired key and decides which tier it is.
//
// Two things do not count, because neither is evidence against this socket:
//
// An expiry while the socket has no live connection. It is already dialing or
// backing off, and every key it feeds will expire until it comes back;
// redialling mid-dial only makes the outage longer, and at startup it aborts
// the very first connection attempt before a single key has been written.
//
// An expiry inside the grace period after a connection came up. Those keys went
// stale during the outage that preceded it — Redis reports an expiry when it
// reclaims the key, which is after the reconnect, not when the TTL lapsed — so
// counting them redials the socket that has just fixed the problem. On a venue
// whose dial takes longer than the mark price TTL, that is a loop with no exit:
// reconnect, keys expire, redial.
//
// What is left is what the escalation is for: a socket that is connected, has
// been for a while, and is delivering for nobody.
func (runner *socketRunner) noteExpiry(specification core.StreamSpec) {
	runner.mutex.Lock()
	now := runner.process.now()
	if runner.connection == nil || now.Sub(runner.connectedAt) < runner.process.options.ConnectGrace {
		runner.mutex.Unlock()
		return
	}
	runner.expiredAt[specification] = now
	n := 0
	for k, at := range runner.expiredAt {
		if now.Sub(at) > runner.process.options.ExpiryWindow {
			delete(runner.expiredAt, k)
			continue
		}
		n++
	}
	escalate := n >= runner.quorum
	if escalate {
		clear(runner.expiredAt)
	}
	stream := runner.streams[specification]
	runner.mutex.Unlock()

	if escalate {
		runner.process.options.Log.Error("streams expired together, redialing socket",
			"socket", runner.plan.ID, "expired", n, "quorum", runner.quorum)
		// Close first, because that is what a healthy read loop reacts to;
		// then signal, so a read that does not come back is abandoned rather
		// than leaving the socket wedged forever.
		runner.closeConn()
		select {
		case runner.redial <- struct{}{}:
		default:
		}
		return
	}
	if stream != nil {
		stream.restart()
	}
}

// setConn adopts a new connection. The expiries recorded against the previous
// one are dropped with it: they were explained by the outage this connection
// just ended.
func (runner *socketRunner) setConn(connection core.Conn) {
	runner.mutex.Lock()
	defer runner.mutex.Unlock()
	runner.connection = connection
	runner.connectedAt = runner.process.now()
	clear(runner.expiredAt)
}

func (runner *socketRunner) currentConn() core.Conn {
	runner.mutex.Lock()
	defer runner.mutex.Unlock()
	return runner.connection
}

// closeConn drops the connection and unblocks whatever is reading it. It is
// safe to call from any goroutine and more than once.
func (runner *socketRunner) closeConn() {
	runner.mutex.Lock()
	connection := runner.connection
	runner.connection = nil
	runner.connectedAt = time.Time{}
	runner.mutex.Unlock()

	if connection != nil {
		_ = connection.Close()
	}
}

// stopStreams waits for every stream goroutine to finish. The session context
// is already cancelled by the time it runs.
func (runner *socketRunner) stopStreams() {
	for _, stream := range runner.order {
		stream.wait()
	}
}

// ---------- stream ----------

// A streamRunner publishes one stream: exactly one Redis key.
//
// It holds only the newest message it was handed. Falling behind therefore
// drops the older price rather than growing a queue of them, which is the same
// thing the last-value cache it feeds does, and the alternative is publishing a
// price that was already wrong when it was queued.
type streamRunner struct {
	socket        *socketRunner
	specification core.StreamSpec
	wake          chan struct{}

	mutex   sync.Mutex
	pending *core.Message
	task    *Task
}

// start launches the goroutine under supervision for one session.
func (runner *streamRunner) start(ctx context.Context) {
	task := Start(ctx, TaskOptions{
		Name:        runner.specification.String(),
		Run:         runner.run,
		LeakTimeout: runner.socket.process.options.LeakTimeout,
		Backoff:     runner.socket.process.options.StreamBackoff,
		Log:         runner.socket.process.options.Log,
		OnExit:      runner.onExit,
		// No Unblock: this goroutine parks on its own channels and its
		// context, never on the socket, so cancelling is enough to free it.
		// The socket's connection is closed by the session, once, rather than
		// by each of the streams that share it.
	})
	runner.mutex.Lock()
	runner.task = task
	runner.mutex.Unlock()
}

// wait blocks until the goroutine has stopped being supervised.
func (runner *streamRunner) wait() {
	runner.mutex.Lock()
	task := runner.task
	runner.mutex.Unlock()
	if task != nil {
		task.Wait()
	}
}

// restart is tier 1: relaunch this stream's goroutine and nothing else.
func (runner *streamRunner) restart() {
	runner.mutex.Lock()
	task := runner.task
	runner.mutex.Unlock()
	if task == nil {
		return
	}
	runner.socket.process.options.Health.StreamRestarted(runner.specification)
	task.Restart()
}

// onExit counts a leaked goroutine. There is no self-kill, so leaks accumulate;
// holding the venue at DEGRADED is what makes one impossible to miss.
func (runner *streamRunner) onExit(err error) {
	if !errors.Is(err, ErrLeaked) {
		return
	}
	runner.socket.process.countLeak()
}

// run publishes whatever the read loop hands over, until its context ends.
func (runner *streamRunner) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-runner.wake:
			message := runner.take()
			if message == nil {
				continue
			}
			runner.publish(ctx, *message)
		}
	}
}

// deliver hands the newest message to the goroutine without blocking the read
// loop: one slow stream must not stall the socket every other stream shares.
func (runner *streamRunner) deliver(message core.Message) {
	runner.mutex.Lock()
	runner.pending = &message
	runner.mutex.Unlock()

	select {
	case runner.wake <- struct{}{}:
	default:
	}
}

func (runner *streamRunner) take() *core.Message {
	runner.mutex.Lock()
	defer runner.mutex.Unlock()
	message := runner.pending
	runner.pending = nil
	return message
}

// publish stamps the status the tracker computed and writes the message.
//
// A failed write is counted and rate-limit logged by the publisher and is not
// escalated here: the key then expires on its own, which is the trigger the
// fallback and the restart tiers are both built on.
func (runner *streamRunner) publish(ctx context.Context, message core.Message) {
	process := runner.socket.process

	// Fallback first: this message is the evidence the socket is delivering
	// again, and asking for a status before disengaging would report the
	// stream as still on REST.
	if process.options.OnMessage != nil {
		process.options.OnMessage(message.Specification)
	}
	process.options.Health.Received(message.Specification)

	envelope := envelopeOf(message.Proto)
	if envelope == nil {
		process.options.Log.Error("message carries no envelope", "stream", message.Specification.String())
		return
	}
	// Never publish data without a status, and never one the adapter guessed:
	// the adapter knows the frame parsed, not whether the socket behind it is
	// healthy.
	envelope.Status, envelope.StatusReason = process.options.Health.Status(message.Specification)

	_ = process.options.Publisher.Publish(ctx, message.Key, message.Proto, message.TimeToLive)
}

// ---------- process-level accounting ----------

// countLeak increments the leaked goroutine count and pushes it into health,
// which holds the venue at DEGRADED for as long as it is above zero.
func (process *Process) countLeak() {
	process.mutex.Lock()
	process.leaked++
	n := process.leaked
	process.mutex.Unlock()

	process.options.Health.Leaked(n)
}

// Leaked is how many goroutines have failed to return within the timeout.
func (process *Process) Leaked() int {
	process.mutex.Lock()
	defer process.mutex.Unlock()
	return process.leaked
}

// observeSkew records the venue's clock against ours. A venue clock ahead of
// ours reads positive; the sign is kept because losing it hides which way the
// two disagree.
//
// Only a timestamp the venue stamped as it sent the frame can be differenced
// against arrival. An event time — KuCoin stamps a funding rate with the
// settlement instant it describes, which is hours old by the time it is pushed
// — would read as a four-hour clock skew and take a perfectly healthy venue to
// STALE once a minute.
func (process *Process) observeSkew(message core.Message) {
	envelope := envelopeOf(message.Proto)
	if envelope == nil || !envelope.ExchangeTimeIsSendTime || envelope.ExchangeTimeNs <= 0 || envelope.RecvTimeNs <= 0 {
		return
	}
	process.options.Health.ClockSkew((envelope.ExchangeTimeNs - envelope.RecvTimeNs) / int64(time.Millisecond))
}

// countParseError classifies a rejected frame from the error itself rather than
// by matching strings, and counts a range rejection separately: it is the one
// parse failure that would otherwise have published a plausible wrong price.
func (process *Process) countParseError(socketID string, err error) {
	kind, channel := "unclassified", core.ChannelName(manoochv1.Channel_CHANNEL_UNSPECIFIED)

	var parseError *core.ParseError
	if errors.As(err, &parseError) {
		kind = parseError.Kind
		channel = core.ChannelName(parseError.Channel)
	}
	process.options.Metrics.ParseErrors.WithLabelValues(process.options.Venue, channel, kind).Inc()
	if kind == core.KindRange {
		process.options.Metrics.RangeErrors.WithLabelValues(process.options.Venue, channel).Inc()
	}
	process.options.Health.FrameRejected(socketID)

	now := process.now()
	process.mutex.Lock()
	log := now.Sub(process.lastParseLog) >= parseErrorLogInterval
	if log {
		process.lastParseLog = now
	}
	process.mutex.Unlock()

	if log {
		process.options.Log.Warn("frame not parsed", "socket", socketID, "kind", kind, "channel", channel, "error", err.Error())
	}
}

// envelopeOf reaches the envelope every payload in the schema carries in field
// 1.
func envelopeOf(m proto.Message) *manoochv1.Envelope {
	enveloped, ok := m.(interface{ GetEnv() *manoochv1.Envelope })
	if !ok {
		return nil
	}
	return enveloped.GetEnv()
}
