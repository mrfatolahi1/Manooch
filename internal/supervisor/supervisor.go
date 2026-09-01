package supervisor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/health"
	"github.com/you/manooch/internal/obs"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/internal/transport"
	"google.golang.org/protobuf/proto"
)

// parseErrLogInterval caps the parse-error log rate. A venue sending frames we
// cannot read is one fact, not one fact per frame.
const parseErrLogInterval = time.Second

// defaultExpiryWindow is how long an expired key counts towards the quorum that
// redials a socket. Wide enough that keys on the same cadence expiring in
// sequence are seen together, short enough that yesterday's outage does not.
const defaultExpiryWindow = 10 * time.Second

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
	Metrics   *obs.Metrics
	Log       *slog.Logger

	// StreamBackoff is the wait before relaunching one stream goroutine;
	// SocketBackoff the wait before redialling a socket.
	StreamBackoff transport.Policy
	SocketBackoff transport.Policy

	// Breaker stops connection attempts entirely after repeated failure.
	Breaker transport.BreakerOptions

	// LeakTimeout bounds the wait for a stopped goroutine to return.
	LeakTimeout time.Duration

	// ExpiryWindow is how long an expired key counts towards the quorum that
	// escalates from restarting one stream to redialling the socket. Zero
	// means defaultExpiryWindow.
	ExpiryWindow time.Duration

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
	opts Options
	now  func() time.Time

	sockets  []*socketRunner
	byStream map[core.StreamSpec]*socketRunner

	mu           sync.Mutex
	leaked       int
	lastParseLog time.Time
}

// New builds the supervision tree. It opens nothing.
func New(opts Options) (*Process, error) {
	switch {
	case opts.Venue == "":
		return nil, errors.New("supervisor: no venue")
	case opts.Adapter == nil:
		return nil, errors.New("supervisor: no adapter")
	case len(opts.Plans) == 0:
		return nil, errors.New("supervisor: no socket plans")
	case opts.Publisher == nil:
		return nil, errors.New("supervisor: no publisher")
	case opts.Health == nil:
		return nil, errors.New("supervisor: no health tracker")
	case opts.Metrics == nil:
		return nil, errors.New("supervisor: no metrics")
	case opts.Log == nil:
		return nil, errors.New("supervisor: no logger")
	case opts.LeakTimeout <= 0:
		return nil, fmt.Errorf("supervisor: goroutine leak timeout is %v", opts.LeakTimeout)
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.ExpiryWindow <= 0 {
		opts.ExpiryWindow = defaultExpiryWindow
	}

	p := &Process{opts: opts, now: opts.Now, byStream: map[core.StreamSpec]*socketRunner{}}
	for _, plan := range opts.Plans {
		s := newSocketRunner(p, plan)
		p.sockets = append(p.sockets, s)
		for _, spec := range plan.Specs {
			p.byStream[spec] = s
		}
	}
	return p, nil
}

// Run supervises every socket until ctx ends.
func (p *Process) Run(ctx context.Context) {
	var wg sync.WaitGroup
	for _, s := range p.sockets {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.run(ctx)
		}()
	}
	wg.Wait()
}

// KeyExpired escalates a stream whose Redis key reached its TTL.
//
// One key is tier 1: restart that stream's goroutine and nothing else. Enough
// of a socket's keys expiring together is tier 2: the socket is delivering for
// nobody, so redial it. There is no tier 3 — the process does not exit.
func (p *Process) KeyExpired(spec core.StreamSpec) {
	s := p.byStream[spec]
	if s == nil {
		return
	}
	s.noteExpiry(spec)
}

// ---------- socket ----------

// A socketRunner owns one websocket: dialling it, reading it, and the stream
// goroutines that publish what comes off it.
type socketRunner struct {
	p       *Process
	plan    core.SocketPlan
	breaker *transport.Breaker
	// quorum is how many of this socket's streams must expire together before
	// the failure is treated as the socket's rather than the streams'.
	quorum int

	streams map[core.StreamSpec]*streamRunner
	// order keeps stream startup deterministic, for logs and tests.
	order []*streamRunner

	mu        sync.Mutex
	conn      core.Conn
	expiredAt map[core.StreamSpec]time.Time
}

func newSocketRunner(p *Process, plan core.SocketPlan) *socketRunner {
	s := &socketRunner{
		p:         p,
		plan:      plan,
		breaker:   transport.NewBreaker(p.opts.Breaker),
		streams:   make(map[core.StreamSpec]*streamRunner, len(plan.Specs)),
		expiredAt: map[core.StreamSpec]time.Time{},
	}
	// Half the socket's streams, never fewer than two: one key expiring is a
	// stream problem, most of them expiring is a connection problem.
	s.quorum = max(2, len(plan.Specs)/2)

	for _, spec := range plan.Specs {
		if _, dup := s.streams[spec]; dup {
			continue
		}
		r := &streamRunner{s: s, spec: spec, wake: make(chan struct{}, 1)}
		s.streams[spec] = r
		s.order = append(s.order, r)
	}
	return s
}

// run dials and reads the socket until ctx ends, redialling with backoff.
func (s *socketRunner) run(ctx context.Context) {
	log := s.p.opts.Log.With("socket", s.plan.ID)

	for attempt := 0; ctx.Err() == nil; {
		// The breaker is asked before every attempt. While it is open no
		// connection is made at all — not a slow one, not a probe — because a
		// venue that is refusing us is asking to be left alone, and a client
		// that keeps knocking is how an IP ban is earned.
		if wait := s.breaker.Retry(); wait > 0 {
			s.p.opts.Health.SocketState(s.plan.ID, health.SocketCircuitOpen,
				fmt.Sprintf("%d consecutive failures", s.breaker.Failures()))
			log.Error("circuit open, making no connection attempt",
				"failures", s.breaker.Failures(), "retry_in", wait.String())
			if !transport.Wait(ctx, wait) {
				return
			}
			continue
		}

		s.p.opts.Health.SocketState(s.plan.ID, health.SocketDialing, "dialing")
		conn, err := s.p.opts.Adapter.Dial(ctx, s.plan)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			s.breaker.Fail()
			log.Error("dial failed", "error", err.Error(), "failures", s.breaker.Failures())
			if !s.p.opts.SocketBackoff.Sleep(ctx, attempt) {
				return
			}
			attempt++
			continue
		}

		s.breaker.Succeed()
		attempt = 0
		s.setConn(conn)
		s.p.opts.Health.SocketState(s.plan.ID, health.SocketConnected, "")
		log.Info("socket connected", "streams", len(s.plan.Specs))

		reason := s.session(ctx)

		s.closeConn()
		if ctx.Err() != nil {
			log.Info("socket closed")
			return
		}
		s.p.opts.Health.SocketState(s.plan.ID, health.SocketDialing, reason)
		s.p.opts.Health.Reconnected(s.plan.ID)
		log.Error("socket down, reconnecting", "reason", reason)

		if !s.p.opts.SocketBackoff.Sleep(ctx, attempt) {
			return
		}
		attempt++
	}
}

// session runs the read loop with the stream goroutines behind it, returning
// why it ended.
func (s *socketRunner) session(ctx context.Context) string {
	sctx, cancel := context.WithCancel(ctx)

	// Cancelling sctx does not unblock a read already parked in the socket.
	// Closing the connection is the only thing that does, so a watchdog does
	// it the moment the session context ends — including when the process is
	// shutting down and the read loop below is still waiting on a frame.
	watchdog := make(chan struct{})
	go func() {
		select {
		case <-sctx.Done():
			s.closeConn()
		case <-watchdog:
		}
	}()

	for _, r := range s.order {
		r.start(sctx)
	}

	// One deferred block rather than three, because the order is the restart
	// procedure in miniature and LIFO would invert it: cancel first, close the
	// connection second — a goroutine parked in Read never sees the cancel —
	// and only then wait for the goroutines to come back.
	defer func() {
		cancel()
		close(watchdog)
		s.closeConn()
		s.stopStreams()
	}()

	conn := s.currentConn()
	if conn == nil {
		return "connection closed"
	}

	for {
		frame, recvNs, err := conn.Read(sctx)
		if err != nil {
			if ctx.Err() != nil {
				return "shutting down"
			}
			return "read: " + err.Error()
		}
		s.handleFrame(frame, recvNs)
	}
}

// handleFrame turns one frame into published messages.
//
// A frame that will not parse is counted, rate-limit logged and skipped. One
// malformed frame is not a reason to go dark on every other stream, and the
// keys it would have refreshed expire on their own and report themselves stale.
func (s *socketRunner) handleFrame(frame []byte, recvNs int64) {
	msgs, err := s.p.opts.Adapter.Parse(frame, recvNs)
	if err != nil {
		s.p.countParseError(s.plan.ID, err)
		return
	}
	if len(msgs) == 0 {
		return // an ack, a pong or a heartbeat
	}

	// Recorded before publishing, so it is present even when Redis is refusing
	// writes — which is exactly when it is worth reading.
	s.p.observeSkew(msgs[0])

	counted := make(map[pb.Channel]bool, len(msgs))
	for _, m := range msgs {
		if !counted[m.Channel] {
			counted[m.Channel] = true
			s.p.opts.Metrics.WSFramesReceived.WithLabelValues(
				s.p.opts.Venue,
				core.MarketTypeName(m.Spec.Instrument.MarketType),
				core.ChannelName(m.Channel)).Inc()
		}
		if r := s.streams[m.Spec]; r != nil {
			r.deliver(m)
		}
	}
}

// noteExpiry records one expired key and decides which tier it is.
func (s *socketRunner) noteExpiry(spec core.StreamSpec) {
	s.mu.Lock()
	now := s.p.now()
	s.expiredAt[spec] = now
	n := 0
	for k, at := range s.expiredAt {
		if now.Sub(at) > s.p.opts.ExpiryWindow {
			delete(s.expiredAt, k)
			continue
		}
		n++
	}
	escalate := n >= s.quorum
	if escalate {
		clear(s.expiredAt)
	}
	r := s.streams[spec]
	s.mu.Unlock()

	if escalate {
		s.p.opts.Log.Error("streams expired together, redialing socket",
			"socket", s.plan.ID, "expired", n, "quorum", s.quorum)
		// Closing is what ends the session: the read loop is parked in the
		// socket and will not see anything else.
		s.closeConn()
		return
	}
	if r != nil {
		r.restart()
	}
}

func (s *socketRunner) setConn(c core.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conn = c
}

func (s *socketRunner) currentConn() core.Conn {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn
}

// closeConn drops the connection and unblocks whatever is reading it. It is
// safe to call from any goroutine and more than once.
func (s *socketRunner) closeConn() {
	s.mu.Lock()
	c := s.conn
	s.conn = nil
	s.mu.Unlock()

	if c != nil {
		_ = c.Close()
	}
}

// stopStreams waits for every stream goroutine to finish. The session context
// is already cancelled by the time it runs.
func (s *socketRunner) stopStreams() {
	for _, r := range s.order {
		r.wait()
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
	s    *socketRunner
	spec core.StreamSpec
	wake chan struct{}

	mu      sync.Mutex
	pending *core.Message
	task    *Task
}

// start launches the goroutine under supervision for one session.
func (r *streamRunner) start(ctx context.Context) {
	t := Start(ctx, TaskOptions{
		Name:        r.spec.String(),
		Run:         r.run,
		LeakTimeout: r.s.p.opts.LeakTimeout,
		Backoff:     r.s.p.opts.StreamBackoff,
		Log:         r.s.p.opts.Log,
		OnExit:      r.onExit,
		// No Unblock: this goroutine parks on its own channels and its
		// context, never on the socket, so cancelling is enough to free it.
		// The socket's connection is closed by the session, once, rather than
		// by each of the streams that share it.
	})
	r.mu.Lock()
	r.task = t
	r.mu.Unlock()
}

// wait blocks until the goroutine has stopped being supervised.
func (r *streamRunner) wait() {
	r.mu.Lock()
	t := r.task
	r.mu.Unlock()
	if t != nil {
		t.Wait()
	}
}

// restart is tier 1: relaunch this stream's goroutine and nothing else.
func (r *streamRunner) restart() {
	r.mu.Lock()
	t := r.task
	r.mu.Unlock()
	if t == nil {
		return
	}
	r.s.p.opts.Health.StreamRestarted(r.spec)
	t.Restart()
}

// onExit counts a leaked goroutine. There is no self-kill, so leaks accumulate;
// holding the venue at DEGRADED is what makes one impossible to miss.
func (r *streamRunner) onExit(err error) {
	if !errors.Is(err, ErrLeaked) {
		return
	}
	r.s.p.countLeak()
}

// run publishes whatever the read loop hands over, until its context ends.
func (r *streamRunner) run(ctx context.Context) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-r.wake:
			m := r.take()
			if m == nil {
				continue
			}
			r.publish(ctx, *m)
		}
	}
}

// deliver hands the newest message to the goroutine without blocking the read
// loop: one slow stream must not stall the socket every other stream shares.
func (r *streamRunner) deliver(m core.Message) {
	r.mu.Lock()
	r.pending = &m
	r.mu.Unlock()

	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *streamRunner) take() *core.Message {
	r.mu.Lock()
	defer r.mu.Unlock()
	m := r.pending
	r.pending = nil
	return m
}

// publish stamps the status the tracker computed and writes the message.
//
// A failed write is counted and rate-limit logged by the publisher and is not
// escalated here: the key then expires on its own, which is the trigger the
// fallback and the restart tiers are both built on.
func (r *streamRunner) publish(ctx context.Context, m core.Message) {
	p := r.s.p

	// Fallback first: this message is the evidence the socket is delivering
	// again, and asking for a status before disengaging would report the
	// stream as still on REST.
	if p.opts.OnMessage != nil {
		p.opts.OnMessage(m.Spec)
	}
	p.opts.Health.Received(m.Spec)

	env := envelopeOf(m.Proto)
	if env == nil {
		p.opts.Log.Error("message carries no envelope", "stream", m.Spec.String())
		return
	}
	// Never publish data without a status, and never one the adapter guessed:
	// the adapter knows the frame parsed, not whether the socket behind it is
	// healthy.
	env.Status, env.StatusReason = p.opts.Health.Status(m.Spec)

	_ = p.opts.Publisher.Publish(ctx, m.Key, m.Proto, m.TTL)
}

// ---------- process-level accounting ----------

// countLeak increments the leaked goroutine count and pushes it into health,
// which holds the venue at DEGRADED for as long as it is above zero.
func (p *Process) countLeak() {
	p.mu.Lock()
	p.leaked++
	n := p.leaked
	p.mu.Unlock()

	p.opts.Health.Leaked(n)
}

// Leaked is how many goroutines have failed to return within the timeout.
func (p *Process) Leaked() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.leaked
}

// observeSkew records the venue's clock against ours. A venue clock ahead of
// ours reads positive; the sign is kept because losing it hides which way the
// two disagree.
func (p *Process) observeSkew(m core.Message) {
	env := envelopeOf(m.Proto)
	if env == nil || env.ExchangeTimeNs <= 0 || env.RecvTimeNs <= 0 {
		return
	}
	p.opts.Health.ClockSkew((env.ExchangeTimeNs - env.RecvTimeNs) / int64(time.Millisecond))
}

// countParseError classifies a rejected frame from the error itself rather than
// by matching strings, and counts a range rejection separately: it is the one
// parse failure that would otherwise have published a plausible wrong price.
func (p *Process) countParseError(socketID string, err error) {
	kind, channel := "unclassified", core.ChannelName(pb.Channel_CHANNEL_UNSPECIFIED)

	var pe *core.ParseError
	if errors.As(err, &pe) {
		kind = pe.Kind
		channel = core.ChannelName(pe.Channel)
	}
	p.opts.Metrics.ParseErrors.WithLabelValues(p.opts.Venue, channel, kind).Inc()
	if kind == core.KindRange {
		p.opts.Metrics.RangeErrors.WithLabelValues(p.opts.Venue, channel).Inc()
	}
	p.opts.Health.FrameRejected(socketID)

	now := p.now()
	p.mu.Lock()
	log := now.Sub(p.lastParseLog) >= parseErrLogInterval
	if log {
		p.lastParseLog = now
	}
	p.mu.Unlock()

	if log {
		p.opts.Log.Warn("frame not parsed", "socket", socketID, "kind", kind, "channel", channel, "error", err.Error())
	}
}

// envelopeOf reaches the envelope every payload in the schema carries in field 1.
func envelopeOf(m proto.Message) *pb.Envelope {
	e, ok := m.(interface{ GetEnv() *pb.Envelope })
	if !ok {
		return nil
	}
	return e.GetEnv()
}
