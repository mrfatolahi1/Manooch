package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/adapter"
	"github.com/you/manooch/internal/config"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/obs"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/internal/synth"
)

// parseErrLogInterval caps the parse-error log rate. A venue sending frames we
// cannot read is one fact, not one fact per frame, and nothing may log per
// message at any level.
const parseErrLogInterval = time.Second

// A feed runs one venue adapter's sockets and publishes what comes off them.
//
// It does not reconnect. A dropped socket logs at ERROR and stops the process,
// which is loud and obvious; making it survivable is a later phase's job and
// needs a supervisor this deliberately is not.
type feed struct {
	adapter core.Adapter
	pub     publish.Publisher
	metrics *obs.Metrics
	log     *slog.Logger
	venue   string

	// stop ends the whole process when a socket dies.
	stop context.CancelFunc

	mu           sync.Mutex
	lastParseLog time.Time
}

// producers is what will feed the publisher once Redis is up: either the
// synthetic generator or a venue adapter's sockets.
type producers struct {
	synthetic bool
	adapter   core.Adapter
	plans     []core.SocketPlan
}

// planProducers resolves everything the config can get wrong. It opens
// nothing — no socket, no REST call — so an unknown venue or a stream this
// venue cannot serve fails at startup rather than becoming a key nobody ever
// writes, which reads exactly like a venue that went quiet.
func planProducers(f flags, cfg *config.Config) (*producers, error) {
	if f.synthetic {
		return &producers{synthetic: true}, nil
	}

	a, err := adapter.New(cfg)
	if err != nil {
		return nil, err
	}
	specs, err := adapter.Specs(cfg)
	if err != nil {
		return nil, err
	}
	plans, err := a.PlanSubscriptions(specs)
	if err != nil {
		return nil, err
	}
	if len(plans) == 0 {
		return nil, errors.New("config declares no streams")
	}
	return &producers{adapter: a, plans: plans}, nil
}

// start launches the producers and returns a group that closes once they have
// all stopped.
func (p *producers) start(ctx context.Context, cfg *config.Config, pub publish.Publisher, metrics *obs.Metrics, log *slog.Logger, stop context.CancelFunc) *sync.WaitGroup {
	var wg sync.WaitGroup

	if p.synthetic {
		log.Warn("synthetic mode: publishing generated data, not venue data")
		gen := synth.New(cfg, pub, log)
		wg.Add(1)
		go func() {
			defer wg.Done()
			gen.Run(ctx)
		}()
		return &wg
	}

	f := &feed{
		adapter: p.adapter,
		pub:     pub,
		metrics: metrics,
		log:     log,
		venue:   p.adapter.Venue(),
		stop:    stop,
	}
	log.Info("venue adapter ready", "sockets", len(p.plans))

	for _, plan := range p.plans {
		wg.Add(1)
		go func() {
			defer wg.Done()
			f.runSocket(ctx, plan)
		}()
	}
	return &wg
}

// runSocket dials one plan and reads it to completion.
func (f *feed) runSocket(ctx context.Context, plan core.SocketPlan) {
	// Closed from this goroutine on the way out, and from the watchdog below
	// when the context ends: cancelling a context does not unblock a read that
	// is already parked in the socket, and only Close does.
	conn, err := f.adapter.Dial(ctx, plan)
	if err != nil {
		f.fail(plan, "dial", err)
		return
	}
	f.log.Info("socket connected", "socket", plan.ID, "streams", len(plan.Specs))

	var closeOnce sync.Once
	closeConn := func() { closeOnce.Do(func() { _ = conn.Close() }) }
	defer closeConn()

	watchdogDone := make(chan struct{})
	defer close(watchdogDone)
	go func() {
		select {
		case <-ctx.Done():
			closeConn()
		case <-watchdogDone:
		}
	}()

	for {
		frame, recvNs, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				f.log.Info("socket closed", "socket", plan.ID)
				return
			}
			f.fail(plan, "read", err)
			return
		}
		f.handleFrame(ctx, plan, frame, recvNs)
	}
}

// handleFrame turns one frame into published messages. A frame we cannot parse
// is counted and, at most once a second, logged; it does not stop the socket.
// One malformed frame is not a reason to go dark on every other stream, and the
// keys it would have refreshed expire on their own and report themselves stale.
func (f *feed) handleFrame(ctx context.Context, plan core.SocketPlan, frame []byte, recvNs int64) {
	msgs, err := f.adapter.Parse(frame, recvNs)
	if err != nil {
		f.countParseError(plan, err)
		return
	}
	if len(msgs) == 0 {
		return // an ack, a pong, or a heartbeat
	}

	// The gap between the venue's clock and ours. Recorded before publishing
	// so it is present even if Redis is refusing writes, which is exactly when
	// it is worth reading.
	f.observeSkew(msgs[0])

	// Counted once per channel the frame carried, so a quiet channel is
	// visible per stream rather than hidden in a socket-wide total.
	counted := make(map[pb.Channel]bool, len(msgs))
	for _, m := range msgs {
		if !counted[m.Channel] {
			counted[m.Channel] = true
			f.metrics.WSFramesReceived.WithLabelValues(
				f.venue,
				core.MarketTypeName(m.Spec.Instrument.MarketType),
				core.ChannelName(m.Channel),
			).Inc()
		}
		// Already counted and rate-limit logged by the publisher.
		_ = f.pub.Publish(ctx, m.Key, m.Proto, m.TTL)
	}
}

// observeSkew records exchange time minus receive time. A venue clock ahead of
// ours reads positive; the sign is kept because losing it would hide which way
// the two disagree, and every freshness number depends on the answer.
func (f *feed) observeSkew(m core.Message) {
	env, ok := m.Proto.(interface{ GetEnv() *pb.Envelope })
	if !ok {
		return
	}
	e := env.GetEnv()
	if e == nil || e.ExchangeTimeNs <= 0 || e.RecvTimeNs <= 0 {
		return
	}
	skewMS := float64(e.ExchangeTimeNs-e.RecvTimeNs) / float64(time.Millisecond)
	f.metrics.ClockSkewMS.WithLabelValues(f.venue).Set(skewMS)
}

// countParseError classifies the failure from the error itself rather than by
// matching strings, and counts a range rejection separately: it is the one
// parse failure that would otherwise have published a plausible wrong price.
func (f *feed) countParseError(plan core.SocketPlan, err error) {
	kind, channel := "unclassified", core.ChannelName(pb.Channel_CHANNEL_UNSPECIFIED)

	var pe *core.ParseError
	if errors.As(err, &pe) {
		kind = pe.Kind
		channel = core.ChannelName(pe.Channel)
	}
	f.metrics.ParseErrors.WithLabelValues(f.venue, channel, kind).Inc()
	if kind == core.KindRange {
		f.metrics.RangeErrors.WithLabelValues(f.venue, channel).Inc()
	}

	now := time.Now()
	f.mu.Lock()
	log := now.Sub(f.lastParseLog) >= parseErrLogInterval
	if log {
		f.lastParseLog = now
	}
	f.mu.Unlock()

	if log {
		f.log.Warn("frame not parsed", "socket", plan.ID, "kind", kind, "channel", channel, "error", err.Error())
	}
}

// fail reports a dead socket and stops the process.
//
// M1 does not reconnect. Exiting is louder than retrying quietly and leaves
// the operator's restart policy in charge, which is the honest behaviour until
// there is a supervisor that can say what it is doing.
func (f *feed) fail(plan core.SocketPlan, op string, err error) {
	f.log.Error("socket failed, shutting down",
		"socket", plan.ID, "op", op, "error", err.Error(),
		"note", "this build does not reconnect")
	f.stop()
}
