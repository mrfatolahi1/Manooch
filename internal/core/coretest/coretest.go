// Package coretest holds fault-injecting doubles for the core interfaces.
//
// Every recovery path in the service is a reaction to a connection
// misbehaving, and the ways a connection misbehaves are not reachable from a
// real socket on demand: it has to return an error now, block forever, hand
// back a frame nobody can parse, or go quiet past the read deadline. These
// doubles make each of those a method call.
package coretest

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/pkg/price"
	"google.golang.org/protobuf/proto"
)

// ErrClosed is what Read returns once the connection has been closed.
var ErrClosed = errors.New("coretest: connection closed")

// ---------- Conn ----------

// A Conn is a core.Conn under test control.
//
// Read never watches the caller's context, exactly like the real one: a
// goroutine parked in it is freed by Close and by nothing else. That is the
// mechanism the whole restart path depends on, so the double must not be more
// forgiving than the thing it stands in for.
type Conn struct {
	frames chan delivery
	closed chan struct{}
	once   sync.Once

	reads  atomic.Int64
	closes atomic.Int64

	mu       sync.Mutex
	wedged   bool
	idle     time.Duration
	idleErr  error
	writes   [][]byte
	writeErr error
}

var _ core.Conn = (*Conn)(nil)

type delivery struct {
	frame []byte
	err   error
}

// NewConn builds a connection that blocks in Read until something is pushed
// into it or it is closed.
func NewConn() *Conn {
	return &Conn{
		frames: make(chan delivery, 64),
		closed: make(chan struct{}),
	}
}

// Push queues one frame for the next Read.
func (c *Conn) Push(frame []byte) { c.frames <- delivery{frame: frame} }

// PushError queues a read failure: the socket dropping, a protocol error, a
// frame past the size limit.
func (c *Conn) PushError(err error) { c.frames <- delivery{err: err} }

// Wedge makes Close stop unblocking Read, which is a socket whose read call
// never comes back. It is how a leaked goroutine is produced on purpose.
func (c *Conn) Wedge() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wedged = true
}

// Silent makes Read return err after d with no frame, which is a connection
// that is up and delivering nothing — the failure TCP will otherwise hold open
// forever.
func (c *Conn) Silent(d time.Duration, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.idle, c.idleErr = d, err
}

// FailWrites makes every Write return err.
func (c *Conn) FailWrites(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.writeErr = err
}

// Read blocks for the next frame. It does not watch ctx.
func (c *Conn) Read(context.Context) ([]byte, int64, error) {
	c.reads.Add(1)

	c.mu.Lock()
	wedged, idle, idleErr := c.wedged, c.idle, c.idleErr
	c.mu.Unlock()

	var deadline <-chan time.Time
	if idle > 0 {
		t := time.NewTimer(idle)
		defer t.Stop()
		deadline = t.C
	}

	closed := c.closed
	if wedged {
		closed = nil // a read that Close does not free
	}

	select {
	case d := <-c.frames:
		return d.frame, time.Now().UnixNano(), d.err
	case <-closed:
		return nil, time.Now().UnixNano(), ErrClosed
	case <-deadline:
		return nil, time.Now().UnixNano(), idleErr
	}
}

// Write records the frame.
func (c *Conn) Write(_ context.Context, b []byte) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.writeErr != nil {
		return c.writeErr
	}
	c.writes = append(c.writes, append([]byte(nil), b...))
	return nil
}

// Close unblocks Read, unless the connection has been wedged.
func (c *Conn) Close() error {
	c.closes.Add(1)
	c.once.Do(func() { close(c.closed) })
	return nil
}

// Reads is how many times Read has been entered.
func (c *Conn) Reads() int64 { return c.reads.Load() }

// Closes is how many times Close has been called.
func (c *Conn) Closes() int64 { return c.closes.Load() }

// IsClosed reports whether Close has been called.
func (c *Conn) IsClosed() bool { return c.closes.Load() > 0 }

// Writes is every frame written, in order.
func (c *Conn) Writes() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([][]byte(nil), c.writes...)
}

// ---------- Adapter ----------

// Venue is the venue name the double answers to.
const Venue = "TESTVENUE"

// MarketType is the only market the double serves.
const MarketType = pb.MarketType_MARKET_TYPE_PERP_LINEAR

// An Adapter is a core.Adapter under test control.
//
// Its default Parse reads a frame as a bare canonical symbol — "BTC_USDT" —
// and emits one message per channel, so a test drives the pipeline by pushing
// a symbol into a Conn rather than by hand-building venue JSON.
type Adapter struct {
	// Channels is what one frame becomes. Zero means the three in scope.
	Channels []pb.Channel
	// TTL is stamped on every message. Zero means one second.
	TTL time.Duration
	// MaxStreamsPerSocket caps a plan. Zero means everything on one socket.
	MaxStreamsPerSocket int

	// DialFunc, ParseFunc, FetchFunc and MetadataFunc replace the defaults
	// below.
	DialFunc     func(context.Context, core.SocketPlan) (core.Conn, error)
	ParseFunc    func(frame []byte, recvNs int64) ([]core.Message, error)
	FetchFunc    func(context.Context, core.StreamSpec) ([]core.Message, error)
	MetadataFunc func(context.Context, pb.MarketType) ([]*pb.InstrumentMeta, error)

	dials atomic.Int64
}

var _ core.Adapter = (*Adapter)(nil)

// Venue returns the double's venue name.
func (a *Adapter) Venue() string { return Venue }

// Dials is how many connection attempts have been made, which is what a
// circuit-breaker test asserts is zero.
func (a *Adapter) Dials() int64 { return a.dials.Load() }

func (a *Adapter) channels() []pb.Channel {
	if len(a.Channels) > 0 {
		return a.Channels
	}
	return []pb.Channel{
		pb.Channel_CHANNEL_MARK_PRICE,
		pb.Channel_CHANNEL_INDEX_PRICE,
		pb.Channel_CHANNEL_FUNDING,
	}
}

func (a *Adapter) ttl() time.Duration {
	if a.TTL > 0 {
		return a.TTL
	}
	return time.Second
}

// VenueSymbol strips the separator: BTC_USDT becomes BTCUSDT.
func (a *Adapter) VenueSymbol(ref core.InstrumentRef) (string, error) {
	if ref.MarketType != MarketType {
		return "", fmt.Errorf("coretest: %s is not served", ref)
	}
	return strings.ReplaceAll(ref.Canonical(), "_", ""), nil
}

// ParseVenueSymbol splits on the quote assets the double knows.
func (a *Adapter) ParseVenueSymbol(s string, mt pb.MarketType) (core.InstrumentRef, error) {
	up := strings.ToUpper(s)
	if strings.Contains(up, "_") {
		return core.ParseCanonical(up, mt)
	}
	for _, q := range []string{"USDT", "USDC", "USD"} {
		if base, ok := strings.CutSuffix(up, q); ok && base != "" {
			return core.ParseCanonical(base+"_"+q, mt)
		}
	}
	return core.InstrumentRef{}, fmt.Errorf("coretest: %q ends in no known quote", s)
}

// PlanSubscriptions puts every spec on one socket unless MaxStreamsPerSocket
// says otherwise.
func (a *Adapter) PlanSubscriptions(specs []core.StreamSpec) ([]core.SocketPlan, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	size := a.MaxStreamsPerSocket
	if size <= 0 {
		size = len(specs)
	}
	var plans []core.SocketPlan
	for i := 0; i < len(specs); i += size {
		plans = append(plans, core.SocketPlan{
			ID:    fmt.Sprintf("test-%d", len(plans)),
			Specs: specs[i:min(i+size, len(specs))],
		})
	}
	return plans, nil
}

// Dial hands back whatever DialFunc returns, counting the attempt.
func (a *Adapter) Dial(ctx context.Context, plan core.SocketPlan) (core.Conn, error) {
	a.dials.Add(1)
	if a.DialFunc != nil {
		return a.DialFunc(ctx, plan)
	}
	return NewConn(), nil
}

// Parse turns one frame into messages.
func (a *Adapter) Parse(frame []byte, recvNs int64) ([]core.Message, error) {
	if a.ParseFunc != nil {
		return a.ParseFunc(frame, recvNs)
	}
	symbol := strings.TrimSpace(string(frame))
	if symbol == "" {
		return nil, nil // a pong: normal traffic, not a failure
	}
	ref, err := core.ParseCanonical(symbol, MarketType)
	if err != nil {
		return nil, core.NewParseError(core.KindField, pb.Channel_CHANNEL_UNSPECIFIED, symbol, err, "symbol")
	}

	out := make([]core.Message, 0, len(a.channels()))
	for _, ch := range a.channels() {
		out = append(out, a.Message(core.StreamSpec{Instrument: ref, Channel: ch}, recvNs, pb.Source_SOURCE_WEBSOCKET))
	}
	return out, nil
}

// FetchOnce is the REST fallback. The default answers the requested channel
// with a message marked SOURCE_REST.
func (a *Adapter) FetchOnce(ctx context.Context, spec core.StreamSpec) ([]core.Message, error) {
	if a.FetchFunc != nil {
		return a.FetchFunc(ctx, spec)
	}
	return []core.Message{a.Message(spec, time.Now().UnixNano(), pb.Source_SOURCE_REST)}, nil
}

// FetchMetadata answers with whatever MetadataFunc returns. Without one it
// answers ErrNotImplemented, which is what a venue that cannot do it says.
func (a *Adapter) FetchMetadata(ctx context.Context, mt pb.MarketType) ([]*pb.InstrumentMeta, error) {
	if a.MetadataFunc != nil {
		return a.MetadataFunc(ctx, mt)
	}
	return nil, core.ErrNotImplemented
}

// Meta builds one instrument's metadata, with a valid envelope so the
// publisher accepts it.
func (a *Adapter) Meta(ref core.InstrumentRef, tick price.Price, lot price.Size, recvNs int64) *pb.InstrumentMeta {
	venueSymbol, _ := a.VenueSymbol(ref)
	return &pb.InstrumentMeta{
		Env: &pb.Envelope{
			Venue:      Venue,
			Instrument: ref.Proto(venueSymbol),
			Channel:    pb.Channel_CHANNEL_METADATA,
			RecvTimeNs: recvNs,
			Source:     pb.Source_SOURCE_REST,
			Status:     pb.Status_STATUS_HEALTHY,
		},
		TickSize:           int64(tick),
		LotSize:            int64(lot),
		MinSize:            int64(lot),
		ContractMultiplier: price.SizeScale,
		Active:             true,
		LastRefreshNs:      recvNs,
	}
}

// RESTCost is one weight unit for everything.
func (a *Adapter) RESTCost(core.Operation) int { return 1 }

// Message builds one normalized message for a spec, with a valid envelope: the
// publisher refuses anything whose status is unset, so a double that skipped
// the envelope would only ever test the rejection path.
func (a *Adapter) Message(spec core.StreamSpec, recvNs int64, src pb.Source) core.Message {
	venueSymbol, _ := a.VenueSymbol(spec.Instrument)
	v, _ := price.ParsePrice("68432.15")

	env := &pb.Envelope{
		Venue:      Venue,
		Instrument: spec.Instrument.Proto(venueSymbol),
		Channel:    spec.Channel,
		// The double stamps the arrival instant as the venue's, so the skew it
		// reports is zero rather than absent: a test about reconnects should
		// not have to reason about a clock as well.
		ExchangeTimeNs:         recvNs,
		RecvTimeNs:             recvNs,
		ExchangeTimeIsSendTime: true,
		Source:                 src,
		Status:                 pb.Status_STATUS_HEALTHY,
	}

	var payload proto.Message
	switch spec.Channel {
	case pb.Channel_CHANNEL_INDEX_PRICE:
		payload = &pb.IndexPrice{Env: env, IndexPrice: int64(v)}
	case pb.Channel_CHANNEL_FUNDING:
		rate, _ := price.ParseRate("0.0001")
		payload = &pb.Funding{Env: env, FundingRate: int64(rate), NextFundingTimeNs: recvNs}
	default:
		payload = &pb.MarkPrice{Env: env, MarkPrice: int64(v)}
	}

	return core.Message{
		Key:     publish.Key(Venue, spec.Instrument.MarketType, spec.Instrument.Canonical(), spec.Channel),
		Proto:   payload,
		TTL:     a.ttl(),
		Channel: spec.Channel,
		Spec:    spec,
	}
}

// Specs expands canonical symbols into one spec per channel.
func Specs(symbols ...string) ([]core.StreamSpec, error) {
	a := &Adapter{}
	var out []core.StreamSpec
	for _, sym := range symbols {
		ref, err := core.ParseCanonical(sym, MarketType)
		if err != nil {
			return nil, err
		}
		for _, ch := range a.channels() {
			out = append(out, core.StreamSpec{Instrument: ref, Channel: ch})
		}
	}
	return out, nil
}
