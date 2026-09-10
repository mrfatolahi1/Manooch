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

	"github.com/you/manooch/gen/manoochv1"
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

	mutex    sync.Mutex
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
func (connection *Conn) Push(frame []byte) { connection.frames <- delivery{frame: frame} }

// PushError queues a read failure: the socket dropping, a protocol error, a
// frame past the size limit.
func (connection *Conn) PushError(err error) { connection.frames <- delivery{err: err} }

// Wedge makes Close stop unblocking Read, which is a socket whose read call
// never comes back. It is how a leaked goroutine is produced on purpose.
func (connection *Conn) Wedge() {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	connection.wedged = true
}

// Silent makes Read return err after d with no frame, which is a connection
// that is up and delivering nothing — the failure TCP will otherwise hold open
// forever.
func (connection *Conn) Silent(duration time.Duration, err error) {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	connection.idle, connection.idleErr = duration, err
}

// FailWrites makes every Write return err.
func (connection *Conn) FailWrites(err error) {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	connection.writeErr = err
}

// Read blocks for the next frame. It does not watch ctx.
func (connection *Conn) Read(context.Context) ([]byte, int64, error) {
	connection.reads.Add(1)

	connection.mutex.Lock()
	wedged, idle, idleErr := connection.wedged, connection.idle, connection.idleErr
	connection.mutex.Unlock()

	var deadline <-chan time.Time
	if idle > 0 {
		timer := time.NewTimer(idle)
		defer timer.Stop()
		deadline = timer.C
	}

	closed := connection.closed
	if wedged {
		closed = nil // a read that Close does not free
	}

	select {
	case delivery := <-connection.frames:
		return delivery.frame, time.Now().UnixNano(), delivery.err
	case <-closed:
		return nil, time.Now().UnixNano(), ErrClosed
	case <-deadline:
		return nil, time.Now().UnixNano(), idleErr
	}
}

// Write records the frame.
func (connection *Conn) Write(_ context.Context, b []byte) error {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	if connection.writeErr != nil {
		return connection.writeErr
	}
	connection.writes = append(connection.writes, append([]byte(nil), b...))
	return nil
}

// Close unblocks Read, unless the connection has been wedged.
func (connection *Conn) Close() error {
	connection.closes.Add(1)
	connection.once.Do(func() { close(connection.closed) })
	return nil
}

// Reads is how many times Read has been entered.
func (connection *Conn) Reads() int64 { return connection.reads.Load() }

// Closes is how many times Close has been called.
func (connection *Conn) Closes() int64 { return connection.closes.Load() }

// IsClosed reports whether Close has been called.
func (connection *Conn) IsClosed() bool { return connection.closes.Load() > 0 }

// Writes is every frame written, in order.
func (connection *Conn) Writes() [][]byte {
	connection.mutex.Lock()
	defer connection.mutex.Unlock()
	return append([][]byte(nil), connection.writes...)
}

// ---------- Adapter ----------

// Venue is the venue name the double answers to.
const Venue = "TESTVENUE"

// MarketType is the only market the double serves.
const MarketType = manoochv1.MarketType_MARKET_TYPE_PERP_LINEAR

// An Adapter is a core.Adapter under test control.
//
// Its default Parse reads a frame as a bare canonical symbol — "BTC_USDT" —
// and emits one message per channel, so a test drives the pipeline by pushing
// a symbol into a Conn rather than by hand-building venue JSON.
type Adapter struct {
	// Channels is what one frame becomes. Zero means the three in scope.
	Channels []manoochv1.Channel
	// TTL is stamped on every message. Zero means one second.
	TimeToLive time.Duration
	// MaxStreamsPerSocket caps a plan. Zero means everything on one socket.
	MaxStreamsPerSocket int

	// DialFunc, ParseFunc, FetchFunc and MetadataFunc replace the defaults
	// below.
	DialFunc     func(context.Context, core.SocketPlan) (core.Conn, error)
	ParseFunc    func(frame []byte, receivedNs int64) ([]core.Message, error)
	FetchFunc    func(context.Context, core.StreamSpec) ([]core.Message, error)
	MetadataFunc func(context.Context, manoochv1.MarketType) ([]*manoochv1.InstrumentMeta, error)

	dials atomic.Int64
}

var _ core.Adapter = (*Adapter)(nil)

// Venue returns the double's venue name.
func (adapter *Adapter) Venue() string { return Venue }

// Dials is how many connection attempts have been made, which is what a
// circuit-breaker test asserts is zero.
func (adapter *Adapter) Dials() int64 { return adapter.dials.Load() }

func (adapter *Adapter) channels() []manoochv1.Channel {
	if len(adapter.Channels) > 0 {
		return adapter.Channels
	}
	return []manoochv1.Channel{
		manoochv1.Channel_CHANNEL_MARK_PRICE,
		manoochv1.Channel_CHANNEL_INDEX_PRICE,
		manoochv1.Channel_CHANNEL_FUNDING,
	}
}

func (adapter *Adapter) timeToLive() time.Duration {
	if adapter.TimeToLive > 0 {
		return adapter.TimeToLive
	}
	return time.Second
}

// VenueSymbol strips the separator: BTC_USDT becomes BTCUSDT.
func (adapter *Adapter) VenueSymbol(reference core.InstrumentRef) (string, error) {
	if reference.MarketType != MarketType {
		return "", fmt.Errorf("coretest: %s is not served", reference)
	}
	return strings.ReplaceAll(reference.Canonical(), "_", ""), nil
}

// ParseVenueSymbol splits on the quote assets the double knows.
func (adapter *Adapter) ParseVenueSymbol(s string, marketType manoochv1.MarketType) (core.InstrumentRef, error) {
	up := strings.ToUpper(s)
	if strings.Contains(up, "_") {
		return core.ParseCanonical(up, marketType)
	}
	for _, q := range []string{"USDT", "USDC", "USD"} {
		if base, ok := strings.CutSuffix(up, q); ok && base != "" {
			return core.ParseCanonical(base+"_"+q, marketType)
		}
	}
	return core.InstrumentRef{}, fmt.Errorf("coretest: %q ends in no known quote", s)
}

// PlanSubscriptions puts every specification on one socket unless
// MaxStreamsPerSocket says otherwise.
func (adapter *Adapter) PlanSubscriptions(specifications []core.StreamSpec) ([]core.SocketPlan, error) {
	if len(specifications) == 0 {
		return nil, nil
	}
	size := adapter.MaxStreamsPerSocket
	if size <= 0 {
		size = len(specifications)
	}
	var plans []core.SocketPlan
	for i := 0; i < len(specifications); i += size {
		plans = append(plans, core.SocketPlan{
			ID:             fmt.Sprintf("test-%d", len(plans)),
			Specifications: specifications[i:min(i+size, len(specifications))],
		})
	}
	return plans, nil
}

// Dial hands back whatever DialFunc returns, counting the attempt.
func (adapter *Adapter) Dial(ctx context.Context, plan core.SocketPlan) (core.Conn, error) {
	adapter.dials.Add(1)
	if adapter.DialFunc != nil {
		return adapter.DialFunc(ctx, plan)
	}
	return NewConn(), nil
}

// Parse turns one frame into messages.
func (adapter *Adapter) Parse(frame []byte, receivedNs int64) ([]core.Message, error) {
	if adapter.ParseFunc != nil {
		return adapter.ParseFunc(frame, receivedNs)
	}
	symbol := strings.TrimSpace(string(frame))
	if symbol == "" {
		return nil, nil // a pong: normal traffic, not a failure
	}
	reference, err := core.ParseCanonical(symbol, MarketType)
	if err != nil {
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_UNSPECIFIED, symbol, err, "symbol")
	}

	out := make([]core.Message, 0, len(adapter.channels()))
	for _, channel := range adapter.channels() {
		out = append(out, adapter.Message(core.StreamSpec{Instrument: reference, Channel: channel}, receivedNs, manoochv1.Source_SOURCE_WEBSOCKET))
	}
	return out, nil
}

// FetchOnce is the REST fallback. The default answers the requested channel
// with a message marked SOURCE_REST.
func (adapter *Adapter) FetchOnce(ctx context.Context, specification core.StreamSpec) ([]core.Message, error) {
	if adapter.FetchFunc != nil {
		return adapter.FetchFunc(ctx, specification)
	}
	return []core.Message{adapter.Message(specification, time.Now().UnixNano(), manoochv1.Source_SOURCE_REST)}, nil
}

// FetchMetadata answers with whatever MetadataFunc returns. Without one it
// answers ErrNotImplemented, which is what a venue that cannot do it says.
func (adapter *Adapter) FetchMetadata(ctx context.Context, marketType manoochv1.MarketType) ([]*manoochv1.InstrumentMeta, error) {
	if adapter.MetadataFunc != nil {
		return adapter.MetadataFunc(ctx, marketType)
	}
	return nil, core.ErrNotImplemented
}

// Metadata builds one instrument's metadata, with a valid envelope so the
// publisher accepts it.
func (adapter *Adapter) Metadata(reference core.InstrumentRef, tick price.Price, lot price.Size, receivedNs int64) *manoochv1.InstrumentMeta {
	venueSymbol, _ := adapter.VenueSymbol(reference)
	return &manoochv1.InstrumentMeta{
		Env: &manoochv1.Envelope{
			Venue:      Venue,
			Instrument: reference.Proto(venueSymbol),
			Channel:    manoochv1.Channel_CHANNEL_METADATA,
			RecvTimeNs: receivedNs,
			Source:     manoochv1.Source_SOURCE_REST,
			Status:     manoochv1.Status_STATUS_HEALTHY,
		},
		TickSize:           int64(tick),
		LotSize:            int64(lot),
		MinSize:            int64(lot),
		ContractMultiplier: price.SizeScale,
		Active:             true,
		LastRefreshNs:      receivedNs,
	}
}

// RESTCost is one weight unit for everything.
func (adapter *Adapter) RESTCost(core.Operation) int { return 1 }

// Message builds one normalized message for a specification, with a valid
// envelope: the publisher refuses anything whose status is unset, so a double
// that skipped the envelope would only ever test the rejection path.
func (adapter *Adapter) Message(specification core.StreamSpec, receivedNs int64, source manoochv1.Source) core.Message {
	venueSymbol, _ := adapter.VenueSymbol(specification.Instrument)
	v, _ := price.ParsePrice("68432.15")

	envelope := &manoochv1.Envelope{
		Venue:      Venue,
		Instrument: specification.Instrument.Proto(venueSymbol),
		Channel:    specification.Channel,
		// The double stamps the arrival instant as the venue's, so the skew it
		// reports is zero rather than absent: a test about reconnects should
		// not have to reason about a clock as well.
		ExchangeTimeNs:         receivedNs,
		RecvTimeNs:             receivedNs,
		ExchangeTimeIsSendTime: true,
		Source:                 source,
		Status:                 manoochv1.Status_STATUS_HEALTHY,
	}

	var payload proto.Message
	switch specification.Channel {
	case manoochv1.Channel_CHANNEL_INDEX_PRICE:
		payload = &manoochv1.IndexPrice{Env: envelope, IndexPrice: int64(v)}
	case manoochv1.Channel_CHANNEL_FUNDING:
		rate, _ := price.ParseRate("0.0001")
		payload = &manoochv1.Funding{Env: envelope, FundingRate: int64(rate), NextFundingTimeNs: receivedNs}
	default:
		payload = &manoochv1.MarkPrice{Env: envelope, MarkPrice: int64(v)}
	}

	return core.Message{
		Key:           publish.Key(Venue, specification.Instrument.MarketType, specification.Instrument.Canonical(), specification.Channel),
		Proto:         payload,
		TimeToLive:    adapter.timeToLive(),
		Channel:       specification.Channel,
		Specification: specification,
	}
}

// Specifications expands canonical symbols into one specification per channel.
func Specifications(symbols ...string) ([]core.StreamSpec, error) {
	adapter := &Adapter{}
	var out []core.StreamSpec
	for _, symbol := range symbols {
		reference, err := core.ParseCanonical(symbol, MarketType)
		if err != nil {
			return nil, err
		}
		for _, channel := range adapter.channels() {
			out = append(out, core.StreamSpec{Instrument: reference, Channel: channel})
		}
	}
	return out, nil
}
