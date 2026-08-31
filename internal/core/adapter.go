package core

import (
	"context"
	"errors"
	"fmt"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"google.golang.org/protobuf/proto"
)

// An Adapter is implemented once per venue. It is the only place a venue's
// habits are allowed to exist: timestamp units, symbol casing, split subjects,
// connection bootstrapping. Nothing venue-specific may leak past this
// interface, or every caller downstream grows a switch on the venue name.
//
// Implementations hold no stream lifecycle state — no reconnect counters, no
// last-seen timestamps, no goroutines. The caller owns all of that, which is
// what lets it be replaced in a later phase without touching a venue package.
type Adapter interface {
	// Venue returns the canonical upper-case venue name, e.g. "BINANCE".
	Venue() string

	// VenueSymbol maps canonical identity to the venue's native string:
	// BTC_USDT + PERP_LINEAR is "BTCUSDT" on Binance, "XBTUSDTM" on KuCoin.
	VenueSymbol(ref InstrumentRef) (string, error)

	// ParseVenueSymbol is the inverse, used when reading REST responses that
	// echo the venue's own symbol.
	ParseVenueSymbol(s string, mt pb.MarketType) (InstrumentRef, error)

	// PlanSubscriptions groups requested streams onto sockets, respecting the
	// venue's limit on streams per connection.
	PlanSubscriptions(specs []StreamSpec) ([]SocketPlan, error)

	// Dial opens one websocket for one plan and completes the subscription
	// handshake. It returns only once the subscriptions are acknowledged, or
	// an error. Any REST call needed to obtain a connection URL happens here.
	Dial(ctx context.Context, plan SocketPlan) (Conn, error)

	// Parse converts one raw frame into zero or more normalized messages.
	// recvNs was stamped in the read loop before this call.
	//
	// Returning (nil, nil) is valid: pongs, acks and heartbeats carry no data.
	Parse(frame []byte, recvNs int64) ([]Message, error)

	// FetchOnce is the REST fallback for a single stream.
	FetchOnce(ctx context.Context, spec StreamSpec) ([]Message, error)

	// FetchMetadata reads the venue's public instrument endpoint.
	FetchMetadata(ctx context.Context, mt pb.MarketType) ([]*pb.InstrumentMeta, error)

	// RESTCost returns the venue's own weight for an operation, which is what
	// a rate limiter has to budget against.
	RESTCost(op Operation) int
}

// ErrNotImplemented is what an adapter returns for a method a later phase
// wires up. It is a distinct error so a caller can tell "this venue cannot do
// that" from "that failed".
var ErrNotImplemented = errors.New("core: not implemented")

// An Operation names a REST call for rate-limit accounting.
type Operation int

// The REST operations an adapter performs.
const (
	OpUnspecified Operation = iota
	OpFetchOnce
	OpFetchMetadata
)

// String renders the operation for logs and metric labels.
func (o Operation) String() string {
	switch o {
	case OpFetchOnce:
		return "fetch_once"
	case OpFetchMetadata:
		return "fetch_metadata"
	default:
		return "unspecified"
	}
}

// A StreamSpec is one instrument on one channel: exactly one Redis key, and
// the unit everything upstream of the publisher is scheduled in.
type StreamSpec struct {
	Instrument InstrumentRef
	Channel    pb.Channel
}

// String renders the spec for logs: "BTC_USDT:PERP_LINEAR mark_price".
func (s StreamSpec) String() string {
	return s.Instrument.String() + " " + ChannelName(s.Channel)
}

// A SocketPlan is the set of streams one websocket will carry. The adapter
// decides the grouping because only it knows the venue's limits; the caller
// decides when to dial.
type SocketPlan struct {
	// ID is stable across runs for the same config, so a log line or a
	// reconnect metric points at the same socket between restarts.
	ID    string
	Specs []StreamSpec
}

// A Message is one normalized payload ready to publish. The adapter fills
// everything here; the publisher stamps the rest of the envelope.
type Message struct {
	// Key is built with publish.Key, never by concatenation.
	Key string
	// Proto is the payload: MarkPrice, IndexPrice, Funding, InstrumentMeta.
	Proto proto.Message
	// TTL is 0 for a key that never expires.
	TTL     time.Duration
	Channel pb.Channel
	Spec    StreamSpec
}

// A Conn is one open websocket. transport implements it; adapters return it
// from Dial and the read loop drives it.
type Conn interface {
	// Read blocks for the next frame and returns it with the wall-clock
	// nanoseconds at which it arrived.
	Read(ctx context.Context) (frame []byte, recvNs int64, err error)

	// Write sends one frame, for venues that need client-initiated pings.
	Write(ctx context.Context, b []byte) error

	// Close is safe to call from a goroutine other than the one in Read, and
	// unblocks it. Cancelling a context does not.
	Close() error
}

// Parse error kinds. They are the "kind" label on manooch_parse_errors_total,
// so they are a closed set rather than free text.
const (
	// KindJSON is a frame that is not the shape the venue documents.
	KindJSON = "json"
	// KindField is a field that is present but unusable: a malformed decimal,
	// a symbol that maps to no instrument, an event type we do not handle.
	KindField = "field"
	// KindRange is a number that does not fit the fixed-point scale exactly.
	// It is counted separately because it is the one parse failure that would
	// otherwise publish a plausible wrong price.
	KindRange = "range"
	// KindVenue is an error frame the venue sent us.
	KindVenue = "venue"
)

// A ParseError is what Adapter.Parse returns for a frame it cannot turn into
// messages. It carries the metric labels the caller needs, so classification
// happens once here rather than by matching on error strings.
//
// Parse returns an error instead of a partial result: half a frame published
// as if it were whole is the silent wrongness this service exists to prevent.
type ParseError struct {
	Kind    string
	Channel pb.Channel
	Symbol  string // venue symbol, when the frame names one
	Msg     string
	Err     error
}

// Error renders the failure with its kind and, where known, its symbol.
func (e *ParseError) Error() string {
	s := "parse " + e.Kind
	if e.Symbol != "" {
		s += " " + e.Symbol
	}
	if e.Msg != "" {
		s += ": " + e.Msg
	}
	if e.Err != nil {
		s += ": " + e.Err.Error()
	}
	return s
}

// Unwrap exposes the underlying failure to errors.Is.
func (e *ParseError) Unwrap() error { return e.Err }

// NewParseError builds a ParseError. The variadic form keeps call sites at one
// line on a path that has a lot of them.
func NewParseError(kind string, ch pb.Channel, symbol string, err error, format string, args ...any) *ParseError {
	return &ParseError{Kind: kind, Channel: ch, Symbol: symbol, Msg: fmt.Sprintf(format, args...), Err: err}
}
