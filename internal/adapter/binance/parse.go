package binance

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/pkg/price"
	"google.golang.org/protobuf/proto"
)

// frame is one combined-stream envelope. The single-symbol endpoint sends the
// payload bare, so data is optional and the payload is decoded twice when it is
// absent rather than being required to be wrapped.
type frame struct {
	Stream string          `json:"stream"`
	Data   json.RawMessage `json:"data"`

	// A subscription acknowledgement: {"result":null,"id":1}. ID is a pointer
	// because id 0 is a legal request id and must not read as absent.
	ID *int64 `json:"id"`

	// An error the venue sent us: {"error":{"code":-1121,"msg":"Invalid symbol"}}.
	Error *venueError `json:"error"`
}

type venueError struct {
	Code int64  `json:"code"`
	Msg  string `json:"msg"`
}

// markPriceUpdate is the payload documented at
// https://developers.binance.com/docs/derivatives/usds-margined-futures/websocket-market-streams
//
// Unknown fields are ignored rather than rejected: the venue adds them without
// warning — "ap" on the single-symbol stream, "st" after the CM migration —
// and erroring on one would take the feed down for a field we do not read.
type markPriceUpdate struct {
	Event           string `json:"e"`
	EventTimeMS     int64  `json:"E"`
	Symbol          string `json:"s"`
	MarkPrice       string `json:"p"`
	IndexPrice      string `json:"i"`
	FundingRate     string `json:"r"`
	NextFundingMS   int64  `json:"T"`
	EstimatedSettle string `json:"P"` // dropped: no proto field, and only
	// meaningful in the final hour before settlement
}

// Parse converts one frame into zero or more messages.
//
// It is pure: given the same bytes and the same recvNs it returns the same
// messages, every time, with no clock read and no map iteration. That is what
// makes fixture replay a real test rather than an approximation, and it is why
// publish_time_ns is stamped by the publisher and not here.
//
// A frame that is not data returns (nil, nil): acks and heartbeats are normal
// traffic, not failures.
func (a *Adapter) Parse(raw []byte, recvNs int64) ([]core.Message, error) {
	var f frame
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, core.NewParseError(core.KindJSON, pb.Channel_CHANNEL_UNSPECIFIED, "", err, "frame is not json")
	}

	if f.Error != nil {
		return nil, core.NewParseError(core.KindVenue, pb.Channel_CHANNEL_UNSPECIFIED, "", nil,
			"venue error %d: %s", f.Error.Code, f.Error.Msg)
	}

	// A subscription acknowledgement carries an id and no data.
	body := f.Data
	if len(body) == 0 {
		if f.ID != nil {
			return nil, nil
		}
		body = raw // the single-symbol endpoint sends the payload unwrapped
	}

	var u markPriceUpdate
	if err := json.Unmarshal(body, &u); err != nil {
		return nil, core.NewParseError(core.KindJSON, pb.Channel_CHANNEL_UNSPECIFIED, "", err, "payload is not json")
	}
	if u.Event == "" && u.Symbol == "" {
		return nil, nil // a pong or another control frame with no payload
	}
	if u.Event != eventMarkPriceUpdate {
		return nil, core.NewParseError(core.KindField, pb.Channel_CHANNEL_UNSPECIFIED, u.Symbol, nil,
			"unhandled event type %q", u.Event)
	}

	ref, err := a.ParseVenueSymbol(u.Symbol, MarketType)
	if err != nil {
		return nil, core.NewParseError(core.KindField, pb.Channel_CHANNEL_UNSPECIFIED, u.Symbol, err, "symbol")
	}
	if u.EventTimeMS <= 0 {
		return nil, core.NewParseError(core.KindField, pb.Channel_CHANNEL_UNSPECIFIED, u.Symbol, nil,
			"event time is %d", u.EventTimeMS)
	}
	exchangeNs := msToNs(u.EventTimeMS)

	// The instrument is built once and shared: the three messages describe the
	// same instrument at the same instant, and the publisher only reads it.
	instrument := ref.Proto(strings.ToUpper(u.Symbol))

	mark, err := price.ParsePrice(u.MarkPrice)
	if err != nil {
		return nil, numericError(pb.Channel_CHANNEL_MARK_PRICE, u.Symbol, "p", u.MarkPrice, err)
	}
	index, err := price.ParsePrice(u.IndexPrice)
	if err != nil {
		return nil, numericError(pb.Channel_CHANNEL_INDEX_PRICE, u.Symbol, "i", u.IndexPrice, err)
	}

	msgs := make([]core.Message, 0, len(Channels))
	msgs = append(msgs,
		a.message(ref, instrument, pb.Channel_CHANNEL_MARK_PRICE, exchangeNs, recvNs, func(env *pb.Envelope) proto.Message {
			return &pb.MarkPrice{Env: env, MarkPrice: int64(mark)}
		}),
		a.message(ref, instrument, pb.Channel_CHANNEL_INDEX_PRICE, exchangeNs, recvNs, func(env *pb.Envelope) proto.Message {
			return &pb.IndexPrice{Env: env, IndexPrice: int64(index)}
		}),
	)

	// Delivery symbols answer "" for the funding rate and 0 for the next
	// funding time. We only subscribe to perpetuals, so this should not
	// happen — but zero is a real funding rate and empty is missing data, so
	// the message is skipped rather than published as a rate of zero.
	if u.FundingRate == "" || u.NextFundingMS <= 0 {
		return msgs, nil
	}
	rate, err := price.ParseRate(u.FundingRate)
	if err != nil {
		return nil, numericError(pb.Channel_CHANNEL_FUNDING, u.Symbol, "r", u.FundingRate, err)
	}
	nextNs := msToNs(u.NextFundingMS)
	msgs = append(msgs,
		a.message(ref, instrument, pb.Channel_CHANNEL_FUNDING, exchangeNs, recvNs, func(env *pb.Envelope) proto.Message {
			return &pb.Funding{
				Env:               env,
				FundingRate:       int64(rate),
				NextFundingTimeNs: nextNs,
				// Binance publishes the interval per symbol on a REST endpoint
				// this adapter does not read. Left at zero rather than assumed
				// to be eight hours, which stopped being true for every symbol.
			}
		}),
	)
	return msgs, nil
}

// message builds one normalized message. build receives the envelope so each
// payload owns its own: sharing one Envelope pointer across three messages
// would have the publisher stamp the same publish_seq into all of them.
func (a *Adapter) message(
	ref core.InstrumentRef,
	instrument *pb.Instrument,
	ch pb.Channel,
	exchangeNs, recvNs int64,
	build func(*pb.Envelope) proto.Message,
) core.Message {
	spec := core.StreamSpec{Instrument: ref, Channel: ch}
	env := &pb.Envelope{
		Venue:          Venue,
		Instrument:     instrument,
		Channel:        ch,
		ExchangeTimeNs: exchangeNs,
		RecvTimeNs:     recvNs,
		// Binance stamps the mark price stream and the premiumIndex response
		// with the instant it answered, so the difference against arrival is a
		// clock comparison rather than the age of the value.
		ExchangeTimeIsSendTime: true,
		// Binance's mark price stream carries no sequence number. Saying so is
		// the point: an invented one would let a consumer believe it can
		// detect venue-side gaps here, which it cannot.
		VenueSeqPresent: false,
		Source:          pb.Source_SOURCE_WEBSOCKET,
		Status:          pb.Status_STATUS_HEALTHY,
	}
	return core.Message{
		Key:     publish.Key(Venue, ref.MarketType, ref.Canonical(), ch),
		Proto:   build(env),
		TTL:     a.opts.TTLs[ch],
		Channel: ch,
		Spec:    spec,
	}
}

// numericError classifies a rejected decimal. A value that does not fit the
// scale is counted apart from a malformed one: it is the single failure that
// would otherwise publish a plausible wrong price, and it must never be
// clamped or wrapped into range.
func numericError(ch pb.Channel, symbol, field, value string, err error) error {
	kind := core.KindField
	if errors.Is(err, price.ErrOutOfRange) || errors.Is(err, price.ErrPrecisionLoss) {
		kind = core.KindRange
	}
	return core.NewParseError(kind, ch, symbol, err, "field %q = %q", field, value)
}

// msToNs converts Binance's millisecond timestamps to the nanoseconds every
// timestamp on the wire is in.
func msToNs(ms int64) int64 { return ms * int64(time.Millisecond) }
