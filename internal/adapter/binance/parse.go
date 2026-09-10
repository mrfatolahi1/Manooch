package binance

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/you/manooch/gen/manoochv1"
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
	Code    int64  `json:"code"`
	Message string `json:"msg"`
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
// It is pure: given the same bytes and the same receivedNs it returns the same
// messages, every time, with no clock read and no map iteration. That is what
// makes fixture replay a real test rather than an approximation, and it is why
// publish_time_ns is stamped by the publisher and not here.
//
// A frame that is not data returns (nil, nil): acks and heartbeats are normal
// traffic, not failures.
func (adapter *Adapter) Parse(raw []byte, receivedNs int64) ([]core.Message, error) {
	var frame frame
	if err := json.Unmarshal(raw, &frame); err != nil {
		return nil, core.NewParseError(core.KindJSON, manoochv1.Channel_CHANNEL_UNSPECIFIED, "", err, "frame is not json")
	}

	if frame.Error != nil {
		return nil, core.NewParseError(core.KindVenue, manoochv1.Channel_CHANNEL_UNSPECIFIED, "", nil,
			"venue error %d: %s", frame.Error.Code, frame.Error.Message)
	}

	// A subscription acknowledgement carries an id and no data.
	body := frame.Data
	if len(body) == 0 {
		if frame.ID != nil {
			return nil, nil
		}
		body = raw // the single-symbol endpoint sends the payload unwrapped
	}

	var update markPriceUpdate
	if err := json.Unmarshal(body, &update); err != nil {
		return nil, core.NewParseError(core.KindJSON, manoochv1.Channel_CHANNEL_UNSPECIFIED, "", err, "payload is not json")
	}
	if update.Event == "" && update.Symbol == "" {
		return nil, nil // a pong or another control frame with no payload
	}
	if update.Event != eventMarkPriceUpdate {
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_UNSPECIFIED, update.Symbol, nil,
			"unhandled event type %q", update.Event)
	}

	reference, err := adapter.ParseVenueSymbol(update.Symbol, MarketType)
	if err != nil {
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_UNSPECIFIED, update.Symbol, err, "symbol")
	}
	if update.EventTimeMS <= 0 {
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_UNSPECIFIED, update.Symbol, nil,
			"event time is %d", update.EventTimeMS)
	}
	exchangeNs := millisecondsToNanoseconds(update.EventTimeMS)

	// The instrument is built once and shared: the three messages describe the
	// same instrument at the same instant, and the publisher only reads it.
	instrument := reference.Proto(strings.ToUpper(update.Symbol))

	mark, err := price.ParsePrice(update.MarkPrice)
	if err != nil {
		return nil, numericError(manoochv1.Channel_CHANNEL_MARK_PRICE, update.Symbol, "p", update.MarkPrice, err)
	}
	index, err := price.ParsePrice(update.IndexPrice)
	if err != nil {
		return nil, numericError(manoochv1.Channel_CHANNEL_INDEX_PRICE, update.Symbol, "i", update.IndexPrice, err)
	}

	messages := make([]core.Message, 0, len(Channels))
	messages = append(messages,
		adapter.message(reference, instrument, manoochv1.Channel_CHANNEL_MARK_PRICE, exchangeNs, receivedNs, func(envelope *manoochv1.Envelope) proto.Message {
			return &manoochv1.MarkPrice{Env: envelope, MarkPrice: int64(mark)}
		}),
		adapter.message(reference, instrument, manoochv1.Channel_CHANNEL_INDEX_PRICE, exchangeNs, receivedNs, func(envelope *manoochv1.Envelope) proto.Message {
			return &manoochv1.IndexPrice{Env: envelope, IndexPrice: int64(index)}
		}),
	)

	// Delivery symbols answer "" for the funding rate and 0 for the next
	// funding time. We only subscribe to perpetuals, so this should not
	// happen — but zero is a real funding rate and empty is missing data, so
	// the message is skipped rather than published as a rate of zero.
	if update.FundingRate == "" || update.NextFundingMS <= 0 {
		return messages, nil
	}
	rate, err := price.ParseRate(update.FundingRate)
	if err != nil {
		return nil, numericError(manoochv1.Channel_CHANNEL_FUNDING, update.Symbol, "r", update.FundingRate, err)
	}
	nextNs := millisecondsToNanoseconds(update.NextFundingMS)
	messages = append(messages,
		adapter.message(reference, instrument, manoochv1.Channel_CHANNEL_FUNDING, exchangeNs, receivedNs, func(envelope *manoochv1.Envelope) proto.Message {
			return &manoochv1.Funding{
				Env:               envelope,
				FundingRate:       int64(rate),
				NextFundingTimeNs: nextNs,
				// Binance publishes the interval per symbol on a REST endpoint
				// this adapter does not read. Left at zero rather than assumed
				// to be eight hours, which stopped being true for every symbol.
			}
		}),
	)
	return messages, nil
}

// message builds one normalized message. build receives the envelope so each
// payload owns its own: sharing one Envelope pointer across three messages
// would have the publisher stamp the same publish_seq into all of them.
func (adapter *Adapter) message(
	reference core.InstrumentRef,
	instrument *manoochv1.Instrument,
	channel manoochv1.Channel,
	exchangeNs, receivedNs int64,
	build func(*manoochv1.Envelope) proto.Message,
) core.Message {
	specification := core.StreamSpec{Instrument: reference, Channel: channel}
	envelope := &manoochv1.Envelope{
		Venue:          Venue,
		Instrument:     instrument,
		Channel:        channel,
		ExchangeTimeNs: exchangeNs,
		RecvTimeNs:     receivedNs,
		// Binance stamps the mark price stream and the premiumIndex response
		// with the instant it answered, so the difference against arrival is a
		// clock comparison rather than the age of the value.
		ExchangeTimeIsSendTime: true,
		// Binance's mark price stream carries no sequence number. Saying so is
		// the point: an invented one would let a consumer believe it can
		// detect venue-side gaps here, which it cannot.
		VenueSeqPresent: false,
		Source:          manoochv1.Source_SOURCE_WEBSOCKET,
		Status:          manoochv1.Status_STATUS_HEALTHY,
	}
	return core.Message{
		Key:           publish.Key(Venue, reference.MarketType, reference.Canonical(), channel),
		Proto:         build(envelope),
		TimeToLive:    adapter.options.TimeToLive[channel],
		Channel:       channel,
		Specification: specification,
	}
}

// numericError classifies a rejected decimal. A value that does not fit the
// scale is counted apart from a malformed one: it is the single failure that
// would otherwise publish a plausible wrong price, and it must never be
// clamped or wrapped into range.
func numericError(channel manoochv1.Channel, symbol, field, value string, err error) error {
	kind := core.KindField
	if errors.Is(err, price.ErrOutOfRange) || errors.Is(err, price.ErrPrecisionLoss) {
		kind = core.KindRange
	}
	return core.NewParseError(kind, channel, symbol, err, "field %q = %q", field, value)
}

// millisecondsToNanoseconds converts Binance's millisecond timestamps to the
// nanoseconds every timestamp on the wire is in.
func millisecondsToNanoseconds(milliseconds int64) int64 {
	return milliseconds * int64(time.Millisecond)
}
