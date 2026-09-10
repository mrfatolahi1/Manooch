package kucoin

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

// The two subjects /contract/instrument carries, at two different cadences:
// mark and index once a second, funding once a minute. That difference is why
// the venue file gives every channel its own cadence and therefore its own TTL.
const (
	subjectMarkIndex = "mark.index.price"
	subjectFunding   = "funding.rate"
)

// Plausible bounds on a millisecond timestamp: 2001-09-09 to 2286-11-20.
//
// KuCoin is not consistent about units across its futures topics — some carry
// nanoseconds — and /contract/instrument carrying milliseconds is a fact about
// today's venue, not a guarantee. A value outside this range is rejected rather
// than converted, because the alternative to noticing a unit change is
// publishing a timestamp that is off by a factor of a million and looks fine.
const (
	minTimestampMS = 1_000_000_000_000
	maxTimestampMS = 10_000_000_000_000
)

// frame is one websocket message. Data is left raw so the subject decides how
// to read it: the two subjects share a topic and share nothing else.
type frame struct {
	Topic   string          `json:"topic"`
	Type    string          `json:"type"`
	Subject string          `json:"subject"`
	Data    json.RawMessage `json:"data"`

	// Control fields, present on welcome, ack, pong and error frames.
	ID   string `json:"id"`
	Code int64  `json:"code"`
}

// markIndexData is the mark.index.price payload.
//
// The numbers are json.Number, never float64. KuCoin sends them unquoted —
// 90445.02, not "90445.02" — and unmarshalling that into a float64 would round
// it before pkg/price ever saw a digit, silently, defeating the entire
// fixed-point design. json.Number keeps the venue's own digit string, which is
// what price.ParsePrice is built to read.
type markIndexData struct {
	MarkPrice   json.Number `json:"markPrice"`
	IndexPrice  json.Number `json:"indexPrice"`
	Granularity int64       `json:"granularity"`
	TimestampMS int64       `json:"timestamp"`
}

// fundingData is the funding.rate payload. It carries no next-funding time; see
// Parse.
type fundingData struct {
	FundingRate json.Number `json:"fundingRate"`
	Granularity int64       `json:"granularity"`
	TimestampMS int64       `json:"timestamp"`
}

// Parse converts one frame into zero or more messages.
//
// It is pure: given the same bytes and the same receivedNs it returns the same
// messages, every time, with no clock read and no map iteration. That is what
// makes fixture replay a real test, and it is why publish_time_ns is stamped by
// the publisher and not here.
//
// A welcome, an ack or a pong returns (nil, nil): they are normal traffic on a
// healthy socket, and counting them as failures would show parse errors
// climbing on a connection that is working perfectly.
func (adapter *Adapter) Parse(raw []byte, receivedNs int64) ([]core.Message, error) {
	var frame frame
	if err := json.Unmarshal(raw, &frame); err != nil {
		return nil, core.NewParseError(core.KindJSON, manoochv1.Channel_CHANNEL_UNSPECIFIED, "", err, "frame is not json")
	}

	switch frame.Type {
	case typeError:
		// The venue puts its explanation in data, as a string. Passing it
		// through is the difference between a fixable error and "code 404".
		return nil, core.NewParseError(core.KindVenue, manoochv1.Channel_CHANNEL_UNSPECIFIED, "", nil,
			"venue error %d: %s", frame.Code, errorText(frame.Data))
	case typeWelcome, typeAck, typePong:
		return nil, nil
	}
	if len(frame.Data) == 0 {
		return nil, nil // a control frame with no payload
	}

	symbol, ok := symbolOf(frame.Topic)
	if !ok {
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_UNSPECIFIED, "", nil,
			"topic %q is not %s:{symbol}", frame.Topic, instrumentTopic)
	}
	reference, err := adapter.ParseVenueSymbol(symbol, MarketType)
	if err != nil {
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_UNSPECIFIED, symbol, err, "symbol")
	}

	switch frame.Subject {
	case subjectMarkIndex:
		return adapter.parseMarkIndex(reference, symbol, frame.Data, receivedNs)
	case subjectFunding:
		return adapter.parseFunding(reference, symbol, frame.Data, receivedNs)
	}
	return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_UNSPECIFIED, symbol, nil,
		"unhandled subject %q", frame.Subject)
}

// parseMarkIndex turns one mark.index.price payload into two messages on two
// keys, sharing one exchange time and one instrument.
func (adapter *Adapter) parseMarkIndex(reference core.InstrumentRef, symbol string, data []byte, receivedNs int64) ([]core.Message, error) {
	var markIndexData markIndexData
	if err := json.Unmarshal(data, &markIndexData); err != nil {
		return nil, core.NewParseError(core.KindJSON, manoochv1.Channel_CHANNEL_MARK_PRICE, symbol, err, "payload is not json")
	}
	exchangeNs, err := timestampNs(markIndexData.TimestampMS)
	if err != nil {
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_MARK_PRICE, symbol, err,
			"timestamp %d", markIndexData.TimestampMS)
	}

	mark, err := price.ParsePrice(markIndexData.MarkPrice.String())
	if err != nil {
		return nil, numericError(manoochv1.Channel_CHANNEL_MARK_PRICE, symbol, "markPrice", markIndexData.MarkPrice.String(), err)
	}
	index, err := price.ParsePrice(markIndexData.IndexPrice.String())
	if err != nil {
		return nil, numericError(manoochv1.Channel_CHANNEL_INDEX_PRICE, symbol, "indexPrice", markIndexData.IndexPrice.String(), err)
	}

	// The instrument is built once and shared: both messages describe the same
	// instrument at the same instant, and the publisher only reads it.
	instrument := reference.Proto(symbol)
	return []core.Message{
		adapter.message(reference, instrument, manoochv1.Channel_CHANNEL_MARK_PRICE, exchangeNs, receivedNs, func(envelope *manoochv1.Envelope) proto.Message {
			return &manoochv1.MarkPrice{Env: envelope, MarkPrice: int64(mark)}
		}),
		adapter.message(reference, instrument, manoochv1.Channel_CHANNEL_INDEX_PRICE, exchangeNs, receivedNs, func(envelope *manoochv1.Envelope) proto.Message {
			return &manoochv1.IndexPrice{Env: envelope, IndexPrice: int64(index)}
		}),
	}, nil
}

// parseFunding turns one funding.rate payload into one message.
//
// next_funding_time_ns is left at zero: this stream does not carry it, and
// KuCoin publishes only how long is left rather than when. Computing it from a
// funding interval and our own clock would put a number we made up under a
// field a consumer reads as the venue's — which is exactly the quiet wrongness
// this service exists to prevent. Zero says "not supplied", and
// adapter-kucoin.md says why.
//
// interval_seconds is left at zero for the same reason. The payload's
// granularity is how often the venue pushes this subject, not how often funding
// settles, and passing one off as the other would be a different wrong number.
func (adapter *Adapter) parseFunding(reference core.InstrumentRef, symbol string, data []byte, receivedNs int64) ([]core.Message, error) {
	var fundingData fundingData
	if err := json.Unmarshal(data, &fundingData); err != nil {
		return nil, core.NewParseError(core.KindJSON, manoochv1.Channel_CHANNEL_FUNDING, symbol, err, "payload is not json")
	}
	exchangeNs, err := timestampNs(fundingData.TimestampMS)
	if err != nil {
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_FUNDING, symbol, err,
			"timestamp %d", fundingData.TimestampMS)
	}
	if fundingData.FundingRate.String() == "" {
		// Missing, not zero. Zero is a real funding rate, and publishing an
		// absent one as zero is a number a strategy trades on.
		return nil, nil
	}
	rate, err := price.ParseRate(fundingData.FundingRate.String())
	if err != nil {
		return nil, numericError(manoochv1.Channel_CHANNEL_FUNDING, symbol, "fundingRate", fundingData.FundingRate.String(), err)
	}

	return []core.Message{
		adapter.message(reference, reference.Proto(symbol), manoochv1.Channel_CHANNEL_FUNDING, exchangeNs, receivedNs, func(envelope *manoochv1.Envelope) proto.Message {
			return &manoochv1.Funding{Env: envelope, FundingRate: int64(rate)}
		}),
	}, nil
}

// message builds one normalized message. build receives the envelope so each
// payload owns its own: sharing one Envelope pointer across two messages would
// have the publisher stamp the same publish_seq into both.
func (adapter *Adapter) message(
	reference core.InstrumentRef,
	instrument *manoochv1.Instrument,
	channel manoochv1.Channel,
	exchangeNs, receivedNs int64,
	build func(*manoochv1.Envelope) proto.Message,
) core.Message {
	specification := core.StreamSpec{Instrument: reference, Channel: channel}
	envelope := &manoochv1.Envelope{
		Venue:                  Venue,
		Instrument:             instrument,
		Channel:                channel,
		ExchangeTimeNs:         exchangeNs,
		RecvTimeNs:             receivedNs,
		ExchangeTimeIsSendTime: exchangeTimeIsSendTime(channel),
		// KuCoin's instrument topic carries no sequence number. Saying so is
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

// errorText reads the explanation an error frame carries in data. It is a
// string there, unlike every other frame where data is an object, so a failure
// to read it as one is not worth an error of its own.
func errorText(data []byte) string {
	var s string
	if err := json.Unmarshal(data, &s); err != nil {
		return string(data)
	}
	return s
}

// exchangeTimeIsSendTime reports whether this venue's timestamp on a channel is
// the instant it sent the value.
//
// mark.index.price is stamped as it is pushed, so the difference against
// arrival is a clock comparison. funding.rate is stamped with the settlement
// instant it describes, which is hours old by the time it arrives — and the
// REST endpoint answers the same way. Differencing that would report a
// four-hour clock skew on a venue that is working perfectly, and take every
// stream on it to STALE once a minute.
func exchangeTimeIsSendTime(channel manoochv1.Channel) bool {
	return channel != manoochv1.Channel_CHANNEL_FUNDING
}

// symbolOf splits "/contract/instrument:XBTUSDTM" into its symbol.
func symbolOf(topic string) (string, bool) {
	base, symbol, found := strings.Cut(topic, ":")
	if !found || base != instrumentTopic || symbol == "" {
		return "", false
	}
	return symbol, true
}

// errTimestampUnit is a timestamp that is not plausibly milliseconds since the
// epoch. It is its own error so the failure names the cause rather than
// arriving as an out-of-range price three fields later.
var errTimestampUnit = errors.New("kucoin: timestamp is not milliseconds since the epoch")

// timestampNs converts a millisecond timestamp, asserting the magnitude first.
//
// A venue that switches a topic to nanoseconds, or to seconds, would otherwise
// be converted without complaint into a time in 1970 or in the year 56000, and
// every freshness number computed from it would be wrong while looking fine.
func timestampNs(milliseconds int64) (int64, error) {
	if milliseconds < minTimestampMS || milliseconds > maxTimestampMS {
		return 0, errTimestampUnit
	}
	return milliseconds * int64(time.Millisecond), nil
}

// numericError classifies a rejected decimal. A value that does not fit the
// scale is counted apart from a malformed one: it is the single failure that
// would otherwise publish a plausible wrong price, and it must never be clamped
// or wrapped into range.
func numericError(channel manoochv1.Channel, symbol, field, value string, err error) error {
	kind := core.KindField
	if errors.Is(err, price.ErrOutOfRange) || errors.Is(err, price.ErrPrecisionLoss) {
		kind = core.KindRange
	}
	return core.NewParseError(kind, channel, symbol, err, "field %q = %q", field, value)
}
