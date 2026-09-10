package kucoin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/ratelimit"
	"github.com/you/manooch/pkg/price"
	"google.golang.org/protobuf/proto"
)

// The public endpoints this adapter reads. All three are unauthenticated.
//
// Mark and index come from one call and funding from another, which is the same
// split the websocket has: the two are different subjects at different
// cadences, and one endpoint answering both would have been the surprise.
const (
	markPricePath   = "/api/v1/mark-price/%s/current"
	fundingRatePath = "/api/v1/funding-rate/%s/current"
	contractsPath   = "/api/v1/contracts/active"
)

// Caps on a REST response body. A gateway error page is not a price, and
// reading an unbounded body from a host having a bad day is how a fallback
// becomes the outage. The contract list is genuinely large, so it gets its own.
const (
	maxRESTBodyBytes     = 1 << 20
	maxMetadataBodyBytes = 32 << 20
)

// contractPerpetualLinear is KuCoin's type code for a linear perpetual swap.
// The same endpoint lists inverse swaps and dated futures, whose tick sizes and
// multipliers are not a linear perpetual's.
const contractPerpetualLinear = "FFWCSX"

// statusOpen is the only contract status KuCoin considers live.
const statusOpen = "Open"

// envelopeOf is the wrapper every KuCoin REST response carries. A 200 with a
// code other than 200000 is still a failure, and reading only the HTTP status
// would take the error body for data.
type restEnvelope struct {
	Code    string          `json:"code"`
	Message string          `json:"msg"`
	Data    json.RawMessage `json:"data"`
}

// currentValue is /mark-price/{symbol}/current and
// /funding-rate/{symbol}/current. Both answer the same shape; only what
// "value" means differs.
//
// Numbers are json.Number for the same reason they are on the websocket:
// KuCoin sends them unquoted, and float64 would round them before pkg/price
// saw a digit.
type currentValue struct {
	Symbol      string      `json:"symbol"`
	Granularity int64       `json:"granularity"`
	TimePointMS int64       `json:"timePoint"`
	Value       json.Number `json:"value"`
	IndexPrice  json.Number `json:"indexPrice"`
}

// FetchOnce reads one stream's current value over REST.
//
// It returns only the requested channel. The mark-price endpoint answers the
// index price too, but a caller polling because one key expired asked for that
// key, not to have another overwritten from a source it did not choose.
//
// The returned message is SOURCE_REST, so a consumer can tell a polled value
// from a streamed one; nothing else about it differs.
func (adapter *Adapter) FetchOnce(ctx context.Context, specification core.StreamSpec) ([]core.Message, error) {
	if err := adapter.checkSpecification(specification); err != nil {
		return nil, err
	}
	if adapter.options.RESTEndpoint == "" {
		return nil, fmt.Errorf("kucoin: no rest endpoint")
	}
	symbol, err := adapter.VenueSymbol(specification.Instrument)
	if err != nil {
		return nil, err
	}
	// The poll does not happen if there is no budget for it. The caller marks
	// the stream STALE, which is the truth: nothing is refreshing that key.
	if err := adapter.options.Limiter.Allow(ctx, Venue, ratelimit.LimitRESTWeight, adapter.RESTCost(core.OpFetchOnce)); err != nil {
		return nil, fmt.Errorf("kucoin: fetch %s: %w", specification, err)
	}

	path := markPricePath
	if specification.Channel == manoochv1.Channel_CHANNEL_FUNDING {
		path = fundingRatePath
	}
	data, receivedNs, err := adapter.get(ctx, adapter.options.RESTEndpoint+fmt.Sprintf(path, symbol), maxRESTBodyBytes, specification.Channel, symbol)
	if err != nil {
		return nil, fmt.Errorf("kucoin: fetch %s: %w", specification, err)
	}

	var current currentValue
	if err := json.Unmarshal(data, &current); err != nil {
		return nil, core.NewParseError(core.KindJSON, specification.Channel, symbol, err, "response is not json")
	}
	exchangeNs, err := timestampNs(current.TimePointMS)
	if err != nil {
		return nil, core.NewParseError(core.KindField, specification.Channel, symbol, err, "timePoint %d", current.TimePointMS)
	}

	message, err := adapter.restMessage(specification, symbol, current, exchangeNs, receivedNs)
	if err != nil {
		return nil, err
	}
	if message == nil {
		return nil, nil
	}
	return []core.Message{*message}, nil
}

// restMessage builds the one message the caller asked for. A nil message with
// a nil error is a value the venue did not answer with, which is missing data
// rather than a zero.
//
// The symbol comes from the specification, never from the response: the
// funding endpoint answers with the index symbol (".XBTUSDTMFPI8H") rather
// than the contract's, and mapping that back would fail or, worse, succeed
// onto the wrong key.
func (adapter *Adapter) restMessage(specification core.StreamSpec, symbol string, current currentValue, exchangeNs, receivedNs int64) (*core.Message, error) {
	reference := specification.Instrument
	instrument := reference.Proto(symbol)

	build := func(payload func(*manoochv1.Envelope) proto.Message) *core.Message {
		message := adapter.message(reference, instrument, specification.Channel, exchangeNs, receivedNs, payload)
		message.Proto.(interface{ GetEnv() *manoochv1.Envelope }).GetEnv().Source = manoochv1.Source_SOURCE_REST
		return &message
	}

	switch specification.Channel {
	case manoochv1.Channel_CHANNEL_MARK_PRICE:
		v, err := price.ParsePrice(current.Value.String())
		if err != nil {
			return nil, numericError(specification.Channel, symbol, "value", current.Value.String(), err)
		}
		return build(func(envelope *manoochv1.Envelope) proto.Message {
			return &manoochv1.MarkPrice{Env: envelope, MarkPrice: int64(v)}
		}), nil

	case manoochv1.Channel_CHANNEL_INDEX_PRICE:
		if current.IndexPrice.String() == "" {
			return nil, nil
		}
		v, err := price.ParsePrice(current.IndexPrice.String())
		if err != nil {
			return nil, numericError(specification.Channel, symbol, "indexPrice", current.IndexPrice.String(), err)
		}
		return build(func(envelope *manoochv1.Envelope) proto.Message {
			return &manoochv1.IndexPrice{Env: envelope, IndexPrice: int64(v)}
		}), nil

	case manoochv1.Channel_CHANNEL_FUNDING:
		if current.Value.String() == "" {
			return nil, nil
		}
		v, err := price.ParseRate(current.Value.String())
		if err != nil {
			return nil, numericError(specification.Channel, symbol, "value", current.Value.String(), err)
		}
		// next_funding_time_ns stays zero here too. The venue answers with how
		// long is left rather than when, and turning that into an absolute time
		// would publish our clock as though it were the venue's.
		return build(func(envelope *manoochv1.Envelope) proto.Message {
			return &manoochv1.Funding{Env: envelope, FundingRate: int64(v)}
		}), nil
	}
	return nil, fmt.Errorf("kucoin: %s: channel %s is not served", specification, core.ChannelName(specification.Channel))
}

// get performs one public GET, unwraps KuCoin's envelope and returns the data
// with the instant it landed.
//
// receivedNs is stamped the moment the body is read and before anything looks
// at it, for the same reason the websocket read loop stamps it there: measured
// after parsing it would fold our own work into the venue's latency.
func (adapter *Adapter) get(ctx context.Context, url string, limit int64, channel manoochv1.Channel, symbol string) ([]byte, int64, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	response, err := adapter.options.HTTPClient.Do(request)
	if err != nil {
		return nil, 0, err
	}
	defer response.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(response.Body, limit))
	receivedNs := time.Now().UnixNano()

	if readErr != nil {
		return nil, receivedNs, readErr
	}
	if response.StatusCode != http.StatusOK {
		return nil, receivedNs, core.NewParseError(core.KindVenue, channel, symbol, nil,
			"%s: %s", response.Status, truncate(string(body), 200))
	}

	var envelope restEnvelope
	if err := json.Unmarshal(body, &envelope); err != nil {
		return nil, receivedNs, core.NewParseError(core.KindJSON, channel, symbol, err, "response is not json")
	}
	if envelope.Code != codeOK {
		// A 200 with a rejection code inside it. Passing the venue's own code
		// and message through is the difference between a fixable error and
		// "the request worked but there was no data".
		return nil, receivedNs, core.NewParseError(core.KindVenue, channel, symbol, nil,
			"code %s: %s", envelope.Code, truncate(envelope.Message, 200))
	}
	if len(envelope.Data) == 0 {
		return nil, receivedNs, core.NewParseError(core.KindField, channel, symbol, nil, "response carries no data")
	}
	return envelope.Data, receivedNs, nil
}
