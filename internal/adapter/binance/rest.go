package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/ratelimit"
	"github.com/you/manooch/pkg/price"
	"google.golang.org/protobuf/proto"
)

// premiumIndexPath carries the same three values as the websocket stream under
// different names, which is what makes it a usable fallback rather than a
// second source that could disagree.
const premiumIndexPath = "/fapi/v1/premiumIndex"

// Caps on a REST response body. A gateway error page is not a price list, and
// reading an unbounded body from a host having a bad day is how a fallback
// becomes the outage.
//
// The metadata cap is far larger because the response genuinely is: the whole
// USD-M contract list runs to a couple of megabytes, and a cap that truncated
// it would look exactly like a venue that had delisted everything.
const (
	maxRESTBodyBytes     = 1 << 20
	maxMetadataBodyBytes = 32 << 20
)

// premiumIndex is the /fapi/v1/premiumIndex response for one symbol.
type premiumIndex struct {
	Symbol          string `json:"symbol"`
	MarkPrice       string `json:"markPrice"`
	IndexPrice      string `json:"indexPrice"`
	LastFundingRate string `json:"lastFundingRate"`
	NextFundingTime int64  `json:"nextFundingTime"`
	TimeMS          int64  `json:"time"`
}

// FetchOnce reads one stream's current value over REST.
//
// It returns only the requested channel. The endpoint answers all three, but a
// caller polling because one key expired wants that key refreshed, not two
// others silently overwritten from a source it did not ask for.
//
// The returned message is SOURCE_REST, so a consumer can tell a polled value
// from a streamed one; nothing else about it differs.
func (adapter *Adapter) FetchOnce(ctx context.Context, specification core.StreamSpec) ([]core.Message, error) {
	if err := adapter.checkSpecification(specification); err != nil {
		return nil, err
	}
	if adapter.options.RESTEndpoint == "" {
		return nil, fmt.Errorf("binance: no rest endpoint")
	}
	symbol, err := adapter.VenueSymbol(specification.Instrument)
	if err != nil {
		return nil, err
	}
	// The poll does not happen if there is no budget for it. The caller marks
	// the stream STALE, which is the truth: nothing is refreshing that key.
	if err := adapter.options.Limiter.Allow(ctx, Venue, ratelimit.LimitRESTWeight, adapter.RESTCost(core.OpFetchOnce)); err != nil {
		return nil, fmt.Errorf("binance: fetch %s: %w", specification, err)
	}

	u := adapter.options.RESTEndpoint + premiumIndexPath + "?" + url.Values{"symbol": {symbol}}.Encode()
	body, receivedNs, err := adapter.get(ctx, u, maxRESTBodyBytes, specification.Channel, symbol)
	if err != nil {
		return nil, fmt.Errorf("binance: fetch %s: %w", specification, err)
	}

	var premiumIndex premiumIndex
	if err := json.Unmarshal(body, &premiumIndex); err != nil {
		return nil, core.NewParseError(core.KindJSON, specification.Channel, symbol, err, "response is not json")
	}
	if premiumIndex.TimeMS <= 0 {
		return nil, core.NewParseError(core.KindField, specification.Channel, symbol, nil, "time is %d", premiumIndex.TimeMS)
	}

	message, err := adapter.restMessage(specification, premiumIndex, receivedNs)
	if err != nil {
		return nil, err
	}
	if message == nil {
		return nil, nil
	}
	return []core.Message{*message}, nil
}

// restMessage builds the one message the caller asked for. A nil message with
// a nil error is a value the venue did not answer with — an empty funding rate
// on a delivery symbol — which is missing data, not a zero.
func (adapter *Adapter) restMessage(specification core.StreamSpec, premiumIndex premiumIndex, receivedNs int64) (*core.Message, error) {
	reference := specification.Instrument
	instrument := reference.Proto(premiumIndex.Symbol)
	exchangeNs := millisecondsToNanoseconds(premiumIndex.TimeMS)

	build := func(channel manoochv1.Channel, payload func(*manoochv1.Envelope) proto.Message) *core.Message {
		message := adapter.message(reference, instrument, channel, exchangeNs, receivedNs, payload)
		message.Proto.(interface{ GetEnv() *manoochv1.Envelope }).GetEnv().Source = manoochv1.Source_SOURCE_REST
		return &message
	}

	switch specification.Channel {
	case manoochv1.Channel_CHANNEL_MARK_PRICE:
		v, err := price.ParsePrice(premiumIndex.MarkPrice)
		if err != nil {
			return nil, numericError(specification.Channel, premiumIndex.Symbol, "markPrice", premiumIndex.MarkPrice, err)
		}
		return build(specification.Channel, func(envelope *manoochv1.Envelope) proto.Message {
			return &manoochv1.MarkPrice{Env: envelope, MarkPrice: int64(v)}
		}), nil

	case manoochv1.Channel_CHANNEL_INDEX_PRICE:
		v, err := price.ParsePrice(premiumIndex.IndexPrice)
		if err != nil {
			return nil, numericError(specification.Channel, premiumIndex.Symbol, "indexPrice", premiumIndex.IndexPrice, err)
		}
		return build(specification.Channel, func(envelope *manoochv1.Envelope) proto.Message {
			return &manoochv1.IndexPrice{Env: envelope, IndexPrice: int64(v)}
		}), nil

	case manoochv1.Channel_CHANNEL_FUNDING:
		if premiumIndex.LastFundingRate == "" || premiumIndex.NextFundingTime <= 0 {
			return nil, nil
		}
		v, err := price.ParseRate(premiumIndex.LastFundingRate)
		if err != nil {
			return nil, numericError(specification.Channel, premiumIndex.Symbol, "lastFundingRate", premiumIndex.LastFundingRate, err)
		}
		next := millisecondsToNanoseconds(premiumIndex.NextFundingTime)
		return build(specification.Channel, func(envelope *manoochv1.Envelope) proto.Message {
			return &manoochv1.Funding{Env: envelope, FundingRate: int64(v), NextFundingTimeNs: next}
		}), nil
	}
	return nil, fmt.Errorf("binance: %s: channel %s is not served", specification, core.ChannelName(specification.Channel))
}

// get performs one public GET and returns the body with the instant it landed.
//
// receivedNs is stamped the moment the body is read and before anything looks
// at it, for the same reason the websocket read loop stamps it there: measured
// after parsing it would fold our own work into the venue's latency.
//
// A non-200 comes back as a ParseError of kind venue carrying Binance's own
// code and message, which is the difference between a fixable error and
// "HTTP 400".
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
	return body, receivedNs, nil
}

// truncate bounds an error body so one bad response cannot fill the log.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
