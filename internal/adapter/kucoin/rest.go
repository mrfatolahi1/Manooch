package kucoin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
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
	Code string          `json:"code"`
	Msg  string          `json:"msg"`
	Data json.RawMessage `json:"data"`
}

// currentValue is /mark-price/{symbol}/current and /funding-rate/{symbol}/current.
// Both answer the same shape; only what "value" means differs.
//
// Numbers are json.Number for the same reason they are on the websocket: KuCoin
// sends them unquoted, and float64 would round them before pkg/price saw a digit.
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
func (a *Adapter) FetchOnce(ctx context.Context, spec core.StreamSpec) ([]core.Message, error) {
	if err := a.checkSpec(spec); err != nil {
		return nil, err
	}
	if a.opts.RESTEndpoint == "" {
		return nil, fmt.Errorf("kucoin: no rest endpoint")
	}
	sym, err := a.VenueSymbol(spec.Instrument)
	if err != nil {
		return nil, err
	}
	// The poll does not happen if there is no budget for it. The caller marks
	// the stream STALE, which is the truth: nothing is refreshing that key.
	if err := a.opts.Limiter.Allow(ctx, Venue, ratelimit.LimitRESTWeight, a.RESTCost(core.OpFetchOnce)); err != nil {
		return nil, fmt.Errorf("kucoin: fetch %s: %w", spec, err)
	}

	path := markPricePath
	if spec.Channel == pb.Channel_CHANNEL_FUNDING {
		path = fundingRatePath
	}
	data, recvNs, err := a.get(ctx, a.opts.RESTEndpoint+fmt.Sprintf(path, sym), maxRESTBodyBytes, spec.Channel, sym)
	if err != nil {
		return nil, fmt.Errorf("kucoin: fetch %s: %w", spec, err)
	}

	var cur currentValue
	if err := json.Unmarshal(data, &cur); err != nil {
		return nil, core.NewParseError(core.KindJSON, spec.Channel, sym, err, "response is not json")
	}
	exchangeNs, err := timestampNs(cur.TimePointMS)
	if err != nil {
		return nil, core.NewParseError(core.KindField, spec.Channel, sym, err, "timePoint %d", cur.TimePointMS)
	}

	msg, err := a.restMessage(spec, sym, cur, exchangeNs, recvNs)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	return []core.Message{*msg}, nil
}

// restMessage builds the one message the caller asked for. A nil message with a
// nil error is a value the venue did not answer with, which is missing data
// rather than a zero.
//
// The symbol comes from the spec, never from the response: the funding endpoint
// answers with the index symbol (".XBTUSDTMFPI8H") rather than the contract's,
// and mapping that back would fail or, worse, succeed onto the wrong key.
func (a *Adapter) restMessage(spec core.StreamSpec, sym string, cur currentValue, exchangeNs, recvNs int64) (*core.Message, error) {
	ref := spec.Instrument
	instrument := ref.Proto(sym)

	build := func(payload func(*pb.Envelope) proto.Message) *core.Message {
		m := a.message(ref, instrument, spec.Channel, exchangeNs, recvNs, payload)
		m.Proto.(interface{ GetEnv() *pb.Envelope }).GetEnv().Source = pb.Source_SOURCE_REST
		return &m
	}

	switch spec.Channel {
	case pb.Channel_CHANNEL_MARK_PRICE:
		v, err := price.ParsePrice(cur.Value.String())
		if err != nil {
			return nil, numericError(spec.Channel, sym, "value", cur.Value.String(), err)
		}
		return build(func(env *pb.Envelope) proto.Message {
			return &pb.MarkPrice{Env: env, MarkPrice: int64(v)}
		}), nil

	case pb.Channel_CHANNEL_INDEX_PRICE:
		if cur.IndexPrice.String() == "" {
			return nil, nil
		}
		v, err := price.ParsePrice(cur.IndexPrice.String())
		if err != nil {
			return nil, numericError(spec.Channel, sym, "indexPrice", cur.IndexPrice.String(), err)
		}
		return build(func(env *pb.Envelope) proto.Message {
			return &pb.IndexPrice{Env: env, IndexPrice: int64(v)}
		}), nil

	case pb.Channel_CHANNEL_FUNDING:
		if cur.Value.String() == "" {
			return nil, nil
		}
		v, err := price.ParseRate(cur.Value.String())
		if err != nil {
			return nil, numericError(spec.Channel, sym, "value", cur.Value.String(), err)
		}
		// next_funding_time_ns stays zero here too. The venue answers with how
		// long is left rather than when, and turning that into an absolute time
		// would publish our clock as though it were the venue's.
		return build(func(env *pb.Envelope) proto.Message {
			return &pb.Funding{Env: env, FundingRate: int64(v)}
		}), nil
	}
	return nil, fmt.Errorf("kucoin: %s: channel %s is not served", spec, core.ChannelName(spec.Channel))
}

// get performs one public GET, unwraps KuCoin's envelope and returns the data
// with the instant it landed.
//
// recvNs is stamped the moment the body is read and before anything looks at
// it, for the same reason the websocket read loop stamps it there: measured
// after parsing it would fold our own work into the venue's latency.
func (a *Adapter) get(ctx context.Context, url string, limit int64, ch pb.Channel, symbol string) ([]byte, int64, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, 0, err
	}
	resp, err := a.opts.HTTPClient.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	body, readErr := io.ReadAll(io.LimitReader(resp.Body, limit))
	recvNs := time.Now().UnixNano()

	if readErr != nil {
		return nil, recvNs, readErr
	}
	if resp.StatusCode != http.StatusOK {
		return nil, recvNs, core.NewParseError(core.KindVenue, ch, symbol, nil,
			"%s: %s", resp.Status, truncate(string(body), 200))
	}

	var env restEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return nil, recvNs, core.NewParseError(core.KindJSON, ch, symbol, err, "response is not json")
	}
	if env.Code != codeOK {
		// A 200 with a rejection code inside it. Passing the venue's own code
		// and message through is the difference between a fixable error and
		// "the request worked but there was no data".
		return nil, recvNs, core.NewParseError(core.KindVenue, ch, symbol, nil,
			"code %s: %s", env.Code, truncate(env.Msg, 200))
	}
	if len(env.Data) == 0 {
		return nil, recvNs, core.NewParseError(core.KindField, ch, symbol, nil, "response carries no data")
	}
	return env.Data, recvNs, nil
}
