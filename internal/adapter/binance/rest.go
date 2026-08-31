package binance

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/pkg/price"
	"google.golang.org/protobuf/proto"
)

// premiumIndexPath carries the same three values as the websocket stream under
// different names, which is what makes it a usable fallback rather than a
// second source that could disagree.
const premiumIndexPath = "/fapi/v1/premiumIndex"

// maxRESTBodyBytes caps a REST response. A gateway error page is not a price
// list, and reading an unbounded body from a host having a bad day is how a
// fallback becomes the outage.
const maxRESTBodyBytes = 1 << 20

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
func (a *Adapter) FetchOnce(ctx context.Context, spec core.StreamSpec) ([]core.Message, error) {
	if err := a.checkSpec(spec); err != nil {
		return nil, err
	}
	if a.opts.RESTEndpoint == "" {
		return nil, fmt.Errorf("binance: no rest endpoint")
	}
	sym, err := a.VenueSymbol(spec.Instrument)
	if err != nil {
		return nil, err
	}

	u := a.opts.RESTEndpoint + premiumIndexPath + "?" + url.Values{"symbol": {sym}}.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, fmt.Errorf("binance: fetch %s: %w", spec, err)
	}

	resp, err := a.opts.HTTPClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("binance: fetch %s: %w", spec, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxRESTBodyBytes))

	// Stamped here for the same reason the read loop stamps it there: this is
	// when the data arrived, and anything measured after parsing would fold
	// our own work into the venue's latency.
	recvNs := time.Now().UnixNano()

	if err != nil {
		return nil, fmt.Errorf("binance: fetch %s: %w", spec, err)
	}
	if resp.StatusCode != http.StatusOK {
		// Binance answers a rejection with a code and a message; passing them
		// through is the difference between a fixable error and "HTTP 400".
		return nil, core.NewParseError(core.KindVenue, spec.Channel, sym, nil,
			"%s: %s", resp.Status, truncate(string(body), 200))
	}

	var pi premiumIndex
	if err := json.Unmarshal(body, &pi); err != nil {
		return nil, core.NewParseError(core.KindJSON, spec.Channel, sym, err, "response is not json")
	}
	if pi.TimeMS <= 0 {
		return nil, core.NewParseError(core.KindField, spec.Channel, sym, nil, "time is %d", pi.TimeMS)
	}

	msg, err := a.restMessage(spec, pi, recvNs)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}
	return []core.Message{*msg}, nil
}

// restMessage builds the one message the caller asked for. A nil message with
// a nil error is a value the venue did not answer with — an empty funding rate
// on a delivery symbol — which is missing data, not a zero.
func (a *Adapter) restMessage(spec core.StreamSpec, pi premiumIndex, recvNs int64) (*core.Message, error) {
	ref := spec.Instrument
	instrument := ref.Proto(pi.Symbol)
	exchangeNs := msToNs(pi.TimeMS)

	build := func(ch pb.Channel, payload func(*pb.Envelope) proto.Message) *core.Message {
		m := a.message(ref, instrument, ch, exchangeNs, recvNs, payload)
		m.Proto.(interface{ GetEnv() *pb.Envelope }).GetEnv().Source = pb.Source_SOURCE_REST
		return &m
	}

	switch spec.Channel {
	case pb.Channel_CHANNEL_MARK_PRICE:
		v, err := price.ParsePrice(pi.MarkPrice)
		if err != nil {
			return nil, numericError(spec.Channel, pi.Symbol, "markPrice", pi.MarkPrice, err)
		}
		return build(spec.Channel, func(env *pb.Envelope) proto.Message {
			return &pb.MarkPrice{Env: env, MarkPrice: int64(v)}
		}), nil

	case pb.Channel_CHANNEL_INDEX_PRICE:
		v, err := price.ParsePrice(pi.IndexPrice)
		if err != nil {
			return nil, numericError(spec.Channel, pi.Symbol, "indexPrice", pi.IndexPrice, err)
		}
		return build(spec.Channel, func(env *pb.Envelope) proto.Message {
			return &pb.IndexPrice{Env: env, IndexPrice: int64(v)}
		}), nil

	case pb.Channel_CHANNEL_FUNDING:
		if pi.LastFundingRate == "" || pi.NextFundingTime <= 0 {
			return nil, nil
		}
		v, err := price.ParseRate(pi.LastFundingRate)
		if err != nil {
			return nil, numericError(spec.Channel, pi.Symbol, "lastFundingRate", pi.LastFundingRate, err)
		}
		next := msToNs(pi.NextFundingTime)
		return build(spec.Channel, func(env *pb.Envelope) proto.Message {
			return &pb.Funding{Env: env, FundingRate: int64(v), NextFundingTimeNs: next}
		}), nil
	}
	return nil, fmt.Errorf("binance: %s: channel %s is not served", spec, core.ChannelName(spec.Channel))
}

// truncate bounds an error body so one bad response cannot fill the log.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
