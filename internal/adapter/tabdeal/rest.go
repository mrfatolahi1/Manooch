package tabdeal

import (
	"bytes"
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
)

const depthPath = "/fapi/v1/depth"

type depthResponse struct {
	LastUpdateID uint64              `json:"lastUpdateId"`
	EventTime    int64               `json:"E"`
	Symbol       string              `json:"symbol"`
	Bids         [][]json.RawMessage `json:"bids"`
	Asks         [][]json.RawMessage `json:"asks"`
}

func (a *Adapter) FetchOnce(ctx context.Context, spec core.StreamSpec) ([]core.Message, error) {
	if err := a.check(spec); err != nil {
		return nil, err
	}
	if a.options.RESTEndpoint == "" {
		return nil, fmt.Errorf("tabdeal: no rest endpoint")
	}
	symbol, err := a.VenueSymbol(spec.Instrument)
	if err != nil {
		return nil, err
	}
	if err := a.options.Limiter.Allow(ctx, Venue, ratelimit.LimitRESTWeight, a.RESTCost(core.OpFetchOnce)); err != nil {
		return nil, fmt.Errorf("tabdeal: fetch %s: %w", spec, err)
	}
	u := a.options.RESTEndpoint + depthPath + "?symbol=" + url.QueryEscape(symbol) + "&limit=50"
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.options.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, readErr := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	receivedNs := time.Now().UnixNano()
	if readErr != nil {
		return nil, readErr
	}
	if resp.StatusCode != http.StatusOK {
		return nil, core.NewParseError(core.KindVenue, spec.Channel, symbol, nil, "%s: %s", resp.Status, string(body))
	}
	var d depthResponse
	dec := json.NewDecoder(bytes.NewReader(body))
	dec.UseNumber()
	if err := dec.Decode(&d); err != nil {
		return nil, core.NewParseError(core.KindJSON, spec.Channel, symbol, err, "response is not json")
	}
	if d.Symbol == "" {
		d.Symbol = symbol
	}
	bids, err := rawLevels(d.Bids)
	if err != nil {
		return nil, core.NewParseError(core.KindJSON, spec.Channel, symbol, err, "invalid bids")
	}
	asks, err := rawLevels(d.Asks)
	if err != nil {
		return nil, core.NewParseError(core.KindJSON, spec.Channel, symbol, err, "invalid asks")
	}
	msg, err := a.book(spec.Instrument, d.Symbol, bids, asks, d.EventTime, receivedNs, manoochv1.Source_SOURCE_REST)
	if err != nil {
		return nil, err
	}
	if d.LastUpdateID != 0 {
		msg.Proto.(*manoochv1.OrderBook).Env.VenueSeq = d.LastUpdateID
		msg.Proto.(*manoochv1.OrderBook).Env.VenueSeqPresent = true
	}
	return []core.Message{msg}, nil
}

func rawLevels(raw [][]json.RawMessage) ([][]string, error) {
	out := make([][]string, 0, len(raw))
	for _, pair := range raw {
		if len(pair) != 2 {
			return nil, fmt.Errorf("level must contain two values")
		}
		row := make([]string, 2)
		for i, value := range pair {
			var s string
			if err := json.Unmarshal(value, &s); err == nil {
				row[i] = s
			} else {
				row[i] = string(value)
			}
		}
		out = append(out, row)
	}
	return out, nil
}
