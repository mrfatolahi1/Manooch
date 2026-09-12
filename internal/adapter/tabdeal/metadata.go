package tabdeal

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
)

type exchangeInfo struct {
	ServerTime int64    `json:"serverTime"`
	Symbols    []market `json:"symbols"`
}
type market struct {
	Symbol            string `json:"symbol"`
	Status            string `json:"status"`
	PricePrecision    int    `json:"pricePrecision"`
	QuantityPrecision int    `json:"quantityPrecision"`
}

func (a *Adapter) FetchMetadata(ctx context.Context, marketType manoochv1.MarketType) ([]*manoochv1.InstrumentMeta, error) {
	if marketType != MarketType {
		return nil, fmt.Errorf("tabdeal: market type %s is not served", core.MarketTypeName(marketType))
	}
	if a.options.RESTEndpoint == "" {
		return nil, fmt.Errorf("tabdeal: no rest endpoint")
	}
	if err := a.options.Limiter.Allow(ctx, Venue, ratelimit.LimitRESTWeight, a.RESTCost(core.OpFetchMetadata)); err != nil {
		return nil, err
	}
	u := a.options.RESTEndpoint + "/fapi/v1/exchangeInfo"
	if _, err := url.Parse(u); err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	resp, err := a.options.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
	received := time.Now().UnixNano()
	if err != nil {
		return nil, err
	}
	if resp.StatusCode != http.StatusOK {
		return nil, core.NewParseError(core.KindVenue, manoochv1.Channel_CHANNEL_METADATA, "", nil, "%s: %s", resp.Status, string(body))
	}
	var info exchangeInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, core.NewParseError(core.KindJSON, manoochv1.Channel_CHANNEL_METADATA, "", err, "exchangeInfo is not json")
	}
	exchangeNs := info.ServerTime * int64(time.Millisecond)
	out := make([]*manoochv1.InstrumentMeta, 0, len(info.Symbols))
	for _, m := range info.Symbols {
		ref, err := a.ParseVenueSymbol(m.Symbol, marketType)
		if err != nil {
			continue
		}
		tick, _ := price.ParsePrice(fmt.Sprintf("1e-%d", m.PricePrecision))
		lot, _ := price.ParseSize(fmt.Sprintf("1e-%d", m.QuantityPrecision))
		out = append(out, &manoochv1.InstrumentMeta{Env: &manoochv1.Envelope{Venue: Venue, Instrument: ref.Proto(m.Symbol), Channel: manoochv1.Channel_CHANNEL_METADATA, ExchangeTimeNs: exchangeNs, RecvTimeNs: received, ExchangeTimeIsSendTime: info.ServerTime > 0, Source: manoochv1.Source_SOURCE_REST, Status: manoochv1.Status_STATUS_HEALTHY}, TickSize: int64(tick), LotSize: int64(lot), MinSize: int64(lot), ContractMultiplier: 1, Active: m.Status == "TRADING", LastRefreshNs: received})
	}
	if len(out) == 0 {
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_METADATA, "", nil, "exchangeInfo listed no linear perpetuals")
	}
	return out, nil
}
