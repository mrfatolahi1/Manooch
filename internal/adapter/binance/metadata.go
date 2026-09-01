package binance

import (
	"context"
	"encoding/json"
	"fmt"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/ratelimit"
	"github.com/you/manooch/pkg/price"
)

// exchangeInfoPath is the public instrument list. No credential, no signature:
// it is the same document anyone can fetch with a browser.
const exchangeInfoPath = "/fapi/v1/exchangeInfo"

// contractTypePerpetual is the only contract type this adapter serves. The
// endpoint also lists dated delivery contracts on the same symbols' pairs, and
// their tick sizes are not a perpetual's.
const contractTypePerpetual = "PERPETUAL"

// statusTrading is the only status Binance considers live.
const statusTrading = "TRADING"

// Filter types, which is where Binance keeps everything that would otherwise
// be a field.
const (
	filterPrice       = "PRICE_FILTER"
	filterLotSize     = "LOT_SIZE"
	filterMinNotional = "MIN_NOTIONAL"
)

// linearContractMultiplier is 1, at size scale. USD-M perpetuals are quoted in
// base units rather than contracts, so a size is already an amount of the base
// asset. It is stated rather than left zero: a consumer multiplying by a
// missing multiplier gets an order size of nothing.
const linearContractMultiplier = int64(price.SizeScale)

// exchangeInfo is the part of /fapi/v1/exchangeInfo this adapter reads.
// Everything else — rate limits, assets, order types — is ignored rather than
// rejected: the venue adds fields without warning.
type exchangeInfo struct {
	ServerTimeMS int64            `json:"serverTime"`
	Symbols      []exchangeSymbol `json:"symbols"`
}

type exchangeSymbol struct {
	Symbol       string           `json:"symbol"`
	ContractType string           `json:"contractType"`
	Status       string           `json:"status"`
	Filters      []exchangeFilter `json:"filters"`
}

// exchangeFilter holds every filter's fields together because Binance keys them
// by filterType rather than by shape. Values are strings, as everywhere else on
// this venue, and go to price.Parse* as digit strings.
type exchangeFilter struct {
	Type     string `json:"filterType"`
	TickSize string `json:"tickSize"`
	StepSize string `json:"stepSize"`
	MinQty   string `json:"minQty"`
	MaxQty   string `json:"maxQty"`
	Notional string `json:"notional"`
}

// FetchMetadata reads the venue's public instrument list.
//
// Symbols this adapter does not serve are skipped rather than rejected: the
// endpoint answers with every contract Binance lists, including dated
// deliveries and quote assets we have no mapping for, and erroring on one would
// take the whole refresh down for an instrument nobody asked about.
//
// A symbol that is served but whose filters do not parse is an error. That is
// the difference between "not ours" and "ours, and wrong".
func (a *Adapter) FetchMetadata(ctx context.Context, mt pb.MarketType) ([]*pb.InstrumentMeta, error) {
	if mt != MarketType {
		return nil, fmt.Errorf("binance: market type %s is not served", core.MarketTypeName(mt))
	}
	if a.opts.RESTEndpoint == "" {
		return nil, fmt.Errorf("binance: no rest endpoint")
	}
	if err := a.opts.Limiter.Allow(ctx, Venue, ratelimit.LimitRESTWeight, a.RESTCost(core.OpFetchMetadata)); err != nil {
		return nil, fmt.Errorf("binance: fetch metadata: %w", err)
	}

	body, recvNs, err := a.get(ctx, a.opts.RESTEndpoint+exchangeInfoPath,
		maxMetadataBodyBytes, pb.Channel_CHANNEL_METADATA, "")
	if err != nil {
		return nil, fmt.Errorf("binance: fetch metadata: %w", err)
	}

	var info exchangeInfo
	if err := json.Unmarshal(body, &info); err != nil {
		return nil, core.NewParseError(core.KindJSON, pb.Channel_CHANNEL_METADATA, "", err, "exchangeInfo is not json")
	}
	if info.ServerTimeMS <= 0 {
		return nil, core.NewParseError(core.KindField, pb.Channel_CHANNEL_METADATA, "", nil,
			"serverTime is %d", info.ServerTimeMS)
	}
	exchangeNs := msToNs(info.ServerTimeMS)

	out := make([]*pb.InstrumentMeta, 0, len(info.Symbols))
	for _, sym := range info.Symbols {
		if sym.ContractType != contractTypePerpetual {
			continue
		}
		ref, err := a.ParseVenueSymbol(sym.Symbol, mt)
		if err != nil {
			continue // a quote asset we have no mapping for
		}
		meta, err := a.instrumentMeta(ref, sym, exchangeNs, recvNs)
		if err != nil {
			return nil, err
		}
		out = append(out, meta)
	}
	if len(out) == 0 {
		return nil, core.NewParseError(core.KindField, pb.Channel_CHANNEL_METADATA, "", nil,
			"exchangeInfo listed no %s contracts", contractTypePerpetual)
	}
	return out, nil
}

// instrumentMeta converts one symbol's filters into the normalized message.
func (a *Adapter) instrumentMeta(ref core.InstrumentRef, sym exchangeSymbol, exchangeNs, recvNs int64) (*pb.InstrumentMeta, error) {
	meta := &pb.InstrumentMeta{
		Env: &pb.Envelope{
			Venue:          Venue,
			Instrument:     ref.Proto(sym.Symbol),
			Channel:        pb.Channel_CHANNEL_METADATA,
			ExchangeTimeNs: exchangeNs,
			RecvTimeNs:     recvNs,
			Source:         pb.Source_SOURCE_REST,
			Status:         pb.Status_STATUS_HEALTHY,
		},
		ContractMultiplier: linearContractMultiplier,
		Active:             sym.Status == statusTrading,
		LastRefreshNs:      recvNs,
	}

	for _, f := range sym.Filters {
		var err error
		switch f.Type {
		case filterPrice:
			meta.TickSize, err = parseMetaPrice(sym.Symbol, "tickSize", f.TickSize)
		case filterLotSize:
			err = fillLotSize(meta, sym.Symbol, f)
		case filterMinNotional:
			meta.MinNotional, err = parseMetaPrice(sym.Symbol, "notional", f.Notional)
		}
		if err != nil {
			return nil, err
		}
	}
	if meta.TickSize <= 0 || meta.LotSize <= 0 {
		// Precision a consumer cannot round an order to is not metadata.
		return nil, core.NewParseError(core.KindField, pb.Channel_CHANNEL_METADATA, sym.Symbol, nil,
			"tick_size %d lot_size %d", meta.TickSize, meta.LotSize)
	}
	return meta, nil
}

// fillLotSize fills the three size fields the LOT_SIZE filter carries. Each is
// checked on its own: a filter that half-parses would otherwise leave a
// plausible zero where a minimum order size belongs.
func fillLotSize(meta *pb.InstrumentMeta, symbol string, f exchangeFilter) error {
	for _, fl := range []struct {
		dst   *int64
		name  string
		value string
	}{
		{&meta.LotSize, "stepSize", f.StepSize},
		{&meta.MinSize, "minQty", f.MinQty},
		{&meta.MaxSize, "maxQty", f.MaxQty},
	} {
		v, err := parseMetaSize(symbol, fl.name, fl.value)
		if err != nil {
			return err
		}
		*fl.dst = v
	}
	return nil
}

// parseMetaPrice and parseMetaSize hand the venue's digit string straight to
// pkg/price. An empty value is zero rather than an error: not every symbol
// carries every filter, and a filter that is absent is missing data, not a
// malformed number.
func parseMetaPrice(symbol, field, value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	v, err := price.ParsePrice(value)
	if err != nil {
		return 0, numericError(pb.Channel_CHANNEL_METADATA, symbol, field, value, err)
	}
	return int64(v), nil
}

func parseMetaSize(symbol, field, value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	v, err := price.ParseSize(value)
	if err != nil {
		return 0, numericError(pb.Channel_CHANNEL_METADATA, symbol, field, value, err)
	}
	return int64(v), nil
}
