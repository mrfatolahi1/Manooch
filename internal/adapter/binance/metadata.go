package binance

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/you/manooch/gen/manoochv1"
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
func (adapter *Adapter) FetchMetadata(ctx context.Context, marketType manoochv1.MarketType) ([]*manoochv1.InstrumentMeta, error) {
	if marketType != MarketType {
		return nil, fmt.Errorf("binance: market type %s is not served", core.MarketTypeName(marketType))
	}
	if adapter.options.RESTEndpoint == "" {
		return nil, fmt.Errorf("binance: no rest endpoint")
	}
	if err := adapter.options.Limiter.Allow(ctx, Venue, ratelimit.LimitRESTWeight, adapter.RESTCost(core.OpFetchMetadata)); err != nil {
		return nil, fmt.Errorf("binance: fetch metadata: %w", err)
	}

	body, receivedNs, err := adapter.get(ctx, adapter.options.RESTEndpoint+exchangeInfoPath,
		maxMetadataBodyBytes, manoochv1.Channel_CHANNEL_METADATA, "")
	if err != nil {
		return nil, fmt.Errorf("binance: fetch metadata: %w", err)
	}

	var exchangeInfo exchangeInfo
	if err := json.Unmarshal(body, &exchangeInfo); err != nil {
		return nil, core.NewParseError(core.KindJSON, manoochv1.Channel_CHANNEL_METADATA, "", err, "exchangeInfo is not json")
	}
	if exchangeInfo.ServerTimeMS <= 0 {
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_METADATA, "", nil,
			"serverTime is %d", exchangeInfo.ServerTimeMS)
	}
	exchangeNs := millisecondsToNanoseconds(exchangeInfo.ServerTimeMS)

	out := make([]*manoochv1.InstrumentMeta, 0, len(exchangeInfo.Symbols))
	for _, symbol := range exchangeInfo.Symbols {
		if symbol.ContractType != contractTypePerpetual {
			continue
		}
		reference, err := adapter.ParseVenueSymbol(symbol.Symbol, marketType)
		if err != nil {
			continue // a quote asset we have no mapping for
		}
		metadata, err := adapter.instrumentMetadata(reference, symbol, exchangeNs, receivedNs)
		if err != nil {
			return nil, err
		}
		out = append(out, metadata)
	}
	if len(out) == 0 {
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_METADATA, "", nil,
			"exchangeInfo listed no %s contracts", contractTypePerpetual)
	}
	return out, nil
}

// instrumentMetadata converts one symbol's filters into the normalized message.
func (adapter *Adapter) instrumentMetadata(reference core.InstrumentRef, symbol exchangeSymbol, exchangeNs, receivedNs int64) (*manoochv1.InstrumentMeta, error) {
	metadata := &manoochv1.InstrumentMeta{
		Env: &manoochv1.Envelope{
			Venue:          Venue,
			Instrument:     reference.Proto(symbol.Symbol),
			Channel:        manoochv1.Channel_CHANNEL_METADATA,
			ExchangeTimeNs: exchangeNs,
			RecvTimeNs:     receivedNs,
			// serverTime is the venue's clock at the moment it answered, so it
			// is a send time like every other timestamp Binance gives us.
			// Leaving this false would tell a consumer the opposite and drop
			// the message out of the publish-latency histogram.
			ExchangeTimeIsSendTime: true,
			Source:                 manoochv1.Source_SOURCE_REST,
			Status:                 manoochv1.Status_STATUS_HEALTHY,
		},
		ContractMultiplier: linearContractMultiplier,
		Active:             symbol.Status == statusTrading,
		LastRefreshNs:      receivedNs,
	}

	for _, filter := range symbol.Filters {
		var err error
		switch filter.Type {
		case filterPrice:
			metadata.TickSize, err = parseMetadataPrice(symbol.Symbol, "tickSize", filter.TickSize)
		case filterLotSize:
			err = fillLotSize(metadata, symbol.Symbol, filter)
		case filterMinNotional:
			metadata.MinNotional, err = parseMetadataPrice(symbol.Symbol, "notional", filter.Notional)
		}
		if err != nil {
			return nil, err
		}
	}
	if metadata.TickSize <= 0 || metadata.LotSize <= 0 {
		// Precision a consumer cannot round an order to is not metadata.
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_METADATA, symbol.Symbol, nil,
			"tick_size %d lot_size %d", metadata.TickSize, metadata.LotSize)
	}
	return metadata, nil
}

// fillLotSize fills the three size fields the LOT_SIZE filter carries. Each is
// checked on its own: a filter that half-parses would otherwise leave a
// plausible zero where a minimum order size belongs.
func fillLotSize(metadata *manoochv1.InstrumentMeta, symbol string, filter exchangeFilter) error {
	for _, field := range []struct {
		destination *int64
		name        string
		value       string
	}{
		{&metadata.LotSize, "stepSize", filter.StepSize},
		{&metadata.MinSize, "minQty", filter.MinQty},
		{&metadata.MaxSize, "maxQty", filter.MaxQty},
	} {
		v, err := parseMetadataSize(symbol, field.name, field.value)
		if err != nil {
			return err
		}
		*field.destination = v
	}
	return nil
}

// parseMetadataPrice and parseMetadataSize hand the venue's digit string
// straight to pkg/price. An empty value is zero rather than an error: not every
// symbol carries every filter, and a filter that is absent is missing data, not
// a malformed number.
func parseMetadataPrice(symbol, field, value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	v, err := price.ParsePrice(value)
	if err != nil {
		return 0, numericError(manoochv1.Channel_CHANNEL_METADATA, symbol, field, value, err)
	}
	return int64(v), nil
}

func parseMetadataSize(symbol, field, value string) (int64, error) {
	if value == "" {
		return 0, nil
	}
	v, err := price.ParseSize(value)
	if err != nil {
		return 0, numericError(manoochv1.Channel_CHANNEL_METADATA, symbol, field, value, err)
	}
	return int64(v), nil
}
