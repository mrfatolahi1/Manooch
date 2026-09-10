package kucoin

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/ratelimit"
	"github.com/you/manooch/pkg/price"
)

// contract is one entry of /api/v1/contracts/active.
//
// Every number is json.Number: KuCoin sends tickSize as 0.1 and multiplier as
// 0.001, unquoted, and a float64 here would round the multiplier that every
// downstream order size is computed from.
type contract struct {
	Symbol      string      `json:"symbol"`
	Type        string      `json:"type"`
	Status      string      `json:"status"`
	IsInverse   bool        `json:"isInverse"`
	TickSize    json.Number `json:"tickSize"`
	LotSize     json.Number `json:"lotSize"`
	MaxOrderQty json.Number `json:"maxOrderQty"`
	Multiplier  json.Number `json:"multiplier"`
}

// FetchMetadata reads the venue's public contract list.
//
// Contracts this adapter does not serve are skipped rather than rejected: the
// endpoint answers with every contract KuCoin lists, inverse swaps and dated
// futures included, and erroring on one would take the whole refresh down for
// an instrument nobody asked about.
//
// A contract that is served but whose numbers do not parse is an error. That is
// the difference between "not ours" and "ours, and wrong".
func (adapter *Adapter) FetchMetadata(ctx context.Context, marketType manoochv1.MarketType) ([]*manoochv1.InstrumentMeta, error) {
	if marketType != MarketType {
		return nil, fmt.Errorf("kucoin: market type %s is not served", core.MarketTypeName(marketType))
	}
	if adapter.options.RESTEndpoint == "" {
		return nil, fmt.Errorf("kucoin: no rest endpoint")
	}
	if err := adapter.options.Limiter.Allow(ctx, Venue, ratelimit.LimitRESTWeight, adapter.RESTCost(core.OpFetchMetadata)); err != nil {
		return nil, fmt.Errorf("kucoin: fetch metadata: %w", err)
	}

	data, receivedNs, err := adapter.get(ctx, adapter.options.RESTEndpoint+contractsPath,
		maxMetadataBodyBytes, manoochv1.Channel_CHANNEL_METADATA, "")
	if err != nil {
		return nil, fmt.Errorf("kucoin: fetch metadata: %w", err)
	}

	var contracts []contract
	if err := json.Unmarshal(data, &contracts); err != nil {
		return nil, core.NewParseError(core.KindJSON, manoochv1.Channel_CHANNEL_METADATA, "", err, "contracts are not json")
	}

	out := make([]*manoochv1.InstrumentMeta, 0, len(contracts))
	for _, contract := range contracts {
		if contract.IsInverse || contract.Type != contractPerpetualLinear {
			continue
		}
		reference, err := adapter.ParseVenueSymbol(contract.Symbol, marketType)
		if err != nil {
			continue // a quote asset we have no mapping for
		}
		metadata, err := adapter.instrumentMetadata(reference, contract, receivedNs)
		if err != nil {
			return nil, err
		}
		out = append(out, metadata)
	}
	if len(out) == 0 {
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_METADATA, "", nil,
			"contracts listed no linear perpetuals")
	}
	return out, nil
}

// instrumentMetadata converts one contract into the normalized message.
//
// exchange_time_ns is left at zero: this endpoint carries no server time, and
// stamping our own clock into a field named for the venue's would be a value we
// derived presented as one the venue supplied.
//
// min_notional is left at zero for the same reason: KuCoin publishes no minimum
// notional for futures, and a number computed from tick size and lot size would
// look exactly like one the venue had given us.
func (adapter *Adapter) instrumentMetadata(reference core.InstrumentRef, contract contract, receivedNs int64) (*manoochv1.InstrumentMeta, error) {
	metadata := &manoochv1.InstrumentMeta{
		Env: &manoochv1.Envelope{
			Venue:      Venue,
			Instrument: reference.Proto(contract.Symbol),
			Channel:    manoochv1.Channel_CHANNEL_METADATA,
			RecvTimeNs: receivedNs,
			Source:     manoochv1.Source_SOURCE_REST,
			Status:     manoochv1.Status_STATUS_HEALTHY,
		},
		Active:        contract.Status == statusOpen,
		LastRefreshNs: receivedNs,
	}

	tick, err := parseMetadataPrice(contract.Symbol, "tickSize", contract.TickSize)
	if err != nil {
		return nil, err
	}
	metadata.TickSize = tick

	for _, f := range []struct {
		destination *int64
		name        string
		value       json.Number
	}{
		// lotSize is the minimum order increment, in contracts. It is both the
		// step and the minimum here: KuCoin publishes no separate minimum.
		{&metadata.LotSize, "lotSize", contract.LotSize},
		{&metadata.MinSize, "lotSize", contract.LotSize},
		{&metadata.MaxSize, "maxOrderQty", contract.MaxOrderQty},
		// A KuCoin futures contract is a fixed amount of the base asset, not
		// one unit of it. Without this every order size downstream is wrong by
		// the multiplier, silently.
		{&metadata.ContractMultiplier, "multiplier", contract.Multiplier},
	} {
		v, err := parseMetadataSize(contract.Symbol, f.name, f.value)
		if err != nil {
			return nil, err
		}
		*f.destination = v
	}

	if metadata.TickSize <= 0 || metadata.LotSize <= 0 || metadata.ContractMultiplier <= 0 {
		// Precision a consumer cannot round an order to, or a multiplier it
		// cannot size one with, is not metadata.
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_METADATA, contract.Symbol, nil,
			"tick_size %d lot_size %d contract_multiplier %d",
			metadata.TickSize, metadata.LotSize, metadata.ContractMultiplier)
	}
	return metadata, nil
}

// parseMetadataPrice and parseMetadataSize hand the venue's digit string
// straight to pkg/price. An absent number is zero rather than an error: a field
// the venue did not send is missing data, not a malformed value.
func parseMetadataPrice(symbol, field string, value json.Number) (int64, error) {
	if value.String() == "" {
		return 0, nil
	}
	v, err := price.ParsePrice(value.String())
	if err != nil {
		return 0, numericError(manoochv1.Channel_CHANNEL_METADATA, symbol, field, value.String(), err)
	}
	return int64(v), nil
}

func parseMetadataSize(symbol, field string, value json.Number) (int64, error) {
	if value.String() == "" {
		return 0, nil
	}
	v, err := price.ParseSize(value.String())
	if err != nil {
		return 0, numericError(manoochv1.Channel_CHANNEL_METADATA, symbol, field, value.String(), err)
	}
	return int64(v), nil
}
