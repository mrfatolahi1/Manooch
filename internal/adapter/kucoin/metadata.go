package kucoin

import (
	"context"
	"encoding/json"
	"fmt"

	pb "github.com/you/manooch/gen/manoochv1"
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
func (a *Adapter) FetchMetadata(ctx context.Context, mt pb.MarketType) ([]*pb.InstrumentMeta, error) {
	if mt != MarketType {
		return nil, fmt.Errorf("kucoin: market type %s is not served", core.MarketTypeName(mt))
	}
	if a.opts.RESTEndpoint == "" {
		return nil, fmt.Errorf("kucoin: no rest endpoint")
	}
	if err := a.opts.Limiter.Allow(ctx, Venue, ratelimit.LimitRESTWeight, a.RESTCost(core.OpFetchMetadata)); err != nil {
		return nil, fmt.Errorf("kucoin: fetch metadata: %w", err)
	}

	data, recvNs, err := a.get(ctx, a.opts.RESTEndpoint+contractsPath,
		maxMetadataBodyBytes, pb.Channel_CHANNEL_METADATA, "")
	if err != nil {
		return nil, fmt.Errorf("kucoin: fetch metadata: %w", err)
	}

	var contracts []contract
	if err := json.Unmarshal(data, &contracts); err != nil {
		return nil, core.NewParseError(core.KindJSON, pb.Channel_CHANNEL_METADATA, "", err, "contracts are not json")
	}

	out := make([]*pb.InstrumentMeta, 0, len(contracts))
	for _, c := range contracts {
		if c.IsInverse || c.Type != contractPerpetualLinear {
			continue
		}
		ref, err := a.ParseVenueSymbol(c.Symbol, mt)
		if err != nil {
			continue // a quote asset we have no mapping for
		}
		meta, err := a.instrumentMeta(ref, c, recvNs)
		if err != nil {
			return nil, err
		}
		out = append(out, meta)
	}
	if len(out) == 0 {
		return nil, core.NewParseError(core.KindField, pb.Channel_CHANNEL_METADATA, "", nil,
			"contracts listed no linear perpetuals")
	}
	return out, nil
}

// instrumentMeta converts one contract into the normalized message.
//
// exchange_time_ns is left at zero: this endpoint carries no server time, and
// stamping our own clock into a field named for the venue's would be a value we
// derived presented as one the venue supplied.
//
// min_notional is left at zero for the same reason: KuCoin publishes no minimum
// notional for futures, and a number computed from tick size and lot size would
// look exactly like one the venue had given us.
func (a *Adapter) instrumentMeta(ref core.InstrumentRef, c contract, recvNs int64) (*pb.InstrumentMeta, error) {
	meta := &pb.InstrumentMeta{
		Env: &pb.Envelope{
			Venue:      Venue,
			Instrument: ref.Proto(c.Symbol),
			Channel:    pb.Channel_CHANNEL_METADATA,
			RecvTimeNs: recvNs,
			Source:     pb.Source_SOURCE_REST,
			Status:     pb.Status_STATUS_HEALTHY,
		},
		Active:        c.Status == statusOpen,
		LastRefreshNs: recvNs,
	}

	tick, err := parseMetaPrice(c.Symbol, "tickSize", c.TickSize)
	if err != nil {
		return nil, err
	}
	meta.TickSize = tick

	for _, f := range []struct {
		dst   *int64
		name  string
		value json.Number
	}{
		// lotSize is the minimum order increment, in contracts. It is both the
		// step and the minimum here: KuCoin publishes no separate minimum.
		{&meta.LotSize, "lotSize", c.LotSize},
		{&meta.MinSize, "lotSize", c.LotSize},
		{&meta.MaxSize, "maxOrderQty", c.MaxOrderQty},
		// A KuCoin futures contract is a fixed amount of the base asset, not
		// one unit of it. Without this every order size downstream is wrong by
		// the multiplier, silently.
		{&meta.ContractMultiplier, "multiplier", c.Multiplier},
	} {
		v, err := parseMetaSize(c.Symbol, f.name, f.value)
		if err != nil {
			return nil, err
		}
		*f.dst = v
	}

	if meta.TickSize <= 0 || meta.LotSize <= 0 || meta.ContractMultiplier <= 0 {
		// Precision a consumer cannot round an order to, or a multiplier it
		// cannot size one with, is not metadata.
		return nil, core.NewParseError(core.KindField, pb.Channel_CHANNEL_METADATA, c.Symbol, nil,
			"tick_size %d lot_size %d contract_multiplier %d",
			meta.TickSize, meta.LotSize, meta.ContractMultiplier)
	}
	return meta, nil
}

// parseMetaPrice and parseMetaSize hand the venue's digit string straight to
// pkg/price. An absent number is zero rather than an error: a field the venue
// did not send is missing data, not a malformed value.
func parseMetaPrice(symbol, field string, value json.Number) (int64, error) {
	if value.String() == "" {
		return 0, nil
	}
	v, err := price.ParsePrice(value.String())
	if err != nil {
		return 0, numericError(pb.Channel_CHANNEL_METADATA, symbol, field, value.String(), err)
	}
	return int64(v), nil
}

func parseMetaSize(symbol, field string, value json.Number) (int64, error) {
	if value.String() == "" {
		return 0, nil
	}
	v, err := price.ParseSize(value.String())
	if err != nil {
		return 0, numericError(pb.Channel_CHANNEL_METADATA, symbol, field, value.String(), err)
	}
	return int64(v), nil
}
