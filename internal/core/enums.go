package core

import (
	"fmt"
	"strings"

	pb "github.com/you/manooch/gen/manoochv1"
)

// Names are derived from the protobuf enum names rather than kept in a
// parallel table, so they cannot drift from schema/manooch.proto.
const (
	marketTypePrefix = "MARKET_TYPE_"
	channelPrefix    = "CHANNEL_"
	statusPrefix     = "STATUS_"
	sourcePrefix     = "SOURCE_"
)

// MarketTypeName renders a MarketType the way it appears in config files and
// Redis keys: "SPOT", "PERP_LINEAR".
func MarketTypeName(mt pb.MarketType) string {
	name, ok := pb.MarketType_name[int32(mt)]
	if !ok {
		return "UNKNOWN"
	}
	return strings.TrimPrefix(name, marketTypePrefix)
}

// ParseMarketType is the inverse of MarketTypeName. The unspecified value is
// rejected: an unidentified market type must never reach a key.
func ParseMarketType(s string) (pb.MarketType, error) {
	v, ok := pb.MarketType_value[marketTypePrefix+strings.ToUpper(s)]
	if !ok || v == int32(pb.MarketType_MARKET_TYPE_UNSPECIFIED) {
		return pb.MarketType_MARKET_TYPE_UNSPECIFIED, fmt.Errorf("unknown market_type %q", s)
	}
	return pb.MarketType(v), nil
}

// ChannelName renders a Channel the way it appears in config files and Redis
// keys: "orderbook", "mark_price".
func ChannelName(ch pb.Channel) string {
	name, ok := pb.Channel_name[int32(ch)]
	if !ok {
		return "unknown"
	}
	return strings.ToLower(strings.TrimPrefix(name, channelPrefix))
}

// ParseChannel is the inverse of ChannelName. The unspecified value is rejected.
func ParseChannel(s string) (pb.Channel, error) {
	v, ok := pb.Channel_value[channelPrefix+strings.ToUpper(s)]
	if !ok || v == int32(pb.Channel_CHANNEL_UNSPECIFIED) {
		return pb.Channel_CHANNEL_UNSPECIFIED, fmt.Errorf("unknown channel %q", s)
	}
	return pb.Channel(v), nil
}

// StatusName renders a Status for humans: "HEALTHY", "STALE".
func StatusName(st pb.Status) string {
	name, ok := pb.Status_name[int32(st)]
	if !ok {
		return "UNKNOWN"
	}
	return strings.TrimPrefix(name, statusPrefix)
}

// SourceName renders a Source for humans: "WEBSOCKET", "REST".
func SourceName(src pb.Source) string {
	name, ok := pb.Source_name[int32(src)]
	if !ok {
		return "UNKNOWN"
	}
	return strings.TrimPrefix(name, sourcePrefix)
}

// IsDerivative reports whether the market type is a perpetual or dated future.
// Only derivatives have a mark price, an index price or funding.
func IsDerivative(mt pb.MarketType) bool {
	switch mt {
	case pb.MarketType_MARKET_TYPE_PERP_LINEAR,
		pb.MarketType_MARKET_TYPE_PERP_INVERSE,
		pb.MarketType_MARKET_TYPE_FUTURE_LINEAR,
		pb.MarketType_MARKET_TYPE_FUTURE_INVERSE:
		return true
	}
	return false
}

// IsInverse reports whether the market type is inverse-settled. On an inverse
// instrument a size is a count of contracts, not an amount of base asset;
// collapsing the two makes every position wrong by roughly the price.
func IsInverse(mt pb.MarketType) bool {
	switch mt {
	case pb.MarketType_MARKET_TYPE_PERP_INVERSE,
		pb.MarketType_MARKET_TYPE_FUTURE_INVERSE:
		return true
	}
	return false
}

// ChannelValidFor reports whether a channel can exist on a market type. Funding
// on a spot market would otherwise become a stream that is never populated.
func ChannelValidFor(ch pb.Channel, mt pb.MarketType) bool {
	switch ch {
	case pb.Channel_CHANNEL_METADATA, pb.Channel_CHANNEL_HEALTH:
		return true
	case pb.Channel_CHANNEL_MARK_PRICE, pb.Channel_CHANNEL_INDEX_PRICE,
		pb.Channel_CHANNEL_FUNDING:
		return IsDerivative(mt)
	}
	return false
}
