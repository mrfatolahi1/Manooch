package core

import (
	"fmt"
	"strings"

	"github.com/you/manooch/gen/manoochv1"
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
func MarketTypeName(marketType manoochv1.MarketType) string {
	name, ok := manoochv1.MarketType_name[int32(marketType)]
	if !ok {
		return "UNKNOWN"
	}
	return strings.TrimPrefix(name, marketTypePrefix)
}

// ParseMarketType is the inverse of MarketTypeName. The unspecified value is
// rejected: an unidentified market type must never reach a key.
func ParseMarketType(s string) (manoochv1.MarketType, error) {
	v, ok := manoochv1.MarketType_value[marketTypePrefix+strings.ToUpper(s)]
	if !ok || v == int32(manoochv1.MarketType_MARKET_TYPE_UNSPECIFIED) {
		return manoochv1.MarketType_MARKET_TYPE_UNSPECIFIED, fmt.Errorf("unknown market_type %q", s)
	}
	return manoochv1.MarketType(v), nil
}

// ChannelName renders a Channel the way it appears in config files and Redis
// keys: "mark_price", "index_price".
func ChannelName(channel manoochv1.Channel) string {
	name, ok := manoochv1.Channel_name[int32(channel)]
	if !ok {
		return "unknown"
	}
	return strings.ToLower(strings.TrimPrefix(name, channelPrefix))
}

// ParseChannel is the inverse of ChannelName. The unspecified value is
// rejected.
func ParseChannel(s string) (manoochv1.Channel, error) {
	v, ok := manoochv1.Channel_value[channelPrefix+strings.ToUpper(s)]
	if !ok || v == int32(manoochv1.Channel_CHANNEL_UNSPECIFIED) {
		return manoochv1.Channel_CHANNEL_UNSPECIFIED, fmt.Errorf("unknown channel %q", s)
	}
	return manoochv1.Channel(v), nil
}

// StatusName renders a Status for humans: "HEALTHY", "STALE".
func StatusName(status manoochv1.Status) string {
	name, ok := manoochv1.Status_name[int32(status)]
	if !ok {
		return "UNKNOWN"
	}
	return strings.TrimPrefix(name, statusPrefix)
}

// SourceName renders a Source for humans: "WEBSOCKET", "REST".
func SourceName(source manoochv1.Source) string {
	name, ok := manoochv1.Source_name[int32(source)]
	if !ok {
		return "UNKNOWN"
	}
	return strings.TrimPrefix(name, sourcePrefix)
}

// IsDerivative reports whether the market type is a perpetual or dated future.
// Only derivatives have a mark price, an index price or funding.
func IsDerivative(marketType manoochv1.MarketType) bool {
	switch marketType {
	case manoochv1.MarketType_MARKET_TYPE_PERP_LINEAR,
		manoochv1.MarketType_MARKET_TYPE_PERP_INVERSE,
		manoochv1.MarketType_MARKET_TYPE_FUTURE_LINEAR,
		manoochv1.MarketType_MARKET_TYPE_FUTURE_INVERSE:
		return true
	}
	return false
}

// IsInverse reports whether the market type is inverse-settled. On an inverse
// instrument a size is a count of contracts, not an amount of base asset;
// collapsing the two makes every position wrong by roughly the price.
func IsInverse(marketType manoochv1.MarketType) bool {
	switch marketType {
	case manoochv1.MarketType_MARKET_TYPE_PERP_INVERSE,
		manoochv1.MarketType_MARKET_TYPE_FUTURE_INVERSE:
		return true
	}
	return false
}

// ChannelValidFor reports whether a channel can exist on a market type. Funding
// on a spot market would otherwise become a stream that is never populated.
func ChannelValidFor(channel manoochv1.Channel, marketType manoochv1.MarketType) bool {
	switch channel {
	case manoochv1.Channel_CHANNEL_METADATA, manoochv1.Channel_CHANNEL_HEALTH:
		return true
	case manoochv1.Channel_CHANNEL_MARK_PRICE, manoochv1.Channel_CHANNEL_INDEX_PRICE,
		manoochv1.Channel_CHANNEL_FUNDING:
		return IsDerivative(marketType)
	}
	return false
}
