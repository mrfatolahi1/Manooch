package publish_test

import (
	"strings"
	"testing"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/publish"
)

func TestKey(t *testing.T) {
	cases := []struct {
		venue  string
		mt     pb.MarketType
		symbol string
		ch     pb.Channel
		want   string
	}{
		{"BINANCE", pb.MarketType_MARKET_TYPE_SPOT, "BTC_USDT", pb.Channel_CHANNEL_ORDERBOOK,
			"Manooch:BINANCE:SPOT:BTC_USDT:orderbook"},
		{"BINANCE", pb.MarketType_MARKET_TYPE_PERP_LINEAR, "BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE,
			"Manooch:BINANCE:PERP_LINEAR:BTC_USDT:mark_price"},
		{"BINANCE", pb.MarketType_MARKET_TYPE_PERP_LINEAR, "BTC_USDT", pb.Channel_CHANNEL_HEALTH,
			"Manooch:BINANCE:PERP_LINEAR:BTC_USDT:health"},
		{"BINANCE", pb.MarketType_MARKET_TYPE_PERP_INVERSE, "BTC_USD", pb.Channel_CHANNEL_FUNDING,
			"Manooch:BINANCE:PERP_INVERSE:BTC_USD:funding"},
		// Casing is enforced by the builder, not asked of the caller.
		{"binance", pb.MarketType_MARKET_TYPE_SPOT, "btc_usdt", pb.Channel_CHANNEL_TRADES,
			"Manooch:BINANCE:SPOT:BTC_USDT:trades"},
	}
	for _, tc := range cases {
		if got := publish.Key(tc.venue, tc.mt, tc.symbol, tc.ch); got != tc.want {
			t.Errorf("Key(%q, %v, %q, %v) = %q, want %q", tc.venue, tc.mt, tc.symbol, tc.ch, got, tc.want)
		}
	}
}

func TestVenueKey(t *testing.T) {
	if got := publish.VenueKey("BINANCE", publish.SubjectHealth); got != "Manooch:BINANCE:venue:health" {
		t.Errorf("VenueKey health = %q", got)
	}
	if got := publish.VenueKey("binance", publish.SubjectRateLimit); got != "Manooch:BINANCE:venue:ratelimit" {
		t.Errorf("VenueKey ratelimit = %q", got)
	}
}

func TestMatchPattern(t *testing.T) {
	if got := publish.MatchPattern("BINANCE"); got != "Manooch:BINANCE:*" {
		t.Errorf("MatchPattern(BINANCE) = %q", got)
	}
	if got := publish.MatchPattern(""); got != "Manooch:*" {
		t.Errorf("MatchPattern(\"\") = %q", got)
	}
}

func TestParseKeyRoundTrip(t *testing.T) {
	keys := []string{
		"Manooch:BINANCE:SPOT:BTC_USDT:orderbook",
		"Manooch:BINANCE:PERP_LINEAR:BTC_USDT:mark_price",
		"Manooch:BINANCE:PERP_LINEAR:ETH_USDT:index_price",
		"Manooch:BINANCE:PERP_INVERSE:BTC_USD:funding",
		"Manooch:BINANCE:FUTURE_LINEAR:BTC_USDT:trades",
		"Manooch:OKX:MARGIN:SOL_USDT:health",
		"Manooch:BINANCE:SPOT:BTC_USDT:metadata",
		"Manooch:BINANCE:venue:health",
		"Manooch:BINANCE:venue:ratelimit",
	}
	for _, k := range keys {
		parts, err := publish.ParseKey(k)
		if err != nil {
			t.Errorf("ParseKey(%q): %v", k, err)
			continue
		}
		if got := parts.String(); got != k {
			t.Errorf("round trip: %q -> %+v -> %q", k, parts, got)
		}
	}
}

func TestParseKeyRejects(t *testing.T) {
	cases := []struct {
		name string
		key  string
	}{
		{"empty", ""},
		{"wrong prefix case", "manooch:BINANCE:SPOT:BTC_USDT:orderbook"},
		{"wrong prefix word", "Manuch:BINANCE:SPOT:BTC_USDT:orderbook"},
		{"lower case venue", "Manooch:binance:SPOT:BTC_USDT:orderbook"},
		{"lower case market type", "Manooch:BINANCE:spot:BTC_USDT:orderbook"},
		{"unknown market type", "Manooch:BINANCE:SWAP:BTC_USDT:orderbook"},
		{"unspecified market type", "Manooch:BINANCE:UNSPECIFIED:BTC_USDT:orderbook"},
		{"lower case symbol", "Manooch:BINANCE:SPOT:btc_usdt:orderbook"},
		{"symbol without separator", "Manooch:BINANCE:SPOT:BTCUSDT:orderbook"},
		{"symbol with dash", "Manooch:BINANCE:SPOT:BTC-USDT:orderbook"},
		{"upper case channel", "Manooch:BINANCE:SPOT:BTC_USDT:ORDERBOOK"},
		{"unknown channel", "Manooch:BINANCE:SPOT:BTC_USDT:candles"},
		{"unspecified channel", "Manooch:BINANCE:SPOT:BTC_USDT:unspecified"},
		{"too few components", "Manooch:BINANCE:SPOT"},
		{"too many components", "Manooch:BINANCE:SPOT:BTC_USDT:orderbook:extra"},
		{"stream key length under venue scope", "Manooch:BINANCE:venue:health:extra"},
		{"venue subject upper case", "Manooch:BINANCE:venue:HEALTH"},
		{"trailing separator", "Manooch:BINANCE:SPOT:BTC_USDT:"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := publish.ParseKey(tc.key); err == nil {
				t.Fatalf("ParseKey(%q) succeeded, want an error", tc.key)
			} else if tc.key != "" && !strings.Contains(err.Error(), tc.key) {
				t.Errorf("error does not quote the key: %v", err)
			}
		})
	}
}
