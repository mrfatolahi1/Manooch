// Package adapter_test holds the assertions that only make sense with more than
// one venue in the room: that a value below float64 precision survives both
// adapters exactly, and that identical input normalizes to identical output.
package adapter_test

import (
	"errors"
	"testing"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/adapter/adaptertest"
	"github.com/you/manooch/internal/adapter/binance"
	"github.com/you/manooch/internal/adapter/kucoin"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/pkg/price"
	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"
)

// ttls are deliberately identical across the two venues here. In production
// they are not — KuCoin's funding cadence is a minute against Binance's second,
// so its funding key lives 180s against 3s — and holding them equal is what
// makes the comparison below about normalization rather than about cadence.
var ttls = map[pb.Channel]time.Duration{
	pb.Channel_CHANNEL_MARK_PRICE:  3 * time.Second,
	pb.Channel_CHANNEL_INDEX_PRICE: 3 * time.Second,
	pb.Channel_CHANNEL_FUNDING:     3 * time.Second,
}

// exchangeMS is the one instant both venues stamp their frames with.
const exchangeMS = 1731899129000

func newBinance(t *testing.T) core.Adapter {
	t.Helper()
	a, err := binance.New(binance.Options{
		WSEndpoint:          "wss://fstream.binance.com/stream",
		SymbolOverrides:     map[string]string{"BTC_USDT": "BTCUSDT"},
		MaxStreamsPerSocket: 100,
		TTLs:                ttls,
	})
	if err != nil {
		t.Fatalf("binance.New: %v", err)
	}
	return a
}

func newKuCoin(t *testing.T) core.Adapter {
	t.Helper()
	a, err := kucoin.New(kucoin.Options{
		WSEndpoint:          "https://api-futures.kucoin.com",
		SymbolOverrides:     map[string]string{"BTC_USDT": "XBTUSDTM"},
		MaxStreamsPerSocket: 50,
		TTLs:                ttls,
	})
	if err != nil {
		t.Fatalf("kucoin.New: %v", err)
	}
	return a
}

// binanceFrame is one markPriceUpdate with the given decimals. Binance quotes
// its numbers.
func binanceFrame(mark, index, rate string) []byte {
	return []byte(`{"e":"markPriceUpdate","E":` + itoa(exchangeMS) + `,"s":"BTCUSDT",` +
		`"p":"` + mark + `","i":"` + index + `","r":"` + rate + `","T":` + itoa(exchangeMS+3600000) + `}`)
}

// kucoinMarkIndexFrame is one mark.index.price message. KuCoin does not quote
// its numbers, which is the whole point of the precision test below.
func kucoinMarkIndexFrame(mark, index string) []byte {
	return []byte(`{"topic":"/contract/instrument:XBTUSDTM","type":"message",` +
		`"subject":"mark.index.price","data":{"markPrice":` + mark + `,"indexPrice":` + index +
		`,"granularity":1000,"timestamp":` + itoa(exchangeMS) + `}}`)
}

func kucoinFundingFrame(rate string) []byte {
	return []byte(`{"topic":"/contract/instrument:XBTUSDTM","type":"message",` +
		`"subject":"funding.rate","data":{"granularity":60000,"fundingRate":` + rate +
		`,"timestamp":` + itoa(exchangeMS) + `}}`)
}

func itoa(v int64) string {
	if v == 0 {
		return "0"
	}
	var b []byte
	for v > 0 {
		b = append([]byte{byte('0' + v%10)}, b...)
		v /= 10
	}
	return string(b)
}

// byChannel indexes a Parse result so a test can name a channel rather than an
// offset.
func byChannel(t *testing.T, msgs []core.Message) map[pb.Channel]core.Message {
	t.Helper()
	out := make(map[pb.Channel]core.Message, len(msgs))
	for _, m := range msgs {
		out[m.Channel] = m
	}
	return out
}

// ---------- precision ----------

// TestSubFloatPrecisionRoundTrips is the test that catches a json.Unmarshal
// into float64 slipping into either adapter.
//
// 1234567.89012345678 at the price scale is 123456789012345678: eighteen
// significant digits, where float64 holds fifteen or sixteen. A value that comes
// back exact cannot have been through one. Binance quotes the number and KuCoin
// does not, so the two paths through the decoder are both covered.
func TestSubFloatPrecisionRoundTrips(t *testing.T) {
	const (
		decimal = "1234567.89012345678"
		want    = int64(123456789012345678)
	)

	for name, got := range map[string]int64{
		"binance": markPriceOf(t, newBinance(t), binanceFrame(decimal, decimal, "0.00038167")),
		"kucoin":  markPriceOf(t, newKuCoin(t), kucoinMarkIndexFrame(decimal, decimal)),
	} {
		t.Run(name, func(t *testing.T) {
			if got != want {
				t.Errorf("mark price = %d, want %d exactly", got, want)
			}
			// And it renders back to the digits the venue sent.
			if s := price.Price(got).String(); s != decimal {
				t.Errorf("round trip = %s, want %s", s, decimal)
			}
		})
	}
}

// TestSmallestRepresentablePriceSurvives: the bottom of the range is where a
// float64 multiply loses everything. 1e-11 is one unit at the price scale.
func TestSmallestRepresentablePriceSurvives(t *testing.T) {
	const decimal = "0.00000000001"
	for name, got := range map[string]int64{
		"binance": markPriceOf(t, newBinance(t), binanceFrame(decimal, decimal, "0.00038167")),
		"kucoin":  markPriceOf(t, newKuCoin(t), kucoinMarkIndexFrame(decimal, decimal)),
	} {
		t.Run(name, func(t *testing.T) {
			if got != 1 {
				t.Errorf("mark price = %d, want 1", got)
			}
		})
	}
}

// TestFinerThanTheScaleIsRejected: 0.000000000123456789 needs 1e-18 and the
// price scale is 1e-11. It is rejected rather than truncated, which is exactly
// what a float64 path would not do — that path would accept it, round it, and
// publish a price nobody could tell from a real one.
func TestFinerThanTheScaleIsRejected(t *testing.T) {
	const decimal = "0.000000000123456789"

	for name, tc := range map[string]struct {
		a     core.Adapter
		frame []byte
	}{
		"binance": {newBinance(t), binanceFrame(decimal, decimal, "0.00038167")},
		"kucoin":  {newKuCoin(t), kucoinMarkIndexFrame(decimal, decimal)},
	} {
		t.Run(name, func(t *testing.T) {
			msgs, err := tc.a.Parse(tc.frame, adaptertest.RecvNs)
			if len(msgs) != 0 {
				t.Errorf("Parse produced %d messages for a value finer than the scale", len(msgs))
			}
			if !errors.Is(err, price.ErrPrecisionLoss) {
				t.Fatalf("Parse = %v, want price.ErrPrecisionLoss", err)
			}
			// Counted apart from a malformed value: this is the one failure
			// that would otherwise publish a plausible wrong price.
			var pe *core.ParseError
			if !errors.As(err, &pe) || pe.Kind != core.KindRange {
				t.Errorf("kind = %v, want %q", pe, core.KindRange)
			}
		})
	}
}

func markPriceOf(t *testing.T, a core.Adapter, frame []byte) int64 {
	t.Helper()
	msgs, err := a.Parse(frame, adaptertest.RecvNs)
	if err != nil {
		t.Fatalf("%s Parse: %v", a.Venue(), err)
	}
	m, ok := byChannel(t, msgs)[pb.Channel_CHANNEL_MARK_PRICE]
	if !ok {
		t.Fatalf("%s produced no mark price", a.Venue())
	}
	return m.Proto.(*pb.MarkPrice).MarkPrice
}

// ---------- cross-venue normalization ----------

// TestIdenticalInputNormalizesIdentically is what "normalize everything" means,
// made testable: the same logical values from two venues that spell, quote and
// split them differently must come out as the same bytes apart from the venue
// name and the venue's own symbol.
//
// If this ever fails, one adapter is deciding something the other is not, and
// a consumer would have to know which venue it was reading to interpret the
// number.
func TestIdenticalInputNormalizesIdentically(t *testing.T) {
	const (
		mark  = "90445.02"
		index = "90440.135"
	)

	bin, err := newBinance(t).Parse(binanceFrame(mark, index, "0.00038167"), adaptertest.RecvNs)
	if err != nil {
		t.Fatalf("binance Parse: %v", err)
	}
	kc, err := newKuCoin(t).Parse(kucoinMarkIndexFrame(mark, index), adaptertest.RecvNs)
	if err != nil {
		t.Fatalf("kucoin Parse: %v", err)
	}

	b, k := byChannel(t, bin), byChannel(t, kc)
	for _, ch := range []pb.Channel{pb.Channel_CHANNEL_MARK_PRICE, pb.Channel_CHANNEL_INDEX_PRICE} {
		t.Run(core.ChannelName(ch), func(t *testing.T) {
			bm, ok := b[ch]
			if !ok {
				t.Fatalf("binance produced no %s", core.ChannelName(ch))
			}
			km, ok := k[ch]
			if !ok {
				t.Fatalf("kucoin produced no %s", core.ChannelName(ch))
			}

			if bm.TTL != km.TTL {
				t.Errorf("ttl %v and %v", bm.TTL, km.TTL)
			}
			// The key differs only in the venue component, which is the point:
			// market type, symbol and channel are the venue-independent
			// identity, and both adapters built the key with publish.Key.
			if got, want := strip(km.Key, "KUCOIN"), strip(bm.Key, "BINANCE"); got != want {
				t.Errorf("keys differ beyond the venue: %q and %q", km.Key, bm.Key)
			}
			// The specs are the same instrument on the same channel, which is
			// what the supervisor schedules and the health tracker registers.
			if bm.Spec != km.Spec {
				t.Errorf("specs differ: %v and %v", bm.Spec, km.Spec)
			}

			if diff := comparePayloads(bm.Proto, km.Proto); diff != "" {
				t.Errorf("normalized payloads differ beyond venue and venue_symbol: %s", diff)
			}
		})
	}
}

// TestFundingDiffersOnlyWhereTheVenuesDo: KuCoin's stream carries no next
// funding time and Binance's does. That is a difference in what the venues
// supply, not in how we normalize, and it is asserted here so that it stays the
// only one — a derived value filling that gap would pass silently otherwise.
func TestFundingDiffersOnlyWhereTheVenuesDo(t *testing.T) {
	const rate = "-0.002966"

	bin, err := newBinance(t).Parse(binanceFrame("90445.02", "90440.135", rate), adaptertest.RecvNs)
	if err != nil {
		t.Fatalf("binance Parse: %v", err)
	}
	kc, err := newKuCoin(t).Parse(kucoinFundingFrame(rate), adaptertest.RecvNs)
	if err != nil {
		t.Fatalf("kucoin Parse: %v", err)
	}

	bf, ok := byChannel(t, bin)[pb.Channel_CHANNEL_FUNDING]
	if !ok {
		t.Fatal("binance produced no funding")
	}
	kf, ok := byChannel(t, kc)[pb.Channel_CHANNEL_FUNDING]
	if !ok {
		t.Fatal("kucoin produced no funding")
	}

	if got, want := kf.Proto.(*pb.Funding).FundingRate, bf.Proto.(*pb.Funding).FundingRate; got != want {
		t.Errorf("funding rate = %d and %d for the same decimal", got, want)
	}
	if kf.Proto.(*pb.Funding).NextFundingTimeNs != 0 {
		t.Error("kucoin filled next_funding_time_ns; the stream does not carry one")
	}
	if bf.Proto.(*pb.Funding).NextFundingTimeNs == 0 {
		t.Error("binance dropped next_funding_time_ns; the stream does carry one")
	}

	// With that one venue-supplied difference removed, everything else matches.
	bClone := proto.Clone(bf.Proto).(*pb.Funding)
	bClone.NextFundingTimeNs = 0
	if diff := comparePayloads(bClone, kf.Proto); diff != "" {
		t.Errorf("funding differs beyond next_funding_time_ns: %s", diff)
	}
}

// comparePayloads reports what differs between two payloads once the two fields
// that are allowed to differ — the venue name and the venue's own symbol — are
// cleared. It returns "" when they are otherwise identical.
func comparePayloads(a, b proto.Message) string {
	ca, cb := neutralize(a), neutralize(b)
	if proto.Equal(ca, cb) {
		return ""
	}
	return prototext.Format(ca) + "\nvs\n" + prototext.Format(cb)
}

// neutralize clones a payload and clears the two fields a venue owns.
func neutralize(m proto.Message) proto.Message {
	c := proto.Clone(m)
	env := c.(interface{ GetEnv() *pb.Envelope }).GetEnv()
	env.Venue = ""
	if env.Instrument != nil {
		env.Instrument.VenueSymbol = ""
	}
	return c
}

// strip removes a venue name from a key so two venues' keys can be compared.
func strip(key, venue string) string {
	for i := 0; i+len(venue) <= len(key); i++ {
		if key[i:i+len(venue)] == venue {
			return key[:i] + key[i+len(venue):]
		}
	}
	return key
}
