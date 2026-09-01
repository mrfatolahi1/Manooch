package binance_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/adapter/adaptertest"
	"github.com/you/manooch/internal/adapter/binance"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/ratelimit"
	"github.com/you/manooch/internal/transport"
	"github.com/you/manooch/pkg/price"
)

// fixtureDir holds one raw venue frame per case, beside the golden output it
// must normalize to.
var fixtureDir = filepath.Join("..", "..", "..", "testdata", "binance")

func newAdapter(t *testing.T) *binance.Adapter {
	t.Helper()

	a, err := binance.New(binance.Options{
		WSEndpoint:          "wss://fstream.binance.com/stream",
		RESTEndpoint:        "https://fapi.binance.com",
		SymbolOverrides:     map[string]string{"BTC_USDT": "BTCUSDT"},
		MaxStreamsPerSocket: 100,
		ReadTimeout:         60 * time.Second,
		TTLs: map[pb.Channel]time.Duration{
			pb.Channel_CHANNEL_MARK_PRICE:  3 * time.Second,
			pb.Channel_CHANNEL_INDEX_PRICE: 3 * time.Second,
			pb.Channel_CHANNEL_FUNDING:     3 * time.Second,
		},
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return a
}

func spec(t *testing.T, symbol string, ch pb.Channel) core.StreamSpec {
	t.Helper()

	ref, err := core.ParseCanonical(symbol, binance.MarketType)
	if err != nil {
		t.Fatal(err)
	}
	return core.StreamSpec{Instrument: ref, Channel: ch}
}

// TestConformance is the shared suite. A second venue calls the same function
// against its own fixture directory; if it needs a change there, the Adapter
// interface was the wrong shape.
func TestConformance(t *testing.T) {
	adaptertest.RunAdapterConformance(t, newAdapter(t), fixtureDir)
}

// TestOneFrameIsThreeMessages is the shape of this venue: mark price, index
// price and funding all arrive together and land on three separate keys, each
// with its own envelope so the publisher can sequence them independently.
func TestOneFrameIsThreeMessages(t *testing.T) {
	a := newAdapter(t)
	frame := []byte(`{"stream":"btcusdt@markPrice@1s","data":{"e":"markPriceUpdate",` +
		`"E":1562305380000,"s":"BTCUSDT","p":"11794.15000000","i":"11784.62659091",` +
		`"P":"11784.25641265","r":"0.00038167","T":1562306400000}}`)

	msgs, err := a.Parse(frame, adaptertest.RecvNs)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(msgs) != 3 {
		t.Fatalf("Parse produced %d messages, want 3", len(msgs))
	}

	wantKeys := []string{
		"Manooch:BINANCE:PERP_LINEAR:BTC_USDT:mark_price",
		"Manooch:BINANCE:PERP_LINEAR:BTC_USDT:index_price",
		"Manooch:BINANCE:PERP_LINEAR:BTC_USDT:funding",
	}
	for i, want := range wantKeys {
		if msgs[i].Key != want {
			t.Errorf("message %d key = %q, want %q", i, msgs[i].Key, want)
		}
	}

	// Milliseconds converted to nanoseconds, shared across all three.
	const wantExchangeNs = 1562305380000 * int64(time.Millisecond)
	for i, m := range msgs {
		env := m.Proto.(interface{ GetEnv() *pb.Envelope }).GetEnv()
		if env.ExchangeTimeNs != wantExchangeNs {
			t.Errorf("message %d exchange_time_ns = %d, want %d", i, env.ExchangeTimeNs, wantExchangeNs)
		}
		if env.Source != pb.Source_SOURCE_WEBSOCKET {
			t.Errorf("message %d source = %v", i, env.Source)
		}
		if env.VenueSeqPresent {
			t.Errorf("message %d claims a venue sequence; this stream carries none", i)
		}
	}

	// Each message needs its own envelope: one shared pointer would have the
	// publisher stamp the same publish_seq into all three.
	e0 := msgs[0].Proto.(interface{ GetEnv() *pb.Envelope }).GetEnv()
	e1 := msgs[1].Proto.(interface{ GetEnv() *pb.Envelope }).GetEnv()
	if e0 == e1 {
		t.Error("two messages share one envelope")
	}

	mark := msgs[0].Proto.(*pb.MarkPrice)
	if got := price.Price(mark.MarkPrice).String(); got != "11794.15" {
		t.Errorf("mark price = %s, want 11794.15", got)
	}
	index := msgs[1].Proto.(*pb.IndexPrice)
	if got := price.Price(index.IndexPrice).String(); got != "11784.62659091" {
		t.Errorf("index price = %s, want 11784.62659091", got)
	}
	funding := msgs[2].Proto.(*pb.Funding)
	if got := price.Rate(funding.FundingRate).String(); got != "0.00038167" {
		t.Errorf("funding rate = %s, want 0.00038167", got)
	}
	if got, want := funding.NextFundingTimeNs, 1562306400000*int64(time.Millisecond); got != want {
		t.Errorf("next_funding_time_ns = %d, want %d", got, want)
	}
}

// TestEmptyFundingRateSkipsFunding: zero is a real funding rate and empty is
// missing data. Publishing "" as 0 would put a number a strategy trades on
// under a key that should simply have gone stale.
func TestEmptyFundingRateSkipsFunding(t *testing.T) {
	a := newAdapter(t)
	frame := []byte(`{"e":"markPriceUpdate","E":1562305380000,"s":"BTCUSDT",` +
		`"p":"11794.15000000","i":"11784.62659091","r":"","T":0}`)

	msgs, err := a.Parse(frame, adaptertest.RecvNs)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("Parse produced %d messages, want 2 (mark and index, no funding)", len(msgs))
	}
	for _, m := range msgs {
		if m.Channel == pb.Channel_CHANNEL_FUNDING {
			t.Error("a funding message was produced from an empty rate")
		}
	}
}

// TestOutOfRangePriceIsRejected: acceptance requires a price outside the scale
// to be rejected and counted, never wrapped or clamped into something that
// looks like a price.
func TestOutOfRangePriceIsRejected(t *testing.T) {
	a := newAdapter(t)
	frame := []byte(`{"e":"markPriceUpdate","E":1562305380000,"s":"BTCUSDT",` +
		`"p":"99999999999.00000000","i":"11784.62659091","r":"0.00038167","T":1562306400000}`)

	msgs, err := a.Parse(frame, adaptertest.RecvNs)
	if len(msgs) != 0 {
		t.Errorf("Parse produced %d messages for an out-of-range price", len(msgs))
	}

	var pe *core.ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("Parse error = %v (%T), want *core.ParseError", err, err)
	}
	if pe.Kind != core.KindRange {
		t.Errorf("kind = %q, want %q", pe.Kind, core.KindRange)
	}
	if !errors.Is(err, price.ErrOutOfRange) {
		t.Errorf("error does not wrap price.ErrOutOfRange: %v", err)
	}
}

// TestControlFramesAreNotErrors: acks and pongs are normal traffic. Treating
// one as a failure would count parse errors on a healthy socket.
func TestControlFramesAreNotErrors(t *testing.T) {
	a := newAdapter(t)
	for _, frame := range []string{`{"result":null,"id":1}`, `{"result":null,"id":0}`} {
		msgs, err := a.Parse([]byte(frame), adaptertest.RecvNs)
		if err != nil {
			t.Errorf("Parse(%s) = %v, want no error", frame, err)
		}
		if len(msgs) != 0 {
			t.Errorf("Parse(%s) produced %d messages", frame, len(msgs))
		}
	}
}

func TestVenueSymbol(t *testing.T) {
	a := newAdapter(t)
	cases := []struct{ canonical, want string }{
		{"BTC_USDT", "BTCUSDT"}, // via symbol_overrides
		{"ETH_USDT", "ETHUSDT"}, // via the strip-separator rule
		{"1000PEPE_USDT", "1000PEPEUSDT"},
	}
	for _, tc := range cases {
		ref, err := core.ParseCanonical(tc.canonical, binance.MarketType)
		if err != nil {
			t.Fatal(err)
		}
		got, err := a.VenueSymbol(ref)
		if err != nil {
			t.Errorf("VenueSymbol(%s): %v", tc.canonical, err)
			continue
		}
		if got != tc.want {
			t.Errorf("VenueSymbol(%s) = %q, want %q", tc.canonical, got, tc.want)
		}
	}

	// Spot is not served, and answering for it would put a spot price under a
	// perpetual's key.
	spotRef, err := core.ParseCanonical("BTC_USDT", pb.MarketType_MARKET_TYPE_SPOT)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.VenueSymbol(spotRef); err == nil {
		t.Error("VenueSymbol accepted a SPOT instrument")
	}
}

func TestParseVenueSymbol(t *testing.T) {
	a := newAdapter(t)
	cases := []struct{ in, want string }{
		{"BTCUSDT", "BTC_USDT"},
		{"ETHUSDT", "ETH_USDT"},
		{"ETHBTC", "ETH_BTC"},
		{"BTCUSDC", "BTC_USDC"},
	}
	for _, tc := range cases {
		ref, err := a.ParseVenueSymbol(tc.in, binance.MarketType)
		if err != nil {
			t.Errorf("ParseVenueSymbol(%s): %v", tc.in, err)
			continue
		}
		if got := ref.Canonical(); got != tc.want {
			t.Errorf("ParseVenueSymbol(%s) = %q, want %q", tc.in, got, tc.want)
		}
		if ref.Settle != ref.Quote {
			t.Errorf("ParseVenueSymbol(%s) settle = %q, want the quote %q", tc.in, ref.Settle, ref.Quote)
		}
	}

	// A dated contract is not a perpetual, and its price is not one either.
	if _, err := a.ParseVenueSymbol("BTCUSDT_240329", binance.MarketType); err == nil {
		t.Error("ParseVenueSymbol accepted a dated contract")
	}
	if _, err := a.ParseVenueSymbol("BTCXYZ", binance.MarketType); err == nil {
		t.Error("ParseVenueSymbol accepted an unknown quote asset")
	}
	if _, err := a.ParseVenueSymbol("", binance.MarketType); err == nil {
		t.Error("ParseVenueSymbol accepted an empty symbol")
	}
}

// TestRoundTripSymbols: whatever VenueSymbol writes, ParseVenueSymbol must read
// back. A one-way mapping puts REST responses under keys the websocket never
// writes to.
func TestRoundTripSymbols(t *testing.T) {
	a := newAdapter(t)
	for _, canonical := range []string{"BTC_USDT", "ETH_USDT", "SOL_USDT", "ETH_BTC"} {
		ref, err := core.ParseCanonical(canonical, binance.MarketType)
		if err != nil {
			t.Fatal(err)
		}
		sym, err := a.VenueSymbol(ref)
		if err != nil {
			t.Fatalf("VenueSymbol(%s): %v", canonical, err)
		}
		back, err := a.ParseVenueSymbol(sym, binance.MarketType)
		if err != nil {
			t.Fatalf("ParseVenueSymbol(%s): %v", sym, err)
		}
		if back != ref {
			t.Errorf("%s -> %s -> %s", canonical, sym, back)
		}
	}
}

// TestPlanSubscriptionsDeduplicates: all three channels ride one venue stream,
// so subscribing per channel would spend three of the venue's slots and
// deliver the same frame three times.
func TestPlanSubscriptionsDeduplicates(t *testing.T) {
	a := newAdapter(t)
	var specs []core.StreamSpec
	for _, sym := range []string{"ETH_USDT", "BTC_USDT"} {
		for _, ch := range binance.Channels {
			specs = append(specs, spec(t, sym, ch))
		}
	}

	plans, err := a.PlanSubscriptions(specs)
	if err != nil {
		t.Fatalf("PlanSubscriptions: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %d, want 1", len(plans))
	}
	if len(plans[0].Specs) != 6 {
		t.Errorf("plan carries %d specs, want 6", len(plans[0].Specs))
	}

	url, err := a.SocketURL(plans[0])
	if err != nil {
		t.Fatalf("SocketURL: %v", err)
	}
	// Symbols lower case in the path, sorted so the URL is stable across runs.
	want := "wss://fstream.binance.com/stream?streams=btcusdt@markPrice@1s/ethusdt@markPrice@1s"
	if url != want {
		t.Errorf("SocketURL = %q, want %q", url, want)
	}
}

// TestPlanSubscriptionsIsStable: plan IDs label reconnect metrics and log
// lines, so the same config must produce the same IDs whatever order the
// streams arrive in.
func TestPlanSubscriptionsIsStable(t *testing.T) {
	a := newAdapter(t)
	forward := []core.StreamSpec{
		spec(t, "BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE),
		spec(t, "ETH_USDT", pb.Channel_CHANNEL_MARK_PRICE),
		spec(t, "SOL_USDT", pb.Channel_CHANNEL_FUNDING),
	}
	reversed := []core.StreamSpec{forward[2], forward[1], forward[0]}

	a1, err := a.PlanSubscriptions(forward)
	if err != nil {
		t.Fatal(err)
	}
	a2, err := a.PlanSubscriptions(reversed)
	if err != nil {
		t.Fatal(err)
	}
	if len(a1) != len(a2) {
		t.Fatalf("plans = %d and %d", len(a1), len(a2))
	}
	for i := range a1 {
		if a1[i].ID != a2[i].ID {
			t.Errorf("plan %d id = %q and %q", i, a1[i].ID, a2[i].ID)
		}
		u1, _ := a.SocketURL(a1[i])
		u2, _ := a.SocketURL(a2[i])
		if u1 != u2 {
			t.Errorf("plan %d url = %q and %q", i, u1, u2)
		}
	}
}

// TestPlanSubscriptionsChunks respects the venue's per-socket limit.
func TestPlanSubscriptionsChunks(t *testing.T) {
	a, err := binance.New(binance.Options{
		WSEndpoint:          "wss://fstream.binance.com/stream",
		MaxStreamsPerSocket: 2,
		TTLs: map[pb.Channel]time.Duration{
			pb.Channel_CHANNEL_MARK_PRICE:  time.Second,
			pb.Channel_CHANNEL_INDEX_PRICE: time.Second,
			pb.Channel_CHANNEL_FUNDING:     time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	var specs []core.StreamSpec
	for _, sym := range []string{"BTC_USDT", "ETH_USDT", "SOL_USDT", "XRP_USDT", "ADA_USDT"} {
		specs = append(specs, spec(t, sym, pb.Channel_CHANNEL_MARK_PRICE))
	}
	plans, err := a.PlanSubscriptions(specs)
	if err != nil {
		t.Fatalf("PlanSubscriptions: %v", err)
	}
	if len(plans) != 3 {
		t.Fatalf("plans = %d, want 3 (5 streams at 2 per socket)", len(plans))
	}
	seen := map[string]bool{}
	for _, p := range plans {
		if len(p.Specs) > 2 {
			t.Errorf("plan %s carries %d streams, over the limit of 2", p.ID, len(p.Specs))
		}
		if seen[p.ID] {
			t.Errorf("duplicate plan id %q", p.ID)
		}
		seen[p.ID] = true
	}
}

// TestPlanSubscriptionsRejectsUnservedStreams: a stream this adapter cannot
// carry must be a startup error, not a key nobody ever writes — an unwritten
// key looks exactly like a venue that went quiet.
func TestPlanSubscriptionsRejectsUnservedStreams(t *testing.T) {
	a := newAdapter(t)

	spotRef, err := core.ParseCanonical("BTC_USDT", pb.MarketType_MARKET_TYPE_SPOT)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.PlanSubscriptions([]core.StreamSpec{
		{Instrument: spotRef, Channel: pb.Channel_CHANNEL_MARK_PRICE},
	}); err == nil {
		t.Error("PlanSubscriptions accepted a SPOT stream")
	}

	if _, err := a.PlanSubscriptions([]core.StreamSpec{
		spec(t, "BTC_USDT", pb.Channel_CHANNEL_METADATA),
	}); err == nil {
		t.Error("PlanSubscriptions accepted a metadata stream this adapter does not serve")
	}
}

func TestNewRejectsIncompleteOptions(t *testing.T) {
	full := binance.Options{
		WSEndpoint:          "wss://fstream.binance.com/stream",
		MaxStreamsPerSocket: 10,
		TTLs: map[pb.Channel]time.Duration{
			pb.Channel_CHANNEL_MARK_PRICE:  time.Second,
			pb.Channel_CHANNEL_INDEX_PRICE: time.Second,
			pb.Channel_CHANNEL_FUNDING:     time.Second,
		},
	}
	if _, err := binance.New(full); err != nil {
		t.Fatalf("New with complete options: %v", err)
	}

	noEndpoint := full
	noEndpoint.WSEndpoint = ""
	if _, err := binance.New(noEndpoint); err == nil {
		t.Error("New accepted an empty ws endpoint")
	}

	noLimit := full
	noLimit.MaxStreamsPerSocket = 0
	if _, err := binance.New(noLimit); err == nil {
		t.Error("New accepted max_streams_per_socket of 0")
	}

	// A missing TTL would publish a key with no expiry, which reports a dead
	// stream as fresh forever.
	missingTTL := full
	missingTTL.TTLs = map[pb.Channel]time.Duration{pb.Channel_CHANNEL_MARK_PRICE: time.Second}
	if _, err := binance.New(missingTTL); err == nil {
		t.Error("New accepted options with no funding TTL")
	}
}

func TestRESTCost(t *testing.T) {
	a := newAdapter(t)
	if got := a.RESTCost(core.OpFetchOnce); got != 1 {
		t.Errorf("RESTCost(fetch_once) = %d, want 1", got)
	}
	if got := a.RESTCost(core.OpFetchMetadata); got != 1 {
		t.Errorf("RESTCost(fetch_metadata) = %d, want 1", got)
	}
}

// ---------- REST fallback ----------

const premiumIndexBody = `{"symbol":"BTCUSDT","markPrice":"11794.15000000",` +
	`"indexPrice":"11784.62659091","estimatedSettlePrice":"11784.25641265",` +
	`"lastFundingRate":"0.00038167","interestRate":"0.00010000",` +
	`"nextFundingTime":1562306400000,"time":1562305380000}`

func restAdapter(t *testing.T, h http.HandlerFunc) *binance.Adapter {
	t.Helper()

	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	a, err := binance.New(binance.Options{
		WSEndpoint:          "wss://fstream.binance.com/stream",
		RESTEndpoint:        srv.URL,
		MaxStreamsPerSocket: 100,
		TTLs: map[pb.Channel]time.Duration{
			pb.Channel_CHANNEL_MARK_PRICE:  3 * time.Second,
			pb.Channel_CHANNEL_INDEX_PRICE: 3 * time.Second,
			pb.Channel_CHANNEL_FUNDING:     3 * time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

// TestFetchOnceReturnsOnlyTheRequestedChannel: the endpoint answers all three,
// but a caller polling because one key expired asked for that key, and
// overwriting two others from a source it did not choose is a surprise.
func TestFetchOnceReturnsOnlyTheRequestedChannel(t *testing.T) {
	var gotPath string
	a := restAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.RequestURI()
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(premiumIndexBody))
	})

	for _, tc := range []struct {
		ch    pb.Channel
		check func(t *testing.T, m core.Message)
	}{
		{pb.Channel_CHANNEL_MARK_PRICE, func(t *testing.T, m core.Message) {
			if got := price.Price(m.Proto.(*pb.MarkPrice).MarkPrice).String(); got != "11794.15" {
				t.Errorf("mark price = %s", got)
			}
		}},
		{pb.Channel_CHANNEL_INDEX_PRICE, func(t *testing.T, m core.Message) {
			if got := price.Price(m.Proto.(*pb.IndexPrice).IndexPrice).String(); got != "11784.62659091" {
				t.Errorf("index price = %s", got)
			}
		}},
		{pb.Channel_CHANNEL_FUNDING, func(t *testing.T, m core.Message) {
			if got := price.Rate(m.Proto.(*pb.Funding).FundingRate).String(); got != "0.00038167" {
				t.Errorf("funding rate = %s", got)
			}
		}},
	} {
		t.Run(core.ChannelName(tc.ch), func(t *testing.T) {
			msgs, err := a.FetchOnce(context.Background(), spec(t, "BTC_USDT", tc.ch))
			if err != nil {
				t.Fatalf("FetchOnce: %v", err)
			}
			if len(msgs) != 1 {
				t.Fatalf("FetchOnce returned %d messages, want 1", len(msgs))
			}
			m := msgs[0]
			if m.Channel != tc.ch {
				t.Errorf("channel = %v, want %v", m.Channel, tc.ch)
			}

			// A polled value must say so, or a consumer cannot tell a live
			// stream from a stream being propped up by REST.
			env := m.Proto.(interface{ GetEnv() *pb.Envelope }).GetEnv()
			if env.Source != pb.Source_SOURCE_REST {
				t.Errorf("source = %v, want REST", env.Source)
			}
			if env.Status == pb.Status_STATUS_UNSPECIFIED {
				t.Error("status is unspecified")
			}
			if env.RecvTimeNs <= 0 {
				t.Error("recv_time_ns not stamped")
			}
			if env.PublishTimeNs != 0 {
				t.Error("publish_time_ns set outside the publisher")
			}
			tc.check(t, m)
		})
	}

	if gotPath != "/fapi/v1/premiumIndex?symbol=BTCUSDT" {
		t.Errorf("requested %q", gotPath)
	}
}

// TestFetchOnceSurfacesVenueErrors: Binance answers a rejection with a code and
// a message, and dropping them leaves an operator with "HTTP 400".
func TestFetchOnceSurfacesVenueErrors(t *testing.T) {
	a := restAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		_, _ = w.Write([]byte(`{"code":-1121,"msg":"Invalid symbol."}`))
	})

	_, err := a.FetchOnce(context.Background(), spec(t, "BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE))
	if err == nil {
		t.Fatal("FetchOnce against a 400 succeeded")
	}
	var pe *core.ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("error = %v (%T), want *core.ParseError", err, err)
	}
	if pe.Kind != core.KindVenue {
		t.Errorf("kind = %q, want %q", pe.Kind, core.KindVenue)
	}
	if !contains(err.Error(), "-1121") {
		t.Errorf("error drops the venue's code: %v", err)
	}
}

func contains(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// ---------- rate limiting ----------

// denyingLimiter refuses everything, which is what a spent budget looks like
// from inside an adapter.
type denyingLimiter struct{}

func (denyingLimiter) Allow(context.Context, string, ratelimit.LimitKind, int) error {
	return ratelimit.ErrBudgetExhausted
}
func (denyingLimiter) Used(string, ratelimit.LimitKind) (int, int) { return 0, 0 }

// TestLimiterDenialStopsTheRequest: a denial means the operation does not
// happen and the caller is told, never that we proceeded on the assumption
// that it was probably fine. The fallback poller turns the error into STALE
// and the supervisor turns it into a failed dial.
func TestLimiterDenialStopsTheRequest(t *testing.T) {
	var calls int
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		_, _ = w.Write([]byte(premiumIndexBody))
	}))
	t.Cleanup(srv.Close)

	a, err := binance.New(binance.Options{
		WSEndpoint:          "wss://fstream.binance.com/stream",
		RESTEndpoint:        srv.URL,
		MaxStreamsPerSocket: 100,
		Limiter:             denyingLimiter{},
		TTLs: map[pb.Channel]time.Duration{
			pb.Channel_CHANNEL_MARK_PRICE:  3 * time.Second,
			pb.Channel_CHANNEL_INDEX_PRICE: 3 * time.Second,
			pb.Channel_CHANNEL_FUNDING:     3 * time.Second,
		},
		Dial: func(context.Context, transport.Options) (core.Conn, error) {
			t.Error("Dial reached the transport with no connect budget")
			return nil, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = a.FetchOnce(context.Background(), spec(t, "BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE))
	if !errors.Is(err, ratelimit.ErrBudgetExhausted) {
		t.Errorf("FetchOnce = %v, want ErrBudgetExhausted", err)
	}
	if calls != 0 {
		t.Errorf("the venue was called %d times with no budget", calls)
	}

	plans, err := a.PlanSubscriptions([]core.StreamSpec{spec(t, "BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE)})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := a.Dial(context.Background(), plans[0]); !errors.Is(err, ratelimit.ErrBudgetExhausted) {
		t.Errorf("Dial = %v, want ErrBudgetExhausted", err)
	}
}

// ---------- metadata ----------

// exchangeInfoBody is one perpetual, one dated delivery contract on the same
// pair, and one symbol whose quote asset this adapter has no mapping for.
const exchangeInfoBody = `{
  "timezone":"UTC","serverTime":1562305380000,
  "rateLimits":[{"rateLimitType":"REQUEST_WEIGHT","interval":"MINUTE","limit":2400}],
  "symbols":[
    {"symbol":"BTCUSDT","pair":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING",
     "baseAsset":"BTC","quoteAsset":"USDT",
     "filters":[
       {"filterType":"PRICE_FILTER","maxPrice":"4529764","minPrice":"556.80","tickSize":"0.10"},
       {"filterType":"LOT_SIZE","maxQty":"1000","minQty":"0.001","stepSize":"0.001"},
       {"filterType":"MIN_NOTIONAL","notional":"100"},
       {"filterType":"MAX_NUM_ORDERS","limit":200}
     ]},
    {"symbol":"BTCUSDT_240329","pair":"BTCUSDT","contractType":"CURRENT_QUARTER","status":"TRADING",
     "filters":[{"filterType":"PRICE_FILTER","tickSize":"0.10"},
                {"filterType":"LOT_SIZE","maxQty":"1000","minQty":"0.001","stepSize":"0.001"}]},
    {"symbol":"BTCXYZ","pair":"BTCXYZ","contractType":"PERPETUAL","status":"TRADING",
     "filters":[{"filterType":"PRICE_FILTER","tickSize":"0.10"},
                {"filterType":"LOT_SIZE","maxQty":"1000","minQty":"0.001","stepSize":"0.001"}]},
    {"symbol":"ETHUSDT","pair":"ETHUSDT","contractType":"PERPETUAL","status":"SETTLING",
     "filters":[{"filterType":"PRICE_FILTER","tickSize":"0.01"},
                {"filterType":"LOT_SIZE","maxQty":"10000","minQty":"0.01","stepSize":"0.01"}]}
  ]}`

// TestFetchMetadata: the public instrument list, mapped onto the scales
// everything else in the service uses.
func TestFetchMetadata(t *testing.T) {
	var gotPath string
	a := restAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.RequestURI()
		_, _ = w.Write([]byte(exchangeInfoBody))
	})

	metas, err := a.FetchMetadata(context.Background(), binance.MarketType)
	if err != nil {
		t.Fatalf("FetchMetadata: %v", err)
	}
	if gotPath != "/fapi/v1/exchangeInfo" {
		t.Errorf("requested %q", gotPath)
	}

	// The dated contract and the unmappable quote asset are skipped, not
	// errors: the endpoint lists every contract Binance has, and failing on one
	// nobody asked about would take the whole refresh down.
	if len(metas) != 2 {
		t.Fatalf("metas = %d, want 2 (BTCUSDT and ETHUSDT)", len(metas))
	}

	btc := metas[0]
	if got := btc.Env.Instrument.Canonical; got != "BTC_USDT" {
		t.Errorf("canonical = %q", got)
	}
	if got := btc.Env.Instrument.VenueSymbol; got != "BTCUSDT" {
		t.Errorf("venue_symbol = %q", got)
	}
	if got := price.Price(btc.TickSize).String(); got != "0.1" {
		t.Errorf("tick_size = %s, want 0.1", got)
	}
	if got := price.Size(btc.LotSize).String(); got != "0.001" {
		t.Errorf("lot_size = %s, want 0.001", got)
	}
	if got := price.Size(btc.MinSize).String(); got != "0.001" {
		t.Errorf("min_size = %s, want 0.001", got)
	}
	if got := price.Size(btc.MaxSize).String(); got != "1000" {
		t.Errorf("max_size = %s, want 1000", got)
	}
	if got := price.Price(btc.MinNotional).String(); got != "100" {
		t.Errorf("min_notional = %s, want 100", got)
	}
	// Linear perps trade in base units, so the multiplier is one — stated, not
	// left zero: a consumer multiplying by a missing multiplier sizes nothing.
	if got := price.Size(btc.ContractMultiplier).String(); got != "1" {
		t.Errorf("contract_multiplier = %s, want 1", got)
	}
	if !btc.Active {
		t.Error("a TRADING symbol is not active")
	}
	if btc.LastRefreshNs <= 0 {
		t.Error("last_refresh_ns not stamped")
	}
	if btc.Env.Source != pb.Source_SOURCE_REST {
		t.Errorf("source = %v, want REST", btc.Env.Source)
	}
	if btc.Env.Status == pb.Status_STATUS_UNSPECIFIED {
		t.Error("status is unspecified")
	}
	if want := 1562305380000 * int64(time.Millisecond); btc.Env.ExchangeTimeNs != want {
		t.Errorf("exchange_time_ns = %d, want %d", btc.Env.ExchangeTimeNs, want)
	}
	// serverTime is the venue's clock when it answered, so it is a send time.
	// Saying otherwise tells a consumer this timestamp may be hours old and
	// drops the message out of the publish-latency histogram.
	if !btc.Env.ExchangeTimeIsSendTime {
		t.Error("exchange_time_is_send_time is false; serverTime is a send time")
	}
	if btc.Env.PublishTimeNs != 0 || btc.Env.PublishSeq != 0 {
		t.Error("the adapter stamped publisher-owned fields")
	}

	// A symbol that is not trading is published and marked inactive rather
	// than dropped: silence would read as a symbol the venue never had.
	eth := metas[1]
	if eth.Active {
		t.Error("a SETTLING symbol is active")
	}
}

// TestFetchMetadataRejectsUnusableFilters: a symbol we do serve, with no
// precision, is an error. Precision a consumer cannot round an order to is not
// metadata.
func TestFetchMetadataRejectsUnusableFilters(t *testing.T) {
	a := restAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"serverTime":1562305380000,"symbols":[
		  {"symbol":"BTCUSDT","contractType":"PERPETUAL","status":"TRADING","filters":[]}]}`))
	})

	_, err := a.FetchMetadata(context.Background(), binance.MarketType)
	var pe *core.ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("FetchMetadata = %v (%T), want *core.ParseError", err, err)
	}
	if pe.Kind != core.KindField {
		t.Errorf("kind = %q, want %q", pe.Kind, core.KindField)
	}
}

// TestFetchMetadataRejectsAnEmptyList: a response listing no perpetuals is not
// an empty venue, it is a response we did not understand.
func TestFetchMetadataRejectsAnEmptyList(t *testing.T) {
	a := restAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"serverTime":1562305380000,"symbols":[]}`))
	})
	if _, err := a.FetchMetadata(context.Background(), binance.MarketType); err == nil {
		t.Error("FetchMetadata accepted a list with no perpetuals")
	}
}

// TestFetchMetadataSurfacesVenueErrors keeps Binance's own message, the same
// way FetchOnce does.
func TestFetchMetadataSurfacesVenueErrors(t *testing.T) {
	a := restAdapter(t, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests)
		_, _ = w.Write([]byte(`{"code":-1003,"msg":"Too many requests."}`))
	})
	_, err := a.FetchMetadata(context.Background(), binance.MarketType)
	if err == nil {
		t.Fatal("FetchMetadata against a 429 succeeded")
	}
	if !contains(err.Error(), "-1003") {
		t.Errorf("error drops the venue's code: %v", err)
	}
}

// TestStalledVenueDoesNotParkAFetch: the same bound on the REST path. A poll
// that never returns holds one of the fallback's few concurrent slots, and the
// context it is given is the run context, which has no deadline.
func TestStalledVenueDoesNotParkAFetch(t *testing.T) {
	stalled := make(chan struct{})

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-stalled
	}))
	t.Cleanup(srv.Close)
	// Registered after the server's own cleanup so it runs before it: Close
	// waits for outstanding requests, and this is what lets that one finish.
	t.Cleanup(func() { close(stalled) })

	a, err := binance.New(binance.Options{
		WSEndpoint:          "wss://fstream.binance.com/stream",
		RESTEndpoint:        srv.URL,
		MaxStreamsPerSocket: 100,
		HTTPTimeout:         200 * time.Millisecond,
		TTLs: map[pb.Channel]time.Duration{
			pb.Channel_CHANNEL_MARK_PRICE:  3 * time.Second,
			pb.Channel_CHANNEL_INDEX_PRICE: 3 * time.Second,
			pb.Channel_CHANNEL_FUNDING:     3 * time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		_, err := a.FetchOnce(context.Background(), spec(t, "BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE))
		done <- err
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("FetchOnce succeeded against a venue that answered nothing")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("FetchOnce parked on a stalled venue; it would hold a fallback slot forever")
	}
}
