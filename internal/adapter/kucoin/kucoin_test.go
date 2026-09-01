package kucoin_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/adapter/kucoin"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/core/coretest"
	"github.com/you/manooch/internal/ratelimit"
	"github.com/you/manooch/internal/transport"
)

// fixtureDir holds one raw venue frame per case, beside the golden output it
// must normalize to.
var fixtureDir = filepath.Join("..", "..", "..", "testdata", "kucoin")

// ttls are the venue's real shape: mark and index at one second, funding at one
// minute, each times health.ttl_multiplier. One number for all three would
// expire the funding key between updates.
var ttls = map[pb.Channel]time.Duration{
	pb.Channel_CHANNEL_MARK_PRICE:  3 * time.Second,
	pb.Channel_CHANNEL_INDEX_PRICE: 3 * time.Second,
	pb.Channel_CHANNEL_FUNDING:     180 * time.Second,
}

func newAdapter(t *testing.T) *kucoin.Adapter {
	t.Helper()
	return newAdapterWith(t, kucoin.Options{})
}

// newAdapterWith fills in whatever a case did not set, so each test names only
// what it is actually about.
func newAdapterWith(t *testing.T, opts kucoin.Options) *kucoin.Adapter {
	t.Helper()

	if opts.WSEndpoint == "" {
		opts.WSEndpoint = "https://api-futures.kucoin.com"
	}
	if opts.RESTEndpoint == "" {
		opts.RESTEndpoint = "https://api-futures.kucoin.com"
	}
	if opts.SymbolOverrides == nil {
		opts.SymbolOverrides = map[string]string{"BTC_USDT": "XBTUSDTM"}
	}
	if opts.MaxStreamsPerSocket == 0 {
		opts.MaxStreamsPerSocket = 50
	}
	if opts.TTLs == nil {
		opts.TTLs = ttls
	}
	if opts.ConnectID == nil {
		opts.ConnectID = func() string { return "test-connect-id" }
	}
	a, err := kucoin.New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return a
}

func spec(t *testing.T, symbol string, ch pb.Channel) core.StreamSpec {
	t.Helper()
	ref, err := core.ParseCanonical(symbol, kucoin.MarketType)
	if err != nil {
		t.Fatal(err)
	}
	return core.StreamSpec{Instrument: ref, Channel: ch}
}

// ---------- symbols ----------

// TestVenueSymbol: the rule is {BASE}{QUOTE}M, and symbol_overrides is where
// the assets KuCoin spells differently are stated exactly.
func TestVenueSymbol(t *testing.T) {
	a := newAdapter(t)
	cases := []struct{ canonical, want string }{
		{"BTC_USDT", "XBTUSDTM"}, // via symbol_overrides: bitcoin is XBT here
		{"ETH_USDT", "ETHUSDTM"}, // via the rule
		{"SOL_USDC", "SOLUSDCM"}, //
		{"1000PEPE_USDT", "1000PEPEUSDTM"},
	}
	for _, tc := range cases {
		ref, err := core.ParseCanonical(tc.canonical, kucoin.MarketType)
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
		{"XBTUSDTM", "BTC_USDT"}, // the reversed override
		{"ETHUSDTM", "ETH_USDT"},
		{"SOLUSDCM", "SOL_USDC"},
		{"ATOMUSDTM", "ATOM_USDT"}, // a base that itself ends in M
	}
	for _, tc := range cases {
		ref, err := a.ParseVenueSymbol(tc.in, kucoin.MarketType)
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

	// Without the M it is an index or a spot pair, and its price is not a
	// perpetual's.
	if _, err := a.ParseVenueSymbol("XBTUSDT", kucoin.MarketType); err == nil {
		t.Error("ParseVenueSymbol accepted a symbol with no M suffix")
	}
	if _, err := a.ParseVenueSymbol("XBTXYZM", kucoin.MarketType); err == nil {
		t.Error("ParseVenueSymbol accepted an unknown quote asset")
	}
	if _, err := a.ParseVenueSymbol("", kucoin.MarketType); err == nil {
		t.Error("ParseVenueSymbol accepted an empty symbol")
	}
	if _, err := a.ParseVenueSymbol("XBTUSDTM", pb.MarketType_MARKET_TYPE_SPOT); err == nil {
		t.Error("ParseVenueSymbol accepted a market type this adapter does not serve")
	}
}

// TestRoundTripSymbols: whatever VenueSymbol writes, ParseVenueSymbol must read
// back. A one-way mapping puts REST responses under keys the websocket never
// writes to.
func TestRoundTripSymbols(t *testing.T) {
	a := newAdapter(t)
	for _, canonical := range []string{"BTC_USDT", "ETH_USDT", "SOL_USDT", "ATOM_USDT", "AVAX_USDC"} {
		ref, err := core.ParseCanonical(canonical, kucoin.MarketType)
		if err != nil {
			t.Fatal(err)
		}
		sym, err := a.VenueSymbol(ref)
		if err != nil {
			t.Fatalf("VenueSymbol(%s): %v", canonical, err)
		}
		back, err := a.ParseVenueSymbol(sym, kucoin.MarketType)
		if err != nil {
			t.Fatalf("ParseVenueSymbol(%s): %v", sym, err)
		}
		if back != ref {
			t.Errorf("%s -> %s -> %s", canonical, sym, back)
		}
	}
}

// ---------- planning ----------

// TestPlanSubscriptionsDeduplicates: both subjects ride one topic per symbol,
// so subscribing per channel would spend three of the venue's subscription
// slots and deliver each frame three times.
func TestPlanSubscriptionsDeduplicates(t *testing.T) {
	a := newAdapter(t)
	var specs []core.StreamSpec
	for _, sym := range []string{"ETH_USDT", "BTC_USDT"} {
		for _, ch := range kucoin.Channels {
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
	if plans[0].ID != "kucoin-0" {
		t.Errorf("plan id = %q", plans[0].ID)
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
	}
}

func TestPlanSubscriptionsChunks(t *testing.T) {
	a := newAdapterWith(t, kucoin.Options{MaxStreamsPerSocket: 2})

	var specs []core.StreamSpec
	for _, sym := range []string{"BTC_USDT", "ETH_USDT", "SOL_USDT", "XRP_USDT", "ADA_USDT"} {
		specs = append(specs, spec(t, sym, pb.Channel_CHANNEL_MARK_PRICE))
	}
	plans, err := a.PlanSubscriptions(specs)
	if err != nil {
		t.Fatalf("PlanSubscriptions: %v", err)
	}
	if len(plans) != 3 {
		t.Fatalf("plans = %d, want 3 (5 symbols at 2 per socket)", len(plans))
	}
}

// TestPlanSubscriptionsRejectsUnservedStreams: a stream this adapter cannot
// carry must be a startup error, not a key nobody ever writes.
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
	full := kucoin.Options{
		WSEndpoint:          "https://api-futures.kucoin.com",
		MaxStreamsPerSocket: 50,
		TTLs:                ttls,
	}
	if _, err := kucoin.New(full); err != nil {
		t.Fatalf("New with complete options: %v", err)
	}

	noEndpoint := full
	noEndpoint.WSEndpoint = ""
	if _, err := kucoin.New(noEndpoint); err == nil {
		t.Error("New accepted an empty ws endpoint")
	}

	noLimit := full
	noLimit.MaxStreamsPerSocket = 0
	if _, err := kucoin.New(noLimit); err == nil {
		t.Error("New accepted max_streams_per_socket of 0")
	}

	// A missing TTL would publish a key with no expiry, which reports a dead
	// stream as fresh forever.
	missingTTL := full
	missingTTL.TTLs = map[pb.Channel]time.Duration{pb.Channel_CHANNEL_MARK_PRICE: time.Second}
	if _, err := kucoin.New(missingTTL); err == nil {
		t.Error("New accepted options with no funding TTL")
	}
}

func TestRESTCost(t *testing.T) {
	a := newAdapter(t)
	if got := a.RESTCost(core.OpFetchOnce); got <= 0 {
		t.Errorf("RESTCost(fetch_once) = %d", got)
	}
	if got := a.RESTCost(core.OpFetchMetadata); got <= 0 {
		t.Errorf("RESTCost(fetch_metadata) = %d", got)
	}
	if got := a.RESTCost(core.OpUnspecified); got != 0 {
		t.Errorf("RESTCost(unspecified) = %d, want 0", got)
	}
}

// ---------- bullet and dial ----------

// bulletServer answers the bootstrap POST with a body, counting the calls.
func bulletServer(t *testing.T, body func() (int, string)) (*httptest.Server, *int) {
	t.Helper()
	calls := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/bullet-public" {
			t.Errorf("bullet requested %q", r.URL.Path)
		}
		if r.Method != http.MethodPost {
			t.Errorf("bullet method %q, want POST", r.Method)
		}
		// Public means public: no key, no signature, no session.
		for _, h := range []string{"KC-API-KEY", "KC-API-SIGN", "KC-API-PASSPHRASE", "Authorization"} {
			if r.Header.Get(h) != "" {
				t.Errorf("bullet sent %s; this endpoint is unauthenticated", h)
			}
		}
		calls++
		code, b := body()
		w.WriteHeader(code)
		_, _ = w.Write([]byte(b))
	}))
	t.Cleanup(srv.Close)
	return srv, &calls
}

func bulletBody(t *testing.T) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(fixtureDir, "bullet_response.json"))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

// acking is a connection that answers each subscribe frame with the ack the
// venue would send, so Dial can complete without a real socket.
type acking struct {
	*coretest.Conn
	t *testing.T
}

func (c *acking) Write(ctx context.Context, b []byte) error {
	if err := c.Conn.Write(ctx, b); err != nil {
		return err
	}
	var req struct {
		ID    string `json:"id"`
		Type  string `json:"type"`
		Topic string `json:"topic"`
	}
	if err := json.Unmarshal(b, &req); err != nil {
		c.t.Errorf("frame written to the socket is not json: %s", b)
		return nil
	}
	if req.Type == "subscribe" {
		c.Push([]byte(`{"id":"` + req.ID + `","type":"ack"}`))
	}
	return nil
}

// TestDialBootstrapsAndSubscribes is the shape of this venue: a public REST
// call for a token, a socket built from what it answered, and a subscription
// per topic that is acknowledged before Dial returns.
func TestDialBootstrapsAndSubscribes(t *testing.T) {
	srv, calls := bulletServer(t, func() (int, string) { return http.StatusOK, bulletBody(t) })

	var dialedURL string
	conn := &acking{Conn: coretest.NewConn(), t: t}
	a := newAdapterWith(t, kucoin.Options{
		WSEndpoint: srv.URL,
		Dial: func(_ context.Context, o transport.Options) (core.Conn, error) {
			dialedURL = o.URL
			// The welcome frame arrives before anything we sent.
			conn.Push([]byte(`{"id":"test-connect-id","type":"welcome"}`))
			return conn, nil
		},
	})

	plans, err := a.PlanSubscriptions([]core.StreamSpec{
		spec(t, "BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE),
		spec(t, "ETH_USDT", pb.Channel_CHANNEL_MARK_PRICE),
	})
	if err != nil {
		t.Fatal(err)
	}

	c, err := a.Dial(context.Background(), plans[0])
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer c.Close()

	if *calls != 1 {
		t.Errorf("bullet called %d times, want 1", *calls)
	}
	if !strings.HasPrefix(dialedURL, "wss://ws-api-futures.kucoin.com/?") {
		t.Errorf("dialed %q, want the endpoint the bullet answered with", dialedURL)
	}
	if !strings.Contains(dialedURL, "connectId=test-connect-id") {
		t.Errorf("dialed url carries no connect id: %q", dialedURL)
	}
	if !strings.Contains(dialedURL, "token=") {
		t.Errorf("dialed url carries no token: %q", dialedURL)
	}

	// One subscribe per topic, with response:true — without it a refused
	// subscription looks exactly like one that took and has nothing to say.
	writes := conn.Writes()
	var topics []string
	for _, w := range writes {
		var req struct {
			Type     string `json:"type"`
			Topic    string `json:"topic"`
			Response bool   `json:"response"`
		}
		if err := json.Unmarshal(w, &req); err != nil {
			t.Fatalf("write is not json: %s", w)
		}
		if req.Type != "subscribe" {
			continue
		}
		if !req.Response {
			t.Errorf("subscribe to %s did not ask for an acknowledgement", req.Topic)
		}
		topics = append(topics, req.Topic)
	}
	want := []string{"/contract/instrument:ETHUSDTM", "/contract/instrument:XBTUSDTM"}
	if len(topics) != len(want) {
		t.Fatalf("subscribed to %v, want %v", topics, want)
	}
	for i := range want {
		if topics[i] != want[i] {
			t.Errorf("topic %d = %q, want %q", i, topics[i], want[i])
		}
	}
}

// TestEveryDialFetchesAFreshToken is the acceptance criterion: KuCoin's tokens
// expire, and a reconnect that reused one gets a handshake rejection that reads
// like the venue being down.
func TestEveryDialFetchesAFreshToken(t *testing.T) {
	var issued []string
	srv, calls := bulletServer(t, func() (int, string) {
		token := "token-" + strings.Repeat("x", len(issued)+1)
		issued = append(issued, token)
		return http.StatusOK, `{"code":"200000","data":{"token":"` + token + `","instanceServers":[
		  {"endpoint":"wss://ws-api-futures.kucoin.com/","pingInterval":18000,"pingTimeout":10000}]}}`
	})

	var dialed []string
	a := newAdapterWith(t, kucoin.Options{
		WSEndpoint: srv.URL,
		Dial: func(_ context.Context, o transport.Options) (core.Conn, error) {
			dialed = append(dialed, o.URL)
			c := &acking{Conn: coretest.NewConn(), t: t}
			return c, nil
		},
	})
	plans, err := a.PlanSubscriptions([]core.StreamSpec{spec(t, "BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE)})
	if err != nil {
		t.Fatal(err)
	}

	for i := range 3 {
		c, err := a.Dial(context.Background(), plans[0])
		if err != nil {
			t.Fatalf("dial %d: %v", i, err)
		}
		c.Close()
	}

	if *calls != 3 {
		t.Errorf("bullet called %d times for 3 dials, want 3: a cached token is a stale token", *calls)
	}
	for i, u := range dialed {
		if !strings.Contains(u, issued[i]) {
			t.Errorf("dial %d used %q, want the token issued for it (%s)", i, u, issued[i])
		}
	}
}

// TestDialRefusedSubscriptionFails: Dial promises a connection whose
// subscriptions are live. A socket that opened and was then refused its topics
// would sit there consuming a connection slot and delivering nothing, which
// from above is indistinguishable from a venue that went quiet.
func TestDialRefusedSubscriptionFails(t *testing.T) {
	srv, _ := bulletServer(t, func() (int, string) { return http.StatusOK, bulletBody(t) })

	conn := coretest.NewConn()
	conn.Push([]byte(`{"id":"sub-0","type":"error","code":404,"data":"topic /contract/instrument:XBTUSDTM is not supported"}`))
	a := newAdapterWith(t, kucoin.Options{
		WSEndpoint: srv.URL,
		Dial:       func(context.Context, transport.Options) (core.Conn, error) { return conn, nil },
	})
	plans, err := a.PlanSubscriptions([]core.StreamSpec{spec(t, "BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE)})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := a.Dial(context.Background(), plans[0]); err == nil {
		t.Fatal("Dial succeeded against a refused subscription")
	}
	if !conn.IsClosed() {
		t.Error("the socket was left open after a failed subscription")
	}
}

// TestDialTimesOutWaitingForAcks: a socket that opens and never acknowledges is
// not a connection, and Dial must not hand one back.
func TestDialTimesOutWaitingForAcks(t *testing.T) {
	srv, _ := bulletServer(t, func() (int, string) { return http.StatusOK, bulletBody(t) })

	conn := coretest.NewConn()
	a := newAdapterWith(t, kucoin.Options{
		WSEndpoint:       srv.URL,
		SubscribeTimeout: 50 * time.Millisecond,
		Dial:             func(context.Context, transport.Options) (core.Conn, error) { return conn, nil },
	})
	plans, err := a.PlanSubscriptions([]core.StreamSpec{spec(t, "BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE)})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := a.Dial(context.Background(), plans[0]); err == nil {
		t.Fatal("Dial succeeded with no acknowledgement")
	}
	if !conn.IsClosed() {
		t.Error("the socket was left open after the handshake timed out")
	}
}

// TestBulletFailureIsADialFailure: no token, no connection. The supervisor
// backs off and the streams report DEGRADED, which is what is actually true.
func TestBulletFailureIsADialFailure(t *testing.T) {
	for name, body := range map[string]func() (int, string){
		"http error": func() (int, string) { return http.StatusTooManyRequests, `{"code":"429000","msg":"Too Many Requests"}` },
		"venue code": func() (int, string) { return http.StatusOK, `{"code":"400001","msg":"no token for you"}` },
		"no token": func() (int, string) {
			return http.StatusOK, `{"code":"200000","data":{"instanceServers":[{"endpoint":"wss://x/"}]}}`
		},
		"no server": func() (int, string) {
			return http.StatusOK, `{"code":"200000","data":{"token":"t","instanceServers":[]}}`
		},
		"no ping interval": func() (int, string) {
			return http.StatusOK, `{"code":"200000","data":{"token":"t","instanceServers":[{"endpoint":"wss://x/","pingTimeout":10000}]}}`
		},
		"not json": func() (int, string) { return http.StatusOK, `<html>maintenance</html>` },
	} {
		t.Run(name, func(t *testing.T) {
			srv, _ := bulletServer(t, body)
			a := newAdapterWith(t, kucoin.Options{
				WSEndpoint: srv.URL,
				Dial: func(context.Context, transport.Options) (core.Conn, error) {
					t.Error("a socket was dialed without a usable bullet")
					return nil, nil
				},
			})
			plans, err := a.PlanSubscriptions([]core.StreamSpec{spec(t, "BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE)})
			if err != nil {
				t.Fatal(err)
			}
			if _, err := a.Dial(context.Background(), plans[0]); err == nil {
				t.Error("Dial succeeded with no usable bullet")
			}
		})
	}
}

// TestLimiterDenialStopsTheDial: the bullet call costs budget on every
// reconnect, so a spent budget must stop the dial rather than spend it anyway.
func TestLimiterDenialStopsTheDial(t *testing.T) {
	srv, calls := bulletServer(t, func() (int, string) { return http.StatusOK, bulletBody(t) })

	a := newAdapterWith(t, kucoin.Options{
		WSEndpoint: srv.URL,
		Limiter:    denyingLimiter{},
		Dial: func(context.Context, transport.Options) (core.Conn, error) {
			t.Error("a socket was dialed with no connect budget")
			return nil, nil
		},
	})
	plans, err := a.PlanSubscriptions([]core.StreamSpec{spec(t, "BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE)})
	if err != nil {
		t.Fatal(err)
	}

	if _, err := a.Dial(context.Background(), plans[0]); !errors.Is(err, ratelimit.ErrBudgetExhausted) {
		t.Errorf("Dial = %v, want ErrBudgetExhausted", err)
	}
	if *calls != 0 {
		t.Errorf("the bullet endpoint was called %d times with no budget", *calls)
	}
}

// denyingLimiter refuses everything, which is what a spent budget looks like
// from inside an adapter.
type denyingLimiter struct{}

func (denyingLimiter) Allow(context.Context, string, ratelimit.LimitKind, int) error {
	return ratelimit.ErrBudgetExhausted
}
func (denyingLimiter) Used(string, ratelimit.LimitKind) (int, int) { return 0, 0 }
