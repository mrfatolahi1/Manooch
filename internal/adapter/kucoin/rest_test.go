package kucoin_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/adapter/kucoin"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/ratelimit"
	"github.com/you/manooch/pkg/price"
)

// The two public endpoints the fallback reads. The mark-price call answers both
// prices; funding is a separate call, the same split the websocket has.
const (
	markPriceBody = `{"code":"200000","data":{"symbol":"XBTUSDTM","granularity":1000,` +
		`"timePoint":1731899129000,"value":90445.02,"indexPrice":90440.135}}`
	fundingRateBody = `{"code":"200000","data":{"symbol":".XBTUSDTMFPI8H","granularity":28800000,` +
		`"timePoint":1731898800000,"value":-0.002966,"predictedValue":-0.000001}}`
)

// restAdapter points the adapter at a test server for both bases.
func restAdapter(t *testing.T, handler http.HandlerFunc) *kucoin.Adapter {
	t.Helper()

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return newAdapterWith(t, kucoin.Options{WebSocketEndpoint: server.URL, RESTEndpoint: server.URL})
}

// TestFetchOnceReturnsOnlyTheRequestedChannel: the mark-price endpoint answers
// the index price too, but a caller polling because one key expired asked for
// that key, and overwriting another from a source it did not choose resets the
// one signal saying that other key is fine.
func TestFetchOnceReturnsOnlyTheRequestedChannel(t *testing.T) {
	var paths []string
	adapter := restAdapter(t, func(response http.ResponseWriter, request *http.Request) {
		paths = append(paths, request.URL.Path)
		if request.URL.Path == "/api/v1/funding-rate/XBTUSDTM/current" {
			_, _ = response.Write([]byte(fundingRateBody))
			return
		}
		_, _ = response.Write([]byte(markPriceBody))
	})

	for _, testCase := range []struct {
		channel manoochv1.Channel
		check   func(t *testing.T, message core.Message)
	}{
		{manoochv1.Channel_CHANNEL_MARK_PRICE, func(t *testing.T, message core.Message) {
			if got := price.Price(message.Proto.(*manoochv1.MarkPrice).MarkPrice).String(); got != "90445.02" {
				t.Errorf("mark price = %s", got)
			}
		}},
		{manoochv1.Channel_CHANNEL_INDEX_PRICE, func(t *testing.T, message core.Message) {
			if got := price.Price(message.Proto.(*manoochv1.IndexPrice).IndexPrice).String(); got != "90440.135" {
				t.Errorf("index price = %s", got)
			}
		}},
		{manoochv1.Channel_CHANNEL_FUNDING, func(t *testing.T, message core.Message) {
			funding := message.Proto.(*manoochv1.Funding)
			if got := price.Rate(funding.FundingRate).String(); got != "-0.002966" {
				t.Errorf("funding rate = %s", got)
			}
			// The endpoint answers how long is left, not when. Turning that
			// into an absolute time would publish our clock as the venue's.
			if funding.NextFundingTimeNs != 0 {
				t.Errorf("next_funding_time_ns = %d; KuCoin does not supply it", funding.NextFundingTimeNs)
			}
		}},
	} {
		t.Run(core.ChannelName(testCase.channel), func(t *testing.T) {
			messages, err := adapter.FetchOnce(context.Background(), specification(t, "BTC_USDT", testCase.channel))
			if err != nil {
				t.Fatalf("FetchOnce: %v", err)
			}
			if len(messages) != 1 {
				t.Fatalf("FetchOnce returned %d messages, want 1", len(messages))
			}
			message := messages[0]
			if message.Channel != testCase.channel {
				t.Errorf("channel = %v, want %v", message.Channel, testCase.channel)
			}

			envelope := message.Proto.(interface{ GetEnv() *manoochv1.Envelope }).GetEnv()
			if envelope.Source != manoochv1.Source_SOURCE_REST {
				t.Errorf("source = %v, want REST", envelope.Source)
			}
			if envelope.Status == manoochv1.Status_STATUS_UNSPECIFIED {
				t.Error("status is unspecified")
			}
			if envelope.RecvTimeNs <= 0 || envelope.ExchangeTimeNs <= 0 {
				t.Errorf("recv %d exchange %d", envelope.RecvTimeNs, envelope.ExchangeTimeNs)
			}
			if envelope.PublishTimeNs != 0 {
				t.Error("publish_time_ns set outside the publisher")
			}
			// The symbol comes from the specification, never from the response: the
			// funding endpoint answers with the index symbol, and mapping that
			// back would fail or, worse, succeed onto the wrong key.
			if envelope.Instrument.VenueSymbol != "XBTUSDTM" {
				t.Errorf("venue_symbol = %q, want XBTUSDTM", envelope.Instrument.VenueSymbol)
			}
			testCase.check(t, message)
		})
	}

	want := map[string]bool{
		"/api/v1/mark-price/XBTUSDTM/current":   true,
		"/api/v1/funding-rate/XBTUSDTM/current": true,
	}
	for _, p := range paths {
		if !want[p] {
			t.Errorf("requested %q", p)
		}
	}
}

// TestFetchOnceSurfacesVenueErrors: KuCoin answers a rejection with 200 and a
// code inside the body. Reading only the HTTP status would take the error for
// data.
func TestFetchOnceSurfacesVenueErrors(t *testing.T) {
	for name, handler := range map[string]http.HandlerFunc{
		"http error": func(response http.ResponseWriter, request *http.Request) {
			response.WriteHeader(http.StatusTooManyRequests)
			_, _ = response.Write([]byte(`{"code":"429000","msg":"Too Many Requests"}`))
		},
		"venue code in a 200": func(response http.ResponseWriter, request *http.Request) {
			_, _ = response.Write([]byte(`{"code":"100001","msg":"symbol not exists"}`))
		},
		"no data": func(response http.ResponseWriter, request *http.Request) {
			_, _ = response.Write([]byte(`{"code":"200000"}`))
		},
	} {
		t.Run(name, func(t *testing.T) {
			adapter := restAdapter(t, handler)
			_, err := adapter.FetchOnce(context.Background(), specification(t, "BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE))
			if err == nil {
				t.Fatal("FetchOnce succeeded")
			}
			var parseError *core.ParseError
			if !errors.As(err, &parseError) {
				t.Fatalf("error = %v (%T), want *core.ParseError", err, err)
			}
		})
	}
}

// TestFetchOnceRejectsAWrongTimestampUnit: the same assertion the websocket
// makes, on the same grounds. A venue that changes units under us must fail
// loudly rather than publish a time in the year 56000.
func TestFetchOnceRejectsAWrongTimestampUnit(t *testing.T) {
	adapter := restAdapter(t, func(response http.ResponseWriter, request *http.Request) {
		_, _ = response.Write([]byte(`{"code":"200000","data":{"symbol":"XBTUSDTM","granularity":1000,` +
			`"timePoint":1731899129000000000,"value":90445.02,"indexPrice":90440.135}}`))
	})
	if _, err := adapter.FetchOnce(context.Background(), specification(t, "BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE)); err == nil {
		t.Error("FetchOnce accepted a nanosecond timestamp")
	}
}

// TestFetchOnceLimiterDenial: the poll does not happen, and the caller is told,
// so the stream goes STALE rather than being silently skipped.
func TestFetchOnceLimiterDenial(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		calls++
		_, _ = response.Write([]byte(markPriceBody))
	}))
	t.Cleanup(server.Close)

	adapter := newAdapterWith(t, kucoin.Options{WebSocketEndpoint: server.URL, RESTEndpoint: server.URL, Limiter: denyingLimiter{}})
	_, err := adapter.FetchOnce(context.Background(), specification(t, "BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE))
	if !errors.Is(err, ratelimit.ErrBudgetExhausted) {
		t.Errorf("FetchOnce = %v, want ErrBudgetExhausted", err)
	}
	if calls != 0 {
		t.Errorf("the venue was called %d times with no budget", calls)
	}
}

// ---------- metadata ----------

// contractsBody is one linear perpetual, one inverse swap and one dated future.
// The endpoint lists all of KuCoin's contracts; only the first is ours.
const contractsBody = `{"code":"200000","data":[
  {"symbol":"XBTUSDTM","type":"FFWCSX","status":"Open","isInverse":false,
   "baseCurrency":"XBT","quoteCurrency":"USDT","settleCurrency":"USDT",
   "tickSize":0.1,"lotSize":1,"maxOrderQty":1000000,"multiplier":0.001},
  {"symbol":"ETHUSDTM","type":"FFWCSX","status":"Pause","isInverse":false,
   "tickSize":0.01,"lotSize":1,"maxOrderQty":1000000,"multiplier":0.01},
  {"symbol":"XBTUSDM","type":"FFWCSX","status":"Open","isInverse":true,
   "tickSize":0.1,"lotSize":1,"maxOrderQty":1000000,"multiplier":-1},
  {"symbol":"XBTMH24","type":"FFICSX","status":"Open","isInverse":true,
   "tickSize":1,"lotSize":1,"maxOrderQty":1000000,"multiplier":-1}]}`

// TestFetchMetadata: the public contract list, mapped onto the scales
// everything else in the service uses.
func TestFetchMetadata(t *testing.T) {
	var gotPath string
	adapter := restAdapter(t, func(response http.ResponseWriter, request *http.Request) {
		gotPath = request.URL.Path
		_, _ = response.Write([]byte(contractsBody))
	})

	metadataList, err := adapter.FetchMetadata(context.Background(), kucoin.MarketType)
	if err != nil {
		t.Fatalf("FetchMetadata: %v", err)
	}
	if gotPath != "/api/v1/contracts/active" {
		t.Errorf("requested %q", gotPath)
	}
	// The inverse swap and the dated future are skipped, not errors: the
	// endpoint lists every contract KuCoin has.
	if len(metadataList) != 2 {
		t.Fatalf("metas = %d, want 2 (the two linear perpetuals)", len(metadataList))
	}

	btc := metadataList[0]
	if got := btc.Env.Instrument.Canonical; got != "BTC_USDT" {
		t.Errorf("canonical = %q", got)
	}
	if got := btc.Env.Instrument.VenueSymbol; got != "XBTUSDTM" {
		t.Errorf("venue_symbol = %q", got)
	}
	if got := price.Price(btc.TickSize).String(); got != "0.1" {
		t.Errorf("tick_size = %s, want 0.1", got)
	}
	if got := price.Size(btc.LotSize).String(); got != "1" {
		t.Errorf("lot_size = %s, want 1 contract", got)
	}
	if got := price.Size(btc.MaxSize).String(); got != "1000000" {
		t.Errorf("max_size = %s, want 1000000", got)
	}
	// The contract multiplier is the whole reason this channel exists for
	// KuCoin: a futures contract is 0.001 BTC, not one, and a consumer
	// multiplying by a missing multiplier sizes every order a thousand times
	// too large.
	if got := price.Size(btc.ContractMultiplier).String(); got != "0.001" {
		t.Errorf("contract_multiplier = %s, want 0.001", got)
	}
	if !btc.Active {
		t.Error("an Open contract is not active")
	}
	if btc.LastRefreshNs <= 0 {
		t.Error("last_refresh_ns not stamped")
	}
	if btc.Env.Source != manoochv1.Source_SOURCE_REST || btc.Env.Status == manoochv1.Status_STATUS_UNSPECIFIED {
		t.Errorf("source = %v status = %v", btc.Env.Source, btc.Env.Status)
	}
	// This endpoint carries no server time, and stamping ours into a field
	// named for the venue's would be a value we derived presented as theirs.
	if btc.Env.ExchangeTimeNs != 0 || btc.Env.ExchangeTimeIsSendTime {
		t.Errorf("exchange_time_ns = %d send_time = %v; KuCoin's contract list carries no server time",
			btc.Env.ExchangeTimeNs, btc.Env.ExchangeTimeIsSendTime)
	}
	// KuCoin publishes no minimum notional for futures. Zero says "not
	// supplied"; a number computed here would look like one they gave us.
	if btc.MinNotional != 0 {
		t.Errorf("min_notional = %d; KuCoin does not publish one", btc.MinNotional)
	}

	// A paused contract is published and marked inactive rather than dropped:
	// silence would read as a symbol the venue never had.
	if metadataList[1].Active {
		t.Error("a Paused contract is active")
	}
}

// TestFetchMetadataRejectsUnusableNumbers: a contract we do serve, with no
// precision or no multiplier, is an error. A multiplier a consumer cannot size
// an order with is not metadata.
func TestFetchMetadataRejectsUnusableNumbers(t *testing.T) {
	for name, body := range map[string]string{
		"no tick size":  `{"code":"200000","data":[{"symbol":"XBTUSDTM","type":"FFWCSX","status":"Open","lotSize":1,"multiplier":0.001}]}`,
		"no multiplier": `{"code":"200000","data":[{"symbol":"XBTUSDTM","type":"FFWCSX","status":"Open","tickSize":0.1,"lotSize":1}]}`,
		"no lot size":   `{"code":"200000","data":[{"symbol":"XBTUSDTM","type":"FFWCSX","status":"Open","tickSize":0.1,"multiplier":0.001}]}`,
	} {
		t.Run(name, func(t *testing.T) {
			adapter := restAdapter(t, func(response http.ResponseWriter, request *http.Request) {
				_, _ = response.Write([]byte(body))
			})
			_, err := adapter.FetchMetadata(context.Background(), kucoin.MarketType)
			var parseError *core.ParseError
			if !errors.As(err, &parseError) {
				t.Fatalf("FetchMetadata = %v (%T), want *core.ParseError", err, err)
			}
			if parseError.Kind != core.KindField {
				t.Errorf("kind = %q, want %q", parseError.Kind, core.KindField)
			}
		})
	}
}

// TestFetchMetadataRejectsAnEmptyList: a response listing no linear perpetuals
// is not an empty venue, it is a response we did not understand.
func TestFetchMetadataRejectsAnEmptyList(t *testing.T) {
	adapter := restAdapter(t, func(response http.ResponseWriter, request *http.Request) {
		_, _ = response.Write([]byte(`{"code":"200000","data":[]}`))
	})
	if _, err := adapter.FetchMetadata(context.Background(), kucoin.MarketType); err == nil {
		t.Error("FetchMetadata accepted a list with no linear perpetuals")
	}
}

// TestFetchMetadataUnservedMarketType: only one market is served, and answering
// for another would put an inverse contract's multiplier under a linear key.
func TestFetchMetadataUnservedMarketType(t *testing.T) {
	adapter := restAdapter(t, func(response http.ResponseWriter, request *http.Request) {
		t.Error("the venue was called for a market type this adapter does not serve")
	})
	if _, err := adapter.FetchMetadata(context.Background(), manoochv1.MarketType_MARKET_TYPE_SPOT); err == nil {
		t.Error("FetchMetadata accepted SPOT")
	}
}
