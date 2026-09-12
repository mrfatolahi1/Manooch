package tabdeal

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
)

func testAdapter(t *testing.T, rest string) *Adapter {
	t.Helper()
	a, err := New(Options{WebSocketEndpoint: "ws://example.test", RESTEndpoint: rest, MaxStreamsPerSocket: 10, TimeToLive: map[manoochv1.Channel]time.Duration{manoochv1.Channel_CHANNEL_ORDERBOOK: time.Second}})
	if err != nil {
		t.Fatal(err)
	}
	return a
}

func TestParseDepth(t *testing.T) {
	a := testAdapter(t, "")
	msgs, err := a.Parse([]byte(`{"stream":"btcusdt_usdt@depth@2000ms","data":{"e":"depthUpdate","E":1700000000123,"s":"BTC_USDT","b":[["100.1","2.5"]],"a":[["100.2","1.25"]]}}`), 1700000000999000000)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages=%d", len(msgs))
	}
	book := msgs[0].Proto.(*manoochv1.OrderBook)
	if book.Depth != 1 || len(book.Bids) != 1 || book.Bids[0].Price != 10010000000000 || book.Bids[0].Size != 250000000 {
		t.Fatalf("unexpected book: %v", book)
	}
	if book.Env.ExchangeTimeNs != 1700000000123*int64(time.Millisecond) || book.Env.Source != manoochv1.Source_SOURCE_WEBSOCKET {
		t.Fatalf("unexpected envelope: %v", book.Env)
	}
}

func TestParseEncodedDepthPayload(t *testing.T) {
	a := testAdapter(t, "")
	frame := `{"stream":"special_margin-BTC_USDT-depth-1000ms","data":"{\"e\":\"depthUpdate\",\"E\":1700000000123,\"s\":\"BTCUSDT\",\"b\":[[\"100.1\",\"2.5\"]],\"a\":[[\"100.2\",\"1.25\"]]}"}`
	msgs, err := a.Parse([]byte(frame), 1700000000999000000)
	if err != nil || len(msgs) != 1 {
		t.Fatalf("parse encoded payload: messages=%d err=%v", len(msgs), err)
	}
}

func TestFetchOnceDepth(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/fapi/v1/depth" || r.URL.Query().Get("symbol") != "BTC_USDT" {
			t.Errorf("request %s", r.URL.String())
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"lastUpdateId":42,"bids":[["1.1","3"]],"asks":[["1.2","4"]]}`))
	}))
	defer server.Close()
	a := testAdapter(t, server.URL)
	ref, _ := core.ParseCanonical("BTC_USDT", MarketType)
	msgs, err := a.FetchOnce(context.Background(), core.StreamSpec{Instrument: ref, Channel: manoochv1.Channel_CHANNEL_ORDERBOOK})
	if err != nil {
		t.Fatal(err)
	}
	book := msgs[0].Proto.(*manoochv1.OrderBook)
	if book.Env.Source != manoochv1.Source_SOURCE_REST || book.Env.VenueSeq != 42 || !book.Env.VenueSeqPresent {
		t.Fatalf("unexpected envelope: %v", book.Env)
	}
}
