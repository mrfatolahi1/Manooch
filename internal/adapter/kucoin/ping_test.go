package kucoin_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"testing"
	"time"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/adapter/kucoin"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/core/coretest"
	"github.com/you/manooch/internal/transport"
)

// fastPingBullet is the real bootstrap response with an interval short enough
// to observe. The interval comes from the venue, so this is the venue asking
// for a ping every 20ms rather than a number the test invented.
const fastPingBullet = `{"code":"200000","data":{"token":"t","instanceServers":[
  {"endpoint":"wss://ws-api-futures.kucoin.com/","pingInterval":20,"pingTimeout":40}]}}`

// dialPinging returns a connection from Dial along with the socket underneath
// it, so a test can read what the ping goroutine wrote.
func dialPinging(t *testing.T, bullet string) (core.Conn, *coretest.Conn) {
	t.Helper()

	server, _ := bulletServer(t, func() (int, string) { return http.StatusOK, bullet })
	ackingConn := &acking{Conn: coretest.NewConn(), t: t}
	adapter := newAdapterWith(t, kucoin.Options{
		WebSocketEndpoint: server.URL,
		Dial:              func(context.Context, transport.Options) (core.Conn, error) { return ackingConn, nil },
	})
	plans, err := adapter.PlanSubscriptions([]core.StreamSpec{specification(t, "BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE)})
	if err != nil {
		t.Fatal(err)
	}
	connection, err := adapter.Dial(context.Background(), plans[0])
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	return connection, ackingConn.Conn
}

// pings counts the application-level ping frames written to a socket and
// reports whether their ids are unique. KuCoin wants an id per frame, and a
// repeated one is a frame it is entitled to ignore.
func pings(t *testing.T, connection *coretest.Conn) (int, bool) {
	t.Helper()

	seen := map[string]bool{}
	n, unique := 0, true
	for _, w := range connection.Writes() {
		var request struct {
			ID   string `json:"id"`
			Type string `json:"type"`
		}
		if err := json.Unmarshal(w, &request); err != nil {
			t.Fatalf("write is not json: %s", w)
		}
		if request.Type != "ping" {
			continue
		}
		n++
		if request.ID == "" || seen[request.ID] {
			unique = false
		}
		seen[request.ID] = true
	}
	return n, unique
}

// TestClientPings is the quirk: KuCoin requires the client to ping, unlike
// Binance where the server does. Missing the interval gets the connection
// dropped, so the connection owns a ticker of its own — internal/transport only
// answers protocol-level pings, which is a different thing entirely.
func TestClientPings(t *testing.T) {
	connection, socket := dialPinging(t, fastPingBullet)
	defer connection.Close()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if n, _ := pings(t, socket); n >= 3 {
			break
		}
		time.Sleep(2 * time.Millisecond)
	}

	n, unique := pings(t, socket)
	if n < 3 {
		t.Errorf("%d pings in two seconds at a 20ms interval; the venue would have dropped us", n)
	}
	if !unique {
		t.Error("ping ids repeat; the venue is entitled to ignore a repeated id")
	}
}

// TestClosingStopsThePing: the ticker belongs to the connection, so it has to
// die with it. A ping goroutine outliving its socket is a leak per reconnect.
func TestClosingStopsThePing(t *testing.T) {
	connection, socket := dialPinging(t, fastPingBullet)

	time.Sleep(60 * time.Millisecond)
	if err := connection.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if !socket.IsClosed() {
		t.Error("closing the wrapper did not close the socket under it")
	}

	// Wait for the goroutine rather than sleeping past it. A ping already being
	// written when Close lands still completes, so counting immediately would
	// race; what has to be true is that the ticker is gone, not that no write
	// was in flight.
	w, ok := connection.(interface{ Wait() })
	if !ok {
		t.Fatal("the connection Dial returned does not own a ping goroutine")
	}
	w.Wait()

	before, _ := pings(t, socket)
	time.Sleep(150 * time.Millisecond)
	after, _ := pings(t, socket)
	if after != before {
		t.Errorf("pings kept going after Close: %d then %d", before, after)
	}

	// Close is called from the supervisor's goroutine, more than once, while
	// another is parked in Read. It must tolerate all of that.
	if err := connection.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestFailedPingClosesTheConnection: a ping that cannot be written is a socket
// that is already gone. Closing is what unblocks the read loop above and starts
// the reconnect; retrying quietly would leave a connection that is up, silent,
// and about to be dropped for missing pings.
func TestFailedPingClosesTheConnection(t *testing.T) {
	connection, socket := dialPinging(t, fastPingBullet)
	defer connection.Close()

	socket.FailWrites(errors.New("broken pipe"))

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) && !socket.IsClosed() {
		time.Sleep(2 * time.Millisecond)
	}
	if !socket.IsClosed() {
		t.Error("a failed ping left the connection open")
	}
}
