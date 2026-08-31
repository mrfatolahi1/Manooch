package transport_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/transport"
)

// serve runs a websocket server that hands each connection to fn, and returns
// its ws:// address.
func serve(t *testing.T, fn func(ctx context.Context, c *websocket.Conn)) string {
	t.Helper()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer c.CloseNow()
		fn(r.Context(), c)
	}))
	t.Cleanup(srv.Close)
	return "ws://" + strings.TrimPrefix(srv.URL, "http://")
}

func dial(t *testing.T, url string, opts transport.Options) core.Conn {
	t.Helper()
	opts.URL = url

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	c, err := transport.Dial(ctx, opts)
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

// TestReadStampsArrival: recv_time_ns is the basis of every freshness and
// clock-skew number downstream, so it must bracket the read and nothing else.
func TestReadStampsArrival(t *testing.T) {
	url := serve(t, func(ctx context.Context, c *websocket.Conn) {
		time.Sleep(20 * time.Millisecond)
		_ = c.Write(ctx, websocket.MessageText, []byte(`{"hello":"world"}`))
		<-ctx.Done()
	})
	c := dial(t, url, transport.Options{ReadTimeout: 2 * time.Second})

	before := time.Now().UnixNano()
	frame, recvNs, err := c.Read(context.Background())
	after := time.Now().UnixNano()
	if err != nil {
		t.Fatalf("Read: %v", err)
	}

	if got := string(frame); got != `{"hello":"world"}` {
		t.Errorf("frame = %q", got)
	}
	if recvNs < before || recvNs > after {
		t.Errorf("recv_time_ns %d outside the read window [%d, %d]", recvNs, before, after)
	}
}

// TestIdleSocketErrors: a connected but silent socket is a disconnect. Without
// the deadline, Read blocks forever on a half-open connection and the stream
// looks alive to everything above it.
func TestIdleSocketErrors(t *testing.T) {
	url := serve(t, func(ctx context.Context, c *websocket.Conn) { <-ctx.Done() })
	c := dial(t, url, transport.Options{ReadTimeout: 100 * time.Millisecond})

	start := time.Now()
	_, _, err := c.Read(context.Background())
	if !errors.Is(err, transport.ErrIdle) {
		t.Fatalf("Read of a silent socket = %v, want ErrIdle", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("Read blocked %v past the 100ms deadline", elapsed)
	}
}

// TestCallerCancellationIsNotIdle: a caller cancelling its own context is a
// shutdown, not a dead venue, and must not be reported as one.
func TestCallerCancellationIsNotIdle(t *testing.T) {
	url := serve(t, func(ctx context.Context, c *websocket.Conn) { <-ctx.Done() })
	c := dial(t, url, transport.Options{ReadTimeout: time.Minute})

	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	_, _, err := c.Read(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Read after cancel = %v, want context.Canceled", err)
	}
	if errors.Is(err, transport.ErrIdle) {
		t.Error("caller cancellation reported as an idle socket")
	}
}

// TestCloseUnblocksRead is what makes stream cancellation work: cancelling a
// context does not unblock a read already in flight, and only Close does.
func TestCloseUnblocksRead(t *testing.T) {
	url := serve(t, func(ctx context.Context, c *websocket.Conn) { <-ctx.Done() })
	c := dial(t, url, transport.Options{ReadTimeout: time.Minute})

	var wg sync.WaitGroup
	errc := make(chan error, 1)
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _, err := c.Read(context.Background())
		errc <- err
	}()

	// Long enough that Read is parked in the socket, not still starting up.
	time.Sleep(50 * time.Millisecond)
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-errc:
		if err == nil {
			t.Error("Read returned no error after Close")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not unblock Read")
	}
	wg.Wait()

	// Close runs once however many callers reach it.
	if err := c.Close(); err != nil {
		t.Errorf("second Close: %v", err)
	}
}

// TestOversizedFrameErrors: an oversized frame must fail the connection rather
// than arrive truncated, because half a message parses into a plausible wrong
// one that nothing downstream can tell from a real one.
func TestOversizedFrameErrors(t *testing.T) {
	url := serve(t, func(ctx context.Context, c *websocket.Conn) {
		_ = c.Write(ctx, websocket.MessageText, []byte(strings.Repeat("x", 4096)))
		<-ctx.Done()
	})
	c := dial(t, url, transport.Options{ReadTimeout: 2 * time.Second, MaxFrameBytes: 1024})

	frame, _, err := c.Read(context.Background())
	if !errors.Is(err, transport.ErrFrameTooBig) {
		t.Fatalf("Read of an oversized frame = %v, want ErrFrameTooBig", err)
	}
	if frame != nil {
		t.Errorf("truncated frame returned: %d bytes", len(frame))
	}
}

// TestServerPingsAreAnswered checks the claim the Binance adapter relies on:
// the library replies to a server ping on its own, so no adapter has to.
func TestServerPingsAreAnswered(t *testing.T) {
	pongs := make(chan struct{}, 1)
	url := serve(t, func(ctx context.Context, c *websocket.Conn) {
		ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		// A pong is only processed while something is reading, on both ends.
		ctx = c.CloseRead(ctx)
		if err := c.Ping(ctx); err == nil { // blocks until the pong arrives
			select {
			case pongs <- struct{}{}:
			default:
			}
		}
		<-ctx.Done()
	})
	c := dial(t, url, transport.Options{ReadTimeout: 5 * time.Second})

	// Control frames are only handled while a read is in flight.
	readCtx, cancelRead := context.WithCancel(context.Background())
	defer cancelRead()
	go func() { _, _, _ = c.Read(readCtx) }()

	select {
	case <-pongs:
	case <-time.After(5 * time.Second):
		t.Fatal("server ping went unanswered")
	}

	tc, ok := c.(*transport.Conn)
	if !ok {
		t.Fatalf("Dial returned %T", c)
	}
	if got := tc.ServerPings(); got < 1 {
		t.Errorf("ServerPings() = %d, want at least 1", got)
	}
}

func TestDialRejectsEmptyURL(t *testing.T) {
	if _, err := transport.Dial(context.Background(), transport.Options{}); err == nil {
		t.Fatal("Dial with no url succeeded")
	}
}

// TestDialErrorNamesTheStatus: a venue that refuses the handshake answers with
// a status, and an error without it is an unexplained "bad handshake".
func TestDialErrorNamesTheStatus(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Error(w, "nope", http.StatusTooManyRequests)
	}))
	defer srv.Close()

	_, err := transport.Dial(context.Background(), transport.Options{
		URL: "ws://" + strings.TrimPrefix(srv.URL, "http://"),
	})
	if err == nil {
		t.Fatal("Dial against a rejecting server succeeded")
	}
	if !strings.Contains(err.Error(), "429") {
		t.Errorf("error does not name the status: %v", err)
	}
}
