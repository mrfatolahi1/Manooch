// Package transport owns the websocket connections an adapter reads from. It
// knows nothing about venues, payloads or Redis: it opens a socket, hands
// frames up with the instant they arrived, and fails loudly when one does not.
package transport

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/coder/websocket"
	"github.com/you/manooch/internal/core"
)

// DefaultMaxFrameBytes is the read limit when Options leaves one unset. The
// library's own default is 32 KiB, small enough that a venue snapshot would
// fail the connection.
const DefaultMaxFrameBytes = 1 << 20

// Connection failures worth telling apart. Everything above reacts to a dead
// socket the same way in M1, but the reason belongs in the log line.
var (
	// ErrIdle is a socket that is connected and silent past its read timeout.
	// It is a disconnect: TCP will hold a half-open connection open forever,
	// and a stream that never errors looks alive to everything above it.
	ErrIdle = errors.New("transport: no frame within the read timeout")

	// ErrFrameTooBig is a frame past the read limit. It is an error rather
	// than a truncation: half a message parses into a plausible wrong one.
	ErrFrameTooBig = errors.New("transport: frame exceeds the read limit")
)

// Options configures one connection.
type Options struct {
	// URL is the full wss:// address, including any subscription query the
	// venue puts in the path.
	URL string

	// ReadTimeout bounds the wait for one frame. Zero disables the bound,
	// which is only ever right in a test.
	ReadTimeout time.Duration

	// MaxFrameBytes caps one message. Zero means DefaultMaxFrameBytes.
	MaxFrameBytes int64

	// HTTPClient performs the opening handshake. Zero means http.DefaultClient.
	HTTPClient *http.Client

	// HTTPHeader is sent with the handshake request.
	HTTPHeader http.Header
}

// A Dialer opens a connection. Adapters take one so a test can hand them a
// socket that replays fixtures instead of reaching the internet.
type Dialer func(ctx context.Context, options Options) (core.Conn, error)

// A Conn is one open websocket, satisfying core.Conn.
type Conn struct {
	webSocket   *websocket.Conn
	url         string
	readTimeout time.Duration

	closeOnce sync.Once
	closeErr  error

	// serverPings counts pings the venue sent us. The library answers them
	// itself; this is how that claim is checked rather than assumed.
	serverPings atomic.Int64
}

var _ core.Conn = (*Conn)(nil)

// Dial opens a websocket and returns it ready to read.
//
// The handshake is bounded by ctx and by Options.HTTPClient.Timeout, which the
// library turns into a handshake deadline before cloning the client without it,
// so the established connection is unaffected. No timeout is invented here: the
// adapter knows what a reasonable connect time is for its venue.
func Dial(ctx context.Context, options Options) (core.Conn, error) {
	if options.URL == "" {
		return nil, errors.New("transport: no url")
	}
	limit := options.MaxFrameBytes
	if limit <= 0 {
		limit = DefaultMaxFrameBytes
	}

	connection := &Conn{url: options.URL, readTimeout: options.ReadTimeout}

	webSocket, response, err := websocket.Dial(ctx, options.URL, &websocket.DialOptions{
		HTTPClient: options.HTTPClient,
		HTTPHeader: options.HTTPHeader,
		// The library replies to a ping before this returns; the counter is
		// only so a test can prove that happened.
		OnPingReceived: func(context.Context, []byte) bool {
			connection.serverPings.Add(1)
			return true
		},
	})
	if err != nil {
		// A rejected handshake answers with a status and a body explaining
		// why; without it the error is an unexplained "bad handshake".
		if response != nil {
			return nil, fmt.Errorf("transport: dial %s: %s: %w", options.URL, response.Status, err)
		}
		return nil, fmt.Errorf("transport: dial %s: %w", options.URL, err)
	}
	webSocket.SetReadLimit(limit)
	connection.webSocket = webSocket
	return connection, nil
}

// Read blocks for the next frame and returns it with the instant it arrived.
//
// It is not safe to call from two goroutines at once; Close and Write are.
func (connection *Conn) Read(ctx context.Context) ([]byte, int64, error) {
	readCtx := ctx
	if connection.readTimeout > 0 {
		var cancel context.CancelFunc
		readCtx, cancel = context.WithTimeout(ctx, connection.readTimeout)
		defer cancel()
	}

	_, b, err := connection.webSocket.Read(readCtx)

	// Stamped here, immediately after the frame lands and before anything
	// looks at it. Every freshness number and the clock-skew gauge are
	// measured from this line; taken after parsing they would include our own
	// work and report the venue as slower than it is.
	receivedNs := time.Now().UnixNano()

	if err != nil {
		return nil, receivedNs, connection.readError(ctx, err)
	}
	return b, receivedNs, nil
}

// readError names the failure. A read timeout arrives as a context deadline on
// the derived context, which is indistinguishable from a caller-side
// cancellation unless the parent is checked.
func (connection *Conn) readError(parent context.Context, err error) error {
	switch {
	case parent.Err() != nil:
		return parent.Err()
	case errors.Is(err, context.DeadlineExceeded):
		return fmt.Errorf("%w (%v)", ErrIdle, connection.readTimeout)
	case errors.Is(err, websocket.ErrMessageTooBig):
		return ErrFrameTooBig
	default:
		return fmt.Errorf("transport: read %s: %w", connection.url, err)
	}
}

// Write sends one text frame, for venues that require client-initiated
// application-level pings or a subscription message after connecting.
func (connection *Conn) Write(ctx context.Context, b []byte) error {
	if err := connection.webSocket.Write(ctx, websocket.MessageText, b); err != nil {
		return fmt.Errorf("transport: write %s: %w", connection.url, err)
	}
	return nil
}

// Close drops the connection and unblocks a Read in another goroutine.
//
// It closes without the closing handshake on purpose: that handshake waits for
// the peer's close frame, which only the blocked Read could deliver, so a
// graceful close would deadlock against the very goroutine it has to free.
// Cancelling a context does not unblock Read either — this is the only thing
// that does.
func (connection *Conn) Close() error {
	connection.closeOnce.Do(func() { connection.closeErr = connection.webSocket.CloseNow() })
	return connection.closeErr
}

// ServerPings is how many ping frames the venue has sent. The library answered
// each one; a venue that disconnects us for missing pongs while this climbs is
// asking for something else.
func (connection *Conn) ServerPings() int64 { return connection.serverPings.Load() }

// URL is the address this connection was opened to, for logs.
func (connection *Conn) URL() string { return connection.url }
