package kucoin

import (
	"context"
	"encoding/json"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/you/manooch/internal/core"
)

// pingWriteTimeout bounds one ping write. A write that cannot complete inside
// the interval is a connection that is already gone, and blocking on it would
// hold the ticker past the next ping too.
const pingWriteTimeout = 5 * time.Second

// pingRequest is the application-level keepalive KuCoin requires from the
// client. It is not a websocket protocol ping: internal/transport already
// answers those, and KuCoin drops a connection that only does that.
type pingRequest struct {
	ID   string `json:"id"`
	Type string `json:"type"`
}

// A pingingConn is a core.Conn that sends the venue's required client ping for
// as long as it is open.
//
// The goroutine belongs to the connection, not to the adapter: it starts when
// the socket is handed over and stops when the socket is closed, so the adapter
// still holds no lifecycle state of its own and can be replaced or re-planned
// without anything to unwind. Missing the interval gets us disconnected, so
// this is the connection's own obligation rather than a policy for the
// supervisor to schedule.
type pingingConn struct {
	core.Conn

	interval time.Duration

	once   sync.Once
	closed chan struct{}
	done   chan struct{}

	// seq numbers the pings. KuCoin wants an id per frame and echoes it back
	// on the pong; a counter is enough and, unlike a timestamp, cannot repeat.
	seq atomic.Int64
}

var _ core.Conn = (*pingingConn)(nil)

// newPingingConn wraps a connection and starts pinging it.
func newPingingConn(conn core.Conn, interval time.Duration) *pingingConn {
	c := &pingingConn{
		Conn:     conn,
		interval: interval,
		closed:   make(chan struct{}),
		done:     make(chan struct{}),
	}
	go c.run()
	return c
}

// run pings until the connection is closed.
//
// A failed write closes the connection rather than being retried. The socket is
// already gone at that point, and closing is what unblocks the read loop above
// and starts the reconnect — where silently retrying would leave a connection
// that is up, silent, and about to be dropped by the venue for missing pings.
func (c *pingingConn) run() {
	defer close(c.done)

	if c.interval <= 0 {
		return
	}
	tick := time.NewTicker(c.interval)
	defer tick.Stop()

	for {
		select {
		case <-c.closed:
			return
		case <-tick.C:
			if err := c.ping(); err != nil {
				_ = c.Close()
				return
			}
		}
	}
}

// ping writes one keepalive frame.
func (c *pingingConn) ping() error {
	b, err := json.Marshal(pingRequest{
		ID:   "ping-" + strconv.FormatInt(c.seq.Add(1), 10),
		Type: typePing,
	})
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), pingWriteTimeout)
	defer cancel()
	return c.Conn.Write(ctx, b)
}

// Close stops the ping and closes the underlying connection. It is safe from
// any goroutine and more than once, which the supervisor relies on: closing is
// the only thing that frees a goroutine parked in Read.
func (c *pingingConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return c.Conn.Close()
}

// Wait blocks until the ping goroutine has stopped. It exists for tests: a
// leaked ticker is exactly the kind of thing that only shows up in production.
func (c *pingingConn) Wait() { <-c.done }
