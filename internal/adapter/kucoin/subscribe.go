package kucoin

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/ratelimit"
)

// Frame types KuCoin uses on the wire. They are a closed set: a type we do not
// recognise is a protocol change, not something to guess at.
const (
	typeWelcome   = "welcome"
	typeAck       = "ack"
	typePong      = "pong"
	typePing      = "ping"
	typeError     = "error"
	typeMessage   = "message"
	typeSubscribe = "subscribe"
)

// subscribeRequest is one subscription. response:true is what makes the venue
// acknowledge it; without it a subscription that was refused looks exactly like
// one that took and has nothing to say yet.
type subscribeRequest struct {
	ID       string `json:"id"`
	Type     string `json:"type"`
	Topic    string `json:"topic"`
	Response bool   `json:"response"`
}

// control is the envelope every non-data frame shares.
type control struct {
	ID   string `json:"id"`
	Type string `json:"type"`
	Code int64  `json:"code"`
	Data string `json:"data"`
}

// subscribe sends one frame per topic and waits for every acknowledgement.
//
// It returns only once all of them are in, because core.Adapter.Dial promises a
// connection whose subscriptions are live. A socket that opened but was refused
// its topics is indistinguishable, from above, from a venue that has gone
// quiet — and it would sit there consuming a connection slot forever.
func (a *Adapter) subscribe(ctx context.Context, conn core.Conn, topics []string) error {
	if err := a.opts.Limiter.Allow(ctx, Venue, ratelimit.LimitSubscriptions, len(topics)); err != nil {
		return fmt.Errorf("kucoin: subscribe: %w", err)
	}

	// Closing is the only thing that frees a goroutine parked in Read —
	// cancelling a context does not — so the handshake deadline is enforced by
	// closing the socket, the same way the supervisor enforces every other one.
	// A watchdog rather than a read deadline because the read below has to
	// survive several frames, not one.
	stop := a.closeAfter(ctx, conn, a.opts.SubscribeTimeout)
	defer stop()

	pending := make(map[string]string, len(topics))
	for i, topic := range topics {
		id := fmt.Sprintf("sub-%d", i)
		req, err := json.Marshal(subscribeRequest{ID: id, Type: typeSubscribe, Topic: topic, Response: true})
		if err != nil {
			return fmt.Errorf("kucoin: subscribe %s: %w", topic, err)
		}
		if err := conn.Write(ctx, req); err != nil {
			return fmt.Errorf("kucoin: subscribe %s: %w", topic, err)
		}
		pending[id] = topic
	}

	for len(pending) > 0 {
		frame, _, err := conn.Read(ctx)
		if err != nil {
			return fmt.Errorf("kucoin: waiting for %d subscription acks: %w", len(pending), err)
		}

		var c control
		if err := json.Unmarshal(frame, &c); err != nil {
			// A frame we cannot read during the handshake is a protocol
			// problem, not a data problem: the whole dial fails rather than
			// leaving a socket half subscribed.
			return core.NewParseError(core.KindJSON, pb.Channel_CHANNEL_UNSPECIFIED, "", err, "handshake frame is not json")
		}
		switch c.Type {
		case typeAck:
			delete(pending, c.ID)
		case typeError:
			return core.NewParseError(core.KindVenue, pb.Channel_CHANNEL_UNSPECIFIED, "", nil,
				"subscribe %s refused: code %d: %s", pending[c.ID], c.Code, c.Data)
		}
		// Everything else — the welcome frame, a data message that arrived
		// before its ack — is dropped. A mark price lost during a handshake is
		// replaced a second later; a dial that stalled waiting for one is not.
	}
	return nil
}

// closeAfter closes conn once d has passed or ctx has ended, and returns a
// function that calls off the watchdog.
//
// It exists because core.Conn.Read is documented not to watch the caller's
// context: a socket that opens and then says nothing would otherwise park the
// dial forever, holding a connection slot the venue counts and delivering
// nothing to anyone.
func (a *Adapter) closeAfter(ctx context.Context, conn core.Conn, d time.Duration) func() {
	done := make(chan struct{})
	go func() {
		timer := time.NewTimer(d)
		defer timer.Stop()
		select {
		case <-done:
		case <-timer.C:
			_ = conn.Close()
		case <-ctx.Done():
			_ = conn.Close()
		}
	}()
	return func() { close(done) }
}
