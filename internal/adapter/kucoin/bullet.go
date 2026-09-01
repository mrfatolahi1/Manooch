package kucoin

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/google/uuid"
	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/ratelimit"
	"github.com/you/manooch/internal/transport"
)

// bulletPath is the public connection bootstrap.
//
// It takes no key, no signature and no body. It is public in the strict sense:
// the same POST a browser makes to open KuCoin's own chart page. The token it
// returns is a short-lived handle on an anonymous connection, not a credential,
// and it grants nothing but the public feed.
const bulletPath = "/api/v1/bullet-public"

// bulletWeight is what one bullet call costs against the venue's request
// budget. It is spent on every dial, which is the reason a reconnect storm is
// more expensive here than on a venue that lets you dial the socket directly.
const bulletWeight = 10

// instrumentTopic carries both subjects this adapter reads.
const instrumentTopic = "/contract/instrument"

// codeOK is what KuCoin puts in "code" on a successful REST response. It is a
// string, and a 200 with a non-OK code is still a failure.
const codeOK = "200000"

// defaultSubscribeTimeout bounds the wait for every subscription to be
// acknowledged. Dial returns only once they are, so a socket that opens and
// then never acknowledges must not sit there looking like a live connection.
const defaultSubscribeTimeout = 15 * time.Second

// maxBulletBodyBytes caps the bullet response. It is a few hundred bytes; a
// megabyte of it is a gateway error page.
const maxBulletBodyBytes = 1 << 20

// bulletResponse is /api/v1/bullet-public.
type bulletResponse struct {
	Code string      `json:"code"`
	Data *bulletData `json:"data"`
	Msg  string      `json:"msg"`
}

type bulletData struct {
	Token           string         `json:"token"`
	InstanceServers []bulletServer `json:"instanceServers"`
}

// bulletServer is one endpoint the venue is offering, with the ping contract
// it expects on that endpoint. The intervals come from the venue rather than
// from config because the venue is free to change them per connection.
type bulletServer struct {
	Endpoint     string `json:"endpoint"`
	Protocol     string `json:"protocol"`
	Encrypt      bool   `json:"encrypt"`
	PingInterval int64  `json:"pingInterval"` // milliseconds
	PingTimeout  int64  `json:"pingTimeout"`  // milliseconds
}

// A bullet is one usable connection offer: where to connect, with what token,
// and how often we have to ping once we are there.
type bullet struct {
	Token        string
	Endpoint     string
	PingInterval time.Duration
	PingTimeout  time.Duration
}

// URL is the socket address for this offer.
//
// connectID is ours and must be unique per connection: KuCoin uses it to tell
// two connections apart, and reusing one is how a reconnect gets the previous
// session closed underneath it.
func (b bullet) URL(connectID string) string {
	sep := "?"
	if strings.Contains(b.Endpoint, "?") {
		sep = "&"
	}
	return b.Endpoint + sep + url.Values{
		"token":     {b.Token},
		"connectId": {connectID},
	}.Encode()
}

// newConnectID is a fresh identifier per connection.
func newConnectID() string { return uuid.NewString() }

// fetchBullet performs the bootstrap POST.
//
// The token is never cached. KuCoin's tokens expire, and a reconnect that
// reuses one gets a handshake rejection that reads like the venue being down —
// so every dial pays for a fresh one, and that cost is the honest price of
// reconnecting to this venue.
func (a *Adapter) fetchBullet(ctx context.Context) (bullet, error) {
	if err := a.opts.Limiter.Allow(ctx, Venue, ratelimit.LimitRESTWeight, bulletWeight); err != nil {
		return bullet{}, fmt.Errorf("kucoin: bullet: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.opts.WSEndpoint+bulletPath, nil)
	if err != nil {
		return bullet{}, fmt.Errorf("kucoin: bullet: %w", err)
	}
	resp, err := a.opts.HTTPClient.Do(req)
	if err != nil {
		return bullet{}, fmt.Errorf("kucoin: bullet: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(io.LimitReader(resp.Body, maxBulletBodyBytes))
	if err != nil {
		return bullet{}, fmt.Errorf("kucoin: bullet: %w", err)
	}
	if resp.StatusCode != http.StatusOK {
		return bullet{}, core.NewParseError(core.KindVenue, pb.Channel_CHANNEL_UNSPECIFIED, "", nil,
			"bullet: %s: %s", resp.Status, truncate(string(body), 200))
	}
	return parseBullet(body)
}

// parseBullet reads the bootstrap response. It is separate from the HTTP call
// so a committed fixture can prove the mapping without a server.
func parseBullet(body []byte) (bullet, error) {
	var r bulletResponse
	if err := json.Unmarshal(body, &r); err != nil {
		return bullet{}, core.NewParseError(core.KindJSON, pb.Channel_CHANNEL_UNSPECIFIED, "", err, "bullet is not json")
	}
	if r.Code != codeOK {
		return bullet{}, core.NewParseError(core.KindVenue, pb.Channel_CHANNEL_UNSPECIFIED, "", nil,
			"bullet: code %s: %s", r.Code, r.Msg)
	}
	if r.Data == nil || r.Data.Token == "" || len(r.Data.InstanceServers) == 0 {
		return bullet{}, core.NewParseError(core.KindField, pb.Channel_CHANNEL_UNSPECIFIED, "", nil,
			"bullet: no token or no instance server")
	}

	// The first server the venue offers, not one we pick: the token is issued
	// against the set it came with, and choosing differently later is how a
	// working bootstrap turns into an unexplained handshake failure.
	s := r.Data.InstanceServers[0]
	if s.Endpoint == "" {
		return bullet{}, core.NewParseError(core.KindField, pb.Channel_CHANNEL_UNSPECIFIED, "", nil,
			"bullet: instance server has no endpoint")
	}
	if s.PingInterval <= 0 || s.PingTimeout <= 0 {
		// Without these the client cannot keep the connection alive, and a
		// default of ours would be a number the venue never agreed to.
		return bullet{}, core.NewParseError(core.KindField, pb.Channel_CHANNEL_UNSPECIFIED, "", nil,
			"bullet: pingInterval %d pingTimeout %d", s.PingInterval, s.PingTimeout)
	}
	return bullet{
		Token:        r.Data.Token,
		Endpoint:     s.Endpoint,
		PingInterval: time.Duration(s.PingInterval) * time.Millisecond,
		PingTimeout:  time.Duration(s.PingTimeout) * time.Millisecond,
	}, nil
}

// Dial opens one socket for one plan and completes the subscription handshake.
//
// Four steps, all of them inside this method because core.Adapter says Dial
// returns a connection that is already subscribed:
//
//  1. POST the public bullet endpoint for a token and a server address.
//  2. Connect to that address with the token and a fresh connect id.
//  3. Send one subscribe frame per topic and wait for every acknowledgement.
//  4. Start the client-side ping the venue requires, owned by the connection.
//
// Nothing above this method had to change to accommodate any of it: the
// supervisor asks for a connection and gets one, exactly as it does for a venue
// whose subscriptions are in the URL.
func (a *Adapter) Dial(ctx context.Context, plan core.SocketPlan) (core.Conn, error) {
	topics, err := a.topics(plan)
	if err != nil {
		return nil, err
	}
	if len(topics) == 0 {
		return nil, fmt.Errorf("kucoin: plan %s has no topics", plan.ID)
	}
	if err := a.opts.Limiter.Allow(ctx, Venue, ratelimit.LimitWSConnect, 1); err != nil {
		return nil, fmt.Errorf("kucoin: dial %s: %w", plan.ID, err)
	}

	b, err := a.fetchBullet(ctx)
	if err != nil {
		return nil, err
	}

	conn, err := a.opts.Dial(ctx, transport.Options{
		URL:           b.URL(a.opts.ConnectID()),
		ReadTimeout:   a.opts.ReadTimeout,
		MaxFrameBytes: a.opts.MaxFrameBytes,
		HTTPClient:    a.opts.HTTPClient,
	})
	if err != nil {
		return nil, err
	}

	if err := a.subscribe(ctx, conn, topics); err != nil {
		// Close what we opened. A socket left dangling after a failed
		// subscription still counts against the venue's connection limit and
		// still delivers nothing.
		_ = conn.Close()
		return nil, err
	}
	return newPingingConn(conn, b.PingInterval), nil
}

// truncate bounds an error body so one bad response cannot fill the log.
func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}
