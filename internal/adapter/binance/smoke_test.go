//go:build smoke

// Smoke tests reach the real venue over the public internet. They are behind a
// build tag and out of normal CI: a red build must mean our code broke, not
// that Binance was slow or a runner had no egress.
//
//	go test -tags=smoke -count=1 -v ./internal/adapter/binance
package binance_test

import (
	"context"
	"testing"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/adapter/binance"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/pkg/price"
)

// maxClockSkew is what a synced host should manage against a venue on the far
// side of the internet. Past it, either our clock is wrong or the venue's is,
// and every freshness number the service publishes is wrong with it.
const maxClockSkew = 5000 * time.Millisecond

// firstFrameDeadline bounds the wait for real data. The stream is documented
// to update once a second; fifteen seconds is a dead connection, not a slow one.
const firstFrameDeadline = 15 * time.Second

// TestLiveMarkPriceStream connects to Binance, subscribes to one symbol, and
// asserts a well-formed message arrives with a plausible clock skew.
func TestLiveMarkPriceStream(t *testing.T) {
	a := newAdapter(t)

	plans, err := a.PlanSubscriptions([]core.StreamSpec{
		spec(t, "BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE),
	})
	if err != nil {
		t.Fatalf("PlanSubscriptions: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %d, want 1", len(plans))
	}

	ctx, cancel := context.WithTimeout(context.Background(), firstFrameDeadline)
	defer cancel()

	conn, err := a.Dial(ctx, plans[0])
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	for {
		frame, recvNs, err := conn.Read(ctx)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		msgs, err := a.Parse(frame, recvNs)
		if err != nil {
			t.Fatalf("Parse %s: %v", frame, err)
		}
		if len(msgs) == 0 {
			continue // an ack or a control frame
		}

		if len(msgs) != 3 {
			t.Errorf("one frame produced %d messages, want 3", len(msgs))
		}
		for _, m := range msgs {
			env := m.Proto.(interface{ GetEnv() *pb.Envelope }).GetEnv()

			skew := time.Duration(env.ExchangeTimeNs-env.RecvTimeNs) * time.Nanosecond
			if skew < -maxClockSkew || skew > maxClockSkew {
				t.Errorf("%s: clock skew %v exceeds %v", m.Key, skew, maxClockSkew)
			}
			if env.Status != pb.Status_STATUS_HEALTHY {
				t.Errorf("%s: status = %v", m.Key, env.Status)
			}
			if env.VenueSeqPresent {
				t.Errorf("%s: claims a venue sequence; this stream carries none", m.Key)
			}
			if m.TTL <= 0 {
				t.Errorf("%s: ttl = %v", m.Key, m.TTL)
			}
		}

		mark, ok := msgs[0].Proto.(*pb.MarkPrice)
		if !ok {
			t.Fatalf("first message is %T, want *pb.MarkPrice", msgs[0].Proto)
		}
		if mark.MarkPrice <= 0 {
			t.Fatalf("mark price = %d", mark.MarkPrice)
		}
		t.Logf("BTC_USDT mark %s, index %s, skew %v",
			price.Price(mark.MarkPrice),
			price.Price(msgs[1].Proto.(*pb.IndexPrice).IndexPrice),
			time.Duration(mark.Env.ExchangeTimeNs-mark.Env.RecvTimeNs))
		return
	}
}

// TestLiveServerPingsAreAnswered checks the claim the adapter relies on rather
// than assuming it: Binance's futures server pings periodically and drops a
// connection that does not pong, and the library is supposed to answer for us.
//
// Binance pings every three minutes, so this runs longer than the rest.
func TestLiveServerPingsAreAnswered(t *testing.T) {
	if testing.Short() {
		t.Skip("waits for a venue ping cycle")
	}
	a := newAdapter(t)

	plans, err := a.PlanSubscriptions([]core.StreamSpec{
		spec(t, "BTC_USDT", pb.Channel_CHANNEL_MARK_PRICE),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	conn, err := a.Dial(ctx, plans[0])
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer conn.Close()

	// A connection that survives a full ping cycle with frames still arriving
	// is the evidence: an unanswered pong gets us disconnected.
	var frames int
	for {
		if _, _, err := conn.Read(ctx); err != nil {
			if frames > 180 && ctx.Err() != nil {
				return // ran the cycle out, still connected
			}
			t.Fatalf("connection dropped after %d frames: %v", frames, err)
		}
		frames++
	}
}

// TestLiveFetchMetadata proves the startup dependency can actually be met.
// Until it is, the venue reports STALE and publishes nothing, so a metadata
// endpoint that has moved is an outage rather than a missing extra.
func TestLiveFetchMetadata(t *testing.T) {
	a := newAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), firstFrameDeadline)
	defer cancel()

	metas, err := a.FetchMetadata(ctx, binance.MarketType)
	if err != nil {
		t.Fatalf("FetchMetadata: %v", err)
	}
	if len(metas) < 10 {
		t.Errorf("the venue listed %d perpetuals, which is fewer than it has", len(metas))
	}

	for _, m := range metas {
		if m.Env.Instrument.Canonical != "BTC_USDT" {
			continue
		}
		if m.TickSize <= 0 || m.LotSize <= 0 || m.ContractMultiplier <= 0 {
			t.Errorf("BTC_USDT tick=%d lot=%d multiplier=%d", m.TickSize, m.LotSize, m.ContractMultiplier)
		}
		t.Logf("BTC_USDT tick %s lot %s min_notional %s active %v",
			price.Price(m.TickSize), price.Size(m.LotSize),
			price.Price(m.MinNotional), m.Active)
		return
	}
	t.Error("the venue listed no BTC_USDT perpetual")
}
