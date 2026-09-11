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

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/adapter/binance"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/pkg/price"
)

// maxClockSkew is what a synced host should manage against a venue on the far
// side of the internet. Past it, either our clock is wrong or the venue's is,
// and every freshness number the service publishes is wrong with it.
const maxClockSkew = 5000 * time.Millisecond

// firstFrameDeadline bounds the wait for real data. The stream is documented to
// update once a second; fifteen seconds is a dead connection, not a slow one.
const firstFrameDeadline = 15 * time.Second

// TestLiveMarkPriceStream connects to Binance, subscribes to one symbol, and
// asserts a well-formed message arrives with a plausible clock skew.
func TestLiveMarkPriceStream(t *testing.T) {
	adapter := newAdapter(t)

	plans, err := adapter.PlanSubscriptions([]core.StreamSpec{
		specification(t, "BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE),
	})
	if err != nil {
		t.Fatalf("PlanSubscriptions: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %d, want 1", len(plans))
	}

	ctx, cancel := context.WithTimeout(context.Background(), firstFrameDeadline)
	defer cancel()

	connection, err := adapter.Dial(ctx, plans[0])
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer connection.Close()

	for {
		frame, receivedNs, err := connection.Read(ctx)
		if err != nil {
			t.Fatalf("Read: %v", err)
		}
		messages, err := adapter.Parse(frame, receivedNs)
		if err != nil {
			t.Fatalf("Parse %s: %v", frame, err)
		}
		if len(messages) == 0 {
			continue // an ack or a control frame
		}

		if len(messages) != 3 {
			t.Errorf("one frame produced %d messages, want 3", len(messages))
		}
		for _, message := range messages {
			envelope := message.Proto.(interface{ GetEnv() *manoochv1.Envelope }).GetEnv()

			skew := time.Duration(envelope.ExchangeTimeNs-envelope.RecvTimeNs) * time.Nanosecond
			if skew < -maxClockSkew || skew > maxClockSkew {
				t.Errorf("%s: clock skew %v exceeds %v", message.Key, skew, maxClockSkew)
			}
			if envelope.Status != manoochv1.Status_STATUS_HEALTHY {
				t.Errorf("%s: status = %v", message.Key, envelope.Status)
			}
			if envelope.VenueSeqPresent {
				t.Errorf("%s: claims a venue sequence; this stream carries none", message.Key)
			}
			if message.TimeToLive <= 0 {
				t.Errorf("%s: ttl = %v", message.Key, message.TimeToLive)
			}
		}

		mark, ok := messages[0].Proto.(*manoochv1.MarkPrice)
		if !ok {
			t.Fatalf("first message is %T, want *manoochv1.MarkPrice", messages[0].Proto)
		}
		if mark.MarkPrice <= 0 {
			t.Fatalf("mark price = %d", mark.MarkPrice)
		}
		t.Logf("BTC_USDT mark %s, index %s, skew %v",
			price.Price(mark.MarkPrice),
			price.Price(messages[1].Proto.(*manoochv1.IndexPrice).IndexPrice),
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
	adapter := newAdapter(t)

	plans, err := adapter.PlanSubscriptions([]core.StreamSpec{
		specification(t, "BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE),
	})
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Minute)
	defer cancel()

	connection, err := adapter.Dial(ctx, plans[0])
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer connection.Close()

	// A connection that survives a full ping cycle with frames still arriving
	// is the evidence: an unanswered pong gets us disconnected.
	var frames int
	for {
		if _, _, err := connection.Read(ctx); err != nil {
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
	adapter := newAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), firstFrameDeadline)
	defer cancel()

	metadataList, err := adapter.FetchMetadata(ctx, binance.MarketType)
	if err != nil {
		t.Fatalf("FetchMetadata: %v", err)
	}
	if len(metadataList) < 10 {
		t.Errorf("the venue listed %d perpetuals, which is fewer than it has", len(metadataList))
	}

	for _, metadata := range metadataList {
		if metadata.Env.Instrument.Canonical != "BTC_USDT" {
			continue
		}
		if metadata.TickSize <= 0 || metadata.LotSize <= 0 || metadata.ContractMultiplier <= 0 {
			t.Errorf("BTC_USDT tick=%d lot=%d multiplier=%d", metadata.TickSize, metadata.LotSize, metadata.ContractMultiplier)
		}
		t.Logf("BTC_USDT tick %s lot %s min_notional %s active %v",
			price.Price(metadata.TickSize), price.Size(metadata.LotSize),
			price.Price(metadata.MinNotional), metadata.Active)
		return
	}
	t.Error("the venue listed no BTC_USDT perpetual")
}
