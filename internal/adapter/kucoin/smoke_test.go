//go:build smoke

// Smoke tests reach the real venue over the public internet. They are behind a
// build tag and out of normal CI: a red build must mean our code broke, not
// that KuCoin was slow or a runner had no egress.
//
//	go test -tags=smoke -count=1 -v ./internal/adapter/kucoin
package kucoin_test

import (
	"context"
	"testing"
	"time"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/adapter/kucoin"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/pkg/price"
)

// maxClockSkew is what a synced host should manage against a venue on the far
// side of the internet. Past it, either our clock is wrong or the venue's is,
// and every freshness number the service publishes is wrong with it.
const maxClockSkew = 5000 * time.Millisecond

// firstFrameDeadline bounds the wait for real data. mark.index.price is
// documented to update once a second; fifteen seconds is a dead connection.
const firstFrameDeadline = 15 * time.Second

// liveAdapter is the adapter against the real venue, configured the way the
// shipped venue file configures it.
func liveAdapter(t *testing.T) *kucoin.Adapter {
	t.Helper()
	return newAdapterWith(t, kucoin.Options{
		WebSocketEndpoint: "https://api-futures.kucoin.com",
		RESTEndpoint:      "https://api-futures.kucoin.com",
		ReadTimeout:       60 * time.Second,
		ConnectID:         nil, // a real UUID per connection, as production does
	})
}

func livePlan(t *testing.T, adapter *kucoin.Adapter) core.SocketPlan {
	t.Helper()
	plans, err := adapter.PlanSubscriptions([]core.StreamSpec{
		specification(t, "BTC_USDT", manoochv1.Channel_CHANNEL_MARK_PRICE),
	})
	if err != nil {
		t.Fatalf("PlanSubscriptions: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %d, want 1", len(plans))
	}
	return plans[0]
}

// TestLiveInstrumentStream connects to KuCoin through the bullet bootstrap,
// subscribes to one instrument, and asserts a well-formed message arrives with
// a plausible clock skew.
//
// A Dial that returns at all is the bullet call succeeding: without a token and
// an endpoint there is no socket to return.
func TestLiveInstrumentStream(t *testing.T) {
	adapter := liveAdapter(t)

	ctx, cancel := context.WithTimeout(context.Background(), firstFrameDeadline)
	defer cancel()

	connection, err := adapter.Dial(ctx, livePlan(t, adapter))
	if err != nil {
		t.Fatalf("Dial (bullet, socket and subscription): %v", err)
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
			continue // the welcome, an ack, or a pong
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
				t.Errorf("%s: claims a venue sequence; this topic carries none", message.Key)
			}
			if message.TimeToLive <= 0 {
				t.Errorf("%s: ttl = %v", message.Key, message.TimeToLive)
			}
		}

		mark, ok := messages[0].Proto.(*manoochv1.MarkPrice)
		if !ok {
			// funding.rate arrives once a minute and may land first.
			continue
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

// TestLiveClientPingKeepsTheConnection is the quirk that has no equivalent on
// Binance: KuCoin drops a connection whose client stops pinging. The bullet
// response asks for a ping every 18 seconds and allows 10 seconds of slack, so
// a connection still delivering after both have passed is the ping working.
//
// Nothing here sends a ping: the connection Dial returned owns that ticker.
func TestLiveClientPingKeepsTheConnection(t *testing.T) {
	if testing.Short() {
		t.Skip("waits out a full ping interval plus its timeout")
	}
	adapter := liveAdapter(t)

	// pingInterval + pingTimeout is 28s on this venue; 40 leaves room for the
	// venue to act on a ping we failed to send.
	const past = 40 * time.Second

	ctx, cancel := context.WithTimeout(context.Background(), past+firstFrameDeadline)
	defer cancel()

	connection, err := adapter.Dial(ctx, livePlan(t, adapter))
	if err != nil {
		t.Fatalf("Dial: %v", err)
	}
	defer connection.Close()

	deadline := time.Now().Add(past)
	var frames, messages int
	for time.Now().Before(deadline) {
		frame, receivedNs, err := connection.Read(ctx)
		if err != nil {
			t.Fatalf("connection dropped after %v and %d frames: %v",
				past-time.Until(deadline), frames, err)
		}
		frames++
		parsed, err := adapter.Parse(frame, receivedNs)
		if err != nil {
			t.Fatalf("Parse %s: %v", frame, err)
		}
		messages += len(parsed)
	}

	// Still connected is the point; still delivering is the proof that it is a
	// live connection rather than a socket nobody has noticed is dead.
	if messages == 0 {
		t.Errorf("%d frames in %v but no market data; the connection is not delivering", frames, past)
	}
	t.Logf("survived %v with %d frames and %d messages", past, frames, messages)
}

// TestLiveFetchOnce exercises the REST fallback path against the real
// endpoints, including the funding call whose response names the index symbol
// rather than the contract's.
func TestLiveFetchOnce(t *testing.T) {
	adapter := liveAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), firstFrameDeadline)
	defer cancel()

	for _, channel := range []manoochv1.Channel{
		manoochv1.Channel_CHANNEL_MARK_PRICE,
		manoochv1.Channel_CHANNEL_INDEX_PRICE,
		manoochv1.Channel_CHANNEL_FUNDING,
	} {
		t.Run(core.ChannelName(channel), func(t *testing.T) {
			messages, err := adapter.FetchOnce(ctx, specification(t, "BTC_USDT", channel))
			if err != nil {
				t.Fatalf("FetchOnce: %v", err)
			}
			if len(messages) != 1 {
				t.Fatalf("messages = %d, want 1", len(messages))
			}
			envelope := messages[0].Proto.(interface{ GetEnv() *manoochv1.Envelope }).GetEnv()
			if envelope.Source != manoochv1.Source_SOURCE_REST {
				t.Errorf("source = %v, want REST", envelope.Source)
			}
			if envelope.Instrument.VenueSymbol != "XBTUSDTM" {
				t.Errorf("venue_symbol = %q, want XBTUSDTM", envelope.Instrument.VenueSymbol)
			}
			skew := time.Duration(envelope.ExchangeTimeNs-envelope.RecvTimeNs) * time.Nanosecond
			t.Logf("%s skew %v", core.ChannelName(channel), skew)
		})
	}
}

// TestLiveFetchMetadata proves the startup dependency can actually be met, and
// that the contract multiplier — the number every downstream order size depends
// on — is really there.
func TestLiveFetchMetadata(t *testing.T) {
	adapter := liveAdapter(t)
	ctx, cancel := context.WithTimeout(context.Background(), firstFrameDeadline)
	defer cancel()

	metadataList, err := adapter.FetchMetadata(ctx, kucoin.MarketType)
	if err != nil {
		t.Fatalf("FetchMetadata: %v", err)
	}
	if len(metadataList) < 10 {
		t.Errorf("the venue listed %d linear perpetuals, which is fewer than it has", len(metadataList))
	}

	for _, metadata := range metadataList {
		if metadata.Env.Instrument.Canonical != "BTC_USDT" {
			continue
		}
		if metadata.TickSize <= 0 || metadata.LotSize <= 0 || metadata.ContractMultiplier <= 0 {
			t.Errorf("BTC_USDT tick=%d lot=%d multiplier=%d", metadata.TickSize, metadata.LotSize, metadata.ContractMultiplier)
		}
		t.Logf("BTC_USDT tick %s lot %s multiplier %s active %v",
			price.Price(metadata.TickSize), price.Size(metadata.LotSize),
			price.Size(metadata.ContractMultiplier), metadata.Active)
		return
	}
	t.Error("the venue listed no BTC_USDT perpetual")
}
