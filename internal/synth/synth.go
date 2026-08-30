// Package synth generates plausible market data.
//
// It exists so that the publisher, manooch-tap and manooch-status can be
// exercised end to end before any exchange adapter exists — and so that when
// the first adapter lands in M1, a bug can be attributed to the adapter rather
// than to the plumbing underneath it.
//
// The numbers are invented. The envelopes are not: real timestamps, real
// sequence numbers, real fixed-point values at the global scales.
//
// Dev only. Remove at M4.
package synth

import (
	"context"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"sync"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/config"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/pkg/price"
	"google.golang.org/protobuf/proto"
)

// Cadences for the channels a venue does not publish on the book's schedule.
// These are invented for the generator and say nothing about any real venue.
const (
	tradesCadence     = 250 * time.Millisecond
	markPriceCadence  = time.Second
	indexPriceCadence = time.Second
	fundingCadence    = time.Second

	fundingIntervalSeconds = 8 * 60 * 60
)

// A Generator publishes synthetic data for every stream in the config.
type Generator struct {
	cfg *config.Config
	pub publish.Publisher
	log *slog.Logger

	mu      sync.Mutex
	markets map[string]*market
}

// New builds a generator over the streams the config declares.
func New(cfg *config.Config, pub publish.Publisher, log *slog.Logger) *Generator {
	return &Generator{cfg: cfg, pub: pub, log: log, markets: map[string]*market{}}
}

// Run publishes until ctx is cancelled, then returns once every stream has
// stopped.
func (g *Generator) Run(ctx context.Context) {
	streams := g.cfg.Streams()
	g.log.Info("synthetic generator starting", "streams", len(streams))

	var wg sync.WaitGroup
	for _, s := range streams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			g.runStream(ctx, s)
		}()
	}
	wg.Wait()
	g.log.Info("synthetic generator stopped")
}

func (g *Generator) runStream(ctx context.Context, s config.Stream) {
	cadence, ttl := g.schedule(s.Channel)
	key := publish.Key(g.cfg.Venue, s.MarketType, s.Symbol, s.Channel)

	ref, err := core.ParseCanonical(s.Symbol, s.MarketType)
	if err != nil {
		// Load() already rejected anything that could land here.
		g.log.Error("synthetic stream skipped", "key", key, "error", err.Error())
		return
	}
	instrument := ref.Proto(s.VenueSymbol)
	mkt := g.market(s.MarketType, s.Symbol, ref.Base)

	ticker := time.NewTicker(cadence)
	defer ticker.Stop()

	var venueSeq uint64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			venueSeq++
			msg := g.build(s, instrument, mkt, venueSeq)
			if msg == nil {
				return // channel the generator does not produce
			}
			// A failed write is already counted and rate-limit logged by the
			// publisher. Nothing useful to add per message.
			_ = g.pub.Publish(ctx, key, msg, ttl)
		}
	}
}

// schedule returns how often a channel ticks and how long its key should live.
//
// Trades get no TTL. They are event driven, so an empty minute is normal and
// an expiring key would report a healthy stream as stale. Their liveness comes
// from the socket instead.
func (g *Generator) schedule(ch pb.Channel) (cadence, ttl time.Duration) {
	switch ch {
	case pb.Channel_CHANNEL_ORDERBOOK:
		cadence = g.cfg.BookCadence()
	case pb.Channel_CHANNEL_TRADES:
		return tradesCadence, 0
	case pb.Channel_CHANNEL_MARK_PRICE:
		cadence = markPriceCadence
	case pb.Channel_CHANNEL_INDEX_PRICE:
		cadence = indexPriceCadence
	case pb.Channel_CHANNEL_FUNDING:
		cadence = fundingCadence
	default:
		cadence = time.Second
	}
	return cadence, g.cfg.TTL(cadence)
}

func (g *Generator) build(s config.Stream, instrument *pb.Instrument, mkt *market, venueSeq uint64) proto.Message {
	env := g.envelope(instrument, s.Channel, venueSeq)
	mid, tick := mkt.step()

	switch s.Channel {
	case pb.Channel_CHANNEL_ORDERBOOK:
		return buildBook(env, mid, tick, mkt, s.BookDepth)
	case pb.Channel_CHANNEL_TRADES:
		return buildTrades(env, mid, tick, mkt)
	case pb.Channel_CHANNEL_MARK_PRICE:
		return &pb.MarkPrice{Env: env, MarkPrice: int64(mid) + mkt.jitter(tick)}
	case pb.Channel_CHANNEL_INDEX_PRICE:
		return &pb.IndexPrice{Env: env, IndexPrice: int64(mid) + mkt.jitter(tick)}
	case pb.Channel_CHANNEL_FUNDING:
		return buildFunding(env, mkt)
	default:
		return nil
	}
}

// envelope fills the fields an adapter owns. The publisher fills the rest.
func (g *Generator) envelope(instrument *pb.Instrument, ch pb.Channel, venueSeq uint64) *pb.Envelope {
	now := time.Now()
	// A plausible venue-to-us delay, so latency histograms have something with
	// shape in them rather than a spike at zero.
	exchangeTime := now.Add(-time.Duration(5+rand.IntN(20)) * time.Millisecond)
	recvTime := now.Add(-time.Duration(rand.IntN(500)) * time.Microsecond)

	return &pb.Envelope{
		Venue:           g.cfg.Venue,
		Instrument:      instrument,
		Channel:         ch,
		ExchangeTimeNs:  exchangeTime.UnixNano(),
		RecvTimeNs:      recvTime.UnixNano(),
		VenueSeq:        venueSeq,
		VenueSeqPresent: true,
		Source:          pb.Source_SOURCE_WEBSOCKET,
		Status:          pb.Status_STATUS_HEALTHY,
	}
}

func buildBook(env *pb.Envelope, mid price.Price, tick price.Price, mkt *market, depth uint32) *pb.OrderBook {
	if depth == 0 {
		depth = 1
	}
	book := &pb.OrderBook{
		Env:   env,
		Bids:  make([]*pb.PriceLevel, 0, depth),
		Asks:  make([]*pb.PriceLevel, 0, depth),
		Depth: depth,
	}
	// Half a tick of spread either side of the mid, then one tick per level.
	for i := range int64(depth) {
		bid := int64(mid) - int64(tick)*(i+1)
		ask := int64(mid) + int64(tick)*(i+1)
		if bid <= 0 {
			break
		}
		book.Bids = append(book.Bids, &pb.PriceLevel{Price: bid, Size: mkt.size()})
		book.Asks = append(book.Asks, &pb.PriceLevel{Price: ask, Size: mkt.size()})
	}
	book.Depth = uint32(len(book.Bids))
	return book
}

func buildTrades(env *pb.Envelope, mid price.Price, tick price.Price, mkt *market) *pb.Trades {
	n := 1 + rand.IntN(3)
	trades := &pb.Trades{Env: env, Trades: make([]*pb.Trade, 0, n)}
	for i := range n {
		side := pb.Side_SIDE_BUY
		if rand.IntN(2) == 0 {
			side = pb.Side_SIDE_SELL
		}
		trades.Trades = append(trades.Trades, &pb.Trade{
			TradeId:        fmt.Sprintf("%d-%d", env.RecvTimeNs, i),
			Price:          int64(mid) + mkt.jitter(tick),
			Size:           mkt.size(),
			AggressorSide:  side,
			ExchangeTimeNs: env.ExchangeTimeNs,
		})
	}
	return trades
}

func buildFunding(env *pb.Envelope, mkt *market) *pb.Funding {
	// A rate in the ballpark of +/- 1bp, at the 1e-12 rate scale.
	rate := int64(rand.IntN(2*int(price.RateScale/10_000)+1)) - int64(price.RateScale/10_000)
	next := time.Now().Truncate(fundingIntervalSeconds * time.Second).Add(fundingIntervalSeconds * time.Second)
	return &pb.Funding{
		Env:               env,
		FundingRate:       rate,
		NextFundingTimeNs: next.UnixNano(),
		IntervalSeconds:   fundingIntervalSeconds,
	}
}

// ---------- per-instrument state ----------

// market is the shared price for one instrument, so that the book, the trades
// and the mark price of the same symbol tell the same story.
type market struct {
	mu   sync.Mutex
	mid  price.Price
	tick price.Price
	unit price.Size // typical trade size
}

func (g *Generator) market(mt pb.MarketType, symbol, base string) *market {
	k := core.MarketTypeName(mt) + ":" + symbol
	g.mu.Lock()
	defer g.mu.Unlock()
	if m, ok := g.markets[k]; ok {
		return m
	}
	m := newMarket(base)
	g.markets[k] = m
	return m
}

// seeds are plausible mid prices, tick sizes and trade sizes by base asset.
var seeds = map[string]struct {
	mid  string
	tick string
	unit string
}{
	"BTC": {"68432.15", "0.1", "0.015"},
	"ETH": {"3521.40", "0.01", "0.25"},
	"SOL": {"152.37", "0.001", "5"},
}

func newMarket(base string) *market {
	s, ok := seeds[base]
	if !ok {
		s = struct{ mid, tick, unit string }{"1.00", "0.0001", "100"}
	}
	mid, _ := price.ParsePrice(s.mid)
	tick, _ := price.ParsePrice(s.tick)
	unit, _ := price.ParseSize(s.unit)
	return &market{mid: mid, tick: tick, unit: unit}
}

// step advances the random walk one increment and returns the new mid.
func (m *market) step() (price.Price, price.Price) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.mid += price.Price(int64(m.tick) * int64(rand.IntN(5)-2))
	if floor := m.tick * 100; m.mid < floor {
		m.mid = floor
	}
	return m.mid, m.tick
}

// jitter is a few ticks either way, for prices that should sit near the mid
// without sitting exactly on it.
func (m *market) jitter(tick price.Price) int64 {
	return int64(tick) * int64(rand.IntN(5)-2)
}

// size is a trade or level size around the instrument's typical unit.
func (m *market) size() int64 {
	m.mu.Lock()
	unit := int64(m.unit)
	m.mu.Unlock()
	return unit/2 + rand.Int64N(unit*2+1)
}
