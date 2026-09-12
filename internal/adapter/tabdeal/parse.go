package tabdeal

import (
	"encoding/json"
	"errors"
	"strings"
	"time"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/publish"
	"github.com/you/manooch/pkg/price"
	"google.golang.org/protobuf/proto"
)

type frame struct {
	Stream    string          `json:"stream"`
	Data      json.RawMessage `json:"data"`
	Event     string          `json:"e"`
	EventTime int64           `json:"E"`
	Symbol    string          `json:"s"`
}
type depthData struct {
	Event     string              `json:"e"`
	EventTime int64               `json:"E"`
	Symbol    string              `json:"s"`
	Bids      [][]json.RawMessage `json:"b"`
	Asks      [][]json.RawMessage `json:"a"`
}

func (a *Adapter) Parse(raw []byte, receivedNs int64) ([]core.Message, error) {
	var f frame
	if err := json.Unmarshal(raw, &f); err != nil {
		return nil, core.NewParseError(core.KindJSON, manoochv1.Channel_CHANNEL_ORDERBOOK, "", err, "frame is not json")
	}
	body := raw
	if len(f.Data) > 0 {
		body = f.Data
	}
	var d depthData
	if err := json.Unmarshal(body, &d); err != nil {
		return nil, core.NewParseError(core.KindJSON, manoochv1.Channel_CHANNEL_ORDERBOOK, f.Symbol, err, "payload is not json")
	}
	if d.Symbol == "" {
		d.Symbol = f.Symbol
	}
	if d.Event == "" {
		d.Event = f.Event
	}
	if d.EventTime == 0 {
		d.EventTime = f.EventTime
	}
	if d.Symbol == "" && len(d.Bids) == 0 && len(d.Asks) == 0 {
		return nil, nil
	}
	if d.Event != "" && d.Event != "depthUpdate" {
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_ORDERBOOK, d.Symbol, nil, "unhandled event type %q", d.Event)
	}
	ref, err := a.ParseVenueSymbol(d.Symbol, MarketType)
	if err != nil {
		return nil, core.NewParseError(core.KindField, manoochv1.Channel_CHANNEL_ORDERBOOK, d.Symbol, err, "symbol")
	}
	bids, err := rawLevels(d.Bids)
	if err != nil {
		return nil, core.NewParseError(core.KindJSON, manoochv1.Channel_CHANNEL_ORDERBOOK, d.Symbol, err, "invalid bids")
	}
	asks, err := rawLevels(d.Asks)
	if err != nil {
		return nil, core.NewParseError(core.KindJSON, manoochv1.Channel_CHANNEL_ORDERBOOK, d.Symbol, err, "invalid asks")
	}
	book, err := a.book(ref, d.Symbol, bids, asks, d.EventTime, receivedNs, manoochv1.Source_SOURCE_WEBSOCKET)
	if err != nil {
		return nil, err
	}
	return []core.Message{book}, nil
}

func (a *Adapter) book(ref core.InstrumentRef, symbol string, bids, asks [][]string, eventMs, receivedNs int64, source manoochv1.Source) (core.Message, error) {
	levels := func(raw [][]string, channel manoochv1.Channel) ([]*manoochv1.PriceLevel, error) {
		out := make([]*manoochv1.PriceLevel, 0, len(raw))
		for _, pair := range raw {
			if len(pair) != 2 {
				return nil, core.NewParseError(core.KindField, channel, symbol, nil, "level must contain price and size")
			}
			p, err := price.ParsePrice(pair[0])
			if err != nil {
				return nil, numericError(channel, symbol, "price", pair[0], err)
			}
			s, err := price.ParseSize(pair[1])
			if err != nil {
				return nil, numericError(channel, symbol, "size", pair[1], err)
			}
			out = append(out, &manoochv1.PriceLevel{Price: int64(p), Size: int64(s)})
		}
		return out, nil
	}
	b, err := levels(bids, manoochv1.Channel_CHANNEL_ORDERBOOK)
	if err != nil {
		return core.Message{}, err
	}
	ask, err := levels(asks, manoochv1.Channel_CHANNEL_ORDERBOOK)
	if err != nil {
		return core.Message{}, err
	}
	if eventMs <= 0 {
		eventMs = 0
	}
	envelope := &manoochv1.Envelope{Venue: Venue, Instrument: ref.Proto(strings.ToUpper(symbol)), Channel: manoochv1.Channel_CHANNEL_ORDERBOOK, ExchangeTimeNs: eventMs * int64(time.Millisecond), RecvTimeNs: receivedNs, Source: source, Status: manoochv1.Status_STATUS_HEALTHY, VenueSeqPresent: false, ExchangeTimeIsSendTime: eventMs > 0}
	depth := len(b)
	if len(ask) > depth {
		depth = len(ask)
	}
	return core.Message{Key: publish.Key(Venue, MarketType, ref.Canonical(), manoochv1.Channel_CHANNEL_ORDERBOOK), Proto: &manoochv1.OrderBook{Env: envelope, Bids: b, Asks: ask, Depth: uint32(depth)}, TimeToLive: a.options.TimeToLive[manoochv1.Channel_CHANNEL_ORDERBOOK], Channel: manoochv1.Channel_CHANNEL_ORDERBOOK, Specification: core.StreamSpec{Instrument: ref, Channel: manoochv1.Channel_CHANNEL_ORDERBOOK}}, nil
}

func numericError(channel manoochv1.Channel, symbol, field, value string, err error) error {
	kind := core.KindField
	if errors.Is(err, price.ErrOutOfRange) || errors.Is(err, price.ErrPrecisionLoss) {
		kind = core.KindRange
	}
	return core.NewParseError(kind, channel, symbol, err, "field %q = %q", field, value)
}

var _ proto.Message = (*manoochv1.OrderBook)(nil)
