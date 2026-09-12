// Package tabdeal adapts Tabdeal's public futures depth feed.
package tabdeal

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/ratelimit"
	"github.com/you/manooch/internal/transport"
)

const Venue = "TABDEAL"
const MarketType = manoochv1.MarketType_MARKET_TYPE_PERP_LINEAR

var Channels = []manoochv1.Channel{manoochv1.Channel_CHANNEL_ORDERBOOK}

type Options struct {
	WebSocketEndpoint   string
	RESTEndpoint        string
	SymbolOverrides     map[string]string
	MaxStreamsPerSocket int
	ReadTimeout         time.Duration
	MaxFrameBytes       int64
	TimeToLive          map[manoochv1.Channel]time.Duration
	Limiter             ratelimit.Limiter
	Dial                transport.Dialer
	HTTPClient          *http.Client
}

type Adapter struct {
	options Options
	reverse map[string]string
}

var _ core.Adapter = (*Adapter)(nil)

func New(options Options) (*Adapter, error) {
	if options.WebSocketEndpoint == "" {
		return nil, fmt.Errorf("tabdeal: no ws endpoint")
	}
	if options.MaxStreamsPerSocket <= 0 {
		return nil, fmt.Errorf("tabdeal: max_streams_per_socket is %d", options.MaxStreamsPerSocket)
	}
	if options.TimeToLive[manoochv1.Channel_CHANNEL_ORDERBOOK] <= 0 {
		return nil, fmt.Errorf("tabdeal: no ttl for channel orderbook")
	}
	if options.Limiter == nil {
		options.Limiter = ratelimit.Unlimited{}
	}
	if options.Dial == nil {
		options.Dial = transport.Dial
	}
	if options.HTTPClient == nil {
		options.HTTPClient = &http.Client{Timeout: 30 * time.Second}
	}
	a := &Adapter{options: options, reverse: map[string]string{}}
	for canonical, symbol := range options.SymbolOverrides {
		a.reverse[strings.ToUpper(symbol)] = strings.ToUpper(canonical)
	}
	return a, nil
}

func (a *Adapter) Venue() string { return Venue }

func (a *Adapter) VenueSymbol(reference core.InstrumentRef) (string, error) {
	if reference.MarketType != MarketType || reference.Expiry != "" {
		return "", fmt.Errorf("tabdeal: %s is not a perpetual linear", reference)
	}
	if s, ok := a.options.SymbolOverrides[reference.Canonical()]; ok {
		return strings.ToUpper(s), nil
	}
	return strings.ToUpper(reference.Canonical()), nil
}

func (a *Adapter) ParseVenueSymbol(symbol string, marketType manoochv1.MarketType) (core.InstrumentRef, error) {
	if marketType != MarketType {
		return core.InstrumentRef{}, fmt.Errorf("tabdeal: market type %s is not served", core.MarketTypeName(marketType))
	}
	symbol = strings.ToUpper(strings.TrimSpace(symbol))
	if symbol == "" {
		return core.InstrumentRef{}, fmt.Errorf("tabdeal: empty venue symbol")
	}
	if canonical, ok := a.reverse[symbol]; ok {
		return core.ParseCanonical(canonical, marketType)
	}
	if strings.Contains(symbol, "_") {
		return core.ParseCanonical(symbol, marketType)
	}
	for _, quote := range []string{"USDT", "USDC", "IRT"} {
		if base, ok := strings.CutSuffix(symbol, quote); ok && base != "" {
			return core.ParseCanonical(base+"_"+quote, marketType)
		}
	}
	return core.InstrumentRef{}, fmt.Errorf("tabdeal: symbol %q has no known quote", symbol)
}

func (a *Adapter) check(spec core.StreamSpec) error {
	if spec.Instrument.MarketType != MarketType {
		return fmt.Errorf("tabdeal: %s: only PERP_LINEAR is served", spec)
	}
	if spec.Channel != manoochv1.Channel_CHANNEL_ORDERBOOK {
		return fmt.Errorf("tabdeal: channel %s is not served", core.ChannelName(spec.Channel))
	}
	return nil
}

func (a *Adapter) PlanSubscriptions(specs []core.StreamSpec) ([]core.SocketPlan, error) {
	if len(specs) == 0 {
		return nil, nil
	}
	bySymbol := map[string][]core.StreamSpec{}
	for _, s := range specs {
		if err := a.check(s); err != nil {
			return nil, err
		}
		symbol, err := a.VenueSymbol(s.Instrument)
		if err != nil {
			return nil, err
		}
		bySymbol[symbol] = append(bySymbol[symbol], s)
	}
	symbols := make([]string, 0, len(bySymbol))
	for s := range bySymbol {
		symbols = append(symbols, s)
	}
	sort.Strings(symbols)
	var plans []core.SocketPlan
	for i := 0; i < len(symbols); i += a.options.MaxStreamsPerSocket {
		end := i + a.options.MaxStreamsPerSocket
		if end > len(symbols) {
			end = len(symbols)
		}
		p := core.SocketPlan{ID: fmt.Sprintf("tabdeal-%d", len(plans))}
		for _, symbol := range symbols[i:end] {
			p.Specifications = append(p.Specifications, bySymbol[symbol]...)
		}
		plans = append(plans, p)
	}
	return plans, nil
}

func (a *Adapter) SocketURL(plan core.SocketPlan) (string, error) {
	if len(plan.Specifications) == 0 {
		return "", fmt.Errorf("tabdeal: plan has no streams")
	}
	parts := make([]string, 0, len(plan.Specifications))
	seen := map[string]bool{}
	for _, s := range plan.Specifications {
		symbol, err := a.VenueSymbol(s.Instrument)
		if err != nil {
			return "", err
		}
		stream := strings.ToLower(symbol) + "@depth@2000ms"
		if !seen[stream] {
			parts = append(parts, stream)
			seen[stream] = true
		}
	}
	return a.options.WebSocketEndpoint, nil
}

func (a *Adapter) Dial(ctx context.Context, plan core.SocketPlan) (core.Conn, error) {
	u, err := a.SocketURL(plan)
	if err != nil {
		return nil, err
	}
	if err := a.options.Limiter.Allow(ctx, Venue, ratelimit.LimitWebSocketConnect, 1); err != nil {
		return nil, fmt.Errorf("tabdeal: dial: %w", err)
	}
	conn, err := a.options.Dial(ctx, transport.Options{URL: u, ReadTimeout: a.options.ReadTimeout, MaxFrameBytes: a.options.MaxFrameBytes, HTTPClient: a.options.HTTPClient})
	if err != nil {
		return nil, err
	}
	params := make([]string, 0, len(plan.Specifications))
	seen := map[string]bool{}
	for _, s := range plan.Specifications {
		symbol, _ := a.VenueSymbol(s.Instrument)
		stream := strings.ToLower(symbol) + "@depth@2000ms"
		if !seen[stream] {
			params = append(params, stream)
			seen[stream] = true
		}
	}
	payload, _ := json.Marshal(map[string]any{"method": "SUBSCRIBE", "params": params, "id": 1})
	if err := conn.Write(ctx, payload); err != nil {
		conn.Close()
		return nil, err
	}
	return conn, nil
}

func (a *Adapter) RESTCost(op core.Operation) int {
	if op == core.OpFetchOnce || op == core.OpFetchMetadata {
		return 1
	}
	return 0
}
