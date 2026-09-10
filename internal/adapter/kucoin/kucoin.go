// Package kucoin adapts KuCoin futures to core.Adapter.
//
// Public endpoints only. The websocket bootstrap posts to
// /api/v1/bullet-public, which needs no key, no signature and no account: it is
// the same call a browser makes to open the venue's own chart page. There is no
// credential, no authenticated stream and no order path anywhere in this
// package, and there must never be one.
//
// KuCoin differs from Binance in four ways, and all four are absorbed here:
// the symbols are spelled differently, the connection has to be bootstrapped
// over REST, the client sends the pings, and one topic carries two subjects at
// two different cadences.
package kucoin

import (
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

// Venue is the canonical name this adapter answers to.
const Venue = "KUCOIN"

// MarketType is the only market this adapter serves. KuCoin's futures API also
// carries inverse contracts on the same endpoint; we do not subscribe to them,
// and a frame for one is rejected rather than mislabelled as linear.
const MarketType = manoochv1.MarketType_MARKET_TYPE_PERP_LINEAR

// Channels are the three this adapter produces. Unlike Binance they do not all
// arrive together: mark and index share one subject at one second, funding is
// a different subject at one minute.
var Channels = []manoochv1.Channel{
	manoochv1.Channel_CHANNEL_MARK_PRICE,
	manoochv1.Channel_CHANNEL_INDEX_PRICE,
	manoochv1.Channel_CHANNEL_FUNDING,
}

// linearSuffix is what KuCoin appends to a linear perpetual's symbol.
// "XBTUSDTM" is the perpetual; "XBTUSDT" without it is not a futures symbol at
// all, so a venue symbol missing it is rejected rather than guessed at.
const linearSuffix = "M"

// defaultHTTPTimeout bounds every outbound HTTP call this adapter makes: the
// REST fetches and, because coder/websocket turns a client timeout into a
// handshake deadline and then clones the client without it, the websocket
// handshake too. The established connection is unaffected.
//
// Without it the calls are bounded by nothing but the OS. A venue that accepts
// a connection and then says nothing parks the dial indefinitely: the streams
// sit at DEGRADED "dialing", no attempt ever fails, so the circuit breaker
// never trips and no reconnect is ever tried.
const defaultHTTPTimeout = 30 * time.Second

// quotes are the quote assets a linear perpetual may settle in, longest first
// so "ETHUSDTM" resolves to USDT rather than USD.
var quotes = []string{"USDT", "USDC", "USD"}

// Options is everything the adapter needs, resolved from config by the caller.
// The adapter never reads config itself: it is handed values so a test can
// build one without a YAML file.
type Options struct {
	// WebSocketEndpoint is the bullet endpoint's host,
	// "https://api-futures.kucoin.com". It is an HTTP base rather than a wss://
	// one because the socket address is not knowable until the bullet call answers
	// with it — see Dial.
	WebSocketEndpoint string
	// RESTEndpoint is the futures API base, "https://api-futures.kucoin.com".
	RESTEndpoint string

	// SymbolOverrides maps canonical to venue symbol where the rule below is
	// wrong. Keys are canonical ("BTC_USDT"), and this is where BTC's spelling
	// as XBT is stated.
	SymbolOverrides map[string]string

	// MaxStreamsPerSocket caps how many venue topics one socket carries.
	MaxStreamsPerSocket int

	// ReadTimeout and MaxFrameBytes are passed through to transport.
	ReadTimeout   time.Duration
	MaxFrameBytes int64

	// TimeToLive is the Redis key expiry per channel, derived from the venue's
	// cadence. They differ here: funding arrives once a minute against mark price
	// once a second, so one number would expire the funding key between updates.
	TimeToLive map[manoochv1.Channel]time.Duration

	// SubscribeTimeout bounds the wait for every subscription to be
	// acknowledged. Zero means defaultSubscribeTimeout.
	SubscribeTimeout time.Duration

	// Limiter budgets the venue's rate limits. Zero means ratelimit.Unlimited,
	// which is only ever right in a test.
	Limiter ratelimit.Limiter

	// Dial opens a socket. Zero means transport.Dial; a test substitutes.
	Dial transport.Dialer

	// HTTPClient performs the bullet call, the REST calls and the websocket
	// handshake. Zero means a client bounded by HTTPTimeout.
	HTTPClient *http.Client

	// HTTPTimeout bounds one outbound HTTP call. Zero means
	// defaultHTTPTimeout. Ignored when HTTPClient is supplied.
	HTTPTimeout time.Duration

	// ConnectID names this connection to the venue. Zero means a fresh UUID
	// per dial, which is what a real run wants; a test pins it.
	ConnectID func() string
}

// An Adapter is the KuCoin implementation of core.Adapter. It holds no stream
// state: no token, no connection, no counters. The bullet token in particular
// is deliberately not cached — see Dial.
type Adapter struct {
	options Options
	// reverse maps venue symbol back to canonical, built once from the
	// overrides so ParseVenueSymbol does not walk a map on the hot path.
	reverse map[string]string
}

var _ core.Adapter = (*Adapter)(nil)

// New builds the adapter. It opens nothing and fetches no token.
func New(options Options) (*Adapter, error) {
	if options.WebSocketEndpoint == "" {
		return nil, fmt.Errorf("kucoin: no ws endpoint for %s", core.MarketTypeName(MarketType))
	}
	if options.MaxStreamsPerSocket <= 0 {
		return nil, fmt.Errorf("kucoin: max_streams_per_socket is %d", options.MaxStreamsPerSocket)
	}
	for _, channel := range Channels {
		if options.TimeToLive[channel] <= 0 {
			return nil, fmt.Errorf("kucoin: no ttl for channel %s", core.ChannelName(channel))
		}
	}
	if options.Dial == nil {
		options.Dial = transport.Dial
	}
	if options.Limiter == nil {
		options.Limiter = ratelimit.Unlimited{}
	}
	if options.HTTPClient == nil {
		timeout := options.HTTPTimeout
		if timeout <= 0 {
			timeout = defaultHTTPTimeout
		}
		options.HTTPClient = &http.Client{Timeout: timeout}
	}
	if options.ConnectID == nil {
		options.ConnectID = newConnectID
	}
	if options.SubscribeTimeout <= 0 {
		options.SubscribeTimeout = defaultSubscribeTimeout
	}

	adapter := &Adapter{options: options, reverse: make(map[string]string, len(options.SymbolOverrides))}
	for canonical, venueSymbol := range options.SymbolOverrides {
		adapter.reverse[strings.ToUpper(venueSymbol)] = strings.ToUpper(canonical)
	}
	return adapter, nil
}

// Venue returns "KUCOIN".
func (adapter *Adapter) Venue() string { return Venue }

// VenueSymbol maps canonical identity to KuCoin's spelling: BTC_USDT becomes
// XBTUSDTM.
//
// The rule is {BASE}{QUOTE}M. It is wrong for exactly the assets KuCoin spells
// differently — bitcoin is XBT here, not BTC — and symbol_overrides is where
// each of those is stated exactly rather than guessed at by a table of aliases
// that would silently mis-map the next one.
func (adapter *Adapter) VenueSymbol(reference core.InstrumentRef) (string, error) {
	if reference.MarketType != MarketType {
		return "", fmt.Errorf("kucoin: %s is not %s", reference, core.MarketTypeName(MarketType))
	}
	if reference.Expiry != "" {
		return "", fmt.Errorf("kucoin: %s is a dated contract, not a perpetual", reference)
	}
	canonical := reference.Canonical()
	if s, ok := adapter.options.SymbolOverrides[canonical]; ok {
		return strings.ToUpper(s), nil
	}
	return strings.ToUpper(strings.ReplaceAll(canonical, "_", "")) + linearSuffix, nil
}

// ParseVenueSymbol turns "XBTUSDTM" back into BTC_USDT. A reversed override
// wins; otherwise the M is stripped and the longest matching quote asset splits
// what is left.
//
// A symbol without the M is rejected. On KuCoin that is an index or a spot
// pair, not a perpetual, and its price is not one either.
func (adapter *Adapter) ParseVenueSymbol(s string, marketType manoochv1.MarketType) (core.InstrumentRef, error) {
	if marketType != MarketType {
		return core.InstrumentRef{}, fmt.Errorf("kucoin: market type %s is not served", core.MarketTypeName(marketType))
	}
	up := strings.ToUpper(strings.TrimSpace(s))
	if up == "" {
		return core.InstrumentRef{}, fmt.Errorf("kucoin: empty venue symbol")
	}
	if canonical, ok := adapter.reverse[up]; ok {
		return core.ParseCanonical(canonical, marketType)
	}

	body, ok := strings.CutSuffix(up, linearSuffix)
	if !ok || body == "" {
		return core.InstrumentRef{}, fmt.Errorf("kucoin: %q does not end in %q and is not a linear perpetual", s, linearSuffix)
	}
	for _, q := range quotes {
		base, ok := strings.CutSuffix(body, q)
		if ok && base != "" {
			return core.ParseCanonical(base+"_"+q, marketType)
		}
	}
	return core.InstrumentRef{}, fmt.Errorf("kucoin: %q ends in no known quote asset", s)
}

// PlanSubscriptions groups streams onto sockets.
//
// The three channels share one topic per symbol — mark and index on one
// subject, funding on another — so the streams are deduplicated by symbol
// before they are chunked. Subscribing per channel would spend three of the
// venue's subscription slots and deliver each frame three times.
func (adapter *Adapter) PlanSubscriptions(specifications []core.StreamSpec) ([]core.SocketPlan, error) {
	if len(specifications) == 0 {
		return nil, nil
	}

	bySymbol := map[string][]core.StreamSpec{}
	for _, specification := range specifications {
		if err := adapter.checkSpecification(specification); err != nil {
			return nil, err
		}
		symbol, err := adapter.VenueSymbol(specification.Instrument)
		if err != nil {
			return nil, err
		}
		bySymbol[symbol] = append(bySymbol[symbol], specification)
	}

	symbols := make([]string, 0, len(bySymbol))
	for symbol := range bySymbol {
		symbols = append(symbols, symbol)
	}
	sort.Strings(symbols)

	var plans []core.SocketPlan
	for i := 0; i < len(symbols); i += adapter.options.MaxStreamsPerSocket {
		chunk := symbols[i:min(i+adapter.options.MaxStreamsPerSocket, len(symbols))]
		plan := core.SocketPlan{ID: fmt.Sprintf("%s-%d", strings.ToLower(Venue), len(plans))}
		for _, symbol := range chunk {
			group := bySymbol[symbol]
			sort.Slice(group, func(x, y int) bool { return group[x].Channel < group[y].Channel })
			plan.Specifications = append(plan.Specifications, group...)
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

// checkSpecification rejects a stream this venue file should never have
// produced. It is a startup error rather than a silently dropped stream: a key
// nobody writes looks identical to a venue that went quiet.
func (adapter *Adapter) checkSpecification(specification core.StreamSpec) error {
	if specification.Instrument.MarketType != MarketType {
		return fmt.Errorf("kucoin: %s: only %s is served", specification, core.MarketTypeName(MarketType))
	}
	for _, channel := range Channels {
		if specification.Channel == channel {
			return nil
		}
	}
	return fmt.Errorf("kucoin: %s: channel %s is not served", specification, core.ChannelName(specification.Channel))
}

// topics is the venue's topic per symbol for one plan, deduplicated and in plan
// order: "/contract/instrument:XBTUSDTM".
func (adapter *Adapter) topics(plan core.SocketPlan) ([]string, error) {
	var (
		out  []string
		seen = map[string]bool{}
	)
	for _, specification := range plan.Specifications {
		symbol, err := adapter.VenueSymbol(specification.Instrument)
		if err != nil {
			return nil, err
		}
		topic := instrumentTopic + ":" + symbol
		if seen[topic] {
			continue
		}
		seen[topic] = true
		out = append(out, topic)
	}
	return out, nil
}

// RESTCost is KuCoin's own request weight per operation, which is what a rate
// limiter budgets against.
//
// The bullet call has no entry here on purpose. core.Operation names the calls
// a caller decides to make — the fallback poll and the metadata refresh —
// whereas the bullet is something Dial does on its own behalf, so it is
// budgeted inside Dial against bulletWeight and never appears at this
// interface. Adding a venue-specific operation to core.Operation would put one
// venue's habit in the package every venue shares.
func (adapter *Adapter) RESTCost(op core.Operation) int {
	switch op {
	case core.OpFetchOnce:
		return 3 // GET /api/v1/mark-price/{symbol}/current
	case core.OpFetchMetadata:
		return 3 // GET /api/v1/contracts/active
	default:
		return 0
	}
}
