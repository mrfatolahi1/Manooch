// Package binance adapts Binance USD-M futures to core.Adapter.
//
// Public endpoints only. There is no credential, no signature and no
// authenticated stream anywhere in this package, and there must never be one:
// this service reads prices and has no business placing an order.
//
// One markPrice frame carries the mark price, the index price and the funding
// rate, so one frame becomes three messages on three keys.
package binance

import (
	"context"
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
const Venue = "BINANCE"

// MarketType is the only market this adapter serves. Binance's USD-M futures
// endpoint also carries dated delivery contracts; we do not subscribe to them,
// and a frame for one is rejected rather than mislabelled as a perpetual.
const MarketType = manoochv1.MarketType_MARKET_TYPE_PERP_LINEAR

// Channels are the three a markPrice frame produces, in the order Parse emits
// them. Fixed order keeps Parse deterministic.
var Channels = []manoochv1.Channel{
	manoochv1.Channel_CHANNEL_MARK_PRICE,
	manoochv1.Channel_CHANNEL_INDEX_PRICE,
	manoochv1.Channel_CHANNEL_FUNDING,
}

// streamSuffix is the 1-second mark price stream. The plain "@markPrice" form
// updates every 3 seconds, which would put every key past its TTL between
// updates.
const streamSuffix = "@markPrice@1s"

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

// eventMarkPriceUpdate is the only event type this adapter handles.
const eventMarkPriceUpdate = "markPriceUpdate"

// quotes are the quote assets a venue symbol may end in, longest first so
// "BTCUSDT" resolves to USDT rather than USD. Reversing the concatenation is
// guesswork by nature; symbol_overrides is how an exception is stated exactly.
var quotes = []string{"USDT", "USDC", "BUSD", "TUSD", "FDUSD", "USD", "BNB", "BTC", "ETH"}

// Options is everything the adapter needs, resolved from config by the caller.
// The adapter never reads config itself: it is handed values so a test can
// build one without a YAML file.
type Options struct {
	// WebSocketEndpoint is the combined-stream base,
	// "wss://fstream.binance.com/stream".
	WebSocketEndpoint string
	// RESTEndpoint is the futures API base, "https://fapi.binance.com".
	RESTEndpoint string

	// SymbolOverrides maps canonical to venue symbol where the rule below is
	// wrong. Keys are canonical ("BTC_USDT").
	SymbolOverrides map[string]string

	// MaxStreamsPerSocket caps how many venue streams one socket carries.
	MaxStreamsPerSocket int

	// ReadTimeout and MaxFrameBytes are passed through to transport.
	ReadTimeout   time.Duration
	MaxFrameBytes int64

	// TimeToLive is the Redis key expiry per channel, derived from the venue's
	// cadence.
	TimeToLive map[manoochv1.Channel]time.Duration

	// Limiter budgets the venue's rate limits. Zero means ratelimit.Unlimited,
	// which is only ever right in a test: the daemon builds one from the venue
	// file before it builds an adapter.
	Limiter ratelimit.Limiter

	// Dial opens a socket. Zero means transport.Dial; a test substitutes.
	Dial transport.Dialer

	// HTTPClient performs REST calls and the websocket handshake. Zero means a
	// client bounded by HTTPTimeout.
	HTTPClient *http.Client

	// HTTPTimeout bounds one outbound HTTP call. Zero means
	// defaultHTTPTimeout. Ignored when HTTPClient is supplied.
	HTTPTimeout time.Duration
}

// An Adapter is the Binance implementation of core.Adapter. It holds no stream
// state: everything below is a pure function of Options and its arguments.
type Adapter struct {
	options Options
	// reverse maps venue symbol back to canonical, built once from the
	// overrides so ParseVenueSymbol does not walk a map on the hot path.
	reverse map[string]string
}

var _ core.Adapter = (*Adapter)(nil)

// New builds the adapter. It opens nothing.
func New(options Options) (*Adapter, error) {
	if options.WebSocketEndpoint == "" {
		return nil, fmt.Errorf("binance: no ws endpoint for %s", core.MarketTypeName(MarketType))
	}
	if options.MaxStreamsPerSocket <= 0 {
		return nil, fmt.Errorf("binance: max_streams_per_socket is %d", options.MaxStreamsPerSocket)
	}
	for _, channel := range Channels {
		if options.TimeToLive[channel] <= 0 {
			return nil, fmt.Errorf("binance: no ttl for channel %s", core.ChannelName(channel))
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

	adapter := &Adapter{options: options, reverse: make(map[string]string, len(options.SymbolOverrides))}
	for canonical, venueSymbol := range options.SymbolOverrides {
		adapter.reverse[strings.ToUpper(venueSymbol)] = strings.ToUpper(canonical)
	}
	return adapter, nil
}

// Venue returns "BINANCE".
func (adapter *Adapter) Venue() string { return Venue }

// VenueSymbol strips the separator: BTC_USDT becomes BTCUSDT. symbol_overrides
// wins, for the symbols where that rule is wrong.
func (adapter *Adapter) VenueSymbol(reference core.InstrumentRef) (string, error) {
	if reference.MarketType != MarketType {
		return "", fmt.Errorf("binance: %s is not %s", reference, core.MarketTypeName(MarketType))
	}
	if reference.Expiry != "" {
		return "", fmt.Errorf("binance: %s is a dated contract, not a perpetual", reference)
	}
	canonical := reference.Canonical()
	if s, ok := adapter.options.SymbolOverrides[canonical]; ok {
		return strings.ToUpper(s), nil
	}
	return strings.ToUpper(strings.ReplaceAll(canonical, "_", "")), nil
}

// ParseVenueSymbol turns "BTCUSDT" back into BTC_USDT. A reversed override
// wins; otherwise the longest matching quote asset splits the string.
//
// A dated contract's symbol carries an underscore ("BTCUSDT_240329") and is
// rejected: its price is not a perpetual's, and labelling it as one would put
// a wrong number under a key a consumer trusts.
func (adapter *Adapter) ParseVenueSymbol(s string, marketType manoochv1.MarketType) (core.InstrumentRef, error) {
	if marketType != MarketType {
		return core.InstrumentRef{}, fmt.Errorf("binance: market type %s is not served", core.MarketTypeName(marketType))
	}
	up := strings.ToUpper(strings.TrimSpace(s))
	if up == "" {
		return core.InstrumentRef{}, fmt.Errorf("binance: empty venue symbol")
	}
	if strings.Contains(up, "_") {
		return core.InstrumentRef{}, fmt.Errorf("binance: %q is a dated contract, not a perpetual", s)
	}

	if canonical, ok := adapter.reverse[up]; ok {
		return core.ParseCanonical(canonical, marketType)
	}
	for _, q := range quotes {
		base, ok := strings.CutSuffix(up, q)
		if ok && base != "" {
			return core.ParseCanonical(base+"_"+q, marketType)
		}
	}
	return core.InstrumentRef{}, fmt.Errorf("binance: %q ends in no known quote asset", s)
}

// PlanSubscriptions groups streams onto sockets.
//
// The three channels all arrive on one venue stream per symbol, so the streams
// are deduplicated before they are chunked: subscribing three times to
// btcusdt@markPrice@1s would spend three of the venue's slots and deliver the
// same frame three times.
func (adapter *Adapter) PlanSubscriptions(specifications []core.StreamSpec) ([]core.SocketPlan, error) {
	if len(specifications) == 0 {
		return nil, nil
	}

	// Grouped by venue stream, in sorted order, so the same config always
	// produces the same plans with the same IDs.
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
		return fmt.Errorf("binance: %s: only %s is served", specification, core.MarketTypeName(MarketType))
	}
	for _, channel := range Channels {
		if specification.Channel == channel {
			return nil
		}
	}
	return fmt.Errorf("binance: %s: channel %s is not served", specification, core.ChannelName(specification.Channel))
}

// streamNames is the venue's stream path for one plan, deduplicated and in
// plan order: "btcusdt@markPrice@1s".
func (adapter *Adapter) streamNames(plan core.SocketPlan) ([]string, error) {
	var (
		names []string
		seen  = map[string]bool{}
	)
	for _, specification := range plan.Specifications {
		symbol, err := adapter.VenueSymbol(specification.Instrument)
		if err != nil {
			return nil, err
		}
		name := strings.ToLower(symbol) + streamSuffix
		if seen[name] {
			continue
		}
		seen[name] = true
		names = append(names, name)
	}
	return names, nil
}

// SocketURL is the combined-stream URL for one plan. Symbols in the path are
// lower case; the payload echoes them upper case.
func (adapter *Adapter) SocketURL(plan core.SocketPlan) (string, error) {
	names, err := adapter.streamNames(plan)
	if err != nil {
		return "", err
	}
	if len(names) == 0 {
		return "", fmt.Errorf("binance: plan %s has no streams", plan.ID)
	}
	// Not url.Values: Binance wants the streams slash-separated in one value,
	// and Encode would escape the slashes.
	return adapter.options.WebSocketEndpoint + "?streams=" + strings.Join(names, "/"), nil
}

// Dial opens one socket for one plan.
//
// There is no subscription message to send or acknowledge: the streams are in
// the URL, so a connection that opens is a subscription that took. Binance
// refuses the handshake for an unknown stream name rather than accepting it
// and staying silent.
func (adapter *Adapter) Dial(ctx context.Context, plan core.SocketPlan) (core.Conn, error) {
	u, err := adapter.SocketURL(plan)
	if err != nil {
		return nil, err
	}
	// Budgeted before the socket is opened, not after: a dial that is refused
	// must not happen at all. The caller reports the streams DEGRADED and
	// backs off, which is the whole reason the limiter exists.
	if err := adapter.options.Limiter.Allow(ctx, Venue, ratelimit.LimitWebSocketConnect, 1); err != nil {
		return nil, fmt.Errorf("binance: dial %s: %w", plan.ID, err)
	}
	return adapter.options.Dial(ctx, transport.Options{
		URL:           u,
		ReadTimeout:   adapter.options.ReadTimeout,
		MaxFrameBytes: adapter.options.MaxFrameBytes,
		HTTPClient:    adapter.options.HTTPClient,
	})
}

// RESTCost is Binance's published request weight per operation, which is what
// a rate limiter budgets against.
func (adapter *Adapter) RESTCost(op core.Operation) int {
	switch op {
	case core.OpFetchOnce:
		return 1 // GET /fapi/v1/premiumIndex with a symbol
	case core.OpFetchMetadata:
		return 1 // GET /fapi/v1/exchangeInfo
	default:
		return 0
	}
}
