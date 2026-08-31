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

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/transport"
)

// Venue is the canonical name this adapter answers to.
const Venue = "BINANCE"

// MarketType is the only market this adapter serves. Binance's USD-M futures
// endpoint also carries dated delivery contracts; we do not subscribe to them,
// and a frame for one is rejected rather than mislabelled as a perpetual.
const MarketType = pb.MarketType_MARKET_TYPE_PERP_LINEAR

// Channels are the three a markPrice frame produces, in the order Parse emits
// them. Fixed order keeps Parse deterministic.
var Channels = []pb.Channel{
	pb.Channel_CHANNEL_MARK_PRICE,
	pb.Channel_CHANNEL_INDEX_PRICE,
	pb.Channel_CHANNEL_FUNDING,
}

// streamSuffix is the 1-second mark price stream. The plain "@markPrice" form
// updates every 3 seconds, which would put every key past its TTL between
// updates.
const streamSuffix = "@markPrice@1s"

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
	// WSEndpoint is the combined-stream base, "wss://fstream.binance.com/stream".
	WSEndpoint string
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

	// TTLs is the Redis key TTL per channel, derived from the venue's cadence.
	TTLs map[pb.Channel]time.Duration

	// Dial opens a socket. Zero means transport.Dial; a test substitutes.
	Dial transport.Dialer

	// HTTPClient performs REST calls and the websocket handshake.
	HTTPClient *http.Client
}

// An Adapter is the Binance implementation of core.Adapter. It holds no stream
// state: everything below is a pure function of Options and its arguments.
type Adapter struct {
	opts Options
	// reverse maps venue symbol back to canonical, built once from the
	// overrides so ParseVenueSymbol does not walk a map on the hot path.
	reverse map[string]string
}

var _ core.Adapter = (*Adapter)(nil)

// New builds the adapter. It opens nothing.
func New(opts Options) (*Adapter, error) {
	if opts.WSEndpoint == "" {
		return nil, fmt.Errorf("binance: no ws endpoint for %s", core.MarketTypeName(MarketType))
	}
	if opts.MaxStreamsPerSocket <= 0 {
		return nil, fmt.Errorf("binance: max_streams_per_socket is %d", opts.MaxStreamsPerSocket)
	}
	for _, ch := range Channels {
		if opts.TTLs[ch] <= 0 {
			return nil, fmt.Errorf("binance: no ttl for channel %s", core.ChannelName(ch))
		}
	}
	if opts.Dial == nil {
		opts.Dial = transport.Dial
	}
	if opts.HTTPClient == nil {
		opts.HTTPClient = http.DefaultClient
	}

	a := &Adapter{opts: opts, reverse: make(map[string]string, len(opts.SymbolOverrides))}
	for canonical, venueSymbol := range opts.SymbolOverrides {
		a.reverse[strings.ToUpper(venueSymbol)] = strings.ToUpper(canonical)
	}
	return a, nil
}

// Venue returns "BINANCE".
func (a *Adapter) Venue() string { return Venue }

// VenueSymbol strips the separator: BTC_USDT becomes BTCUSDT. symbol_overrides
// wins, for the symbols where that rule is wrong.
func (a *Adapter) VenueSymbol(ref core.InstrumentRef) (string, error) {
	if ref.MarketType != MarketType {
		return "", fmt.Errorf("binance: %s is not %s", ref, core.MarketTypeName(MarketType))
	}
	if ref.Expiry != "" {
		return "", fmt.Errorf("binance: %s is a dated contract, not a perpetual", ref)
	}
	canonical := ref.Canonical()
	if s, ok := a.opts.SymbolOverrides[canonical]; ok {
		return strings.ToUpper(s), nil
	}
	return strings.ToUpper(strings.ReplaceAll(canonical, "_", "")), nil
}

// ParseVenueSymbol turns "BTCUSDT" back into BTC_USDT. A reversed override wins;
// otherwise the longest matching quote asset splits the string.
//
// A dated contract's symbol carries an underscore ("BTCUSDT_240329") and is
// rejected: its price is not a perpetual's, and labelling it as one would put
// a wrong number under a key a consumer trusts.
func (a *Adapter) ParseVenueSymbol(s string, mt pb.MarketType) (core.InstrumentRef, error) {
	if mt != MarketType {
		return core.InstrumentRef{}, fmt.Errorf("binance: market type %s is not served", core.MarketTypeName(mt))
	}
	up := strings.ToUpper(strings.TrimSpace(s))
	if up == "" {
		return core.InstrumentRef{}, fmt.Errorf("binance: empty venue symbol")
	}
	if strings.Contains(up, "_") {
		return core.InstrumentRef{}, fmt.Errorf("binance: %q is a dated contract, not a perpetual", s)
	}

	if canonical, ok := a.reverse[up]; ok {
		return core.ParseCanonical(canonical, mt)
	}
	for _, q := range quotes {
		base, ok := strings.CutSuffix(up, q)
		if ok && base != "" {
			return core.ParseCanonical(base+"_"+q, mt)
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
func (a *Adapter) PlanSubscriptions(specs []core.StreamSpec) ([]core.SocketPlan, error) {
	if len(specs) == 0 {
		return nil, nil
	}

	// Grouped by venue stream, in sorted order, so the same config always
	// produces the same plans with the same IDs.
	bySymbol := map[string][]core.StreamSpec{}
	for _, spec := range specs {
		if err := a.checkSpec(spec); err != nil {
			return nil, err
		}
		sym, err := a.VenueSymbol(spec.Instrument)
		if err != nil {
			return nil, err
		}
		bySymbol[sym] = append(bySymbol[sym], spec)
	}

	symbols := make([]string, 0, len(bySymbol))
	for sym := range bySymbol {
		symbols = append(symbols, sym)
	}
	sort.Strings(symbols)

	var plans []core.SocketPlan
	for i := 0; i < len(symbols); i += a.opts.MaxStreamsPerSocket {
		chunk := symbols[i:min(i+a.opts.MaxStreamsPerSocket, len(symbols))]
		plan := core.SocketPlan{ID: fmt.Sprintf("%s-%d", strings.ToLower(Venue), len(plans))}
		for _, sym := range chunk {
			group := bySymbol[sym]
			sort.Slice(group, func(x, y int) bool { return group[x].Channel < group[y].Channel })
			plan.Specs = append(plan.Specs, group...)
		}
		plans = append(plans, plan)
	}
	return plans, nil
}

// checkSpec rejects a stream this venue file should never have produced. It is
// a startup error rather than a silently dropped stream: a key nobody writes
// looks identical to a venue that went quiet.
func (a *Adapter) checkSpec(spec core.StreamSpec) error {
	if spec.Instrument.MarketType != MarketType {
		return fmt.Errorf("binance: %s: only %s is served", spec, core.MarketTypeName(MarketType))
	}
	for _, ch := range Channels {
		if spec.Channel == ch {
			return nil
		}
	}
	return fmt.Errorf("binance: %s: channel %s is not served", spec, core.ChannelName(spec.Channel))
}

// streamNames is the venue's stream path for one plan, deduplicated and in
// plan order: "btcusdt@markPrice@1s".
func (a *Adapter) streamNames(plan core.SocketPlan) ([]string, error) {
	var (
		names []string
		seen  = map[string]bool{}
	)
	for _, spec := range plan.Specs {
		sym, err := a.VenueSymbol(spec.Instrument)
		if err != nil {
			return nil, err
		}
		name := strings.ToLower(sym) + streamSuffix
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
func (a *Adapter) SocketURL(plan core.SocketPlan) (string, error) {
	names, err := a.streamNames(plan)
	if err != nil {
		return "", err
	}
	if len(names) == 0 {
		return "", fmt.Errorf("binance: plan %s has no streams", plan.ID)
	}
	// Not url.Values: Binance wants the streams slash-separated in one value,
	// and Encode would escape the slashes.
	return a.opts.WSEndpoint + "?streams=" + strings.Join(names, "/"), nil
}

// Dial opens one socket for one plan.
//
// There is no subscription message to send or acknowledge: the streams are in
// the URL, so a connection that opens is a subscription that took. Binance
// refuses the handshake for an unknown stream name rather than accepting it
// and staying silent.
func (a *Adapter) Dial(ctx context.Context, plan core.SocketPlan) (core.Conn, error) {
	u, err := a.SocketURL(plan)
	if err != nil {
		return nil, err
	}
	return a.opts.Dial(ctx, transport.Options{
		URL:           u,
		ReadTimeout:   a.opts.ReadTimeout,
		MaxFrameBytes: a.opts.MaxFrameBytes,
		HTTPClient:    a.opts.HTTPClient,
	})
}

// RESTCost is Binance's published request weight per operation, which is what
// a rate limiter budgets against.
func (a *Adapter) RESTCost(op core.Operation) int {
	switch op {
	case core.OpFetchOnce:
		return 1 // GET /fapi/v1/premiumIndex with a symbol
	case core.OpFetchMetadata:
		return 1 // GET /fapi/v1/exchangeInfo
	default:
		return 0
	}
}

// FetchMetadata is not implemented yet.
func (a *Adapter) FetchMetadata(ctx context.Context, mt pb.MarketType) ([]*pb.InstrumentMeta, error) {
	return nil, fmt.Errorf("binance: FetchMetadata: %w", core.ErrNotImplemented)
}
