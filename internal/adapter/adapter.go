// Package adapter selects the venue implementation a run needs. The venue
// packages beneath it do the work; this is the one place that knows which
// names exist, so adding a venue is one case here and one new package.
package adapter

import (
	"fmt"
	"maps"
	"slices"

	"github.com/you/manooch/internal/adapter/binance"
	"github.com/you/manooch/internal/adapter/kucoin"
	"github.com/you/manooch/internal/adapter/tabdeal"
	"github.com/you/manooch/internal/config"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/ratelimit"
)

// Dependencies are the process-level collaborators a venue package is handed.
// They are process-level rather than per-adapter because the venue's budget is:
// two adapters with a limiter each would each stay inside a limit they were
// both spending.
type Dependencies struct {
	// Limiter budgets REST weight and websocket connects. Zero means
	// ratelimit.Unlimited.
	Limiter ratelimit.Limiter
}

// builders maps a venue name to its constructor. A venue package never reads
// config itself — it is handed resolved values — so the translation lives here.
var builders = map[string]func(*config.Config, Dependencies) (core.Adapter, error){
	binance.Venue: newBinance,
	kucoin.Venue:  newKuCoin,
	tabdeal.Venue: newTabdeal,
}

// Venues lists the venues this build serves, sorted, for error messages.
func Venues() []string { return slices.Sorted(maps.Keys(builders)) }

// New builds the adapter for the configured venue.
//
// An unknown venue is a startup error naming it and what is available: a
// process that starts against a venue it cannot serve would sit there
// publishing nothing, which looks exactly like a venue that went quiet.
func New(configuration *config.Config, dependencies Dependencies) (core.Adapter, error) {
	build, ok := builders[configuration.Venue]
	if !ok {
		return nil, fmt.Errorf("no adapter for venue %q; this build serves %v", configuration.Venue, Venues())
	}
	if dependencies.Limiter == nil {
		dependencies.Limiter = ratelimit.Unlimited{}
	}
	return build(configuration, dependencies)
}

// Specifications expands the config's streams into the adapter's unit of work.
// It is here rather than in config because StreamSpec is the adapter boundary's
// type.
func Specifications(configuration *config.Config) ([]core.StreamSpec, error) {
	streams := configuration.Streams()
	specifications := make([]core.StreamSpec, 0, len(streams))
	for _, stream := range streams {
		reference, err := core.ParseCanonical(stream.Symbol, stream.MarketType)
		if err != nil {
			return nil, err
		}
		specifications = append(specifications, core.StreamSpec{Instrument: reference, Channel: stream.Channel})
	}
	return specifications, nil
}

// newKuCoin builds the KuCoin adapter.
//
// endpoints.ws holds the bullet host rather than a socket address: KuCoin does
// not let you dial the socket directly, and the address is only knowable once
// the bullet call has answered. See adapter-kucoin.md.
func newKuCoin(configuration *config.Config, dependencies Dependencies) (core.Adapter, error) {
	marketType := core.MarketTypeName(kucoin.MarketType)
	return kucoin.New(kucoin.Options{
		WebSocketEndpoint:   configuration.Endpoints.WebSocket[marketType],
		RESTEndpoint:        configuration.Endpoints.REST[marketType],
		SymbolOverrides:     configuration.SymbolOverrides,
		MaxStreamsPerSocket: configuration.Connection.MaxStreamsPerSocket,
		ReadTimeout:         configuration.Connection.ReadTimeout.Standard(),
		TimeToLive:          configuration.TimeToLiveByChannel(),
		Limiter:             dependencies.Limiter,
	})
}

func newBinance(configuration *config.Config, dependencies Dependencies) (core.Adapter, error) {
	marketType := core.MarketTypeName(binance.MarketType)
	return binance.New(binance.Options{
		WebSocketEndpoint:   configuration.Endpoints.WebSocket[marketType],
		RESTEndpoint:        configuration.Endpoints.REST[marketType],
		SymbolOverrides:     configuration.SymbolOverrides,
		MaxStreamsPerSocket: configuration.Connection.MaxStreamsPerSocket,
		ReadTimeout:         configuration.Connection.ReadTimeout.Standard(),
		TimeToLive:          configuration.TimeToLiveByChannel(),
		Limiter:             dependencies.Limiter,
	})
}

func newTabdeal(configuration *config.Config, dependencies Dependencies) (core.Adapter, error) {
	marketType := core.MarketTypeName(tabdeal.MarketType)
	return tabdeal.New(tabdeal.Options{
		WebSocketEndpoint: configuration.Endpoints.WebSocket[marketType],
		RESTEndpoint:      configuration.Endpoints.REST[marketType],
		SymbolOverrides:   configuration.SymbolOverrides,
		MaxStreamsPerSocket: configuration.Connection.MaxStreamsPerSocket,
		ReadTimeout:       configuration.Connection.ReadTimeout.Standard(),
		TimeToLive:        configuration.TimeToLiveByChannel(),
		Limiter:           dependencies.Limiter,
	})
}
