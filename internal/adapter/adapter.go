// Package adapter selects the venue implementation a run needs. The venue
// packages beneath it do the work; this is the one place that knows which
// names exist, so adding a venue is one case here and one new package.
package adapter

import (
	"fmt"
	"maps"
	"slices"

	"github.com/you/manooch/internal/adapter/binance"
	"github.com/you/manooch/internal/config"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/ratelimit"
)

// Deps are the process-level collaborators a venue package is handed. They are
// process-level rather than per-adapter because the venue's budget is: two
// adapters with a limiter each would each stay inside a limit they were both
// spending.
type Deps struct {
	// Limiter budgets REST weight and websocket connects. Zero means
	// ratelimit.Unlimited.
	Limiter ratelimit.Limiter
}

// builders maps a venue name to its constructor. A venue package never reads
// config itself — it is handed resolved values — so the translation lives here.
var builders = map[string]func(*config.Config, Deps) (core.Adapter, error){
	binance.Venue: newBinance,
}

// Venues lists the venues this build serves, sorted, for error messages.
func Venues() []string { return slices.Sorted(maps.Keys(builders)) }

// New builds the adapter for the configured venue.
//
// An unknown venue is a startup error naming it and what is available: a
// process that starts against a venue it cannot serve would sit there
// publishing nothing, which looks exactly like a venue that went quiet.
func New(cfg *config.Config, deps Deps) (core.Adapter, error) {
	build, ok := builders[cfg.Venue]
	if !ok {
		return nil, fmt.Errorf("no adapter for venue %q; this build serves %v", cfg.Venue, Venues())
	}
	if deps.Limiter == nil {
		deps.Limiter = ratelimit.Unlimited{}
	}
	return build(cfg, deps)
}

// Specs expands the config's streams into the adapter's unit of work. It is
// here rather than in config because StreamSpec is the adapter boundary's type.
func Specs(cfg *config.Config) ([]core.StreamSpec, error) {
	streams := cfg.Streams()
	specs := make([]core.StreamSpec, 0, len(streams))
	for _, s := range streams {
		ref, err := core.ParseCanonical(s.Symbol, s.MarketType)
		if err != nil {
			return nil, err
		}
		specs = append(specs, core.StreamSpec{Instrument: ref, Channel: s.Channel})
	}
	return specs, nil
}

func newBinance(cfg *config.Config, deps Deps) (core.Adapter, error) {
	mt := core.MarketTypeName(binance.MarketType)
	return binance.New(binance.Options{
		WSEndpoint:          cfg.Endpoints.WS[mt],
		RESTEndpoint:        cfg.Endpoints.REST[mt],
		SymbolOverrides:     cfg.SymbolOverrides,
		MaxStreamsPerSocket: cfg.Connection.MaxStreamsPerSocket,
		ReadTimeout:         cfg.Connection.ReadTimeout.Std(),
		TTLs:                cfg.TTLs(),
		Limiter:             deps.Limiter,
	})
}
