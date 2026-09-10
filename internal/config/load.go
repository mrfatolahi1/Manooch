package config

import (
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"net/url"
	"os"
	"path/filepath"
	"reflect"
	"regexp"
	"slices"
	"strings"

	"github.com/go-playground/validator/v10"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/pkg/price"
	"gopkg.in/yaml.v3"
)

const (
	// DefaultsFile is the base config, shared by every venue.
	DefaultsFile = "defaults.yaml"
	// VenuesDir holds one file per venue, named after the lower-cased venue.
	VenuesDir = "venues"
)

var symbolRe = regexp.MustCompile(core.CanonicalPattern)

// Load reads dir/defaults.yaml, overlays dir/venues/<venue>.yaml and returns
// the result only if every validation rule passes. Merging is per key: the
// venue file overrides the keys it sets and leaves the rest alone.
func Load(dir string, venue string) (*Config, error) {
	defaultsPath := filepath.Join(dir, DefaultsFile)
	venuePath := filepath.Join(dir, VenuesDir, strings.ToLower(venue)+".yaml")

	configuration := &Config{}
	if err := decodeStrict(defaultsPath, configuration); err != nil {
		return nil, err
	}
	if err := decodeStrict(venuePath, configuration); err != nil {
		return nil, err
	}

	provenance, err := newProvenance(defaultsPath, venuePath)
	if err != nil {
		return nil, err
	}

	if want := strings.ToUpper(venue); configuration.Venue != want {
		return nil, provenance.newError("venue", "is %q but %s was requested", configuration.Venue, want)
	}
	if err := configuration.validate(provenance); err != nil {
		return nil, err
	}
	return configuration, nil
}

// decodeStrict decodes one YAML file onto out, rejecting unknown keys.
func decodeStrict(path string, out any) error {
	file, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	defer file.Close()

	decoder := yaml.NewDecoder(file)
	decoder.KnownFields(true)
	if err := decoder.Decode(out); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%s: file is empty", path)
		}
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// ---------- provenance ----------

// provenance remembers which file set which key, so a validation failure can
// name the file to go and fix.
type provenance struct {
	files        map[string]string // dotted key path -> file that set it
	defaultsPath string
}

func newProvenance(defaultsPath, venuePath string) (*provenance, error) {
	provenance := &provenance{files: map[string]string{}, defaultsPath: defaultsPath}
	// Defaults first, venue second: the venue file wins where both set a key.
	for _, path := range []string{defaultsPath, venuePath} {
		var raw map[string]any
		b, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("config: %w", err)
		}
		if err := yaml.Unmarshal(b, &raw); err != nil {
			return nil, fmt.Errorf("%s: %w", path, err)
		}
		flatten("", raw, path, provenance.files)
	}
	return provenance, nil
}

func flatten(prefix string, v any, file string, out map[string]string) {
	switch t := v.(type) {
	case map[string]any:
		for k, value := range t {
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			out[key] = file
			flatten(key, value, file, out)
		}
	case []any:
		for i, value := range t {
			key := fmt.Sprintf("%s[%d]", prefix, i)
			out[key] = file
			flatten(key, value, file, out)
		}
	}
}

// file returns the file responsible for a key path, walking up to the nearest
// ancestor that was set when the key itself is absent.
func (provenance *provenance) file(path string) string {
	for current := path; current != ""; {
		if f, ok := provenance.files[current]; ok {
			return f
		}
		i := strings.LastIndexAny(current, ".[")
		if i <= 0 {
			break
		}
		current = current[:i]
	}
	return provenance.defaultsPath
}

// newError builds an error naming both the offending key and its file.
func (provenance *provenance) newError(path, format string, arguments ...any) error {
	return fmt.Errorf("%s: %s: %s", provenance.file(path), path, fmt.Sprintf(format, arguments...))
}

// ---------- validation ----------

func (configuration *Config) validate(provenance *provenance) error {
	var failures []error

	v := validator.New()
	v.RegisterTagNameFunc(yamlTagName)
	if err := v.Struct(configuration); err != nil {
		var validationErrors validator.ValidationErrors
		if !errors.As(err, &validationErrors) {
			return fmt.Errorf("config: %w", err)
		}
		for _, fieldError := range validationErrors {
			path := strings.TrimPrefix(fieldError.Namespace(), "Config.")
			failures = append(failures, provenance.newError(path, "%s", describe(fieldError)))
		}
	}

	failures = append(failures, configuration.validateScales(provenance)...)
	failures = append(failures, configuration.validateHTTP(provenance)...)
	failures = append(failures, configuration.validateHealth(provenance)...)
	failures = append(failures, configuration.validateEndpoints(provenance)...)
	failures = append(failures, configuration.validateRateLimit(provenance)...)
	failures = append(failures, configuration.validateCadence(provenance)...)
	failures = append(failures, configuration.validateSymbolOverrides(provenance)...)
	failures = append(failures, configuration.resolveInstruments(provenance)...)

	return errors.Join(failures...)
}

// validateScales rejects scales that disagree with pkg/price; a mismatch puts
// every number off by a power of ten.
func (configuration *Config) validateScales(provenance *provenance) []error {
	var failures []error
	for _, s := range []struct {
		key  string
		got  int
		want int
	}{
		{"scales.price_exp", configuration.Scales.PriceExp, price.PriceExp},
		{"scales.size_exp", configuration.Scales.SizeExp, price.SizeExp},
		{"scales.rate_exp", configuration.Scales.RateExp, price.RateExp},
	} {
		if s.got != s.want {
			failures = append(failures, provenance.newError(s.key, "is %d but pkg/price is compiled for %d", s.got, s.want))
		}
	}
	return failures
}

// validateHTTP keeps the admin surface on loopback: /metrics leaks which books
// we watch and /debug/pprof hands out a heap dump to anyone who asks.
func (configuration *Config) validateHTTP(provenance *provenance) []error {
	const key = "service.http.listen"
	if !configuration.Service.HTTP.Enabled {
		return nil
	}
	host, _, err := net.SplitHostPort(configuration.Service.HTTP.Listen)
	if err != nil {
		return []error{provenance.newError(key, "must be host:port, got %q", configuration.Service.HTTP.Listen)}
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return []error{provenance.newError(key, "must bind to a loopback address, got %q; /metrics and /debug/pprof must not be reachable off-host", configuration.Service.HTTP.Listen)}
}

func (configuration *Config) validateHealth(provenance *provenance) []error {
	if configuration.Health.ClockSkewStaleMS <= configuration.Health.ClockSkewDegradedMS {
		return []error{provenance.newError("health.clock_skew_stale_ms",
			"is %d but must be greater than health.clock_skew_degraded_ms (%d)",
			configuration.Health.ClockSkewStaleMS, configuration.Health.ClockSkewDegradedMS)}
	}
	return nil
}

// validateRateLimit checks the venue's own limits against the plan we build
// from them. A socket carrying more streams than the venue accepts on one
// connection is refused at subscribe time, which looks like a venue outage
// rather than a config error.
func (configuration *Config) validateRateLimit(provenance *provenance) []error {
	if configuration.Connection.MaxStreamsPerSocket > configuration.RateLimit.SubscriptionsPerConnection {
		return []error{provenance.newError("connection.max_streams_per_socket",
			"is %d but rate_limit.subscriptions_per_connection is %d; the venue would refuse the extra subscriptions",
			configuration.Connection.MaxStreamsPerSocket, configuration.RateLimit.SubscriptionsPerConnection)}
	}
	return nil
}

func (configuration *Config) validateEndpoints(provenance *provenance) []error {
	var failures []error
	check := func(kind string, m map[string]string, schemes ...string) {
		// Sorted so several broken endpoints report in a stable order.
		for _, name := range slices.Sorted(maps.Keys(m)) {
			raw := m[name]
			key := "endpoints." + kind + "." + name
			if _, err := core.ParseMarketType(name); err != nil {
				failures = append(failures, provenance.newError(key, "%v", err))
			}
			parsed, err := url.Parse(raw)
			if err != nil || parsed.Host == "" {
				failures = append(failures, provenance.newError(key, "is not a URL: %q", raw))
				continue
			}
			if !slices.Contains(schemes, parsed.Scheme) {
				failures = append(failures, provenance.newError(key, "scheme %q must be one of %s", parsed.Scheme, strings.Join(schemes, ", ")))
			}
		}
	}
	// endpoints.ws accepts an https base as well as a wss one. Not every venue
	// lets you dial the socket directly: KuCoin hands out the address over a
	// public REST call, so what belongs here is where to ask rather than where
	// to connect. Rejecting https would have forced that URL into a key the
	// adapter does not read, which is a config file that lies.
	check("ws", configuration.Endpoints.WebSocket, "ws", "wss", "http", "https")
	check("rest", configuration.Endpoints.REST, "http", "https")
	return failures
}

// validateCadence checks every quirks.cadence key names a real channel and
// carries a positive duration. The cadence is what a stream's key TTL is
// derived from, so a missing or zero one publishes a key that expires
// immediately and reports a healthy stream as dead.
func (configuration *Config) validateCadence(provenance *provenance) []error {
	var failures []error
	// Sorted so several broken entries report in a stable order.
	for _, name := range slices.Sorted(maps.Keys(configuration.Quirks.Cadence)) {
		key := "quirks.cadence." + name
		channel, err := core.ParseChannel(name)
		if err != nil {
			failures = append(failures, provenance.newError(key, "%v", err))
			continue
		}
		if core.ChannelName(channel) != name {
			failures = append(failures, provenance.newError(key, "channel %q must be lower snake case", name))
		}
		if configuration.Quirks.Cadence[name] <= 0 {
			failures = append(failures, provenance.newError(key, "must be greater than 0, got %s", configuration.Quirks.Cadence[name]))
		}
	}
	return failures
}

func (configuration *Config) validateSymbolOverrides(provenance *provenance) []error {
	var failures []error
	for _, canonical := range slices.Sorted(maps.Keys(configuration.SymbolOverrides)) {
		venueSymbol := configuration.SymbolOverrides[canonical]
		key := "symbol_overrides." + canonical
		if !symbolRe.MatchString(canonical) {
			failures = append(failures, provenance.newError(key, "key must match %s", core.CanonicalPattern))
		}
		if venueSymbol == "" {
			failures = append(failures, provenance.newError(key, "venue symbol must not be empty"))
		}
	}
	return failures
}

// resolveInstruments checks every instrument block and fills in the parsed
// enums. It runs last, so those fields are only populated on a config that
// passed.
func (configuration *Config) resolveInstruments(provenance *provenance) []error {
	var failures []error
	seenMarket := map[string]int{}

	for i := range configuration.Instruments {
		in := &configuration.Instruments[i]
		base := fmt.Sprintf("instruments[%d]", i)

		if prev, duplicate := seenMarket[in.MarketType]; duplicate {
			failures = append(failures, provenance.newError(base+".market_type",
				"market_type %q is already configured by instruments[%d]", in.MarketType, prev))
		}
		seenMarket[in.MarketType] = i

		marketType, err := core.ParseMarketType(in.MarketType)
		if err != nil {
			failures = append(failures, provenance.newError(base+".market_type", "%v", err))
			continue // everything below needs a market type
		}
		in.ResolvedMarketType = marketType

		if _, ok := configuration.Endpoints.WebSocket[in.MarketType]; !ok {
			failures = append(failures, provenance.newError("endpoints.ws", "has no entry for market_type %q used by %s", in.MarketType, base))
		}

		in.ResolvedChannels = in.ResolvedChannels[:0]
		for j, name := range in.Channels {
			key := fmt.Sprintf("%s.channels[%d]", base, j)
			channel, err := core.ParseChannel(name)
			if err != nil {
				failures = append(failures, provenance.newError(key, "%v", err))
				continue
			}
			if !core.ChannelValidFor(channel, marketType) {
				failures = append(failures, provenance.newError(key, "channel %q does not exist on market_type %s", name, in.MarketType))
				continue
			}
			if slices.Contains(in.ResolvedChannels, channel) {
				failures = append(failures, provenance.newError(key, "channel %q is listed twice", name))
				continue
			}
			// Without a cadence there is no TTL, and a key with no TTL cannot
			// say whether it is fresh.
			if _, ok := configuration.Quirks.Cadence[name]; !ok {
				failures = append(failures, provenance.newError("quirks.cadence",
					"has no entry for channel %q used by %s", name, base))
			}
			in.ResolvedChannels = append(in.ResolvedChannels, channel)
		}

		for j, symbol := range in.Symbols {
			key := fmt.Sprintf("%s.symbols[%d]", base, j)
			if !symbolRe.MatchString(symbol) {
				failures = append(failures, provenance.newError(key, "symbol %q must match %s", symbol, core.CanonicalPattern))
				continue
			}
			if slices.Index(in.Symbols, symbol) != j {
				failures = append(failures, provenance.newError(key, "symbol %q is listed twice", symbol))
			}
		}
	}
	return failures
}

// yamlTagName makes validator report the YAML key an operator wrote rather than
// the Go field name they have never seen.
func yamlTagName(field reflect.StructField) string {
	name, _, _ := strings.Cut(field.Tag.Get("yaml"), ",")
	if name == "" || name == "-" {
		return field.Name
	}
	return name
}

func describe(fieldError validator.FieldError) string {
	switch fieldError.Tag() {
	case "required":
		return "is required"
	case "oneof":
		return "must be one of: " + strings.ReplaceAll(fieldError.Param(), " ", ", ")
	case "eq":
		return fmt.Sprintf("must be %q, got %v", fieldError.Param(), fieldError.Value())
	case "gt":
		return fmt.Sprintf("must be greater than %s, got %v", fieldError.Param(), fieldError.Value())
	case "gte":
		return fmt.Sprintf("must be at least %s, got %v", fieldError.Param(), fieldError.Value())
	case "lte":
		return fmt.Sprintf("must be at most %s, got %v", fieldError.Param(), fieldError.Value())
	case "min":
		return fmt.Sprintf("must have at least %s entries", fieldError.Param())
	case "hostname_port":
		return fmt.Sprintf("must be host:port, got %v", fieldError.Value())
	case "uppercase":
		return fmt.Sprintf("must be upper case, got %v", fieldError.Value())
	default:
		return fmt.Sprintf("fails rule %q (got %v)", fieldError.Tag(), fieldError.Value())
	}
}
