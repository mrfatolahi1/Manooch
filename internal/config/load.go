package config

import (
	"errors"
	"fmt"
	"io"
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

// Load reads dir/defaults.yaml, overlays dir/venues/<venue>.yaml, and returns
// the merged configuration only if every validation rule passes.
//
// Merging is per key: the venue file overrides the individual keys it sets and
// leaves the rest of defaults.yaml alone.
func Load(dir string, venue string) (*Config, error) {
	defaultsPath := filepath.Join(dir, DefaultsFile)
	venuePath := filepath.Join(dir, VenuesDir, strings.ToLower(venue)+".yaml")

	cfg := &Config{}
	if err := decodeStrict(defaultsPath, cfg); err != nil {
		return nil, err
	}
	if err := decodeStrict(venuePath, cfg); err != nil {
		return nil, err
	}

	prov, err := newProvenance(defaultsPath, venuePath)
	if err != nil {
		return nil, err
	}

	if want := strings.ToUpper(venue); cfg.Venue != want {
		return nil, prov.errf("venue", "is %q but %s was requested", cfg.Venue, want)
	}
	if err := cfg.validate(prov); err != nil {
		return nil, err
	}
	return cfg, nil
}

// decodeStrict decodes one YAML file onto out. Unknown keys are an error: a
// misspelled key that is quietly dropped leaves the service running on a
// default the operator did not choose and cannot see.
func decodeStrict(path string, out any) error {
	f, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("config: %w", err)
	}
	defer f.Close()

	dec := yaml.NewDecoder(f)
	dec.KnownFields(true)
	if err := dec.Decode(out); err != nil {
		if errors.Is(err, io.EOF) {
			return fmt.Errorf("%s: file is empty", path)
		}
		return fmt.Errorf("%s: %w", path, err)
	}
	return nil
}

// ---------- provenance ----------

// provenance remembers which file set which key, so that a validation failure
// can tell the operator where to go and fix it.
type provenance struct {
	files        map[string]string // dotted key path -> file that set it
	defaultsPath string
}

func newProvenance(defaultsPath, venuePath string) (*provenance, error) {
	p := &provenance{files: map[string]string{}, defaultsPath: defaultsPath}
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
		flatten("", raw, path, p.files)
	}
	return p, nil
}

func flatten(prefix string, v any, file string, out map[string]string) {
	switch t := v.(type) {
	case map[string]any:
		for k, val := range t {
			key := k
			if prefix != "" {
				key = prefix + "." + k
			}
			out[key] = file
			flatten(key, val, file, out)
		}
	case []any:
		for i, val := range t {
			key := fmt.Sprintf("%s[%d]", prefix, i)
			out[key] = file
			flatten(key, val, file, out)
		}
	}
}

// file returns the file responsible for a key path, walking up to the nearest
// ancestor that was actually set when the key itself is absent.
func (p *provenance) file(path string) string {
	for cur := path; cur != ""; {
		if f, ok := p.files[cur]; ok {
			return f
		}
		i := strings.LastIndexAny(cur, ".[")
		if i <= 0 {
			break
		}
		cur = cur[:i]
	}
	return p.defaultsPath
}

// errf builds an error that names both the offending key and its file.
func (p *provenance) errf(path, format string, args ...any) error {
	return fmt.Errorf("%s: %s: %s", p.file(path), path, fmt.Sprintf(format, args...))
}

// ---------- validation ----------

func (c *Config) validate(p *provenance) error {
	var errs []error

	v := validator.New()
	v.RegisterTagNameFunc(yamlTagName)
	if err := v.Struct(c); err != nil {
		var ve validator.ValidationErrors
		if !errors.As(err, &ve) {
			return fmt.Errorf("config: %w", err)
		}
		for _, fe := range ve {
			path := strings.TrimPrefix(fe.Namespace(), "Config.")
			errs = append(errs, p.errf(path, "%s", describe(fe)))
		}
	}

	errs = append(errs, c.validateScales(p)...)
	errs = append(errs, c.validateHTTP(p)...)
	errs = append(errs, c.validateHealth(p)...)
	errs = append(errs, c.validateEndpoints(p)...)
	errs = append(errs, c.validateSymbolOverrides(p)...)
	errs = append(errs, c.resolveInstruments(p)...)

	return errors.Join(errs...)
}

// validateScales rejects a config whose scales disagree with pkg/price. The
// wire format has no room for a per-message scale beyond the envelope's
// override, so a mismatch here is every number off by a power of ten.
func (c *Config) validateScales(p *provenance) []error {
	var errs []error
	for _, s := range []struct {
		key  string
		got  int
		want int
	}{
		{"scales.price_exp", c.Scales.PriceExp, price.PriceExp},
		{"scales.size_exp", c.Scales.SizeExp, price.SizeExp},
		{"scales.rate_exp", c.Scales.RateExp, price.RateExp},
	} {
		if s.got != s.want {
			errs = append(errs, p.errf(s.key, "is %d but pkg/price is compiled for %d", s.got, s.want))
		}
	}
	return errs
}

// validateHTTP enforces that the admin surface stays on loopback. /metrics
// leaks the shape of the book we are watching and /debug/pprof will hand out a
// heap dump to anyone who asks.
func (c *Config) validateHTTP(p *provenance) []error {
	const key = "service.http.listen"
	if !c.Service.HTTP.Enabled {
		return nil
	}
	host, _, err := net.SplitHostPort(c.Service.HTTP.Listen)
	if err != nil {
		return []error{p.errf(key, "must be host:port, got %q", c.Service.HTTP.Listen)}
	}
	if host == "localhost" {
		return nil
	}
	if ip := net.ParseIP(host); ip != nil && ip.IsLoopback() {
		return nil
	}
	return []error{p.errf(key, "must bind to a loopback address, got %q; /metrics and /debug/pprof must not be reachable off-host", c.Service.HTTP.Listen)}
}

func (c *Config) validateHealth(p *provenance) []error {
	if c.Health.ClockSkewStaleMS <= c.Health.ClockSkewDegradedMS {
		return []error{p.errf("health.clock_skew_stale_ms",
			"is %d but must be greater than health.clock_skew_degraded_ms (%d)",
			c.Health.ClockSkewStaleMS, c.Health.ClockSkewDegradedMS)}
	}
	return nil
}

func (c *Config) validateEndpoints(p *provenance) []error {
	var errs []error
	check := func(kind string, m map[string]string, schemes ...string) {
		for name, raw := range m {
			key := "endpoints." + kind + "." + name
			if _, err := core.ParseMarketType(name); err != nil {
				errs = append(errs, p.errf(key, "%v", err))
			}
			u, err := url.Parse(raw)
			if err != nil || u.Host == "" {
				errs = append(errs, p.errf(key, "is not a URL: %q", raw))
				continue
			}
			if !slices.Contains(schemes, u.Scheme) {
				errs = append(errs, p.errf(key, "scheme %q must be one of %s", u.Scheme, strings.Join(schemes, ", ")))
			}
		}
	}
	check("ws", c.Endpoints.WS, "ws", "wss")
	check("rest", c.Endpoints.REST, "http", "https")
	return errs
}

func (c *Config) validateSymbolOverrides(p *provenance) []error {
	var errs []error
	for canonical, venueSymbol := range c.SymbolOverrides {
		key := "symbol_overrides." + canonical
		if !symbolRe.MatchString(canonical) {
			errs = append(errs, p.errf(key, "key must match %s", core.CanonicalPattern))
		}
		if venueSymbol == "" {
			errs = append(errs, p.errf(key, "venue symbol must not be empty"))
		}
	}
	return errs
}

// resolveInstruments checks every instrument block and fills in the parsed
// enums. It is the last step, so the resolved fields are only ever populated
// on a config that passed.
func (c *Config) resolveInstruments(p *provenance) []error {
	var errs []error
	seenMarket := map[string]int{}

	for i := range c.Instruments {
		in := &c.Instruments[i]
		base := fmt.Sprintf("instruments[%d]", i)

		if prev, dup := seenMarket[in.MarketType]; dup {
			errs = append(errs, p.errf(base+".market_type",
				"market_type %q is already configured by instruments[%d]", in.MarketType, prev))
		}
		seenMarket[in.MarketType] = i

		mt, err := core.ParseMarketType(in.MarketType)
		if err != nil {
			errs = append(errs, p.errf(base+".market_type", "%v", err))
			continue // everything below needs a market type
		}
		in.MT = mt

		if !slices.Contains(c.Quirks.BookDepthsSupported, in.BookDepth) {
			errs = append(errs, p.errf(base+".book_depth",
				"is %d but quirks.book_depths_supported is %v", in.BookDepth, c.Quirks.BookDepthsSupported))
		}

		if _, ok := c.Endpoints.WS[in.MarketType]; !ok {
			errs = append(errs, p.errf("endpoints.ws", "has no entry for market_type %q used by %s", in.MarketType, base))
		}

		in.Chans = in.Chans[:0]
		for j, name := range in.Channels {
			key := fmt.Sprintf("%s.channels[%d]", base, j)
			ch, err := core.ParseChannel(name)
			if err != nil {
				errs = append(errs, p.errf(key, "%v", err))
				continue
			}
			if !core.ChannelValidFor(ch, mt) {
				errs = append(errs, p.errf(key, "channel %q does not exist on market_type %s", name, in.MarketType))
				continue
			}
			if slices.Contains(in.Chans, ch) {
				errs = append(errs, p.errf(key, "channel %q is listed twice", name))
				continue
			}
			in.Chans = append(in.Chans, ch)
		}

		for j, sym := range in.Symbols {
			key := fmt.Sprintf("%s.symbols[%d]", base, j)
			if !symbolRe.MatchString(sym) {
				errs = append(errs, p.errf(key, "symbol %q must match %s", sym, core.CanonicalPattern))
				continue
			}
			if slices.Index(in.Symbols, sym) != j {
				errs = append(errs, p.errf(key, "symbol %q is listed twice", sym))
			}
		}
	}
	return errs
}

// yamlTagName makes validator report the YAML key an operator wrote rather
// than the Go field name they have never seen.
func yamlTagName(f reflect.StructField) string {
	name, _, _ := strings.Cut(f.Tag.Get("yaml"), ",")
	if name == "" || name == "-" {
		return f.Name
	}
	return name
}

func describe(fe validator.FieldError) string {
	switch fe.Tag() {
	case "required":
		return "is required"
	case "oneof":
		return "must be one of: " + strings.ReplaceAll(fe.Param(), " ", ", ")
	case "eq":
		return fmt.Sprintf("must be %q, got %v", fe.Param(), fe.Value())
	case "gt":
		return fmt.Sprintf("must be greater than %s, got %v", fe.Param(), fe.Value())
	case "gte":
		return fmt.Sprintf("must be at least %s, got %v", fe.Param(), fe.Value())
	case "lte":
		return fmt.Sprintf("must be at most %s, got %v", fe.Param(), fe.Value())
	case "min":
		return fmt.Sprintf("must have at least %s entries", fe.Param())
	case "hostname_port":
		return fmt.Sprintf("must be host:port, got %v", fe.Value())
	case "uppercase":
		return fmt.Sprintf("must be upper case, got %v", fe.Value())
	default:
		return fmt.Sprintf("fails rule %q (got %v)", fe.Tag(), fe.Value())
	}
}
