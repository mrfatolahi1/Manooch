// Package config loads and validates the service configuration.
//
// Unknown keys are a startup error, not a warning: a typo'd key that is
// silently ignored leaves the service on a default nobody chose. There is no
// reload — no watcher, no SIGHUP — because behaviour that changes under a
// running process cannot be reconstructed afterwards.
package config

import (
	"fmt"
	"strings"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"gopkg.in/yaml.v3"
)

// A Duration is a time.Duration that reads and writes as "2s" in YAML.
type Duration time.Duration

// Std returns the standard library duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

// String renders the duration the way it is written in YAML.
func (d Duration) String() string { return time.Duration(d).String() }

// UnmarshalYAML parses a Go duration string such as "500ms".
func (d *Duration) UnmarshalYAML(n *yaml.Node) error {
	var s string
	if err := n.Decode(&s); err != nil {
		return fmt.Errorf("line %d: duration must be a string like \"500ms\" or \"1h\"", n.Line)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("line %d: invalid duration %q", n.Line, s)
	}
	*d = Duration(v)
	return nil
}

// MarshalYAML writes the duration back as a string.
func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

// Config is defaults.yaml overlaid with one venue file. Any key the venue file
// sets wins.
type Config struct {
	// From defaults.yaml.
	Service    ServiceConfig    `yaml:"service"`
	Redis      RedisConfig      `yaml:"redis"`
	Scales     ScalesConfig     `yaml:"scales"`
	Publish    PublishConfig    `yaml:"publish"`
	Health     HealthConfig     `yaml:"health"`
	Fallback   FallbackConfig   `yaml:"fallback"`
	Supervisor SupervisorConfig `yaml:"supervisor"`
	Metadata   MetadataConfig   `yaml:"metadata"`

	// From the venue file.
	Venue           string             `yaml:"venue"            validate:"required,uppercase"`
	Enabled         bool               `yaml:"enabled"`
	Endpoints       EndpointsConfig    `yaml:"endpoints"`
	RateLimit       RateLimitConfig    `yaml:"rate_limit"`
	Connection      ConnectionConfig   `yaml:"connection"`
	Quirks          QuirksConfig       `yaml:"quirks"`
	SymbolOverrides map[string]string  `yaml:"symbol_overrides"`
	Instruments     []InstrumentConfig `yaml:"instruments" validate:"required,min=1,dive"`
}

// ServiceConfig is the service section.
type ServiceConfig struct {
	LogLevel string     `yaml:"log_level" validate:"required,oneof=debug info warn error"`
	HTTP     HTTPConfig `yaml:"http"`
}

// HTTPConfig is the service.http section: the admin surface.
type HTTPConfig struct {
	Enabled bool `yaml:"enabled"`
	// Listen must be a loopback address: /debug/pprof will hand a heap dump to
	// anyone who can reach it.
	Listen string `yaml:"listen" validate:"required,hostname_port"`
}

// RedisConfig is the redis section.
type RedisConfig struct {
	Addr        string   `yaml:"addr"         validate:"required,hostname_port"`
	DB          int      `yaml:"db"           validate:"gte=0"`
	DialTimeout Duration `yaml:"dial_timeout" validate:"required,gt=0"`
	ReadTimeout Duration `yaml:"read_timeout" validate:"required,gt=0"`
	PoolSize    int      `yaml:"pool_size"    validate:"required,gte=1"`
}

// ScalesConfig restates the fixed-point scales. Load checks them against
// pkg/price: a disagreement is a silent power-of-ten error in every number.
type ScalesConfig struct {
	PriceExp int `yaml:"price_exp"`
	SizeExp  int `yaml:"size_exp"`
	RateExp  int `yaml:"rate_exp"`
}

// PublishConfig is the publish section.
type PublishConfig struct {
	SchemaVersion uint32 `yaml:"schema_version" validate:"required,gte=1"`
	Cadence       string `yaml:"cadence"        validate:"required,eq=every_update"`
}

// HealthConfig is the health section.
type HealthConfig struct {
	HeartbeatInterval Duration `yaml:"heartbeat_interval" validate:"required,gt=0"`
	// TTLMultiplier scales a stream's cadence into its key TTL. Below 2 a
	// single late message expires the key and a healthy stream reads as stale.
	TTLMultiplier       int   `yaml:"ttl_multiplier"         validate:"required,gte=2"`
	ClockSkewDegradedMS int64 `yaml:"clock_skew_degraded_ms" validate:"required,gt=0"`
	ClockSkewStaleMS    int64 `yaml:"clock_skew_stale_ms"    validate:"required,gt=0"`
}

// FallbackConfig is the fallback section.
type FallbackConfig struct {
	Enabled            bool     `yaml:"enabled"`
	MaxConcurrentPolls int      `yaml:"max_concurrent_polls" validate:"required,gte=1"`
	PollInterval       Duration `yaml:"poll_interval"        validate:"required,gt=0"`
	SweepInterval      Duration `yaml:"sweep_interval"       validate:"required,gt=0"`
	MaxDuration        Duration `yaml:"max_duration"         validate:"required,gt=0"`
}

// SupervisorConfig is the supervisor section.
type SupervisorConfig struct {
	StreamRestartBackoff   BackoffConfig        `yaml:"stream_restart_backoff"`
	SocketReconnectBackoff BackoffConfig        `yaml:"socket_reconnect_backoff"`
	CircuitBreaker         CircuitBreakerConfig `yaml:"circuit_breaker"`
	GoroutineLeakTimeout   Duration             `yaml:"goroutine_leak_timeout" validate:"required,gt=0"`
}

// BackoffConfig is one backoff block under supervisor.
type BackoffConfig struct {
	Initial    Duration `yaml:"initial"    validate:"required,gt=0"`
	Max        Duration `yaml:"max"        validate:"required,gt=0"`
	Multiplier float64  `yaml:"multiplier" validate:"required,gt=1"`
	Jitter     string   `yaml:"jitter"     validate:"required,oneof=none full equal"`
}

// CircuitBreakerConfig is the supervisor.circuit_breaker section.
type CircuitBreakerConfig struct {
	ConsecutiveFailures int      `yaml:"consecutive_failures" validate:"required,gte=1"`
	OpenDuration        Duration `yaml:"open_duration"        validate:"required,gt=0"`
}

// MetadataConfig is the metadata section. Parsed and validated; unread until M3.
type MetadataConfig struct {
	RefreshInterval Duration `yaml:"refresh_interval" validate:"required,gt=0"`
	StartupRequired bool     `yaml:"startup_required"`
	FetchTimeout    Duration `yaml:"fetch_timeout"    validate:"required,gt=0"`
}

// EndpointsConfig maps a market type name ("SPOT", "PERP_LINEAR") to a URL.
type EndpointsConfig struct {
	WS   map[string]string `yaml:"ws"   validate:"required,min=1"`
	REST map[string]string `yaml:"rest" validate:"required,min=1"`
}

// RateLimitConfig is the rate_limit section. Parsed and validated; the rate
// limiter that reads it is M3.
type RateLimitConfig struct {
	RESTWeightPerMinute int `yaml:"rest_weight_per_minute" validate:"required,gt=0"`
	// MaxWeightFraction is the share of the venue's published budget to use.
	// Never 1: the venue counts weight differently than we do.
	MaxWeightFraction          float64 `yaml:"max_weight_fraction"          validate:"required,gt=0,lte=1"`
	WSConnectPer5Min           int     `yaml:"ws_connect_per_5min"          validate:"required,gt=0"`
	WSConnectFraction          float64 `yaml:"ws_connect_fraction"          validate:"required,gt=0,lte=1"`
	SubscriptionsPerConnection int     `yaml:"subscriptions_per_connection" validate:"required,gt=0"`
}

// ConnectionConfig is the connection section.
type ConnectionConfig struct {
	MaxStreamsPerSocket int      `yaml:"max_streams_per_socket" validate:"required,gt=0"`
	PingInterval        Duration `yaml:"ping_interval"          validate:"required,gt=0"`
	PongTimeout         Duration `yaml:"pong_timeout"           validate:"required,gt=0"`
	ReadTimeout         Duration `yaml:"read_timeout"           validate:"required,gt=0"`
	// MaxAge is how old a connection may get before it is redialled on
	// purpose. Venues drop long-lived sockets on a schedule of their own —
	// Binance at 24 hours — and a planned redial is a clean handover where an
	// unplanned one is a gap.
	MaxAge Duration `yaml:"max_age" validate:"required,gt=0"`
}

// QuirksConfig is the quirks section: per-venue behaviour the adapter must honour.
type QuirksConfig struct {
	TimestampUnit string `yaml:"timestamp_unit" validate:"required,oneof=ms us ns s"`
	// Cadence is how often the venue updates each channel, keyed by channel
	// name. It is per channel rather than per venue because KuCoin pushes
	// funding once a minute and mark price once a second: one number would
	// make the slower channel's key expire between updates.
	Cadence map[string]Duration `yaml:"cadence" validate:"min=1"`
}

// InstrumentConfig is one block of instruments sharing a market type.
type InstrumentConfig struct {
	MarketType string   `yaml:"market_type" validate:"required"`
	Channels   []string `yaml:"channels"    validate:"required,min=1"`
	Symbols    []string `yaml:"symbols"     validate:"required,min=1"`

	// Resolved by Load once the strings above have been checked.
	MT    pb.MarketType `yaml:"-"`
	Chans []pb.Channel  `yaml:"-"`
}

// VenueSymbol maps a canonical symbol to what this venue calls it, falling back
// to the concatenation most venues use.
func (c *Config) VenueSymbol(canonical string) string {
	if s, ok := c.SymbolOverrides[canonical]; ok {
		return s
	}
	return strings.ReplaceAll(canonical, "_", "")
}

// Cadence is how often the venue updates a channel, or zero when the venue file
// declares none. Load rejects a configured channel with no cadence, so zero
// only reaches a caller asking about a channel nobody subscribed to.
func (c *Config) Cadence(ch pb.Channel) time.Duration {
	return c.Quirks.Cadence[core.ChannelName(ch)].Std()
}

// TTL is a channel's Redis key TTL: its cadence times health.ttl_multiplier.
// Key present means fresh, key absent means stale; there is no third state.
func (c *Config) TTL(ch pb.Channel) time.Duration {
	return c.Cadence(ch) * time.Duration(c.Health.TTLMultiplier)
}

// TTLs is the TTL of every channel the venue declares a cadence for, which is
// what an adapter needs to stamp its messages.
func (c *Config) TTLs() map[pb.Channel]time.Duration {
	out := make(map[pb.Channel]time.Duration, len(c.Quirks.Cadence))
	for name := range c.Quirks.Cadence {
		ch, err := core.ParseChannel(name)
		if err != nil {
			continue // Load already rejected it
		}
		out[ch] = c.TTL(ch)
	}
	return out
}

// A Stream is one instrument on one channel: exactly one Redis key.
type Stream struct {
	MarketType  pb.MarketType
	Symbol      string // canonical, "BTC_USDT"
	VenueSymbol string // what this venue calls it, "BTCUSDT"
	Channel     pb.Channel
}

// Streams expands the instrument blocks into individual streams. Both
// --validate and the synthetic generator walk it.
func (c *Config) Streams() []Stream {
	var out []Stream
	for _, in := range c.Instruments {
		for _, sym := range in.Symbols {
			for _, ch := range in.Chans {
				out = append(out, Stream{
					MarketType:  in.MT,
					Symbol:      sym,
					VenueSymbol: c.VenueSymbol(sym),
					Channel:     ch,
				})
			}
		}
	}
	return out
}
