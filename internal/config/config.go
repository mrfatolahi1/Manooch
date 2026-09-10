// Package config loads and validates the service configuration.
//
// Unknown keys are a startup error, not a warning: a typo'd key that is
// silently ignored leaves the service on a default nobody chose. There is no
// reload — no watcher, no SIGHUP — because behaviour that changes under a
// running process cannot be reconstructed afterwards.
package config

import (
	"fmt"
	"time"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"gopkg.in/yaml.v3"
)

// A Duration is a time.Duration that reads and writes as "2s" in YAML.
type Duration time.Duration

// Standard returns the standard library duration.
func (duration Duration) Standard() time.Duration { return time.Duration(duration) }

// String renders the duration the way it is written in YAML.
func (duration Duration) String() string { return time.Duration(duration).String() }

// UnmarshalYAML parses a Go duration string such as "500ms".
func (duration *Duration) UnmarshalYAML(node *yaml.Node) error {
	var s string
	if err := node.Decode(&s); err != nil {
		return fmt.Errorf("line %d: duration must be a string like \"500ms\" or \"1h\"", node.Line)
	}
	v, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("line %d: invalid duration %q", node.Line, s)
	}
	*duration = Duration(v)
	return nil
}

// MarshalYAML writes the duration back as a string.
func (duration Duration) MarshalYAML() (any, error) { return time.Duration(duration).String(), nil }

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
	Database    int      `yaml:"db"           validate:"gte=0"`
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
	// TimeToLiveMultiplier scales a stream's cadence into its key TTL. Below 2 a
	// single late message expires the key and a healthy stream reads as stale.
	TimeToLiveMultiplier int   `yaml:"ttl_multiplier"         validate:"required,gte=2"`
	ClockSkewDegradedMS  int64 `yaml:"clock_skew_degraded_ms" validate:"required,gt=0"`
	ClockSkewStaleMS     int64 `yaml:"clock_skew_stale_ms"    validate:"required,gt=0"`
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

// MetadataConfig is the metadata section, read by internal/metadata.
type MetadataConfig struct {
	RefreshInterval Duration `yaml:"refresh_interval" validate:"required,gt=0"`
	StartupRequired bool     `yaml:"startup_required"`
	FetchTimeout    Duration `yaml:"fetch_timeout"    validate:"required,gt=0"`
}

// EndpointsConfig maps a market type name ("SPOT", "PERP_LINEAR") to a URL.
type EndpointsConfig struct {
	WebSocket map[string]string `yaml:"ws"   validate:"required,min=1"`
	REST      map[string]string `yaml:"rest" validate:"required,min=1"`
}

// RateLimitConfig is the rate_limit section, translated into the buckets
// internal/ratelimit enforces.
type RateLimitConfig struct {
	RESTWeightPerMinute int `yaml:"rest_weight_per_minute" validate:"required,gt=0"`
	// MaxWeightFraction is the share of the venue's published budget to use.
	// Never 1: the venue counts weight differently than we do.
	MaxWeightFraction          float64 `yaml:"max_weight_fraction"          validate:"required,gt=0,lte=1"`
	WebSocketConnectPer5Min    int     `yaml:"ws_connect_per_5min"          validate:"required,gt=0"`
	WebSocketConnectFraction   float64 `yaml:"ws_connect_fraction"          validate:"required,gt=0,lte=1"`
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

// QuirksConfig is the quirks section: per-venue behaviour the adapter must
// honour.
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
	ResolvedMarketType manoochv1.MarketType `yaml:"-"`
	ResolvedChannels   []manoochv1.Channel  `yaml:"-"`
}

// Cadence is how often the venue updates a channel, or zero when the venue file
// declares none. Load rejects a configured channel with no cadence, so zero
// only reaches a caller asking about a channel nobody subscribed to.
func (configuration *Config) Cadence(channel manoochv1.Channel) time.Duration {
	return configuration.Quirks.Cadence[core.ChannelName(channel)].Standard()
}

// TimeToLive is a channel's Redis key expiry: its cadence times
// health.ttl_multiplier. Key present means fresh, key absent means stale;
// there is no third state.
func (configuration *Config) TimeToLive(channel manoochv1.Channel) time.Duration {
	return configuration.Cadence(channel) * time.Duration(configuration.Health.TimeToLiveMultiplier)
}

// TimeToLiveByChannel is the time-to-live of every channel the venue declares a
// cadence for, which is what an adapter needs to stamp its messages.
func (configuration *Config) TimeToLiveByChannel() map[manoochv1.Channel]time.Duration {
	out := make(map[manoochv1.Channel]time.Duration, len(configuration.Quirks.Cadence))
	for name := range configuration.Quirks.Cadence {
		channel, err := core.ParseChannel(name)
		if err != nil {
			continue // Load already rejected it
		}
		out[channel] = configuration.TimeToLive(channel)
	}
	return out
}

// A Stream is one instrument on one channel: exactly one Redis key.
//
// There is no venue symbol here on purpose. Only the adapter knows what a venue
// calls an instrument, and a rule in this package would have to be right for
// every venue at once — which stopped being possible the moment a second one
// spelled bitcoin XBT.
type Stream struct {
	MarketType manoochv1.MarketType
	Symbol     string // canonical, "BTC_USDT"
	Channel    manoochv1.Channel
}

// Streams expands the instrument blocks into individual streams.
func (configuration *Config) Streams() []Stream {
	var out []Stream
	for _, in := range configuration.Instruments {
		for _, symbol := range in.Symbols {
			for _, channel := range in.ResolvedChannels {
				out = append(out, Stream{MarketType: in.ResolvedMarketType, Symbol: symbol, Channel: channel})
			}
		}
	}
	return out
}
