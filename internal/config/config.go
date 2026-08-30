// Package config loads and validates Manooch's configuration.
//
// Two properties are deliberate. Unknown keys are a startup error rather than
// a warning, because a typo'd key that is silently ignored means the service
// runs with a default nobody chose. And there is no reload: no watcher, no
// SIGHUP. A price feed whose behaviour changes underneath a running process is
// a feed whose behaviour at any past moment cannot be reconstructed. To change
// config, restart.
package config

import (
	"fmt"
	"strings"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"gopkg.in/yaml.v3"
)

// A Duration is a time.Duration that reads and writes as "2s" in YAML.
type Duration time.Duration

// Std returns the standard library duration.
func (d Duration) Std() time.Duration { return time.Duration(d) }

func (d Duration) String() string { return time.Duration(d).String() }

// UnmarshalYAML parses a Go duration string.
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

// MarshalYAML keeps --validate output readable.
func (d Duration) MarshalYAML() (any, error) { return time.Duration(d).String(), nil }

// Config is the fully resolved configuration: defaults.yaml overlaid with one
// venue file. The two files have disjoint sections in practice, but any key
// the venue file sets wins.
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

type ServiceConfig struct {
	LogLevel string     `yaml:"log_level" validate:"required,oneof=debug info warn error"`
	HTTP     HTTPConfig `yaml:"http"`
}

type HTTPConfig struct {
	Enabled bool `yaml:"enabled"`
	// Listen must be a loopback address. /metrics and /debug/pprof are not
	// endpoints to put on an interface anyone else can reach.
	Listen string `yaml:"listen" validate:"required,hostname_port"`
}

type RedisConfig struct {
	Addr        string   `yaml:"addr"         validate:"required,hostname_port"`
	DB          int      `yaml:"db"           validate:"gte=0"`
	DialTimeout Duration `yaml:"dial_timeout" validate:"required,gt=0"`
	ReadTimeout Duration `yaml:"read_timeout" validate:"required,gt=0"`
	PoolSize    int      `yaml:"pool_size"    validate:"required,gte=1"`
}

// ScalesConfig restates the fixed-point scales. They are checked against the
// constants compiled into pkg/price: config and code disagreeing about where
// the decimal point sits is a silent factor-of-1000 error in every price.
type ScalesConfig struct {
	PriceExp int `yaml:"price_exp"`
	SizeExp  int `yaml:"size_exp"`
	RateExp  int `yaml:"rate_exp"`
}

type PublishConfig struct {
	SchemaVersion uint32 `yaml:"schema_version" validate:"required,gte=1"`
	Cadence       string `yaml:"cadence"        validate:"required,eq=every_update"`
}

type HealthConfig struct {
	HeartbeatInterval Duration `yaml:"heartbeat_interval" validate:"required,gt=0"`
	// TTLMultiplier scales a stream's cadence into its Redis key TTL. Below 2
	// a single late message expires the key and a healthy stream reads as
	// stale, so 2 is the floor.
	TTLMultiplier       int   `yaml:"ttl_multiplier"         validate:"required,gte=2"`
	ClockSkewDegradedMS int64 `yaml:"clock_skew_degraded_ms" validate:"required,gt=0"`
	ClockSkewStaleMS    int64 `yaml:"clock_skew_stale_ms"    validate:"required,gt=0"`
}

type FallbackConfig struct {
	Enabled            bool     `yaml:"enabled"`
	MaxConcurrentPolls int      `yaml:"max_concurrent_polls" validate:"required,gte=1"`
	PollInterval       Duration `yaml:"poll_interval"        validate:"required,gt=0"`
	SweepInterval      Duration `yaml:"sweep_interval"       validate:"required,gt=0"`
	MaxDuration        Duration `yaml:"max_duration"         validate:"required,gt=0"`
}

type SupervisorConfig struct {
	StreamRestartBackoff   BackoffConfig        `yaml:"stream_restart_backoff"`
	SocketReconnectBackoff BackoffConfig        `yaml:"socket_reconnect_backoff"`
	CircuitBreaker         CircuitBreakerConfig `yaml:"circuit_breaker"`
	GoroutineLeakTimeout   Duration             `yaml:"goroutine_leak_timeout" validate:"required,gt=0"`
}

type BackoffConfig struct {
	Initial    Duration `yaml:"initial"    validate:"required,gt=0"`
	Max        Duration `yaml:"max"        validate:"required,gt=0"`
	Multiplier float64  `yaml:"multiplier" validate:"required,gt=1"`
	Jitter     string   `yaml:"jitter"     validate:"required,oneof=none full equal"`
}

type CircuitBreakerConfig struct {
	ConsecutiveFailures int      `yaml:"consecutive_failures" validate:"required,gte=1"`
	OpenDuration        Duration `yaml:"open_duration"        validate:"required,gt=0"`
}

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

type RateLimitConfig struct {
	RESTWeightPerMinute int `yaml:"rest_weight_per_minute" validate:"required,gt=0"`
	// MaxWeightFraction is the share of the venue's published budget we allow
	// ourselves. Never 100%: the venue counts differently than we do.
	MaxWeightFraction          float64 `yaml:"max_weight_fraction"          validate:"required,gt=0,lte=1"`
	WSConnectPer5Min           int     `yaml:"ws_connect_per_5min"          validate:"required,gt=0"`
	WSConnectFraction          float64 `yaml:"ws_connect_fraction"          validate:"required,gt=0,lte=1"`
	SubscriptionsPerConnection int     `yaml:"subscriptions_per_connection" validate:"required,gt=0"`
}

type ConnectionConfig struct {
	MaxStreamsPerSocket int      `yaml:"max_streams_per_socket" validate:"required,gt=0"`
	PingInterval        Duration `yaml:"ping_interval"          validate:"required,gt=0"`
	PongTimeout         Duration `yaml:"pong_timeout"           validate:"required,gt=0"`
	ReadTimeout         Duration `yaml:"read_timeout"           validate:"required,gt=0"`
}

type QuirksConfig struct {
	TimestampUnit       string   `yaml:"timestamp_unit"        validate:"required,oneof=ms us ns s"`
	BookDepthsSupported []uint32 `yaml:"book_depths_supported" validate:"required,min=1,dive,gt=0"`
	BookCadenceMS       int      `yaml:"book_cadence_ms"       validate:"required,gt=0"`
}

// InstrumentConfig is one block of instruments sharing a market type.
type InstrumentConfig struct {
	MarketType string   `yaml:"market_type" validate:"required"`
	BookDepth  uint32   `yaml:"book_depth"  validate:"required,gt=0"`
	Channels   []string `yaml:"channels"    validate:"required,min=1"`
	Symbols    []string `yaml:"symbols"     validate:"required,min=1"`

	// Resolved by Load once the strings above have been checked.
	MT    pb.MarketType `yaml:"-"`
	Chans []pb.Channel  `yaml:"-"`
}

// VenueSymbol maps a canonical symbol to what this venue calls it, falling
// back to the concatenation most venues use.
func (c *Config) VenueSymbol(canonical string) string {
	if s, ok := c.SymbolOverrides[canonical]; ok {
		return s
	}
	return strings.ReplaceAll(canonical, "_", "")
}

// BookCadence is how often the venue republishes a book snapshot.
func (c *Config) BookCadence() time.Duration {
	return time.Duration(c.Quirks.BookCadenceMS) * time.Millisecond
}

// TTL turns a stream cadence into the Redis key TTL for that stream. Key
// present means fresh, key absent means stale — there is no third state and no
// separate timestamp to fall out of sync.
func (c *Config) TTL(cadence time.Duration) time.Duration {
	return cadence * time.Duration(c.Health.TTLMultiplier)
}
