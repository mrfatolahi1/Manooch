package obs

import (
	"net/http"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics is the complete set of collectors Manooch exports.
//
// All of them are declared here in M0, including the ones that stay at zero
// until the phase that populates them. Naming a metric is a decision about
// what an operator will look at during an incident, and it is better made once
// than reinvented in five packages under time pressure.
//
// They live on a private registry rather than the default one. Nothing is
// registered that was not asked for: no Go runtime collector, no process
// collector, no metric that arrives because a dependency imported a package.
type Metrics struct {
	reg *prometheus.Registry

	// Ingest.
	WSFramesReceived *prometheus.CounterVec // venue, market_type, channel
	ParseErrors      *prometheus.CounterVec // venue, channel, kind
	RangeErrors      *prometheus.CounterVec // venue, channel

	// Publish.
	MessagesPublished  *prometheus.CounterVec   // venue, market_type, symbol, channel, source
	PublishLatency     *prometheus.HistogramVec // venue, channel — exchange time to publish time
	InternalLatency    *prometheus.HistogramVec // venue, channel — receive time to publish time
	RedisPublishErrors *prometheus.CounterVec   // venue

	// Health and liveness.
	ClockSkewMS      *prometheus.GaugeVec   // venue
	StreamStatus     *prometheus.GaugeVec   // venue, market_type, symbol, channel — 1 healthy, 2 degraded, 3 stale
	KeyExpired       *prometheus.CounterVec // venue, channel
	Reconnects       *prometheus.CounterVec // venue, socket
	StreamRestarts   *prometheus.CounterVec // venue, market_type, symbol, channel
	LeakedGoroutines *prometheus.GaugeVec   // venue

	// Rate limiting.
	RateLimitUsed   *prometheus.GaugeVec   // venue, kind
	RateLimitDenied *prometheus.CounterVec // venue, kind

	// REST fallback.
	FallbackActive *prometheus.GaugeVec   // venue, market_type, symbol, channel — 0 or 1
	FallbackPolls  *prometheus.CounterVec // venue, channel, result
}

// Stream status gauge values. Numeric so that alerting can threshold on them.
const (
	StreamStatusHealthy  = 1
	StreamStatusDegraded = 2
	StreamStatusStale    = 3
)

// NewMetrics registers every collector on a fresh registry.
func NewMetrics() *Metrics {
	reg := prometheus.NewRegistry()
	m := &Metrics{reg: reg}

	counter := func(name, help string, labels ...string) *prometheus.CounterVec {
		c := prometheus.NewCounterVec(prometheus.CounterOpts{Name: name, Help: help}, labels)
		reg.MustRegister(c)
		return c
	}
	gauge := func(name, help string, labels ...string) *prometheus.GaugeVec {
		g := prometheus.NewGaugeVec(prometheus.GaugeOpts{Name: name, Help: help}, labels)
		reg.MustRegister(g)
		return g
	}
	histogram := func(name, help string, buckets []float64, labels ...string) *prometheus.HistogramVec {
		h := prometheus.NewHistogramVec(prometheus.HistogramOpts{Name: name, Help: help, Buckets: buckets}, labels)
		reg.MustRegister(h)
		return h
	}

	// End-to-end latency spans venue network time, so it needs a wide range.
	// Internal latency is our own overhead and should be microseconds; giving
	// it its own buckets is what makes "the venue is slow" distinguishable
	// from "we are slow".
	publishBuckets := prometheus.ExponentialBuckets(0.0005, 2, 14) // 0.5ms .. ~4s
	internalBuckets := prometheus.ExponentialBuckets(0.00005, 2, 15)

	m.WSFramesReceived = counter("manooch_ws_frames_received_total",
		"Websocket frames received from the venue.", "venue", "market_type", "channel")
	m.ParseErrors = counter("manooch_parse_errors_total",
		"Venue messages that could not be parsed, by kind.", "venue", "channel", "kind")
	m.RangeErrors = counter("manooch_range_errors_total",
		"Numeric values rejected for not fitting the fixed-point scale.", "venue", "channel")

	m.MessagesPublished = counter("manooch_messages_published_total",
		"Messages written to Redis.", "venue", "market_type", "symbol", "channel", "source")
	m.PublishLatency = histogram("manooch_publish_latency_seconds",
		"Exchange timestamp to publish timestamp.", publishBuckets, "venue", "channel")
	m.InternalLatency = histogram("manooch_internal_latency_seconds",
		"Receive timestamp to publish timestamp.", internalBuckets, "venue", "channel")
	m.RedisPublishErrors = counter("manooch_redis_publish_errors_total",
		"Failed Redis writes.", "venue")

	m.ClockSkewMS = gauge("manooch_clock_skew_ms",
		"Estimated clock skew against the venue, in milliseconds.", "venue")
	m.StreamStatus = gauge("manooch_stream_status",
		"Stream status: 1 healthy, 2 degraded, 3 stale.", "venue", "market_type", "symbol", "channel")
	m.KeyExpired = counter("manooch_key_expired_total",
		"Redis keys that reached their TTL, meaning the stream went quiet.", "venue", "channel")
	m.Reconnects = counter("manooch_reconnects_total",
		"Websocket reconnections.", "venue", "socket")
	m.StreamRestarts = counter("manooch_stream_restarts_total",
		"Individual stream restarts.", "venue", "market_type", "symbol", "channel")
	m.LeakedGoroutines = gauge("manooch_leaked_goroutines",
		"Goroutines that did not exit within the shutdown deadline.", "venue")

	m.RateLimitUsed = gauge("manooch_rate_limit_used",
		"Fraction of the venue rate-limit budget in use.", "venue", "kind")
	m.RateLimitDenied = counter("manooch_rate_limit_denied_total",
		"Requests we declined to make to stay inside the budget.", "venue", "kind")

	m.FallbackActive = gauge("manooch_fallback_active",
		"1 while a stream is being served by REST fallback rather than its socket.",
		"venue", "market_type", "symbol", "channel")
	m.FallbackPolls = counter("manooch_fallback_polls_total",
		"REST fallback polls, by result.", "venue", "channel", "result")

	return m
}

// Registry exposes the underlying registry, for tests and for the HTTP handler.
func (m *Metrics) Registry() *prometheus.Registry { return m.reg }

// Handler serves the Prometheus exposition format for this registry only.
func (m *Metrics) Handler() http.Handler {
	return promhttp.HandlerFor(m.reg, promhttp.HandlerOpts{
		Registry:          m.reg,
		ErrorHandling:     promhttp.ContinueOnError,
		EnableOpenMetrics: true,
	})
}
