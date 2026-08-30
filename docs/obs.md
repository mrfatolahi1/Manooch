Covers: M0 · `internal/obs`

## Purpose

The process logger and the full set of Prometheus collectors, on a private registry. Every metric the service will ever export is declared here at M0, including the ones that stay at zero until a later phase.

## Files

| Path | Holds |
|---|---|
| `internal/obs/logging.go` | `NewLogger`, `ParseLevel` |
| `internal/obs/metrics.go` | The `Metrics` struct, `NewMetrics`, `Handler`, `Registry`, and the stream-status gauge constants |

No test file.

## Key types and functions

| Symbol | Signature | Notes |
|---|---|---|
| `NewLogger` | `func(w io.Writer, level, venue string) (*slog.Logger, error)` | JSON handler, `.With("venue", venue)` so every line carries it |
| `ParseLevel` | `func(string) (slog.Level, error)` | `debug`/`info`/`warn`(or `warning`)/`error`, case-insensitive |
| `Metrics` | struct of `*prometheus.CounterVec`, `*GaugeVec`, `*HistogramVec` | |
| `NewMetrics` | `func() *Metrics` | Registers every collector on a fresh `prometheus.NewRegistry()` |
| `Metrics.Handler` | `func() http.Handler` | `promhttp.HandlerFor` over this registry, OpenMetrics enabled |
| `Metrics.Registry` | `func() *prometheus.Registry` | |
| `StreamStatusHealthy`, `StreamStatusDegraded`, `StreamStatusStale` | `= 1`, `2`, `3` | Values for the `StreamStatus` gauge |

### Collectors

| Field | Metric name | Labels | Written at M0 |
|---|---|---|---|
| `WSFramesReceived` | `manooch_ws_frames_received_total` | venue, market_type, channel | no |
| `ParseErrors` | `manooch_parse_errors_total` | venue, channel, kind | no |
| `RangeErrors` | `manooch_range_errors_total` | venue, channel | no |
| `MessagesPublished` | `manooch_messages_published_total` | venue, market_type, symbol, channel, source | yes, `publish.observe` |
| `PublishLatency` | `manooch_publish_latency_seconds` | venue, channel | yes, `publish.observe` |
| `InternalLatency` | `manooch_internal_latency_seconds` | venue, channel | yes, `publish.observe` |
| `RedisPublishErrors` | `manooch_redis_publish_errors_total` | venue | yes, `publish.onWriteError` |
| `ClockSkewMS` | `manooch_clock_skew_ms` | venue | no |
| `StreamStatus` | `manooch_stream_status` | venue, market_type, symbol, channel | no |
| `KeyExpired` | `manooch_key_expired_total` | venue, channel | no |
| `Reconnects` | `manooch_reconnects_total` | venue, socket | no |
| `StreamRestarts` | `manooch_stream_restarts_total` | venue, market_type, symbol, channel | no |
| `LeakedGoroutines` | `manooch_leaked_goroutines` | venue | yes, `cmd/manooch-feed` on shutdown timeout |
| `RateLimitUsed` | `manooch_rate_limit_used` | venue, kind | no |
| `RateLimitDenied` | `manooch_rate_limit_denied_total` | venue, kind | no |
| `FallbackActive` | `manooch_fallback_active` | venue, market_type, symbol, channel | no |
| `FallbackPolls` | `manooch_fallback_polls_total` | venue, channel, result | no |

The two latency histograms have different buckets on purpose: `PublishLatency` uses `ExponentialBuckets(0.0005, 2, 14)` (0.5ms to ~4s) because it spans venue network time; `InternalLatency` uses `ExponentialBuckets(0.00005, 2, 15)` (50µs upward) because it is our own overhead. Separate ranges are what makes "the venue is slow" distinguishable from "we are slow".

## How it is used

`cmd/manooch-feed.run` calls `NewLogger(os.Stdout, cfg.Service.LogLevel, cfg.Venue)` and `NewMetrics()`, passes both into `publish.Options`, and mounts `Metrics.Handler()` at `GET /metrics` via `newMux`. `internal/publish` is the only package that writes counters during normal operation.

## Rules

- **Keep the private registry; do not register the Go or process collectors and do not fall back to `prometheus.DefaultRegisterer`.** The default registry collects whatever any imported package registers into it, so a dependency can silently add series to the scrape.
- **Never log per message, at any level.** A book stream at 10 updates a second across 200 instruments is 2,000 lines a second — a load on the disk, and enough noise to bury the one line that mattered. Lifecycle events and status transitions are logged; data is not.
- **Add new collectors here rather than at the call site.** A metric registered where it is used gets a name chosen under time pressure and is invisible to anyone reading the exported set.
- **Watch label cardinality.** `MessagesPublished`, `StreamStatus`, `StreamRestarts` and `FallbackActive` carry `symbol`; that is bounded by the venue file today. A label taking an unbounded value — a trade id, an error string — would grow the series count without limit.

## Not here

- The HTTP routes that expose `/metrics` and the pprof handlers: `docs/cli.md`.
- Which values are actually recorded and when: `docs/publish.md`.
