Covers: M0 · `internal/obs`

The process logger and the full Prometheus collector set, on a private registry.

| File | Holds |
|---|---|
| `logging.go` | `NewLogger`, `ParseLevel` |
| `metrics.go` | `Metrics`, `NewMetrics`, `Handler`, `Registry`, stream-status constants |

No test file. `NewLogger` returns a JSON `slog.Logger` tagged with the venue. `NewMetrics` registers every collector on a fresh `prometheus.NewRegistry()`. Full API: `go doc ./internal/obs`.

All 17 collectors are declared at M0, including those that stay at zero until a later phase. Only four are written today, all by `internal/publish`: `manooch_messages_published_total`, `manooch_publish_latency_seconds`, `manooch_internal_latency_seconds`, `manooch_redis_publish_errors_total`. `cmd/manooch-feed` sets `manooch_leaked_goroutines` when shutdown exceeds its deadline. The rest — websocket frames, parse and range errors, clock skew, stream status, key expiry, reconnects, restarts, rate limits, fallback — belong to M1–M3.

The two latency histograms use different buckets on purpose: publish latency spans venue network time (`0.5ms` upward), internal latency is our own overhead (`50µs` upward). Sharing buckets would make "the venue is slow" indistinguishable from "we are slow".

## Rules

- **Keep the private registry.** The default registerer collects whatever any imported package registers into it, so a dependency can silently add series to the scrape. No Go or process collector either.
- **Never log per message, at any level.** A book stream at 10 updates/second across 200 instruments is 2,000 lines/second, which buries the one line that mattered. Log lifecycle events and status transitions only.
- **Declare new collectors here, not at the call site.** A metric registered where it is used gets named under time pressure and is invisible to anyone reading the exported set.
- **Watch label cardinality.** Several collectors carry `symbol`, bounded by the venue file. A label taking a trade id or an error string would grow the series count without limit.
