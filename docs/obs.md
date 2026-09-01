Covers: M2 · `internal/obs`

The process logger and the full Prometheus collector set, on a private registry.

| File | Holds |
|---|---|
| `logging.go` | `NewLogger`, `ParseLevel` |
| `metrics.go` | `Metrics`, `NewMetrics`, `Handler`, `Registry`, stream-status constants |

No test file. `NewLogger` returns a JSON `slog.Logger` tagged with the venue. `NewMetrics` registers every collector on a fresh `prometheus.NewRegistry()`. Full API: `go doc ./internal/obs`.

All 17 collectors are declared up front, including those that stay at zero until a later phase.

| Collector | Written by |
|---|---|
| `messages_published_total`, `publish_latency_seconds`, `internal_latency_seconds`, `redis_publish_errors_total` | `internal/publish` |
| `ws_frames_received_total`, `parse_errors_total`, `range_errors_total` | `internal/supervisor`'s read loop |
| `stream_status`, `clock_skew_ms`, `key_expired_total`, `reconnects_total`, `stream_restarts_total`, `leaked_goroutines`, `fallback_active` | `internal/health`, from the state it already tracks |
| `fallback_polls_total` | `internal/fallback`, labelled `ok`, `error`, `empty` or `capacity` |
| `rate_limit_used`, `rate_limit_denied_total` | nothing yet — M3 |

`ws_frames_received_total` is counted once per **distinct channel** a frame produced messages on, so one Binance markPrice frame increments `mark_price`, `index_price` and `funding`. The label set is per stream, and per-stream liveness is what the counter is read for; a socket-wide total would hide one channel going quiet. Acks and pongs produce no messages and increment nothing.

`clock_skew_ms` is `exchange_time_ns - recv_time_ns`, sign kept: losing it would hide which way the two clocks disagree, and every freshness number depends on the answer.

The two latency histograms use different buckets on purpose: publish latency spans venue network time (`0.5ms` upward), internal latency is our own overhead (`50µs` upward). Sharing buckets would make "the venue is slow" indistinguishable from "we are slow".

## Rules

- **Keep the private registry.** The default registerer collects whatever any imported package registers into it, so a dependency can silently add series to the scrape. No Go or process collector either.
- **Never log per message, at any level.** Three channels at one update/second across 200 instruments is 600 lines/second, which buries the one line that mattered. Log lifecycle events and status transitions only. Where a per-frame path must say something — a parse failure — it is rate-limited to one line a second.
- **Declare new collectors here, not at the call site.** A metric registered where it is used gets named under time pressure and is invisible to anyone reading the exported set.
- **Watch label cardinality.** Several collectors carry `symbol`, bounded by the venue file. A label taking a trade id or an error string would grow the series count without limit.
