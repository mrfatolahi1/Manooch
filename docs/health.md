Covers: M3 · `internal/health`

What a consumer reads to decide whether to trust a price. Freshness itself is
the Redis key's TTL, so this package owns only what a TTL cannot express: fresh
but sourced from REST, socket reconnecting, clocks disagreeing.

| File | Holds |
|---|---|
| `health.go` | `Tracker`, `Options`, the event methods, status computation |
| `publish.go` | `Run`, the heartbeat, the health message and its TTL |
| `health_test.go` | One case per status transition |
| `publish_test.go` | Transition and heartbeat triggers, key TTL |
| `../fallback/integration_test.go` | Tag `integration`. The same, against real Redis |

## Freshness is TTL

`ttl = quirks.cadence[channel] × health.ttl_multiplier`, derived by `config.TTL`
and written with `PX` by `publish.RedisPublisher.Publish`. Key present means
fresh, key absent means stale — no second timestamp, because two mechanisms that
are supposed to agree about freshness eventually will not.

TTL alone is binary, so `status` and `status_reason` also ride inside the value.
`DEGRADED` — fresh, but from REST — is what a strategy needs while the data is
still inside its TTL.

## Status semantics

Tested in this order. `STALE` outranks `DEGRADED` throughout: the two differ by
whether a consumer may trade, so a stream that qualifies for both reports stop.

| Status | Cause | `status_reason` |
|---|---|---|
| `STALE` | Instrument metadata has never been fetched | `metadata unavailable` |
| `STALE` | Circuit breaker open on its socket | `circuit open` |
| `STALE` | Fallback failed, empty or capped | `rest poll failed`, `rest returned no value`, `fallback at capacity` |
| `STALE` | On fallback past `fallback.max_duration` | `rest fallback for 5m0s` |
| `STALE` | Skew ≥ `clock_skew_stale_ms` | `clock skew 20000ms` |
| `STALE` | Key expired with nothing serving it | `key expired` |
| `DEGRADED` | Served by REST fallback | `rest fallback` |
| `DEGRADED` | Socket dialing or backing off | the socket's own reason |
| `DEGRADED` | Skew ≥ `clock_skew_degraded_ms` | `clock skew 3000ms` |
| `DEGRADED` | A frame on its socket did not parse | `frame rejected` |
| `HEALTHY` | None of the above | empty |

Venue status adds `leaked goroutines: N`, which never makes an individual price
stale: a leak is a connection-level fact, not a wrong number.

`metadata unavailable` is tested before everything else, and with
`metadata.startup_required: true` it is where every stream starts. Nothing is
published while it holds — no socket is even dialled — because a price at
unknown precision is a number a consumer would size an order from and get
wrong. See [`metadata.md`](metadata.md).

## Key types and functions

| Symbol | What it does |
|---|---|
| `New(Options) (*Tracker, error)` | Builds the tracker; publishes nothing yet |
| `Options` | Venue, `publish.Publisher`, metrics, logger, heartbeat, skew thresholds, `FallbackMaxDuration`, `MetadataRequired`, `Now` |
| `Tracker.Register(spec, venueSymbol, socketID)` | Declares a stream and seeds its status |
| `Tracker.Specs()` | Every registered stream, for the fallback sweep |
| `Tracker.Run(ctx)` | The heartbeat loop |
| `Tracker.Status(spec)` | `(status, reason)` for one stream, stamped before each publish |
| `Tracker.VenueStatus()` | The connection-level answer |
| `Tracker.Received(spec)` / `Polled(spec)` | A websocket message arrived / a REST poll succeeded |
| `Tracker.KeyExpired(spec)` | Redis said the key reached its TTL |
| `Tracker.FrameRejected(socketID)` | A frame on that socket did not parse |
| `Tracker.StreamRestarted(spec)` | Tier-1 restart, counted |
| `Tracker.FallbackEngaged/Failed/Disengaged(spec, …)` | Fallback lifecycle |
| `Tracker.SocketState(id, state, reason)` | `SocketConnected`, `SocketDialing`, `SocketCircuitOpen` |
| `Tracker.Reconnected(id)` | Counts one completed reconnection |
| `Tracker.MetadataState(ok, reason)` | Whether the venue's instrument metadata has arrived |
| `Tracker.ClockSkew(ms)` | Signed: a venue clock ahead of ours is positive |
| `Tracker.Leaked(n)` | Goroutines that never came back |

## Publishing

| Key | Carries |
|---|---|
| `Manooch:{VENUE}:{MARKET_TYPE}:{SYMBOL}:health` | The worst of that instrument's channels, restart count, age, fallback flag |
| `Manooch:{VENUE}:venue:health` | Socket state, clock skew, reconnects, leaked goroutines |

Three triggers, all required: immediately on any transition, every
`health.heartbeat_interval` regardless, and into the last-value key so a cold
consumer reads current state without waiting. TTL is `heartbeat_interval × 3`.

Per-channel status is not lost to the instrument-level key: each data key's own
envelope carries its own, and `manooch_stream_status` exports it as 1/2/3.

## Rules

- **The heartbeat is not optional.** Pub/Sub is fire-and-forget, so without it
  "healthy and quiet" and "the health publisher is dead" are the same
  observation. Silence must never be ambiguous.
- **Health keys expire.** The health channel has to be detectably dead itself,
  or the last message ever published sits in Redis looking current forever.
- **The heartbeat recomputes every stream, not just changed ones.** Crossing
  `fallback.max_duration` is the passage of time and fires no event; only a tick
  notices it.
- **An unregistered stream is `STALE`.** The alternative publishes data as
  healthy on the strength of knowing nothing about it.
- **Clock skew comes only from a venue send time.** `internal/supervisor` filters
  on `exchange_time_is_send_time`; an event time — KuCoin stamps a funding rate
  with its settlement instant — would read as a four-hour skew and take a
  healthy venue `STALE` once a minute.
- **Publish transitions outside the mutex.** A Redis round trip under the lock
  puts every reporting goroutine behind the slowest write.

## Not here

Freshness timers (the TTL is the timer), venue rollup status, adaptive
thresholds, the restart tiers (`supervisor.md`), the REST poll (`fallback.md`).
