Covers: M3 · `internal/ratelimit`

Keeps the service inside a venue's published request budget. A denial is a
refusal, not a delay: a caller told no does not make the request, and the stream
it was for reports `DEGRADED` or `STALE`.

| File | Holds |
|---|---|
| `ratelimit.go` | `Limiter`, `LimitKind`, `Bucket`, `Unlimited`, `ErrBudgetExhausted` |
| `local.go` | `LocalLimiter`, the GCRA accounting, the advisory publication |
| `ratelimit_test.go` | Burst, refill, refusal, independence, headroom, publication |

## Key types and functions

| Symbol | What it does |
|---|---|
| `Limiter` | `Allow(ctx, venue, kind, cost)` and `Used(venue, kind)` |
| `LimitKind`, `Kinds` | `LimitRESTWeight`, `LimitWSConnect`, `LimitSubscriptions`, and the ordered set |
| `ErrBudgetExhausted` | Returned rather than waiting past the caller's deadline |
| `Bucket` | `Capacity` operations per `Window`; `Fraction(f)` scales it down |
| `New(Options)`, `Options` | The in-process limiter, one per venue process |
| `LocalLimiter.AttachPublisher(p)` | Supplies the publisher once Redis is dialled |
| `LocalLimiter.Snapshot()`, `.KindNames()` | Current usage; the budgeted kinds, for the startup log |
| `Unlimited` | Permits everything; what an adapter falls back to in a test |

## How it is used

`cmd/manooch-feed` builds one `LocalLimiter` per process from the venue file and
hands it to `adapter.New` through `adapter.Deps`. The adapters call `Allow`
themselves: only they know what a venue charges for a call.

| Operation | When | Kind | Venue |
|---|---|---|---|
| Websocket connect | Every dial | `LimitWSConnect` | Both |
| Subscribe frames, `bullet-public` | Every KuCoin dial | `LimitSubscriptions`, `LimitRESTWeight` (10) | KuCoin |
| Metadata fetch | Hourly | `LimitRESTWeight` | Both |
| Fallback poll | While a stream is stale | `LimitRESTWeight` | Both |

Steady state is near zero: a subscribed websocket makes no requests at all. **The
exposure is reconnects, not polling**, and the circuit breaker in
`internal/transport` does more to prevent a ban than this does.

Accounting is GCRA: one theoretical arrival time per bucket, not a counter and a
refill ticker, so there is no goroutine and no window boundary for a burst to
straddle. A cold bucket serves a whole capacity at once, which is what lets every
socket reconnect; each unit then returns one emission interval at a time. A
reserved slot is not handed back to a cancelled caller — that would let the
caller and its retry both spend it.

## Headroom

The limiter is blind to the order service, which shares this host's IP and
spends the same venue budget; coordinating would make that service depend on
this one. Instead every capacity is `Fraction`-scaled: `rest_weight_per_minute:
6000` with `max_weight_fraction: 0.5` means Manooch uses at most 3000 and leaves
the rest.

`LimitSubscriptions` is derived, not configured: `subscriptions_per_connection ×
the connect budget`, over the connect window. The venue's limit is per connection
— `PlanSubscriptions` already respects that — and a rate invented for it would be
a number nobody chose.

## Advisory publication

`Manooch:{VENUE}:venue:ratelimit` holds a `RateLimit` message with one
`RateLimitBudget` per kind, TTL twice the longest window, written on every
update. **Data on Redis, not a dependency**: the order service may read what
budget is left for its own calls, and nothing breaks if it never does. The
envelope reports `DEGRADED` while a bucket is at capacity; `manooch-status`
renders it `rest_weight=6/2000`.

## The Redis limiter, documented and not built

Two processes on one IP cannot coordinate through an in-process bucket. The fix
is shared GCRA state in Redis — as a **key convention**, never a shared library:
the order service must have no dependency on Manooch.

```
ratelimit:{VENUE}:{kind}    # GCRA state, one Lua script, atomic
```

One key per venue and kind holding the theoretical arrival time in milliseconds,
`kind` being what `LimitKind.String()` produces. A caller runs a Lua script that
reads it, adds `cost × emission interval`, and either accepts and writes it back
or reports how long to wait — inside the script, so two processes cannot
interleave. Either side implements that independently. Not built: one process per
venue is the current topology.

## Rules

- **Fail closed.** A denial means the operation does not happen and the stream it
  was for reports `DEGRADED` or `STALE`, never that we proceeded assuming it was
  probably fine.
- **Never use the whole published limit.** Another process on this IP spends the
  same budget and cannot be seen from here.
- **Never invent a limit the venue does not publish.** An unbudgeted kind is
  allowed and reports `0/0`, which is how a caller tells it from an empty bucket.
- **Budget before the call**, or the refusal is accounting rather than limiting.

## Not here

Reconnect policy and the circuit breaker (`transport.md`), what a denial does to a
stream (`health.md`, `fallback.md`), per-call weights (`adapter-binance.md`,
`adapter-kucoin.md`).
