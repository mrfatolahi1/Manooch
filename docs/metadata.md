Covers: M3 · `internal/metadata`

Instrument definitions — tick size, lot size, minimum notional, contract
multiplier — kept in Redis beside the prices. A price with unknown precision is
a price nobody can size an order against, and a missing contract multiplier is a
silently wrong order size on every venue that trades in contracts.

| File | Holds |
|---|---|
| `metadata.go` | `Refresher`, `Options`, `Reporter`, the refresh cycle and the change log |
| `metadata_test.go` | Startup dependency, change logging, unlisted instruments |
| `../adapter/binance/metadata.go` | `GET /fapi/v1/exchangeInfo` |
| `../adapter/kucoin/metadata.go` | `GET /api/v1/contracts/active` |

## Key types and functions

| Symbol | What it does |
|---|---|
| `New(Options) (*Refresher, error)` | Builds the refresher; fetches nothing yet |
| `Options` | Venue, adapter, publisher, `Reporter`, instruments, market type, interval, fetch timeout, `Required`, backoff |
| `Reporter` | The one thing this package says to health: `MetadataState(ok, reason)` |
| `Refresher.Run(ctx)` | Initial fetch on backoff, then the interval poll |
| `Refresher.Ready()` | Closes once the first fetch has succeeded |
| `Refresher.WaitReady(ctx)` | Blocks until metadata has arrived, or returns at once when it is not required |

## How it is used

`cmd/manooch-feed` builds the refresher beside the health tracker, starts both,
and parks the supervisor and the fallback watcher behind `WaitReady`. Health is
running first on purpose: it is what publishes `STALE` while the fetch is still
failing.

```
Manooch:{VENUE}:{MARKET_TYPE}:{SYMBOL}:metadata     TTL = refresh_interval × 2
```

The whole configured set is republished every cycle, not only what changed:
Pub/Sub is fire-and-forget, so a consumer that missed the one message announcing
a tick size change would never hear about it again.

## Startup dependency

With `metadata.startup_required: true`, the venue comes up `STALE` with
`status_reason: "metadata unavailable"` and **publishes no market data at all** —
no socket is dialled. The initial fetch retries on the socket reconnect backoff
and reports `STALE` throughout.

A later failure is different: the prices are still arriving and last cycle's
metadata is still inside its TTL, so it is logged at `ERROR` and retried on the
next tick. Nothing is republished from a fetch that did not happen — resetting a
key's TTL would claim a freshness nobody has. Two missed cycles and the key
expires, which is the signal.

## Change logging

On each refresh both the old and new values are in hand at zero cost. A change
to `tick_size`, `lot_size`, `min_notional`, `contract_multiplier` or `active`
logs at `WARN` with both values. Exchanges change these without warning and
announce it nowhere a program can read; this line is the only record that it
happened.

**A tick size change does not reinterpret anything already published.** Scaling
is global — `1e-11` for price, `1e-8` for size — so tick size is a fact about the
instrument rather than the exponent anything was encoded at. That is the whole
reason the global scale was chosen over a per-instrument exponent: a venue
moving a tick would otherwise silently change the meaning of every cached value.

## What each venue supplies

| Field | Binance | KuCoin |
|---|---|---|
| `tick_size` | `PRICE_FILTER.tickSize` | `tickSize` |
| `lot_size` | `LOT_SIZE.stepSize` | `lotSize`, in contracts |
| `min_size` | `LOT_SIZE.minQty` | `lotSize` |
| `max_size` | `LOT_SIZE.maxQty` | `maxOrderQty` |
| `min_notional` | `MIN_NOTIONAL.notional` | **0** — not published |
| `contract_multiplier` | **1**, stated not assumed | `multiplier` |
| `active` | `status == "TRADING"` | `status == "Open"` |
| `exchange_time_ns` | `serverTime` | **0** — no server time |

## Rules

- **Never start streaming without metadata.** A price at unknown precision is a
  number a consumer would size an order from and get wrong.
- **Never republish a cycle that failed.** The key expiring is the honest
  signal; a refreshed TTL on stale values is a claim nobody can check.
- **Publish only the configured instruments.** A venue's whole contract list is
  hundreds of keys nobody subscribed to.
- **A configured instrument the venue does not list is a `WARN`.** That stream
  will never produce data, and nothing else in the service would say so.
- **Interval poll only.** No diff events, no on-demand trigger: the venue
  publishes no change feed, and a second answer to "when did this change" would
  eventually disagree with the first.

## Not here

The venue calls themselves (`adapter-binance.md`, `adapter-kucoin.md`), what a
`REST` weight costs (`ratelimit.md`), how `STALE` reaches a consumer
(`health.md`).
