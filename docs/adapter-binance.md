Covers: M3 · `internal/adapter/binance`

Binance USD-M futures over public unauthenticated websockets. Perpetual linear
only. **No credential, no signature, no authenticated stream, no order path.**

| File | Holds |
|---|---|
| `binance.go` | `New`, `Options`, symbol mapping, `PlanSubscriptions`, `SocketURL`, `Dial`, `RESTCost` |
| `parse.go` | `Parse` and the payload structs |
| `rest.go` | `FetchOnce` against `/fapi/v1/premiumIndex`, and the shared `get` |
| `metadata.go` | `FetchMetadata` against `/fapi/v1/exchangeInfo` |
| `binance_test.go` | Conformance, symbol round trip, planning, REST |
| `determinism_test.go` | 1000 parses per fixture |
| `smoke_test.go` | Live venue, build tag `smoke` |
| `testdata/binance/` | One raw frame per case, beside its `.golden` |

## Key types and functions

| Symbol | What it does |
|---|---|
| `Venue` | `"BINANCE"` |
| `MarketType` | `PERP_LINEAR`, the only market served |
| `Channels` | The three a frame produces, in the order `Parse` emits them |
| `Options` | Endpoints, overrides, per-socket limit, read timeout, per-channel TTLs, limiter, dialer |
| `New(Options)` | Builds the adapter; opens nothing |
| `Adapter` | Implements `core.Adapter` |
| `SocketURL(plan)` | The combined-stream URL a plan dials |

## Endpoint

```
wss://fstream.binance.com/stream?streams=btcusdt@markPrice@1s/ethusdt@markPrice@1s
```

Symbols in the path are lower case; the payload echoes them upper case. REST
base for fallback is `https://fapi.binance.com`.

`@markPrice@1s`, never plain `@markPrice`: the plain form updates every three
seconds, which puts every key past its 3s TTL between updates.

There is no subscription message and no ack to wait for — the streams are in
the URL, so a connection that opens is a subscription that took.

## Payload mapping

One `markPriceUpdate` frame becomes **three messages on three keys**, sharing
one `exchange_time_ns` and one `recv_time_ns`, each with its own envelope and
its own `publish_seq`.

| Field | Meaning | Maps to |
|---|---|---|
| `E` | event time, ms | `exchange_time_ns` on all three |
| `s` | symbol | `venue_symbol`, and the canonical identity |
| `p` | mark price | `MarkPrice.mark_price`, price scale |
| `i` | index price | `IndexPrice.index_price`, price scale |
| `P` | estimated settle | **dropped** — no proto field, only meaningful in the last hour before settlement |
| `r` | funding rate | `Funding.funding_rate`, rate scale |
| `T` | next funding, ms | `Funding.next_funding_time_ns` |
| `ap`, `st` | moving average, symbol type | ignored, not an error |

`Funding.interval_seconds` is left `0`. Binance publishes the interval per
symbol on a REST endpoint this adapter does not read, and eight hours stopped
being true for every symbol.

## Quirks

| Quirk | How it is absorbed |
|---|---|
| No sequence number | `venue_seq_present = false`, `venue_seq` zero. An invented one would let a consumer believe it can detect venue-side gaps here. |
| Millisecond timestamps | `msToNs` on every timestamp; `quirks.timestamp_unit: ms` records it. |
| Symbol mapping | Strip the `_`: `BTC_USDT` → `BTCUSDT`. `symbol_overrides` wins where that is wrong. Reversing uses a longest-first quote list. |
| Dated contracts share the endpoint | A symbol containing `_` (`BTCUSDT_240329`) is rejected. Its price is not a perpetual's. |
| Empty funding rate | `r: ""` skips the funding message and still emits mark and index. Zero is a real rate; empty is missing data. |
| Server-initiated pings | `coder/websocket` answers them. Verified, not assumed — `transport.Conn.ServerPings` counts them and two tests check it. |
| 24-hour disconnect | Binance drops long-lived sockets. `connection.max_age` (23h) redials first, so the handover is one we chose the moment of. |
| Three channels, one stream | `PlanSubscriptions` deduplicates by venue stream — subscribing per channel would spend three slots and deliver each frame three times. |
| Contracts trade in base units | `contract_multiplier` is published as **1**, stated rather than left zero: a consumer multiplying by a missing multiplier sizes nothing. |
| Timestamps are send times | `exchange_time_is_send_time` is true on every message. `E` and `premiumIndex.time` are both stamped as the venue answers, so differencing against arrival is a clock comparison. |

## Metadata

`GET /fapi/v1/exchangeInfo`, weight 1, read hourly by `internal/metadata`. Only
`contractType: PERPETUAL` symbols are taken; a dated delivery contract or a
quote asset this adapter has no mapping for is **skipped, not an error** — the
endpoint lists every contract Binance has, and failing on one nobody asked about
would take the whole refresh down. A symbol that *is* served but whose filters do
not parse is an error: that is the difference between "not ours" and "ours, and
wrong". Field mapping is in [`metadata.md`](metadata.md).

## REST fallback

`GET /fapi/v1/premiumIndex?symbol=BTCUSDT` carries the same three values under
different names. `FetchOnce` returns **only the requested channel**: a caller
polling because one key expired asked for that key, not to have two others
overwritten from a source it did not choose. Messages are `SOURCE_REST`.
`internal/fallback` calls it on every expired key; see `fallback.md`.

## Rules

- **Never add a credential or an authenticated endpoint.** This service reads
  prices and has no business placing an order.
- **Reject a range violation, never clamp or wrap it.** A wrapped price is a
  plausible number that nothing downstream can tell from a real one.
- **Tolerate unknown fields.** The venue adds them without warning; erroring on
  one takes the feed down for a field we do not read.
- **Keep `Parse` free of clocks and map iteration**, or the determinism test
  fails and fixture replay stops meaning anything.
