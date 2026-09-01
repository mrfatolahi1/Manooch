Covers: M3 · `internal/adapter/kucoin`

KuCoin futures over public unauthenticated websockets. Perpetual linear only.
**No credential, no signature, no authenticated stream, no order path.** The
bullet call below takes no key and no account — it is the same POST a browser
makes to open the venue's own chart page.

| File | Holds |
|---|---|
| `kucoin.go` | `New`, `Options`, symbol mapping, `PlanSubscriptions`, `RESTCost` |
| `bullet.go` | The bootstrap POST and `Dial` |
| `subscribe.go` | Subscribe frames, ack waiting, the handshake deadline |
| `ping.go` | The client-side keepalive the venue requires |
| `parse.go` | `Parse`, the two subjects, the timestamp assertion |
| `rest.go`, `metadata.go` | `FetchOnce` and `FetchMetadata` against the public REST endpoints |
| `testdata/kucoin/` | One raw frame per case, beside its `.golden` |

## Key types and functions

| Symbol | What it does |
|---|---|
| `Venue` | `"KUCOIN"` |
| `MarketType` | `PERP_LINEAR`, the only market served |
| `Channels` | The three this adapter produces |
| `Options` | Endpoints, overrides, per-socket limit, per-channel TTLs, limiter, dialer, `ConnectID`, `SubscribeTimeout`, `HTTPTimeout` |
| `New(Options)` | Builds the adapter; opens nothing and fetches no token |
| `Adapter` | Implements `core.Adapter` |

## How it is used

Identical to every other venue: `adapter.New` builds it, `internal/supervisor`
calls `Dial`, `Read` and `Parse`, `internal/fallback` calls `FetchOnce`, and
`internal/metadata` calls `FetchMetadata`. Nothing above knows this venue needs a
REST call before it can open a socket.

## Connection bootstrap

`Dial` does four things and returns only when all four have succeeded:

1. `POST https://api-futures.kucoin.com/api/v1/bullet-public` — no auth, no body.
2. Read the token, the server endpoint, `pingInterval` and `pingTimeout`.
3. Connect to `{endpoint}?token={token}&connectId={uuid}`.
4. Send one subscribe frame per topic and wait for every ack.

The token is **never cached**: KuCoin's tokens expire, and a reconnect reusing
one gets a handshake rejection that reads like the venue being down. `bulletWeight`
is 10 weight units on every dial, which is why a reconnect storm costs more here.

The handshake deadline is enforced by **closing the socket**, not by a context:
`core.Conn.Read` does not watch the caller's, so a socket that opens and then
says nothing would park the dial forever holding a connection slot.

## Payload mapping

One topic, `/contract/instrument:{symbol}`, carries two subjects at two
different cadences.

| Subject | Produces | Field | Maps to |
|---|---|---|---|
| `mark.index.price` | `MarkPrice`, `IndexPrice` | `markPrice` | `mark_price`, price scale |
| | | `indexPrice` | `index_price`, price scale |
| | | `timestamp` | `exchange_time_ns`, ms |
| `funding.rate` | `Funding` | `fundingRate` | `funding_rate`, rate scale |
| | | `timestamp` | `exchange_time_ns`, ms, **not** a send time |
| | | `granularity` | dropped — a push cadence, not a funding interval |

## Quirks

| Quirk | How it is absorbed |
|---|---|
| Bitcoin is `XBT` | The rule is `{BASE}{QUOTE}M`; `symbol_overrides` states each exception exactly (`BTC_USDT: XBTUSDTM`). `ParseVenueSymbol` inverts both. |
| No direct dial | The bullet bootstrap, inside `Dial`. `endpoints.ws` holds the bullet host, not a socket address. |
| Client-initiated ping | `pingingConn` owns a ticker at the interval the bullet named, writing `{"id":…,"type":"ping"}` through `Conn.Write`. `internal/transport` answers protocol-level pings; this is a separate obligation, and missing it gets us disconnected. |
| Split subjects, split cadences | `quirks.cadence` is per channel, so funding gets a 180s TTL where mark and index get 30s. |
| Unquoted JSON numbers | Decoded into `json.Number`, never `float64`, so the venue's own digit string reaches `price.ParsePrice`. A quoted number parses identically, so the venue may change its mind. |
| No next funding time | `next_funding_time_ns` stays **zero**, on the stream and over REST. The venue publishes how long is left, not when; deriving an absolute time would publish our clock as though it were theirs. |
| Funding timestamp is a settlement | `exchange_time_is_send_time` is false on funding. Differencing a four-hour-old event time against arrival would report a four-hour clock skew and take every stream STALE once a minute. |
| A venue can accept and then say nothing | The default HTTP client is bounded by `HTTPTimeout` (30s), which bounds the bullet call and becomes the websocket handshake deadline. Unbounded, a stalled venue parks the dial forever: no attempt fails, so the breaker never trips. |
| Inconsistent timestamp units | `timestampNs` rejects anything outside 2001–2286 in milliseconds. A venue that switches a topic to nanoseconds fails loudly instead of publishing a time in the year 56000. |
| Funding REST names the index | `/funding-rate/{symbol}/current` answers with `.XBTUSDTMFPI8H`. The symbol comes from the spec, never the response. |
| Nothing the venue omits is filled in | `min_notional` and metadata's `exchange_time_ns` stay zero: KuCoin publishes neither, and a computed one would look like a number they gave us. |

## Rules

- **Never cache a bullet token.** A stale token fails the handshake, and the
  failure looks like an outage rather than a bug.
- **Never let `Dial` return before every subscription is acknowledged.** A
  socket that opened and was refused its topics is indistinguishable from a
  venue that went quiet, and it consumes a connection slot forever.
- **Never decode a venue number through `float64`.** KuCoin sends them unquoted;
  a float64 rounds them before `pkg/price` sees a digit, silently.
- **Assert the timestamp magnitude, never convert on faith.** This venue is not
  consistent about units across its topics.
- **Never derive a value the venue did not send.** The gaps here are real and
  visible; a filled-in next funding time would be neither.

## Not here

The cadence measurement behind `quirks.cadence` (`config.md`), reconnect policy
(`transport.md`), the restart tiers (`supervisor.md`), status computation
(`health.md`), rate limiting (`ratelimit.md`), metadata (`metadata.md`).
