Covers: M3 · `internal/config`, `config/`

Loads `defaults.yaml` and one venue file, merges them, and returns a validated `Config` or an error naming the offending key and its file.

| File | Holds |
|---|---|
| `config.go` | Config structs, the `Duration` YAML type, `Cadence` / `TTL` / `TTLs` / `Streams` |
| `load.go` | `Load`, strict decoding, the provenance map, every validation rule |
| `load_test.go` | `TestLoadValid`, `TestLoadInvalid` (walks the golden cases), `TestLoadMissingVenueFile` |
| `testdata/valid/` | A config that loads; the base for every invalid case |
| `testdata/invalid/<case>/` | Only the file that case breaks, plus `error.golden` — the exact expected message |
| `config/` | The shipped `defaults.yaml` and `venues/binance.yaml`, `venues/kucoin.yaml` |

`Load(dir, venue)` is the entry point. `Config.Streams()` expands instrument blocks × symbols × channels into one `Stream` per Redis key. Full API: `go doc ./internal/config`.

## Merging and validation

Both files decode onto the **same** `Config` value. `yaml.v3` only writes keys present in the document, so merging is per key: the venue file overrides what it sets and leaves the rest alone. Sequences are replaced whole; maps merge per key.

Struct-tag validation runs first (`RegisterTagNameFunc` makes messages name the YAML key, not the Go field), then the semantic rules — scales against `pkg/price`, loopback `http.listen`, clock-skew ordering, endpoint schemes, symbol patterns, cadence coverage, channel/market-type compatibility. Everything routes through `provenance.errf` to produce `<file>: <key path>: <message>`, and all errors are collected with `errors.Join` so one run reports every problem.

`resolveInstruments` runs last and fills `InstrumentConfig.MT` and `.Chans`, so those are only populated on a config that passed.

## Per-channel cadence

Every channel gets its own base in the venue file, and the two venues disagree
about all of them:

| Channel | Binance | KuCoin | KuCoin TTL |
|---|---|---|---|
| `mark_price` | `1s` | `10s` | 30s |
| `index_price` | `1s` | `10s` | 30s |
| `funding` | `1s` | `60s` | 180s |

`Config.TTL(ch)` is that cadence times `health.ttl_multiplier`; `Config.TTLs()`
returns the whole map, which is what an adapter is handed.

Per channel rather than per venue because KuCoin pushes funding once a minute
against mark price far more often: one number would expire the slower channel's
key between updates and report a healthy stream as dead.

**Cadence is what the venue does, not what its documentation says.** KuCoin's
`mark.index.price` payload carries `granularity: 1000`, which is the granularity
of the value and not a promise to send one — the venue pushes on change.
Measured over two minutes, the gaps were 1.7s mean on BTC and 2.2s on ETH with
worst cases of 18.9s and 12.0s, so a 1s cadence would put both streams
permanently on REST fallback while the socket worked perfectly. 10s covers the
observed tail. The cost is stated in the venue file: an outage there is noticed
in 30 seconds rather than 3.

**A configured channel with no cadence is a startup error.** Without a cadence
there is no TTL, and a key with no TTL cannot say whether it is fresh.

## Symbol overrides and endpoints

`symbol_overrides` maps canonical to venue symbol and is the only place an
exception to a venue's rule is stated. Both venues have one: Binance's
`BTC_USDT: BTCUSDT` restates the rule, KuCoin's `BTC_USDT: XBTUSDTM` states that
the venue spells bitcoin `XBT`. There is no fallback rule in this package — what
a venue calls an instrument is the adapter's answer, and a rule here would have
to be right for every venue at once.

`endpoints.ws` accepts an `https` base as well as a `wss` one. Not every venue
lets you dial the socket directly: KuCoin hands out the address over a public
REST call, so what belongs there is where to ask rather than where to connect.
Rejecting `https` would have forced that URL into a key the adapter does not
read, which is a config file that lies.

## Rate limit

`rate_limit` is translated into `ratelimit.Bucket` values by
`cmd/manooch-feed.newLimiter`; see [`ratelimit.md`](ratelimit.md).

| Key | Becomes |
|---|---|
| `rest_weight_per_minute` × `max_weight_fraction` | `LimitRESTWeight`, per minute |
| `ws_connect_per_5min` × `ws_connect_fraction` | `LimitWSConnect`, per 5 minutes |
| `subscriptions_per_connection` × the connect capacity | `LimitSubscriptions`, per 5 minutes |

`subscriptions_per_connection` is also checked against
`connection.max_streams_per_socket`: a socket planned to carry more streams than
the venue accepts on one connection is refused at subscribe time, which looks
like a venue outage rather than a config error.

## Very important details

- **`connection.max_age` is required.** It is what redials before a venue drops a long-lived socket; leaving it optional would make a missing key mean "never reconnect proactively", which is a gap nobody chose.
- **A little of the config is still not read.** `connection.ping_interval` and `pong_timeout` are not: Binance's pings are the library's business, and KuCoin's interval comes from the bullet response rather than from here. `venue.enabled` is only logged — setting it `false` does **not** stop the feed.
- **`validateHTTP` returns early when `service.http.enabled` is false**, so a non-loopback `listen` is only rejected when a server would actually start.

## Rules

- **Keep `KnownFields(true)`.** A silently dropped typo leaves the service on a default nobody chose.
- **Never put `validate:"required"` on a struct field.** validator compares against the zero value, which panics on a struct containing a map — `EndpointsConfig` has two.
- **Sort map keys in validation rules.** Unordered iteration makes the joined error text vary between runs, and `TestLoadInvalid` compares it to a golden file.
- **No hot reload.** Behaviour that changes under a running process cannot be reconstructed afterwards.
- Regenerate goldens with `go test ./internal/config -update`, and read the diff — those messages are the interface to whoever fixes a bad config.
