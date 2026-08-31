Covers: M1 · `internal/config`, `config/`

Loads `defaults.yaml` and one venue file, merges them, and returns a validated `Config` or an error naming the offending key and its file.

| File | Holds |
|---|---|
| `config.go` | Config structs, the `Duration` YAML type, `VenueSymbol` / `Cadence` / `TTL` / `TTLs` / `Streams` |
| `load.go` | `Load`, strict decoding, the provenance map, every validation rule |
| `load_test.go` | `TestLoadValid`, `TestLoadInvalid` (walks the golden cases), `TestLoadMissingVenueFile` |
| `testdata/valid/` | A config that loads; the base for every invalid case |
| `testdata/invalid/<case>/` | Only the file that case breaks, plus `error.golden` — the exact expected message |
| `config/` | The shipped `defaults.yaml` and `venues/binance.yaml` |

`Load(dir, venue)` is the entry point. `Config.Streams()` expands instrument blocks × symbols × channels into one `Stream` per Redis key. Full API: `go doc ./internal/config`.

## Merging and validation

Both files decode onto the **same** `Config` value. `yaml.v3` only writes keys present in the document, so merging is per key: the venue file overrides what it sets and leaves the rest alone. Sequences are replaced whole; maps merge per key.

Struct-tag validation runs first (`RegisterTagNameFunc` makes messages name the YAML key, not the Go field), then the semantic rules — scales against `pkg/price`, loopback `http.listen`, clock-skew ordering, endpoint schemes, symbol patterns, cadence coverage, channel/market-type compatibility. Everything routes through `provenance.errf` to produce `<file>: <key path>: <message>`, and all errors are collected with `errors.Join` so one run reports every problem.

`resolveInstruments` runs last and fills `InstrumentConfig.MT` and `.Chans`, so those are only populated on a config that passed.

## Per-channel cadence

The three channels update at different rates on different venues, so each gets
its own base in the venue file:

```yaml
quirks:
  timestamp_unit: ms
  cadence:
    mark_price:  1s
    index_price: 1s
    funding:     1s
```

`Config.TTL(ch)` is that cadence times `health.ttl_multiplier` — 3s each here.
`Config.TTLs()` returns the whole map, which is what an adapter is handed.

It is per channel rather than per venue because KuCoin pushes funding once a
minute against mark price once a second: one number would expire the slower
channel's key between updates and report a healthy stream as dead.

**A configured channel with no cadence is a startup error.** Without a cadence
there is no TTL, and a key with no TTL cannot say whether it is fresh.

## Very important details

- **Much of the config is not read yet.** `fallback`, `supervisor`, `metadata` and `rate_limit` parse and validate, then nothing reads them. `venue.enabled` is only logged — setting it `false` does **not** stop the feed. `endpoints`, `connection.max_streams_per_socket` and `connection.read_timeout` are read by the adapter; `connection.ping_interval` and `pong_timeout` are not.
- **`validateHTTP` returns early when `service.http.enabled` is false**, so a non-loopback `listen` is only rejected when a server would actually start.

## Rules

- **Keep `KnownFields(true)`.** A silently dropped typo leaves the service on a default nobody chose.
- **Never put `validate:"required"` on a struct field.** validator compares against the zero value, which panics on a struct containing a map — `EndpointsConfig` has two.
- **Sort map keys in validation rules.** Unordered iteration makes the joined error text vary between runs, and `TestLoadInvalid` compares it to a golden file.
- **No hot reload.** Behaviour that changes under a running process cannot be reconstructed afterwards.
- Regenerate goldens with `go test ./internal/config -update`, and read the diff — those messages are the interface to whoever fixes a bad config.
