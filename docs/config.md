Covers: M0 · `internal/config`, `config/`

## Purpose

Loads `defaults.yaml` and one venue file, merges them, and either returns a fully validated `Config` or an error naming the offending key and the file it came from. Unknown keys are a startup error, not a warning.

## Files

| Path | Holds |
|---|---|
| `internal/config/config.go` | All config structs, the `Duration` YAML type, and the `VenueSymbol` / `BookCadence` / `TTL` / `Streams` helpers |
| `internal/config/load.go` | `Load`, strict decoding, the provenance map, and every validation rule |
| `internal/config/load_test.go` | `TestLoadValid` checks resolved fields; `TestLoadInvalid` walks `testdata/invalid/`; `TestLoadMissingVenueFile` |
| `internal/config/testdata/valid/` | A config that loads, used as the base for every invalid case |
| `internal/config/testdata/invalid/<case>/` | Only the file that case breaks, plus `error.golden` — the exact expected message |
| `config/defaults.yaml` | Shipped defaults, shared by every venue |
| `config/venues/binance.yaml` | Endpoints, limits and quirks. Values are marked `# TODO(M1): verify` |

## Key types and functions

| Symbol | Signature | Notes |
|---|---|---|
| `Load` | `func(dir, venue string) (*Config, error)` | Reads `dir/defaults.yaml` then `dir/venues/<lowercase venue>.yaml` |
| `Config` | struct | Top-level: defaults sections plus the venue file's `Venue`, `Enabled`, `Endpoints`, `RateLimit`, `Connection`, `Quirks`, `SymbolOverrides`, `Instruments` |
| `ServiceConfig`, `HTTPConfig`, `RedisConfig`, `ScalesConfig`, `PublishConfig`, `HealthConfig`, `FallbackConfig`, `SupervisorConfig`, `BackoffConfig`, `CircuitBreakerConfig`, `MetadataConfig`, `EndpointsConfig`, `RateLimitConfig`, `ConnectionConfig`, `QuirksConfig` | structs | One per YAML section; `yaml` and `validate` struct tags |
| `InstrumentConfig` | struct | `MarketType`/`Channels` as strings from YAML, plus `MT pb.MarketType` and `Chans []pb.Channel` (tagged `yaml:"-"`) resolved by `Load` |
| `Duration` | `type Duration time.Duration` | `UnmarshalYAML` parses `"500ms"`; `MarshalYAML` writes it back; `Std()` returns `time.Duration` |
| `Stream` | struct | One instrument on one channel: `MarketType`, `Symbol`, `VenueSymbol`, `Channel`, `BookDepth` |
| `Config.Streams` | `func() []Stream` | Expands instrument blocks × symbols × channels |
| `Config.VenueSymbol` | `func(canonical string) string` | `symbol_overrides` lookup, else `BTC_USDT` → `BTCUSDT` |
| `Config.BookCadence` | `func() time.Duration` | `quirks.book_cadence_ms` |
| `Config.TTL` | `func(cadence time.Duration) time.Duration` | `cadence × health.ttl_multiplier` |
| `DefaultsFile`, `VenuesDir` | `= "defaults.yaml"`, `"venues"` | |
| `decodeStrict` | `func(path string, out any) error` | `yaml.Decoder` with `KnownFields(true)` |
| `provenance` | struct + `file`/`errf` | Flattens both files to dotted key paths so an error can name the file that set a key |

### Merging

Both files decode onto the *same* `Config` value. `yaml.v3` only writes keys present in the document, so the venue file overrides the individual keys it sets and leaves the rest of `defaults.yaml` intact. Sequences are replaced whole; maps merge per key.

### Validation

`validate` runs `go-playground/validator` over the struct tags first — with `RegisterTagNameFunc` set to `yamlTagName` so messages name the YAML key, not the Go field — then the semantic rules: `validateScales`, `validateHTTP`, `validateHealth`, `validateEndpoints`, `validateSymbolOverrides`, `resolveInstruments`. Every error goes through `provenance.errf`, producing `<file>: <key path>: <message>`. All errors are collected with `errors.Join`, so one run reports every problem.

`resolveInstruments` is last and is what fills `InstrumentConfig.MT` and `.Chans`, so those fields are only populated on a config that passed.

## How it is used

`cmd/manooch-feed.run` calls `Load` before anything else and returns the error unchanged; with `--validate` it prints the result and exits. `internal/synth.New` takes the `*Config` and calls `Streams`, `BookCadence` and `TTL`. `internal/publish` does not import this package.

## Rules

- **Keep `KnownFields(true)`.** A typo'd key that is silently dropped leaves the service running on a default nobody chose and nothing in the logs says so.
- **Every new rule must report through `provenance.errf`.** An error that names a Go field, or names no file, is unusable against a two-file merge.
- **Do not put `validate:"required"` on a struct-typed field.** validator's `required` compares against the zero value, which panics on a struct containing a map — `EndpointsConfig` has two. Rely on the inner fields' tags; validator descends automatically.
- **Sort map keys before iterating in a validation rule.** `validateEndpoints` and `validateSymbolOverrides` use `slices.Sorted(maps.Keys(...))`; unordered iteration makes the joined error text vary between runs and `TestLoadInvalid` compares it against a golden file.
- **Do not add hot reload.** There is no watcher and no SIGHUP handler; behaviour that changes under a running process cannot be reconstructed afterwards.
- **Regenerate goldens deliberately**: `go test ./internal/config -update` rewrites every `error.golden`. Read the diff — those messages are the interface to whoever has to fix a bad config.

### Sections that parse but nothing reads

Only `service`, `redis`, `publish.schema_version`, `health.ttl_multiplier`, `quirks.book_cadence_ms`, `quirks.book_depths_supported`, `venue`, `symbol_overrides` and `instruments` are consumed at M0. `fallback`, `supervisor`, `metadata`, `rate_limit`, `connection` and `endpoints` are parsed and validated, then read by nothing — they belong to M1–M3. `venue.enabled` is likewise only logged by `cmd/manooch-feed`: setting it to `false` does **not** stop the feed.

## Not here

- Enum parsing (`ParseMarketType`, `ParseChannel`, `ChannelValidFor`): `docs/core.md`.
- What `TTL` means once it reaches Redis: `docs/publish.md`.
- How to run `--validate`: the repository `README.md`.
