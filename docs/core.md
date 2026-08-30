Covers: M0 · `internal/core`

## Purpose

The canonical way to name an instrument, and the mapping between protobuf enum values and the strings that appear in config files and Redis keys. It is the one package both `internal/config` and `internal/publish` depend on, which is why it depends on neither.

## Files

| Path | Holds |
|---|---|
| `internal/core/instrument.go` | `InstrumentRef`, `ParseCanonical`, `CanonicalPattern` |
| `internal/core/enums.go` | Enum name/parse helpers and the market-structure predicates |

No test file. The behaviour is covered indirectly by `internal/config/load_test.go` (parsing, `ChannelValidFor`) and `internal/publish/keys_test.go` (naming, round-trips).

## Key types and functions

| Symbol | Signature | Notes |
|---|---|---|
| `InstrumentRef` | struct `{Base, Quote, Settle string; MarketType pb.MarketType; Expiry string}` | Comparable, so it works as a map key |
| `InstrumentRef.Canonical` | `func() string` | `Base + "_" + Quote` |
| `InstrumentRef.String` | `func() string` | `BTC_USDT:PERP_LINEAR`, with `:<expiry>` appended when set |
| `InstrumentRef.Proto` | `func(venueSymbol string) *pb.Instrument` | Fills `canonical` and copies the rest |
| `ParseCanonical` | `func(s string, mt pb.MarketType) (InstrumentRef, error)` | Rejects a symbol not matching `CanonicalPattern` and rejects `MARKET_TYPE_UNSPECIFIED` |
| `CanonicalPattern` | `` = `^[A-Z0-9]+_[A-Z0-9]+$` `` | Also used by `internal/config` and `internal/publish` for their own symbol checks |
| `MarketTypeName` | `func(pb.MarketType) string` | `MARKET_TYPE_SPOT` → `SPOT`; unknown value → `"UNKNOWN"` |
| `ParseMarketType` | `func(string) (pb.MarketType, error)` | Case-insensitive; `UNSPECIFIED` is an error |
| `ChannelName` | `func(pb.Channel) string` | `CHANNEL_MARK_PRICE` → `mark_price`; unknown value → `"unknown"` |
| `ParseChannel` | `func(string) (pb.Channel, error)` | Case-insensitive; `UNSPECIFIED` is an error |
| `StatusName` | `func(pb.Status) string` | `HEALTHY`, `DEGRADED`, `STALE` |
| `SourceName` | `func(pb.Source) string` | `WEBSOCKET`, `REST` |
| `IsDerivative` | `func(pb.MarketType) bool` | True for `PERP_*` and `FUTURE_*` |
| `IsInverse` | `func(pb.MarketType) bool` | True for `PERP_INVERSE` and `FUTURE_INVERSE` |
| `ChannelValidFor` | `func(ch pb.Channel, mt pb.MarketType) bool` | `mark_price`, `index_price` and `funding` require `IsDerivative`; the rest are always valid |

The name functions read `pb.MarketType_name` / `pb.Channel_name` and trim a prefix constant rather than keeping a parallel table, so they cannot drift from `schema/manooch.proto`.

`ParseCanonical` fills `Settle` from the market type: inverse settles in the base, other derivatives settle in the quote, spot and margin leave it empty. An adapter whose venue disagrees overwrites the field afterwards.

## How it is used

`internal/config.resolveInstruments` calls `ParseMarketType`, `ParseChannel` and `ChannelValidFor` to turn config strings into enums and to reject impossible combinations. `internal/publish.Key` calls `MarketTypeName` and `ChannelName`; `ParseKey` calls the inverses. `internal/synth.runStream` calls `ParseCanonical` then `Proto` once per stream, at startup, and reuses the `*pb.Instrument` for every message. `cmd/manooch-tap` and `cmd/manooch-status` call `StatusName` and `SourceName` for display.

## Rules

- **Import nothing from this repository except `gen/manoochv1`.** A Redis client or a venue package here would make the identity types unusable from a consumer that has neither, and would become an import cycle as soon as an adapter needs to name an instrument.
- **Do not collapse the linear/inverse distinction.** `IsInverse` decides what a `size` means: base-asset units on a linear instrument, a count of contracts on an inverse one. Treating them alike produces a position wrong by roughly the price.
- **Keep `MarketType` part of the identity.** `BTC_USDT` spot and `BTC_USDT` perpetual are different instruments with different prices; `InstrumentRef.Canonical` deliberately drops the market type, which is why `String` exists and why keys carry it separately.
- **`Name` functions must stay total.** `MarketTypeName` and `ChannelName` return `"UNKNOWN"`/`"unknown"` rather than failing, because `publish.Key` has no error return. `ParseMarketType`/`ParseChannel` then reject those strings, so a key built from an unspecified enum is rejected by `ParseKey` and shows up in `manooch-status` instead of being silently plausible.

## Not here

- The generated enum constants: `docs/contract.md`.
- Key construction from these names: `docs/publish.md`.
- Which channel/market-type pairs a given venue is configured for: `docs/config.md`.
