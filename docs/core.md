Covers: M1 · `internal/core`

Instrument identity, the mapping between protobuf enum values and the strings used in config files and Redis keys, and the venue-adapter port. `internal/config`, `internal/publish`, `internal/transport` and every venue package depend on it, which is why it depends on none of them.

| File | Holds |
|---|---|
| `instrument.go` | `InstrumentRef`, `ParseCanonical`, `CanonicalPattern` |
| `enums.go` | Enum name/parse helpers, market-structure predicates |
| `adapter.go` | `Adapter`, `StreamSpec`, `SocketPlan`, `Message`, `Conn`, `ParseError`, `Operation` — see [`adapter.md`](adapter.md) |

No test file; covered indirectly by `internal/config/load_test.go` and `internal/publish/keys_test.go`.

`InstrumentRef{Base, Quote, Settle, MarketType, Expiry}` is comparable, so it works as a map key. `Canonical()` gives `BTC_USDT`, `String()` adds the market type, `Proto(venueSymbol)` converts to the wire type. `ParseCanonical` fills `Settle` by convention: inverse settles in the base, other derivatives in the quote, spot in neither.

Name helpers read `pb.MarketType_name` / `pb.Channel_name` and trim a prefix rather than keeping a parallel table, so they cannot drift from the `.proto`. Full API: `go doc ./internal/core`.

## Rules

- **Import nothing local except `gen/manoochv1`.** Anything else becomes an import cycle when an adapter needs to name an instrument. This is why `Message.Key` is a plain string the adapter fills with `publish.Key`, rather than something this package builds.
- **Never collapse linear and inverse.** `IsInverse` decides what a `size` means — base units or contracts. Treating them alike makes every position wrong by roughly the price.
- **`MarketType` is part of the identity.** `BTC_USDT` spot and `BTC_USDT` perpetual are different instruments at different prices; `Canonical()` deliberately drops it, which is why `String()` exists.
- **`MarketTypeName`/`ChannelName` stay total**, returning `"UNKNOWN"`/`"unknown"`, because `publish.Key` has no error return. `ParseMarketType`/`ParseChannel` then reject those strings, so a key built from an unspecified enum fails `ParseKey` instead of looking plausible.
