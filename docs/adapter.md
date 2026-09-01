Covers: M3 · `internal/core/adapter.go`, `internal/adapter/`

The inbound port. One implementation per venue, and the only place a venue's
habits are allowed to exist. Everything downstream of `Parse` is venue-agnostic.

| File | Holds |
|---|---|
| `internal/core/adapter.go` | The `Adapter` interface and its types |
| `internal/adapter/adapter.go` | `New`, `Specs`, `Venues`, `Deps` — venue name to implementation |
| `internal/adapter/binance/` | See [`adapter-binance.md`](adapter-binance.md) |
| `internal/adapter/kucoin/` | See [`adapter-kucoin.md`](adapter-kucoin.md) |
| `internal/adapter/adaptertest/` | The conformance suite every adapter must pass |
| `internal/adapter/normalize_test.go` | Precision and cross-venue normalization, which need two venues to mean anything |

## Key types and functions

| Symbol | What it does |
|---|---|
| `core.Adapter` | The interface: `Venue`, `VenueSymbol`, `ParseVenueSymbol`, `PlanSubscriptions`, `Dial`, `Parse`, `FetchOnce`, `FetchMetadata`, `RESTCost` |
| `core.StreamSpec` | One instrument on one channel: exactly one Redis key |
| `core.SocketPlan` | The streams one socket carries, with a stable `ID` for logs and metrics |
| `core.Message` | A normalized payload with its key, TTL, channel and spec |
| `core.Conn` | One open websocket; `internal/transport` implements it |
| `core.ParseError` | A failed frame, carrying its own metric labels (`Kind`, `Channel`, `Symbol`) |
| `core.Operation`, `core.RESTCost` | The venue's own weight for a call the caller decides to make, which is what the limiter budgets against |
| `core.ErrNotImplemented` | What a venue that cannot serve a method returns; a distinct error so "cannot" is not read as "failed" |
| `adapter.New(cfg, deps)` | Builds the configured venue's adapter, or names the venue it cannot serve |
| `adapter.Deps` | The process-level collaborators a venue package is handed; today, the rate limiter |
| `adapter.Specs(cfg)` | Expands `config.Stream` into `[]core.StreamSpec` |
| `adaptertest.RunAdapterConformance` | Drives a fixture directory and asserts the normalized output |
| `adaptertest.RunAdapterDeterminism` | Parses each fixture 1000 times, asserting identical protobuf |

## How it is used

`cmd/manooch-feed` calls `adapter.New` → `adapter.Specs` → `PlanSubscriptions`
before it dials Redis or binds a port, then hands the plans to
`internal/supervisor`, which owns `Dial`, `Read` and `Parse`.
`internal/fallback` calls `FetchOnce` on every expired key, and
`internal/metadata` calls `FetchMetadata` on its refresh cycle. `RESTCost` is
what the adapters budget their own REST calls against.

`Parse` returns `(nil, nil)` for acks, pongs and heartbeats. That is normal
traffic, not a failure, and counting it as one would show parse errors climbing
on a healthy socket.

## Rules

- **Adapters hold no stream lifecycle state.** No reconnect counters, no
  last-seen timestamps, no goroutines. `internal/supervisor` owns all of it,
  which is what let supervision be added in M2 without touching a venue package.
- **`Parse` is pure and deterministic given `(frame, recvNs)`.** No clock read,
  no map iteration, no counter. Fixture replay tests nothing otherwise, and a
  message that differs between identical frames cannot be reasoned about.
- **Adapters never touch Redis, metrics or the config loader.** They are handed
  resolved values in an `Options` struct. All I/O policy belongs to the caller,
  and a venue package that reads config cannot be built in a test without YAML.
- **Adapters never log at INFO or above on a per-message path.** 200 instruments
  at one update a second is 600 lines a second, which buries the line that
  mattered.
- **Return an error, never a partial result.** Half a frame published as if it
  were whole is the silent wrongness this service exists to prevent.
- **Classify failures with `core.ParseError`.** The caller reads `Kind` for its
  metric label rather than matching on error strings, and `KindRange` is counted
  separately because it is the one failure that would otherwise publish a
  plausible wrong price.
- **Declare a method before implementing it.** `FetchOnce` and friends existed
  as signatures before anything called them, so later phases extend behaviour
  instead of reshaping the interface every consumer already compiled against.
- **Keep venue habits out of `core.Operation`.** It names the calls a *caller*
  decides to make. KuCoin's bullet is something `Dial` does on its own behalf, so
  it is budgeted inside the package and never appears at this interface.

## Adding a venue

Written after adding KuCoin, not before. The order matters: each step catches a
class of mistake the next one would otherwise hide.

1. **New package under `internal/adapter/<venue>/`, with an `Options` struct**
   holding what it needs — never `*config.Config`. Take a `ratelimit.Limiter`
   and a `transport.Dialer` so a test can hand over a socket that replays
   fixtures and a limiter that refuses everything.
2. **Symbols first, both directions, with a round-trip test.** KuCoin spells
   bitcoin `XBT` and suffixes linear perps with `M`; the rule is
   `{BASE}{QUOTE}M` and `symbol_overrides` states each exception exactly. A
   one-way mapping puts REST responses under keys the websocket never writes to.
3. **Absorb every quirk inside the package.** Timestamp units, symbol casing,
   split subjects, connection bootstrapping. KuCoin needed a public REST call
   before it could open a socket, an application-level ping the connection owns,
   and two subjects on one topic at two cadences — and none of it reached
   `internal/core`.
4. **Capture frames into `testdata/<venue>/<case>.json`**, run
   `go test ./internal/adapter/... -update`, and *read* the goldens before
   committing. They are the only statement of what a frame becomes.
5. **Call `adaptertest.RunAdapterConformance` and `RunAdapterDeterminism`.**
   **If either needs a change to pass, the interface was wrong** — fix the
   interface, not the suite.
6. **Add one case to `builders` in `internal/adapter/adapter.go`** and one file
   in `config/venues/`.
7. **Run the smoke tests against the real venue.** This is where the assumptions
   die. KuCoin's funding timestamp turned out to be a settlement instant rather
   than a send time, and its mark price cadence turned out to be nothing like
   what the payload's `granularity` field suggests. Neither was visible from a
   fixture, and both would have made the feed report a healthy venue as broken.

## Not here

Reconnect policy and backoff (`transport.md`), the restart tiers
(`supervisor.md`), status computation (`health.md`), fallback activation
(`fallback.md`), rate limiting (`ratelimit.md`), metadata refresh
(`metadata.md`).
