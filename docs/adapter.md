Covers: M2 · `internal/core/adapter.go`, `internal/adapter/`

The inbound port. One implementation per venue, and the only place a venue's
habits are allowed to exist. Everything downstream of `Parse` is venue-agnostic.

| File | Holds |
|---|---|
| `internal/core/adapter.go` | The `Adapter` interface and its types |
| `internal/adapter/adapter.go` | `New`, `Specs`, `Venues` — venue name to implementation |
| `internal/adapter/binance/` | The one venue, see [`adapter-binance.md`](adapter-binance.md) |
| `internal/adapter/adaptertest/` | The conformance suite every adapter must pass |

## Key types and functions

| Symbol | What it does |
|---|---|
| `core.Adapter` | The interface: `Venue`, `VenueSymbol`, `ParseVenueSymbol`, `PlanSubscriptions`, `Dial`, `Parse`, `FetchOnce`, `FetchMetadata`, `RESTCost` |
| `core.StreamSpec` | One instrument on one channel: exactly one Redis key |
| `core.SocketPlan` | The streams one socket carries, with a stable `ID` for logs and metrics |
| `core.Message` | A normalized payload with its key, TTL, channel and spec |
| `core.Conn` | One open websocket; `internal/transport` implements it |
| `core.ParseError` | A failed frame, carrying its own metric labels (`Kind`, `Channel`, `Symbol`) |
| `core.Operation`, `core.RESTCost` | REST weight accounting, for M3's rate limiter |
| `core.ErrNotImplemented` | What a method a later phase wires up returns |
| `adapter.New(cfg)` | Builds the configured venue's adapter, or names the venue it cannot serve |
| `adapter.Specs(cfg)` | Expands `config.Stream` into `[]core.StreamSpec` |
| `adaptertest.RunAdapterConformance` | Drives a fixture directory and asserts the normalized output |
| `adaptertest.RunAdapterDeterminism` | Parses each fixture 1000 times, asserting identical protobuf |

## How it is used

`cmd/manooch-feed` calls `adapter.New` → `adapter.Specs` → `PlanSubscriptions`
before it dials Redis or binds a port, then hands the plans to
`internal/supervisor`, which owns `Dial`, `Read` and `Parse`.
`internal/fallback` calls `FetchOnce` on every expired key. `FetchMetadata` and
`RESTCost` are declared and unused by the daemon; M3 calls them.

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

## Adding a venue

1. New package under `internal/adapter/<venue>/`, with an `Options` struct
   holding what it needs — never `*config.Config`.
2. Implement `core.Adapter`. Absorb every quirk: timestamp units, symbol
   casing, split subjects, connection bootstrapping.
3. Capture frames into `testdata/<venue>/<case>.json`, run
   `go test ./internal/adapter/... -update`, and read the goldens before
   committing them.
4. Call `adaptertest.RunAdapterConformance` and `RunAdapterDeterminism` from the
   package's test. **If either needs a change to pass, the interface was
   wrong** — fix the interface, not the suite.
5. Add one case to `builders` in `internal/adapter/adapter.go`.

## Not here

Reconnect policy and backoff (`transport.md`), the restart tiers
(`supervisor.md`), status computation (`health.md`), fallback activation
(`fallback.md`), rate limiting, metadata refresh.
