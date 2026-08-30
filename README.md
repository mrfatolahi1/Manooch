# Manooch

Manooch is a crypto market-data price service. It connects to exchanges over
public websockets, normalizes what it sees into a single internal format, and
publishes to Redis for downstream consumers — strategy, risk, hedging.

The design has one governing idea:

> A feed that publishes stale or wrong data **without saying so** loses money
> silently. Preventing that is the point of the service.

Every ambiguity is resolved in favour of loud failure over silent degradation.
Unknown config keys stop startup. Numbers that will not fit the fixed-point
scale are errors, not truncations. Redis is configured so a full instance
refuses writes rather than quietly evicting the data that says a stream is
alive. Every published message carries a status and a sequence number, so a
consumer can tell "quiet market" from "I stopped receiving".

## What this is right now: M0

M0 is the skeleton and the contract. **There are no exchange adapters and no
websockets yet.** What exists:

- `schema/manooch.proto` — the wire contract, with generated Go committed in `gen/`
- `pkg/price` — fixed-point numerics; no float64 anywhere in the value path
- `internal/config` — strict YAML loading and validation
- `internal/core` — canonical instrument identity
- `internal/publish` — the Redis key scheme and the publisher
- `internal/obs` — logging and the full metrics set
- `internal/synth` — a dev-only generator so the plumbing can be exercised
- `cmd/manooch-feed`, `cmd/manooch-tap`, `cmd/manooch-status`

Not in M0, and deliberately not designed for: exchange adapters, websocket
handling, health and TTL logic, REST fallback, the stream supervisor, backoff,
rate limiting, metadata refresh. Those are M1–M5. The empty package directories
under `internal/` are where they will go.

Without `--synthetic`, `manooch-feed` starts, proves it can reach Redis, serves
`/healthz` and idles. That is the expected M0 behaviour and it says so in the
log.

## Build

Requires Go (latest stable; see `go.mod`).

```sh
make build            # -> bin/manooch-feed, bin/manooch-status, bin/manooch-tap
make test             # unit tests
make test-integration # needs Docker; runs against a real Redis
make lint             # go vet + gofmt check
```

To regenerate the protobuf code after changing `schema/manooch.proto`:

```sh
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
make proto            # then commit gen/ in the same commit as the .proto change
```

## Run

Everything, in one command:

```sh
docker compose -f deploy/docker-compose.yml up --build     # or: make up
```

That starts Redis with `deploy/redis.conf` and one feed container per venue.
Redis is published on `127.0.0.1:6379` so the CLI tools can be run from the
host. The feed's admin surface is **not** published — it binds to loopback
inside the container, which is the point.

```sh
make down
```

To run the feed outside Docker against a Redis on localhost:

```sh
make run              # copies config/ to .local/config with redis.addr rewritten
```

The committed `config/defaults.yaml` points `redis.addr` at the compose service
name, so `make run` makes a local copy pointing at `127.0.0.1` rather than
asking you to edit tracked config.

### Checking a config without running anything

```sh
manooch-feed --exchange=BINANCE --config=./config --validate
```

Loads and validates the config, prints the fully resolved result and the exact
list of Redis keys it implies, and exits 0. It opens **no sockets and no Redis
connection**, so it is safe to run against production config from anywhere.

A bad config fails at startup with the offending key and the file it came from:

```
config/defaults.yaml: health.ttl_multiplier: must be at least 2, got 1
config/venues/binance.yaml: instruments[0].channels[2]: channel "funding" does not exist on market_type SPOT
```

### Flags

`manooch-feed`:

| Flag | Meaning |
|---|---|
| `--exchange=BINANCE` | required, upper case |
| `--config=./config` | directory holding `defaults.yaml` and `venues/` |
| `--validate` | load, validate, print, exit 0; opens nothing |
| `--synthetic` | dev only: publish generated data instead of connecting to a venue |

## Looking at the data

The wire format is protobuf, so `redis-cli psubscribe` shows you nothing
readable. Two tools exist for that.

`manooch-tap` subscribes and prints:

```sh
manooch-tap --pattern="Manooch:BINANCE:*" [--redis=127.0.0.1:6379] [--json] [--raw]
```

```
15:34:55.140  Manooch:BINANCE:PERP_LINEAR:BTC_USDT:orderbook    seq=50  HEALTHY  bid=68432.15 ask=68432.35 depth=20
15:34:55.140  Manooch:BINANCE:PERP_LINEAR:BTC_USDT:funding      seq=5   HEALTHY  rate=0.000093705267 next=2026-08-30T16:00:00Z interval=28800s
```

It also flags what Redis will not tell you: a jump in `publish_seq` (messages
dropped on the bus) and a change of `instance_id` (the feed restarted).
`--json` prints whole messages as JSON lines; `--raw` writes message bytes to
`--out` for building M1 test fixtures.

`manooch-status` reads the keys — it never subscribes — and prints one row per
stream:

```sh
manooch-status [--venue=BINANCE] [--redis=127.0.0.1:6379]
```

```
VENUE    MARKET TYPE  SYMBOL    CHANNEL      STATUS   AGE    SOURCE     TTL    PUBLISH SEQ
BINANCE  PERP_LINEAR  BTC_USDT  orderbook    HEALTHY  89ms   WEBSOCKET  211ms  168
BINANCE  PERP_LINEAR  BTC_USDT  trades       HEALTHY  139ms  WEBSOCKET  none   67
```

Degraded and stale rows are coloured, and marked with `!` / `!!` so they still
stand out through a pipe. From M2 this is the main operational view.

## How the Redis side works

```
Manooch:{VENUE}:{MARKET_TYPE}:{SYMBOL}:{channel}
Manooch:{VENUE}:venue:{subject}
```

Every publish is one pipelined round trip:

```
SET     <key> <serialized proto> PX <ttl_ms>
PUBLISH <key> <serialized proto>
```

The same string is the key and the Pub/Sub channel; Redis keeps those in
separate namespaces. The `SET` gives a cold consumer current state immediately
instead of making it wait for the next tick, and makes freshness a property of
the data: key present means fresh, key absent means stale, with no second
timestamp to drift out of sync with the first. `ttl == 0` means no expiry, used
for event-driven channels like trades where silence is normal.

Build keys with `publish.Key`. Never by concatenation — a key with a typo in it
is written successfully, published successfully, and read by nobody.

## Numbers

Prices, sizes and rates are `int64` at fixed global scales. Never `float64`,
never a decimal library, on the wire or in internal state.

| Quantity | Scale | 1.0 is | Range |
|---|---|---|---|
| Price | `1e-11` | `100_000_000_000` | `0.00000000001` → `92,233,720` |
| Size | `1e-8` | `100_000_000` | `0.00000001` → `92,233,720,368` |
| Rate | `1e-12` | `1_000_000_000_000` | `±9,223,372` |

`pkg/price` parses a venue's decimal string directly from its digits — not via
`strconv.ParseFloat` and a multiply, which loses the bottom of the range. Out
of range is `ErrOutOfRange`, below the scale is `ErrPrecisionLoss`, and neither
is ever a clamp or a silent truncation. `pkg/price` is under `pkg/` rather than
`internal/` because consumers import it to decode what we publish.

## Dependency policy

No application framework. No Gin, Echo, Fiber, Chi. No Viper or Koanf. No
Cobra. No ORM. Standard library plus this complete list:

```
google.golang.org/protobuf
github.com/redis/go-redis/v9
gopkg.in/yaml.v3
github.com/go-playground/validator/v10
github.com/prometheus/client_golang
github.com/google/uuid
github.com/ory/dockertest/v3        (test only)
```

Nothing else, in any phase. Anything else in `go.mod` is a transitive
dependency of one of these.

## Hard constraints

- **No credentials, ever.** No API keys, no authenticated endpoints, no order
  placement. Public endpoints only. A code path that would need a credential is
  out of scope by definition.
- **No market-data HTTP endpoint**, now or ever. Consumers read Redis. A
  parallel HTTP path would be a second contract to keep in sync, and the two
  would disagree the first time either changed.
- The admin surface (`/healthz`, `/metrics`, `/debug/pprof`) binds to loopback
  only, and config that says otherwise is rejected at startup.

## Invariants

These hold in every phase. A violation is a bug regardless of what the tests say.

1. Never publish data without a `status`.
2. Never publish out-of-range numerics — range-check before every `int64`
   assignment, and reject rather than clamp or wrap.
3. Never log per message.
4. Never build a key by string concatenation — use `publish.Key`.
5. Never set `publish_time_ns` anywhere except immediately before the Redis write.
6. Never add a credential, an authenticated endpoint, or an order-placing code path.
7. Never add a dependency outside the list above.

## Notes on this implementation

- The module path is `github.com/you/manooch`, matching the `go_package` option
  in `schema/manooch.proto`. Rename both together when the repository gets its
  real home.
- Config merging is per key: the venue file overrides the individual keys it
  sets and leaves the rest of `defaults.yaml` alone.
- Beyond the validation rules the brief lists, the loader also rejects: a
  `scales:` block that disagrees with the constants compiled into `pkg/price`
  (otherwise every number is off by a power of ten, silently); a
  `clock_skew_stale_ms` at or below `clock_skew_degraded_ms`; a venue file
  whose `venue:` does not match `--exchange`; a websocket endpoint with a
  non-`ws`/`wss` scheme; an instrument block with no matching `endpoints.ws`
  entry; and duplicate market-type blocks, channels or symbols. Each one is a
  mistake that would otherwise surface as a stream that silently never
  populates.
- `config/venues/binance.yaml` endpoints and quirk values are placeholders
  marked `# TODO(M1): verify`. They parse; they are not yet known to be right.
- `manooch-tap` takes `--out` for `--raw` output, and both CLIs take `--db`.
  These are beyond the flag lists in the brief but the tools are unusable
  without somewhere to write and a way to reach a non-default database.
