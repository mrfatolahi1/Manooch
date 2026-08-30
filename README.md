# Manooch

A crypto market-data price service. It connects to exchanges over public
websockets, normalizes what it sees into one format, and publishes to Redis for
downstream consumers — strategy, risk, hedging. One process serves one venue.

The design has one governing idea: a feed that publishes stale or wrong data
**without saying so** loses money silently. Every ambiguity is resolved in
favour of loud failure — unknown config keys stop startup, numbers that will not
fit the fixed-point scale are errors rather than truncations, and every message
carries a status and a sequence number so a consumer can tell "quiet market"
from "I stopped receiving".

**Milestone M0.** The skeleton and the contract exist. There are no exchange
adapters yet: the only producer is a dev-only synthetic generator. Without
`--synthetic` the feed starts, proves it can reach Redis, serves `/healthz` and
idles. See [`docs/architecture.md`](docs/architecture.md) for what is built and
what each empty directory is reserved for.

No credentials, ever — public endpoints only, no order placement.

## Bring it up

```sh
make up                 # docker compose: Redis + one feed, publishing synthetic data
make down
```

Redis is published on `127.0.0.1:6379` so the CLI tools can run from the host.
The feed's admin surface is not published — it binds to loopback inside the
container.

To run the feed outside Docker against a Redis on localhost:

```sh
make run                # copies config/ to .local/config with redis.addr rewritten
```

## Look at the data

The wire format is protobuf, so `redis-cli psubscribe` shows you nothing
readable. Two tools exist for that.

```sh
manooch-tap --pattern="Manooch:BINANCE:*" [--redis=127.0.0.1:6379] [--json] [--raw]
```

```
16:23:37.125  Manooch:BINANCE:PERP_LINEAR:BTC_USDT:orderbook     seq=1      HEALTHY  bid=68432.15 ask=68432.35 depth=20
16:23:38.023  Manooch:BINANCE:PERP_LINEAR:BTC_USDT:funding       seq=1      HEALTHY  rate=-0.000059051878 next=2026-08-31T00:00:00Z interval=28800s
```

It also flags what Redis will not tell you: a jump in `publish_seq` (messages
dropped on the bus) and a change of `instance_id` (the feed restarted).

```sh
manooch-status [--venue=BINANCE] [--redis=127.0.0.1:6379]
```

```
VENUE    MARKET TYPE  SYMBOL    CHANNEL      STATUS   AGE   SOURCE     TTL    PUBLISH SEQ
BINANCE  PERP_LINEAR  BTC_USDT  orderbook    HEALTHY  19ms  WEBSOCKET  282ms  40
BINANCE  PERP_LINEAR  BTC_USDT  trades       HEALTHY  19ms  WEBSOCKET  none   16
```

## Check a config without running anything

```sh
manooch-feed --exchange=BINANCE --config=./config --validate
```

Prints the resolved config and the exact Redis keys it implies, then exits 0. It
opens no sockets and no Redis connection, so it is safe to run against
production config from anywhere. A bad config fails at startup naming the key
and its file:

```
config/defaults.yaml: health.ttl_multiplier: must be at least 2, got 1
config/venues/binance.yaml: instruments[0].channels[2]: channel "funding" does not exist on market_type SPOT
```

`manooch-feed` takes four flags: `--exchange` (required, upper case),
`--config`, `--validate`, `--synthetic`.

## Develop

Requires Go — see `go.mod` for the version.

```sh
make build            # -> bin/manooch-feed, bin/manooch-status, bin/manooch-tap
make test
make test-integration # needs Docker; runs against a real Redis
make lint             # go vet + gofmt check
make proto            # regenerate gen/ after editing schema/manooch.proto
```

No application framework, no ORM, no CLI or config library. Standard library
plus seven direct dependencies; see [`docs/deploy.md`](docs/deploy.md).

## Documentation

[`docs/`](docs/README.md) explains the code. Start with
[`docs/architecture.md`](docs/architecture.md) — the shape, the dependency rule,
the dataflow for one message, and the decisions table. There is one document per
package.
