<p align="left">
  <img src="docs/assets/manooch-logo.png" alt="Manooch" width="180">
</p>

# Manooch

A crypto market-data price service. It connects to exchanges over public
websockets, normalizes what it sees into one format, and publishes to Redis for
downstream consumers. One process serves one venue.

It mainly uses WebSockets to receive data, with REST API fallback and resubscription mechanism for unhealthy WebSockets. Manooch tries to ensure that fresh data is always available in Redis.

**Milestone M0.** The skeleton and the contract exist. There are no exchange
adapters yet: the only producer is a dev-only synthetic generator.

No credentials, public endpoints only.

## Bring it up

```sh
make up                 # docker compose: Redis + one feed, publishing synthetic data
make down
```

Redis is published on `127.0.0.1:6379` for now.

## Look at the data

```sh
manooch-tap --pattern="Manooch:BINANCE:*" [--redis=127.0.0.1:6379] [--json] [--raw]
```

```
16:23:37.125  Manooch:BINANCE:PERP_LINEAR:BTC_USDT:orderbook     seq=1      HEALTHY  bid=68432.15 ask=68432.35 depth=20
16:23:38.023  Manooch:BINANCE:PERP_LINEAR:BTC_USDT:funding       seq=1      HEALTHY  rate=-0.000059051878 next=2026-08-31T00:00:00Z interval=28800s
```

```sh
manooch-status [--venue=BINANCE] [--redis=127.0.0.1:6379]
```

```
VENUE    MARKET TYPE  SYMBOL    CHANNEL      STATUS   AGE   SOURCE     TTL    PUBLISH SEQ
BINANCE  PERP_LINEAR  BTC_USDT  orderbook    HEALTHY  19ms  WEBSOCKET  282ms  40
BINANCE  PERP_LINEAR  BTC_USDT  trades       HEALTHY  19ms  WEBSOCKET  none   16
```

## Documentation

[`docs/`](docs/README.md) explains the code. Start with
[`docs/architecture.md`](docs/architecture.md) — the shape, the dependency rule,
the dataflow for one message, and the decisions table. There is one document per
package.
