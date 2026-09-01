<p align="left">
  <img src="docs/assets/manooch-logo.png" alt="Manooch" width="180">
</p>

# Manooch

A crypto market-data price service. It connects to exchanges over public
websockets, normalizes what it sees into one format, and publishes to Redis for
downstream consumers. One process serves one venue.

It mainly uses WebSockets to receive data, with REST API fallback and resubscription mechanism for unhealthy WebSockets. Manooch tries to ensure that fresh data is always available in Redis.

**Milestone M2.** Real data from Binance USD-M futures: mark price, index price
and funding, three Redis keys per symbol. The scope is perpetual mark price
only — order books and trades are gone for good. The feed now supervises itself:
a dropped socket redials with jittered backoff behind a circuit breaker, a
failed stream restarts on its own, an expired key is served over REST and
labelled as such, and every stream's health is published on a heartbeat so
silence is never ambiguous.

No credentials, public endpoints only.

## Bring it up

```sh
make up                 # docker compose: Redis + one feed
make down
```

Redis is published on `127.0.0.1:6379` for now. To run the feed against the
real venue from a checkout:

```sh
make run                 # against the real venue; needs a Redis on 127.0.0.1:6379
```

## Look at the data

```sh
manooch-tap --pattern="Manooch:BINANCE:*" [--redis=127.0.0.1:6379] [--json] [--raw]
```

```
16:23:37.125  Manooch:BINANCE:PERP_LINEAR:BTC_USDT:mark_price    seq=16     HEALTHY  mark=68432.15
16:23:37.125  Manooch:BINANCE:PERP_LINEAR:BTC_USDT:index_price   seq=16     HEALTHY  index=68431.25
16:23:37.126  Manooch:BINANCE:PERP_LINEAR:BTC_USDT:funding       seq=16     HEALTHY  rate=0.00038167 next=2026-09-01T00:00:00Z interval=0s
```

```sh
manooch-status [--venue=BINANCE] [--redis=127.0.0.1:6379]
```

```
VENUE    MARKET TYPE  SYMBOL    CHANNEL      STATUS   AGE    SOURCE     TTL   PUBLISH SEQ
BINANCE  PERP_LINEAR  BTC_USDT  funding      HEALTHY  745ms  WEBSOCKET  2.3s  15
BINANCE  PERP_LINEAR  BTC_USDT  index_price  HEALTHY  746ms  WEBSOCKET  2.3s  15
BINANCE  PERP_LINEAR  BTC_USDT  mark_price   HEALTHY  746ms  WEBSOCKET  2.3s  15
```

## Documentation

[`docs/`](docs/README.md) explains the code. Start with
[`docs/architecture.md`](docs/architecture.md) — the shape, the dependency rule,
the dataflow for one message, and the decisions table. There is one document per
package.
