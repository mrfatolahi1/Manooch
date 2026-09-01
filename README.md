<p align="left">
  <img src="docs/assets/manooch-logo.png" alt="Manooch" width="180">
</p>

# Manooch

A crypto market-data price service. It connects to exchanges over public
websockets, normalizes what it sees into one format, and publishes to Redis for
downstream consumers. One process serves one venue.

Two venues: **Binance USD-M futures** and **KuCoin futures**, one process each,
sharing nothing but Redis. Per symbol it publishes mark price, index price,
funding and instrument metadata, plus a health key and a venue-level health and
rate-limit key. The scope is perpetual linear only — order books and trades are
gone for good.

The point of the service is that it never publishes stale or wrong data without
saying so:

- Every message carries a `status` and, when it is not `HEALTHY`, why.
- Freshness is the Redis key's TTL, derived from the venue's measured cadence,
  so key present means fresh and key absent means stale with no second clock to
  disagree.
- A dropped socket redials with jittered backoff behind a circuit breaker; a
  failed stream restarts on its own; an expired key is served over REST and
  labelled `SOURCE_REST` and `DEGRADED`.
- Numbers are `int64` fixed point end to end. A value below `float64` precision
  round-trips exactly, and one that does not fit the scale is rejected rather
  than rounded.
- No venue value is ever derived. Where KuCoin supplies no next funding time,
  the field stays zero.
- The feed publishes nothing at all until it has the instrument metadata to
  publish it with, and reports `STALE` with `metadata unavailable` until then.

No credentials, public endpoints only. There is no authenticated endpoint and no
order-placing code path anywhere in the repository.

## Bring it up

```sh
make up                 # docker compose: Redis + both feeds
make down
```

Killing one feed leaves the other publishing without a gap — that is what one
process per venue is for:

```sh
docker compose -f deploy/docker-compose.yml kill feed-binance
manooch-status --venue=KUCOIN
```

Redis is published on `127.0.0.1:6379`. To run one feed against the real venue
from a checkout:

```sh
make run                        # BINANCE by default
make run EXCHANGE=KUCOIN        # needs a Redis on 127.0.0.1:6379
```

## Look at the data

```sh
manooch-tap --pattern="Manooch:KUCOIN:*" [--redis=127.0.0.1:6379] [--json] [--raw]
```

```
16:23:37.125  Manooch:BINANCE:PERP_LINEAR:BTC_USDT:mark_price    seq=16     HEALTHY  mark=68432.15
16:23:37.125  Manooch:BINANCE:PERP_LINEAR:BTC_USDT:index_price   seq=16     HEALTHY  index=68431.25
16:23:37.126  Manooch:BINANCE:PERP_LINEAR:BTC_USDT:funding       seq=16     HEALTHY  rate=0.00038167 next=2026-09-01T00:00:00Z interval=-
```

```sh
manooch-status [--venue=KUCOIN] [--redis=127.0.0.1:6379]
```

```
VENUE   MARKET TYPE  SYMBOL    CHANNEL      STATUS   AGE    SOURCE     TTL       PUBLISH SEQ  REASON
KUCOIN  venue        -         health       HEALTHY  916ms  -          2.1s      71           skew=-507ms reconnects=0
KUCOIN  venue        -         ratelimit    HEALTHY  35.8s  -          9m24s     73           rest_weight=3/2000 ws_connect=0/75
KUCOIN  PERP_LINEAR  BTC_USDT  funding      HEALTHY  35.6s  WEBSOCKET  2m24s     26
KUCOIN  PERP_LINEAR  BTC_USDT  index_price  HEALTHY  323ms  WEBSOCKET  29.7s     42
KUCOIN  PERP_LINEAR  BTC_USDT  mark_price   HEALTHY  323ms  WEBSOCKET  29.7s     42
KUCOIN  PERP_LINEAR  BTC_USDT  metadata     HEALTHY  1m5s   REST       1h58m54s  1
```

## Documentation

[`docs/`](docs/README.md) explains the code. Start with
[`docs/architecture.md`](docs/architecture.md) — the shape, the dependency rule,
the dataflow for one message, the decisions table, and what adding the second
venue actually cost. There is one document per package.
