Covers: M1 · whole repository

A market-data price service: connect to one exchange over public websockets, normalize, publish to Redis. One process per venue, selected by `--exchange`.

**Scope, permanently:** perpetual linear mark price, index price and funding. Order books and trades were dropped at M1.

## Shape

| Pattern | Where | Status |
|---|---|---|
| Outbound port | `publish.Publisher`; `RedisPublisher` is the only implementation | built |
| Inbound port | `core.Adapter`; `internal/adapter/binance` is the only implementation | built |
| Stateless pipeline | producer → envelope → marshal → Redis write; nothing stored in between | built |
| Shared-nothing by venue | one process, one venue file, nothing shared across venues | built |
| Supervision tree | goroutines started directly, joined on a `sync.WaitGroup`; a dead socket ends the process | M2 |

Not Clean/Onion (no domain model to protect), not event sourcing (current value, not a log), not layered.

## Dependency rule

`internal/core` imports nothing local except `gen/manoochv1`. A Redis client or venue package there would become an import cycle the moment an adapter names an instrument.

```mermaid
graph TD
  feed["cmd/manooch-feed"] --> adapter["internal/adapter"]
  feed --> synth["internal/synth"]
  feed --> obs["internal/obs"]
  cli["cmd/manooch-tap<br/>cmd/manooch-status"] --> publish["internal/publish"]
  adapter --> binance["internal/adapter/binance"]
  adapter --> config["internal/config"]
  binance --> transport["internal/transport"]
  binance --> publish
  binance --> price["pkg/price"]
  transport --> core["internal/core"]
  synth --> config
  synth --> publish
  config --> core
  config --> price
  publish --> core
  publish --> obs
  core --> gen["gen/manoochv1"]
```

`gen/manoochv1` is imported by most packages; only `core`'s edge is drawn. `core` still imports nothing local but `gen/manoochv1`, `core.Adapter` included — which is why `Message.Key` is a plain string an adapter fills with `publish.Key`.

## One message

```mermaid
sequenceDiagram
  participant S as synth.runStream
  participant P as publish.RedisPublisher
  participant R as Redis
  S->>S: build payload + envelope (venue, instrument, times, seq, source, status)
  S->>P: Publish(ctx, publish.Key(...), msg, ttl)
  P->>P: stamp publish_seq, instance_id, schema_version, publish_time_ns
  P->>R: SET key bytes PX ttl
  P->>R: PUBLISH key bytes
```

An M1 adapter replaces `synth`; everything from `Publish` rightward is unchanged.

## Redis layout

```
Manooch:{VENUE}:{MARKET_TYPE}:{SYMBOL}:{channel}
Manooch:{VENUE}:venue:{subject}
```

One pipelined round trip per message: `SET key bytes PX ttl_ms` then `PUBLISH key bytes`. The same string is key and channel; Redis keeps those namespaces separate.

The `SET` makes freshness a property of the data — key present means fresh, key absent means stale — with no second timestamp to drift. A key's TTL is its channel's `quirks.cadence` times `health.ttl_multiplier`.

## Decisions

| Decision | Choice | Why |
|---|---|---|
| Topology | One process per venue | A venue's reconnect storm or ban cannot touch another venue. |
| Transport | Pub/Sub + last-value keys, not Streams | Consumers need current price, not history. |
| Wire format | protobuf, `gen/` committed | Field numbers make additive change safe; consumers never need `protoc`. |
| Numerics | `int64` fixed-point, never `float64` | 53 mantissa bits cannot hold `68432.15` and `0.00000000012` exactly. |
| Freshness | Redis key TTL | One mechanism carries data and liveness, so they cannot disagree. |
| Eviction | `noeviction` | Any eviction policy silently deletes the keys that mean "alive". |
| Drop detection | `publish_seq` + `instance_id` | Pub/Sub is fire-and-forget; a seq gap is the only evidence, and `instance_id` separates it from a restart. |
| Frameworks | None | `net/http.ServeMux` and `flag` suffice. |
| Config reload | Restart only | Behaviour that changes under a running process cannot be reconstructed. |
| Scope | Perp mark price only; books and trades deleted | Two of four channels carried all the depth and sequencing complexity and none of the value this service exists for. |
| Venue boundary | One `core.Adapter` per venue, holding no stream state | Supervision can change without touching a venue package; state in the adapter would make that a rewrite. |
| Parse purity | `Parse` is a pure function of `(frame, recvNs)` | Fixture replay tests nothing otherwise, and a message that differs between identical frames cannot be reasoned about. |
| Cadence | Per channel, in the venue file | KuCoin funding is 60s against 1s mark price; one number would expire the slower key between updates. |
| Missing data | Skip the message, never substitute a zero | Zero is a real funding rate; an empty one published as zero is a number a strategy trades on. |
| Failure mode | Log at ERROR and exit on a dead socket | Louder than retrying quietly, and the restart policy stays with the operator until a supervisor can say what it is doing. |

## Milestone map

| Path | State |
|---|---|
| `pkg/price`, `internal/{core,config,publish,obs,synth}`, `cmd/*` | built |
| `internal/adapter/` | built — `binance`, plus the shared conformance suite |
| `internal/transport/` | built — one websocket connection; reconnect is M2 |
| `testdata/binance/` | built — one raw frame per case, beside its golden |
| `internal/fallback/` | empty — REST polling on expired-key events, M2 |
| `internal/metadata/` | empty — instrument metadata refresh, M3 |

Config sections `fallback`, `supervisor`, `metadata` and `rate_limit` parse and validate but nothing reads them. `Adapter.FetchOnce` is implemented and tested but unused by the daemon; `FetchMetadata` returns `core.ErrNotImplemented`.

Not built, and deliberately not designed for: reconnect, backoff, circuit breaking, the stream supervisor, TTL-driven health transitions, fallback activation, the rate limiter, a second venue.
