Covers: M3 · whole repository

A market-data price service: connect to one exchange over public websockets, normalize, publish to Redis. One process per venue, selected by `--exchange`. Two venues are served, Binance USD-M and KuCoin futures, and the service is complete.

**Scope, permanently:** perpetual linear mark price, index price and funding. Order books and trades were dropped at M1.

## Shape

| Pattern | Where | Status |
|---|---|---|
| Outbound port | `publish.Publisher`; `RedisPublisher` is the only implementation | built |
| Inbound port | `core.Adapter`; `binance` and `kucoin` implement it | built |
| Stateless pipeline | producer → envelope → marshal → Redis write; nothing stored in between | built |
| Shared-nothing by venue | one process, one venue file, nothing shared across venues | built |
| Supervision tree | `internal/supervisor`; stream-level recovery, no process exit | built |

Not Clean/Onion (no domain model to protect), not event sourcing (current value, not a log), not layered.

## Supervision tree

```
process (one per venue)
├── health heartbeat        internal/health — ticker, transitions, the health keys
├── metadata refresher      internal/metadata — the startup gate, then an hourly poll
├── http server             cmd/manooch-feed — loopback only
└── once metadata has landed
    ├── fallback watcher    internal/fallback — expiry subscriber + EXISTS sweep
    └── socket supervisor   internal/supervisor — one per core.SocketPlan
        ├── read loop       its own goroutine: it is what parks in conn.Read
        └── stream goroutine one per (market_type, symbol, channel)
```

Health runs before the gate on purpose: it is what publishes `STALE` with
`status_reason: "metadata unavailable"` while the first fetch is still failing.
Until that fetch succeeds no socket is dialled at all.

Nothing in it exits the process. One stream failing restarts that stream; a read
error or enough of a socket's keys expiring together redials that socket; a
goroutine that will not return is counted, logged and abandoned. The trade is
deliberate — no restart loops, and therefore no reconnect storms, at the cost of
a leak an operator eventually has to notice. `supervisor.md`, `health.md` and
`fallback.md` carry the detail.

## Dependency rule

`internal/core` imports nothing local except `gen/manoochv1`. A Redis client or venue package there would become an import cycle the moment an adapter names an instrument.

```mermaid
graph TD
  feed["cmd/manooch-feed"] --> adapter["internal/adapter"]
  feed --> supervisor["internal/supervisor"]
  feed --> fallback["internal/fallback"]
  feed --> metadata["internal/metadata"]
  feed --> ratelimit["internal/ratelimit"]
  feed --> obs["internal/obs"]
  cli["cmd/manooch-tap<br/>cmd/manooch-status"] --> publish["internal/publish"]
  adapter --> venues["internal/adapter/binance<br/>internal/adapter/kucoin"]
  adapter --> config["internal/config"]
  adapter --> ratelimit
  venues --> transport["internal/transport"]
  venues --> ratelimit
  venues --> publish
  venues --> price["pkg/price"]
  supervisor --> health["internal/health"]
  supervisor --> transport
  fallback --> health
  fallback --> publish
  metadata --> publish
  metadata --> transport
  ratelimit --> publish
  health --> publish
  transport --> core["internal/core"]
  config --> core
  config --> price
  publish --> core
  publish --> obs
  core --> gen["gen/manoochv1"]
```

`supervisor` and `fallback` never import each other: they are joined in
`cmd/manooch-feed` by two function fields, `OnExpired` and `OnMessage`. Neither
imports `config` either — both are handed resolved values, the way an adapter
is.

`gen/manoochv1` is imported by most packages; only `core`'s edge is drawn. `core` still imports nothing local but `gen/manoochv1`, `core.Adapter` included — which is why `Message.Key` is a plain string an adapter fills with `publish.Key`.

## One message

```mermaid
sequenceDiagram
  participant L as supervisor read loop
  participant S as stream goroutine
  participant H as health.Tracker
  participant P as publish.RedisPublisher
  participant R as Redis
  L->>L: conn.Read, stamp recv_time_ns, Adapter.Parse
  L->>S: deliver the newest message for this key
  S->>H: Received(spec)
  H-->>S: status, status_reason
  S->>P: Publish(ctx, publish.Key(...), msg, ttl)
  P->>P: stamp publish_seq, instance_id, schema_version, publish_time_ns
  P->>R: SET key bytes PX ttl
  P->>R: PUBLISH key bytes
```

The REST fallback enters at the same `Publish` with `SOURCE_REST`; the metadata
refresher and the rate limiter enter there with their own envelopes. Everything
from `Publish` rightward is identical for all of them.

## Redis layout

```
Manooch:{VENUE}:{MARKET_TYPE}:{SYMBOL}:{channel}   mark_price, index_price, funding, metadata, health
Manooch:{VENUE}:venue:{subject}                    health, ratelimit
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
| Failure mode | Supervise; the process never exits on a stream or socket failure | A restart drops every other stream on the venue to fix one, and loses the state that says which. |
| Recovery unit | The stream goroutine, then the socket | The smallest thing that can be wrong on its own, so recovering it disturbs nothing else. |
| Leaked goroutines | Counted and held at `DEGRADED`, never self-killed | Go cannot kill a goroutine; with no self-kill the only alternative to visibility is silence. |
| Backoff | Exponential with full jitter | Deterministic backoff makes concurrent failures retry in lockstep, which is the reconnect storm most IP bans come from. |
| Circuit breaker | Stop attempting entirely after 10 consecutive failures | Backoff alone still knocks every minute forever on a venue that is refusing us. |
| Reconnect timing | Proactive at `connection.max_age` | A planned redial is a handover; an unplanned one is a gap nobody chose the moment of. |
| Fallback trigger | The expired key itself, plus an `EXISTS` sweep | A second staleness clock beside the TTL is a second answer that eventually disagrees. |
| Fallback labelling | Same channel and key, `SOURCE_REST` + `STATUS_DEGRADED` | Consumers keep one code path, and the difference is visible to anyone who checks. |
| Health heartbeat | Republish every interval regardless of change | Pub/Sub is fire-and-forget, so silence must never mean both "quiet" and "dead". |
| Metadata | A startup dependency: no metadata, no market data | A price at unknown precision, with no contract multiplier, is a number a consumer would size an order from and get wrong. |
| Metadata refresh | Interval poll only, whole set republished each cycle | The venues publish no change feed, and a consumer that missed the one message announcing a tick change would never hear about it again. |
| Tick size | Informational, never a scaling factor | Scaling is global, so a venue moving a tick reinterprets nothing already cached. That is what the global scale bought over a per-instrument exponent. |
| Rate limiting | In-process token bucket, a fraction of the published limit | The order service shares this IP and cannot be seen from here; coordinating would make it depend on us. |
| Rate-limit denial | Fail closed — the call does not happen, the stream says so | Proceeding on "it is probably fine" is how an IP ban is discovered rather than avoided. |
| Shared limiter | A Redis key convention, documented and not built | A library would be a dependency; a convention is one both sides can implement alone. One process per venue does not need it yet. |
| Venue timestamps | `exchange_time_is_send_time`, defaulting to false | An event time differenced against arrival is the age of the value, not a clock skew. A missing signal beats a wrong one. |
| Cadence | Measured against the venue, not read from its docs | KuCoin's `granularity: 1000` is the granularity of the value, not a promise to send one; trusting it puts a working stream permanently on REST. |

## Milestone map

Complete. Every path below is built and every config section is read.

| Path | State |
|---|---|
| `pkg/price`, `internal/{core,config,publish,obs}`, `cmd/*` | built |
| `internal/adapter/` | built — `binance` and `kucoin`, plus the shared conformance suite |
| `internal/transport/` | built — one websocket connection, backoff, circuit breaker |
| `internal/supervisor/` | built — the restart procedure and both escalation tiers |
| `internal/health/` | built — status computation, transition and heartbeat publishing |
| `internal/fallback/` | built — expiry watcher, sweep backstop, REST poller |
| `internal/metadata/` | built — the startup gate, the hourly refresh, the change log |
| `internal/ratelimit/` | built — `LocalLimiter`; `RedisLimiter` documented, not built |
| `internal/core/coretest/` | built — the `core.Conn` and `core.Adapter` fault-injection doubles |
| `testdata/{binance,kucoin}/` | built — one raw frame per case, beside its golden |

Deliberately not built: `RedisLimiter`, venue-level rollup status, adaptive
staleness thresholds, spot/margin/inverse/dated support, order books, trades, a
third venue, historical storage, an HTTP endpoint serving market data.

## Did the second venue reach `internal/core`?

The question M3 existed to answer. **`internal/core` was not touched to make
KuCoin work.** `RunAdapterConformance` and `RunAdapterDeterminism` run against
`testdata/kucoin/` with no venue-specific branch anywhere in them.

Everything KuCoin does differently was absorbed inside its own package: a public
REST bootstrap before the socket can be opened, a client-side ping the connection
owns, two subjects on one topic at two cadences, unquoted JSON numbers, and a
symbol spelling that shares no rule with Binance's. `Dial` already promised a
connection whose subscriptions were live, which was exactly the shape a
bootstrapping venue needed.

Four things did change, and none of them was a workaround:

| Change | Why it was the interface, not the venue |
|---|---|
| `Envelope.exchange_time_is_send_time` | The pipeline assumed every venue timestamp was a clock reading. KuCoin stamps a funding rate with its settlement instant, and differencing that against arrival reported a four-hour clock skew on a healthy venue. Found by a live smoke test, not a fixture. |
| `supervisor.ConnectGrace` | The escalation counted expiries caused by the reconnect that had just fixed them — a loop with no exit on any venue whose dial outlasts the shortest TTL. Binance's direct dial had hidden it. |
| `endpoints.ws` accepts `https` | Not every venue lets you dial the socket directly. The alternative was a config key holding an address nothing reads. |
| `config.VenueSymbol` deleted | A fallback rule in the config package had to be right for every venue at once, which stopped being possible when the second one spelled bitcoin `XBT`. `--validate` now asks the adapter. |

The two found live are the ones worth remembering: fixtures prove that a frame
we have seen becomes what we expect, and nothing more. What a venue's timestamps
*mean*, and how often it really pushes, only show up against the venue.
