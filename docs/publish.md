Covers: M0 · `internal/publish`

## Purpose

The Redis key scheme and the only write path onto it. Producers depend on the `Publisher` interface, not on a Redis client.

## Files

| Path | Holds |
|---|---|
| `internal/publish/keys.go` | Key construction (`Key`, `VenueKey`, `MatchPattern`) and parsing (`ParseKey`, `KeyParts`) |
| `internal/publish/publisher.go` | `Publisher`, `Options`, `RedisPublisher`, envelope stamping, the pipelined write, metrics and error handling |
| `internal/publish/decode.go` | `NewMessage` and `Decode`: channel → concrete message type |
| `internal/publish/keys_test.go` | `TestKey`, `TestVenueKey`, `TestMatchPattern`, `TestParseKeyRoundTrip`, `TestParseKeyRejects` (18 malformed keys) |
| `internal/publish/integration_test.go` | Build tag `integration`. Real Redis via `dockertest`: SET+PUBLISH both land, TTL expiry fires `__keyevent@0__:expired`, `ttl==0` has no expiry, `publish_seq` gap-free over 10k messages, `instance_id` changes while `publish_seq` resets, and a `noeviction` instance over `maxmemory` returns an error |

## Key types and functions

| Symbol | Signature | Notes |
|---|---|---|
| `Publisher` | `interface { Publish(ctx, key string, msg proto.Message, ttl time.Duration) error; Close() error }` | |
| `RedisPublisher` | struct | The only implementation; `var _ Publisher = (*RedisPublisher)(nil)` |
| `NewRedis` | `func(ctx context.Context, opts Options) (*RedisPublisher, error)` | Pings and fails if Redis is absent |
| `Options` | struct | `Addr`, `DB`, `DialTimeout`, `ReadTimeout`, `PoolSize`, `Venue`, `InstanceID`, `SchemaVersion`, `Metrics`, `Logger`, `Now` (defaults to `time.Now`) |
| `RedisPublisher.Publish` | see `Publisher` | |
| `RedisPublisher.Close` | `func() error` | Closes the pool |
| `Key` | `func(venue string, mt pb.MarketType, symbol string, ch pb.Channel) string` | Upper-cases venue and symbol itself |
| `VenueKey` | `func(venue, subject string) string` | `Manooch:BINANCE:venue:health` |
| `MatchPattern` | `func(venue string) string` | `Manooch:BINANCE:*`, or `Manooch:*` when venue is `""` |
| `ParseKey` | `func(string) (KeyParts, error)` | Validates every component |
| `KeyParts` | struct `{Venue, Symbol, Subject string; MarketType pb.MarketType; Channel pb.Channel; VenueScoped bool}` | `String()` rebuilds the key |
| `NewMessage` | `func(pb.Channel) (proto.Message, error)` | Empty message of the type that channel carries |
| `Decode` | `func(ch pb.Channel, b []byte) (proto.Message, *pb.Envelope, error)` | |
| `Prefix`, `VenueScope`, `SubjectHealth`, `SubjectRateLimit` | `= "Manooch"`, `"venue"`, `"health"`, `"ratelimit"` | |
| `enveloped` | `interface { proto.Message; GetEnv() *pb.Envelope }` | Unexported. Every payload satisfies it because `Envelope` is field 1 |
| `errLogInterval` | `= time.Second` | Unexported. Caps the write-error log rate |

### What `Publish` does

1. Asserts `msg` implements `enveloped` and that `env != nil` and `env.Status != STATUS_UNSPECIFIED` — errors, not writes, if any fails.
2. Under `p.mu`: increments `seq[key]`, stamps `PublishSeq`, `InstanceId`, `SchemaVersion`, `Venue` (only if empty) and `PublishTimeNs`, then `proto.Marshal`.
3. Outside the lock, one pipeline: `SET key bytes PX ms` (or `SET key bytes` when `ttl <= 0`) then `PUBLISH key bytes`, one `Exec`.
4. On error, `onWriteError`; on success, `observe` records `MessagesPublished` plus the two latency histograms.

A `ttl` above zero but under a millisecond is raised to `1`, because `PX 0` is rejected by Redis.

`observe` takes its labels from the envelope rather than re-parsing the key. `env.Instrument == nil` (a venue-scoped message) yields `market_type="venue"` and an empty symbol. Negative latencies are dropped rather than observed: they mean the local clock is behind the venue's, which is a skew signal, not a measurement.

## How it is used

`cmd/manooch-feed.run` builds one `RedisPublisher` from `cfg.Redis` and passes it to `synth.New` as a `Publisher`. `internal/synth.runStream` calls `Key` once per stream at startup and `Publish` on every tick. `cmd/manooch-tap` and `cmd/manooch-status` use `ParseKey`, `MatchPattern` and `Decode`, and talk to Redis with their own clients — they do not use `Publisher`.

## Rules

- **Build every key through `Key` or `VenueKey`.** A key with a typo is written successfully, published successfully, and read by nobody; there is no error anywhere in that sequence, and the stream simply looks dead to its consumer while every metric here says healthy.
- **Set `publish_time_ns` only in `Publish`, immediately before the write.** Set anywhere earlier it measures when we decided to publish rather than when we did, which is exactly the gap an operator is trying to see.
- **Marshal inside the mutex.** `env` is a pointer into the caller's message; a concurrent `Publish` of the same message would race with the stamping. Redis I/O stays outside the lock, so two goroutines publishing the same key can still arrive out of order — in practice one goroutine owns one stream.
- **Never log per message.** `onWriteError` is the only log in the write path and it is capped at one line per second per publisher: Redis being down is one fact, not one fact per message.
- **A failed or unmarshalable publish leaves a gap in `publish_seq`, deliberately.** The sequence is not rolled back, because a message that did not go out is a message the consumer must know it missed.
- **Do not add per-message logging or per-message key parsing.** `observe` reads the envelope precisely to avoid `ParseKey` on the hot path.

## Not here

- Redis server settings that make TTL and OOM behave as assumed: `docs/deploy.md`.
- Which envelope fields the producer must fill before calling `Publish`: `docs/contract.md`.
- The TTL value for a given channel: `docs/synth.md` (`schedule`) and `docs/config.md` (`Config.TTL`).
