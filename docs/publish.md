Covers: M2 · `internal/publish`

The Redis key scheme and the only write path onto it. Producers depend on the `Publisher` interface, not on a Redis client.

| File | Holds |
|---|---|
| `keys.go` | `Key`, `VenueKey`, `MatchPattern`, `ParseKey`, `KeyParts` |
| `publisher.go` | `Publisher`, `Options`, `RedisPublisher`, envelope stamping, the write, metrics, `Redis` |
| `decode.go` | `NewMessage` / `Decode`: channel → concrete message type |
| `keys_test.go` | Key construction, round-trips, 18 malformed keys rejected |
| `integration_test.go` | Tag `integration`. Real Redis via `dockertest` |

Full API: `go doc ./internal/publish`.

## What `Publish` does

1. Requires an envelope with a status set — otherwise an error, not a write.
2. Under the mutex: increments `seq[key]`, stamps `publish_seq`, `instance_id`, `schema_version`, `publish_time_ns`, then marshals.
3. Outside the lock, one pipeline: `SET key bytes PX ms` (or plain `SET` when `ttl <= 0`) then `PUBLISH key bytes`, one `Exec`.
4. Metrics on success from the envelope; on failure a counter plus a log capped at one line per second.

A `ttl` above zero but under a millisecond is raised to `1`, because Redis rejects `PX 0`. Negative latencies are dropped rather than observed — they mean the local clock is behind the venue's.

## TTL is the freshness signal

`ttl = quirks.cadence[channel] × health.ttl_multiplier`, from `config.TTL`. The `SET … PX` makes the last-value cache and the liveness signal one object: key present means fresh, key absent means stale, and there is no second timestamp that could disagree with the first.

That is also the whole trigger for M2's recovery. An expired key fires `__keyevent@0__:expired`, which `internal/fallback` reacts to; nothing polls a separate staleness clock. What a TTL cannot say — fresh but sourced from REST, socket reconnecting — rides inside the value as `status` and `status_reason`, which `internal/supervisor` stamps immediately before each `Publish`. See `health.md`.

`RedisPublisher.Redis` exposes the client for the two reads the write path does not do: the fallback watcher's keyspace subscription and its pipelined `EXISTS` sweep. It is not a way to write.

The integration suite asserts: SET and PUBLISH both land, TTL expiry fires `__keyevent@0__:expired`, `ttl==0` leaves no expiry, `publish_seq` is gap-free across 10k messages, `instance_id` changes while `publish_seq` resets, and an over-`maxmemory` `noeviction` instance returns an error.

## Rules

- **Build every key through `Key` or `VenueKey`.** A typo'd key is written and published successfully and read by nobody — no error anywhere, while the stream looks dead to its consumer and healthy to every metric here.
- **Set `publish_time_ns` only in `Publish`, immediately before the write.** Earlier, it measures when we decided to publish rather than when we did — the exact gap an operator is looking for.
- **Marshal inside the mutex.** `env` is a pointer into the caller's message; a concurrent publish of the same message would race with the stamping. Redis I/O stays outside, so two goroutines on one key can still arrive out of order — in practice one goroutine owns one stream.
- **A failed publish leaves a `publish_seq` gap deliberately.** The sequence is not rolled back: a message that did not go out is one the consumer must know it missed.
- **Never log per message, and never `ParseKey` on the hot path.** `observe` reads labels from the envelope for that reason.
- **Never write through `Redis`.** `Publish` is the only place the envelope is stamped and the only place `publish_time_ns` is set; a second write path would produce messages with neither.
