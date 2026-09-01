Covers: M3 · `cmd/manooch-feed`, `cmd/manooch-tap`, `cmd/manooch-status`

Three binaries. Flags come from the standard library `flag` package; there is no CLI framework. No test files.

| File | Holds |
|---|---|
| `manooch-feed/main.go` | `run` and its steps — `parseFlags`, `dialRedis`, `serveAdmin`, `shutdown`, `printResolved` |
| `manooch-feed/feed.go` | `planProducers`, `newLimiter`, `producers.start` — wiring health, metadata, fallback and the supervisor |
| `manooch-feed/http.go` | `newMux` — the admin surface |
| `manooch-tap/main.go` | Subscribe, decode, gap detection, `summarize`, `--raw` output |
| `manooch-status/main.go` | `scanKeys`, `readRows`, table formatting, colour |

## manooch-feed

`--exchange` (required, must already be upper case), `--config` (`./config`), `--validate`.

`run` orchestrates: `parseFlags` → `config.Load` → if `--validate`, print and return → logger and metrics → `uuid.NewString()` for `instance_id` → `planProducers` → `dialRedis` → `serveAdmin` → `producers.start` → block on `os.Interrupt`/`SIGTERM` → `shutdown`, which stops HTTP, waits for the producers and closes Redis under a 10s deadline.

`planProducers` runs **before** Redis is dialled or a port is bound, and opens nothing: an unknown venue or a stream the adapter cannot serve fails with nothing to clean up. It builds the rate limiter, the venue adapter and its socket plans. The limiter is built there because the adapter needs one, so its publisher arrives later through `AttachPublisher` once Redis is up.

`producers.start` returns a `WaitGroup` over three long-lived goroutines: `health.Tracker.Run` (the heartbeat), `metadata.Refresher.Run` (the instrument refresh), and one that waits on `Refresher.WaitReady` before starting `fallback.Watcher.Run` and `supervisor.Process.Run`. **Nothing streams before metadata lands** — a price at unknown precision is a number nobody can size an order against — and health runs first so the venue publishes `STALE` while the fetch is still failing.

**Nothing here ends the process.** A dead socket redials with jittered backoff behind a circuit breaker, a failed stream relaunches on its own, an expired key is served over REST, and a goroutine that will not come back is counted rather than escalated. Only a signal stops the run.

The watcher and the supervisor each need the other — `OnExpired` → `Process.KeyExpired`, `OnMessage` → `Watcher.Note` — so both are constructed before either starts. `fallback.enabled: false` logs a warning at startup: an expired key then stays expired.

Routes: `GET /healthz` (JSON: status, venue, instance_id, uptime), `GET /metrics`, `/debug/pprof/*`. **No market-data route** — consumers read Redis, and a second path would be a second contract that disagrees with the first the moment either changes.

## manooch-tap

`--pattern` (default `Manooch:*`), `--redis`, `--db`, `--json`, `--raw`, `--out`.

One line per message. It also prints `!!` lines when `publish_seq` jumps (messages dropped on the bus) or `instance_id` changes (the feed restarted) — Redis reports neither. `summarize` prints `next=-` and `interval=-` for a funding message whose venue supplied neither, rather than a settlement in 1970. `--raw` writes payloads to `<out>/<key with ':' → '_'>-<seq>.bin`. Those are our own protobuf messages off Redis — venue frames for `testdata/<venue>/` are captured from the wire or hand-written from the venue's docs.

## manooch-status

`--venue`, `--redis`, `--db`, `--no-color`. Reads keys, never subscribes. It is the main operational view.

| Column | Source |
|---|---|
| `STATUS`, `AGE`, `SOURCE`, `PUBLISH SEQ` | The key's own envelope |
| `TTL` | `PTTL`; `-1` prints `none`, a key that vanished between `SCAN` and `GET` prints `expired between scan and read` |
| `RESTARTS` | `stream_restart_count` from that instrument's health key |
| `REASON` | `status_reason`, or why the row could not be read |

The health and rate-limit keys are rows too. `Manooch:{VENUE}:venue:ratelimit` prints its budgets in `REASON` as `rest_weight=6/2000`. `Manooch:{VENUE}:venue:health` sorts first within its venue and carries socket state, `skew=`, `reconnects=` and, when non-zero, `leaked=` in `REASON`: everything under it is conditional on the socket being up, so reading it second is reading it backwards. `RESTARTS` is per instrument rather than per channel, because the health key is per instrument and a channel's own status already rides inside its data key.

Non-healthy rows are prefixed `!`/`!!` so they survive a pipe; colour only when stdout is a character device and `NO_COLOR` is unset.

## Rules

- **Bind with `net.Listen` before starting the goroutine.** `ListenAndServe` inside a goroutine turns a port clash into a log line in a process that keeps running blind, with no metrics and no `/healthz`.
- **`--validate` must open nothing.** It returns before `NewRedis` and `net.Listen`; new startup work goes after that return. It does build the adapter, because only the adapter knows what a venue calls an instrument, and a printed symbol nothing will subscribe to is worse than none.
- **Resolve the venue before opening anything.** A process that runs while publishing nothing looks exactly like a venue that went quiet.
- **Close the connection to unblock a read.** Cancelling the context does not. `internal/supervisor` owns that procedure now; see `supervisor.md`.
- **`SCAN`, never `KEYS`.** `KEYS` blocks the whole keyspace and stalls every publisher behind it.
- **Close Redis after the `WaitGroup` drains**, not before, or every in-flight publish fails and logs.
- **Keep the admin surface on loopback** — `/debug/pprof` hands a heap dump to anyone who can reach it.
