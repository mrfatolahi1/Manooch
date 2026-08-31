Covers: M1 · `cmd/manooch-feed`, `cmd/manooch-tap`, `cmd/manooch-status`

Three binaries. Flags come from the standard library `flag` package; there is no CLI framework. No test files.

| File | Holds |
|---|---|
| `manooch-feed/main.go` | `run` and its steps — `parseFlags`, `dialRedis`, `serveAdmin`, `shutdown`, `printResolved` |
| `manooch-feed/feed.go` | `planProducers`, `producers.start`, the read loop, metrics, parse-error handling |
| `manooch-feed/http.go` | `newMux` — the admin surface |
| `manooch-tap/main.go` | Subscribe, decode, gap detection, `summarize`, `--raw` output |
| `manooch-status/main.go` | `scanKeys`, `readRows`, table formatting, colour |

## manooch-feed

`--exchange` (required, must already be upper case), `--config` (`./config`), `--validate`, `--synthetic`.

`run` orchestrates: `parseFlags` → `config.Load` → if `--validate`, print and return → logger and metrics → `uuid.NewString()` for `instance_id` → `planProducers` → `dialRedis` → `serveAdmin` → `producers.start` → block on `os.Interrupt`/`SIGTERM` → `shutdown`, which stops HTTP, waits for the producers and closes Redis under a 10s deadline.

`planProducers` runs **before** Redis is dialled or a port is bound, and opens nothing: an unknown venue or a stream the adapter cannot serve fails with nothing to clean up. Without `--synthetic` it builds the venue adapter and its socket plans; with it, the generator.

Each plan gets a goroutine running `Dial` → `Read` → `Parse` → `Publish`. **This build does not reconnect**: a dead socket logs at ERROR and cancels the root context, which shuts the process down the same way a signal does. A frame that will not parse is counted, logged at most once a second, and skipped — one bad frame is not a reason to go dark on every other stream, and the keys it would have refreshed expire and report themselves stale.

Routes: `GET /healthz` (JSON: status, venue, instance_id, uptime), `GET /metrics`, `/debug/pprof/*`. **No market-data route** — consumers read Redis, and a second path would be a second contract that disagrees with the first the moment either changes.

## manooch-tap

`--pattern` (default `Manooch:*`), `--redis`, `--db`, `--json`, `--raw`, `--out`.

One line per message. It also prints `!!` lines when `publish_seq` jumps (messages dropped on the bus) or `instance_id` changes (the feed restarted) — Redis reports neither. `--raw` writes payloads to `<out>/<key with ':' → '_'>-<seq>.bin`. Those are our own protobuf messages off Redis — venue frames for `testdata/<venue>/` are captured from the wire or hand-written from the venue's docs.

## manooch-status

`--venue`, `--redis`, `--db`, `--no-color`. Reads keys, never subscribes. `PTTL` of `-1` prints `none`; a key that vanished between `SCAN` and `GET` prints `expired between scan and read`. Non-healthy rows are prefixed `!`/`!!` so they survive a pipe; colour only when stdout is a character device and `NO_COLOR` is unset.

## Rules

- **Bind with `net.Listen` before starting the goroutine.** `ListenAndServe` inside a goroutine turns a port clash into a log line in a process that keeps running blind, with no metrics and no `/healthz`.
- **`--validate` must open nothing.** It returns before `NewRedis` and `net.Listen`; new startup work goes after that return.
- **Resolve the venue before opening anything.** A process that runs while publishing nothing looks exactly like a venue that went quiet.
- **Close the connection to unblock a read.** Cancelling the context does not; `runSocket` runs a watchdog goroutine that calls `Close` when the context ends.
- **`SCAN`, never `KEYS`.** `KEYS` blocks the whole keyspace and stalls every publisher behind it.
- **Close Redis after the `WaitGroup` drains**, not before, or every in-flight publish fails and logs.
- **Keep the admin surface on loopback** — `/debug/pprof` hands a heap dump to anyone who can reach it.
