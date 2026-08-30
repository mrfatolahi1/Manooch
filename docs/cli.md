Covers: M0 · `cmd/manooch-feed`, `cmd/manooch-tap`, `cmd/manooch-status`

## Purpose

Three binaries: the venue daemon, a Pub/Sub tap and a keyspace status table. Flags are parsed with the standard library `flag` package; there is no CLI framework.

## Files

| Path | Holds |
|---|---|
| `cmd/manooch-feed/main.go` | `run` — flags, config load, `--validate`, startup, shutdown; `printResolved` |
| `cmd/manooch-feed/http.go` | `newMux` — the admin surface |
| `cmd/manooch-tap/main.go` | `run`, the `tap` struct, `summarize`, `writeRaw` |
| `cmd/manooch-status/main.go` | `run`, `scanKeys`, `readRows`, `print`, the `row` struct and formatting helpers |

No test files.

## Key types and functions

| Symbol | Signature | Notes |
|---|---|---|
| `newMux` | `func(m *obs.Metrics, venue, instanceID string, started time.Time) *http.ServeMux` | Routes below |
| `printResolved` | `func(w *os.File, cfg *config.Config, dir string) error` | YAML dump plus the exact Redis keys the config implies |
| `shutdownDeadline` | `= 10 * time.Second` | |
| `tap.handle` | `func(key string, payload []byte, asJSON, raw bool)` | Parse key → decode → gap check → print |
| `tap.seen` | `map[string]seen` where `seen = {publishSeq uint64; instanceID string}` | Per-key drop/restart detection |
| `summarize` | `func(msg any) string` | One line per message type |
| `scanKeys` | `func(ctx, *redis.Client, pattern string) ([]string, error)` | `SCAN` with `COUNT 500`, never `KEYS` |
| `readRows` | `func(ctx, *redis.Client, keys []string) ([]row, error)` | One pipeline of `GET` + `PTTL` per key |
| `colourEnabled` | `func(noColor bool) bool` | False if `--no-color`, if `NO_COLOR` is set, or if stdout is not a character device |

### `manooch-feed`

| Flag | Default | Meaning |
|---|---|---|
| `--exchange` | `""` | Required; must already be upper case or `run` errors |
| `--config` | `./config` | Directory holding `defaults.yaml` and `venues/` |
| `--validate` | `false` | Load, print, exit 0 |
| `--synthetic` | `false` | Run `internal/synth` instead of connecting to a venue |

Startup order in `run`: parse flags → `config.Load` → if `--validate`, print and return → `obs.NewLogger` and `obs.NewMetrics` → `uuid.NewString()` for `instance_id` → `publish.NewRedis` (bounded by `redis.dial_timeout`) → `net.Listen` then `srv.Serve` in a goroutine → optionally start `synth` → block on `signal.NotifyContext` for `SIGINT`/`SIGTERM` → `srv.Shutdown`, wait for the generator, `pub.Close`.

Routes on `newMux`: `GET /healthz` (JSON: `status`, `venue`, `instance_id`, `uptime_seconds`, `uptime`), `GET /metrics`, and `/debug/pprof/` plus `cmdline`, `profile`, `symbol`, `trace`. There is no market-data route: consumers read Redis, and a second data path would be a second contract to keep in sync.

### `manooch-tap`

`--pattern` (default `Manooch:*`), `--redis` (`127.0.0.1:6379`), `--db` (`0`), `--json`, `--raw`, `--out` (`testdata/raw`).

Default output is one line per message: publish timestamp, key, `publish_seq`, status and a `summarize` line. It also prints `!!` lines when `publish_seq` jumps (messages dropped on the bus) or `instance_id` changes (the feed restarted) — Redis reports neither. `--json` emits `{"key":…,"message":…}` per line via `protojson`. `--raw` writes each payload to `<out>/<key with ':' replaced by '_'>-<seq>.bin`.

### `manooch-status`

`--venue` (default: all), `--redis`, `--db`, `--no-color`. Reads keys; never subscribes. Columns: venue, market type, symbol, channel, status, age (now minus `publish_time_ns`), source, TTL, publish seq. `PTTL` of `-1` prints `none`; a key that vanished between the `SCAN` and the `GET` prints `expired between scan and read`. Non-healthy rows are prefixed `!` or `!!` so they survive a pipe, and coloured only when `colourEnabled` says so.

## How it is used

`manooch-feed` is the deployed process; `deploy/docker-compose.yml` runs it with `--synthetic` at M0. The other two are operator tools run against Redis directly. All three import `internal/publish`; only the feed imports `internal/config`, `internal/obs` and `internal/synth`.

## Rules

- **Bind the HTTP listener with `net.Listen` before starting the goroutine.** `ListenAndServe` inside a goroutine turns a port clash into a log line in a process that then runs blind, with no metrics and no `/healthz`.
- **`--validate` must open nothing.** It returns before `publish.NewRedis` and before `net.Listen`, so it is safe to run against production config from anywhere. Any new work added to `run` goes after that return.
- **Use `SCAN`, not `KEYS`.** `KEYS` walks the whole keyspace in one blocking call and stalls every publisher behind it.
- **Close Redis after waiting for the generator, not before.** `pub.Close` runs after the `WaitGroup` drains; closing the pool first would make every in-flight `Publish` fail and log.
- **Keep the admin surface on loopback.** `internal/config.validateHTTP` rejects a non-loopback `service.http.listen`, so `/metrics` and `/debug/pprof` — which will hand out a heap dump to anyone who asks — cannot be reached off-host.

## Not here

- Config keys and validation rules: `docs/config.md`.
- How to run the binaries: the repository `README.md`.
- What the metrics mean: `docs/obs.md`.
