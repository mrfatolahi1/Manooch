Covers: M0 · `deploy/`, `Makefile`, `go.mod`, `.dockerignore`

## Purpose

The container image, the Redis configuration it assumes, and the build entry points.

## Files

| Path | Holds |
|---|---|
| `deploy/Dockerfile` | Two stages: `golang:1.27-alpine` builds all three binaries with `CGO_ENABLED=0`, `alpine:3.22` runs them as uid 10001. Entrypoint `manooch-feed` |
| `deploy/docker-compose.yml` | `redis` and `feed-binance`; one service block per venue |
| `deploy/redis.conf` | Six directives, each load-bearing — see below |
| `Makefile` | `proto`, `build`, `test`, `test-integration`, `lint`, `validate`, `run`, `up`, `down`, `clean` |
| `go.mod` / `go.sum` | Module `github.com/you/manooch`, Go 1.27.0, seven direct dependencies |
| `.dockerignore` | Keeps `.git`, `bin/`, `.local/`, `testdata/raw/` out of the build context |

## Key settings

`deploy/redis.conf`:

| Directive | Value | Why |
|---|---|---|
| `appendonly` / `save` | `no` / `""` | All data is ephemeral; the cache rebuilds from the venue within one cadence. Persistence would add fsync latency on the publish path for nothing |
| `maxmemory` | `2gb` | |
| `maxmemory-policy` | `noeviction` | Any eviction policy would silently delete last-value keys under pressure, and a missing key is exactly how Manooch says "stale" — a healthy stream would read as expired. `noeviction` returns a write error instead, which `publish.onWriteError` counts and logs |
| `notify-keyspace-events` | `Ex` | Expired-key events; `internal/publish/integration_test.go` asserts they fire, and M2's fallback will consume them |
| `client-output-buffer-limit pubsub` | `256mb 64mb 60` | The default 32mb/8mb/60 is crossed by a book firehose, and Redis disconnects an over-budget subscriber with no notice to either side. Raising it makes that rarer, not impossible — consumers still have to watch `publish_seq` |

`deploy/docker-compose.yml`: `redis` publishes `127.0.0.1:6379:6379` so the CLI tools can run from the host, and has a `redis-cli ping` healthcheck. `feed-binance` builds from the repo root, mounts `../config` read-only at `/etc/manooch`, waits on `service_healthy`, runs with `--synthetic`, has a `wget` healthcheck against `/healthz`, `mem_limit: 512m` and `restart: unless-stopped`. It publishes no ports: the admin surface stays inside the container.

Makefile notes: `lint` runs `go vet` plus a `gofmt -l` check. `run` builds `.local/config` from `config/` with `redis.addr` rewritten to `127.0.0.1:6379`, because the committed default points at the compose service name. `test-integration` passes `-tags=integration`, which needs Docker.

## How it is used

`make up` builds the image and starts both services. `make build` produces `bin/manooch-feed`, `bin/manooch-status`, `bin/manooch-tap`. `make proto` regenerates `gen/manoochv1/` and needs `protoc` and `protoc-gen-go` on `PATH`.

## Rules

- **Do not change `maxmemory-policy` away from `noeviction`.** `TestNoEvictionSurfacesWriteErrors` asserts a write against an exhausted instance returns an OOM error; under any eviction policy it would succeed while the data quietly disappeared.
- **Keep `notify-keyspace-events` including `E` and `x`.** `TestKeyExpiresAndNotifies` subscribes to `__keyevent@0__:expired`; without the flag the event never fires and M2's fallback has no trigger.
- **Do not publish the feed's port in compose.** `/debug/pprof` will hand out a heap dump to anyone who can reach it.
- **Keep `CGO_ENABLED=0`.** The runtime image is Alpine (musl) and the build image's libc need not match; a dynamically linked binary would fail at start with a link error rather than at build time.
- **Add a dependency only if it is on the list in the repository `README.md`.** Everything else in `go.mod` is transitive.
- **Alpine, not distroless, on purpose.** A shell, `wget` and `nslookup` in the image are worth more than the megabytes when debugging a feed at three in the morning — and the compose healthcheck uses `wget`.

## Not here

- Commands and their output: the repository `README.md`.
- Config file contents: `docs/config.md`.
- What the integration tests assert: `docs/publish.md`.
