Covers: M3 · `deploy/`, `Makefile`, `go.mod`, `.dockerignore`, `.gitignore`

| File | Holds |
|---|---|
| `deploy/Dockerfile` | `golang:1.27-alpine` builds all three binaries `CGO_ENABLED=0`; `alpine:3.22` runs them as uid 10001 |
| `deploy/docker-compose.yml` | `redis`, `feed-binance`, `feed-kucoin`; one service block per venue |
| `deploy/redis.conf` | Six directives, all load-bearing |
| `Makefile` | `proto`, `build`, `test`, `test-integration`, `test-smoke`, `lint`, `validate`, `run`, `up`, `down`, `clean` |
| `go.mod` / `go.sum` | Module `github.com/you/manooch`, Go 1.27.0 |
| `.dockerignore` / `.gitignore` | Keep `.git`, `bin/`, `.local/`, `testdata/raw/` out of the context and the repo |

## redis.conf

| Directive | Value | Why |
|---|---|---|
| `appendonly` / `save` | `no` / `""` | Data is ephemeral; the cache rebuilds within one cadence |
| `maxmemory` | `2gb` | |
| `maxmemory-policy` | `noeviction` | **Critical.** Any eviction policy silently deletes last-value keys under pressure, and a missing key is how a stale stream is reported — a working stream would read as expired. `noeviction` returns a write error instead |
| `notify-keyspace-events` | `Ex` | Expired-key events; `internal/fallback` triggers on them |
| `client-output-buffer-limit pubsub` | `256mb 64mb 60` | A subscriber that falls behind is dropped by Redis with no notice to either side. Raising the limit makes that rarer, not impossible — consumers still watch `publish_seq` |

## One container per venue

`feed-binance` and `feed-kucoin` are the same image with a different
`--exchange`, the same config mounted read-only, `depends_on` Redis healthy,
`mem_limit: 512m` and `restart: unless-stopped`. They share nothing but Redis:
no connection, no rate-limit budget in process, no goroutine.

That is the isolation the topology exists for, and it is checkable rather than
claimed:

```sh
docker compose -f deploy/docker-compose.yml kill feed-binance
manooch-status --venue=KUCOIN
```

KuCoin keeps publishing with no gap. Verified on 2026-09-02: every KuCoin key
stayed `HEALTHY` across the kill and `manooch-tap` reported no `publish_seq`
gap. Binance's own keys expire and vanish, which is the correct report of a
venue that is gone — its `venue:ratelimit` key outlives them only because its
TTL is ten minutes.

Adding a third venue is one service block here, one file in `config/venues/`,
one adapter package and one line in `internal/adapter.builders`.

## Allowed dependencies

The complete direct list; everything else in `go.mod` is transitive.

```
google.golang.org/protobuf          github.com/prometheus/client_golang
github.com/redis/go-redis/v9        github.com/google/uuid
gopkg.in/yaml.v3                    github.com/ory/dockertest/v3  (test only)
github.com/go-playground/validator/v10
github.com/coder/websocket
```

No framework, no ORM, no CLI or config library: `net/http.ServeMux` and `flag` cover what is needed.

## Notes

- Redis is published on `127.0.0.1:6379` for the CLI tools. The feed publishes **no** ports — the admin surface stays inside the container.
- `make run` copies `config/` to `.local/config` with `redis.addr` rewritten to `127.0.0.1`, because the committed default names the compose service. `make run EXCHANGE=KUCOIN` runs the other venue.
- Both feeds bind the admin surface to `127.0.0.1:9101` inside their own container, so there is no clash and nothing is reachable off-host.
- `make lint` is `go vet` plus a `gofmt -l` check. `make proto` needs `protoc` and `protoc-gen-go` on `PATH`.
- `make test` is unit tests only. `make test-integration` needs Docker; `make test-smoke` reaches the real venue and is **not** in CI, so a red build means our code broke rather than that the exchange was slow.
- Keep `CGO_ENABLED=0`: the runtime image is musl and the build image's libc need not match.
- Alpine, not distroless — the compose healthcheck uses `wget`.
