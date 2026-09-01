Start with [`architecture.md`](architecture.md): the shape, the dependency rule, the dataflow for one message, and the decisions table.

| Document | Covers |
|---|---|
| [`architecture.md`](architecture.md) | Whole repository — patterns, boundaries, Redis layout, milestone map |
| [`contract.md`](contract.md) | `schema/`, `gen/manoochv1/` — the wire format |
| [`price.md`](price.md) | `pkg/price` — fixed-point types and the decimal parser |
| [`config.md`](config.md) | `internal/config`, `config/` — loading, merging, validation |
| [`core.md`](core.md) | `internal/core` — instrument identity, enum mapping |
| [`adapter.md`](adapter.md) | `internal/core/adapter.go`, `internal/adapter/` — the inbound port |
| [`adapter-binance.md`](adapter-binance.md) | `internal/adapter/binance` — endpoint, payload mapping, quirks |
| [`transport.md`](transport.md) | `internal/transport` — websocket connections, backoff, circuit breaker |
| [`supervisor.md`](supervisor.md) | `internal/supervisor` — the restart procedure and the escalation tiers |
| [`health.md`](health.md) | `internal/health` — status semantics, TTL as freshness, the heartbeat |
| [`fallback.md`](fallback.md) | `internal/fallback` — expiry trigger, REST polling, escalation to `STALE` |
| [`publish.md`](publish.md) | `internal/publish` — key scheme and the write path |
| [`obs.md`](obs.md) | `internal/obs` — logger and collectors |
| [`cli.md`](cli.md) | `cmd/*` — the three binaries |
| [`deploy.md`](deploy.md) | `deploy/`, `Makefile`, `go.mod` |

Building and running: the repository [`README.md`](../README.md). Per-symbol detail: `go doc ./internal/<pkg>` — every exported symbol has a doc comment.

`internal/metadata/` is empty; `architecture.md` says what it is reserved for.
