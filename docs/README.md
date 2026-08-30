Start with `architecture.md` — it has the dependency rule, the dataflow for one message, and the milestone map. Everything else assumes it.

| Document | Covers | Paths |
|---|---|---|
| `architecture.md` | Shape, boundaries, dataflow, Redis layout, decisions, milestone map | whole repository |
| `contract.md` | The wire format: enums, messages, who fills which envelope field | `schema/`, `gen/manoochv1/` |
| `price.md` | Fixed-point `int64` types and the decimal parser | `pkg/price` |
| `config.md` | Strict YAML loading, two-file merge, validation rules | `internal/config`, `config/` |
| `core.md` | Instrument identity and enum ↔ string mapping | `internal/core` |
| `publish.md` | Redis key scheme and the write path | `internal/publish` |
| `obs.md` | Logger and the Prometheus collector set | `internal/obs` |
| `synth.md` | Dev-only data generator (removed at M4) | `internal/synth` |
| `cli.md` | The three binaries, their flags and HTTP routes | `cmd/manooch-feed`, `cmd/manooch-tap`, `cmd/manooch-status` |
| `deploy.md` | Image, compose, `redis.conf`, Makefile, module | `deploy/`, `Makefile`, `go.mod` |

For how to build and run, see the repository `README.md`. `internal/adapter/`, `internal/transport/`, `internal/fallback/`, `internal/metadata/` and `testdata/` are empty at M0 and have no documents; `architecture.md` says what each is reserved for.
