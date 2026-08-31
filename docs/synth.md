Covers: M1 · `internal/synth`

Generates mark price, index price and funding so the publisher, `manooch-tap` and `manooch-status` can be exercised without an exchange connection. Package doc says `Dev only. Remove at M4.`

Everything lives in `synth.go`. No test file.

`New(cfg, pub, log)` then `Run(ctx)` starts one goroutine per `config.Stream` and returns when all have stopped. Each ticks at its channel's cadence, builds a payload, and calls `publish.Publisher.Publish`.

The numbers are invented; the envelopes are not. `envelope` sets `venue`, `instrument`, `channel`, a per-stream `venue_seq`, `source: WEBSOCKET` and `status: HEALTHY`, and backdates `exchange_time_ns` by 5–24ms and `recv_time_ns` by under 500µs so the latency histograms have a distribution rather than a spike at zero.

A `market` per market-type + symbol holds a mutex-guarded mid price that random-walks ±2 ticks, so the mark and index price of one symbol tell the same story. Seeds exist for `BTC`, `ETH` and `SOL`; anything else gets `1.00` / tick `0.0001`.

## Cadence and TTL

`schedule` reads the channel's cadence from the venue file (`quirks.cadence`) and multiplies by `health.ttl_multiplier`, so synthetic streams expire on the same schedule real ones do — which is what makes `manooch-status` against `--synthetic` a rehearsal rather than a different thing. A channel the venue file declares no cadence for falls back to one second; the generator has to tick at something, and the venue file is where a real rate is written down.

## Rules

- **Only `cmd/manooch-feed` may import this.** It is scheduled for deletion at M4; a dependency from `internal/publish` or `internal/config` would outlive it.
- **Take `publish.Publisher`, not `*RedisPublisher`.** That is the seam a venue adapter replaces this package at.
- **Keep the envelope real even though the numbers are fake.** The whole point is that a bug can be blamed on the adapter rather than the plumbing — which only holds if the plumbing was exercised with correct timestamps, sequence numbers and fixed-point values.
- **Publish errors are dropped on purpose**: `internal/publish` already counts and rate-limits them, and re-reporting would log per message.
