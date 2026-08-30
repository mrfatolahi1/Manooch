Covers: M0 · `internal/synth`

Generates market data so the publisher, `manooch-tap` and `manooch-status` can be exercised before any exchange adapter exists. Package doc says `Dev only. Remove at M4.`

Everything lives in `synth.go`. No test file.

`New(cfg, pub, log)` then `Run(ctx)` starts one goroutine per `config.Stream` and returns when all have stopped. Each ticks at its channel's cadence, builds a payload, and calls `publish.Publisher.Publish`.

The numbers are invented; the envelopes are not. `envelope` sets `venue`, `instrument`, `channel`, a per-stream `venue_seq`, `source: WEBSOCKET` and `status: HEALTHY`, and backdates `exchange_time_ns` by 5–24ms and `recv_time_ns` by under 500µs so the latency histograms have a distribution rather than a spike at zero.

A `market` per market-type + symbol holds a mutex-guarded mid price that random-walks ±2 ticks, so the book, trades and mark price of one symbol tell the same story. Seeds exist for `BTC`, `ETH` and `SOL`; anything else gets `1.00` / tick `0.0001` / size `100`.

## Cadence and TTL

`schedule` returns `cfg.BookCadence()` for the book and invented constants for the rest (trades 250ms, mark/index/funding 1s), then `cfg.TTL(cadence)`. **Trades are the exception: TTL 0, no expiry** — they are event driven, so an empty minute is normal and an expiring key would report a working stream as dead.

## Rules

- **Only `cmd/manooch-feed` may import this.** It is scheduled for deletion at M4; a dependency from `internal/publish` or `internal/config` would outlive it.
- **Take `publish.Publisher`, not `*RedisPublisher`.** That is the seam an M1 adapter replaces this package at.
- **Keep the envelope real even though the numbers are fake.** The whole point is that when the first adapter lands, a bug can be blamed on the adapter rather than the plumbing — which only holds if the plumbing was exercised with correct timestamps, sequence numbers and fixed-point values.
- **Publish errors are dropped on purpose**: `internal/publish` already counts and rate-limits them, and re-reporting would log per message.
