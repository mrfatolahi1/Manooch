Covers: M0 · `internal/synth`

## Purpose

Generates market data so the publisher, `manooch-tap` and `manooch-status` can be exercised before any exchange adapter exists. The package doc says `Dev only. Remove at M4.` The numbers are invented; the envelopes are not.

## Files

| Path | Holds |
|---|---|
| `internal/synth/synth.go` | Everything: the `Generator`, per-stream loops, message builders and the shared per-instrument price walk |

No test file.

## Key types and functions

| Symbol | Signature | Notes |
|---|---|---|
| `Generator` | struct | Holds the config, a `publish.Publisher`, a logger, and a mutex-guarded `map[string]*market` |
| `New` | `func(cfg *config.Config, pub publish.Publisher, log *slog.Logger) *Generator` | |
| `Generator.Run` | `func(ctx context.Context)` | One goroutine per `config.Stream`; returns when all have stopped |
| `runStream` | `func(ctx, s config.Stream)` | Unexported. Resolves the instrument once, then ticks |
| `schedule` | `func(ch pb.Channel) (cadence, ttl time.Duration)` | Unexported. See below |
| `build` | `func(s, instrument, mkt, venueSeq) proto.Message` | Unexported. Dispatches on channel; returns `nil` for a channel it does not produce, which ends that stream's loop |
| `envelope` | `func(instrument, ch, venueSeq) *pb.Envelope` | Unexported. Fills the producer-owned fields |
| `buildBook`, `buildTrades`, `buildFunding` | unexported | `MarkPrice` and `IndexPrice` are built inline in `build` |
| `market` | struct `{mu; mid, tick price.Price; unit price.Size}` | Unexported. Shared per market type + symbol |
| `Generator.market` | `func(mt, symbol, base string) *market` | Unexported. Get-or-create, keyed `MARKET_TYPE:SYMBOL` |
| `newMarket`, `seeds` | unexported | `seeds` gives mid/tick/unit for `BTC`, `ETH`, `SOL`; anything else gets `1.00` / `0.0001` / `100` |
| `market.step`, `market.jitter`, `market.size` | unexported | `step` moves the mid ±2 ticks and floors it at `tick × 100` |
| `tradesCadence`, `markPriceCadence`, `indexPriceCadence`, `fundingCadence` | `= 250ms`, `1s`, `1s`, `1s` | |
| `fundingIntervalSeconds` | `= 8 * 60 * 60` | |

### Cadence and TTL

`schedule` returns `cfg.BookCadence()` for `orderbook` and the constants above for the rest, then `cfg.TTL(cadence)`. Trades are the exception: `schedule` returns `(tradesCadence, 0)`. A zero TTL writes a key with no expiry, because trades are event driven — a quiet minute is normal, and an expiring key would report a working stream as dead.

The cadences other than the book's are invented for this generator and say nothing about any real venue.

### Envelope

`envelope` sets `Venue`, `Instrument`, `Channel`, `VenueSeq` (a per-stream counter), `VenueSeqPresent: true`, `Source: SOURCE_WEBSOCKET` and `Status: STATUS_HEALTHY`. `ExchangeTimeNs` is backdated 5–24ms and `RecvTimeNs` by under 500µs, so the latency histograms in `internal/obs` have a distribution rather than a spike at zero. `publish.RedisPublisher.Publish` fills the rest.

## How it is used

`cmd/manooch-feed.run` constructs a `Generator` and runs it in one goroutine only when `--synthetic` is passed. Without the flag the feed logs that it has no adapter and idles. `Run` returns after `ctx` is cancelled and every stream goroutine has exited; the feed waits on that with a `sync.WaitGroup` under a 10-second deadline.

## Rules

- **Nothing outside this package may import it except `cmd/manooch-feed`.** It is scheduled for deletion at M4; a dependency from `internal/publish` or `internal/config` would outlive it.
- **Keep the envelope real even though the numbers are fake.** Its whole value is that when the first adapter lands in M1, a bug can be attributed to the adapter rather than to the plumbing underneath — which only holds if the plumbing has been exercised with correct timestamps, sequence numbers and fixed-point values.
- **Publish errors are dropped on purpose.** `runStream` discards the error from `Publish` because `internal/publish` already counts it and rate-limits the log; re-reporting it here would log per message.
- **Take `publish.Publisher`, not `*publish.RedisPublisher`.** That is the seam an M1 adapter replaces this package at.
- **Build values through `pkg/price`.** `newMarket` parses its seeds from decimal strings, so the generator produces values at the same scale as a real venue and cannot invent an out-of-range one.

## Not here

- What the feed does without `--synthetic`: `docs/cli.md`.
- What `Publish` adds to the envelope: `docs/publish.md`.
- Where the stream list comes from: `docs/config.md` (`Config.Streams`).
