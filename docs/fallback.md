Covers: M2 · `internal/fallback`

Notices when a stream's Redis key expires and serves that stream over REST until
its socket comes back. The key expiring is the whole trigger: there is no
separate staleness timer, because two mechanisms meant to agree about freshness
eventually will not.

| File | Holds |
|---|---|
| `watcher.go` | `Watcher`, `Options`, `New`, `Run`, `Expired`, the sweep |
| `poller.go` | `Note`, `Active`, the per-stream poller and its failure paths |
| `fallback_test.go` | Engagement, the cap, disengagement, every failure path |
| `integration_test.go` | Tag `integration`. Real Redis: TTL expiry, notifications, the sweep |

## Trigger

| Path | How | Why it exists |
|---|---|---|
| Primary | `SUBSCRIBE __keyevent@{db}__:expired`, reacting to the exact key named | Immediate, and names the one stream affected |
| Backstop | Every `fallback.sweep_interval`, one pipelined `EXISTS` over the whole key set | Expiry events are Pub/Sub, so they can simply not arrive; Redis also only emits one when the key is actually reclaimed |

`deploy/redis.conf` sets `notify-keyspace-events Ex`. The backstop is required,
not optional — the integration suite proves it by disabling notifications
entirely and asserting the sweep still finds the key.

One pipelined round trip, never one call per key: a per-key poll over two
hundred instruments is six hundred round trips a sweep, which makes the outage
worse at exactly the wrong moment.

## Behaviour

| Aspect | Rule |
|---|---|
| Labelling | Same channel, same key, `source: SOURCE_REST`, `status: STATUS_DEGRADED` |
| Scope | Only the expired channel is polled and published |
| Concurrency | `fallback.max_concurrent_polls` (4); past it a stream goes `STALE`, never a queue |
| Exit | A websocket message, via `Note`; `source` returns to `SOURCE_WEBSOCKET` |
| Maximum | Past `fallback.max_duration` (5m) the status escalates to `STALE`, and polling continues |

Binance answers all three channels from one `GET /fapi/v1/premiumIndex` call.
Only the expired one is republished: the other two are still fresh from the
socket, and rewriting them would reset their TTL — the one signal saying they
are fine — from a source nobody asked for.

## Key types and functions

| Symbol | What it does |
|---|---|
| `New(Options) (*Watcher, error)` | Builds the watcher; subscribes to nothing yet |
| `Options` | Venue, adapter, publisher, `*redis.Client`, `DB`, `*health.Tracker`, metrics, logger, specs, the four `fallback.*` durations and the cap, `OnExpired`, `Now` |
| `Watcher.Run(ctx)` | Subscription plus sweep, until `ctx` ends |
| `Watcher.Expired(ctx, spec)` | Reports the expiry once and engages the poller |
| `Watcher.Note(spec)` | A websocket message arrived; ends fallback for that stream |
| `Watcher.Active()` | Streams currently served over REST |

`Options.Redis` is read from and never written to. Everything published goes
through `Options.Publisher`, which is the only place the envelope is stamped.

## How it is used

`cmd/manooch-feed` runs `Watcher.Run` beside the supervisor, wiring
`OnExpired` to `supervisor.Process.KeyExpired` and the supervisor's `OnMessage`
to `Watcher.Note`. Disabling `fallback.enabled` logs a warning at startup: an
expired key then stays expired.

## Rules

- **Every failure path ends in `STALE`.** A poll that errors, a venue that
  answers nothing usable, a write that does not land, a stream turned away by
  the cap. A fallback that quietly skips is the exact failure the service exists
  to prevent: the key stays absent and nothing anywhere says why.
- **Past the cap a stream is refused, not queued.** A queued poll arrives after
  it stopped being worth having and is then published as though it were current.
- **`Note` signals the poller, it does not wait for it.** It runs on the publish
  path of a stream that is working again; waiting would stall the socket behind
  an HTTP call to a venue having a bad day.
- **A poll that was in flight when the socket recovered is discarded.** Landing
  it would put `SOURCE_REST` and `DEGRADED` back onto a healthy stream and reset
  its TTL from a source nobody is using.
- **An expiry is escalated once per outage.** The sweep re-finds a still-missing
  key every interval; counting each as fresh would turn one dead stream into a
  restart every five seconds and a metric nobody can read.
- **Long-running fallback keeps polling while reporting `STALE`.** Stopping
  would leave a consumer with no value at all rather than one clearly labelled
  as not to be traded on.

## Not here

The rate limiter, metadata polling, deciding what `STALE` means (`health.md`),
the restart tiers (`supervisor.md`), the REST call itself (the venue adapter's
`FetchOnce`).
