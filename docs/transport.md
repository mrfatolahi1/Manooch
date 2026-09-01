Covers: M2 · `internal/transport`

One open websocket wrapping `coder/websocket` as `core.Conn`, plus the retry
arithmetic above it. It knows nothing about venues, payloads or Redis: it opens
a socket, hands frames up with the instant they arrived, fails loudly when one
does not, and says how long to wait before trying again.

| File | Holds |
|---|---|
| `wsconn.go` | `Dial`, `Options`, `Dialer`, `Conn`, `ErrIdle`, `ErrFrameTooBig` |
| `backoff.go` | `Policy`, `Wait`, the jitter modes |
| `breaker.go` | `Breaker`, `BreakerOptions`, `NewBreaker` |
| `wsconn_test.go` | Arrival stamping, idle deadline, cross-goroutine close, size limit, pings |
| `backoff_test.go` | Growth, cap, overflow, jitter spread across concurrent failures |
| `breaker_test.go` | Threshold, no attempts while open, the single probe, reset on success |

## Key types and functions

| Symbol | What it does |
|---|---|
| `Dial(ctx, Options) (core.Conn, error)` | Opens a socket, bounded by `ctx` alone |
| `Options` | `URL`, `ReadTimeout`, `MaxFrameBytes`, `HTTPClient`, `HTTPHeader` |
| `Dialer` | `func(ctx, Options) (core.Conn, error)`; adapters take one so a test can substitute |
| `Conn.Read(ctx)` | Blocks for one frame, returns it with `recvNs` |
| `Conn.Write(ctx, b)` | One text frame, for client-initiated pings (M3) |
| `Conn.Close()` | Safe from another goroutine, and unblocks `Read` |
| `Conn.ServerPings()` / `Conn.URL()` | Pings the venue sent; the address, for logs |
| `ErrIdle` | Connected and silent past the read timeout |
| `ErrFrameTooBig` | A frame past `MaxFrameBytes` |
| `DefaultMaxFrameBytes` | 1 MiB, when `Options` leaves one unset |
| `Policy` | `Initial`, `Max`, `Multiplier`, `Jitter`, `Rand` |
| `Policy.Delay(attempt)` / `Sleep(ctx, attempt)` | The jittered wait, and waiting it out; false means do not retry |
| `Wait(ctx, d)` | Sleeps, or gives up on cancellation |
| `JitterNone` / `JitterFull` / `JitterEqual` | The modes; anything unrecognised is treated as full |
| `NewBreaker(BreakerOptions) *Breaker` | `ConsecutiveFailures`, `OpenDuration`, `Now` |
| `Breaker.Retry()` | Zero means dial now; anything else is how long to wait |
| `Breaker.Fail()` / `Succeed()` / `Open()` / `Failures()` | Record an attempt; read the state |

## Where recv_time_ns is stamped

In `Conn.Read`, on the line immediately after `ws.Read` returns, before the
error is even checked:

```go
_, b, err := c.ws.Read(readCtx)
recvNs := time.Now().UnixNano()
```

Every freshness number and the clock-skew gauge are measured from that line.
Stamped after parsing, it would fold our own work into the venue's latency.
`internal/adapter/binance` carries it through `Parse` untouched, and
`RunAdapterConformance` asserts it comes out equal to what went in.
`publish_time_ns` is stamped in `publish.RedisPublisher.Publish` and nowhere
else, so the gap between the two is real elapsed time.

## Backoff

```
sleep = random(0, min(cap, initial × multiplier^attempt))
```

Full jitter, not equal and not fixed. With several streams failing on the same
frame, deterministic backoff makes them all retry on the same frame — the
reconnect storm, which is where real IP bans come from far more than
steady-state polling. `supervisor.stream_restart_backoff` and
`socket_reconnect_backoff` are separate policies.

The ceiling is computed in `float64` and clamped rather than in
`time.Duration`: a socket failing for a day reaches an attempt count where
`int64` nanoseconds overflow into a negative delay — a retry with no wait.

## Circuit breaker

After `supervisor.circuit_breaker.consecutive_failures` (10) in a row it opens
for `open_duration` (5m). While open **no connection attempt is made at all** —
not a slow one, not a probe — and the affected streams publish `STALE` with
`circuit open`. On expiry exactly one attempt is handed out; success closes it,
failure reopens it for the full duration.

Backoff alone still dials a venue that is refusing us every minute forever. The
breaker is what makes the client stop knocking.

## Proactive reconnect

`connection.max_age` (23h) redials before the venue does it for us. Binance
drops a futures socket at 24 hours; going first is the difference between a
handover and a gap — we choose the moment, keys stay inside their TTL across it,
and nobody discovers the disconnect by not being sent data. The timer lives in
`internal/supervisor`.

## Failure handling

| Situation | Result |
|---|---|
| Frame arrives | `(frame, recvNs, nil)` |
| Silent past `ReadTimeout` | `ErrIdle` |
| Caller cancelled its context | `ctx.Err()`, never `ErrIdle` |
| Frame over `MaxFrameBytes` | `ErrFrameTooBig`, connection failed |
| Handshake refused | Error naming the HTTP status |

## Rules

- **Stamp `recvNs` in the read loop, never later.** It is the basis of every
  freshness and clock-skew calculation downstream.
- **Enforce the read deadline.** TCP holds a half-open connection open forever;
  a socket that is connected and silent is a disconnect, and a stream that never
  errors looks alive to everything above it.
- **`Close` skips the closing handshake.** That handshake waits for the peer's
  close frame, which only the blocked `Read` could deliver, so a graceful close
  would deadlock against the goroutine it has to free. Cancelling a context does
  not unblock `Read`; this is the only thing that does.
- **A caller's own cancellation is not an idle socket.** A read timeout arrives
  as a deadline on the derived context, indistinguishable from a shutdown unless
  the parent is checked first, and reporting a clean shutdown as a dead venue
  sends someone to look at the wrong thing.
- **Oversized frames error, never truncate.** Half a message parses into a
  plausible wrong one.
- **No metrics, no logging here.** Counting belongs to the caller, which knows
  what channel a frame turned into; this package cannot.
- **Use full jitter, and treat an unset mode as full.** A misspelt `jitter:`
  key must not silently become the one setting that causes lockstep retries.
- **Ask the breaker before every attempt.** `Retry` is what hands out the
  post-expiry probe, so reading it and ignoring the answer turns the open period
  into an ordinary backoff.

## Not here

The supervision loop that calls all of this (`supervisor.md`), the rate limiter,
application-level ping tickers (an adapter's `Dial` owns those, via `Write`).
