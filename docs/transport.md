Covers: M1 · `internal/transport`

One open websocket, wrapping `coder/websocket` as `core.Conn`. It knows nothing
about venues, payloads or Redis: it opens a socket, hands frames up with the
instant they arrived, and fails loudly when one does not.

| File | Holds |
|---|---|
| `wsconn.go` | `Dial`, `Options`, `Dialer`, `Conn`, `ErrIdle`, `ErrFrameTooBig` |
| `wsconn_test.go` | Arrival stamping, idle deadline, cross-goroutine close, size limit, ping answering |

## Key types and functions

| Symbol | What it does |
|---|---|
| `Dial(ctx, Options) (core.Conn, error)` | Opens a socket, bounded by `ctx` alone |
| `Options` | `URL`, `ReadTimeout`, `MaxFrameBytes`, `HTTPClient`, `HTTPHeader` |
| `Dialer` | `func(ctx, Options) (core.Conn, error)`; adapters take one so a test can substitute |
| `Conn.Read(ctx)` | Blocks for one frame, returns it with `recvNs` |
| `Conn.Write(ctx, b)` | One text frame, for client-initiated pings (M3) |
| `Conn.Close()` | Safe from another goroutine, and unblocks `Read` |
| `Conn.ServerPings()` | Ping frames the venue sent; the library answered each |
| `Conn.URL()` | The address, for logs |
| `ErrIdle` | Connected and silent past the read timeout |
| `ErrFrameTooBig` | A frame past `MaxFrameBytes` |
| `DefaultMaxFrameBytes` | 1 MiB, when `Options` leaves one unset |

## Where recv_time_ns is stamped

In `Conn.Read`, on the line immediately after `ws.Read` returns, before the
error is even checked:

```go
_, b, err := c.ws.Read(readCtx)
recvNs := time.Now().UnixNano()
```

Every freshness number and the clock-skew gauge are measured from that line.
Stamped after parsing, it would fold our own work into the venue's latency and
report the venue as slower than it is. `internal/adapter/binance` carries the
value through `Parse` untouched, and `RunAdapterConformance` asserts it comes
out equal to what went in.

`publish_time_ns` is stamped in `publish.RedisPublisher.Publish` and nowhere
else, so the gap between the two is real elapsed time rather than an artefact
of where the code chose to look at the clock.

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
  the parent is checked first — and reporting a clean shutdown as a dead venue
  sends someone to look at the wrong thing.
- **Oversized frames error, never truncate.** Half a message parses into a
  plausible wrong one.
- **No metrics, no logging here.** Counting belongs to the caller, which knows
  what channel a frame turned into; this package cannot.

## Not here

Reconnect, backoff, circuit breaking, the socket supervisor, application-level
ping tickers (an adapter's `Dial` owns those, via `Write`).
