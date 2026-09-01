Covers: M2 · `internal/supervisor`

Keeps one venue's sockets and streams running. Recovery is stream-level: a
failed stream relaunches that stream's goroutine and nothing else, a dead socket
redials that socket, and the process never exits on either.

| File | Holds |
|---|---|
| `task.go` | `Task`, `TaskOptions`, `Start`, `StopGoroutine`, `ErrLeaked` |
| `supervisor.go` | `Process`, `Options`, `New`, the socket loop, the stream goroutines |
| `task_test.go` | Relaunch, backoff, leak counting, the close-before-wait ordering |
| `supervisor_test.go` | Reconnect, idle socket, circuit breaker, both escalation tiers |
| `../core/coretest` | The `core.Conn` and `core.Adapter` doubles every case above drives |

## Shape

```
process (one per venue)
└── socket supervisor      one per core.SocketPlan
    ├── read loop          its own goroutine: it is what parks in conn.Read
    └── stream goroutine   one per (market_type, symbol, channel)
```

A stream goroutine owns exactly one Redis key. It holds only the newest message
handed to it, so falling behind drops the older price rather than growing a
queue of them — the same thing the last-value cache it feeds does.

## The restart procedure

`StopGoroutine` is the whole mechanism, and the order is the point:

1. **Cancel the context**, so a goroutine that watches it can leave.
2. **Close the connection.** Mandatory. Go cannot kill a goroutine, and one
   parked in `conn.Read` never observes step 1 — breaking what it is blocked on
   is the only thing that ends that call.
3. **Wait, bounded** by `supervisor.goroutine_leak_timeout`, because step 2 is
   not guaranteed to have worked.

A goroutine still running at the end of step 3 is leaked: counted, logged at
ERROR, relaunched anyway. Refusing would trade one stuck stream for a dead one.

## Escalation

| Tier | Trigger | Action |
|---|---|---|
| 1 | One stream's key expires | `Task.Restart` on that stream's goroutine |
| 2 | Read error, or ≥ quorum of a socket's keys expire inside `ExpiryWindow` | Close and redial that socket |
| 2 | Connection reaches `connection.max_age` | Planned redial, nothing having failed |

There is no tier 3. Quorum is half the socket's streams, never fewer than two:
one key expiring is a stream problem, most of them is a connection problem.

## Key types and functions

| Symbol | What it does |
|---|---|
| `New(Options) (*Process, error)` | Builds the tree; opens nothing |
| `Options` | Venue, adapter, plans, publisher, `*health.Tracker`, metrics, logger, both backoff policies, `transport.BreakerOptions`, `LeakTimeout`, `ConnMaxAge`, `ExpiryWindow`, `OnMessage`, `Now` |
| `Process.Run(ctx)` | Supervises every socket until `ctx` ends |
| `Process.KeyExpired(spec)` | The escalation entry point, called by the fallback watcher |
| `Process.Leaked()` | Goroutines that never came back |
| `Start(ctx, TaskOptions) *Task` | One supervised, restartable goroutine |
| `TaskOptions` | `Name`, `Run`, `Unblock`, `LeakTimeout`, `Backoff`, `OnExit`, `Log` |
| `Task.Restart()` | Asks for a stop and relaunch; non-blocking, and collapses |
| `Task.Wait()` / `Task.Done()` | Blocks until supervision stops / its channel |
| `Task.Restarts()` / `Task.Leaks()` | Counts, for metrics and tests |
| `StopGoroutine(cancel, unblock, exit, timeout)` | The procedure above; `ErrLeaked` if it did not work |
| `ErrLeaked` | A goroutine that did not return in time |

## How it is used

`cmd/manooch-feed` builds one `Process` from the adapter's `SocketPlan`s and
runs it beside `health.Tracker.Run` and `fallback.Watcher.Run`. `OnMessage` is
wired to `Watcher.Note` and `Watcher.OnExpired` to `Process.KeyExpired`, which
is the only coupling between the two.

## Rules

- **Never cancel a stream without closing its connection.** A goroutine in
  `conn.Read` never sees the cancel, so cancelling alone hangs shutdown for the
  full leak timeout and then leaks anyway.
- **The read loop gets its own goroutine.** It is the one that parks in `Read`;
  running it inline would mean waiting for it unbounded, and one wedged socket
  would hold the process open past every shutdown deadline.
- **A leaked goroutine forces the venue to `DEGRADED`.** There is no self-kill,
  so leaks accumulate; the only thing standing between that and silence is
  making the count visible.
- **A restart request during a stop or backoff is dropped.** One failure noticed
  by three watchers has to be one restart, not three.
- **A publish failure does not restart the stream.** The publisher counts and
  rate-limit logs it, and the key expiring is the trigger both recovery tiers
  are already built on.

## Not here

Process exit on failure, an external monitor, the backoff arithmetic
(`transport.Policy`), the REST poll (`fallback.md`), status computation
(`health.md`).
