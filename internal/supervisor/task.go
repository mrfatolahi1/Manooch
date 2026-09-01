// Package supervisor keeps one venue's sockets and streams running.
//
// Recovery is stream-level. A failed stream restarts that stream's goroutine
// and nothing else; a dead socket redials that socket. The process never kills
// itself and there is no external monitor container, which is a deliberate
// trade: no restart loops, and therefore no reconnect storms, at the cost of a
// leaked goroutine an operator eventually has to notice.
package supervisor

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/you/manooch/internal/transport"
)

// ErrLeaked is what stopping a goroutine returns when it did not come back
// within the leak timeout. It is not fatal: the task relaunches anyway, and the
// count is what makes the leak visible.
var ErrLeaked = errors.New("supervisor: goroutine did not return")

// TaskOptions configures one supervised goroutine.
type TaskOptions struct {
	// Name identifies the task in logs.
	Name string

	// Run is the goroutine body. Returning any error, nil included, means the
	// task has stopped and will be relaunched after backoff.
	Run func(ctx context.Context) error

	// Unblock closes whatever Run may be parked in — for a read loop, the
	// connection.
	//
	// It is mandatory for any Run that can block on I/O. Go cannot kill a
	// goroutine: a goroutine sitting in conn.Read will never observe its
	// context being cancelled, and the only thing that makes that call return
	// is closing the connection underneath it.
	Unblock func()

	// LeakTimeout bounds the wait for a stopped goroutine to return.
	LeakTimeout time.Duration

	// Backoff is the wait between a stop and the relaunch.
	Backoff transport.Policy

	// OnExit is called after each run ends, with the error it returned or
	// ErrLeaked. It runs on the supervision goroutine, so it must not block.
	OnExit func(err error)

	Log *slog.Logger
}

// A Task is one goroutine kept running until its context ends.
type Task struct {
	opts    TaskOptions
	restart chan struct{}
	done    chan struct{}

	restarts atomic.Uint32
	leaks    atomic.Uint32
}

// Start launches the task. It returns once the goroutine is running.
func Start(ctx context.Context, opts TaskOptions) *Task {
	t := &Task{
		opts:    opts,
		restart: make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	go t.loop(ctx)
	return t
}

// Restart asks for the running goroutine to be stopped and relaunched. It does
// not block, and a second call while a restart is pending is a no-op: one
// restart is one restart however many things noticed at once.
func (t *Task) Restart() {
	select {
	case t.restart <- struct{}{}:
	default:
	}
}

// Wait blocks until the task has stopped supervising, which happens when its
// context ends. A leaked goroutine may still be running when it returns —
// that is what ErrLeaked and the leak count are for.
func (t *Task) Wait() { <-t.done }

// Done closes when the task has stopped supervising.
func (t *Task) Done() <-chan struct{} { return t.done }

// Restarts is how many times the goroutine has been relaunched.
func (t *Task) Restarts() uint32 { return t.restarts.Load() }

// Leaks is how many of this task's goroutines failed to return in time.
func (t *Task) Leaks() uint32 { return t.leaks.Load() }

// loop runs the goroutine, restarting it until ctx ends.
func (t *Task) loop(ctx context.Context) {
	defer close(t.done)

	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return
		}

		runCtx, cancel := context.WithCancel(ctx)
		exit := make(chan error, 1) // buffered: a leaked goroutine must not block on the send
		go func() { exit <- t.opts.Run(runCtx) }()

		var err error
		var shuttingDown bool
		select {
		case err = <-exit:
			cancel()
		case <-t.restart:
			err = t.stop(cancel, exit)
		case <-ctx.Done():
			err = t.stop(cancel, exit)
			shuttingDown = true
		}

		t.report(err)
		if shuttingDown || ctx.Err() != nil {
			return
		}

		t.restarts.Add(1)
		if !t.opts.Backoff.Sleep(ctx, attempt) {
			return
		}
	}
}

// stop is the restart procedure, and the order is the whole point:
//
//  1. cancel the context, so a goroutine that watches it can leave;
//  2. close the connection, because one blocked in Read cannot see step 1;
//  3. wait, bounded, because step 2 is not guaranteed to have worked.
//
// A goroutine that has not returned by the end of step 3 is leaked. It is
// counted and logged, and the caller relaunches anyway: refusing to would trade
// one stuck stream for a permanently dead one.
func (t *Task) stop(cancel context.CancelFunc, exit <-chan error) error {
	cancel()
	if t.opts.Unblock != nil {
		t.opts.Unblock()
	}

	timer := time.NewTimer(t.opts.LeakTimeout)
	defer timer.Stop()

	select {
	case err := <-exit:
		return err
	case <-timer.C:
		t.leaks.Add(1)
		return ErrLeaked
	}
}

// report logs and hands the exit to OnExit.
func (t *Task) report(err error) {
	if t.opts.Log != nil && errors.Is(err, ErrLeaked) {
		t.opts.Log.Error("goroutine leaked",
			"task", t.opts.Name,
			"timeout", t.opts.LeakTimeout.String(),
			"note", "relaunching anyway; the process does not exit on this")
	}
	if t.opts.OnExit != nil {
		t.opts.OnExit(err)
	}
}
