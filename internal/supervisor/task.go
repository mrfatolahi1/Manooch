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
	// goroutine: a goroutine sitting in Conn.Read will never observe its
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
	options TaskOptions
	restart chan struct{}
	done    chan struct{}

	restarts atomic.Uint32
	leaks    atomic.Uint32
}

// Start launches the task. It returns once the goroutine is running.
func Start(ctx context.Context, options TaskOptions) *Task {
	task := &Task{
		options: options,
		restart: make(chan struct{}, 1),
		done:    make(chan struct{}),
	}
	go task.loop(ctx)
	return task
}

// Restart asks for the running goroutine to be stopped and relaunched. It does
// not block, and a second call while a restart is pending is a no-op: one
// restart is one restart however many things noticed at once.
func (task *Task) Restart() {
	select {
	case task.restart <- struct{}{}:
	default:
	}
}

// Wait blocks until the task has stopped supervising, which happens when its
// context ends. A leaked goroutine may still be running when it returns —
// that is what ErrLeaked and the leak count are for.
func (task *Task) Wait() { <-task.done }

// Done closes when the task has stopped supervising.
func (task *Task) Done() <-chan struct{} { return task.done }

// Restarts is how many times the goroutine has been relaunched.
func (task *Task) Restarts() uint32 { return task.restarts.Load() }

// Leaks is how many of this task's goroutines failed to return in time.
func (task *Task) Leaks() uint32 { return task.leaks.Load() }

// loop runs the goroutine, restarting it until ctx ends.
func (task *Task) loop(ctx context.Context) {
	defer close(task.done)

	for attempt := 0; ; attempt++ {
		if ctx.Err() != nil {
			return
		}

		// Anything that asked for a restart while the previous run was being
		// stopped, or during the backoff, was asking about a goroutine that is
		// already gone. Relaunching and immediately stopping again would turn
		// one failure noticed by three watchers into three restarts. Nothing
		// is stale before the first launch, so this starts at the second.
		if attempt > 0 {
			select {
			case <-task.restart:
			default:
			}
		}

		runCtx, cancel := context.WithCancel(ctx)
		exit := make(chan error, 1) // buffered: a leaked goroutine must not block on the send
		go func() { exit <- task.options.Run(runCtx) }()

		var err error
		var shuttingDown bool
		select {
		case err = <-exit:
			cancel()
		case <-task.restart:
			err = task.stop(cancel, exit)
		case <-ctx.Done():
			err = task.stop(cancel, exit)
			shuttingDown = true
		}

		task.report(err)
		if shuttingDown || ctx.Err() != nil {
			return
		}

		task.restarts.Add(1)
		if !task.options.Backoff.Sleep(ctx, attempt) {
			return
		}
	}
}

// stop runs the restart procedure and counts a leak when it does not work.
func (task *Task) stop(cancel context.CancelFunc, exit <-chan error) error {
	err := StopGoroutine(cancel, task.options.Unblock, exit, task.options.LeakTimeout)
	if errors.Is(err, ErrLeaked) {
		task.leaks.Add(1)
	}
	return err
}

// StopGoroutine ends one goroutine, and the order is the whole point:
//
//  1. cancel the context, so a goroutine that watches it can leave;
//  2. unblock it — close the connection — because one parked in Read cannot
//     see step 1: Go cannot kill a goroutine, and the only way to end a
//     blocking call is to break what it is blocked on;
//  3. wait, bounded, because step 2 is not guaranteed to have worked.
//
// exit must be buffered, or a goroutine that returns after the timeout blocks
// forever on the send and leaks for a second reason.
//
// A goroutine still running at the end of step 3 is leaked, and ErrLeaked says
// so. The caller relaunches anyway: refusing to would trade one stuck stream
// for a permanently dead one.
func StopGoroutine(cancel context.CancelFunc, unblock func(), exit <-chan error, timeout time.Duration) error {
	if cancel != nil {
		cancel()
	}
	if unblock != nil {
		unblock()
	}

	timer := time.NewTimer(timeout)
	defer timer.Stop()

	select {
	case err := <-exit:
		return err
	case <-timer.C:
		return ErrLeaked
	}
}

// report logs and hands the exit to OnExit.
func (task *Task) report(err error) {
	if task.options.Log != nil && errors.Is(err, ErrLeaked) {
		task.options.Log.Error("goroutine leaked",
			"task", task.options.Name,
			"timeout", task.options.LeakTimeout.String(),
			"note", "relaunching anyway; the process does not exit on this")
	}
	if task.options.OnExit != nil {
		task.options.OnExit(err)
	}
}
