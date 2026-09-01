package supervisor_test

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/you/manooch/internal/core/coretest"
	"github.com/you/manooch/internal/supervisor"
	"github.com/you/manooch/internal/transport"
)

// TestTaskRelaunchesAfterBackoff: a goroutine that returns on its own comes
// back, and it waits first — relaunching immediately is the tight loop the
// backoff exists to prevent.
func TestTaskRelaunchesAfterBackoff(t *testing.T) {
	var runs atomic.Int64
	starts := make(chan time.Time, 16)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task := supervisor.Start(ctx, supervisor.TaskOptions{
		Name: "returns immediately",
		Run: func(context.Context) error {
			runs.Add(1)
			select {
			case starts <- time.Now():
			default:
			}
			return nil
		},
		LeakTimeout: time.Second,
		Backoff: transport.Policy{
			Initial: 20 * time.Millisecond, Max: 20 * time.Millisecond,
			Multiplier: 2, Jitter: transport.JitterNone,
		},
		Log: quiet(),
	})

	eventually(t, "three relaunches", func() bool { return runs.Load() >= 3 })

	first, second := <-starts, <-starts
	if gap := second.Sub(first); gap < 15*time.Millisecond {
		t.Errorf("relaunched after %v, want at least the 20ms backoff", gap)
	}
	if task.Restarts() == 0 {
		t.Error("restart count stayed at zero across three relaunches")
	}

	cancel()
	select {
	case <-task.Done():
	case <-time.After(settle):
		t.Fatal("task did not stop after its context was cancelled")
	}
}

// TestTaskCountsAGoroutineThatIgnoresItsContext: Go cannot kill a goroutine, so
// this one is never coming back. It has to be counted rather than waited for.
func TestTaskCountsAGoroutineThatIgnoresItsContext(t *testing.T) {
	stuck := make(chan struct{})
	defer close(stuck)

	exits := make(chan error, 4)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task := supervisor.Start(ctx, supervisor.TaskOptions{
		Name: "ignores its context",
		Run: func(context.Context) error {
			<-stuck // neither cancellation nor anything else frees this
			return nil
		},
		LeakTimeout: 50 * time.Millisecond,
		Backoff:     fastBackoff(),
		OnExit:      func(err error) { exits <- err },
		Log:         quiet(),
	})

	task.Restart()

	select {
	case err := <-exits:
		if !errors.Is(err, supervisor.ErrLeaked) {
			t.Fatalf("exit = %v, want ErrLeaked", err)
		}
	case <-time.After(settle):
		t.Fatal("the leak was never reported")
	}

	if task.Leaks() == 0 {
		t.Error("leak count stayed at zero")
	}
	// And it relaunched anyway: refusing to would trade one stuck goroutine
	// for a permanently dead stream.
	eventually(t, "the relaunch", func() bool { return task.Restarts() > 0 })
}

// TestStopGoroutineClosesBeforeWaiting is the ordering the whole restart path
// rests on. A goroutine parked in Read never observes its context; only closing
// the connection underneath it returns that call.
func TestStopGoroutineClosesBeforeWaiting(t *testing.T) {
	conn := coretest.NewConn()

	ctx, cancel := context.WithCancel(context.Background())
	exit := make(chan error, 1)
	reading := make(chan struct{})

	go func() {
		close(reading)
		_, _, err := conn.Read(ctx)
		exit <- err
	}()
	<-reading
	time.Sleep(20 * time.Millisecond) // parked in the read, not still starting

	// Cancelling alone must not be enough, or the double is more forgiving
	// than the socket it stands in for and none of this is being tested.
	cancel()
	select {
	case err := <-exit:
		t.Fatalf("Read returned %v on context cancellation alone", err)
	case <-time.After(50 * time.Millisecond):
	}

	if err := supervisor.StopGoroutine(nil, func() { _ = conn.Close() }, exit, time.Second); err == nil {
		t.Error("StopGoroutine reported no error for a read that was closed out")
	}
	if !conn.IsClosed() {
		t.Error("the connection was not closed")
	}
}

// TestStopGoroutineReportsALeak: the bounded wait is there because closing is
// not guaranteed to have worked.
func TestStopGoroutineReportsALeak(t *testing.T) {
	conn := coretest.NewConn()
	conn.Wedge() // Close no longer unblocks Read

	exit := make(chan error, 1) // buffered, or the leak would block on the send
	go func() {
		_, _, err := conn.Read(context.Background())
		exit <- err
	}()

	start := time.Now()
	err := supervisor.StopGoroutine(nil, func() { _ = conn.Close() }, exit, 50*time.Millisecond)
	if !errors.Is(err, supervisor.ErrLeaked) {
		t.Fatalf("StopGoroutine = %v, want ErrLeaked", err)
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Errorf("waited %v for a 50ms timeout", elapsed)
	}
}

// TestTaskRestartIsCollapsed: several things noticing the same failure at once
// is one restart, not several.
func TestTaskRestartIsCollapsed(t *testing.T) {
	var runs atomic.Int64
	release := make(chan struct{})
	var once sync.Once
	defer once.Do(func() { close(release) })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task := supervisor.Start(ctx, supervisor.TaskOptions{
		Name: "collapsible",
		Run: func(rctx context.Context) error {
			runs.Add(1)
			select {
			case <-rctx.Done():
			case <-release:
			}
			return nil
		},
		LeakTimeout: time.Second,
		Backoff: transport.Policy{
			Initial: 50 * time.Millisecond, Max: 50 * time.Millisecond,
			Multiplier: 2, Jitter: transport.JitterNone,
		},
		Log: quiet(),
	})

	eventually(t, "the first run", func() bool { return runs.Load() == 1 })
	for range 20 {
		task.Restart()
	}
	eventually(t, "the relaunch", func() bool { return runs.Load() == 2 })

	time.Sleep(150 * time.Millisecond)
	if got := runs.Load(); got != 2 {
		t.Errorf("%d runs after 20 concurrent restart requests, want 2", got)
	}
}
