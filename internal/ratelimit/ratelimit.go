// Package ratelimit keeps the service inside a venue's published request
// budget.
//
// Steady state costs nothing: a websocket that is subscribed and delivering
// makes no requests at all. What consumes budget is reconnecting — the
// handshake itself, and on KuCoin the bullet call every dial has to make — plus
// the hourly metadata refresh and whatever fallback polling is running while a
// stream is stale. The exposure is therefore a reconnect storm, which is why
// the circuit breaker in internal/transport does more to prevent a ban than
// anything here does.
//
// Denial is a refusal, never a delay that is quietly given up on: a caller
// that is told no does not make the request, and the stream it was for reports
// DEGRADED or STALE. An operation that is skipped silently is the failure this
// service exists to prevent.
package ratelimit

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// ErrBudgetExhausted is what Allow returns rather than waiting past the
// caller's deadline. It is a distinct error so a caller can tell "we declined
// to make this request" from "the request failed".
var ErrBudgetExhausted = errors.New("ratelimit: budget exhausted")

// A LimitKind names one budget. They are separate buckets because the venue
// counts them separately: spending REST weight must never stop a reconnect.
type LimitKind int

// The budgets a venue publishes.
const (
	// LimitRESTWeight is the venue's request-weight budget, spent by metadata
	// refreshes, fallback polls and KuCoin's bullet call.
	LimitRESTWeight LimitKind = iota
	// LimitWSConnect is how often a new websocket may be opened.
	LimitWSConnect
	// LimitSubscriptions is how many subscribe frames may be sent.
	LimitSubscriptions
)

// Kinds is every budget, in order, for callers that report on all of them.
var Kinds = []LimitKind{LimitRESTWeight, LimitWSConnect, LimitSubscriptions}

// String renders the kind for logs, metric labels and the advisory Redis key.
// It is a closed set rather than free text so the metric can be summed.
func (k LimitKind) String() string {
	switch k {
	case LimitRESTWeight:
		return "rest_weight"
	case LimitWSConnect:
		return "ws_connect"
	case LimitSubscriptions:
		return "subscriptions"
	default:
		return "unspecified"
	}
}

// A Limiter decides whether an operation may happen now.
//
// Adapters take one rather than reaching for a package-level instance: the
// budget belongs to the process, and a test must be able to hand over one that
// refuses everything.
type Limiter interface {
	// Allow blocks until budget is available or ctx expires. It returns
	// ErrBudgetExhausted rather than waiting past the deadline.
	Allow(ctx context.Context, venue string, kind LimitKind, cost int) error

	// Used is the budget currently spent and the budget available, for the
	// advisory publication and the gauge. A kind with no bucket answers (0, 0).
	Used(venue string, kind LimitKind) (used, capacity int)
}

// Unlimited is a Limiter that permits everything. It is what an adapter falls
// back to when no limiter was supplied, which is only ever the case in a test:
// the daemon builds one from the venue file before it builds an adapter.
type Unlimited struct{}

var _ Limiter = Unlimited{}

// Allow permits the operation.
func (Unlimited) Allow(context.Context, string, LimitKind, int) error { return nil }

// Used reports no budget, which is how a caller tells an unbudgeted kind from
// one sitting at zero.
func (Unlimited) Used(string, LimitKind) (int, int) { return 0, 0 }

// A Bucket is one kind's budget: Capacity operations per Window.
type Bucket struct {
	Capacity int
	Window   time.Duration
}

// Fraction scales a venue's published limit down to the share this process
// will use. It is never 1: the in-process limiter is blind to the order
// service, which shares the host's IP and spends against the same budget.
func (b Bucket) Fraction(f float64) Bucket {
	b.Capacity = int(float64(b.Capacity) * f)
	if b.Capacity < 1 {
		b.Capacity = 1
	}
	return b
}

// interval is how long one unit of budget takes to come back.
func (b Bucket) interval() time.Duration { return b.Window / time.Duration(b.Capacity) }

// Validate rejects a bucket that cannot be enforced.
func (b Bucket) Validate(kind LimitKind) error {
	switch {
	case b.Capacity <= 0:
		return fmt.Errorf("ratelimit: %s capacity is %d", kind, b.Capacity)
	case b.Window <= 0:
		return fmt.Errorf("ratelimit: %s window is %v", kind, b.Window)
	case b.interval() <= 0:
		return fmt.Errorf("ratelimit: %s window %v is too short for %d operations", kind, b.Window, b.Capacity)
	}
	return nil
}
