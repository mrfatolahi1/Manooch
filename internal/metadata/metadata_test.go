package metadata_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/core/coretest"
	"github.com/you/manooch/internal/metadata"
	"github.com/you/manooch/internal/transport"
	"github.com/you/manooch/pkg/price"
	"google.golang.org/protobuf/proto"
)

// recorder captures published metadata instead of dialing Redis.
type recorder struct {
	mu   sync.Mutex
	msgs map[string]*pb.InstrumentMeta
	ttl  time.Duration
	n    int
	err  error
}

func newRecorder() *recorder { return &recorder{msgs: map[string]*pb.InstrumentMeta{}} }

func (r *recorder) Publish(_ context.Context, key string, msg proto.Message, ttl time.Duration) error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.err != nil {
		return r.err
	}
	r.msgs[key] = proto.Clone(msg).(*pb.InstrumentMeta)
	r.ttl, r.n = ttl, r.n+1
	return nil
}

func (r *recorder) Close() error { return nil }

func (r *recorder) get(key string) *pb.InstrumentMeta {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.msgs[key]
}

func (r *recorder) count() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.n
}

// reporter records what the refresher told health.
type reporter struct {
	mu     sync.Mutex
	ok     bool
	reason string
	calls  int
}

func (h *reporter) MetadataState(ok bool, reason string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.ok, h.reason, h.calls = ok, reason, h.calls+1
}

func (h *reporter) state() (bool, string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.ok, h.reason
}

func refs(t *testing.T, symbols ...string) []core.InstrumentRef {
	t.Helper()
	out := make([]core.InstrumentRef, 0, len(symbols))
	for _, s := range symbols {
		ref, err := core.ParseCanonical(s, coretest.MarketType)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, ref)
	}
	return out
}

// logs captures log output so a test can assert a WARN line was written; the
// change log is the only record that a venue moved a tick size.
type logs struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (l *logs) Write(p []byte) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.Write(p)
}

func (l *logs) String() string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.buf.String()
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newRefresher(t *testing.T, opts metadata.Options) *metadata.Refresher {
	t.Helper()
	if opts.Venue == "" {
		opts.Venue = coretest.Venue
	}
	if opts.Log == nil {
		opts.Log = quiet()
	}
	if opts.MarketType == pb.MarketType_MARKET_TYPE_UNSPECIFIED {
		opts.MarketType = coretest.MarketType
	}
	if opts.Interval == 0 {
		opts.Interval = 20 * time.Millisecond
	}
	if opts.FetchTimeout == 0 {
		opts.FetchTimeout = time.Second
	}
	if opts.Backoff.Initial == 0 {
		opts.Backoff = transport.Policy{Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2, Jitter: "none"}
	}
	r, err := metadata.New(opts)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return r
}

func metaFor(t *testing.T, a *coretest.Adapter, symbols ...string) []*pb.InstrumentMeta {
	t.Helper()
	tick, err := price.ParsePrice("0.1")
	if err != nil {
		t.Fatal(err)
	}
	lot, err := price.ParseSize("0.001")
	if err != nil {
		t.Fatal(err)
	}
	out := make([]*pb.InstrumentMeta, 0, len(symbols))
	for _, ref := range refs(t, symbols...) {
		out = append(out, a.Meta(ref, tick, lot, 1))
	}
	return out
}

// TestPublishesEveryConfiguredInstrument: the whole set goes out each cycle, on
// the metadata key, with a TTL of twice the refresh interval.
func TestPublishesEveryConfiguredInstrument(t *testing.T) {
	a := &coretest.Adapter{}
	a.MetadataFunc = func(context.Context, pb.MarketType) ([]*pb.InstrumentMeta, error) {
		return metaFor(t, a, "BTC_USDT", "ETH_USDT", "SOL_USDT"), nil
	}
	pub := newRecorder()
	health := &reporter{}

	r := newRefresher(t, metadata.Options{
		Adapter:     a,
		Publisher:   pub,
		Health:      health,
		Instruments: refs(t, "BTC_USDT", "ETH_USDT"),
		Interval:    time.Hour,
		Required:    true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go r.Run(ctx)

	if !r.WaitReady(ctx) {
		t.Fatal("metadata never became ready")
	}

	for _, key := range []string{
		"Manooch:TESTVENUE:PERP_LINEAR:BTC_USDT:metadata",
		"Manooch:TESTVENUE:PERP_LINEAR:ETH_USDT:metadata",
	} {
		m := pub.get(key)
		if m == nil {
			t.Fatalf("no message on %s", key)
		}
		if m.Env.Status == pb.Status_STATUS_UNSPECIFIED {
			t.Errorf("%s: status is unspecified", key)
		}
		if m.TickSize <= 0 || m.LotSize <= 0 || m.ContractMultiplier <= 0 {
			t.Errorf("%s: tick=%d lot=%d multiplier=%d", key, m.TickSize, m.LotSize, m.ContractMultiplier)
		}
	}
	// SOL was not configured, so it is not published: a venue's whole contract
	// list is hundreds of keys nobody subscribed to.
	if m := pub.get("Manooch:TESTVENUE:PERP_LINEAR:SOL_USDT:metadata"); m != nil {
		t.Error("an unconfigured instrument was published")
	}
	if pub.ttl != 2*time.Hour {
		t.Errorf("ttl = %v, want twice the refresh interval", pub.ttl)
	}
	if ok, _ := health.state(); !ok {
		t.Error("health was not told metadata arrived")
	}
}

// TestStartupFailureKeepsTheVenueStale is the acceptance criterion: a failed
// initial fetch means STALE and no market data, not a feed that starts anyway
// at unknown precision.
func TestStartupFailureKeepsTheVenueStale(t *testing.T) {
	var attempts int
	var mu sync.Mutex
	a := &coretest.Adapter{}
	a.MetadataFunc = func(context.Context, pb.MarketType) ([]*pb.InstrumentMeta, error) {
		mu.Lock()
		attempts++
		n := attempts
		mu.Unlock()
		if n < 3 {
			return nil, errors.New("venue unreachable")
		}
		return metaFor(t, a, "BTC_USDT"), nil
	}
	pub := newRecorder()
	health := &reporter{}

	r := newRefresher(t, metadata.Options{
		Adapter:     a,
		Publisher:   pub,
		Health:      health,
		Instruments: refs(t, "BTC_USDT"),
		Interval:    time.Hour,
		Required:    true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Before anything runs, nothing may stream.
	select {
	case <-r.Ready():
		t.Fatal("ready before the first fetch")
	default:
	}

	go r.Run(ctx)

	// While it is failing, health must say so in the words a consumer reads.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if ok, reason := health.state(); !ok && reason == "metadata unavailable" {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if ok, reason := health.state(); !ok && reason != "metadata unavailable" {
		t.Errorf("status_reason = %q, want %q", reason, "metadata unavailable")
	}

	if !r.WaitReady(ctx) {
		t.Fatal("metadata never became ready after the venue recovered")
	}
	if ok, _ := health.state(); !ok {
		t.Error("health still reports metadata unavailable after a successful fetch")
	}
	if pub.get("Manooch:TESTVENUE:PERP_LINEAR:BTC_USDT:metadata") == nil {
		t.Error("nothing was published after the fetch succeeded")
	}
}

// TestChangeLogging: exchanges move these without warning and announce it
// nowhere a program can read, so this line is the only record.
func TestChangeLogging(t *testing.T) {
	tick, _ := price.ParsePrice("0.1")
	tick2, _ := price.ParsePrice("0.5")
	lot, _ := price.ParseSize("0.001")

	var cycle int
	var mu sync.Mutex
	a := &coretest.Adapter{}
	a.MetadataFunc = func(context.Context, pb.MarketType) ([]*pb.InstrumentMeta, error) {
		mu.Lock()
		cycle++
		n := cycle
		mu.Unlock()
		ref := refs(t, "BTC_USDT")[0]
		if n == 1 {
			return []*pb.InstrumentMeta{a.Meta(ref, tick, lot, 1)}, nil
		}
		return []*pb.InstrumentMeta{a.Meta(ref, tick2, lot, 2)}, nil
	}

	out := &logs{}
	r := newRefresher(t, metadata.Options{
		Adapter:     a,
		Publisher:   newRecorder(),
		Instruments: refs(t, "BTC_USDT"),
		Interval:    10 * time.Millisecond,
		Required:    true,
		Log:         slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go r.Run(ctx)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(out.String(), "instrument metadata changed") {
			break
		}
		time.Sleep(time.Millisecond)
	}
	got := out.String()
	if !strings.Contains(got, "instrument metadata changed") {
		t.Fatalf("no change was logged:\n%s", got)
	}
	// Both values, or the line does not say what changed.
	for _, want := range []string{"field=tick_size", "from=0.1", "to=0.5", "level=WARN"} {
		if !strings.Contains(got, want) {
			t.Errorf("change log is missing %q:\n%s", want, got)
		}
	}
}

// TestUnlistedInstrumentIsNotFatalButIsLogged: a configured symbol the venue
// does not list will never produce data, and nothing else in the service would
// say so.
func TestUnlistedInstrumentIsNotFatalButIsLogged(t *testing.T) {
	a := &coretest.Adapter{}
	a.MetadataFunc = func(context.Context, pb.MarketType) ([]*pb.InstrumentMeta, error) {
		return metaFor(t, a, "BTC_USDT"), nil
	}
	out := &logs{}
	r := newRefresher(t, metadata.Options{
		Adapter:     a,
		Publisher:   newRecorder(),
		Instruments: refs(t, "BTC_USDT", "DOGE_USDT"),
		Interval:    time.Hour,
		Required:    true,
		Log:         slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go r.Run(ctx)

	if !r.WaitReady(ctx) {
		t.Fatal("metadata never became ready")
	}
	if got := out.String(); !strings.Contains(got, "DOGE_USDT") {
		t.Errorf("the unlisted instrument was not logged:\n%s", got)
	}
}

// TestNoneListedIsAFailure: if the venue lists nothing we asked for, the config
// names instruments this venue does not have, and starting would publish
// nothing while looking healthy.
func TestNoneListedIsAFailure(t *testing.T) {
	a := &coretest.Adapter{}
	a.MetadataFunc = func(context.Context, pb.MarketType) ([]*pb.InstrumentMeta, error) {
		return metaFor(t, a, "SOL_USDT"), nil
	}
	health := &reporter{}
	r := newRefresher(t, metadata.Options{
		Adapter:     a,
		Publisher:   newRecorder(),
		Health:      health,
		Instruments: refs(t, "BTC_USDT"),
		Interval:    time.Hour,
		Required:    true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	r.Run(ctx)

	if ok, _ := health.state(); ok {
		t.Error("health reports metadata available when nothing configured was listed")
	}
	select {
	case <-r.Ready():
		t.Error("ready with no configured instrument listed")
	default:
	}
}

// TestNotRequiredDoesNotBlockStreaming: when metadata is not a startup
// dependency, WaitReady returns at once and the refresher still runs.
func TestNotRequiredDoesNotBlockStreaming(t *testing.T) {
	a := &coretest.Adapter{}
	a.MetadataFunc = func(context.Context, pb.MarketType) ([]*pb.InstrumentMeta, error) {
		return nil, errors.New("venue unreachable")
	}
	r := newRefresher(t, metadata.Options{
		Adapter:     a,
		Publisher:   newRecorder(),
		Instruments: refs(t, "BTC_USDT"),
		Interval:    time.Hour,
	})
	if !r.WaitReady(context.Background()) {
		t.Error("WaitReady blocked with Required false")
	}
}

func TestNewRejectsIncompleteOptions(t *testing.T) {
	a := &coretest.Adapter{}
	full := metadata.Options{
		Venue:        coretest.Venue,
		Adapter:      a,
		Publisher:    newRecorder(),
		Log:          quiet(),
		Instruments:  refs(t, "BTC_USDT"),
		MarketType:   coretest.MarketType,
		Interval:     time.Hour,
		FetchTimeout: time.Second,
	}
	if _, err := metadata.New(full); err != nil {
		t.Fatalf("New with complete options: %v", err)
	}

	for name, mutate := range map[string]func(*metadata.Options){
		"no venue":         func(o *metadata.Options) { o.Venue = "" },
		"no adapter":       func(o *metadata.Options) { o.Adapter = nil },
		"no publisher":     func(o *metadata.Options) { o.Publisher = nil },
		"no logger":        func(o *metadata.Options) { o.Log = nil },
		"no instruments":   func(o *metadata.Options) { o.Instruments = nil },
		"no interval":      func(o *metadata.Options) { o.Interval = 0 },
		"no fetch timeout": func(o *metadata.Options) { o.FetchTimeout = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			opts := full
			mutate(&opts)
			if _, err := metadata.New(opts); err == nil {
				t.Error("New accepted them")
			}
		})
	}
}
