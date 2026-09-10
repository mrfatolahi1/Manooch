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

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/core/coretest"
	"github.com/you/manooch/internal/metadata"
	"github.com/you/manooch/internal/transport"
	"github.com/you/manooch/pkg/price"
	"google.golang.org/protobuf/proto"
)

// recorder captures published metadata instead of dialing Redis.
type recorder struct {
	mutex      sync.Mutex
	messages   map[string]*manoochv1.InstrumentMeta
	timeToLive time.Duration
	n          int
	err        error
}

func newRecorder() *recorder { return &recorder{messages: map[string]*manoochv1.InstrumentMeta{}} }

func (recorder *recorder) Publish(_ context.Context, key string, message proto.Message, timeToLive time.Duration) error {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	if recorder.err != nil {
		return recorder.err
	}
	recorder.messages[key] = proto.Clone(message).(*manoochv1.InstrumentMeta)
	recorder.timeToLive, recorder.n = timeToLive, recorder.n+1
	return nil
}

func (recorder *recorder) Close() error { return nil }

func (recorder *recorder) get(key string) *manoochv1.InstrumentMeta {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return recorder.messages[key]
}

func (recorder *recorder) count() int {
	recorder.mutex.Lock()
	defer recorder.mutex.Unlock()
	return recorder.n
}

// reporter records what the refresher told health.
type reporter struct {
	mutex  sync.Mutex
	ok     bool
	reason string
	calls  int
}

func (h *reporter) MetadataState(ok bool, reason string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.ok, h.reason, h.calls = ok, reason, h.calls+1
}

func (h *reporter) state() (bool, string) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	return h.ok, h.reason
}

func refs(t *testing.T, symbols ...string) []core.InstrumentRef {
	t.Helper()
	out := make([]core.InstrumentRef, 0, len(symbols))
	for _, s := range symbols {
		reference, err := core.ParseCanonical(s, coretest.MarketType)
		if err != nil {
			t.Fatal(err)
		}
		out = append(out, reference)
	}
	return out
}

// logs captures log output so a test can assert a WARN line was written; the
// change log is the only record that a venue moved a tick size.
type logs struct {
	mutex sync.Mutex
	buf   bytes.Buffer
}

func (l *logs) Write(p []byte) (int, error) {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.buf.Write(p)
}

func (l *logs) String() string {
	l.mutex.Lock()
	defer l.mutex.Unlock()
	return l.buf.String()
}

func quiet() *slog.Logger { return slog.New(slog.NewTextHandler(io.Discard, nil)) }

func newRefresher(t *testing.T, options metadata.Options) *metadata.Refresher {
	t.Helper()
	if options.Venue == "" {
		options.Venue = coretest.Venue
	}
	if options.Log == nil {
		options.Log = quiet()
	}
	if options.MarketType == manoochv1.MarketType_MARKET_TYPE_UNSPECIFIED {
		options.MarketType = coretest.MarketType
	}
	if options.Interval == 0 {
		options.Interval = 20 * time.Millisecond
	}
	if options.FetchTimeout == 0 {
		options.FetchTimeout = time.Second
	}
	if options.Backoff.Initial == 0 {
		options.Backoff = transport.Policy{Initial: time.Millisecond, Max: 5 * time.Millisecond, Multiplier: 2, Jitter: "none"}
	}
	refresher, err := metadata.New(options)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return refresher
}

func metaFor(t *testing.T, adapter *coretest.Adapter, symbols ...string) []*manoochv1.InstrumentMeta {
	t.Helper()
	tick, err := price.ParsePrice("0.1")
	if err != nil {
		t.Fatal(err)
	}
	lot, err := price.ParseSize("0.001")
	if err != nil {
		t.Fatal(err)
	}
	out := make([]*manoochv1.InstrumentMeta, 0, len(symbols))
	for _, reference := range refs(t, symbols...) {
		out = append(out, adapter.Metadata(reference, tick, lot, 1))
	}
	return out
}

// TestPublishesEveryConfiguredInstrument: the whole set goes out each cycle, on
// the metadata key, with a TTL of twice the refresh interval.
func TestPublishesEveryConfiguredInstrument(t *testing.T) {
	adapter := &coretest.Adapter{}
	adapter.MetadataFunc = func(context.Context, manoochv1.MarketType) ([]*manoochv1.InstrumentMeta, error) {
		return metaFor(t, adapter, "BTC_USDT", "ETH_USDT", "SOL_USDT"), nil
	}
	recorder := newRecorder()
	health := &reporter{}

	refresher := newRefresher(t, metadata.Options{
		Adapter:     adapter,
		Publisher:   recorder,
		Health:      health,
		Instruments: refs(t, "BTC_USDT", "ETH_USDT"),
		Interval:    time.Hour,
		Required:    true,
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go refresher.Run(ctx)

	if !refresher.WaitReady(ctx) {
		t.Fatal("metadata never became ready")
	}

	for _, key := range []string{
		"Manooch:TESTVENUE:PERP_LINEAR:BTC_USDT:metadata",
		"Manooch:TESTVENUE:PERP_LINEAR:ETH_USDT:metadata",
	} {
		metadata := recorder.get(key)
		if metadata == nil {
			t.Fatalf("no message on %s", key)
		}
		if metadata.Env.Status == manoochv1.Status_STATUS_UNSPECIFIED {
			t.Errorf("%s: status is unspecified", key)
		}
		if metadata.TickSize <= 0 || metadata.LotSize <= 0 || metadata.ContractMultiplier <= 0 {
			t.Errorf("%s: tick=%d lot=%d multiplier=%d", key, metadata.TickSize, metadata.LotSize, metadata.ContractMultiplier)
		}
	}
	// SOL was not configured, so it is not published: a venue's whole contract
	// list is hundreds of keys nobody subscribed to.
	if metadata := recorder.get("Manooch:TESTVENUE:PERP_LINEAR:SOL_USDT:metadata"); metadata != nil {
		t.Error("an unconfigured instrument was published")
	}
	if recorder.timeToLive != 2*time.Hour {
		t.Errorf("ttl = %v, want twice the refresh interval", recorder.timeToLive)
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
	var mutex sync.Mutex
	adapter := &coretest.Adapter{}
	adapter.MetadataFunc = func(context.Context, manoochv1.MarketType) ([]*manoochv1.InstrumentMeta, error) {
		mutex.Lock()
		attempts++
		n := attempts
		mutex.Unlock()
		if n < 3 {
			return nil, errors.New("venue unreachable")
		}
		return metaFor(t, adapter, "BTC_USDT"), nil
	}
	recorder := newRecorder()
	health := &reporter{}

	refresher := newRefresher(t, metadata.Options{
		Adapter:     adapter,
		Publisher:   recorder,
		Health:      health,
		Instruments: refs(t, "BTC_USDT"),
		Interval:    time.Hour,
		Required:    true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	// Before anything runs, nothing may stream.
	select {
	case <-refresher.Ready():
		t.Fatal("ready before the first fetch")
	default:
	}

	go refresher.Run(ctx)

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

	if !refresher.WaitReady(ctx) {
		t.Fatal("metadata never became ready after the venue recovered")
	}
	if ok, _ := health.state(); !ok {
		t.Error("health still reports metadata unavailable after a successful fetch")
	}
	if recorder.get("Manooch:TESTVENUE:PERP_LINEAR:BTC_USDT:metadata") == nil {
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
	var mutex sync.Mutex
	adapter := &coretest.Adapter{}
	adapter.MetadataFunc = func(context.Context, manoochv1.MarketType) ([]*manoochv1.InstrumentMeta, error) {
		mutex.Lock()
		cycle++
		n := cycle
		mutex.Unlock()
		reference := refs(t, "BTC_USDT")[0]
		if n == 1 {
			return []*manoochv1.InstrumentMeta{adapter.Metadata(reference, tick, lot, 1)}, nil
		}
		return []*manoochv1.InstrumentMeta{adapter.Metadata(reference, tick2, lot, 2)}, nil
	}

	out := &logs{}
	refresher := newRefresher(t, metadata.Options{
		Adapter:     adapter,
		Publisher:   newRecorder(),
		Instruments: refs(t, "BTC_USDT"),
		Interval:    10 * time.Millisecond,
		Required:    true,
		Log:         slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go refresher.Run(ctx)

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
	adapter := &coretest.Adapter{}
	adapter.MetadataFunc = func(context.Context, manoochv1.MarketType) ([]*manoochv1.InstrumentMeta, error) {
		return metaFor(t, adapter, "BTC_USDT"), nil
	}
	out := &logs{}
	refresher := newRefresher(t, metadata.Options{
		Adapter:     adapter,
		Publisher:   newRecorder(),
		Instruments: refs(t, "BTC_USDT", "DOGE_USDT"),
		Interval:    time.Hour,
		Required:    true,
		Log:         slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: slog.LevelWarn})),
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	go refresher.Run(ctx)

	if !refresher.WaitReady(ctx) {
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
	adapter := &coretest.Adapter{}
	adapter.MetadataFunc = func(context.Context, manoochv1.MarketType) ([]*manoochv1.InstrumentMeta, error) {
		return metaFor(t, adapter, "SOL_USDT"), nil
	}
	health := &reporter{}
	refresher := newRefresher(t, metadata.Options{
		Adapter:     adapter,
		Publisher:   newRecorder(),
		Health:      health,
		Instruments: refs(t, "BTC_USDT"),
		Interval:    time.Hour,
		Required:    true,
	})

	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	refresher.Run(ctx)

	if ok, _ := health.state(); ok {
		t.Error("health reports metadata available when nothing configured was listed")
	}
	select {
	case <-refresher.Ready():
		t.Error("ready with no configured instrument listed")
	default:
	}
}

// TestNotRequiredDoesNotBlockStreaming: when metadata is not a startup
// dependency, WaitReady returns at once and the refresher still runs.
func TestNotRequiredDoesNotBlockStreaming(t *testing.T) {
	adapter := &coretest.Adapter{}
	adapter.MetadataFunc = func(context.Context, manoochv1.MarketType) ([]*manoochv1.InstrumentMeta, error) {
		return nil, errors.New("venue unreachable")
	}
	refresher := newRefresher(t, metadata.Options{
		Adapter:     adapter,
		Publisher:   newRecorder(),
		Instruments: refs(t, "BTC_USDT"),
		Interval:    time.Hour,
	})
	if !refresher.WaitReady(context.Background()) {
		t.Error("WaitReady blocked with Required false")
	}
}

func TestNewRejectsIncompleteOptions(t *testing.T) {
	adapter := &coretest.Adapter{}
	full := metadata.Options{
		Venue:        coretest.Venue,
		Adapter:      adapter,
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
		"no venue":         func(options *metadata.Options) { options.Venue = "" },
		"no adapter":       func(options *metadata.Options) { options.Adapter = nil },
		"no publisher":     func(options *metadata.Options) { options.Publisher = nil },
		"no logger":        func(options *metadata.Options) { options.Log = nil },
		"no instruments":   func(options *metadata.Options) { options.Instruments = nil },
		"no interval":      func(options *metadata.Options) { options.Interval = 0 },
		"no fetch timeout": func(options *metadata.Options) { options.FetchTimeout = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			options := full
			mutate(&options)
			if _, err := metadata.New(options); err == nil {
				t.Error("New accepted them")
			}
		})
	}
}
