// Package adaptertest is the conformance suite every venue adapter must pass.
//
// It is deliberately venue-agnostic: it drives core.Adapter and asserts the
// normalized output, so a second venue calls the same function against its own
// fixture directory with no change here. If a new venue needs a change to this
// file, the interface it is testing was the wrong shape.
package adaptertest

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/internal/publish"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Update rewrites the .golden files instead of comparing against them.
// Regenerating is fine; committing the diff without reading it is not — those
// files are the only statement of what a frame is supposed to become.
var Update = flag.Bool("update", false, "rewrite the .golden files")

// ReceivedNs is the arrival time every fixture is parsed with. It is a constant
// so the golden output is a function of the frame alone; a real clock here
// would make every golden file rot within a nanosecond of being written.
const ReceivedNs = 1562305380123456789

// determinismRuns is how many times Parse is called on one fixture in
// RunAdapterDeterminism. Everything else rests on this property, so it is
// worth the cost of proving rather than assuming.
const determinismRuns = 1000

// result is the golden form of one Parse call.
type result struct {
	Error    *resultError    `json:"error"`
	Messages []resultMessage `json:"messages"`
}

type resultError struct {
	Kind    string `json:"kind"`
	Channel string `json:"channel"`
	Symbol  string `json:"symbol,omitempty"`
	Message string `json:"message"`
}

type resultMessage struct {
	Key        string          `json:"key"`
	Channel    string          `json:"channel"`
	TimeToLive string          `json:"ttl"`
	Payload    json.RawMessage `json:"payload"`
}

// RunAdapterConformance drives every fixture in dir through a.Parse and
// compares the normalized output against the fixture's .golden file.
//
// A fixture is <case>.json holding one raw venue frame exactly as it arrives
// on the wire; <case>.golden holds what it must become.
func RunAdapterConformance(t *testing.T, adapter core.Adapter, dir string) {
	t.Helper()

	frames, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) == 0 {
		t.Fatalf("no fixtures in %s", dir)
	}

	for _, path := range frames {
		name := strings.TrimSuffix(filepath.Base(path), ".json")
		t.Run(name, func(t *testing.T) {
			frame, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}

			messages, parseErr := adapter.Parse(frame, ReceivedNs)

			// A frame either becomes messages or fails. A partial result
			// published as if it were whole is the silent wrongness this
			// service exists to prevent.
			if parseErr != nil && len(messages) > 0 {
				t.Errorf("Parse returned %d messages alongside an error: %v", len(messages), parseErr)
			}
			if parseErr != nil {
				var parseError *core.ParseError
				if !errors.As(parseErr, &parseError) {
					t.Errorf("Parse error is %T, not *core.ParseError: %v", parseErr, parseErr)
				}
			}
			for i, message := range messages {
				checkMessage(t, adapter, i, message)
			}
			checkDeterminism(t, adapter, frame)

			got, err := render(messages, parseErr)
			if err != nil {
				t.Fatal(err)
			}
			compareGolden(t, strings.TrimSuffix(path, ".json")+".golden", got)
		})
	}
}

// RunAdapterDeterminism proves the property everything else rests on: the same
// frame and the same arrival time produce byte-identical protobuf, every time.
// Without it, a fixture test proves only what happened on one run.
func RunAdapterDeterminism(t *testing.T, adapter core.Adapter, dir string) {
	t.Helper()

	frames, err := filepath.Glob(filepath.Join(dir, "*.json"))
	if err != nil {
		t.Fatal(err)
	}
	if len(frames) == 0 {
		t.Fatalf("no fixtures in %s", dir)
	}

	for _, path := range frames {
		name := strings.TrimSuffix(filepath.Base(path), ".json")
		t.Run(name, func(t *testing.T) {
			frame, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			want, wantErr := marshalAll(t, adapter, frame)

			for i := range determinismRuns {
				got, gotErr := marshalAll(t, adapter, frame)
				if (gotErr == nil) != (wantErr == nil) {
					t.Fatalf("run %d: error %v, first run %v", i, gotErr, wantErr)
				}
				if len(got) != len(want) {
					t.Fatalf("run %d: %d messages, first run %d", i, len(got), len(want))
				}
				for j := range got {
					if !bytesEqual(got[j], want[j]) {
						t.Fatalf("run %d message %d: protobuf differs from the first run", i, j)
					}
				}
			}
		})
	}
}

// marshalAll parses one frame and marshals every message deterministically.
func marshalAll(t *testing.T, adapter core.Adapter, frame []byte) ([][]byte, error) {
	t.Helper()

	messages, err := adapter.Parse(frame, ReceivedNs)
	if err != nil {
		return nil, err
	}
	out := make([][]byte, 0, len(messages))
	for _, message := range messages {
		b, metadataErr := proto.MarshalOptions{Deterministic: true}.Marshal(message.Proto)
		if metadataErr != nil {
			t.Fatalf("marshal %s: %v", message.Key, metadataErr)
		}
		out = append(out, b)
	}
	return out, nil
}

func bytesEqual(a, b []byte) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

// checkMessage asserts the invariants every adapter owes its caller, whatever
// the venue. They are checked here rather than in a golden file because a
// golden records what happened; these say what must be true.
func checkMessage(t *testing.T, adapter core.Adapter, i int, message core.Message) {
	t.Helper()
	where := fmt.Sprintf("message %d (%s)", i, core.ChannelName(message.Channel))

	envelope := envelopeOf(t, where, message.Proto)
	if envelope == nil {
		return
	}

	// Never publish data without a status: a consumer that cannot tell healthy
	// from stale is worse off than one with no data.
	if envelope.Status == manoochv1.Status_STATUS_UNSPECIFIED {
		t.Errorf("%s: status is unspecified", where)
	}
	if envelope.Venue != adapter.Venue() {
		t.Errorf("%s: venue = %q, want %q", where, envelope.Venue, adapter.Venue())
	}
	if envelope.Channel != message.Channel {
		t.Errorf("%s: envelope channel %v disagrees with the message's %v", where, envelope.Channel, message.Channel)
	}
	if message.Specification.Channel != message.Channel {
		t.Errorf("%s: spec channel %v disagrees with the message's %v", where, message.Specification.Channel, message.Channel)
	}

	// Keys are built by publish.Key, never by concatenation: a key with a typo
	// is written and published successfully and read by nobody.
	want := publish.Key(adapter.Venue(), message.Specification.Instrument.MarketType, message.Specification.Instrument.Canonical(), message.Channel)
	if message.Key != want {
		t.Errorf("%s: key = %q, want %q", where, message.Key, want)
	}

	// recv_time_ns is stamped in the read loop and carried through untouched.
	if envelope.RecvTimeNs != ReceivedNs {
		t.Errorf("%s: recv_time_ns = %d, want the value handed to Parse (%d)", where, envelope.RecvTimeNs, ReceivedNs)
	}
	if envelope.ExchangeTimeNs <= 0 {
		t.Errorf("%s: exchange_time_ns = %d", where, envelope.ExchangeTimeNs)
	}

	// Fields the publisher owns. An adapter that fills one here would have it
	// overwritten, and publish_time_ns in particular would then measure when
	// we decided to publish rather than when we did.
	if envelope.PublishTimeNs != 0 {
		t.Errorf("%s: publish_time_ns is set by the adapter (%d); only the publisher may", where, envelope.PublishTimeNs)
	}
	if envelope.PublishSeq != 0 {
		t.Errorf("%s: publish_seq is set by the adapter (%d)", where, envelope.PublishSeq)
	}
	if envelope.InstanceId != "" {
		t.Errorf("%s: instance_id is set by the adapter (%q)", where, envelope.InstanceId)
	}
	if envelope.SchemaVersion != 0 {
		t.Errorf("%s: schema_version is set by the adapter (%d)", where, envelope.SchemaVersion)
	}

	// A sequence number that is not present must be zero: a non-zero one that
	// nobody set reads as a real venue sequence to a consumer checking gaps.
	if !envelope.VenueSeqPresent && envelope.VenueSeq != 0 {
		t.Errorf("%s: venue_seq = %d with venue_seq_present false", where, envelope.VenueSeq)
	}

	if envelope.Instrument == nil {
		t.Errorf("%s: no instrument", where)
		return
	}
	if envelope.Instrument.VenueSymbol == "" {
		t.Errorf("%s: no venue_symbol; the order service has nothing to trade on", where)
	}
	if got, want := envelope.Instrument.Canonical, message.Specification.Instrument.Canonical(); got != want {
		t.Errorf("%s: canonical = %q, want %q", where, got, want)
	}
	if message.TimeToLive < 0 {
		t.Errorf("%s: ttl = %v", where, message.TimeToLive)
	}
}

// checkDeterminism is the cheap in-line version of RunAdapterDeterminism: two
// runs, so a fixture that parses differently on a second call fails in the
// conformance run rather than only in the slower dedicated test.
func checkDeterminism(t *testing.T, adapter core.Adapter, frame []byte) {
	t.Helper()

	first, firstErr := marshalAll(t, adapter, frame)
	second, secondErr := marshalAll(t, adapter, frame)
	if (firstErr == nil) != (secondErr == nil) {
		t.Errorf("Parse is not deterministic: errors %v then %v", firstErr, secondErr)
		return
	}
	if len(first) != len(second) {
		t.Errorf("Parse is not deterministic: %d messages then %d", len(first), len(second))
		return
	}
	for i := range first {
		if !bytesEqual(first[i], second[i]) {
			t.Errorf("Parse is not deterministic: message %d differs between runs", i)
		}
	}
}

func envelopeOf(t *testing.T, where string, message proto.Message) *manoochv1.Envelope {
	t.Helper()

	enveloped, ok := message.(interface{ GetEnv() *manoochv1.Envelope })
	if !ok {
		t.Errorf("%s: %T carries no envelope", where, message)
		return nil
	}
	envelope := enveloped.GetEnv()
	if envelope == nil {
		t.Errorf("%s: %T has a nil envelope", where, message)
	}
	return envelope
}

// render turns one Parse result into the golden form. protojson injects
// randomised whitespace on purpose, so its output is re-encoded through
// encoding/json, which sorts map keys and gives a stable file.
func render(messages []core.Message, parseErr error) ([]byte, error) {
	out := result{Messages: []resultMessage{}}

	if parseErr != nil {
		var parseError *core.ParseError
		if errors.As(parseErr, &parseError) {
			out.Error = &resultError{
				Kind:    parseError.Kind,
				Channel: core.ChannelName(parseError.Channel),
				Symbol:  parseError.Symbol,
				Message: parseError.Error(),
			}
		} else {
			out.Error = &resultError{Kind: "unclassified", Message: parseErr.Error()}
		}
	}

	for _, message := range messages {
		raw, err := protojson.Marshal(message.Proto)
		if err != nil {
			return nil, err
		}
		var normalized any
		if err := json.Unmarshal(raw, &normalized); err != nil {
			return nil, err
		}
		payload, err := json.Marshal(normalized)
		if err != nil {
			return nil, err
		}
		out.Messages = append(out.Messages, resultMessage{
			Key:        message.Key,
			Channel:    core.ChannelName(message.Channel),
			TimeToLive: message.TimeToLive.String(),
			Payload:    payload,
		})
	}

	b, err := json.MarshalIndent(out, "", "  ")
	if err != nil {
		return nil, err
	}
	return append(b, '\n'), nil
}

func compareGolden(t *testing.T, path string, got []byte) {
	t.Helper()

	if *Update {
		if err := os.WriteFile(path, got, 0o644); err != nil {
			t.Fatal(err)
		}
		return
	}
	want, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("%v (run: go test ./internal/adapter/... -update)", err)
	}
	if string(got) != string(want) {
		t.Errorf("normalized output does not match %s\n got: %s\nwant: %s", filepath.Base(path), got, want)
	}
}
