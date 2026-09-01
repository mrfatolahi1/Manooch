package kucoin_test

import (
	"errors"
	"testing"
	"time"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/adapter/adaptertest"
	"github.com/you/manooch/internal/core"
	"github.com/you/manooch/pkg/price"
)

// TestConformance is the shared suite, run against this venue's fixtures with
// no venue-specific branch anywhere in it. That is the real test of M3: if
// KuCoin had needed a change there, the Adapter interface was the wrong shape.
func TestConformance(t *testing.T) {
	adaptertest.RunAdapterConformance(t, newAdapter(t), fixtureDir)
}

// TestParseIsDeterministic parses every fixture a thousand times and asserts
// byte-identical protobuf each time. Fixture goldens only prove what one run
// produced; without this, a map iteration or a clock read inside Parse would
// pass conformance and publish a different message every second in production.
func TestParseIsDeterministic(t *testing.T) {
	adaptertest.RunAdapterDeterminism(t, newAdapter(t), fixtureDir)
}

// TestOneTopicTwoSubjects is the shape of this venue: mark and index arrive
// together once a second, funding arrives on its own once a minute, and the
// three land on three keys with two different TTLs.
func TestOneTopicTwoSubjects(t *testing.T) {
	a := newAdapter(t)

	msgs, err := a.Parse([]byte(`{"topic":"/contract/instrument:XBTUSDTM","type":"message",`+
		`"subject":"mark.index.price","data":{"markPrice":90445.02,"indexPrice":90440.135,`+
		`"granularity":1000,"timestamp":1731899129000}}`), adaptertest.RecvNs)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("mark.index.price produced %d messages, want 2", len(msgs))
	}
	wantKeys := []string{
		"Manooch:KUCOIN:PERP_LINEAR:BTC_USDT:mark_price",
		"Manooch:KUCOIN:PERP_LINEAR:BTC_USDT:index_price",
	}
	for i, want := range wantKeys {
		if msgs[i].Key != want {
			t.Errorf("message %d key = %q, want %q", i, msgs[i].Key, want)
		}
		if msgs[i].TTL != 3*time.Second {
			t.Errorf("message %d ttl = %v, want 3s", i, msgs[i].TTL)
		}
	}

	const wantExchangeNs = 1731899129000 * int64(time.Millisecond)
	for i, m := range msgs {
		env := m.Proto.(interface{ GetEnv() *pb.Envelope }).GetEnv()
		if env.ExchangeTimeNs != wantExchangeNs {
			t.Errorf("message %d exchange_time_ns = %d, want %d", i, env.ExchangeTimeNs, wantExchangeNs)
		}
		if env.VenueSeqPresent {
			t.Errorf("message %d claims a venue sequence; this topic carries none", i)
		}
		if env.Instrument.VenueSymbol != "XBTUSDTM" {
			t.Errorf("message %d venue_symbol = %q", i, env.Instrument.VenueSymbol)
		}
	}
	// Each message needs its own envelope: one shared pointer would have the
	// publisher stamp the same publish_seq into both.
	e0 := msgs[0].Proto.(interface{ GetEnv() *pb.Envelope }).GetEnv()
	e1 := msgs[1].Proto.(interface{ GetEnv() *pb.Envelope }).GetEnv()
	if e0 == e1 {
		t.Error("two messages share one envelope")
	}

	if got := price.Price(msgs[0].Proto.(*pb.MarkPrice).MarkPrice).String(); got != "90445.02" {
		t.Errorf("mark price = %s, want 90445.02", got)
	}
	if got := price.Price(msgs[1].Proto.(*pb.IndexPrice).IndexPrice).String(); got != "90440.135" {
		t.Errorf("index price = %s, want 90440.135", got)
	}

	// Funding is a different subject at a different cadence, and therefore a
	// different TTL: 60s cadence against 1s, so 180s against 3s.
	funding, err := a.Parse([]byte(`{"topic":"/contract/instrument:XBTUSDTM","subject":"funding.rate",`+
		`"data":{"granularity":60000,"fundingRate":-0.002966,"timestamp":1551770400000}}`), adaptertest.RecvNs)
	if err != nil {
		t.Fatalf("Parse funding: %v", err)
	}
	if len(funding) != 1 {
		t.Fatalf("funding.rate produced %d messages, want 1", len(funding))
	}
	if funding[0].TTL != 180*time.Second {
		t.Errorf("funding ttl = %v, want 180s", funding[0].TTL)
	}
	f := funding[0].Proto.(*pb.Funding)
	if got := price.Rate(f.FundingRate).String(); got != "-0.002966" {
		t.Errorf("funding rate = %s, want -0.002966", got)
	}

	// The stream does not carry a next funding time and this adapter does not
	// invent one. A value extrapolated from a funding interval would look
	// exactly like one the venue supplied.
	if f.NextFundingTimeNs != 0 {
		t.Errorf("next_funding_time_ns = %d; KuCoin does not supply it on this stream", f.NextFundingTimeNs)
	}
	// granularity is how often the venue pushes the subject, not how often
	// funding settles. Passing one off as the other is a different wrong number.
	if f.IntervalSeconds != 0 {
		t.Errorf("interval_seconds = %d; the payload's granularity is a push cadence", f.IntervalSeconds)
	}
}

// TestUnquotedNumbersKeepTheirDigits is the quirk that would otherwise be
// invisible: KuCoin sends prices as JSON numbers, and unmarshalling one into a
// float64 rounds it before pkg/price ever sees a digit.
func TestUnquotedNumbersKeepTheirDigits(t *testing.T) {
	a := newAdapter(t)

	// 18 significant digits. float64 holds 15 or 16, so a value that survives
	// this round trip cannot have gone through one.
	msgs, err := a.Parse([]byte(`{"topic":"/contract/instrument:ETHUSDTM","type":"message",`+
		`"subject":"mark.index.price","data":{"markPrice":1234567.89012345678,`+
		`"indexPrice":0.00000000001,"granularity":1000,"timestamp":1731899129000}}`), adaptertest.RecvNs)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := msgs[0].Proto.(*pb.MarkPrice).MarkPrice; got != 123456789012345678 {
		t.Errorf("mark price = %d, want 123456789012345678 exactly", got)
	}
	if got := msgs[1].Proto.(*pb.IndexPrice).IndexPrice; got != 1 {
		t.Errorf("index price = %d, want 1 (the smallest representable price)", got)
	}
}

// TestQuotedNumbersParseToo: decoding into json.Number rather than float64 also
// makes the adapter indifferent to whether the venue quotes its numbers. If
// KuCoin starts sending "90445.02" the digits still reach pkg/price unchanged,
// where a float64 field would have failed outright and a string field would
// fail today.
func TestQuotedNumbersParseToo(t *testing.T) {
	a := newAdapter(t)
	msgs, err := a.Parse([]byte(`{"topic":"/contract/instrument:XBTUSDTM","type":"message",`+
		`"subject":"mark.index.price","data":{"markPrice":"90445.02","indexPrice":"90440.135",`+
		`"granularity":1000,"timestamp":1731899129000}}`), adaptertest.RecvNs)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(msgs) != 2 {
		t.Fatalf("messages = %d, want 2", len(msgs))
	}
	if got := price.Price(msgs[0].Proto.(*pb.MarkPrice).MarkPrice).String(); got != "90445.02" {
		t.Errorf("mark price = %s, want 90445.02", got)
	}
}

// TestWrongTimestampUnitIsRejected: KuCoin is not consistent about units across
// its futures topics. A nanosecond timestamp converted as milliseconds lands in
// the year 56000 and every freshness number computed from it is wrong while
// looking fine, so the magnitude is asserted rather than trusted.
func TestWrongTimestampUnitIsRejected(t *testing.T) {
	a := newAdapter(t)
	for name, ts := range map[string]string{
		"nanoseconds": "1731899129000000000",
		"seconds":     "1731899129",
		"zero":        "0",
		"negative":    "-1731899129000",
	} {
		t.Run(name, func(t *testing.T) {
			msgs, err := a.Parse([]byte(`{"topic":"/contract/instrument:XBTUSDTM","type":"message",`+
				`"subject":"mark.index.price","data":{"markPrice":90445.02,"indexPrice":90440.135,`+
				`"granularity":1000,"timestamp":`+ts+`}}`), adaptertest.RecvNs)
			if len(msgs) != 0 {
				t.Errorf("Parse produced %d messages for a %s timestamp", len(msgs), name)
			}
			var pe *core.ParseError
			if !errors.As(err, &pe) {
				t.Fatalf("Parse error = %v (%T), want *core.ParseError", err, err)
			}
			if pe.Kind != core.KindField {
				t.Errorf("kind = %q, want %q", pe.Kind, core.KindField)
			}
		})
	}
}

// TestOutOfRangePriceIsRejected: a price outside the scale is rejected and
// counted, never wrapped or clamped into something that looks like a price.
func TestOutOfRangePriceIsRejected(t *testing.T) {
	a := newAdapter(t)
	msgs, err := a.Parse([]byte(`{"topic":"/contract/instrument:XBTUSDTM","type":"message",`+
		`"subject":"mark.index.price","data":{"markPrice":99999999999,"indexPrice":90440.135,`+
		`"granularity":1000,"timestamp":1731899129000}}`), adaptertest.RecvNs)
	if len(msgs) != 0 {
		t.Errorf("Parse produced %d messages for an out-of-range price", len(msgs))
	}

	var pe *core.ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("Parse error = %v (%T), want *core.ParseError", err, err)
	}
	if pe.Kind != core.KindRange {
		t.Errorf("kind = %q, want %q", pe.Kind, core.KindRange)
	}
	if !errors.Is(err, price.ErrOutOfRange) {
		t.Errorf("error does not wrap price.ErrOutOfRange: %v", err)
	}
}

// TestControlFramesAreNotErrors: the welcome, the acks and the pongs are normal
// traffic on a healthy socket. Counting one as a failure would show parse
// errors climbing on a connection that is working perfectly.
func TestControlFramesAreNotErrors(t *testing.T) {
	a := newAdapter(t)
	for _, frame := range []string{
		`{"id":"test-connect-id","type":"welcome"}`,
		`{"id":"sub-0","type":"ack"}`,
		`{"id":"ping-7","type":"pong"}`,
	} {
		msgs, err := a.Parse([]byte(frame), adaptertest.RecvNs)
		if err != nil {
			t.Errorf("Parse(%s) = %v, want no error", frame, err)
		}
		if len(msgs) != 0 {
			t.Errorf("Parse(%s) produced %d messages", frame, len(msgs))
		}
	}
}

// TestErrorFrameIsTyped: a venue error is a classified failure, not a partial
// message. Half a frame published as if it were whole is the silent wrongness
// this service exists to prevent.
func TestErrorFrameIsTyped(t *testing.T) {
	a := newAdapter(t)
	msgs, err := a.Parse([]byte(`{"id":"sub-0","type":"error","code":404,"data":"topic not supported"}`), adaptertest.RecvNs)
	if len(msgs) != 0 {
		t.Errorf("Parse produced %d messages alongside a venue error", len(msgs))
	}
	var pe *core.ParseError
	if !errors.As(err, &pe) {
		t.Fatalf("Parse error = %v (%T), want *core.ParseError", err, err)
	}
	if pe.Kind != core.KindVenue {
		t.Errorf("kind = %q, want %q", pe.Kind, core.KindVenue)
	}
}

// TestZeroFundingRateIsPublished: zero is a real funding rate. Skipping it as
// though it were missing data would leave the key to expire on a venue that was
// answering perfectly well.
func TestZeroFundingRateIsPublished(t *testing.T) {
	a := newAdapter(t)
	msgs, err := a.Parse([]byte(`{"topic":"/contract/instrument:XBTUSDTM","subject":"funding.rate",`+
		`"data":{"granularity":60000,"fundingRate":0,"timestamp":1551770400000}}`), adaptertest.RecvNs)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(msgs) != 1 {
		t.Fatalf("messages = %d, want 1", len(msgs))
	}
	if got := msgs[0].Proto.(*pb.Funding).FundingRate; got != 0 {
		t.Errorf("funding rate = %d, want 0", got)
	}
	if msgs[0].TTL != 180*time.Second {
		t.Errorf("ttl = %v, want 180s", msgs[0].TTL)
	}
}

// TestUnknownSymbolAndSubjectAreClassified: all three are field failures, so
// the caller labels the metric from the error rather than by matching strings.
func TestUnknownSymbolAndSubjectAreClassified(t *testing.T) {
	a := newAdapter(t)
	for name, frame := range map[string]string{
		"unknown symbol":  `{"topic":"/contract/instrument:XBTXYZM","type":"message","subject":"mark.index.price","data":{"markPrice":1,"indexPrice":1,"timestamp":1731899129000}}`,
		"unknown subject": `{"topic":"/contract/instrument:XBTUSDTM","type":"message","subject":"open.interest","data":{"openInterest":123}}`,
		"unknown topic":   `{"topic":"/contractMarket/tickerV2:XBTUSDTM","type":"message","subject":"tickerV2","data":{"bestBidPrice":1}}`,
	} {
		t.Run(name, func(t *testing.T) {
			msgs, err := a.Parse([]byte(frame), adaptertest.RecvNs)
			if len(msgs) != 0 {
				t.Errorf("Parse produced %d messages", len(msgs))
			}
			var pe *core.ParseError
			if !errors.As(err, &pe) {
				t.Fatalf("Parse error = %v (%T), want *core.ParseError", err, err)
			}
			if pe.Kind != core.KindField {
				t.Errorf("kind = %q, want %q", pe.Kind, core.KindField)
			}
		})
	}
}

// TestMalformedFrameDoesNotPanic. A venue that starts sending shapes we cannot
// read must degrade the feed, not stop the process.
func TestMalformedFrameDoesNotPanic(t *testing.T) {
	a := newAdapter(t)
	for _, frame := range []string{
		`{"topic":`,
		``,
		`[]`,
		`{"topic":"/contract/instrument:XBTUSDTM","subject":"mark.index.price","data":"not an object"}`,
		`{"topic":"/contract/instrument:XBTUSDTM","subject":"mark.index.price","data":{"markPrice":true,"indexPrice":1,"timestamp":1731899129000}}`,
	} {
		msgs, _ := a.Parse([]byte(frame), adaptertest.RecvNs)
		if len(msgs) != 0 {
			t.Errorf("Parse(%q) produced %d messages", frame, len(msgs))
		}
	}
}
