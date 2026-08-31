package binance_test

import (
	"testing"

	"github.com/you/manooch/internal/adapter/adaptertest"
)

// TestParseIsDeterministic parses every fixture a thousand times and asserts
// byte-identical protobuf each time.
//
// This is the property everything else rests on. Fixture goldens only prove
// what one run produced; without this, a map iteration or a clock read inside
// Parse would pass the conformance suite and publish a different message every
// second in production.
func TestParseIsDeterministic(t *testing.T) {
	adaptertest.RunAdapterDeterminism(t, newAdapter(t), fixtureDir)
}
