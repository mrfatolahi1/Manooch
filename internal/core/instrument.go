// Package core holds the canonical identity types shared across Manooch: the
// one way an instrument is named, whatever a venue calls it.
package core

import (
	"fmt"
	"regexp"
	"strings"

	pb "github.com/you/manooch/gen/manoochv1"
)

// CanonicalPattern is the shape of a canonical symbol: "BTC_USDT".
const CanonicalPattern = `^[A-Z0-9]+_[A-Z0-9]+$`

var canonicalRe = regexp.MustCompile(CanonicalPattern)

// An InstrumentRef identifies one instrument on one market type. It is
// comparable, so it works as a map key.
//
// MarketType is part of the identity, not a decoration on it. BTC_USDT spot
// and BTC_USDT perpetual-linear are different instruments with different
// prices, and BTC_USD perpetual-inverse is a third one where a size means a
// count of contracts rather than an amount of BTC. Collapsing any of these
// into "BTC" is how a hedge ends up the wrong size.
type InstrumentRef struct {
	Base       string
	Quote      string
	Settle     string
	MarketType pb.MarketType
	Expiry     string // "20260626" for dated futures, "" otherwise
}

// Canonical returns the venue-independent symbol, "BTC_USDT".
func (r InstrumentRef) Canonical() string { return r.Base + "_" + r.Quote }

// String renders the full identity, including the parts Canonical drops.
func (r InstrumentRef) String() string {
	s := r.Canonical() + ":" + MarketTypeName(r.MarketType)
	if r.Expiry != "" {
		s += ":" + r.Expiry
	}
	return s
}

// ParseCanonical builds an InstrumentRef from a canonical symbol and a market
// type, filling in the settlement asset from the market type's convention:
// linear settles in the quote, inverse settles in the base, spot settles in
// neither. An adapter whose venue disagrees overwrites Settle afterwards.
func ParseCanonical(s string, mt pb.MarketType) (InstrumentRef, error) {
	if !canonicalRe.MatchString(s) {
		return InstrumentRef{}, fmt.Errorf("symbol %q does not match %s", s, CanonicalPattern)
	}
	if mt == pb.MarketType_MARKET_TYPE_UNSPECIFIED {
		return InstrumentRef{}, fmt.Errorf("symbol %q: market type is unspecified", s)
	}

	base, quote, _ := strings.Cut(s, "_")
	r := InstrumentRef{Base: base, Quote: quote, MarketType: mt}
	switch {
	case IsInverse(mt):
		r.Settle = base
	case IsDerivative(mt):
		r.Settle = quote
	}
	return r, nil
}

// Proto converts to the wire type. venueSymbol is what this venue calls the
// instrument ("BTCUSDT"); it is carried on every message because the order
// service needs it and should never have to reconstruct it.
func (r InstrumentRef) Proto(venueSymbol string) *pb.Instrument {
	return &pb.Instrument{
		Base:        r.Base,
		Quote:       r.Quote,
		Settle:      r.Settle,
		MarketType:  r.MarketType,
		Expiry:      r.Expiry,
		Canonical:   r.Canonical(),
		VenueSymbol: venueSymbol,
	}
}
