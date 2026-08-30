// Package publish owns the Redis key scheme and the write path onto it.
package publish

import (
	"fmt"
	"regexp"
	"strings"

	pb "github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
)

// The key scheme is
//
//	Manooch:{VENUE}:{MARKET_TYPE}:{SYMBOL}:{channel}
//	Manooch:{VENUE}:venue:{subject}
//
// with "Manooch" in title case, venue, market type and symbol upper case, and
// the channel in lower snake case.
//
// Build every key through Key or VenueKey. Concatenating one at a call site is
// forbidden, and not as a matter of taste: a key with a typo in it is written
// successfully, published successfully, and read by nobody. There is no error
// anywhere in that sequence — the stream simply appears dead to its consumer
// while every metric on this side says healthy.
const (
	// Prefix is the first component of every key Manooch writes.
	Prefix = "Manooch"
	// VenueScope is the market-type component of venue-wide keys.
	VenueScope = "venue"

	sep = ":"
)

// Venue-wide subjects.
const (
	SubjectHealth    = "health"
	SubjectRateLimit = "ratelimit"
)

var (
	venueRe   = regexp.MustCompile(`^[A-Z0-9]+$`)
	symbolRe  = regexp.MustCompile(core.CanonicalPattern)
	subjectRe = regexp.MustCompile(`^[a-z0-9_]+$`)
)

// Key builds the key for one stream. It is also the Pub/Sub channel name:
// Redis keeps keys and channels in separate namespaces, so one string can
// safely be both, and a consumer that has a key has the channel for free.
func Key(venue string, mt pb.MarketType, symbol string, ch pb.Channel) string {
	var b strings.Builder
	b.Grow(len(Prefix) + len(venue) + len(symbol) + 32)
	b.WriteString(Prefix)
	b.WriteString(sep)
	b.WriteString(strings.ToUpper(venue))
	b.WriteString(sep)
	b.WriteString(core.MarketTypeName(mt))
	b.WriteString(sep)
	b.WriteString(strings.ToUpper(symbol))
	b.WriteString(sep)
	b.WriteString(core.ChannelName(ch))
	return b.String()
}

// VenueKey builds a venue-wide key, one that is about the connection to the
// venue rather than about any single instrument.
func VenueKey(venue, subject string) string {
	return Prefix + sep + strings.ToUpper(venue) + sep + VenueScope + sep + strings.ToLower(subject)
}

// MatchPattern is the SCAN/PSUBSCRIBE glob for one venue, or for every venue
// when venue is empty.
func MatchPattern(venue string) string {
	if venue == "" {
		return Prefix + sep + "*"
	}
	return Prefix + sep + strings.ToUpper(venue) + sep + "*"
}

// KeyParts is a parsed key.
//
// Consumers of market data never call ParseKey: the instrument identity they
// need is inside the message, in a structured form that cannot be ambiguous.
// This exists for manooch-status and manooch-tap, which are handed keys by
// Redis and have nothing else to go on.
type KeyParts struct {
	Venue      string
	MarketType pb.MarketType
	Symbol     string
	Channel    pb.Channel

	// VenueScoped is true for Manooch:{VENUE}:venue:{subject} keys, where
	// MarketType, Symbol and Channel are unset.
	VenueScoped bool
	Subject     string
}

// String rebuilds the key, so ParseKey and Key round-trip.
func (p KeyParts) String() string {
	if p.VenueScoped {
		return VenueKey(p.Venue, p.Subject)
	}
	return Key(p.Venue, p.MarketType, p.Symbol, p.Channel)
}

// ParseKey splits a key and validates every component. Anything it does not
// recognise is an error rather than a best guess: a key that does not parse is
// a key something else wrote, or one we wrote wrongly, and both are worth
// hearing about.
func ParseKey(s string) (KeyParts, error) {
	parts := strings.Split(s, sep)
	if len(parts) < 4 || len(parts) > 5 {
		return KeyParts{}, fmt.Errorf("key %q: want %s:{VENUE}:{MARKET_TYPE}:{SYMBOL}:{channel}", s, Prefix)
	}
	if parts[0] != Prefix {
		return KeyParts{}, fmt.Errorf("key %q: prefix must be %q", s, Prefix)
	}
	if !venueRe.MatchString(parts[1]) {
		return KeyParts{}, fmt.Errorf("key %q: venue %q must be upper case", s, parts[1])
	}
	kp := KeyParts{Venue: parts[1]}

	if parts[2] == VenueScope {
		if len(parts) != 4 {
			return KeyParts{}, fmt.Errorf("key %q: venue-scoped key must be %s:{VENUE}:%s:{subject}", s, Prefix, VenueScope)
		}
		if !subjectRe.MatchString(parts[3]) {
			return KeyParts{}, fmt.Errorf("key %q: subject %q must be lower snake case", s, parts[3])
		}
		kp.VenueScoped = true
		kp.Subject = parts[3]
		return kp, nil
	}

	if len(parts) != 5 {
		return KeyParts{}, fmt.Errorf("key %q: want 5 components, got %d", s, len(parts))
	}
	mt, err := core.ParseMarketType(parts[2])
	if err != nil {
		return KeyParts{}, fmt.Errorf("key %q: %w", s, err)
	}
	if core.MarketTypeName(mt) != parts[2] {
		return KeyParts{}, fmt.Errorf("key %q: market type %q must be upper case", s, parts[2])
	}
	if !symbolRe.MatchString(parts[3]) {
		return KeyParts{}, fmt.Errorf("key %q: symbol %q must match %s", s, parts[3], core.CanonicalPattern)
	}
	ch, err := core.ParseChannel(parts[4])
	if err != nil {
		return KeyParts{}, fmt.Errorf("key %q: %w", s, err)
	}
	if core.ChannelName(ch) != parts[4] {
		return KeyParts{}, fmt.Errorf("key %q: channel %q must be lower snake case", s, parts[4])
	}

	kp.MarketType, kp.Symbol, kp.Channel = mt, parts[3], ch
	return kp, nil
}
