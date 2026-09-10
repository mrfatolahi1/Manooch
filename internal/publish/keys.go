// Package publish owns the Redis key scheme and the write path onto it.
package publish

import (
	"fmt"
	"regexp"
	"strings"

	"github.com/you/manooch/gen/manoochv1"
	"github.com/you/manooch/internal/core"
)

// The key scheme is
//
//	Manooch:{VENUE}:{MARKET_TYPE}:{SYMBOL}:{channel}
//	Manooch:{VENUE}:venue:{subject}
//
// Build every key through Key or VenueKey, never by concatenation: a key with a
// typo is written and published successfully and read by nobody, so the stream
// looks dead to its consumer while every metric here says healthy.
const (
	// Prefix is the first component of every key Manooch writes.
	Prefix = "Manooch"
	// VenueScope is the market-type component of venue-wide keys.
	VenueScope = "venue"

	separator = ":"
)

// Venue-wide subjects.
const (
	SubjectHealth    = "health"
	SubjectRateLimit = "ratelimit"
)

// venueSubjects is what each venue-wide subject carries. A key's subject is
// the only thing that says what its bytes are, the same way a data key's
// channel component is.
var venueSubjects = map[string]manoochv1.Channel{
	SubjectHealth:    manoochv1.Channel_CHANNEL_HEALTH,
	SubjectRateLimit: manoochv1.Channel_CHANNEL_RATELIMIT,
}

// ChannelForSubject maps a venue-scoped key's subject to the channel whose
// message type it holds. It exists for manooch-tap and manooch-status, which
// are handed arbitrary keys by Redis and have nothing else to go on.
func ChannelForSubject(subject string) (manoochv1.Channel, bool) {
	channel, ok := venueSubjects[subject]
	return channel, ok
}

var (
	venueRe   = regexp.MustCompile(`^[A-Z0-9]+$`)
	symbolRe  = regexp.MustCompile(core.CanonicalPattern)
	subjectRe = regexp.MustCompile(`^[a-z0-9_]+$`)
)

// Key builds the key for one stream, which is also its Pub/Sub channel name:
// Redis keeps the two in separate namespaces, so one string is safely both.
func Key(venue string, marketType manoochv1.MarketType, symbol string, channel manoochv1.Channel) string {
	var builder strings.Builder
	builder.Grow(len(Prefix) + len(venue) + len(symbol) + 32)
	builder.WriteString(Prefix)
	builder.WriteString(separator)
	builder.WriteString(strings.ToUpper(venue))
	builder.WriteString(separator)
	builder.WriteString(core.MarketTypeName(marketType))
	builder.WriteString(separator)
	builder.WriteString(strings.ToUpper(symbol))
	builder.WriteString(separator)
	builder.WriteString(core.ChannelName(channel))
	return builder.String()
}

// VenueKey builds a key about the venue connection rather than an instrument.
func VenueKey(venue, subject string) string {
	return Prefix + separator + strings.ToUpper(venue) + separator + VenueScope + separator + strings.ToLower(subject)
}

// MatchPattern is the SCAN/PSUBSCRIBE glob for one venue, or every venue when
// venue is empty.
func MatchPattern(venue string) string {
	if venue == "" {
		return Prefix + separator + "*"
	}
	return Prefix + separator + strings.ToUpper(venue) + separator + "*"
}

// KeyParts is a parsed key. Data consumers never call ParseKey — the instrument
// identity they need is structured inside the message — but manooch-tap and
// manooch-status are handed keys by Redis and have nothing else to go on.
type KeyParts struct {
	Venue      string
	MarketType manoochv1.MarketType
	Symbol     string
	Channel    manoochv1.Channel

	// VenueScoped is true for Manooch:{VENUE}:venue:{subject} keys, where
	// MarketType, Symbol and Channel are unset.
	VenueScoped bool
	Subject     string
}

// String rebuilds the key, so ParseKey and Key round-trip.
func (keyParts KeyParts) String() string {
	if keyParts.VenueScoped {
		return VenueKey(keyParts.Venue, keyParts.Subject)
	}
	return Key(keyParts.Venue, keyParts.MarketType, keyParts.Symbol, keyParts.Channel)
}

// ParseKey splits a key and validates every component. An unrecognised key was
// either written by something else or written by us wrongly; both are worth an
// error rather than a best guess.
func ParseKey(s string) (KeyParts, error) {
	parts := strings.Split(s, separator)
	if len(parts) < 4 || len(parts) > 5 {
		return KeyParts{}, fmt.Errorf("key %q: want %s:{VENUE}:{MARKET_TYPE}:{SYMBOL}:{channel}", s, Prefix)
	}
	if parts[0] != Prefix {
		return KeyParts{}, fmt.Errorf("key %q: prefix must be %q", s, Prefix)
	}
	if !venueRe.MatchString(parts[1]) {
		return KeyParts{}, fmt.Errorf("key %q: venue %q must be upper case", s, parts[1])
	}
	keyParts := KeyParts{Venue: parts[1]}

	if parts[2] == VenueScope {
		if len(parts) != 4 {
			return KeyParts{}, fmt.Errorf("key %q: venue-scoped key must be %s:{VENUE}:%s:{subject}", s, Prefix, VenueScope)
		}
		if !subjectRe.MatchString(parts[3]) {
			return KeyParts{}, fmt.Errorf("key %q: subject %q must be lower snake case", s, parts[3])
		}
		keyParts.VenueScoped = true
		keyParts.Subject = parts[3]
		return keyParts, nil
	}

	if len(parts) != 5 {
		return KeyParts{}, fmt.Errorf("key %q: want 5 components, got %d", s, len(parts))
	}
	marketType, err := core.ParseMarketType(parts[2])
	if err != nil {
		return KeyParts{}, fmt.Errorf("key %q: %w", s, err)
	}
	if core.MarketTypeName(marketType) != parts[2] {
		return KeyParts{}, fmt.Errorf("key %q: market type %q must be upper case", s, parts[2])
	}
	if !symbolRe.MatchString(parts[3]) {
		return KeyParts{}, fmt.Errorf("key %q: symbol %q must match %s", s, parts[3], core.CanonicalPattern)
	}
	channel, err := core.ParseChannel(parts[4])
	if err != nil {
		return KeyParts{}, fmt.Errorf("key %q: %w", s, err)
	}
	if core.ChannelName(channel) != parts[4] {
		return KeyParts{}, fmt.Errorf("key %q: channel %q must be lower snake case", s, parts[4])
	}

	keyParts.MarketType, keyParts.Symbol, keyParts.Channel = marketType, parts[3], channel
	return keyParts, nil
}
