// Package price implements the fixed-point types every price, size and rate is
// carried in, on the wire and in memory.
//
//	Price   1e-11    0.00000000001 .. 92,233,720
//	Size    1e-8     0.00000001    .. 92,233,720,368
//	Rate    1e-12    -9,223,372    .. 9,223,372
//
// There is no float64 in the value path here and must not be one in a caller's:
// 53 bits of mantissa cannot hold 68432.15 and 0.00000000012 exactly in the
// same feed.
package price

import (
	"errors"
	"strconv"
	"strings"
)

// Decimal exponents of the three scales.
const (
	PriceExp = -11
	SizeExp  = -8
	RateExp  = -12
)

// Integer value of 1.0 at each scale.
const (
	PriceScale = 100_000_000_000
	SizeScale  = 100_000_000
	RateScale  = 1_000_000_000_000
)

// A Price is a price at scale 1e-11. Negative prices are not representable.
type Price int64

// A Size is a quantity at scale 1e-8: base units on a linear instrument,
// contracts on an inverse one. Negative sizes are not representable.
type Size int64

// A Rate is a rate at scale 1e-12. Rates may be negative.
type Rate int64

// Parse errors. A value that does not fit the scale exactly is an error, never
// a clamped or truncated number that looks fine downstream.
var (
	ErrEmpty         = errors.New("price: empty string")
	ErrSyntax        = errors.New("price: not a decimal number")
	ErrNotFinite     = errors.New("price: NaN and Inf are not representable")
	ErrNegative      = errors.New("price: negative value")
	ErrOutOfRange    = errors.New("price: value out of range for the scale")
	ErrPrecisionLoss = errors.New("price: more precision than the scale holds")
)

// maxInt64Digits is the digit count of math.MaxInt64; a longer digit string is
// rejected without being built.
const maxInt64Digits = 19

// expLimit saturates the parsed exponent, keeping the shift arithmetic from
// overflowing on adversarial input.
const expLimit = 1 << 30

// ParsePrice parses a venue's decimal string into a Price.
func ParsePrice(s string) (Price, error) {
	v, err := parse(s, PriceExp, false)
	return Price(v), err
}

// ParseSize parses a venue's decimal string into a Size.
func ParseSize(s string) (Size, error) {
	v, err := parse(s, SizeExp, false)
	return Size(v), err
}

// ParseRate parses a venue's decimal string into a Rate. Unlike prices and
// sizes, rates may be negative.
func ParseRate(s string) (Rate, error) {
	v, err := parse(s, RateExp, true)
	return Rate(v), err
}

// parse converts a decimal string to an integer at 10**scaleExp.
//
// It works on the digits directly rather than through strconv.ParseFloat and a
// multiply, which loses the bottom of the range. Significant digits are
// collected as bytes, the decimal point and exponent fold into one
// power-of-ten shift, and strconv.ParseInt does the range check.
func parse(s string, scaleExp int, signed bool) (int64, error) {
	if s == "" {
		return 0, ErrEmpty
	}

	i := 0
	negative := false
	switch s[0] {
	case '+':
		i = 1
	case '-':
		negative = true
		i = 1
	}
	if i == len(s) {
		return 0, ErrSyntax
	}

	// These parse happily as float64 and must not parse at all here.
	switch strings.ToLower(s[i:]) {
	case "nan", "inf", "infinity":
		return 0, ErrNotFinite
	}

	// digits holds significant digits only: leading zeros are never appended,
	// so digits[0] is never '0' and an empty slice means the value is zero.
	var digits []byte
	var frac int // digits after the decimal point
	sawDot, sawDigit, nonzero := false, false, false
	for ; i < len(s); i++ {
		c := s[i]
		if c == 'e' || c == 'E' {
			break
		}
		if c == '.' {
			if sawDot {
				return 0, ErrSyntax
			}
			sawDot = true
			continue
		}
		if c < '0' || c > '9' {
			return 0, ErrSyntax
		}
		sawDigit = true
		if sawDot {
			frac++
		}
		if c != '0' {
			nonzero = true
		}
		if nonzero {
			digits = append(digits, c)
		}
	}
	if !sawDigit {
		return 0, ErrSyntax
	}

	exp := 0
	if i < len(s) {
		i++ // consume 'e' or 'E'
		esign := 1
		if i < len(s) && (s[i] == '+' || s[i] == '-') {
			if s[i] == '-' {
				esign = -1
			}
			i++
		}
		if i == len(s) {
			return 0, ErrSyntax
		}
		for ; i < len(s); i++ {
			c := s[i]
			if c < '0' || c > '9' {
				return 0, ErrSyntax
			}
			if exp < expLimit {
				exp = exp*10 + int(c-'0')
			}
		}
		exp *= esign
	}

	// Zero is zero at every exponent, and -0 is not negative.
	if len(digits) == 0 {
		return 0, nil
	}
	if negative && !signed {
		return 0, ErrNegative
	}

	// digits means digits * 10**(exp-frac); we want that times 10**-scaleExp.
	shift := exp - frac - scaleExp
	switch {
	case shift < 0:
		// Digits dropped off the right must all be zero, or the value does not
		// fit the scale exactly.
		drop := -shift
		if drop >= len(digits) {
			return 0, ErrPrecisionLoss
		}
		for _, c := range digits[len(digits)-drop:] {
			if c != '0' {
				return 0, ErrPrecisionLoss
			}
		}
		digits = digits[:len(digits)-drop]
	case shift > 0:
		if len(digits)+shift > maxInt64Digits {
			return 0, ErrOutOfRange
		}
		for k := 0; k < shift; k++ {
			digits = append(digits, '0')
		}
	}

	if len(digits) > maxInt64Digits {
		return 0, ErrOutOfRange
	}
	v, err := strconv.ParseInt(string(digits), 10, 64)
	if err != nil {
		return 0, ErrOutOfRange
	}
	if negative {
		v = -v
	}
	return v, nil
}

// format renders v at 10**scaleExp as the shortest exact decimal: no trailing
// fractional zeros, no exponent. It inverts parse for every value parse emits.
func format(v int64, scaleExp int) string {
	negative := v < 0
	var u uint64
	if negative {
		u = uint64(-(v + 1)) + 1 // correct at math.MinInt64, unlike -v
	} else {
		u = uint64(v)
	}

	d := strconv.FormatUint(u, 10)
	scale := -scaleExp
	if len(d) <= scale {
		d = strings.Repeat("0", scale-len(d)+1) + d
	}
	intPart, fracPart := d[:len(d)-scale], d[len(d)-scale:]
	fracPart = strings.TrimRight(fracPart, "0")

	var builder strings.Builder
	builder.Grow(len(d) + 2)
	if negative {
		builder.WriteByte('-')
	}
	builder.WriteString(intPart)
	if fracPart != "" {
		builder.WriteByte('.')
		builder.WriteString(fracPart)
	}
	return builder.String()
}

// String renders the exact decimal value.
func (price Price) String() string { return format(int64(price), PriceExp) }

// String renders the exact decimal value.
func (size Size) String() string { return format(int64(size), SizeExp) }

// String renders the exact decimal value.
func (rate Rate) String() string { return format(int64(rate), RateExp) }

// Float is lossy and is for display only. Never feed it back into a
// calculation.
func (price Price) Float() float64 { return float64(price) / float64(PriceScale) }

// Float is lossy and is for display only.
func (size Size) Float() float64 { return float64(size) / float64(SizeScale) }

// Float is lossy and is for display only.
func (rate Rate) Float() float64 { return float64(rate) / float64(RateScale) }

// Compare returns -1, 0 or +1 as the receiver is less than, equal to, or
// greater than other.
func (price Price) Compare(other Price) int { return compareInt64(int64(price), int64(other)) }

// Compare returns -1, 0 or +1 as the receiver is less than, equal to, or
// greater than other.
func (size Size) Compare(other Size) int { return compareInt64(int64(size), int64(other)) }

// Compare returns -1, 0 or +1 as the receiver is less than, equal to, or
// greater than other.
func (rate Rate) Compare(other Rate) int { return compareInt64(int64(rate), int64(other)) }

func compareInt64(first, second int64) int {
	switch {
	case first < second:
		return -1
	case first > second:
		return 1
	default:
		return 0
	}
}
