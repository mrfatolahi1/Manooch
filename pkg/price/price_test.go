package price

import (
	"errors"
	"math"
	"strings"
	"testing"
)

func TestParsePrice(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want Price
		err  error
	}{
		// The three ends of the range this scale exists to cover.
		{"btc scale", "68432.15", 6_843_215_000_000_000, nil},
		{"meme coin scale", "0.0000082", 820_000, nil},
		{"btc quoted small cap", "0.00000000012", 12, nil},

		{"one", "1", PriceScale, nil},
		{"zero", "0", 0, nil},
		{"zero with fraction", "0.000", 0, nil},
		{"negative zero", "-0", 0, nil},
		{"smallest representable", "0.00000000001", 1, nil},
		{"leading plus", "+1.5", 150_000_000_000, nil},
		{"leading zeros", "0068432.15", 6_843_215_000_000_000, nil},
		{"trailing zeros", "68432.150000", 6_843_215_000_000_000, nil},
		{"integer with point", "42.", 4_200_000_000_000, nil},
		{"point with fraction", ".5", 50_000_000_000, nil},

		// Scientific notation — venues emit all of these shapes.
		{"sci small", "1.2e-9", 120, nil},
		{"sci upper e", "6.843215E4", 6_843_215_000_000_000, nil},
		{"sci positive exponent", "1e2", 100 * PriceScale, nil},
		{"sci explicit plus exponent", "1e+2", 100 * PriceScale, nil},
		{"sci at the bottom", "1e-11", 1, nil},
		{"sci zero mantissa huge exponent", "0e999999999999", 0, nil},

		// MaxInt64 at 1e-11 is 92233720.36854775807.
		{"max exact", "92233720.36854775807", math.MaxInt64, nil},
		{"max integer", "92233720", 9_223_372_000_000_000_000, nil},
		{"one past max", "92233720.36854775808", 0, ErrOutOfRange},
		{"overflow integer", "92233721", 0, ErrOutOfRange},
		{"overflow big", "1e20", 0, ErrOutOfRange},
		{"overflow huge exponent", "1e999999999999", 0, ErrOutOfRange},
		{"overflow long digits", "123456789012345678901234", 0, ErrOutOfRange},

		// Below the scale: truncating would silently change the number.
		{"precision loss one place", "0.000000000001", 0, ErrPrecisionLoss},
		{"precision loss deep", "1e-12", 0, ErrPrecisionLoss},
		{"precision loss tiny exponent", "1e-999999999999", 0, ErrPrecisionLoss},
		{"precision loss trailing digit", "1.234567890123", 0, ErrPrecisionLoss},

		// Prices are never negative.
		{"negative", "-1", 0, ErrNegative},
		{"negative fraction", "-0.5", 0, ErrNegative},

		{"empty", "", 0, ErrEmpty},
		{"nan", "NaN", 0, ErrNotFinite},
		{"nan lower", "nan", 0, ErrNotFinite},
		{"inf", "Inf", 0, ErrNotFinite},
		{"negative inf", "-Infinity", 0, ErrNotFinite},
		{"letters", "abc", 0, ErrSyntax},
		{"two points", "1.2.3", 0, ErrSyntax},
		{"bare point", ".", 0, ErrSyntax},
		{"bare sign", "-", 0, ErrSyntax},
		{"dangling exponent", "1e", 0, ErrSyntax},
		{"dangling exponent sign", "1e+", 0, ErrSyntax},
		{"exponent not a number", "1eX", 0, ErrSyntax},
		{"trailing space", "1.5 ", 0, ErrSyntax},
		{"underscore", "1_000", 0, ErrSyntax},
		{"hex", "0x10", 0, ErrSyntax},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			got, err := ParsePrice(testCase.in)
			if !errors.Is(err, testCase.err) {
				t.Fatalf("ParsePrice(%q) error = %v, want %v", testCase.in, err, testCase.err)
			}
			if got != testCase.want {
				t.Fatalf("ParsePrice(%q) = %d, want %d", testCase.in, got, testCase.want)
			}
		})
	}
}

func TestParseSize(t *testing.T) {
	cases := []struct {
		in   string
		want Size
		err  error
	}{
		{"1", SizeScale, nil},
		{"0.00000001", 1, nil},
		{"1234.5678", 123_456_780_000, nil},
		{"92233720368.54775807", math.MaxInt64, nil},
		{"92233720369", 0, ErrOutOfRange},
		{"0.000000001", 0, ErrPrecisionLoss},
		{"-1", 0, ErrNegative},
		{"", 0, ErrEmpty},
	}
	for _, testCase := range cases {
		got, err := ParseSize(testCase.in)
		if !errors.Is(err, testCase.err) || got != testCase.want {
			t.Errorf("ParseSize(%q) = %d, %v; want %d, %v", testCase.in, got, err, testCase.want, testCase.err)
		}
	}
}

func TestParseRate(t *testing.T) {
	cases := []struct {
		in   string
		want Rate
		err  error
	}{
		{"0.0001", 100_000_000, nil},
		{"-0.0001", -100_000_000, nil}, // funding goes negative routinely
		{"-0.000000000001", -1, nil},   // smallest representable, negative
		{"0.000000000001", 1, nil},
		{"9223372.036854775807", math.MaxInt64, nil},
		{"-9223372.036854775807", -math.MaxInt64, nil},
		{"9223373", 0, ErrOutOfRange},
		{"-9223373", 0, ErrOutOfRange},
		{"0.0000000000001", 0, ErrPrecisionLoss},
	}
	for _, testCase := range cases {
		got, err := ParseRate(testCase.in)
		if !errors.Is(err, testCase.err) || got != testCase.want {
			t.Errorf("ParseRate(%q) = %d, %v; want %d, %v", testCase.in, got, err, testCase.want, testCase.err)
		}
	}
}

// TestRoundTripCanonical: a canonical decimal survives parse -> String intact.
func TestRoundTripCanonical(t *testing.T) {
	prices := []string{
		"0", "1", "68432.15", "0.0000082", "0.00000000012",
		"0.00000000001", "92233720.36854775807", "0.1", "12345.6789",
	}
	for _, s := range prices {
		price, err := ParsePrice(s)
		if err != nil {
			t.Fatalf("ParsePrice(%q): %v", s, err)
		}
		if got := price.String(); got != s {
			t.Errorf("Price round trip: %q -> %d -> %q", s, price, got)
		}
	}

	sizes := []string{"0", "1", "0.00000001", "1234.5678", "92233720368.54775807"}
	for _, s := range sizes {
		v, err := ParseSize(s)
		if err != nil {
			t.Fatalf("ParseSize(%q): %v", s, err)
		}
		if got := v.String(); got != s {
			t.Errorf("Size round trip: %q -> %d -> %q", s, v, got)
		}
	}

	rates := []string{"0", "0.0001", "-0.0001", "0.000000000001", "-9223372.036854775807"}
	for _, s := range rates {
		v, err := ParseRate(s)
		if err != nil {
			t.Fatalf("ParseRate(%q): %v", s, err)
		}
		if got := v.String(); got != s {
			t.Errorf("Rate round trip: %q -> %d -> %q", s, v, got)
		}
	}
}

// TestRoundTripNonCanonical: the same number written differently lands on one
// canonical output.
func TestRoundTripNonCanonical(t *testing.T) {
	for _, in := range []string{"68432.150", "+68432.15", "0068432.15", "6.843215e4", "684321500e-4"} {
		price, err := ParsePrice(in)
		if err != nil {
			t.Fatalf("ParsePrice(%q): %v", in, err)
		}
		if got := price.String(); got != "68432.15" {
			t.Errorf("ParsePrice(%q).String() = %q, want %q", in, got, "68432.15")
		}
	}
}

func TestCompare(t *testing.T) {
	cases := []struct {
		first, second Price
		want          int
	}{
		{1, 2, -1},
		{2, 1, 1},
		{2, 2, 0},
		{0, 0, 0},
		{0, 1, -1},
		{math.MaxInt64, math.MaxInt64, 0},
		{math.MaxInt64, math.MaxInt64 - 1, 1},
	}
	for _, testCase := range cases {
		if got := testCase.first.Compare(testCase.second); got != testCase.want {
			t.Errorf("Price(%d).Compare(%d) = %d, want %d", testCase.first, testCase.second, got, testCase.want)
		}
	}

	// Rates compare across zero.
	if got := Rate(-1).Compare(Rate(1)); got != -1 {
		t.Errorf("Rate(-1).Compare(1) = %d, want -1", got)
	}
	if got := Rate(0).Compare(Rate(0)); got != 0 {
		t.Errorf("Rate(0).Compare(0) = %d, want 0", got)
	}
	if got := Size(5).Compare(Size(4)); got != 1 {
		t.Errorf("Size(5).Compare(4) = %d, want 1", got)
	}
}

func TestFloatIsApproximate(t *testing.T) {
	price, err := ParsePrice("68432.15")
	if err != nil {
		t.Fatal(err)
	}
	if math.Abs(price.Float()-68432.15) > 1e-6 {
		t.Errorf("Price.Float() = %v, want ~68432.15", price.Float())
	}
	if got := Rate(-100_000_000).Float(); math.Abs(got-(-0.0001)) > 1e-12 {
		t.Errorf("Rate.Float() = %v, want ~-0.0001", got)
	}
}

// FuzzParsePrice asserts the two properties that must hold for arbitrary venue
// input: the parser never panics, and never returns a wrapped value. A wrapped
// int64 is invisible — no panic, no error, just a negative price.
func FuzzParsePrice(f *testing.F) {
	seeds := []string{
		"", "0", "1", "-1", "68432.15", "0.0000082", "0.00000000012",
		"1.2e-9", "1e20", "NaN", "Inf", "+.5", "9" + strings.Repeat("0", 40),
		"0.000000000001", "92233720.36854775807", "1e999999999999",
		"---", "..", "1e-1e-1", "0x10", "1_000",
	}
	for _, s := range seeds {
		f.Add(s)
	}

	f.Fuzz(func(t *testing.T, s string) {
		price, err := ParsePrice(s)
		if err != nil {
			if price != 0 {
				t.Fatalf("ParsePrice(%q) returned %d alongside error %v", s, price, err)
			}
			return
		}
		if price < 0 {
			t.Fatalf("ParsePrice(%q) = %d: negative result means the value wrapped", s, price)
		}
		// A successful parse must round-trip through its canonical form.
		again, err := ParsePrice(price.String())
		if err != nil {
			t.Fatalf("ParsePrice(%q) = %d, but re-parsing %q failed: %v", s, price, price.String(), err)
		}
		if again != price {
			t.Fatalf("ParsePrice(%q) = %d, re-parse of %q = %d", s, price, price.String(), again)
		}

		// Sizes and rates share the parser.
		if size, err := ParseSize(s); err == nil && size < 0 {
			t.Fatalf("ParseSize(%q) = %d: negative result means the value wrapped", s, size)
		}
		if rate, err := ParseRate(s); err == nil {
			if again, err := ParseRate(rate.String()); err != nil || again != rate {
				t.Fatalf("ParseRate(%q) = %d, re-parse of %q = %d, %v", s, rate, rate.String(), again, err)
			}
		}
	})
}
