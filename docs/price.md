Covers: M0 · `pkg/price`

## Purpose

Fixed-point `int64` types for every price, size and rate on the wire and in memory. It lives under `pkg/` rather than `internal/` because consumers import it to decode what Manooch publishes.

## Files

| Path | Holds |
|---|---|
| `pkg/price/price.go` | Types, scale constants, errors, the parser, formatting and comparison |
| `pkg/price/price_test.go` | Table tests per type, round-trip tests, `Cmp`, and `FuzzParsePrice` |

## Key types and functions

| Symbol | Signature | Notes |
|---|---|---|
| `PriceExp`, `SizeExp`, `RateExp` | `= -11`, `-8`, `-12` | Decimal exponent of each scale |
| `PriceScale`, `SizeScale`, `RateScale` | `= 100_000_000_000`, `100_000_000`, `1_000_000_000_000` | Integer value of `1.0` |
| `Price`, `Size`, `Rate` | `type X int64` | `Price` and `Size` reject negatives; `Rate` allows them |
| `ParsePrice` | `func(string) (Price, error)` | |
| `ParseSize` | `func(string) (Size, error)` | |
| `ParseRate` | `func(string) (Rate, error)` | Signed |
| `String` | `func (Price) string` (also `Size`, `Rate`) | Shortest exact decimal: no trailing fractional zeros, no exponent |
| `Float` | `func (Price) float64` (also `Size`, `Rate`) | Lossy; for logs and display only |
| `Cmp` | `func (Price) Cmp(Price) int` (also `Size`, `Rate`) | `-1`, `0`, `+1` |
| `ErrEmpty`, `ErrSyntax`, `ErrNotFinite`, `ErrNegative`, `ErrOutOfRange`, `ErrPrecisionLoss` | `error` | Compare with `errors.Is` |
| `parse` | `func(s string, scaleExp int, signed bool) (int64, error)` | Unexported; the one code path behind all three `Parse*` |
| `format` | `func(v int64, scaleExp int) string` | Unexported; inverse of `parse` |

### How `parse` works

It never goes through `float64`. It collects the mantissa's significant digits as bytes (leading zeros are dropped, so `digits[0]` is never `'0'` and an empty slice means the value is exactly zero), folds the decimal point and any exponent into one power-of-ten `shift`, then:

- `shift < 0` — drop `-shift` digits off the right. If any dropped digit is non-zero, or `-shift >= len(digits)`, return `ErrPrecisionLoss`.
- `shift > 0` — if `len(digits)+shift > 19` (the digit count of `MaxInt64`) return `ErrOutOfRange` without allocating; otherwise append zeros.
- Hand the digit string to `strconv.ParseInt`, which range-checks exactly. Any error becomes `ErrOutOfRange`.

The parsed exponent saturates at `expLimit = 1 << 30`, so `1e999999999999` cannot overflow the `shift` arithmetic.

## How it is used

`internal/config.validateScales` compares `scales.price_exp`/`size_exp`/`rate_exp` against `PriceExp`/`SizeExp`/`RateExp` and refuses to start on a mismatch. `internal/synth` parses its seed prices and sizes in `newMarket` and stores `Price`/`Size` values. `cmd/manooch-tap` wraps raw `int64` fields from decoded messages in `price.Price`/`Size`/`Rate` to render them.

## Rules

- **Do not add a `float64` path** — no `FromFloat`, no `strconv.ParseFloat` shortcut. A `float64` mantissa is 53 bits and cannot hold `68432.15` and `0.00000000012` exactly in the same feed; the resulting error is one unit in the last place, silent, and reaches a risk engine as a wrong number.
- **Do not clamp or truncate.** More precision than the scale holds is `ErrPrecisionLoss`, not a rounded value, because truncation changes the number with nothing to indicate it did.
- **Range-check before the assignment, not after.** Go's `int64` arithmetic wraps in silence: `1e20` at price scale becomes a negative number with no panic and no error. `FuzzParsePrice` asserts specifically that no successful `ParsePrice` returns a negative value.
- **Keep `String` exact and canonical.** `TestRoundTripCanonical` asserts `ParsePrice(s).String() == s` for the three ends of the range; `TestRoundTripNonCanonical` asserts `68432.150`, `+68432.15`, `0068432.15`, `6.843215e4` and `684321500e-4` all render as `68432.15`.
- **Changing a scale constant is a wire-format change.** `Envelope.price_exp`/`size_exp` are `0` on every message, meaning "the global scale"; a consumer built against the old constant would misread every number. Bump `schema_version` and see `docs/contract.md`.

## Not here

- The `int64` fields themselves: `docs/contract.md`.
- Where scales are checked against config: `docs/config.md`.
