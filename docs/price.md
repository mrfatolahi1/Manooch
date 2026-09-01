Covers: M3 · `pkg/price`

Fixed-point `int64` types for every price, size and rate. Under `pkg/` because consumers import it to decode what we publish.

| File | Holds |
|---|---|
| `price.go` | Types, scale constants, errors, parser, formatting, comparison |
| `price_test.go` | Table tests per type, round-trips, `Cmp`, `FuzzParsePrice` |

| Scale | Exp | 1.0 is | Range |
|---|---|---|---|
| `Price` | `-11` | `100_000_000_000` | `0.00000000001` .. `92233720.36854775807` |
| `Size` | `-8` | `100_000_000` | `0.00000001` .. `92233720368.54775807` |
| `Rate` | `-12` | `1_000_000_000_000` | `±9223372.036854775807` |

`ParsePrice` / `ParseSize` / `ParseRate` in, `String` / `Float` / `Cmp` out. `Price` and `Size` reject negatives; `Rate` allows them. Errors: `ErrEmpty`, `ErrSyntax`, `ErrNotFinite`, `ErrNegative`, `ErrOutOfRange`, `ErrPrecisionLoss` — compare with `errors.Is`. Full API: `go doc ./pkg/price`.

## How parsing works

Unexported `parse` never touches `float64`. It collects significant digits as bytes (leading zeros dropped, so an empty slice means zero), folds the decimal point and exponent into one power-of-ten `shift`, then:

- `shift < 0` — drop `-shift` digits; any non-zero dropped digit is `ErrPrecisionLoss`.
- `shift > 0` — `len(digits)+shift > 19` is `ErrOutOfRange` before allocating; else append zeros.
- `strconv.ParseInt` does the final exact range check.

The parsed exponent saturates at `1 << 30`, so `1e999999999999` cannot overflow the shift arithmetic.

## Rules

- **No `float64` path — ever.** 53 mantissa bits cannot hold `68432.15` and `0.00000000012` exactly in one feed; the error is one unit in the last place, silent, and reaches a risk engine as a wrong number.
- **Never clamp or truncate.** More precision than the scale holds is an error, because truncation changes the number with nothing to signal it.
- **Range-check before assignment.** `int64` wraps in silence: `1e20` becomes negative with no panic. `FuzzParsePrice` asserts no successful parse returns a negative.
- **Changing a scale constant is a wire-format change.** `Envelope.price_exp`/`size_exp` are `0` on every message, meaning "global scale"; bump `schema_version`.
