# Schema

`manooch.proto` is the contract between Manooch and every consumer (strategy,
risk, hedging). Generated Go lives in `gen/manoochv1/` and is **committed** — a
consumer must never need protoc to build against us.

## Evolution rules

These are not style preferences. A consumer deployed months ago is still
decoding messages we publish today, and protobuf will decode a changed field
without complaining — silently, into the wrong meaning.

- **Never renumber or reuse a field number.** A retired field number is retired
  forever. Mark it `reserved` rather than letting a future field claim it.
- **Never change a field's type.** Not even `int64` → `uint64`, not even
  `int32` → `int64`. The wire is permissive; the consumer is not.
- **New fields are always additive with a new number.** Old consumers ignore
  what they do not know, which is the only safe form of change.
- **Bump `schema_version` on any semantic change to an existing field's
  meaning.** Same number, same type, different meaning is the one change
  protobuf cannot protect anyone from. `Envelope.schema_version` is how a
  consumer notices.
- **Regenerate and commit `gen/` in the same commit as any `.proto` change.**
  A `gen/` that lags the `.proto` is a contract nobody can read.

## Regenerating

```sh
protoc --proto_path=schema \
  --go_out=. --go_opt=module=github.com/you/manooch \
  schema/manooch.proto
go build ./gen/...
```

Requires `protoc` and `protoc-gen-go` on `PATH`:

```
go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
```

## Scales

Every `price`-ish `int64` is at `1e-11`, every `size`-ish `int64` at `1e-8`,
every rate at `1e-12`. See `pkg/price`. `Envelope.price_exp` / `size_exp` are
`0` for "the global scale"; a non-zero value means this message deviates and
the consumer must honour it.
