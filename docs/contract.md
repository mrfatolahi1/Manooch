Covers: M0 · `schema/manooch.proto`, `gen/manoochv1/`

The wire format between Manooch and every consumer. Generated Go is committed so a consumer never needs `protoc`.

| File | Holds |
|---|---|
| `schema/manooch.proto` | 5 enums, 11 messages, `proto3`, package `manooch.v1` |
| `schema/README.md` | Evolution rules and the regeneration command |
| `gen/manoochv1/manooch.pb.go` | Generated; 16 Go types. Do not edit |

Enums: `MarketType` (spot, margin, perp/future × linear/inverse), `Channel` (orderbook, trades, mark_price, index_price, funding, metadata, health), `Source`, `Status`, `Side` — each with `_UNSPECIFIED = 0`.

Messages: `Instrument`, `Envelope`, `PriceLevel`, `OrderBook`, `Trade`, `Trades`, `MarkPrice`, `IndexPrice`, `Funding`, `InstrumentMeta`, `Health`. The last two are defined but nothing writes them at M0.

**Every payload carries `Envelope env = 1`.** That is what `publish.enveloped` asserts on and how `publish.Decode` reaches the envelope without knowing the concrete type.

## Who fills the envelope

| Filled by | Fields |
|---|---|
| Producer (`internal/synth` now, an adapter in M1) | `venue`, `instrument`, `channel`, `exchange_time_ns`, `recv_time_ns`, `venue_seq`, `venue_seq_present`, `source`, `status`, `status_reason` |
| `publish.RedisPublisher.Publish` | `publish_seq`, `instance_id`, `schema_version`, `publish_time_ns`, and `venue` if left empty |
| Nothing at M0 | `price_exp`, `size_exp` — left `0`, which the schema defines as the global scale (`-11`, `-8`) |

## Rules

- **Never renumber, reuse, or retype a field.** protobuf decodes a changed field without complaining; an old consumer reads the new bytes into the old meaning and reports no error.
- **New fields are additive with a new number.** Ignoring what it does not know is the only change an old consumer survives.
- **Bump `schema_version` when an existing field's meaning changes.** Same number and type with different semantics is the one change the wire format cannot signal on its own.
- **Regenerate and commit `gen/` in the same commit as the `.proto`** (`make proto`).
- **Keep `Envelope` as field 1 of every payload**, or `publish.Decode` breaks.
