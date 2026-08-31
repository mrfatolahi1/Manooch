Covers: M1 · `schema/manooch.proto`, `gen/manoochv1/`

The wire format between Manooch and every consumer. Generated Go is committed so a consumer never needs `protoc`.

| File | Holds |
|---|---|
| `schema/manooch.proto` | 4 enums, 7 messages, `proto3`, package `manooch.v1` |
| `schema/README.md` | Evolution rules and the regeneration command |
| `gen/manoochv1/manooch.pb.go` | Generated; 11 Go types. Do not edit |

Enums: `MarketType` (spot, margin, perp/future × linear/inverse), `Channel` (mark_price, index_price, funding, metadata, health), `Source`, `Status` — each with `_UNSPECIFIED = 0`.

Messages: `Instrument`, `Envelope`, `MarkPrice`, `IndexPrice`, `Funding`, `InstrumentMeta`, `Health`. The last two are defined but nothing writes them yet.

## Scope reduction at M1

The service is perp mark price only. `OrderBook`, `Trades`, `Trade`, `PriceLevel` and `Side` are **deleted**, and `Channel` reserves the numbers they used:

```protobuf
reserved 1, 2;
reserved "CHANNEL_ORDERBOOK", "CHANNEL_TRADES";
```

Both forms matter. The numeric reservation stops a future channel decoding as the retired one; the name reservation stops a config or a `protojson` payload written against the old names from resolving to something new.

`publish.schema_version` is **2**. Nothing about a `Channel` value's meaning is visible on the wire, so the version is the only signal a consumer gets that number 1 no longer means what it did.

`MarketType` keeps every value. Only `PERP_LINEAR` is used; deleting the rest buys nothing and reservations would clutter the file.

**Every payload carries `Envelope env = 1`.** That is what `publish.enveloped` asserts on and how `publish.Decode` reaches the envelope without knowing the concrete type.

## Who fills the envelope

| Filled by | Fields |
|---|---|
| Producer (a venue adapter, or `internal/synth`) | `venue`, `instrument`, `channel`, `exchange_time_ns`, `recv_time_ns`, `venue_seq`, `venue_seq_present`, `source`, `status`, `status_reason` |
| `publish.RedisPublisher.Publish` | `publish_seq`, `instance_id`, `schema_version`, `publish_time_ns`, and `venue` if left empty |
| Nothing yet | `price_exp`, `size_exp` — left `0`, which the schema defines as the global scale (`-11`, `-8`) |

## Rules

- **Never renumber, reuse, or retype a field.** protobuf decodes a changed field without complaining; an old consumer reads the new bytes into the old meaning and reports no error.
- **New fields are additive with a new number.** Ignoring what it does not know is the only change an old consumer survives.
- **Bump `schema_version` when an existing field's meaning changes.** Same number and type with different semantics is the one change the wire format cannot signal on its own.
- **Reserve by number and by name when deleting.** A reused number decodes silently into the retired meaning.
- **Regenerate and commit `gen/` in the same commit as the `.proto`** (`make proto`).
- **Keep `Envelope` as field 1 of every payload**, or `publish.Decode` breaks.
