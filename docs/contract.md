Covers: M3 · `schema/manooch.proto`, `gen/manoochv1/`

The wire format between Manooch and every consumer. Generated Go is committed so a consumer never needs `protoc`.

| File | Holds |
|---|---|
| `schema/manooch.proto` | 4 enums, 11 messages, `proto3`, package `manooch.v1` |
| `schema/README.md` | Evolution rules and the regeneration command |
| `gen/manoochv1/manooch.pb.go` | Generated; 15 Go types. Do not edit |

Enums: `MarketType` (spot, margin, perp/future × linear/inverse), `Channel` (orderbook, mark_price, index_price, funding, metadata, health, ratelimit), `Source`, `Status` — each with `_UNSPECIFIED = 0`.

Messages: `Instrument`, `Envelope`, `PriceLevel`, `OrderBook`, `MarkPrice`, `IndexPrice`, `Funding`, `InstrumentMeta`, `RateLimit`, `RateLimitBudget`, `Health`. Everything here is written by something.

## `exchange_time_is_send_time`

Added at M3, field 17. It says whether `exchange_time_ns` is the venue's clock at the moment it sent the message, so a consumer may difference it against `recv_time_ns`, or an event time that may be far in the past.

KuCoin stamps a funding rate with the instant it settled — hours old on arrival — so differencing it reports a four-hour clock skew on a healthy venue. `internal/supervisor` measures skew only from a send time, and `publish.RedisPublisher` keeps event times out of the publish-latency histogram for the same reason. **The default is false**: a missing signal is better than a wrong one, so an adapter opts in.

## Order books

`OrderBook` carries a complete snapshot for one configured symbol. `bids` are descending by price and `asks` ascending; `depth` is the number of levels delivered. Tabdeal's futures adapter fills it from the public depth REST endpoint and its public depth stream.

```protobuf
reserved 2;
reserved "CHANNEL_TRADES";
```

Both forms matter. The numeric reservation stops a future channel decoding as the retired one; the name reservation stops a config or a `protojson` payload written against the old names from resolving to something new.

`publish.schema_version` is **3** because channel 1 is active again for order books.

`MarketType` keeps every value. Only `PERP_LINEAR` is used; deleting the rest buys nothing and reservations would clutter the file.

**Every payload carries `Envelope env = 1`.** That is what `publish.enveloped` asserts on and how `publish.Decode` reaches the envelope without knowing the concrete type.

## Who fills the envelope

| Filled by | Fields |
|---|---|
| Producer (a venue adapter, `internal/metadata`, `internal/ratelimit`) | `venue`, `instrument`, `channel`, `exchange_time_ns`, `exchange_time_is_send_time`, `recv_time_ns`, `venue_seq`, `venue_seq_present`, `source`, `status`, `status_reason` |
| `publish.RedisPublisher.Publish` | `publish_seq`, `instance_id`, `schema_version`, `publish_time_ns`, and `venue` if left empty |
| Nothing yet | `price_exp`, `size_exp` — left `0`, which the schema defines as the global scale (`-11`, `-8`) |

## Rules

- **Never renumber, reuse, or retype a field.** protobuf decodes a changed field without complaining; an old consumer reads the new bytes into the old meaning and reports no error.
- **New fields are additive with a new number.** Ignoring what it does not know is the only change an old consumer survives.
- **Bump `schema_version` when an existing field's meaning changes.** Same number and type with different semantics is the one change the wire format cannot signal on its own.
- **Reserve by number and by name when deleting.** A reused number decodes silently into the retired meaning.
- **Regenerate and commit `gen/` in the same commit as the `.proto`** using the
  command in `schema/README.md`.
- **Keep `Envelope` as field 1 of every payload**, or `publish.Decode` breaks.
