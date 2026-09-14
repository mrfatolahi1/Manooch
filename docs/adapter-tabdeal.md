Covers: `internal/adapter/tabdeal`

Tabdeal's futures adapter publishes perpetual-linear order-book snapshots for
the configured symbols. It subscribes to `special_margin@SYMBOL@depth@1000ms`
on `wss://api1.tabdeal.org/special_margin/stream/` and uses
`GET /fapi/v1/depth?symbol=...&limit=50` for REST fallback. The public API uses
underscore symbols such as `BTC_USDT`; stream payloads without the underscore
are accepted as well.

Each snapshot becomes one `OrderBook` protobuf message with fixed-point
`PriceLevel` bids and asks. `lastUpdateId` is carried as `venue_seq` for REST
snapshots, while Tabdeal's `E` field is treated as an optional send timestamp.

## Two symbol spellings, not one

Tabdeal spells the same instrument two different ways depending on which API
it's used with, and this adapter has to track both:

- **Market data** (`exchangeInfo`, `depth`, the depth stream): underscored,
  e.g. `BTC_USDT`. `VenueSymbol`/`ParseVenueSymbol` and every REST/WS call
  this adapter makes use this form — confirmed live: `GET
  /fapi/v1/depth?symbol=BTCUSDT` (no underscore) returns
  `{"code":1208,"msg":"Invalid symbol"}`, and subscribing to
  `special_margin@BTCUSDT@depth@1000ms` gets acked but silently delivers no
  frames.
- **Order placement** (`POST`/`GET`/`DELETE /fapi/v1/order`): contiguous,
  e.g. `BTCUSDT`, per Tabdeal's own API docs. `InstrumentMeta.venue_symbol` is
  documented as what an order service sends verbatim (`schema/manooch.proto`:
  `"BTCUSDT" — required, order service needs it`), so `FetchMetadata` builds
  it with the local `orderSymbol` helper rather than copying `exchangeInfo`'s
  own `"symbol"` field — that field is in the market-data spelling and would
  reach an order service in the wrong form otherwise.

Do not add a `symbol_overrides` entry to fix this: it feeds `VenueSymbol`,
which every market-data request also depends on, and would break the depth
REST/WS calls in exactly the way described above.

Order book is the only channel this adapter can serve. Tabdeal's public FAPI
(`ping`, `time`, `exchangeInfo`, `depth`, `aggDepth`, plus the depth and trade
broadcast streams) has no mark price, index price, or funding rate surface —
nothing equivalent to Binance Futures' `/fapi/v1/premiumIndex` or a
`markPrice` stream. `mark_price`/`index_price`/`funding` cannot be added to
`config/venues/tabdeal.yaml` until Tabdeal publishes one.
