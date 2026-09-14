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

Order book is the only channel this adapter can serve. Tabdeal's public FAPI
(`ping`, `time`, `exchangeInfo`, `depth`, `aggDepth`, plus the depth and trade
broadcast streams) has no mark price, index price, or funding rate surface —
nothing equivalent to Binance Futures' `/fapi/v1/premiumIndex` or a
`markPrice` stream. `mark_price`/`index_price`/`funding` cannot be added to
`config/venues/tabdeal.yaml` until Tabdeal publishes one.
