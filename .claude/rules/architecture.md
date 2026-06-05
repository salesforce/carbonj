# Architecture rules

The service is an **ingest → accumulate → store → query** pipeline. Respect package boundaries:

- `engine/` — ingestion (Line/Pickle/Netty, Kinesis), relay routing, input queues, point processors, HTTP servlets.
- `accumulator/` — aggregator-style rollups; `AccumulatorImpl` slots points and applies `MetricAggregationRule`s.
- `db/` — storage facade `TimeSeriesStoreImpl`.
  - `db/index/` — name → numeric id, glob queries (`MetricIndexImpl`, `IndexStoreRocksDB`, `QueryUtils`).
  - `db/points/` — per-resolution RocksDB column families and archive readers/writers.
  - `db/model/` — domain types (`Metric`, `Series`, `RetentionPolicy`, `DataPointStore`).
- `admin/` — REST admin endpoints rooted at `/_dw/rest/carbonj/...`.

## Invariants — do not break

- **Retention dbName values** (`60s24h`, `5m7d`, `30m2y`) are part of the public surface. Admin APIs and external scripts depend on them.
- The 60s / 5m / 30m archive layout is the storage contract — schema changes here need migration plans.
- Kinesis tunables live in `cfgKinesis.java`; checkpointing is `FileCheckPointMgr` or `DynamoDbCheckPointMgr`.
- Runtime configuration order of authority: env vars → `application.yml` profile → `service.args` → defaults.
- Python/shell helpers in `carbonj.service/src/main/docker/files/` are baked into the image. Update them in lockstep with related Java changes.

When wiring new beans/tunables, look at the matching `cfg*` class first instead of inventing a new injection site.
