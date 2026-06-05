---
name: debug
description: Structured triage workflow for CarbonJ bugs — reproduce, isolate to a pipeline phase (ingest / accumulate / store / query), then write a failing test before patching.
---

# /debug — CarbonJ bug triage

## 1. Reproduce
- Capture exact command, profile, and inputs that trigger the bug.
- If from production logs, note timestamps, metric names, and any `cfgKinesis` / `cfgAccumulator` overrides in effect.

## 2. Localize to a pipeline phase
CarbonJ is **ingest → accumulate → store → query**. Identify which phase failed:

| Symptom | Likely phase | Where to look |
|---|---|---|
| Points dropped at the edge, line-protocol parse errors | ingest | `engine/LineProtocolHandler`, `engine/PickleProtocolHandler`, `netty/`, `InputQueue`, `PointProcessor*` |
| Kinesis lag, checkpoint stuck, KCL warnings | ingest (Kinesis) | `engine/KinesisConsumer`, `KinesisRecordProcessor`, `cfgKinesis`, `FileCheckPointMgr`, `DynamoDbCheckPointMgr` |
| Wrong rollup values, missing aggregated series | accumulate | `accumulator/AccumulatorImpl`, `MetricAggregationRule`, `aggregation-rules-test.conf`, `recovery/` |
| Glob queries return nothing / wrong ids | store/index | `db/index/MetricIndexImpl`, `IndexStoreRocksDB`, `QueryUtils`, `QueryPart` |
| Data missing in 5m or 30m archive | store/points | `db/points/DataPointArchiveRocksDB`, `IntervalProcessor*`, `StagingFile*` |
| HTTP 500 / wrong JSON shape | query | `engine/Graphite*Servlet`, `admin/` |
| Wrong routing between shards | relay | `Relay`, `RelayRouter`, `RelayRules`, `relay-rules.conf`, `blacklist.conf` |

## 3. Read the matching `cfg*` class
The bean wiring for the phase is in the corresponding `@Configuration` class. Tunables almost always live there, not at the use site.

## 4. Write a failing test
Add a JUnit 5 test in the matching package under `carbonj.service/src/test/java/...`. Reuse fixtures from `carbonj.service/src/test/resources/` (e.g. `aggregation-rules-test.conf`).

Run scoped:
```bash
./gradlew :carbonj.service:test --tests <FQCN>.<method> --info
```

## 5. Patch and verify
- Make the smallest change that turns the test green.
- Re-run the full module suite: `./gradlew :carbonj.service:test`.
- Check license headers if you added new files: `./gradlew licenseMain`.

## Common gotchas
- Do not change the dbName values `60s24h`, `5m7d`, `30m2y` — admin APIs depend on them.
- LocalStack/Testcontainers tests need Docker; if the bug is in `kinesis/`, you need it running locally.
- Releases must run from `master`; debug commits should land on a feature branch.
