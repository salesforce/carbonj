# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

CarbonJ is a high-performance, drop-in replacement for `carbon-cache` and `carbon-relay` in the Graphite metrics stack. It is a Spring Boot 3 application running on JDK 17 that uses an embedded RocksDB for time-series storage. It supports both direct ingestion (Line / Pickle protocols) and AWS Kinesis-based ingestion, and serves Graphite-compatible JSON / Pickle queries.

The repo is a Gradle multi-project build with two modules:
- `carbonj.service` — the Spring Boot service (the actual server)
- `cc-metrics-reporter` — a small library that publishes internal Dropwizard metrics

## Common commands

Build, test, and Docker image (run from repo root):

```bash
./gradlew build                                            # full build + tests
./gradlew :carbonj.service:test                            # tests for the service module only
./gradlew :carbonj.service:test --tests <FQCN>             # run a single JUnit5 test class/method
./gradlew :carbonj.service:bootJar                         # produce the runnable Spring Boot jar
./gradlew :carbonj.service:docker -PdockerRepo="my-repo/"  # build the multi-arch container image
./gradlew :carbonj.service:rpm                             # build the RPM package
./gradlew printCoverageReport                              # JaCoCo coverage report (CSV printed)
```

Notes on testing:
- Tests use JUnit 5 (`useJUnitPlatform()`) and are launched with `--add-opens java.base/java.util=ALL-UNNAMED` and `AWS_REGION=us-east-1` set automatically.
- Some integration tests use Testcontainers + LocalStack (Kinesis / DynamoDB), so a working Docker daemon is required for the full suite.
- Test resources live in `carbonj.service/src/test/resources` (e.g. `aggregation-rules-test.conf`, `relay-rules.conf`, `storage-aggregation.conf`).

Running the service locally (after `bootJar`):

```bash
java --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED \
     -jar carbonj.service/build/libs/carbonj.service-*.jar
# defaults: HTTP on :2001, Spring profile `dev`, store disabled
```

Helm deploy is documented in `README.md`; values live in `kube/helm/carbonj/values.yaml`.

## High-level architecture

The service is structured around an **ingest → accumulate → store → query** pipeline. The packages below correspond to those phases (all under `carbonj.service/src/main/java/com/demandware/carbonj/service`).

### Ingestion (`engine/`)
- `CarbonJServiceMain` — Spring Boot entry point.
- `LineProtocolHandler`, `PickleProtocolHandler`, `netty/` — Netty-based listeners for Carbon line and pickle protocols.
- `KinesisConsumer`, `KinesisRecordProcessor`, `kinesis/` — alternative ingestion path using AWS Kinesis (KCL v3). `cfgKinesis.java` holds tunables like `maxRecords`. Checkpointing is via `FileCheckPointMgr` or `DynamoDbCheckPointMgr`.
- `Relay`, `RelayRouter`, `RelayRules`, `DestinationGroup`, `destination/` — carbon-relay-compatible routing of incoming points to one or more downstream destinations (other CarbonJ shards, Kinesis streams, etc.). `relay-rules.conf` and `blacklist.conf` configure this.
- `InputQueue`, `PointProcessor*`, `Consumers` — bounded queues and worker pools that decode incoming `DataPoint`s and push them downstream.

### Aggregation (`accumulator/`)
Implements the Graphite "aggregator-style" rollups defined in `aggregation-rules-test.conf` / `audit-rules.conf`. `AccumulatorImpl` time-slots points, applies `MetricAggregationRule`s, and emits aggregated points back into the pipeline. `recovery/` reloads in-flight slots after a restart.

### Storage (`db/`)
`TimeSeriesStoreImpl` is the central facade; it owns:
- `db/index/` — name → numeric id mapping and tree navigation backed by `IndexStoreRocksDB`. `MetricIndexImpl` translates Graphite glob patterns (`QueryUtils`, `QueryPart`) into id lookups.
- `db/points/` — per-resolution RocksDB column families (`60s24h`, `5m7d`, `30m2y` archives) accessed through `DataPointArchiveRocksDB`. Aggregation between resolutions is handled by `IntervalProcessor*` and the `StagingFile*` classes (sequentialize random writes by sorting before merging into the lower-resolution archive).
- `db/model/` — domain types (`Metric`, `Series`, `RetentionPolicy`, `DataPointStore`, etc.).

The retention layout (60-second / 5-minute / 30-minute archives) is the key invariant — when modifying storage code, preserve the dbName values (`60s24h`, `5m7d`, `30m2y`) since admin APIs and external scripts depend on them.

### HTTP / admin surface (`engine/Graphite*Servlet`, `admin/`)
Exposes Graphite-compatible read endpoints plus a CarbonJ-specific admin REST API rooted at `/_dw/rest/carbonj/...` (see `admin-api.md` and `delete-api.md` at the repo root).

### Configuration
- Spring `@Configuration` classes are named `cfg*.java` (`cfgCarbonJ`, `cfgKinesis`, `cfgAccumulator`, `cfgDataPoints`, `cfgMetricIndex`, `cfgTimeSeriesStorage`, `cfgCentralThreadPools`, …). Look here first when wiring up new beans or tunables.
- Runtime config: `carbonj.service/src/main/resources/application.yml` (profiles: `dev`, `test`, `prod`). The shipped Docker image overrides these via env vars and files copied in via `src/main/docker/files/` (notably `service.args`, `audit-rules.conf`, `blacklist.conf`, `query-blacklist.conf`, `relay-rules.conf`, `storage-aggregation.conf`).
- `version` is set in root `gradle.properties` and propagated by the `net.researchgate.release` plugin.

### Runtime ops scripts
`carbonj.service/src/main/docker/files/` ships several Python / shell helpers baked into the container image (`deletemetrics.py`, `cj-load.py`, `check-invalid-namespaces.py`, `reportRocksDbMetrics.sh`, `disklog.sh`, etc.). Update these in lockstep with related Java changes — the Dockerfile copies them into `/app/bin/`.

## Conventions worth knowing

- All sources are licensed under BSD-3-Clause; the `com.github.hierynomus.license` Gradle plugin enforces the header from `LICENSE-HEADER-JAVA` on every Java file and `src/main/docker/files/*`. `licenseFormat` will fix headers; `licenseMain` runs as part of `build` and will fail on missing headers.
- Java 17 source/target across all subprojects; do not introduce features beyond JDK 17.
- Releases run from `master` only (`release.git.requireBranch = 'master'`).
- CI (`.github/workflows/gradle.yml`) runs `./gradlew build printCoverageReport` on every push and publishes a multi-arch Docker image on non-dependabot branches.
