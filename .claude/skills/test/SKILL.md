---
name: test
description: Run CarbonJ tests. Defaults to the service module's full unit+integration suite. Pass an FQCN (e.g. `com.demandware.carbonj.service.db.points.SomeTest`) or pattern to scope.
---

# /test — Run CarbonJ tests

## Default (full service suite)
```bash
./gradlew :carbonj.service:test
```

## Targeted run
If the user passed an argument (FQCN or pattern):
```bash
./gradlew :carbonj.service:test --tests "<arg>"
```

## Both modules
```bash
./gradlew test
```

## Prerequisites
- JDK 17 on PATH (`java -version`).
- **Docker daemon running** if any test under `kinesis/`, `recovery/`, or anything using `@Testcontainers` is in scope (LocalStack-based). If Docker is not available, scope to unit-only with `--tests`.

## Success
- Gradle exits 0.
- Final line shows the test summary; per-test report at `carbonj.service/build/reports/tests/test/index.html`.

## On failure
1. Inspect the surefire XML at `carbonj.service/build/test-results/test/TEST-*.xml`.
2. Re-run a single failing test: `./gradlew :carbonj.service:test --tests <FQCN>.<method> --info`.
3. If the failure mentions Docker / Testcontainers / LocalStack, confirm `docker ps` works, then retry.
4. If the failure mentions license headers, run `./gradlew licenseFormat` and rebuild.
