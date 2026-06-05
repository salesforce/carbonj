# Testing rules

- Tests use **JUnit 5** (`useJUnitPlatform()`); do not introduce JUnit 4 styles.
- Run the service module's tests with `./gradlew :carbonj.service:test`.
- Run a single test class/method:
  `./gradlew :carbonj.service:test --tests com.demandware.carbonj.service.SomeTest.someMethod`
- Tests run with `--add-opens java.base/java.util=ALL-UNNAMED` and `AWS_REGION=us-east-1` set automatically by the build.
- **Integration tests need Docker** — they use Testcontainers + LocalStack (Kinesis / DynamoDB). If Docker is unavailable, scope to unit tests with `--tests <FQCN>`.
- Test resources live in `carbonj.service/src/test/resources/` (e.g. `aggregation-rules-test.conf`, `relay-rules.conf`, `storage-aggregation.conf`). Reuse existing fixtures rather than inventing new config files.
- After running tests, HTML reports are at `carbonj.service/build/reports/tests/test/index.html` and surefire XML at `carbonj.service/build/test-results/test/`.
- JaCoCo coverage runs as part of `build`; print a summary with `./gradlew printCoverageReport`.
- Do **not** mock RocksDB — there are real on-disk fixtures in tests; follow the patterns in `db/` tests.
- Before declaring a change done, run at minimum `./gradlew :carbonj.service:test` for the touched module.
