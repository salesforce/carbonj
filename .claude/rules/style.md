# Code style rules

- **Java 17** source/target across all subprojects. Do not use language features beyond JDK 17.
- BSD-3-Clause license header (from `LICENSE-HEADER-JAVA`) is required on every Java file and every file in `carbonj.service/src/main/docker/files/`. The `licenseMain` task runs as part of `build` and **fails the build on missing headers**.
  - To auto-add/repair: `./gradlew licenseFormat`
  - To check: `./gradlew licenseMain` (or just `./gradlew build`)
- Spring `@Configuration` classes follow the `cfg*` naming convention (e.g. `cfgKinesis`, `cfgCarbonJ`). Add new config classes under `engine/` or the relevant package using the same pattern.
- Use `Slf4jLogger` / `org.slf4j.Logger` for logging — no `System.out.println`.
- Prefer constructor injection over field injection for new Spring beans.
- Versions for third-party deps live in root `gradle.properties`; do **not** hard-code versions in `build.gradle` files.
- Do not introduce new build tools or plugins without a clear reason; prefer existing tooling.
