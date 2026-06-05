---
name: lint
description: Verify and auto-fix the BSD-3-Clause license headers that the build enforces. CarbonJ has no checkstyle/spotless — license headers are the only enforced static check.
---

# /lint — License header check (the only enforced static check)

## Verify
```bash
./gradlew licenseMain
```
Runs as part of `./gradlew build`. Fails if any Java file or `src/main/docker/files/*` is missing the BSD-3-Clause header from `LICENSE-HEADER-JAVA`.

## Auto-fix
```bash
./gradlew licenseFormat
```
Inserts/repairs headers in place. Re-run `./gradlew licenseMain` to confirm.

## Success
- `licenseMain` exits 0 with no `Missing header` lines.

## On failure
- The error lists the offending files. Run `./gradlew licenseFormat`, then `git diff` to verify the change is just header insertion before committing.

## Note
There is no project-wide checkstyle, spotless, or formatter task. Java compilation itself surfaces the only other "static" feedback.
