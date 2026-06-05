---
name: review
description: Self-review checklist for a CarbonJ change before opening or merging a PR. Aligns with what CI enforces.
---

# /review — CarbonJ self-review checklist

Run through this before pushing or requesting review.

## 1. Build & tests pass
```bash
./gradlew build printCoverageReport
```
This mirrors CI (`.github/workflows/gradle.yml`). If this is green, CI will be green.

For a faster loop while iterating:
```bash
./gradlew :carbonj.service:test
```

## 2. License headers
- Any new `.java` file under either module?
- Any new file under `carbonj.service/src/main/docker/files/`?

If yes:
```bash
./gradlew licenseFormat && ./gradlew licenseMain
```

## 3. Storage compatibility
- Did you touch anything in `db/`?
  - dbName values (`60s24h`, `5m7d`, `30m2y`) **unchanged**?
  - RocksDB column-family names unchanged?
  - Any on-disk format change → flag in PR description; needs a migration plan.

## 4. Configuration surface
- New tunable? Added to the matching `cfg*` class **and** documented in `application.yml` (with a default that matches existing behavior)?
- Touched `service.args`, `relay-rules.conf`, `blacklist.conf`, or other files in `src/main/docker/files/`? Confirm Dockerfile still picks them up.

## 5. Public APIs
- HTTP endpoint signatures under `/_dw/rest/carbonj/...` or Graphite servlets unchanged? If changed, update `admin-api.md` / `delete-api.md`.

## 6. Ops scripts
- Did the Java change rename a metric, env var, or path that a script in `src/main/docker/files/*.py` / `*.sh` reads? Update the script in the same PR.

## 7. Commit hygiene
- Branch is **not** `master` (releases run from `master` only).
- Commits compile individually if you bisected.
- PR description lists: behavior change, risk area, rollout/rollback notes if it touches storage or ingestion.

## 8. Final
```bash
git status
git diff --stat origin/master...HEAD
```
Nothing unintended staged.

## Success
- CI is green.
- All checklist items above either apply (and were addressed) or are explicitly N/A.
