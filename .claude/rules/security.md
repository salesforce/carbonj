# Security rules

- Never read or commit secrets. Files matching `**/*.env`, `**/secrets/**`, `**/credentials*`, `**/*.pem`, `**/*.key`, `**/.aws/**` must not be opened, edited, or printed.
- AWS access in tests uses Testcontainers/LocalStack — do not introduce real AWS credentials in test fixtures.
- Do not log raw metric payloads or customer namespace names at INFO; use DEBUG and gate behind a flag.
- Releases run from `master` only (`release.git.requireBranch = 'master'`). Do not push tags or run the release plugin from feature branches.
- The Docker image login uses a GitHub Actions secret (`SALESFORCE_DOCKER_HUB_SECRET`); never echo or hardcode it locally.
