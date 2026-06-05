---
name: run
description: Build the bootJar and start CarbonJ locally on the default `dev` profile.
---

# /run — Start CarbonJ locally

## 1. Build the runnable jar
```bash
./gradlew :carbonj.service:bootJar
```
Output: `carbonj.service/build/libs/carbonj.service-<version>.jar`

## 2. Run
```bash
java --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED \
     -jar carbonj.service/build/libs/carbonj.service-*.jar
```

Defaults: HTTP on `:2001`, Spring profile `dev`, store disabled.

## Override profile / config
```bash
java -Dspring.profiles.active=dev \
     --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED \
     -jar carbonj.service/build/libs/carbonj.service-*.jar
```

Runtime config is at `carbonj.service/src/main/resources/application.yml` (profiles: `dev`, `test`, `prod`). For container-style overrides see `carbonj.service/src/main/docker/files/service.args`.

## Success
- Banner prints, log shows `Started CarbonJServiceMain in N seconds`.
- `curl http://localhost:2001/_dw/healthcheck` returns 200 (or your project's healthcheck path).

## On failure
- `Address already in use :2001` → stop the prior instance or change the port.
- `ClassNotFoundException` → re-run `./gradlew clean :carbonj.service:bootJar`.
- Kinesis / store errors on `dev` profile → confirm the profile actually disables those (look at `application.yml`); start with `-Dspring.profiles.active=dev` explicitly.
