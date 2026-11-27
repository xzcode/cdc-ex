#!/usr/bin/env bash
# CI-only Maven transport fallback. Never suppress a build or test failure.
set -euo pipefail
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
set +e
mvn "$@" 2>&1 | tee "$work/central.log"
rc=${PIPESTATUS[0]}
set -e
if [[ "$rc" == 0 ]]; then exit 0; fi
# Retry exactly once, only when Central explicitly rate-limited an artifact transfer.
if ! grep -E 'Could not transfer (artifact|metadata).*https://repo\.maven\.apache\.org/maven2.*(429|Too Many Requests)' "$work/central.log" >/dev/null; then
  exit "$rc"
fi
echo 'Maven Central returned HTTP 429; retrying the identical build against its alternate HTTPS endpoint.'
cat >"$work/settings.xml" <<'XML'
<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0">
  <mirrors>
    <mirror>
      <id>central-alternate</id>
      <mirrorOf>central</mirrorOf>
      <url>https://repo1.maven.org/maven2</url>
    </mirror>
  </mirrors>
</settings>
XML
# Avoid retrying against a caller-supplied settings file; this script is for
# public-dependency GitHub CI and never replaces private mirror configuration.
for argument in "$@"; do
  case "$argument" in -s|--settings|--settings=*|-s=*) exit "$rc";; esac
done
mvn -s "$work/settings.xml" -C "$@"
