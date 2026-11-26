# M9 integration and release handoff

The M1–M9 implementation is in the `0.1.0-rc.1` source tree. [ROADMAP.md](ROADMAP.md) maps each deliverable to its acceptance script, and [ACCEPTANCE.md](ACCEPTANCE.md) describes the exact gate and evidence boundaries.

## Release ownership and provenance

The integration branch is validated before it is advanced to `master`. The `V1 CI` workflow builds one tested JAR, runs all required integration jobs, verifies the Docker distribution and validates packaging. Its final gate requires every dependency to succeed; skipped or cancelled gates cannot count as a release pass.

`Publish release candidate` runs only after a successful own-repository push to `master`. It checks the workflow identity and exact tested commit, refuses an outdated master run, downloads the tested binary and measurement report from that exact run, and validates their build metadata and hashes. It never substitutes a fresh untested JAR. Existing tags/releases are preserved. The version comes from `VERSION`; the release is explicitly marked prerelease.

Each release includes executable JAR, exact Git source archive, SHA-256 checksums, pinned-plugin CycloneDX SBOM, actual nested runtime dependency hashes, frontend asset hashes, build/run provenance and measured recovery workload output. The release notes receive the exact validated SHA and CI URL at publication. Those artifacts are the final evidence, rather than a mutable hand-written success count in this file.

## Operational handoff

Use [RUNBOOK.md](RUNBOOK.md) for setup, permissions, backup, resource limits, repair and generation replacement. The UI and CLI use the same provider implementations and durable runtime. Preserve each task's whole state directory and the deployment key consistently; do not edit/delete offsets or journals to bypass a failed preflight.

The release protocol fingerprint intentionally distinguishes this candidate from earlier preview state. Existing preview users must reconcile data and create a reviewed new generation; there is no automatic checkpoint migration. Source transactions are not an end-to-end transaction across sinks. Replay, bounded lifetime receipt retention and the strict per-provider type/topology limitations remain part of the delivered contract.

## Diagnosing a failed gate

Read the failed job and the matching script first. A blocked fixture may expose a real consistency guard, an unsupported configuration or a test setup race; preserve that distinction. Fix the owning provider/runtime or fixture, rerun the relevant case, and then require a complete V1 CI run for the resulting source commit. Do not publish by disabling the failing gate or by using an earlier successful artifact with different source.
