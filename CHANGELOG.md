# Changelog

Curated notes for the planned first public release. The package version is `0.1.0`;
no release date, published tag, or completed publication is asserted.

## Unreleased - 0.1.0 first public release candidate

### Included

- Credential-free Open-Meteo current weather adapter with explicitly selected numeric readings,
  UTC source timestamps, sanitized fixtures, and unit/contract/malformed-response tests.
- Provider-neutral scheduled HTTPS ingestion on Python 3.13 ARM64, with bounded transport,
  DynamoDB snapshots, and one provider configuration per stack.

### Compatibility and storage

- Snapshot contract v2 stores only configured keys, TTL, and selected native scalar fields.
  Old envelope rows and consumers are incompatible; no migration, backfill, or rekeying runs.
  See [migration guidance](docs/migration.md).
- Numeric sort keys encode exact whole-second source time; fractional source times are rejected.
  Date-`N`/time-`N` supports one stream per table; identity-bearing `S`/`B` partitions separate streams.
- TTL derives from source time, not polling/replay time. Every successful observation writes
  unconditionally, replacing the whole snapshot even when unchanged. Last arrival wins; stale
  retries can overwrite same-time corrections. No revision history or exactly-once guarantee.

### Packaging and safeguards

- Code-only ARM64 packaging bundles no pip dependencies. AWS-managed Lambda supplies boto3,
  botocore, and urllib3; the local lockfile does not pin deployed versions. Compatibility and
  security maintenance remain necessary. Logging uses stdlib with native Lambda JSON formatting.
- The deploy helper cleans only generated regular `.DS_Store` files in its function build tree,
  then verifies exact source bytes and code-only contents after the final SAM build and before
  deployment/manual changeset confirmation. Failed build, cleanup, or verification blocks deploy.
- Pytest enables `--strict-markers` and `--strict-config` while retaining 100% application coverage.
- Two exact initial-commit scanner exceptions cover verified mocked SSM boolean-keyword false
  positives, not credentials; no rule/directory suppression or secret incident is claimed.
  See [scanning policy](SECURITY.md#secret-scanning).
- Operations/recovery distinguish Scheduler delivery DLQ handling from Lambda execution failures;
  no Lambda execution destination/DLQ, automatic replay, or data-history recovery is added.

Final candidate/ref/metadata review, independent gates, dependency security review, future hosted
CI/private reporting setup, and explicit human publication approval remain required; see the
[release preparation plan](RELEASE_PREPARATION_PLAN.md).
