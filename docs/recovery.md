# Recovery

Table and log deletion/replacement policies are `Delete`. Recreating infrastructure does not restore
observations; arrange data recovery outside the stack before destructive changes. `make delete`
requires typing the stack name.

1. Restore one known-good revision containing matching core, handler, root registry, explicit
    `BaseProvider` adapter, and sanitized tests; do not mix legacy modules with the current package.
2. Restore local deployment choices in root `config.yaml`; keep `schedule.enabled: false`.
3. Run `make check` for code health and `make validate` for completed root readiness before AWS work.
4. Obtain approval under the [cost gate](costs.md) for deployment and any data recovery.
5. For future authenticated providers only, restore the external SSM SecureString separately.
6. Manually deploy using the intended effective stack/table name; inspect and confirm the changeset.
7. Import historical data only through a reviewed, separately approved recovery workflow.
8. Run an approved `make smoke-test`, then enable scheduling only after validation and approval.

Weather requires no credential restoration. Root `config.yaml` is the sole deployment input; required
identity, request, selection, key, and TTL choices have no fallback. A `null` table name derives from
the effective stack name, including `STACK_NAME`; an explicit name disables isolation and warns.
Sanitized `tests/fixtures/config.yaml` is test evidence, not configuration to deploy or edit.

Use the [current package ownership table](architecture.md#core-and-plugin-boundary) when restoring
source. Core holds `IngestionService` and the `BaseProvider` contract; package-root `registry.py`
selects factories and `handler.py` composes runtime dependencies. Rebuild from that revision rather
than restoring stale build artifacts. The Lambda entrypoint and infrastructure remain unchanged.

Rebuilding restores application source, not historical AWS-managed boto3, botocore, or urllib3
versions. The comments-only SAM manifest bundles none of these libraries; `uv.lock` constrains
local development only. A known-good source revision therefore does not guarantee identical runtime
behavior. Review managed runtime changes and [dependency compatibility](operations.md#runtime-dependencies)
before an approved live verification; unsupported Python runtimes still require operator migration.

The sanitized root config is tracked. Private backups are not automatically loaded.
Restore only needed values locally; do not commit
private coordinates, secrets, or exports. CloudWatch logs are temporary, not a recovery source.

Check accepts an unfinished scaffold; validate fails fast on missing choices and uses no live
provider/SSM/AWS calls. The code-only SAM application build installs no pip dependencies; local
bootstrap and tool installation can download packages. Disabled scheduling is valid.
Neither verifies account access, endpoint availability, or credentials.

Snapshot v2 rows contain only keys/TTL/selected native fields and are incompatible with old
envelope rows/consumers. Same-key recovery/import writes replace the whole item, even if unchanged;
a delayed stale replay can overwrite a same-time correction. There is no revision history or
guaranteed latest-revision ordering. TTL remains source-based, not recovery/poll time.

Numeric sort keys require whole-second source time; fractional source times are rejected, with no
implicit system-time fallback. Date-`N`/time-`N` supports only one stream per table; distinct same-time
streams overwrite. An identity-bearing `S`/`B` partition
separates streams with time-`N` sort keys; one provider configuration alone does not ensure isolation.
Preserve existing data, including smoke-test records. Imports require a separately approved recovery
workflow; key/table changes require a [separately approved transition](migration.md#separately-approved-transition).
Recovery does not authorize deletion; cloud recovery requires the cost gate.

## Failed invocations and replay

The Scheduler SQS DLQ captures exhausted **delivery** failures. It does not capture ingestion errors
after Lambda accepts an asynchronous invocation. Lambda execution retries are separate; the current
template has no Lambda execution-failure destination or Lambda DLQ. Inspect Lambda errors/logs
separately rather than assuming failed work is present in the Scheduler queue.

No automatic replay or data-history recovery is implemented. Before any manual replay, validate
the provider/configuration, inspect available failure evidence privately, and obtain explicit
[cost approval](costs.md) for API calls, invocations, and writes. Replaying a scheduled event fetches
the provider again; it does not reconstruct the original response or missing source samples.
Previously committed writes may remain. Same-key writes replace whole snapshots unconditionally;
a delayed stale retry/recovery write may overwrite a newer same-time correction. Preserve source
timestamps and source-based TTL; replay provides neither revision history nor latest-revision
ordering. Do not add queues, destinations, or replay automation as an implicit recovery step.
