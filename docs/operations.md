# Operations

## Routine checks

Follow the [cost approval gate](costs.md) before AWS operations. Local checks are the no-charge
alternative; they do not verify deployed resources or live API behavior.

Run `make check` for repository health; it accepts the unfinished scaffold. After manual API
preparation and matching plugin/tests, fill root `config.yaml` and run `make validate` for local
readiness. Readiness uses mocked HTTP, fails fast on the named config error, and does not contact
providers/SSM/AWS. The code-only SAM application build installs no pip dependencies; local bootstrap
and tool installation can download packages. Readiness does not verify accounts or credentials.
See [README](../README.md#quick-start) for the required preparation sequence.

```bash
make smoke-test
make logs
```

`make smoke-test` output confirms a direct Lambda invocation completed. It does not read DynamoDB
table data. It still calls the provider API and writes observations when `schedule.enabled: false`;
obtain explicit approval before invoking it. Weather needs no SSM credential.

Weather emits one snapshot per site/UTC `current.time`, containing selected `Decimal` readings as
DynamoDB `N`. Rows contain only configured keys, TTL, and selected fields, not an envelope,
coordinates/site, units, or `ingested_at`. Numeric `ts` is exact whole-second Unix source time.
Every successful observation replaces the whole item, including unchanged polls and revisions.
Last arrival wins; stale retries may overwrite a same-time correction, with no revision history
or guaranteed latest-revision ordering. TTL uses source time and does not extend with each poll.
`current.interval` and poll frequency do not generate observations or fill gaps.
Date-`N`/time-`N` is single-stream per table; same-time different streams collide even with distinct
identities. One provider configuration need not mean one stream. Use an identity-bearing `S`/`B`
partition with time-`N` sort keys or separate tables before combining streams or changing location.
V2 rows are incompatible with old rows/consumers; see [migration.md](migration.md).

## AWS Console checks

1. Open **Lambda**, select the stack's `-fetcher` function, then open **Monitor** to check errors.
2. Open **EventBridge Scheduler**, select the stack's `-schedule` schedule, then confirm its state matches
    `schedule.enabled` in `config.yaml`.
    In **CloudWatch** > **Metrics** > **Scheduler**, check recent invocation attempts and target errors.
3. Open **CloudWatch** > **Log groups**, select `/aws/lambda/<stack-name>-fetcher`, then inspect recent logs.
4. Open **DynamoDB** > **Tables** and select the stack table. Obtain approval before **Explore table
    items** reads or scans; inspect only necessary data and keep it private.

When enabled, investigate Lambda error/throttle alarms and Scheduler DLQ messages separately.
See [failure boundaries](#delivery-and-execution-failures) before considering replay.

## Pause ingestion

After cost approval, set `schedule.enabled: false`, then run `make validate` and `make deploy`.
Deploy reruns readiness and requires manual changeset confirmation.

The scaffold starts paused with an unfinished expression. A completed config may remain paused;
this is not a readiness failure. Disabling scheduling does not stop
direct invocations or remove table capacity charges.

## Resume ingestion

After approval, set `schedule.enabled: true`, run `make validate`, manually deploy and confirm the
changeset, then run a separately approved `make smoke-test`.

Approve deployment, recurring scheduling, and smoke-test charges first. Use the same effective
`STACK_NAME` for deploy, logs, smoke tests, and deletion. Explicit table names do not change with
stack overrides; each additional stack normally provisions another charged table.

## Logs

CloudWatch logs expire after 14 days by default. Increase retention only when operationally justified.

The entrypoint remains `scheduled_ingestor.handler.lambda_handler`. Root `handler.py` delegates to
`core.ingestion.IngestionService.run()` and returns `status: "ok"`, `written`, and `duplicates`.
Its completion log retains `provider`, `observation_count`, `written_count`, and `duplicate_count`;
observation count equals written plus duplicate counts. Failures still propagate for retry handling.
`written`/`written_count` includes inserts, replacements, and unchanged repeats; the built-in
repository leaves `duplicates`/`duplicate_count` at zero on successful runs. These counters do not
measure new source samples or distinguish corrections from unchanged polls.
Optional SSM retrieval/caching belongs to the service instance reused by the handler; weather skips it.
The handler uses stdlib logging with structured `extra` fields; SAM enables native Lambda JSON
formatting. Lambda supports `timestamp`, `level`, `message`, and invocation `requestId` alongside
those fields. Powertools-specific cold-start/function context fields are no longer added by a
decorator; no Powertools environment settings remain. Events, credentials, and raw responses are
not logged. Local tests check log records and safe fields, not Lambda's JSON formatter or request
context; actual CloudWatch output has not been verified by those tests.

The existing `logging.level` setting controls the native application threshold; `WARNING` maps to
`WARN` and `CRITICAL` to `FATAL`. No new resources or permissions are introduced; log volume can
change, so no cost savings are assumed.

## Runtime dependencies

The deployment bundles no third-party pip dependencies. The AWS-managed Python 3.13 Lambda runtime
supplies boto3, botocore, and the SDK dependency urllib3; these are not Python stdlib modules.
`src/requirements.txt` remains comments-only, while the local `dev` extra installs SDK/HTTP libraries
and tools for tests. `uv.lock` controls that local environment, not deployed library versions.

AWS updates managed runtime dependencies, not all libraries an application might use, and does not
guarantee instantaneous security patches. Monitor runtime updates, library advisories, and Python
support dates; review compatibility and security exposure. Migrating off unsupported Python runtimes
remains the operator's responsibility. Managed updates do not remove application maintenance.

This policy trades bundled version control for managed dependencies. Transport uses botocore
connection classes/private session and pool attributes, plus urllib3 APIs requiring 2.x; managed
updates can affect compatibility. Local checks do not prove compatibility with the deployed ARM64
runtime's library versions. After cost approval, verify deployed versions and transport behavior
when investigating updates; keep environment details private. A local dependency upgrade alone
neither patches nor pins the deployed SDK.

`make build` checks exact application module bytes and rejects bundled dependencies or other
non-source files after removing generated `.DS_Store` files. The deploy helper repeats this gate
after its final `sam build`: remove only regular `.DS_Store` files under
`.aws-sam/build/FetcherFunction`, verify exact source bytes and code-only contents, then run
`sam deploy --confirm-changeset`. Build, cleanup, or verification failure stops deployment.
Source files and other functions' build trees are outside this cleanup. The earlier validation
build alone is not the final artifact guarantee. A clean package inventory confirms application
packaging, not the contents or security of the managed runtime; cost approval remains required.

## Delivery and execution failures

The template's SQS DLQ belongs to **EventBridge Scheduler delivery**. Scheduler's target retry
policy and DLQ cover failure to deliver an invocation to Lambda, not an ingestion exception after
Lambda accepts it. The optional DLQ alarm monitors that delivery queue.

**Lambda asynchronous execution** has separate service retry handling. The template configures no
Lambda execution-failure destination or Lambda DLQ; exhausted execution failures are not captured
by the Scheduler queue. Inspect Lambda errors and logs separately. No execution-failure recovery
queue or automatic replay is supplied. Any additional infrastructure needs separate review and
[cost approval](costs.md); see [recovery](recovery.md#failed-invocations-and-replay).

## Transport failures

HTTP retries 429/500/502/503/504 up to three total attempts per invocation; each attempt has a 4-second
wall-clock cap. Retry-After and backoff delays never exceed 1 second or consume the persistence reserve.
Oversized bodies, including error bodies, fail without retry or unbounded draining. Malformed JSON
errors contain no raw body. SSM/DynamoDB have 1/2-second connect/read ceilings, no automatic retries,
and 3-second operation caps. Exceptions propagate as Lambda execution failures, not Scheduler
delivery failures; see [delivery and execution failures](#delivery-and-execution-failures).

Lambda remaining time controls a fresh deadline, retaining 0.5 seconds for shutdown and 4 seconds
during provider work for persistence. An uncached credential also needs a 3-second SSM allowance;
each write must fit before starting. Budget exhaustion can leave earlier writes committed;
retrying replaces matching whole items again and may overwrite intervening same-time corrections.
See [runtime budgets](architecture.md#runtime-budgets).
DNS wait exhaustion discards its result; the single shared resolver slot rejects new lookups until
the native call exits. A permanently stalled lookup keeps that execution environment unavailable for
new connections without growing workers or starting late HTTP/writes. Enclosing timer exhaustion is
not retryable within the invocation; unowned active watchdogs are rejected without alteration.
These bounds limit duration/retry exposure, not AWS billing guarantees; no resources, timeout increase,
dependencies, IAM, or log retention change. Numeric key values and replacement semantics are
described in [migration.md](migration.md). Follow the [cost gate](costs.md).

## Delete

Separately approved `make delete` requires typing the effective stack name. Table and log deletion/replacement policies
are `Delete`; deletion is destructive. Arrange any required recovery outside the stack first.
