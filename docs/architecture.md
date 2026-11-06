# Architecture

## Runtime flow

1. EventBridge Scheduler invokes Lambda.
2. Root `handler.py` composes settings, HTTP client, provider, repository, and `IngestionService`.
3. The service retrieves and caches an optional SecureString from SSM Parameter Store.
4. The service calls the provider's `fetch()` with the HTTP client and optional credential.
5. The provider's `normalize()` validates the response and returns observations.
6. The service passes observations to the repository for validation and unconditional DynamoDB writes.
7. Equal physical keys replace the whole snapshot, including unchanged repeats.
8. The handler logs counts and returns the service result.

Settings and the required runtime `TableSchema` come from environment variables. Dependencies are
composed once at module import and reused across invocations. The entrypoint remains
`scheduled_ingestor.handler.lambda_handler`; its result is `status: "ok"`, `written`, and `duplicates`.
Logging retains `provider`, `observation_count`, `written_count`, and `duplicate_count` without
logging scheduler events, credentials, or raw responses. Exceptions propagate for retry handling.
`written` counts inserts, replacements, and unchanged repeats; the built-in repository's
`duplicates` remains zero on successful runs.

The handler uses standard-library `logging.getLogger()` with structured `extra` fields, not
Powertools. SAM configures native Lambda JSON application logging and its level; Lambda supplies
standard metadata and request context, including `requestId`. No custom formatter or context
decorator is installed. This describes the configured runtime behavior, not a measured AWS result;
see [logging details](operations.md#logs).

The current explicit registry allowlist contains only credential-free Open-Meteo weather.
It requests only configured current measurements, using UTC source time and stable site identity
for normalization. Equal physical keys use last-successful-write/arrival replacement, not latest-revision
ordering: a delayed stale retry may overwrite a same-time correction. No revision history is retained.
Delivery and processing remain at-least-once, not exactly-once.

Snapshot row contract v2 has only configured keys, TTL, and selected native scalar fields.
`TableSchema` enforces every selected field and its type before writes. It enforces the application
contract before writes; DynamoDB enforces its key schema, not non-key field types.
The runtime schema is required, with no fallback. Weather values are `Decimal` serialized as DynamoDB
`N`. Providers return aware `Observation.observed_at`; the repository encodes keys and source-based
TTL. A numeric sort key is exact whole-second Unix source time; fractional source times are rejected.
Numeric date partitions contain no stream identity: date-`N`/time-`N` is single-stream per table,
and same-time different streams collide. Identity-bearing `S`/`B` partitions separate streams with
time-`N` sort keys. One provider configuration need not represent one stream.
See [storage encoding](configuration.md#storage).

## Core and plugin boundary

| Ownership | Files / responsibility | Expected developer work |
| --- | --- | --- |
| Core | `core/__init__.py`, `core/provider.py`: package and `BaseProvider` ABC | Normally unchanged |
| Core | `core/config.py`, `core/http.py`: runtime settings and bounded HTTPS | Normally unchanged |
| Core | `core/models.py`, `core/storage.py`, `core/repository.py`: snapshots, validation, whole-item replacement | Normally unchanged |
| Core | `core/ingestion.py`: fetch/normalize/persist service and optional credential cache | Normally unchanged |
| Composition | Package-root `handler.py`, `registry.py`: Lambda wiring and explicit provider factories | Register a new factory; handler normally unchanged |
| Deployment | `scripts/config.py`, `template.yaml`, `Makefile`: config rendering and deployment | Normally unchanged |
| Plugin | `providers/__init__.py`, `providers/<name>_provider.py` | Subclass `BaseProvider`; implement API fetch/normalize |
| User choices | Root `config.yaml` | Fill identity, endpoint/options, selected mappings, schedule, keys/TTL |
| Plugin evidence | Sanitized fixtures, provider unit/contract tests | Match response shape, units, time, errors |

Runtime paths in this table are relative to `src/scheduled_ingestor/`. The included concrete adapter
is `providers/open_meteo_provider.py`; there are no legacy root-module forwarding shims.

`BaseProvider` declares abstract `fetch()` and `normalize()` methods. `OpenMeteoProvider` inherits it
explicitly. Abstract methods prevent instantiating incomplete subclasses; they do not validate
signatures, API behavior, response shape, or normalized values. Runtime tests establish those contracts.

`IngestionService(provider, http, repository, credential_parameter_name=None, ssm_client=None)` takes
injected dependencies. `run(remaining_time=None)` orchestrates ingestion; it caches credentials per instance
only when configured. Core imports neither the registry nor concrete providers. The package-root
`PROVIDER_FACTORIES` allowlist maps names to factories taking `(base_url, options)` and returning
`BaseProvider` instances. Adding a module alone does not register it.

Preparing a different API normally changes only its plugin, root registry/config, and sanitized
tests/docs, not reusable core or infrastructure. Configuration is not a generic field interpreter.

## Runtime budgets

The handler passes Lambda's remaining-milliseconds callback to each service run. A fresh monotonic
deadline withholds 0.5 seconds for shutdown and 4 seconds from provider fetch/normalize for persistence.
An uncached SSM read requires another 3 seconds before it starts; cached credentials skip that reserve.
Each DynamoDB write requires 3 seconds available, checked again between observations. Partial writes
can remain committed after exhaustion; retries replace matching rows, including unchanged snapshots
or a same-time correction that arrived in between.

HTTP allows at most three attempts across one invocation, each capped at 4 seconds including response
headers/body, with connect/read ceilings of 2/3 seconds. All status bodies are bounded to 1,000,001
bytes read; bodies above 1,000,000 bytes fail before retry. urllib3 automatic retries/drains are disabled.
Transient statuses remain retryable; backoff and Retry-After delays are capped at 1 second and admitted
only within the provider allowance. SDK clients use connect/read ceilings of 1/2 seconds and one total
attempt (no automatic retry), with a 3-second wall-clock cap around SSM and each DynamoDB write.

POSIX real-time timers on the Lambda main thread enforce wall-clock allowances through synchronous
transport reads, including trickling bodies; socket timeouts alone are not whole-stream deadlines.
Only an innermost HTTP operation cap is retryable. Invocation/enclosing expiry remains failed even
if an inner caller catches it; SDK transport exception wrapping cannot convert it into a fresh retry.
Owned nested timers restore the remaining enclosing deadline on success or error. Unowned active
timers, including periodic timers, are rejected before their timer or handler is changed. Inactive
handlers are restored, and invocation state is cleared on success or failure.

Native DNS is isolated from the caller's alarm dispatch: all provider and composed SDK direct/proxy
pools use one process-shared DNS-only worker slot. A lookup waits only for the active operation's
remaining allowance. There is no queue or replacement worker while a previous lookup is alive;
later callers fail immediately. The daemon worker exits when native resolution returns, and another
lookup may then use the slot. If native DNS never returns, the slot stays unavailable until the
execution environment ends; repeated warm invocations cannot accumulate threads or queued work.
An abandoned result is discarded, and only the main thread may connect, negotiate TLS, request HTTP,
or write observations, after checking the remaining deadline. The already-running native DNS query
cannot be canceled and may continue DNS traffic; no HTTP/SDK work is delegated or continued by it.
Hostname/Host/SNI, TLS verification, resolver address order, IPv6 scope IDs, source binding and SDK
proxy routing remain unchanged; no process-wide socket monkeypatch is used. Wildcard and numeric
source tuples bind directly, preserving IPv6 flowinfo and scope IDs; named source hosts use the same
resolver slot and deadline, restricted to the destination address family. Existing in-flight SDK
writes may have committed before a timeout; a retry can replace matching rows again.
Standalone service runs/HTTP requests use a fresh 20-second allowance; protected work requires the main
thread. No config knobs, dependencies, or infrastructure are added.

## Deployment boundary

One stack represents one provider configuration. Deploy another stack for another provider, schedule, Region, or lifecycle. This avoids a complex multi-provider orchestrator and keeps IAM and failures isolated.

Each additional stack duplicates provisioned table capacity charges, even with scheduling disabled.
Default table names derive from the effective stack name, including `STACK_NAME`; explicit table
names disable that isolation and emit a warning. Follow the [cost approval gate](costs.md).

## Resource ownership

SAM/CloudFormation owns application resources. SSM secret values remain external so stack events and repository files never hold credentials.

`scripts/config.py` reads sole user config `config.yaml`; required unfinished values fail readiness.
Root owns deployment/request/mapping choices; runtime schema derives once from keys, TTL, and selections.
Registry entries remain explicit. Generic optional SSM support remains available for future providers.

SAM packages only application Python source and a comments-only `src/requirements.txt`, with no
bundled pip dependencies. The package gate checks exact source bytes and rejects extra files,
including dependency distributions. `make build` removes generated `.DS_Store` files first.
The managed Python 3.13 Lambda runtime supplies boto3, botocore, and the SDK's urllib3 dependency;
all three are third-party libraries, not stdlib. Logging uses stdlib plus Lambda's native formatter.

`pyproject.toml` has empty application dependencies; its `dev` extra and `uv.lock` supply and lock
local development dependencies only. Managed runtime library versions can change independently;
they are neither pinned by the source lock nor fully reproducible from an application revision.
This intentionally gives up bundled version control; security and compatibility duties remain.
See [runtime maintenance](operations.md#runtime-dependencies).

`make check` verifies scaffold health without completed root config. `make validate` checks root
readiness locally, then SAM/cfn-lint/build/package readiness. Neither calls providers or AWS.
The SAM application build installs no pip dependencies; local bootstrap/tool installation can
download packages. Deployment is manual, cost-approved, and confirms a changeset.

V2 rows omit the old envelope and are incompatible with old rows/consumers. Prefer a fresh independently
named stack/table under a separately approved transition; this change performs or authorizes no
data migration, backfill, rekeying, or infrastructure replacement.

Table deletion and replacement policies remain `Delete`; key/table changes can destroy data.
`make delete` requires typing the stack name. Plan recovery outside the stack before destructive
changes; see [migration.md](migration.md).

## Decisions

- SAM: concise serverless CloudFormation and local validation.
- YAML: readable single application config with comments.
- ARM64: low-cost default; adapters must use compatible dependencies.
- DynamoDB provisioned capacity: configuration explicitly sets read and write capacity units.
- Explicit log group: deterministic 14-day default retention.
- No VPC: public API access without NAT Gateway cost.
- Native metrics: avoid custom-metric cost.
