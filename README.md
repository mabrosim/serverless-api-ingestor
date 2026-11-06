# Serverless API Ingestor

## Purpose

Near-zero-cost, serverless scheduled HTTPS API time-series collector and DynamoDB snapshot store on
AWS (EventBridge Scheduler + ARM64 Python 3.13 Lambda + DynamoDB with TTL, managed via AWS SAM and
a single `config.yaml`). It calls an external HTTPS API on a schedule, parses its response through
a provider adapter, normalizes observations, and writes validated snapshots to DynamoDB.

Open-Meteo current weather is the credential-free primary example (`open_meteo` is the only built-in
adapter today). Infrastructure is provider-neutral.

**Pipeline boundary:** This repository handles reliable, near-zero-cost **scheduled data fetching,
validation, and raw time-series snapshot storage** in DynamoDB. Downstream analytics, indicator
calculation (such as RSI or window aggregations), alerting, dashboards, and ML predictions are
intentional next-step consumers outside this repository.

**Example adapter ideas** (not built-in adapters; polling may miss transitions or exact timing):

- **Home brewing & cellar monitoring:** room/cellar temperature, humidity, and beer-keg status over
  time.
- **Weather & home energy awareness:** outdoor/indoor temperature and dynamic electricity/spot price
  tracking.
- **Crypto & stock market snapshots for signal processing:** cryptocurrency rates and stock-market
  quotes (for example, building a clean 30-minute or weekly price series to calculate RSI, moving
  averages, or critical price-change alerts in a separate consumer).
- **Smart-home & IoT API state snapshots:** API-reported smart-door/sensor state and timestamps,
  air quality, river/tide levels, or solar-generation APIs.

Built for both human and LLM-agent developers: a human developer studies and tries the API,
chooses stored fields and the DynamoDB schema in the sole `config.yaml`, and reviews diffs and AWS
costs. Because a custom `BaseProvider` subclass (`fetch()` and `normalize()`) is small and bounded,
developers with limited Python experience can also pair with an AI coding assistant (such as GitHub
Copilot in VS Code or Claude) to generate the adapter, sanitized fixtures, and tests while offline
`make check` and `make validate` gates verify the contract locally.

Near-zero cost for small workloads drives the design; it is not a guarantee of free API or AWS use.

## Architecture

```text
EventBridge Scheduler -> Lambda -> HTTPS API
                              -> provider adapter
                              -> DynamoDB
```

Supporting resources:

- EventBridge Scheduler schedule and least-privilege execution role
- Python Lambda on ARM64
- DynamoDB provisioned-capacity table with enabled TTL
- Optional SSM Parameter Store reference for future providers needing credentials
- CloudWatch log group with 14-day retention
- SQS dead-letter queue
- Optional CloudWatch alarms

## Notes

- Near-zero cost for small personal workloads
- One provider configuration per deployed stack
- No VPC, NAT Gateway, API Gateway, or always-running resources
- No secrets, account IDs, ARNs, or personal AWS metadata in Git
- Deterministic DynamoDB keys with whole-snapshot last-arrival-wins replacement
- One application configuration file: `config.yaml`
- SAM/CloudFormation owns all AWS resources

## Repository guide

Normally edit configuration; for a new API, also edit its provider, registry, and corresponding tests.
Reusable core and infrastructure usually stay unchanged, but are not immutable; feature or
architecture changes need maintainer review.

| Object | Purpose | When to edit |
| --- | --- | --- |
| [config.yaml](config.yaml) | Sole user-edited app/deployment settings; source of selected non-key field types and runtime storage schema. | Required: fill scaffold for API, selected fields, storage, schedule, and deployment. Do not manually edit core schema for these choices. |
| [open_meteo_provider.py](src/scheduled_ingestor/providers/open_meteo_provider.py) | Included API adapter. | Adapt fetch/normalize logic; a new API contract requires a matching `BaseProvider` subclass in [providers/](src/scheduled_ingestor/providers/). |
| [registry.py](src/scheduled_ingestor/registry.py) | Explicit provider factory allowlist. | Only when registering providers. |
| [tests/unit/providers/](tests/unit/providers/), [tests/contract/](tests/contract/), [tests/fixtures/](tests/fixtures/) | Provider behavior, contract/regression checks, sanitized responses. | Update with adapters, including malformed-response cases. |
| [core/provider.py](src/scheduled_ingestor/core/provider.py) | Abstract `fetch()`/`normalize()` contract. | Normally unchanged; reviewed contract changes only. |
| [core/config.py](src/scheduled_ingestor/core/config.py) | Runtime Lambda environment reader, not user YAML configuration. | Normally unchanged. |
| [core/](src/scheduled_ingestor/core/) (`http.py`, `models.py`, `storage.py`, `repository.py`, `ingestion.py`) | Bounded HTTPS transport, native snapshots, schema validation, whole-item replacement, orchestration. | Normally unchanged; reviewed core maintenance/features require matching tests. |
| [handler.py](src/scheduled_ingestor/handler.py) | Thin Lambda composition entrypoint. | Normally unchanged. |
| [scripts/config.py](scripts/config.py) | Root config validation, SAM parameters, deployment helper. | Normally unchanged. |
| [template.yaml](template.yaml) | Lambda, DynamoDB, Scheduler, IAM, logs, and DLQ infrastructure. | Only intentional infrastructure changes, with explicit [cost approval](docs/costs.md#contributor-approval-policy). Settings belong in root config. |
| [Makefile](Makefile) | Local validation/build/deployment commands. | Normally unchanged. |
| [pyproject.toml](pyproject.toml), [uv.lock](uv.lock) | Local package metadata, `dev` dependencies, and local dependency lock; not deployed runtime versions. | Deliberate, reviewed packaging/dependency changes only. |
| [src/requirements.txt](src/requirements.txt) | Comments-only SAM manifest; no bundled pip dependencies. | Keep comments-only; changing the code-only dependency policy requires review. |
| [.python-version](.python-version) | Local Python runtime pin. | Normally unchanged. |
| [README.md](README.md), [docs/](docs/) | Human usage, design, and operations guidance. | Update when usage or behavior changes. |
| [.github/workflows/](.github/workflows/) | CI validation and security checks. | Normally unchanged. |
| [CONTRIBUTING.md](CONTRIBUTING.md), [SECURITY.md](SECURITY.md), [LICENSE](LICENSE) | Contribution, security, and license reference. | Normally unchanged; review policies and license obligations before reuse. |
| [.gitignore](.gitignore) | Excludes private settings and generated artifacts. | Extend only for new private/generated artifacts. |

Generated/local artifacts (`.venv/`, `.aws-sam/`, `build/`, `dist/`, caches, and `*.egg-info/`)
are not source: do not edit or commit them. Bootstrap, build, test, and packaging commands
regenerate them.

## Requirements

Install:

- uv via [Homebrew](https://brew.sh/):

  Install uv; `make bootstrap` below installs project dependencies:

  ```bash
  brew install uv
  ```

- AWS SAM CLI: [official installation guide](https://docs.aws.amazon.com/serverless-application-model/latest/developerguide/install-sam-cli.html)
- AWS CLI: [official installation guide](https://docs.aws.amazon.com/cli/latest/userguide/getting-started-install.html)

uv manages local Python 3.13; AWS CLI and SAM CLI remain separately installed tools.
The SAM CLI is required for `make validate`, `make build`, and `make check`.

AWS credentials are not required for bootstrap, checks, or readiness validation.
AWS CLI profile setup is optional early preparation, not deployment approval; never commit
profiles/output. Deployment requires explicit cost approval.

## Recommended deployment workflow

Use one working clone and one root `config.yaml` per independently managed stack.
This is a recommendation, not a technical requirement; no separate hosted Git repository is needed.

1. Clone this repository into a descriptive folder, such as `weather-ingestor`, and enter it.
  The folder name does not name the AWS stack.
2. Prepare the adapter and root [config.yaml](config.yaml) for the intended API.
  A new API needs adapter implementation, registration, and sanitized fixtures/tests, not just
  config changes; follow [Provider adapters](docs/adapters.md).
3. Choose a distinct `application.stack_name` for each independently managed stack in the target
  AWS account/Region; account and Region also identify the deployment target.
4. Leave `storage.table_name: null` for stack-derived table isolation.
  An explicit table name disables that isolation.
5. Follow [Quick start](#quick-start) below for validation, explicit cost approval, and deployment.

For another independently managed ingestor, use another working clone and repeat these steps.
For updates, reuse the existing stack name, account, and Region rather than creating a new stack.
Each stack adds resources and costs, including provisioned table capacity even with scheduling off.

Root `config.yaml` is tracked: review its Git diffs and do not commit private deployment settings.
Reusable adapter changes and tests can be committed without private settings.
Apply upstream fixes to each working clone.

## Quick start

### 1. Clone, read, bootstrap, check

Clone this repository and enter its root directory. Read this README before running commands.

```bash
make bootstrap
make check
```

Bootstrap runs `uv sync --locked --extra dev --python 3.13`, downloading Python 3.13 if needed,
creating a uv-managed `.venv`, and installing the project in editable mode with the `dev` extra.
Make commands use `.venv` directly; shell activation is not required.

The committed `.python-version` selects Python 3.13; `uv.lock` records resolved local dependencies.
Locked sync refuses an outdated lockfile rather than updating it. Dependency upgrades are deliberate;
see [CONTRIBUTING.md](CONTRIBUTING.md). The build backend remains setuptools.

SAM packages application source with a comments-only `src/requirements.txt`; it installs no
third-party pip dependencies. `pyproject.toml` has empty application dependencies; its `dev` extra
supplies local tools plus boto3, botocore, and urllib3 for development/tests.

Deployed code relies on AWS-managed Python 3.13 Lambda libraries: boto3, botocore, and the SDK's
urllib3 dependency. These are third-party runtime-provided libraries, not Python's standard library.
AWS updates managed runtime dependencies, not every library you might use; patch timing is not
guaranteed. This trades bundled version control for managed updates, while compatibility/security
monitoring and migration off unsupported Python runtimes remain your responsibility. The local
`uv.lock` does not pin deployed library versions or guarantee runtime reproducibility.
See [runtime maintenance](docs/operations.md#runtime-dependencies).

`make check` verifies repository health with sanitized fixtures; the unfinished root scaffold is
valid for this step. It already runs lint/tests, `sam validate --lint --template-file template.yaml`,
cfn-lint, build, and package checks. Ignore generic SAM post-build suggestions and continue these
numbered README steps.

**Optional early AWS setup:** local `make check` and `make validate` need no AWS credentials.
Start by listing profiles locally:

```bash
aws configure list-profiles
```

Reuse an existing suitable profile; replace `example` with its name throughout. Configure a new
profile only if needed. For a profile supporting browser-based sign-in:

```bash
aws login --profile example
```

Only for IAM Identity Center profiles, use `aws sso login --profile example` instead; run
`aws configure sso --profile example` only if that profile still needs configuration.

Optionally lint the template with the selected profile and Region:

```bash
sam validate --profile example --region eu-north-1 --lint --template-file template.yaml
```

Replace `eu-north-1` with your intended Region. This is template lint only, not authentication,
target-account verification, or application readiness validation. Early setup grants no approval
for deployment or chargeable AWS testing; continue with step 2.

### 2. Prepare the API manually (required)

Read the [Open-Meteo API documentation](https://open-meteo.com/en/docs), terms, and rate limits before
any live request. Manually send a GET, then inspect its response before configuring ingestion.
This synthetic request represents `example-site` at latitude/longitude `0.0`; site identity is local,
not an API parameter. It explicitly selects three current fields and UTC:

```bash
curl --fail-with-body --get 'https://api.open-meteo.com/v1/forecast' \
  --data-urlencode 'latitude=0.0' \
  --data-urlencode 'longitude=0.0' \
  --data-urlencode 'current=temperature_2m,apparent_temperature,relative_humidity_2m' \
  --data-urlencode 'timezone=UTC'
```

Compact synthetic response excerpt, not output from a live request:

```json
{
  "timezone": "GMT", "utc_offset_seconds": 0,
  "current_units": {
    "time": "iso8601", "temperature_2m": "\u00b0C",
    "apparent_temperature": "\u00b0C", "relative_humidity_2m": "%"
  },
  "current": {
    "time": "2026-01-01T00:00", "temperature_2m": 12.5,
    "apparent_temperature": 11.0, "relative_humidity_2m": 60
  }
}
```

Inspect shape, units, UTC source timestamp, missing/null readings, and error responses.
Decide which fields to store. Review the included weather plugin; for a new/different API, subclass
`BaseProvider`, implement matching `fetch()`/`normalize()`, and register its factory in the package-root
registry. Save sanitized response fixtures and unit/contract/malformed-response tests.
See [docs/adapters.md](docs/adapters.md). Once you capture a sanitized sample JSON response from
your manual `curl` check, you can hand that response and [docs/adapters.md](docs/adapters.md) to an
AI assistant to scaffold the `BaseProvider` subclass, registry entry, and unit/contract tests.
Neither `make check` nor `make validate` runs this GET. Configuration is not a generic JSON field
interpreter: the prepared artifact is matching plugin/tests **and** root `config.yaml`.

### 3. Fill root configuration

Merge these fields into existing sections in `config.yaml`, preserving other settings;
do not add duplicate mappings such as a second `storage` section:

```yaml
application:
  stack_name: open-meteo
  environment: dev
provider:
  name: open_meteo
  base_url: https://api.open-meteo.com
  credential_parameter_name: null
  options:
    latitude: "0.0"
    longitude: "0.0"
    site: example-site
    measurements:
      - { field: temperature_2m, name: temperature, type: N, unit: degC }
      - { field: apparent_temperature, name: apparent, type: N, unit: degC }
      - { field: relative_humidity_2m, name: humidity, type: N, unit: "%" }
schedule:
  expression: rate(15 minutes)
  enabled: false
storage:
  partition_key:
    name: date
    type: N
  sort_key:
    name: ts
    type: N
  ttl:
    attribute_name: expts
    retention_days: 90
```

Coordinates and site above are synthetic; choose your own deployment values locally without
committing private settings. The sanitized root `config.yaml` is tracked, not ignored. Keep private
backups outside tracked files; backups are not additional deployment inputs.

The root is a commented, intentionally incomplete scaffold, not a deployable example. Fill required
null/empty identity, request, selections, schedule expression, keys, and TTL names there.
Safe runtime defaults remain; deployment identity and request/schema choices have no fallback.
Weather requests only selected fields and uses UTC `current.time`; it needs no credential.
With the date-`N`/time-`N` example, `ts` is exact whole-second Unix source time, with no identity
suffix. This key pair supports only one stream per table: different streams at the same time
collide and overwrite. One provider configuration does not guarantee one stream; an identity-bearing
`S`/`B` partition key separates streams with time-`N` sort keys.
Every successful observation replaces the whole item, including unchanged polls and same-time
revisions. Polling does not fill source-time gaps or extend source-based TTL.
See [key semantics](docs/configuration.md#storage) for the current storage contract.
`schedule.enabled: false` is a valid finished state, not an incomplete-config marker.
There is no second example/user config. `tests/fixtures/config.yaml` is a sanitized test fixture,
not a deployment configuration to edit.

### 4. Check health and deployment readiness

```bash
make check
make validate
```

`make validate` checks root readiness: registered `BaseProvider` inheritance and one mocked HTTPS
GET, selections/schema, rendered environment size, and source package readiness, then
SAM/cfn-lint/build/package checks. It does not validate normalization against a live response.
The vanilla scaffold fails with `application.stack_name is invalid`; fix reported fields and rerun.
Validation is fail-fast, not an exhaustive list of every unfinished field.
Both commands avoid provider, SSM, and AWS calls. The code-only SAM application build installs no
pip dependencies; local bootstrap and tool installation can download packages.
Contributor test-only demonstration: `make validate CONFIG=tests/fixtures/config.yaml`.
Neither command verifies endpoint availability, live credentials, or AWS account readiness.

### 5. Deploy manually

Obtain explicit approval under [docs/costs.md](docs/costs.md) before AWS deployment or testing.
Only after approval, reuse the profile selected in step 1 (or select one there now) and set the
target Region; replace both example values:

```bash
export AWS_PROFILE=example
export AWS_REGION=eu-north-1
```

If authentication has expired, sign in again using the applicable method in step 1.
Privately verify the selected profile targets the approved AWS account and Region before running
`make deploy`; local readiness and template lint do not verify them.

```bash
make deploy
```

Deployment reruns readiness and template/build checks, then asks for manual CloudFormation
changeset confirmation. Inspect changes before confirming; no command grants cost approval.

Optional overrides:

```bash
STACK_NAME=example-weather AWS_PROFILE=example AWS_REGION=eu-north-1 make deploy
```

With `storage.table_name: null`, the table is `<effective-stack-name>-observations`, including a
`STACK_NAME` override. Explicit table names disable this isolation and produce a warning.

### 6. Smoke-test (separately approved)

A direct invocation still calls the public API and writes to AWS while scheduling is disabled;
it requires separate cost approval.

```bash
make smoke-test
```

The output confirms the direct Lambda invocation completed. It does not read DynamoDB table data or
print credentials.

### 7. Inspect logs

```bash
make logs
```

Logs expire automatically after 14 days by default. Change `logging.retention_days` in `config.yaml` if needed.

### 8. Inspect stored observations (optional; separately approved)

Obtain explicit read-cost approval under [docs/costs.md](docs/costs.md) before live inspection.
The Console uses the same DynamoDB APIs and read capacity as the CLI; it is not free by interface,
and provisioned capacity or request charges may apply. For near-zero-cost flow verification,
reuse existing smoke-test counts or local sanitized fixtures instead of live reads.
This optional step does not authorize reads, deployment, or deletion.

- Select the intended AWS account and Region. In DynamoDB **Tables**, select the deployed stack's
  actual `TableName` output (default: `<effective-stack-name>-observations`).
- Open **Explore table items** and choose **Query**; avoid automatic **Scan** or repeated refresh.
  Use a known partition value as a key equality condition, matching the configured key name/type
  (for example, `date` with Number / `N`); do not guess the value's encoding.
- Set a small item/evaluated limit when available before running one bounded Query. Select
  descending order on the configured sort key (`ts` in the example) for newest within that partition.
  Filter expressions do not replace key conditions or limits.
- Inspect selected fields and the configured TTL attribute (`expts` in the example); rows contain
  no raw payload or envelope. TTL expiry does not guarantee immediate deletion.

A Query cannot find the globally newest row across all partitions. Scan is unordered and may read
the whole table. Eventual consistency may delay visibility of just-written items.

### 9. Enable or pause scheduled ingestion (separately approved)

Review the desired interval; `rate(15 minutes)` below is an example, not a required frequency.
Before cloud commands or Console checks, obtain explicit [cost approval](docs/costs.md) for the
deployment update, recurring Lambda/provider/DynamoDB/log activity, and live verification.
The local alternative is to remain paused (`enabled: false`) and run only `make validate`;
this does not pause an already-enabled deployment.

To enable, merge into the existing `schedule` mapping in `config.yaml`, preserving other fields:

```yaml
schedule:
  expression: rate(15 minutes)
  enabled: true
```

Editing YAML alone has no effect on existing deployments. Reuse the same stack, AWS profile, Region,
and effective `STACK_NAME` from step 5 (including any override); do not rename or create a new stack.
After approval, validate, deploy, and review/confirm the CloudFormation changeset:

```bash
make validate
make deploy
```

In the Console, select the correct account and Region, then **EventBridge Scheduler** > **Schedules**.
Open `${effective-stack-name}-schedule` (the template's `${AWS::StackName}-schedule` Name); verify
`ENABLED`, the intended expression, and the expected stack Lambda target.
`schedule.enabled` maps to `ScheduleState`, controlling the Scheduler resource, not a Lambda
environment variable; Lambda **Configuration** alone does not establish schedule state.

Resource settings and observed recurring invocations are separate checks. After the expected next
interval, inspect recent Lambda logs and existing **CloudWatch** > **Metrics** > **Scheduler**
invocation attempts/target errors; do not add custom metrics. Earlier smoke-test logs are not proof
of scheduling. Exact execution time or an immediate new row is not guaranteed; repeated source
snapshots replace the existing item, even if unchanged. Poll frequency does not create source samples
or fill source-time gaps.

To pause, set `enabled: false`, repeat the same validate/deploy workflow, and verify `DISABLED` in
Scheduler. This pauses future scheduled delivery, not in-flight work, retries, or direct invocations,
and does not stop provisioned table costs. See [detailed operations](docs/operations.md).

### 10. Delete (separately approved)

```bash
make delete
```

Deletion requires separate approval and typing the stack name. Do not delete existing data merely
to adopt a new row shape; plan retention or recovery outside the stack first.

## Commands

```text
make help          Show commands
make bootstrap     Sync locked development dependencies with uv
make format        Apply Ruff formatting and safe fixes
make lint          Run Ruff checks
make test          Run pytest
make validate      Check local config readiness, SAM/CloudFormation, and build
make build         Build SAM application
make check         Run code/fixture health, template validation, and build
make deploy        Deploy configured stack
make smoke-test    Invoke deployed Lambda
make logs          Tail Lambda logs
make delete        Delete stack after explicit confirmation
make clean         Remove local build artifacts
```

## Configuration

`config.yaml` is the single application/deployment configuration file. YAML was selected because it is readable, supports comments, and fits SAM-oriented workflows.

Configuration reads root `config.yaml` without deployment identity/request defaults.
Configuration is validated by `scripts/config.py`; unknown top-level and fixed-section keys and
unsafe values fail early. `provider.options` remains adapter-specific.

Important fields:

- `application.stack_name`: CloudFormation stack name
- `provider.name`: adapter factory registered in `src/scheduled_ingestor/registry.py`
- `provider.base_url`: provider HTTPS base URL
- `provider.credential_parameter_name`: SSM parameter name, never secret value
- `schedule.expression`: EventBridge rate or cron expression
- `logging.retention_days`: CloudWatch retention, default `14`
- `storage.table_name`: `null` for stack-derived naming, or an explicit override
- `storage.provisioned_throughput`: read and write capacity units
- `storage.partition_key` and `storage.sort_key`: DynamoDB key attributes
- `storage.ttl.retention_days`: observation TTL, default `90`
- `alarms.enabled`: create basic native-metric alarms; defaults to `false`

See [docs/configuration.md](docs/configuration.md).

## Provider adapters

Each adapter explicitly inherits `BaseProvider` from
`scheduled_ingestor.core.provider` and implements both abstract methods:

```text
fetch(self, client: HttpClient, credential: str | None) -> object
normalize(self, payload: object, observed_at: datetime) -> list[Observation]
```

These are contract signatures, not runnable code. Abstract methods block incomplete subclass
instantiation, not incorrect API behavior or signatures; runtime tests remain required.

To add a provider:

1. Add `src/scheduled_ingestor/providers/<name>_provider.py`.
2. Subclass `BaseProvider`; implement both methods and declare supported selections once in root config.
3. Register it in the explicit allowlist in `src/scheduled_ingestor/registry.py`
  (`open_meteo` only today).
4. Add sanitized response fixtures.
5. Run provider contract tests.
6. Document configuration and optional credential format; generic SSM support remains available.

See [docs/adapters.md](docs/adapters.md).
Reusable `core/` owns the contract, settings, HTTP, snapshots, schema, repository, and
`IngestionService`. Root `handler.py` composes these dependencies and delegates ingestion; the
entrypoint remains `scheduled_ingestor.handler.lambda_handler`. New providers normally need no
core, handler, or infrastructure changes. See the
[core/plugin ownership table](docs/architecture.md#core-and-plugin-boundary) for current paths.

## Storage schema

Root configuration owns keys, TTL, and selected typed measurements. The application's
`TableSchema` validates all required selected fields before writes;
DynamoDB does not enforce non-key attribute types.

```yaml
partition_key:
  name: date
  type: N
sort_key:
  name: ts
  type: N
ttl:
  attribute_name: expts
  retention_days: 90
```

At startup, Lambda constructs the runtime `TableSchema` from the complete storage schema passed by
the deployment config. Repository rows use configured key and TTL names, validate configured field
types, and reject undeclared attributes before unconditional whole-item writes. Runtime schema is required.

Snapshot row contract v2 stores only configured deterministic keys, TTL, and selected native
scalar fields. Weather numbers are `Decimal`; no envelope, coordinates, site attribute, units,
raw payload, or ingestion timestamp is added. One snapshot holds all selected readings at one
stream/source time. Units remain configuration metadata. Providers return aware source time in
`Observation.observed_at`; the repository encodes keys, with no `BaseProvider.set_sort_key` API or
implicit system-time fallback. Numeric `ts` is exact whole-second Unix source time; fractional source
times are rejected. `S`/`B` encoding is unchanged.

Date-`N`/time-`N` is single-stream per table; identity-bearing `S`/`B` partitions separate streams
with time-`N` sort keys. A new site identity alone cannot prevent numeric-only collisions.
Unconditional writes replace the whole snapshot, not individual fields. Last successful write/arrival
wins; delayed stale retries may overwrite a same-time correction. No latest-revision ordering or
revision history is guaranteed. `written` counts inserts, replacements, and unchanged repeats;
built-in `duplicates` remains zero. TTL derives from source time and does not extend per poll.

No generic metadata toggle exists; extra attributes require matching developer code/config/schema
support. V2 is incompatible with old envelope rows/consumers; see
[legacy row compatibility](docs/migration.md#snapshot-contract-v2).

## Security

- Lambda can write only to its table.
- Lambda can read only the configured SSM parameter path.
- Scheduler can invoke only its Lambda target.
- Secrets are fetched at runtime and cached per Lambda execution environment.
- Authorization headers and secret values are never logged.
- The Security workflow runs Gitleaks.
- Public fixtures must be synthetic or sanitized.

Read [SECURITY.md](SECURITY.md) before publishing changes.

## Cost controls

Near-zero cost is a design target, not a guarantee. `make check` is local-only; it does not
create AWS resources or inspect AWS costs.

Use the AWS Billing and Cost Management console for manual cost review. This is the preferred
free option. Cost Explorer API requests cost $0.01 each. Custom billing views and hourly
granularity may incur additional charges.

The checked-in scaffold sets `schedule.enabled: false`; provisioned table charges persist when paused.
Enable scheduling only for approved tests.
Do not run `make deploy`, `make smoke-test`, or destructive cleanup without explicit approval.
One snapshot instead of two metric rows does not guarantee savings at the 1-WCU capacity floor.
Removing the numeric key suffix does not guarantee a lower bill either; every successful observation
writes, including unchanged polls. No cloud cost savings have been measured. The key/replacement
change adds no bundled dependencies, resources, or IAM permissions; runtime remains stdlib plus
managed Lambda SDK/urllib3 libraries.

Keep these low-cost defaults:

- ARM64 Lambda, 128 MB, and a 20-second timeout
- DynamoDB provisioned capacity: 1 read and 1 write capacity unit
- 14-day CloudWatch log retention
- `alarms.enabled: false`
- no VPC, NAT Gateway, API Gateway, provisioned concurrency, X-Ray, or custom metrics
- 90-day DynamoDB TTL

Costs still depend on AWS Region, API pricing, payload size, logs, item count, enabled alarms, and
account free-tier eligibility. Create an AWS Budget separately if desired; this repository does
not manage account-wide governance.
See [docs/costs.md](docs/costs.md).

## Recovery and migration

Restore matching plugin/tests and completed local root choices before an approved manual redeploy.
Recreating infrastructure does not restore rows. Restore external SSM credentials separately only
for future authenticated providers; weather needs none. Data recovery remains a separately approved
workflow outside this application.

See:

- [docs/recovery.md](docs/recovery.md)
- [docs/migration.md](docs/migration.md)
- [docs/operations.md](docs/operations.md)

## Contribution guidance

Follow [CONTRIBUTING.md](CONTRIBUTING.md). Read [architecture](docs/architecture.md) and
[adapter guidance](docs/adapters.md) alongside the
[source contract](src/scheduled_ingestor/core/provider.py).
An ordinary editor is sufficient and no assistant-specific setup is required; AI coding assistants
are also supported.

## License

MIT. See [LICENSE](LICENSE).
