# Configuration

`config.yaml` is the single user-edited configuration file. `template.yaml` is infrastructure source, not runtime configuration.

## Defaults and ownership

`scripts/config.py` reads root configuration without merging deployment identity/request/schema defaults.
The commented scaffold intentionally lacks deployment identity, endpoint/options, selections,
schedule expression, and key/TTL names. `make check` tests code/fixtures independently;
`make validate` rejects unfinished config and never calls API/SSM/AWS. Disabled scheduling is valid.
The package-root `src/scheduled_ingestor/registry.py` maps provider names to factories and currently
contains only `open_meteo`. A new provider explicitly subclasses `core.provider.BaseProvider`;
its factory accepts the configured base URL and typed options. Configuration selects a registered
implementation, not an arbitrary import path. See [adapters.md](adapters.md#implement-and-register).
The root config is sanitized and tracked, not ignored. Private backups are not deployment inputs;
never commit private values. There is no second user/example config. `tests/fixtures/config.yaml`
is sanitized contributor test data only, not deployment config to edit.

## Fill and validate

Complete required root values after [manual API preparation](../README.md#2-prepare-the-api-manually-required)
and matching plugin/fixture tests:

- Application stack identity and environment
- Registered provider, HTTPS endpoint, supported request options and selected outputs
- Schedule expression, even when `enabled: false`
- Key names/types and distinct TTL name; selected names must not collide

Keep safe runtime/capacity/logging defaults unless deliberately changing them. An optional SSM
reference or stack-derived table name may remain `null`; required scaffold nulls/empty selections may not.

```bash
make check
make validate
```

Health checks do not require finished root config. Offline readiness validates options and explicit
`BaseProvider` inheritance. With a nonempty `provider.credential_parameter_name`, it calls
`readiness_credential()` and passes the result to `fetch(mock_client, synthetic_value)`;
otherwise it calls `fetch(mock_client, None)` without calling the hook.
See [synthetic credential requirements](adapters.md#acceptance-criteria).

Readiness requires exactly one constructed HTTPS `HttpClient.get_json()` request with a valid
method signature. Both `get_json(url)` and `get_json(url=url)` are accepted; an invalid signature
raises `ValueError`. It does not call `normalize()` or prove real authentication.
It checks derived schema/rendered environment (<=4096 bytes) and source package; `make validate`
then runs SAM/cfn-lint/build/package checks. These checks do not verify API availability or AWS
account access. The code-only SAM application build installs no pip dependencies; local bootstrap
and tool installation can download packages.
Validation fails fast: vanilla root first reports `application.stack_name is invalid`, skips
downstream readiness, and requires correction/rerun rather than listing all failures together.

Contributor-only fixture demonstration: `make validate CONFIG=tests/fixtures/config.yaml`.
`make deploy` reruns readiness and asks for manual changeset confirmation after explicit
[cost approval](costs.md); validation never approves cloud costs.

Do not place secrets in YAML. Optional `credential_parameter_name` references an externally managed
SSM SecureString; `null` needs no parameter. Weather is credential-free.
`application.stack_name` and an effective `STACK_NAME` override may contain up to 55 characters so
derived Lambda and EventBridge Scheduler names remain within AWS limits.

## Provider options

`provider.options` is adapter-specific. Values become compact JSON in Lambda environment variables. Keep them non-secret and small.
The configuration script accounts for all generated Lambda environment variables and rejects a
configuration whose UTF-8 environment payload exceeds the 4096-byte Lambda limit.

`provider.base_url` must be HTTPS with a normal hostname, IPv4 address, or `localhost`, plus an optional port, path, and query. Credentials and fragments are rejected. `credential_parameter_name` may be empty or a slash-prefixed SSM path up to 2048 characters and 15 non-empty hierarchy levels; its first component cannot be `aws` or `ssm`.

Open-Meteo applies stricter adapter validation: base URLs cannot contain queries, and options must
contain exactly `latitude`, `longitude`, `site`, and `measurements`. Coordinates are decimal strings within
latitude +/-90 and longitude +/-180; `site` is 1-64 letters, digits, underscores, or hyphens.

```yaml
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
```

These coordinates/site are synthetic; choose deployment values locally.
Finite weather fields are `temperature_2m`, `apparent_temperature` (both `N`/`degC`),
and `relative_humidity_2m` (`N`/`%`). Each selection declares source `field`, output `name`, `type`,
and `unit` once; request fields and required runtime schema derive from this list.
`name` is a chosen row attribute, not an automatic generic JSON mapping. Adding unsupported fields
requires developer plugin/tests changes. Weather supports only numeric `N`, not `BOOL` selections.
Keep `site` stable; changed location semantics require a deliberately new stream identity.
Only identity-bearing keys separate streams; date-`N`/time-`N` cannot do so.

## Schedule

The scaffold leaves expression unconfigured and sets `enabled: false`.
Enabling scheduling requires the [AWS cost approval gate](costs.md).

Use EventBridge Scheduler expressions with a positive integer and minute, hour, or day units. The
expression must be no more than 256 characters:

```yaml
expression: rate(1 minute)
```

or a six-field cron expression:

```yaml
expression: cron(0 12 * * ? *)
```

Cron fields must be non-empty. Disable schedule during migration or provider troubleshooting.
The schedule controls fetch frequency, not source timestamps or observation generation.
Open-Meteo uses UTC `current.time`; neither polling nor `current.interval` fills source-time gaps.
`enabled` must be a YAML boolean. `flexible_time_window` must be the boolean `false`; flexible scheduling is not supported by the current template.
The configuration script is authoritative for cron semantics: its semantic validation rejects
descending ranges such as `9-3` and multiple `#` expressions in one day-of-week field. The template
`AllowedPattern` enforces lexical constraints only, because CloudFormation regexes cannot compare
range endpoints or enforce this EventBridge-specific rule.

## Alarms

`alarms.enabled` must be a YAML boolean. `notification_topic_arn` must be `null` or an SNS topic ARN.

The runtime architecture is fixed to `arm64`.

## Logs

Default retention is 14 days:

```yaml
logging:
  level: INFO
  retention_days: 14
```

The template restricts values to CloudWatch-supported retention periods. Stack deletion removes the log group.

`logging.level` accepts `DEBUG`, `INFO`, `WARNING`, `ERROR`, and `CRITICAL`. SAM sets native JSON
logging and maps the application level to Lambda's `DEBUG`, `INFO`, `WARN`, `ERROR`, and `FATAL`,
respectively. The handler uses stdlib logging, with no Powertools environment variables.
This uses the existing root settings; no extra switch, layer, or user configuration is needed.
See [log fields and verification limits](operations.md#logs).

## Storage

`storage` declares the DynamoDB table deployment contract. `billing_mode` is `PROVISIONED` and
`provisioned_throughput` sets read and write capacity units. `partition_key`, `sort_key`, and `ttl`
become the DynamoDB key schema and TTL specification.

`null` `storage.table_name` resolves to `<effective-stack-name>-observations` after the
`STACK_NAME` override. An explicit name emits
`Warning: explicit storage.table_name disables stack-derived table isolation`; choose distinct
names for distinct stacks. Each additional stack provisions another table and capacity charges.

Runtime `fields` derive from configured keys/TTL plus selected measurement names/types, not a second
user declaration. The schema is serialized into `StorageSchemaJson`
and passed through CloudFormation to Lambda; it does not create DynamoDB `AttributeDefinitions` or
alter the table key schema. Lambda receives the runtime schema as compact JSON; key and TTL
and every selected attribute are required with validated types.
Do not add root `storage.fields`: the field list is derived once, including keys/TTL, not duplicated.
Runtime `STORAGE_SCHEMA_JSON` is mandatory; there is no schema fallback.

Runtime settings load in `scheduled_ingestor.core.config.Settings`; schema validation belongs to
`scheduled_ingestor.core.storage.TableSchema`. Root `handler.py` composes these with the provider
and `IngestionService`. The core/plugin reorganization changes neither rendered config nor row shape.

`TableSchema` also rejects unexpected top-level attributes and incompatible DynamoDB values before
writes. DynamoDB itself does not enforce non-key types. Snapshot row contract v2 stores only configured
keys, TTL, and selected native fields; weather `Decimal` readings serialize as DynamoDB `N`.
No envelope, coordinates/site, units, or `ingested_at` are added as separate attributes.
Metadata needs explicit matching developer code/config/schema support; no generic toggle exists.
Providers return aware source time in `Observation.observed_at`; the repository encodes the keys.
There is no `BaseProvider.set_sort_key` API or implicit system-time fallback.

| Key encoding | Stored value / boundary |
| --- | --- |
| Partition `N` | UTC source date as `YYYYMMDD`; no stream identity |
| Sort `N` | Exact whole-second Unix source time; fractional source times rejected |
| Partition `S` / `B` | Provider/dataset/subject identity string / its encoded bytes |
| Sort `S` / `B` | Source-time string / bytes; existing identity suffix retained when partition is `N` |

The date-`N`/time-`N` example is single-stream per table: same-time different streams overwrite
one another. One configured provider need not mean one stream. An identity-bearing `S`/`B`
partition separates streams with a time-`N` sort key; changing only internal identity cannot
separate streams under numeric-only keys. `S`/`B` encoding is unchanged.

Every successful observation unconditionally replaces the whole item at its physical key, including
unchanged polls and same-time revisions. Last successful write/arrival wins, not necessarily the
latest source revision; stale retries may overwrite corrections. No revision history is retained.
TTL is source time plus retention days, floored to Unix seconds; repeated polls do not extend it
with unchanged retention settings.

V2 is incompatible with old envelope rows/consumers.
See [adapters.md](adapters.md) for source-time handling and
[migration.md](migration.md#separately-approved-transition) before changing keys or table names.
