# Provider adapters

Adapters isolate external API behavior from infrastructure and persistence.

## Runtime contract

Providers explicitly subclass `BaseProvider` from `scheduled_ingestor.core.provider` and implement
its abstract `fetch(client, credential)` and `normalize(payload, observed_at)` methods. Fetch returns
an opaque payload; normalize returns `list[Observation]` using the snapshot model in
`scheduled_ingestor.core.models`.

`observed_at` is UTC invocation time supplied by `IngestionService` as
`datetime.now(UTC).replace(microsecond=0)`, not a command to timestamp every observation.
Use API/source timestamps and stable `provider`, `dataset`, and `subject` identities where available
so repeated source samples retain deterministic keys and TTL with unchanged storage settings.
Invocation time can change on retry; no automatic source-time fallback is supplied.
Return a timezone-aware source timestamp in `Observation.observed_at`; the model converts it to UTC.
The repository encodes keys, rejecting fractional source times for numeric sort keys.
`BaseProvider` has no `set_sort_key` API; adapters do not construct physical keys.
See [delivery semantics](architecture.md#runtime-flow) and [weather source time](#weather-example).

Register the adapter factory in `src/scheduled_ingestor/registry.py`; `PROVIDER_FACTORIES` maps names
to factories accepting `(base_url: str, options: dict[str, object])` and returning `BaseProvider`.
The allowlist contains only `open_meteo` today; adding a module does not register it automatically.
Abstract methods prevent instantiating a subclass missing either implementation. They do not check
method signatures, response validity, API semantics, or persistence compatibility; runtime tests must.

Declare endpoint/options, schedule, keys, TTL, and selected measurements in root `config.yaml`.
There are no implicit selections. Infrastructure and handler stay
provider-neutral; developer work belongs in adapter fetch/normalize, root choices, and fixtures/tests.
Root `handler.py` composes `IngestionService` with the selected provider, HTTP client, repository,
and optional SSM dependencies. Core depends on `BaseProvider`, not concrete adapters or the registry.
See [architecture ownership](architecture.md#core-and-plugin-boundary) for the reusable core files.

## Prepare a plugin

1. Read API documentation, terms, and rate limits; manually issue the intended GET before integration.
2. Inspect response shape, field units, source time/timezone, nulls, and API error responses.
3. Choose stored outputs; implement request construction and normalization in
	`providers/<name>_provider.py`.
4. Register the factory and configure its supported options in root `config.yaml`.
5. Save sanitized fixtures and unit/contract/malformed-response tests; run `make check`, then
	`make validate` with completed root choices before a separately approved manual deployment.

The [README request](../README.md#2-prepare-the-api-manually-required) is a human preparation step,
not a request sent by check/validate. Config plus matching plugin/tests is the deliverable for a
different API; mapping arbitrary JSON fields or future boolean outputs is not implemented.

## Implement and register

This synthetic example accepts one fixed numeric selection, not arbitrary JSON mappings. Put it in
`src/scheduled_ingestor/providers/example_provider.py`. Use a sanitized payload with
`time: "2026-01-01T00:00:00+00:00"`, `unit: "degC"`, and a numeric `temperature`; HTTP decoding
produces `Decimal` for fractional JSON numbers.

```python
from datetime import datetime, timedelta
from decimal import Decimal

from scheduled_ingestor.core.http import HttpClient
from scheduled_ingestor.core.models import Observation
from scheduled_ingestor.core.provider import BaseProvider


class ExampleProvider(BaseProvider):
    def __init__(self, base_url: str, options: dict[str, object]) -> None:
        selection = [{"field": "temperature", "name": "temperature", "type": "N", "unit": "degC"}]
        if set(options) != {"site", "measurements"} or options["measurements"] != selection:
            raise ValueError("Expected site and the supported temperature selection")
        site = options["site"]
        if not isinstance(site, str) or not site:
            raise ValueError("Expected a non-empty site")
        self.base_url = base_url
        self.site = site

    def fetch(self, client: HttpClient, credential: str | None) -> object:
        """Fetch the configured credential-free HTTPS endpoint."""
        return client.get_json(self.base_url)

    def normalize(self, payload: object, observed_at: datetime) -> list[Observation]:
        """Validate one reading and use its UTC source time."""
        del observed_at
        if not isinstance(payload, dict) or payload.get("unit") != "degC":
            raise ValueError("Expected a temperature response in degC")
        timestamp = payload.get("time")
        if not isinstance(timestamp, str):
            raise ValueError("Expected an ISO8601 source time")
        source_time = datetime.fromisoformat(timestamp)
        if source_time.tzinfo is None or source_time.utcoffset() != timedelta(0):
            raise ValueError("Expected UTC source time")
        value = payload.get("temperature")
        if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
            raise ValueError("Expected a numeric temperature")
        reading = Decimal(value)
        if not reading.is_finite():
            raise ValueError("Expected a finite temperature")
        return [
            Observation(
                provider="example",
                dataset="current-temperature",
                subject=self.site,
                observed_at=source_time,
                values={"temperature": reading},
            )
        ]
```

In the existing root registry module, add the import and factory entry alongside `open_meteo`:

```python
from scheduled_ingestor.providers.example_provider import ExampleProvider

PROVIDER_FACTORIES["example"] = ExampleProvider
```

The example endpoint is `https://example.com/current`, not a live service to call. Root config would
select `name: example`, `site: example-site`, and the single `measurements` declaration above, alongside
completed deployment/storage choices. Keys and TTL remain repository-generated; normalization
emits only selected values and internal identity/source time. This example is not shipped or registered.
Test it with mocked HTTP and sanitized fixtures before integration. Use the included
`providers/open_meteo_provider.py` as the full weather implementation, not this synthetic API shape.

## Weather example

`OpenMeteoProvider` calls the public `https://api.open-meteo.com/v1/forecast` endpoint without
credentials, requesting only selected current fields with `timezone=UTC`.
Options are decimal-string `latitude`/`longitude` and a short `site` identifier; see
[configuration.md](configuration.md). Fixture coordinates/site are synthetic, not defaults.

HTTP JSON decoding preserves fractional numbers as `Decimal`. Normalization requires finite
readings, field-specific units, and explicit UTC/GMT metadata with zero offset. One snapshot uses
`current.time`, not invocation-time `observed_at`. Its internal dataset is `current-weather`,
subject is stable `site`, and selected `values` contain native numbers. These identity components
are encoded by identity-bearing keys, not redundant attributes; date-`N`/time-`N` omits them.
Temperature/apparent use `degC`; humidity uses `%`.
`current.interval` and the poll schedule do not generate observations or fill gaps in `current.time`.

The shared client bounds every HTTP body, including retryable errors, to 1 MB and closes/releases
responses without automatic draining. It rejects redirects and nonstandard numeric constants.
Three invocation-wide attempts, capped delays, and wall-clock deadlines reserve persistence time;
adapters must not add retries or bypass that transport. HTTP failures and malformed JSON errors omit
request credentials and raw bodies; time-budget failures propagate rather than returning success.

| Source `field` | Example output `name` | Supported `type` | Config `unit` / response unit |
| --- | --- | --- | --- |
| `temperature_2m` | `temperature` | `N` | `degC` / U+00B0 followed by `C` |
| `apparent_temperature` | `apparent` | `N` | `degC` / U+00B0 followed by `C` |
| `relative_humidity_2m` | `humidity` | `N` | `%` / `%` |

Select any nonempty subset. Source fields and output names must each be unique; output names must
not collide with keys/TTL. The request's `current` list derives from selection, not a second setting.
Missing/null, boolean, nonfinite, wrong-unit, or non-UTC readings are rejected rather than stored.

Snapshot row contract v2 stores only configured keys, TTL, and required selected native fields;
DynamoDB does not enforce those non-key types. Numeric `date` is the source UTC date (`YYYYMMDD`);
numeric `ts` is exact whole-second Unix source time, without an identity suffix. Date-`N`/time-`N`
supports one stream per table: same-time different streams collide and overwrite, even with distinct
provider/dataset/subject identities. One provider configuration does not guarantee one stream.
Use an identity-bearing `S`/`B` partition to separate streams with time-`N` sort keys;
existing `S`/`B` key encoding is unchanged. See [storage encoding](configuration.md#storage).
TTL derives from source time plus retention days and does not extend on repeated polls.
Unconditional writes replace the whole item, including unchanged repeats and revised readings.
Last arrival wins; a delayed stale retry can overwrite a same-time correction. There is no
guaranteed latest-revision ordering or revision history.

Keep site identity stable within a stream. Choose a new stream identity when location semantics
change, but date-`N`/time-`N` cannot separate those identities; use an identity-bearing partition
or a separate table before combining streams.
Only keys, TTL, and selected fields are stored: no coordinates, separate site/units, raw payload,
`ingested_at`, or duplicated envelope. Extra metadata requires explicit developer code and matching
config-derived schema support; weather has no generalized metadata toggle.
Old rows/consumers are incompatible; see [migration.md](migration.md). No migration/backfill runs.

## Responsibilities

- Build URL and safe headers.
- Call only HTTPS endpoints.
- Validate response shape.
- Convert payloads to `Observation` objects.
- Put only configured selected scalars in `Observation.values`.
- Never log credentials or raw sensitive payloads.

## Non-responsibilities

- DynamoDB writes
- SSM access
- retries beyond HTTP policy
- CloudFormation configuration

## Acceptance criteria

A new adapter must include sanitized fixtures, unit tests, contract tests, timeout behavior, malformed-response behavior, `429`/`5xx` behavior, and documentation of its credential format.

For future authenticated providers, keep secret values outside Git/config and reference an SSM
SecureString via `credential_parameter_name`. Production `IngestionService` uses its injected SSM
client to read/cache real credentials per instance only when a parameter is configured; it never
calls `readiness_credential()`. Keep strict credential validation in `fetch()`.
Weather needs neither a parameter nor authorization headers.

`BaseProvider.readiness_credential() -> str | None` is optional and non-abstract; default `None`
preserves existing providers. Authenticated adapters may override it with format-valid, obviously
synthetic nonsecret content, including a JSON string if their fetch parser expects it.
Never return actual secrets or read credentials from environment variables, SSM, or the network.
[Offline readiness](configuration.md#fill-and-validate) calls this hook only with a configured
nonempty parameter name; see that contract for request checks and validation limits.
