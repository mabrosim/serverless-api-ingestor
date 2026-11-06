# Contributing

1. Create a focused branch.
2. Read [README.md](README.md), [architecture](docs/architecture.md),
   [adapter guidance](docs/adapters.md), [SECURITY.md](SECURITY.md), and the
   [cost approval policy](docs/costs.md#contributor-approval-policy).
3. Run `make bootstrap`, then `uv run --locked --extra dev pre-commit install`.
4. Add tests with behavior changes; update [CHANGELOG.md](CHANGELOG.md) for release-relevant changes.
5. Run `uv run --locked --extra dev pre-commit run --all-files`, `make format`, and `make check`.
6. Run redacted secret scanning using [SECURITY.md](SECURITY.md#secret-scanning).
7. Document cost or IAM impact.
8. Keep examples synthetic and public-safe.

## API and configuration changes

Use the [README preparation workflow](README.md#quick-start): clone/read/bootstrap/check, read API
docs/terms/rate limits, manually GET and inspect shape/units/source time/nulls/errors, then choose
stored fields. Add `src/scheduled_ingestor/providers/<name>_provider.py` as an explicit `BaseProvider`
subclass with both abstract methods implemented. Update `src/scheduled_ingestor/registry.py` factory
registration, root `config.yaml`, and matching sanitized fixtures/provider tests together.
Reusable core, root handler composition, and deployment rendering normally stay unchanged; use the
[ownership table](docs/architecture.md#core-and-plugin-boundary) and
[provider tutorial](docs/adapters.md#implement-and-register) for current paths and signatures.
ABC completeness is not API/signature validation; test normalization, transport failures, and schema
compatibility at runtime. Do not restore legacy modules or add automatic plugin discovery.
There is no generic JSON mapping engine or second user config; test config is fixture evidence only.

`make check` verifies code/fixture health, templates, build, and package without completed root config.
`make validate` checks completed root readiness with mocked HTTP, then SAM/cfn/build/package checks;
it fails fast, accepts disabled scheduling, and does not verify accounts/endpoints/credentials.
Both avoid provider/AWS calls; the code-only SAM build installs no pip dependencies.
Local bootstrap and tool installation can download packages.
Contributor-only fixture demonstration: `make validate CONFIG=tests/fixtures/config.yaml`.

Pytest enables `--strict-markers` and `--strict-config` in `pyproject.toml`; unknown markers and
invalid configuration must fail rather than be silenced. The application coverage threshold
remains 100%. Keep these gates enabled for focused tests and `make check`.

`SAM_CLI_TELEMETRY=0` disables SAM telemetry.

Root choices define supported selected outputs once; runtime fields derive from keys/TTL/selections.
Weather supports `N` readings only and snapshot contract v2, not the old string-value envelope.
Update readers deliberately and prefer a fresh independently named stack/table for changed shape.
No migration/backfill/replacement/deletion is implied. See [migration.md](docs/migration.md).

Follow the [cost gate](docs/costs.md) before proposing/running cloud work. `make deploy` reruns
readiness and requires manual changeset confirmation; tests/validation never grant cost approval.
Schedule-off still incurs provisioned capacity charges. ARM64, 14-day logs, least-privilege IAM,
and deletion/replacement policies remain unchanged by the snapshot/config work.

Keep `uv.lock` and `.python-version` committed. The `dev` extra supplies contributor tools and
local boto3, botocore, and urllib3; application `dependencies` remain empty. The project continues
to use setuptools, not a uv-specific build backend.

For deliberate dependency upgrades, run `uv lock --upgrade`, review the lockfile diff,
then run `make bootstrap` and the checks above. After changing dependency declarations in
`pyproject.toml`, run `uv lock` to refresh the lockfile and review it before committing.
These commands update local development dependencies, not AWS-managed Lambda libraries.
Keep `src/requirements.txt` comments-only: SAM bundles application source without pip dependencies.
The package gate checks exact source-module bytes and rejects dependency distributions and other
non-source files; `make build` removes generated `.DS_Store` files before that check. The deploy
helper also cleans only generated regular `.DS_Store` files in the function build tree and verifies
its final build before invoking deployment with manual changeset confirmation; see
[operations](docs/operations.md#runtime-dependencies).
Changing this policy requires explicit review, not adding dependencies to bypass the gate.
Monitor managed-library security and compatibility separately; local test versions do not pin
deployed versions. See [runtime maintenance](docs/operations.md#runtime-dependencies).
