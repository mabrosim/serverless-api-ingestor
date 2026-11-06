# Migration

## Code layout compatibility

The core/plugin restructuring is a code import change, not a new row contract or infrastructure
migration. Use `scheduled_ingestor.core` for provider contract, settings, HTTP, models, schema,
repository, and `IngestionService`; provider factories stay in `scheduled_ingestor.registry`.
Concrete providers explicitly subclass `BaseProvider`. Legacy modules were deleted, not kept as
forwarding shims. Update downstream imports to the
[current paths](architecture.md#core-and-plugin-boundary) and rebuild from matching source.

The layout change alone preserves `scheduled_ingestor.handler.lambda_handler`, response/log field
names, measurement/schema derivation, native values, and source-time TTL. It requires no table
migration, backfill, resource move, or credential change. Current key and write semantics are
described below; import compatibility does not imply persistence compatibility.
The earlier snapshot-v2 compatibility boundary below still applies independently.

## Numeric source-time keys and replacement

Numeric sort keys store exact whole-second Unix source time without an identity suffix;
fractional source times are rejected. Providers return aware
`Observation.observed_at`; the repository encodes it. `S`/`B` encoding remains unchanged.
There is no implicit system-time fallback.

Date-`N`/time-`N` supports only one stream per table. Same-time different streams collide and
overwrite; one provider configuration does not prove a single stream. An identity-bearing `S`/`B`
partition separates streams with time-`N` sort keys.

Writes unconditionally replace the whole snapshot, including unchanged polls and same-time
revisions. Last successful write/arrival wins, with no guaranteed latest-revision ordering or revision
history; a delayed stale retry may overwrite a correction. `written` counts every successful insert,
replacement, and unchanged repeat; built-in `duplicates` remains zero. Source-based TTL does not
extend per poll.

## Snapshot contract v2

Current rows contain only configured deterministic keys, TTL, and selected native
fields. Weather writes one snapshot per site/UTC source time with `Decimal` readings as DynamoDB `N`.
There is no stored `schema_version`, old envelope/string `value`, coordinates/site, units, or
`ingested_at`. V2 names the row contract, not an emitted version attribute.
Numeric `ts` is now whole-second Unix source time; numeric-only keys omit stream identity.

Existing rows and consumers of the old two-metric/envelope shape are incompatible. Adding/removing
selected outputs changes the required application contract, not DynamoDB non-key columns, and does
not backfill existing rows. Runtime schema derives once from root keys/TTL/selected names/types;
there is no separate root `storage.fields` declaration or runtime fallback.
Whole-item replacement removes prior fields absent from the incoming snapshot; it is not a merge.
Changing a location requires a new stream identity and keys/table that actually separate streams.

No data migration, backfill, rekeying, infrastructure replacement, or deletion has been executed
or authorized by this change.
Prefer a fresh independently named stack/table for a changed shape; review consumers and preserve
old data. Any replay/import needs its own compatibility and approval plan.

## Separately approved transition

Changing key names/types or the table name can require replacement. Table `DeletionPolicy` and
`UpdateReplacePolicy` remain `Delete`, so replacement or deletion can destroy observations.

Only after approving a transition plan:

1. Obtain [cost approval](costs.md), disable scheduling, and deploy the pause.
2. Arrange historical-data recovery outside this application before replacement.
3. Use distinct effective stack/table names for side-by-side operation; review consumer compatibility.
4. Run `make check` and `make validate`; manually deploy the new stack paused and confirm its changeset.
5. Run an approved smoke test and verify rows without unapproved scans or exports.
6. Enable the new schedule only after validation and explicit recurring-cost approval.
7. Retain old resources/data unless deletion is separately approved; acceptance alone is not permission.

`make delete` is destructive and requires typing the stack name. A `null` table name derives from the
effective stack name, including `STACK_NAME`; an explicit
table name disables that isolation and emits a warning. Additional stacks duplicate provisioned
capacity charges while both exist, even if schedules are disabled.
One snapshot instead of two metric rows does not guarantee savings under minimum provisioned capacity.

Manual DynamoDB PITR or S3 export workflows remain external because they can incur AWS charges.
Treat recovered observations as sensitive and do not publish them.
