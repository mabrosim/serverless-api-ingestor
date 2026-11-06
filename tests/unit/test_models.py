from datetime import UTC, datetime, timedelta, timezone
from decimal import Decimal

import pytest

from scheduled_ingestor.core.models import Observation


def test_keys_are_deterministic() -> None:
    observation = Observation(
        provider="example",
        dataset="sample",
        subject="item",
        observed_at=datetime(2026, 8, 5, 12, 34, tzinfo=UTC),
        values={"reading": Decimal("123.45")},
    )
    assert observation.pk == "PROVIDER#example#DATASET#sample#SUBJECT#item"
    assert observation.sk == "OBSERVED_AT#2026-08-05T12:34:00+00:00"


def test_keys_escape_delimiters_without_colliding() -> None:
    observed_at = datetime(2026, 8, 5, 12, 34, tzinfo=UTC)
    first = Observation(
        provider="example",
        dataset="sample",
        subject="first#SUBJECT#second",
        observed_at=observed_at,
        values={"reading": Decimal("123.45")},
    )
    second = Observation(
        provider="example",
        dataset="sample#SUBJECT#first",
        subject="second",
        observed_at=observed_at,
        values={"reading": Decimal("123.45")},
    )

    assert first.pk != second.pk


@pytest.mark.parametrize("microsecond", [0, 123456])
def test_snapshot_canonicalizes_aware_source_time_to_utc(microsecond: int) -> None:
    snapshot = Observation(
        "example",
        "sample",
        "item",
        datetime(2026, 1, 1, 2, microsecond=microsecond, tzinfo=timezone(timedelta(hours=2))),
        {"reading": Decimal("1")},
    )
    assert snapshot.observed_at == datetime(2026, 1, 1, microsecond=microsecond, tzinfo=UTC)


def test_snapshot_rejects_naive_source_time() -> None:
    timestamp = datetime(2026, 1, 1)
    values = {"reading": Decimal(1)}
    with pytest.raises(ValueError, match="timezone-aware"):
        Observation("example", "sample", "item", timestamp, values)


@pytest.mark.parametrize("identity,values", [("", {"reading": Decimal(1)}), ("item", {})])
def test_snapshot_requires_identity_and_selected_values(identity: str, values: dict) -> None:
    timestamp = datetime(2026, 1, 1, tzinfo=UTC)
    with pytest.raises(ValueError):
        Observation("example", "sample", identity, timestamp, values)
