import json
from dataclasses import replace
from datetime import UTC, datetime, timedelta
from decimal import Decimal
from itertools import product
from pathlib import Path
from typing import TypedDict, Unpack, cast

import pytest
import yaml
from botocore.exceptions import ClientError

from scheduled_ingestor.core.models import Observation
from scheduled_ingestor.core.repository import ObservationRepository
from scheduled_ingestor.core.storage import (
    AttributeSchema,
    DynamoDBAttributeType,
    TableSchema,
)
from scheduled_ingestor.providers.open_meteo_provider import OpenMeteoProvider


class PutItemCall(TypedDict):
    Item: dict[str, object]


class RecordingTable:
    def __init__(self, error: ClientError | None = None) -> None:
        self.calls: list[PutItemCall] = []
        self.error = error

    def put_item(self, **kwargs: Unpack[PutItemCall]) -> None:
        self.calls.append(kwargs)
        if self.error:
            raise self.error


class ReplacingRecordingTable(RecordingTable):
    def __init__(self, key_names: tuple[str, str]) -> None:
        super().__init__()
        self.key_names = key_names
        self.items: dict[tuple[object, object], dict[str, object]] = {}

    def put_item(self, **kwargs: Unpack[PutItemCall]) -> None:
        item = kwargs["Item"]
        key = (item[self.key_names[0]], item[self.key_names[1]])
        super().put_item(**kwargs)
        self.items[key] = dict(item)


def repository_schema(partition_type: str = "S", sort_type: str = "S") -> TableSchema:
    return TableSchema.from_mapping(
        {
            "partition_key": {"name": "partition", "type": partition_type},
            "sort_key": {"name": "ordering", "type": sort_type},
            "ttl": {"attribute_name": "expires_on", "retention_days": 30},
            "fields": [
                {"name": "partition", "type": partition_type},
                {"name": "ordering", "type": sort_type},
                {"name": "expires_on", "type": "N"},
                {"name": "reading", "type": "N"},
            ],
        }
    )


def observation() -> Observation:
    return Observation(
        provider="example",
        dataset="sample",
        subject="item",
        observed_at=datetime(2026, 9, 8, 12, 0, tzinfo=UTC),
        values={"reading": Decimal("2.5")},
    )


def test_repository_writes_configured_keys_ttl_without_condition() -> None:
    table = RecordingTable()
    schema = repository_schema()
    repository = ObservationRepository(table, 30, schema)

    assert repository.put(observation()) is True

    call = table.calls[0]
    item = call["Item"]
    assert item["partition"] == observation().pk
    assert item["ordering"] == observation().sk
    expected_expiry = 1791460800
    assert item["expires_on"] == Decimal(expected_expiry)
    assert "pk" not in item
    assert "sk" not in item
    assert "expires_at" not in item
    assert item == {
        "partition": observation().pk,
        "ordering": observation().sk,
        "expires_on": Decimal(expected_expiry),
        "reading": Decimal("2.5"),
    }
    assert set(call) == {"Item"}


@pytest.mark.parametrize("partition_type", ["S", "N", "B"])
@pytest.mark.parametrize(
    ("source_time", "seconds"),
    [
        ("2026-09-08T12:00:00+00:00", "1788868800"),
        ("2026-09-08T14:00:00+02:00", "1788868800"),
        ("1970-01-01T00:00:00+00:00", "0"),
        ("1969-12-31T23:59:59+00:00", "-1"),
        ("9998-01-01T00:00:01+00:00", "253339228801"),
    ],
)
def test_numeric_sort_key_is_exact_source_unix_seconds(
    partition_type: str, source_time: str, seconds: str
) -> None:
    table = RecordingTable()
    repository = ObservationRepository(table, 30, repository_schema(partition_type, "N"))

    assert (
        repository.put(replace(observation(), observed_at=datetime.fromisoformat(source_time)))
        is True
    )

    key = table.calls[0]["Item"]["ordering"]
    assert isinstance(key, Decimal)
    assert key == Decimal(seconds)
    assert key.as_tuple().exponent == 0


@pytest.mark.parametrize(
    "code", ["ProvisionedThroughputExceededException", "ConditionalCheckFailedException"]
)
def test_repository_propagates_client_error(code: str) -> None:
    error = ClientError(
        {"Error": {"Code": code, "Message": "synthetic failure"}},
        "PutItem",
    )
    repository = ObservationRepository(RecordingTable(error), 30, repository_schema())
    snapshot = observation()

    with pytest.raises(ClientError) as actual_error:
        repository.put(snapshot)

    assert actual_error.value is error


def test_repository_rejects_attribute_conflicting_with_row_field() -> None:
    table = RecordingTable()
    repository = ObservationRepository(table, 30, repository_schema())
    conflicting = replace(observation(), values={"partition": "override"})

    with pytest.raises(ValueError, match="conflicts with row attribute"):
        repository.put(conflicting)

    assert table.calls == []


@pytest.mark.parametrize(
    ("attribute_type", "expected"),
    [("S", "partition"), ("B", b"partition")],
)
def test_numeric_partition_key_path_supports_string_and_binary_keys(
    attribute_type: str, expected: str | bytes
) -> None:
    typed_attribute = cast(DynamoDBAttributeType, attribute_type)
    key = ObservationRepository._key_value(
        AttributeSchema("partition", typed_attribute),
        observation(),
        "partition",
        partition=True,
        numeric_partition=True,
    )

    assert key == expected


def test_repository_rejects_invalid_row_before_write() -> None:
    table = RecordingTable()
    repository = ObservationRepository(table, 30, repository_schema())
    invalid_observation = replace(
        observation(), values=cast(dict[str, Decimal | str | bytes], {"reading": 1.5})
    )

    try:
        repository.put(invalid_observation)
    except ValueError as error:
        assert "DynamoDB-compatible" in str(error)
    else:
        raise AssertionError("Expected invalid row to be rejected")

    assert table.calls == []


def test_repository_rejects_retention_mismatch_before_write() -> None:
    table = RecordingTable()

    try:
        ObservationRepository(table, 90, repository_schema())
    except ValueError as error:
        assert "retention_days" in str(error)
    else:
        raise AssertionError("Expected retention mismatch to be rejected")


def test_weather_repeats_and_revisions_replace_snapshot_at_source_time() -> None:
    table = ReplacingRecordingTable(("date", "ts"))
    config = yaml.safe_load((Path(__file__).parents[1] / "fixtures" / "config.yaml").read_text())
    storage = config["storage"]
    schema = {key: storage[key] for key in ("partition_key", "sort_key", "ttl")}
    schema["fields"] = [
        storage["partition_key"],
        storage["sort_key"],
        {"name": "expts", "type": "N"},
        {"name": "temperature", "type": "N"},
        {"name": "apparent", "type": "N"},
    ]
    repository = ObservationRepository(table, 90, TableSchema.from_mapping(schema))
    provider = OpenMeteoProvider(
        "https://api.open-meteo.com",
        config["provider"]["options"],
    )
    payload = json.loads(
        (Path(__file__).parents[1] / "fixtures" / "open_meteo_response.json").read_text(),
        parse_float=Decimal,
    )
    observations = provider.normalize(payload, datetime(2026, 10, 9, 13, tzinfo=UTC))

    results = [repository.put(item) for item in observations]
    repeat_results = [repository.put(item) for item in observations]
    key = (Decimal("20261009"), Decimal("1791547200"))
    table.items[key]["obsolete"] = "remove on whole-item replacement"
    revised_payload = {**payload, "current": {**payload["current"]}}
    revised_payload["current"]["temperature_2m"] = Decimal("99")
    revised = provider.normalize(revised_payload, datetime(2026, 10, 9, 14, tzinfo=UTC))
    revised_results = [repository.put(item) for item in revised]

    assert results == [True]
    assert repeat_results == revised_results == [True]
    items = [call["Item"] for call in table.calls]
    assert {item["date"] for item in items} == {Decimal("20261009")}
    assert {item["ts"] for item in items} == {Decimal("1791547200")}
    assert len({item["expts"] for item in items}) == 1
    assert items[0]["temperature"] == Decimal("20.25")
    assert items[0]["apparent"] == Decimal("19.5")
    assert set(items[0]) == {"date", "ts", "expts", "temperature", "apparent"}
    assert table.items == {key: items[-1]}
    assert table.items[key]["temperature"] == Decimal("99")
    assert "obsolete" not in table.items[key]


@pytest.mark.parametrize("partition_type,sort_type", tuple(product("SNB", repeat=2)))
def test_same_time_streams_collide_only_with_numeric_partition_and_sort_keys(
    partition_type: str, sort_type: str
) -> None:
    table = ReplacingRecordingTable(("partition", "ordering"))
    repository = ObservationRepository(table, 30, repository_schema(partition_type, sort_type))
    observed_at = datetime(2026, 9, 8, 12, 0, tzinfo=UTC)
    observations = [
        replace(observation(), subject="first", observed_at=observed_at),
        replace(
            observation(),
            subject="second",
            observed_at=observed_at,
            values={"reading": Decimal("99")},
        ),
    ]

    results = [repository.put(item) for item in observations]
    repeat_results = [repository.put(item) for item in observations]

    assert results == [True, True]
    assert repeat_results == [True, True]
    items = [call["Item"] for call in table.calls]
    keys = {(item["partition"], item["ordering"]) for item in items}
    expected_count = 1 if partition_type == sort_type == "N" else 2
    assert len(keys) == len(table.items) == expected_count
    if expected_count == 1:
        assert next(iter(table.items.values()))["reading"] == Decimal("99")


@pytest.mark.parametrize("partition_type,sort_type", tuple(product("SNB", repeat=2)))
def test_fractional_source_time_rejected_only_for_numeric_sort_keys(
    partition_type: str, sort_type: str
) -> None:
    table = RecordingTable()
    repository = ObservationRepository(table, 30, repository_schema(partition_type, sort_type))
    snapshot = replace(observation(), observed_at=observation().observed_at.replace(microsecond=1))

    if sort_type == "N":
        with pytest.raises(ValueError, match="whole-second source time"):
            repository.put(snapshot)
        assert table.calls == []
    else:
        assert repository.put(snapshot) is True


def test_older_source_time_uses_different_numeric_key() -> None:
    table = ReplacingRecordingTable(("partition", "ordering"))
    repository = ObservationRepository(table, 30, repository_schema("N", "N"))

    repository.put(observation())
    repository.put(
        replace(observation(), observed_at=observation().observed_at - timedelta(seconds=1))
    )

    assert set(table.items) == {
        (Decimal("20260908"), Decimal("1788868800")),
        (Decimal("20260908"), Decimal("1788868799")),
    }
