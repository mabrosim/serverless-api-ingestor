from copy import deepcopy
from decimal import Decimal

import pytest

from scheduled_ingestor.core.storage import (
    AttributeSchema,
    TableSchema,
    TtlSchema,
)

VALID_SCHEMA = {
    "partition_key": {"name": "day", "type": "N"},
    "sort_key": {"name": "sequence", "type": "S"},
    "ttl": {"attribute_name": "expires", "retention_days": 30},
    "fields": [
        {"name": "day", "type": "N"},
        {"name": "sequence", "type": "S"},
        {"name": "expires", "type": "N"},
        {"name": "reading", "type": "N"},
    ],
}


def test_table_schema_constructs_and_round_trips_runtime_shape() -> None:
    schema = TableSchema.from_mapping(VALID_SCHEMA)

    assert schema.partition_key.name == "day"
    assert schema.sort_key.type == "S"
    assert schema.ttl.attribute_name == "expires"
    assert schema.to_mapping() == VALID_SCHEMA
    assert TableSchema.from_json(schema.to_json()) == schema


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ({}, "contain only"),
        ({"name": "invalid name", "type": "S"}, "name is invalid"),
        ({"name": "value", "type": "X"}, "type must be one of"),
    ],
)
def test_attribute_schema_rejects_invalid_values(value: object, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        AttributeSchema.from_mapping(value, "storage.field")


@pytest.mark.parametrize(
    ("value", "message"),
    [
        ({}, "contain only"),
        ({"attribute_name": "invalid name", "retention_days": 30}, "attribute_name is invalid"),
        ({"attribute_name": "expires", "retention_days": 0}, "1 through 3650"),
    ],
)
def test_ttl_schema_rejects_invalid_values(value: object, message: str) -> None:
    with pytest.raises(ValueError, match=message):
        TtlSchema.from_mapping(value)


def test_table_schema_rejects_empty_fields() -> None:
    invalid_schema = deepcopy(VALID_SCHEMA)
    invalid_schema["fields"] = []

    with pytest.raises(ValueError, match="non-empty list"):
        TableSchema.from_mapping(invalid_schema)


@pytest.mark.parametrize("value", ["not-json", '{"partition_key": []}'])
def test_table_schema_rejects_invalid_json(value: str) -> None:
    try:
        TableSchema.from_json(value)
    except ValueError as error:
        assert "storage schema" in str(error) or "valid JSON" in str(error)
    else:
        raise AssertionError("Expected invalid JSON schema to be rejected")


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda value: value["fields"].pop(0), "must declare day"),
        (lambda value: value["fields"][2].update(type="S"), "must declare expires as type N"),
        (lambda value: value["sort_key"].update(name="day"), "must differ"),
        (lambda value: value.update(extra=True), "contain only"),
    ],
)
def test_table_schema_rejects_invalid_definition(change: object, message: str) -> None:
    invalid_schema = deepcopy(VALID_SCHEMA)
    change(invalid_schema)

    with pytest.raises(ValueError, match=message):
        TableSchema.from_mapping(invalid_schema)


def test_table_schema_allows_explicit_scalar_named_value() -> None:
    selected_schema = deepcopy(VALID_SCHEMA)
    selected_schema["fields"][-1]["name"] = "value"
    TableSchema.from_mapping(selected_schema).validate_row(
        {
            "day": Decimal(20260908),
            "sequence": "1",
            "expires": Decimal(1800000000),
            "value": Decimal("1.5"),
        }
    )


def test_table_schema_validates_configured_keys_and_declared_types() -> None:
    schema = TableSchema.from_mapping(VALID_SCHEMA)
    row = {
        "day": Decimal(20260908),
        "sequence": "1",
        "expires": Decimal(1_800_000_000),
        "reading": Decimal("1.5"),
    }

    schema.validate_row(row)


@pytest.mark.parametrize(
    ("change", "message"),
    [
        (lambda row: row.pop("day"), "missing required attributes"),
        (lambda row: row.pop("reading"), "missing required attributes"),
        (lambda row: row.update(unexpected="value"), "unexpected attributes"),
        (lambda row: row.update(day="20260908"), "row.day"),
        (lambda row: row.update(reading=1.5), "DynamoDB-compatible"),
        (lambda row: row.update(expires=Decimal("1.5")), "positive integer Unix timestamp"),
        (lambda row: row.update(pk="legacy"), "unexpected attributes"),
        (lambda row: row.update(reading=True), "row.reading"),
        (lambda row: row.update(reading=Decimal("NaN")), "DynamoDB-compatible"),
        (lambda row: row.update(reading=Decimal("Infinity")), "DynamoDB-compatible"),
        (lambda row: row.update(reading=Decimal("1e999")), "DynamoDB-compatible"),
    ],
)
def test_table_schema_rejects_invalid_row(change: object, message: str) -> None:
    schema = TableSchema.from_mapping(VALID_SCHEMA)
    row = {
        "day": Decimal(20260908),
        "sequence": "1",
        "expires": Decimal(1_800_000_000),
        "reading": Decimal("1.5"),
    }
    change(row)

    with pytest.raises(ValueError, match=message):
        schema.validate_row(row)


def test_table_schema_rejects_non_mapping_row() -> None:
    schema = TableSchema.from_mapping(VALID_SCHEMA)

    with pytest.raises(ValueError, match="row must be a mapping"):
        schema.validate_row([])


def test_table_schema_rejects_non_string_row_attribute_name() -> None:
    schema = TableSchema.from_mapping(VALID_SCHEMA)

    with pytest.raises(ValueError, match="attribute names must be strings"):
        schema.validate_row({1: "invalid"})


def test_table_schema_rejects_ttl_name_colliding_with_key() -> None:
    invalid_schema = deepcopy(VALID_SCHEMA)
    invalid_schema["ttl"]["attribute_name"] = "day"

    with pytest.raises(ValueError, match="differ from key names"):
        TableSchema.from_mapping(invalid_schema)


def test_table_schema_rejects_duplicate_field_names() -> None:
    invalid_schema = deepcopy(VALID_SCHEMA)
    invalid_schema["fields"][-1]["name"] = "day"

    with pytest.raises(ValueError, match="must be unique"):
        TableSchema.from_mapping(invalid_schema)


@pytest.mark.parametrize(
    "attribute_type,value", [("N", Decimal("1.5")), ("S", "clear"), ("B", b"reading")]
)
def test_selected_fields_support_declared_native_scalar_types(
    attribute_type: str,
    value: object,
) -> None:
    schema = deepcopy(VALID_SCHEMA)
    schema["fields"][-1]["type"] = attribute_type
    TableSchema.from_mapping(schema).validate_row(
        {
            "day": Decimal(20260908),
            "sequence": "1",
            "expires": Decimal(1800000000),
            "reading": value,
        }
    )


@pytest.mark.parametrize(
    "selections,message",
    [
        ([], "non-empty list"),
        ([{"name": "day", "type": "N"}], "collide"),
        ([{"name": "reading", "type": "N"}, {"name": "reading", "type": "N"}], "unique"),
    ],
)
def test_runtime_selections_reject_missing_or_colliding_fields(
    selections: object,
    message: str,
) -> None:
    schema = TableSchema.from_mapping(VALID_SCHEMA)
    with pytest.raises(ValueError, match=message):
        schema.validate_selections(selections)
