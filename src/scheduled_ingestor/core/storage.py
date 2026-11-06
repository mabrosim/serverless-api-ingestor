import json
import re
from collections.abc import Mapping
from dataclasses import dataclass
from decimal import Decimal, DecimalException
from typing import Literal, cast

from boto3.dynamodb.types import Binary, TypeSerializer

DynamoDBAttributeType = Literal["S", "N", "B"]
DYNAMODB_ATTRIBUTE_TYPES = frozenset({"S", "N", "B"})
ATTRIBUTE_NAME_PATTERN = re.compile(r"[A-Za-z0-9_.-]{1,255}")
SCHEMA_KEYS = frozenset({"partition_key", "sort_key", "ttl", "fields"})
KEY_KEYS = frozenset({"name", "type"})
TTL_KEYS = frozenset({"attribute_name", "retention_days"})


@dataclass(frozen=True)
class AttributeSchema:
    """Describe one DynamoDB attribute and its configured type."""

    name: str
    type: DynamoDBAttributeType

    @classmethod
    def from_mapping(cls, value: object, label: str) -> "AttributeSchema":
        """Build an attribute schema from validated configuration data.

        Args:
            value: Candidate attribute mapping.
            label: Configuration path used in validation errors.

        Returns:
            Parsed attribute schema.

        Raises:
            ValueError: If the mapping or attribute values are invalid.
        """
        if not isinstance(value, Mapping) or set(value) != KEY_KEYS:
            raise ValueError(f"{label} must contain only name and type")
        name = value["name"]
        attribute_type = value["type"]
        if not isinstance(name, str) or not ATTRIBUTE_NAME_PATTERN.fullmatch(name):
            raise ValueError(f"{label}.name is invalid")
        if not isinstance(attribute_type, str) or attribute_type not in DYNAMODB_ATTRIBUTE_TYPES:
            raise ValueError(f"{label}.type must be one of {sorted(DYNAMODB_ATTRIBUTE_TYPES)}")
        return cls(name=name, type=cast(DynamoDBAttributeType, attribute_type))


@dataclass(frozen=True)
class TtlSchema:
    """Describe the table TTL attribute and retention period."""

    attribute_name: str
    retention_days: int

    @classmethod
    def from_mapping(cls, value: object) -> "TtlSchema":
        """Build a TTL schema from configuration data.

        Args:
            value: Candidate TTL mapping.

        Returns:
            Parsed TTL schema.

        Raises:
            ValueError: If the mapping or retention period is invalid.
        """
        if not isinstance(value, Mapping) or set(value) != TTL_KEYS:
            raise ValueError("storage.ttl must contain only attribute_name and retention_days")
        attribute_name = value["attribute_name"]
        retention_days = value["retention_days"]
        if not isinstance(attribute_name, str) or not ATTRIBUTE_NAME_PATTERN.fullmatch(
            attribute_name
        ):
            raise ValueError("storage.ttl.attribute_name is invalid")
        if (
            not isinstance(retention_days, int)
            or isinstance(retention_days, bool)
            or not 1 <= retention_days <= 3650
        ):
            raise ValueError("storage.ttl.retention_days must be 1 through 3650")
        return cls(attribute_name=attribute_name, retention_days=retention_days)


@dataclass(frozen=True)
class TableSchema:
    """Describe the configured DynamoDB keys, TTL, and declared fields."""

    partition_key: AttributeSchema
    sort_key: AttributeSchema
    ttl: TtlSchema
    fields: tuple[AttributeSchema, ...]

    @classmethod
    def from_mapping(cls, value: object) -> "TableSchema":
        """Build a table schema from a configuration mapping.

        Args:
            value: Candidate table schema mapping.

        Returns:
            Parsed and validated table schema.

        Raises:
            ValueError: If the schema definition is invalid.
        """
        if not isinstance(value, Mapping) or set(value) != SCHEMA_KEYS:
            raise ValueError(f"storage schema must contain only {sorted(SCHEMA_KEYS)}")
        partition_key = AttributeSchema.from_mapping(
            value["partition_key"], "storage.partition_key"
        )
        sort_key = AttributeSchema.from_mapping(value["sort_key"], "storage.sort_key")
        ttl = TtlSchema.from_mapping(value["ttl"])
        fields_value = value["fields"]
        if not isinstance(fields_value, list) or not fields_value:
            raise ValueError("storage.fields must be a non-empty list")
        fields = tuple(
            AttributeSchema.from_mapping(field, f"storage.fields[{index}]")
            for index, field in enumerate(fields_value)
        )
        schema = cls(partition_key=partition_key, sort_key=sort_key, ttl=ttl, fields=fields)
        schema._validate_definition()
        return schema

    @classmethod
    def from_json(cls, value: str) -> "TableSchema":
        """Build a table schema from its compact JSON representation.

        Args:
            value: JSON-encoded table schema.

        Returns:
            Parsed and validated table schema.

        Raises:
            ValueError: If the JSON or schema definition is invalid.
        """
        try:
            payload = json.loads(value)
        except json.JSONDecodeError as error:
            raise ValueError("STORAGE_SCHEMA_JSON must contain valid JSON") from error
        try:
            return cls.from_mapping(payload)
        except ValueError as error:
            raise ValueError(f"Invalid storage schema: {error}") from error

    def to_mapping(self) -> dict[str, object]:
        """Return the schema in configuration-mapping form.

        Returns:
            JSON-compatible schema mapping.
        """
        return {
            "partition_key": {"name": self.partition_key.name, "type": self.partition_key.type},
            "sort_key": {"name": self.sort_key.name, "type": self.sort_key.type},
            "ttl": {
                "attribute_name": self.ttl.attribute_name,
                "retention_days": self.ttl.retention_days,
            },
            "fields": [{"name": field.name, "type": field.type} for field in self.fields],
        }

    def to_json(self) -> str:
        """Return the schema as compact JSON.

        Returns:
            Compact JSON representation of the schema.
        """
        return json.dumps(self.to_mapping(), separators=(",", ":"))

    @property
    def field_types(self) -> dict[str, DynamoDBAttributeType]:
        """Return configured field names mapped to DynamoDB types."""
        return {field.name: field.type for field in self.fields}

    def validate_row(self, row: Mapping[str, object]) -> None:
        """Validate a row against configured fields, keys, and TTL.

        Args:
            row: Candidate DynamoDB item.

        Raises:
            ValueError: If the row contains invalid, missing, or unexpected values.
        """
        if not isinstance(row, Mapping):
            raise ValueError("row must be a mapping")
        if any(not isinstance(name, str) for name in row):
            raise ValueError("row attribute names must be strings")

        required = set(self.field_types)
        missing = required - set(row)
        if missing:
            raise ValueError(f"row is missing required attributes: {sorted(missing)}")

        allowed = required
        unexpected = set(row) - allowed
        if unexpected:
            raise ValueError(f"row contains unexpected attributes: {sorted(unexpected)}")

        for name, value in row.items():
            self._validate_dynamodb_value(name, value)
        self._validate_attribute(row, self.partition_key)
        self._validate_attribute(row, self.sort_key)
        for field in self.fields:
            if field.name in row:
                self._validate_attribute(row, field)
        self._validate_ttl(row[self.ttl.attribute_name])

    def validate_selections(self, measurements: object) -> None:
        """Require runtime selections to match all non-key/TTL schema fields.

        Args:
            measurements: Explicit provider measurement declarations.

        Raises:
            ValueError: If selections collide with core fields or differ from schema.
        """
        if not isinstance(measurements, list) or not measurements:
            raise ValueError("provider.options.measurements must be a non-empty list")
        core_names = {self.partition_key.name, self.sort_key.name, self.ttl.attribute_name}
        selected = []
        for item in measurements:
            declaration = (
                {"name": item.get("name"), "type": item.get("type")}
                if isinstance(item, Mapping)
                else item
            )
            selected.append(AttributeSchema.from_mapping(declaration, "selected measurement"))
        if len({item.name for item in selected}) != len(selected):
            raise ValueError("Selected field names must be unique")
        if any(item.name in core_names for item in selected):
            raise ValueError("Selected fields must not collide with keys or TTL")
        expected = {item.name: item.type for item in selected}
        actual = {name: value for name, value in self.field_types.items() if name not in core_names}
        if expected != actual:
            raise ValueError("Selected fields must match runtime storage schema")

    def _validate_definition(self) -> None:
        if self.partition_key.name == self.sort_key.name:
            raise ValueError("storage partition and sort key names must differ")
        if self.ttl.attribute_name in {self.partition_key.name, self.sort_key.name}:
            raise ValueError("storage.ttl.attribute_name must differ from key names")
        names = [field.name for field in self.fields]
        if len(set(names)) != len(names):
            raise ValueError("storage.fields names must be unique")
        field_types = self.field_types
        for key in (self.partition_key, self.sort_key):
            if field_types.get(key.name) != key.type:
                raise ValueError(f"storage.fields must declare {key.name} as type {key.type}")
        if field_types.get(self.ttl.attribute_name) != "N":
            raise ValueError(
                f"storage.fields must declare {self.ttl.attribute_name} as type N for TTL"
            )

    @staticmethod
    def _validate_dynamodb_value(name: str, value: object) -> None:
        try:
            TypeSerializer().serialize(value)
        except (KeyError, TypeError, ValueError, DecimalException) as error:
            raise ValueError(f"row.{name} is not a DynamoDB-compatible value") from error

    @staticmethod
    def _validate_attribute(row: Mapping[str, object], attribute: AttributeSchema) -> None:
        value = row[attribute.name]
        if attribute.type == "S":
            valid = isinstance(value, str) and bool(value)
        elif attribute.type == "N":
            valid = _is_finite_number(value)
        else:
            valid = isinstance(value, (Binary, bytes, bytearray)) and bool(value)
        if not valid:
            raise ValueError(f"row.{attribute.name} must be a non-empty DynamoDB {attribute.type}")

    @staticmethod
    def _validate_ttl(value: object) -> None:
        if not _is_finite_number(value) or value <= 0 or value != int(value):
            raise ValueError("row TTL must be a positive integer Unix timestamp")


def _is_finite_number(value: object) -> bool:
    if isinstance(value, bool) or not isinstance(value, (Decimal, int)):
        return False
    return Decimal(value).is_finite()
