import hashlib
from datetime import UTC, datetime, timedelta
from decimal import Decimal

from scheduled_ingestor.core.models import Observation
from scheduled_ingestor.core.storage import AttributeSchema, TableSchema

UNIX_EPOCH = datetime(1970, 1, 1, tzinfo=UTC)


class ObservationRepository:
    """Replace whole snapshots at equal schema-aware physical keys."""

    def __init__(self, table: object, retention_days: int, schema: TableSchema) -> None:
        self.table = table
        self.schema = schema
        if retention_days != self.schema.ttl.retention_days:
            raise ValueError("retention_days must match storage schema TTL retention_days")
        self.retention_days = self.schema.ttl.retention_days

    def put(self, observation: Observation) -> bool:
        """Insert or replace a whole observation using last-arrival-wins writes.

        Args:
            observation: Observation to persist.

        Returns:
            True on every successful insert or replacement, including unchanged repeats.

        Raises:
            ValueError: If the generated row violates the configured schema.
            ClientError: If DynamoDB fails.
        """
        expires_at = (
            observation.observed_at + timedelta(days=self.retention_days) - UNIX_EPOCH
        ) // timedelta(seconds=1)
        item = self._item(observation, expires_at)
        self.schema.validate_row(item)
        self.table.put_item(Item=item)
        return True

    def _item(self, observation: Observation, expires_at: int) -> dict[str, object]:
        item = {
            self.schema.partition_key.name: self._key_value(
                self.schema.partition_key,
                observation,
                observation.pk,
                partition=True,
                numeric_partition=self.schema.partition_key.type == "N",
            ),
            self.schema.sort_key.name: self._key_value(
                self.schema.sort_key,
                observation,
                observation.sk,
                partition=False,
                numeric_partition=self.schema.partition_key.type == "N",
            ),
            self.schema.ttl.attribute_name: Decimal(expires_at),
        }
        for name, value in observation.values.items():
            if name in item:
                raise ValueError(f"observation attribute conflicts with row attribute: {name}")
            item[name] = value
        return item

    @staticmethod
    def _key_value(
        attribute: AttributeSchema,
        observation: Observation,
        string_value: str,
        partition: bool,
        numeric_partition: bool,
    ) -> object:
        if attribute.type == "S" and not numeric_partition:
            return string_value
        if attribute.type == "B" and not numeric_partition:
            return string_value.encode()
        if partition:
            if attribute.type == "S":
                return string_value
            if attribute.type == "B":
                return string_value.encode()
            return Decimal(observation.observed_at.strftime("%Y%m%d"))
        if attribute.type == "N":
            elapsed = observation.observed_at - UNIX_EPOCH
            if elapsed.microseconds:
                raise ValueError("Numeric sort keys require whole-second source time")
            return Decimal(elapsed.days * 86400 + elapsed.seconds)
        identity = hashlib.sha256(f"{observation.pk}\0{observation.sk}".encode()).digest()
        if attribute.type == "S":
            return f"{string_value}#ID#{identity[:12].hex()}"
        return string_value.encode() + b"#ID#" + identity[:12]
