from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal


def _escape_key_component(value: str) -> str:
    return value.replace("#", "##")


@dataclass(frozen=True)
class Observation:
    """One selected-value snapshot with deterministic identity/source-time keys."""

    provider: str
    dataset: str
    subject: str
    observed_at: datetime
    values: dict[str, Decimal | str | bytes]

    def __post_init__(self) -> None:
        if self.observed_at.tzinfo is None or self.observed_at.utcoffset() is None:
            raise ValueError("Snapshot source time must be timezone-aware")
        object.__setattr__(self, "observed_at", self.observed_at.astimezone(UTC))
        if not all(
            isinstance(value, str) and value
            for value in (self.provider, self.dataset, self.subject)
        ):
            raise ValueError("Snapshot identity components must be non-empty strings")
        if not isinstance(self.values, dict) or not self.values:
            raise ValueError("Snapshot selected values must be a non-empty mapping")

    @property
    def pk(self) -> str:
        """Return the deterministic partition key for this observation."""
        return (
            f"PROVIDER#{_escape_key_component(self.provider)}"
            f"#DATASET#{_escape_key_component(self.dataset)}"
            f"#SUBJECT#{_escape_key_component(self.subject)}"
        )

    @property
    def sk(self) -> str:
        """Return the deterministic sort key for this observation."""
        return f"OBSERVED_AT#{self.observed_at.isoformat()}"
