import json
import os
from dataclasses import dataclass
from urllib.parse import urlsplit, urlunsplit

from scheduled_ingestor.core.storage import TableSchema


def load_provider_options(value: str) -> dict[str, object]:
    """Parse provider options from a JSON string.

    Args:
        value: JSON object containing provider-specific typed options.

    Returns:
        Parsed provider options.

    Raises:
        ValueError: If the JSON is not a mapping with string keys.
    """
    options = json.loads(value)
    if not isinstance(options, dict) or any(not isinstance(key, str) for key in options):
        raise ValueError("PROVIDER_OPTIONS_JSON must be a mapping with string keys")
    return options


def _normalize_api_base_url(value: str) -> str:
    parsed = urlsplit(value)
    return urlunsplit(parsed._replace(path=parsed.path.rstrip("/")))


@dataclass(frozen=True)
class Settings:
    """Runtime settings loaded from Lambda environment variables."""

    table_name: str
    provider_name: str
    api_base_url: str
    credential_parameter_name: str | None
    provider_options: dict[str, object]
    data_retention_days: int
    table_schema: TableSchema

    @classmethod
    def from_environment(cls) -> "Settings":
        """Load and validate runtime settings from environment variables.

        Returns:
            Runtime settings for the configured provider and table.

        Raises:
            KeyError: If a required environment variable is missing.
            ValueError: If an environment value is malformed.
        """
        parameter = os.environ.get("CREDENTIAL_PARAMETER_NAME") or None
        schema_json = os.environ["STORAGE_SCHEMA_JSON"]
        if not schema_json:
            raise ValueError("STORAGE_SCHEMA_JSON must not be empty")
        table_schema = TableSchema.from_json(schema_json)
        options = load_provider_options(os.environ["PROVIDER_OPTIONS_JSON"])
        table_schema.validate_selections(options.get("measurements"))
        return cls(
            table_name=os.environ["TABLE_NAME"],
            provider_name=os.environ["PROVIDER_NAME"],
            api_base_url=_normalize_api_base_url(os.environ["API_BASE_URL"]),
            credential_parameter_name=parameter,
            provider_options=options,
            data_retention_days=table_schema.ttl.retention_days,
            table_schema=table_schema,
        )
