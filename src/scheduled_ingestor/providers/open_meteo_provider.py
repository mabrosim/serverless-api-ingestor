import re
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from urllib.parse import urlencode, urlsplit, urlunsplit

from scheduled_ingestor.core.http import HttpClient
from scheduled_ingestor.core.models import Observation
from scheduled_ingestor.core.provider import BaseProvider

METRICS = {
    "temperature_2m": ("N", "degC", "\N{DEGREE SIGN}C"),
    "apparent_temperature": ("N", "degC", "\N{DEGREE SIGN}C"),
    "relative_humidity_2m": ("N", "%", "%"),
}
OPTION_KEYS = {"latitude", "longitude", "site", "measurements"}
TIME_PATTERN = re.compile(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(?::\d{2})?(?:Z|\+00:00)?", re.ASCII)


def _coordinate(value: object, name: str, limit: int) -> Decimal:
    if not isinstance(value, str) or re.fullmatch(r"[+-]?\d+(?:\.\d+)?", value, re.ASCII) is None:
        raise ValueError(f"provider.options.{name} must be a decimal string")
    try:
        coordinate = Decimal(value)
    except InvalidOperation as error:
        raise ValueError(f"provider.options.{name} must be a decimal string") from error
    if not coordinate.is_finite() or not -limit <= coordinate <= limit:
        raise ValueError(f"provider.options.{name} is outside its decimal range")
    return coordinate


def _source_time(payload: dict, units: dict, current: dict) -> datetime:
    offset = payload.get("utc_offset_seconds")
    if payload.get("timezone") not in ("UTC", "GMT") or type(offset) is not int or offset != 0:
        raise ValueError("Expected explicit UTC response metadata")
    timestamp = current.get("time")
    if units.get("time") != "iso8601" or not isinstance(timestamp, str):
        raise ValueError("Expected ISO8601 current.time")
    if TIME_PATTERN.fullmatch(timestamp) is None:
        raise ValueError("Expected UTC ISO8601 current.time")
    try:
        return datetime.fromisoformat(timestamp).replace(tzinfo=UTC)
    except ValueError as error:
        raise ValueError("Invalid current.time") from error


class OpenMeteoProvider(BaseProvider):
    """Credential-free adapter for Open-Meteo current weather."""

    def __init__(self, base_url: str, options: dict[str, object]) -> None:
        if set(options) != OPTION_KEYS:
            raise ValueError(
                "provider.options must contain only latitude, longitude, site, and measurements"
            )
        parsed = urlsplit(base_url)
        if (
            parsed.scheme != "https"
            or not parsed.netloc
            or parsed.username is not None
            or parsed.query
            or parsed.fragment
        ):
            raise ValueError(
                "Open-Meteo base URL must be HTTPS without credentials, query, or fragment"
            )
        self.base_url = base_url
        self.latitude = _coordinate(options["latitude"], "latitude", 90)
        self.longitude = _coordinate(options["longitude"], "longitude", 180)
        site = options["site"]
        if not isinstance(site, str) or re.fullmatch(r"[A-Za-z0-9_-]{1,64}", site) is None:
            raise ValueError("provider.options.site must be a short site identifier")
        self.site = site
        self.measurements = self._measurements(options["measurements"])

    @staticmethod
    def _measurements(value: object) -> list[dict[str, str]]:
        if not isinstance(value, list) or not value:
            raise ValueError("provider.options.measurements must be a non-empty list")
        for measurement in value:
            if (
                not isinstance(measurement, dict)
                or set(measurement) != {"field", "name", "type", "unit"}
                or any(not isinstance(item, str) for item in measurement.values())
            ):
                raise ValueError("Each measurement must declare field, name, type, and unit")
            field = measurement["field"]
            if (
                field not in METRICS
                or (measurement["type"], measurement["unit"]) != METRICS[field][:2]
            ):
                raise ValueError("Unsupported weather measurement field/type/unit")
            if re.fullmatch(r"[A-Za-z0-9_.-]{1,255}", measurement["name"]) is None:
                raise ValueError("Invalid measurement output name")
        for key in ("field", "name"):
            if len({item[key] for item in value}) != len(value):
                raise ValueError(f"Measurement {key} selections must be unique")
        return [dict(item) for item in value]

    def fetch(self, client: HttpClient, credential: str | None) -> object:
        """Fetch current weather without sending credentials.

        Args:
            client: Existing bounded HTTPS client.
            credential: Unused optional credential from the generic handler.

        Returns:
            Decoded provider response.
        """
        parsed = urlsplit(self.base_url)
        query = urlencode(
            {
                "latitude": str(self.latitude),
                "longitude": str(self.longitude),
                "current": ",".join(item["field"] for item in self.measurements),
                "timezone": "UTC",
            }
        )
        url = urlunsplit(
            parsed._replace(path=f"{parsed.path.rstrip('/')}/v1/forecast", query=query)
        )
        return client.get_json(url)

    def normalize(self, payload: object, observed_at: datetime) -> list[Observation]:
        """Normalize selected fields into one snapshot at UTC source time.

        Args:
            payload: Decoded Open-Meteo response.
            observed_at: Invocation time; not used as the observation time.

        Returns:
            One decimal-valued snapshot.

        Raises:
            ValueError: If readings, units, or UTC source metadata are invalid.
        """
        del observed_at
        if not isinstance(payload, dict):
            raise ValueError("Expected weather response object")
        current = payload.get("current")
        units = payload.get("current_units")
        if not isinstance(current, dict) or not isinstance(units, dict):
            raise ValueError("Expected current and current_units mappings")
        source_time = _source_time(payload, units, current)
        values = {}
        for measurement in self.measurements:
            metric = measurement["field"]
            value = current.get(metric)
            if isinstance(value, bool) or not isinstance(value, (int, Decimal)):
                raise ValueError(f"Invalid current.{metric} reading")
            reading = Decimal(value)
            if not reading.is_finite():
                raise ValueError(f"Invalid current.{metric} reading")
            if units.get(metric) != METRICS[metric][2]:
                raise ValueError(f"Invalid current_units.{metric}")
            values[measurement["name"]] = reading
        return [
            Observation(
                provider="open_meteo",
                dataset="current-weather",
                subject=self.site,
                observed_at=source_time,
                values=values,
            )
        ]
