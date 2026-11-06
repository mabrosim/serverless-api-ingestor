import json
from datetime import UTC, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from unittest.mock import Mock, call
from urllib.parse import parse_qs, urlsplit

import pytest

from scheduled_ingestor.providers.open_meteo_provider import OpenMeteoProvider

METRICS = ("temperature_2m", "apparent_temperature", "relative_humidity_2m")
OPTIONS = {
    "latitude": "0.0",
    "longitude": "0.0",
    "site": "example-site",
    "measurements": [
        {"field": "temperature_2m", "name": "temperature", "type": "N", "unit": "degC"},
        {"field": "apparent_temperature", "name": "apparent", "type": "N", "unit": "degC"},
        {"field": "relative_humidity_2m", "name": "humidity", "type": "N", "unit": "%"},
    ],
}
FIXTURE_PATH = Path(__file__).parents[2] / "fixtures" / "open_meteo_response.json"
INGESTION_TIME = datetime(2026, 10, 9, 13, tzinfo=UTC)


@pytest.fixture
def payload() -> dict:
    return json.loads(FIXTURE_PATH.read_text(), parse_float=Decimal)


@pytest.fixture
def provider() -> OpenMeteoProvider:
    return OpenMeteoProvider("https://api.open-meteo.com", OPTIONS)


def test_fetch_requests_current_weather_without_credentials() -> None:
    client = Mock()
    provider = OpenMeteoProvider("https://api.open-meteo.com/", OPTIONS)

    provider.fetch(client, "UNUSED_SYNTHETIC_CREDENTIAL")

    client.get_json.assert_called_once()
    assert client.get_json.call_args.kwargs == {}
    url = urlsplit(client.get_json.call_args.args[0])
    assert url.path == "/v1/forecast"
    assert parse_qs(url.query) == {
        "latitude": ["0.0"],
        "longitude": ["0.0"],
        "current": ["temperature_2m,apparent_temperature,relative_humidity_2m"],
        "timezone": ["UTC"],
    }


def test_normalize_uses_source_time_for_two_decimal_metrics(payload: dict) -> None:
    provider = OpenMeteoProvider("https://api.open-meteo.com", OPTIONS)

    observations = provider.normalize(payload, datetime(2026, 10, 9, 13, tzinfo=UTC))

    assert len(observations) == 1
    assert observations[0].values == {
        "temperature": Decimal("20.25"),
        "apparent": Decimal("19.5"),
        "humidity": Decimal("60"),
    }
    assert {item.observed_at for item in observations} == {datetime(2026, 10, 9, 12, tzinfo=UTC)}
    assert observations[0].subject == "example-site"


@pytest.mark.parametrize("metric", METRICS)
@pytest.mark.parametrize(
    "reading", [None, True, False, "20.1", [], {}, 20.1, Decimal("NaN"), Decimal("Infinity")]
)
def test_normalize_rejects_invalid_readings(
    payload: dict, metric: str, reading: object, provider: OpenMeteoProvider
) -> None:
    payload["current"][metric] = reading

    with pytest.raises(ValueError, match="reading"):
        provider.normalize(payload, INGESTION_TIME)


@pytest.mark.parametrize("metric", METRICS)
def test_normalize_rejects_missing_reading(
    payload: dict, metric: str, provider: OpenMeteoProvider
) -> None:
    del payload["current"][metric]

    with pytest.raises(ValueError, match="reading"):
        provider.normalize(payload, INGESTION_TIME)


@pytest.mark.parametrize("metric", METRICS)
@pytest.mark.parametrize("unit", [None, "K", "degC", True])
def test_normalize_rejects_invalid_units(
    payload: dict, metric: str, unit: object, provider: OpenMeteoProvider
) -> None:
    payload["current_units"][metric] = unit

    with pytest.raises(ValueError, match="current_units"):
        provider.normalize(payload, INGESTION_TIME)


@pytest.mark.parametrize(
    "value",
    [
        None,
        "2026-10-09",
        "2026-10-09T12:00+01:00",
        "2026-10-09T12:00:00.5Z",
        "2026-02-30T12:00",
        True,
    ],
)
def test_normalize_rejects_invalid_source_time(
    payload: dict, value: object, provider: OpenMeteoProvider
) -> None:
    payload["current"]["time"] = value

    with pytest.raises(ValueError, match=r"current\.time"):
        provider.normalize(payload, INGESTION_TIME)


@pytest.mark.parametrize(
    "key,value",
    [
        ("timezone", None),
        ("timezone", "Europe/London"),
        ("utc_offset_seconds", None),
        ("utc_offset_seconds", True),
        ("utc_offset_seconds", 3600),
    ],
)
def test_normalize_requires_explicit_utc_metadata(
    payload: dict, key: str, value: object, provider: OpenMeteoProvider
) -> None:
    payload[key] = value

    with pytest.raises(ValueError, match="UTC response metadata"):
        provider.normalize(payload, INGESTION_TIME)


@pytest.mark.parametrize("payload", [None, [], {}, {"current": [], "current_units": {}}])
def test_normalize_rejects_malformed_payload(payload: object, provider: OpenMeteoProvider) -> None:
    with pytest.raises(ValueError):
        provider.normalize(payload, INGESTION_TIME)


@pytest.mark.parametrize(
    "key,value",
    [
        ("latitude", "90.1"),
        ("latitude", "-90.1"),
        ("longitude", "180.1"),
        ("longitude", "-180.1"),
        ("latitude", "NaN"),
        ("latitude", "1e1"),
        ("longitude", "Infinity"),
        ("latitude", True),
        ("site", ""),
        ("site", "bad site"),
        ("unknown", "value"),
    ],
)
def test_options_reject_invalid_values(key: str, value: object) -> None:
    with pytest.raises(ValueError, match=r"provider\.options"):
        OpenMeteoProvider("https://api.open-meteo.com", {**OPTIONS, key: value})


@pytest.mark.parametrize("coordinate", ["latitude", "longitude"])
def test_coordinate_conversion_failure_preserves_cause_and_field_error(
    monkeypatch: pytest.MonkeyPatch, coordinate: str
) -> None:
    failure = InvalidOperation("synthetic conversion failure")
    conversions = [failure] if coordinate == "latitude" else [Decimal("0.0"), failure]
    converter = Mock(side_effect=conversions)
    monkeypatch.setattr("scheduled_ingestor.providers.open_meteo_provider.Decimal", converter)

    with pytest.raises(ValueError) as caught:
        OpenMeteoProvider("https://api.example.com", {**OPTIONS, coordinate: "1.25"})

    assert str(caught.value) == f"provider.options.{coordinate} must be a decimal string"
    assert caught.value.__cause__ is failure
    expected_calls = [call("1.25")] if coordinate == "latitude" else [call("0.0"), call("1.25")]
    assert converter.call_args_list == expected_calls


@pytest.mark.parametrize(
    "base_url",
    [
        "http://api.example.com",
        "https:///forecast",
        "https://example@api.example.com",
        "https://api.example.com?field=value",
        "https://api.example.com#fragment",
    ],
)
def test_provider_rejects_unsafe_or_ambiguous_base_url(base_url: str) -> None:
    with pytest.raises(
        ValueError,
        match="Open-Meteo base URL must be HTTPS without credentials, query, or fragment",
    ):
        OpenMeteoProvider(base_url, OPTIONS)


@pytest.mark.parametrize("name", ["", "bad name", "/reading", "a" * 256])
def test_measurements_reject_invalid_output_names_without_mutating_options(name: str) -> None:
    measurement = {**OPTIONS["measurements"][0], "name": name}
    options = {**OPTIONS, "measurements": [measurement]}

    with pytest.raises(ValueError, match="Invalid measurement output name"):
        OpenMeteoProvider("https://api.example.com", options)

    assert options["measurements"] == [measurement]
    assert measurement["name"] == name
    assert OPTIONS["measurements"][0]["name"] == "temperature"


@pytest.mark.parametrize("latitude,longitude", [("90", "180"), ("-90", "-180")])
def test_options_accept_coordinate_boundaries(latitude: str, longitude: str) -> None:
    provider = OpenMeteoProvider(
        "https://api.open-meteo.com", {**OPTIONS, "latitude": latitude, "longitude": longitude}
    )

    assert provider.latitude == Decimal(latitude)
    assert provider.longitude == Decimal(longitude)


def test_selected_humidity_requests_and_stores_no_extra_fields(payload: dict) -> None:
    selected = {**OPTIONS, "measurements": [OPTIONS["measurements"][-1]]}
    provider = OpenMeteoProvider("https://api.open-meteo.com", selected)
    client = Mock()
    provider.fetch(client, None)
    assert parse_qs(urlsplit(client.get_json.call_args.args[0]).query)["current"] == [
        "relative_humidity_2m"
    ]
    assert provider.normalize(payload, INGESTION_TIME)[0].values == {"humidity": Decimal(60)}


@pytest.mark.parametrize(
    "measurements",
    [
        [],
        [None],
        [{"field": "unknown", "name": "x", "type": "N", "unit": "%"}],
        [{"field": "temperature_2m", "name": "x", "type": "S", "unit": "degC"}],
        [{"field": "relative_humidity_2m", "name": "x", "type": "N", "unit": "degC"}],
        [OPTIONS["measurements"][0], OPTIONS["measurements"][0]],
    ],
)
def test_measurements_reject_unsupported_mappings(measurements: object) -> None:
    with pytest.raises(ValueError):
        OpenMeteoProvider("https://api.open-meteo.com", {**OPTIONS, "measurements": measurements})
