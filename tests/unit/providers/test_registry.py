import json
import os

import pytest

from scheduled_ingestor import registry
from scheduled_ingestor.core.models import Observation
from scheduled_ingestor.core.provider import BaseProvider
from scheduled_ingestor.providers.open_meteo_provider import OpenMeteoProvider


def test_create_provider_uses_weather_factory() -> None:
    provider = registry.create_provider(
        "open_meteo",
        "https://api.open-meteo.com",
        json.loads(os.environ["PROVIDER_OPTIONS_JSON"]),
    )

    assert isinstance(provider, OpenMeteoProvider)


def test_create_provider_rejects_removed_crypto_provider() -> None:
    with pytest.raises(ValueError, match="Unknown provider: crypto"):
        registry.create_provider("crypto", "https://api.example.com", {})


class ExampleProvider(BaseProvider):
    def __init__(self, base_url: str, options: dict[str, str]) -> None:
        self.base_url = base_url
        self.options = options

    def fetch(self, client: object, credential: str | None) -> object:
        return {"sample": "value"}

    def normalize(self, payload: object, observed_at: object) -> list[Observation]:
        return []


def test_create_provider_uses_registered_provider_factory(monkeypatch: object) -> None:
    monkeypatch.setitem(registry.PROVIDER_FACTORIES, "example", ExampleProvider)

    provider = registry.create_provider("example", "https://api.example.com", {"region": "test"})

    assert isinstance(provider, ExampleProvider)
    assert provider.base_url == "https://api.example.com"
    assert provider.options == {"region": "test"}


def test_create_provider_rejects_unregistered_provider() -> None:
    with pytest.raises(ValueError, match="Unknown provider: missing"):
        registry.create_provider("missing", "https://api.example.com", {})


def test_create_provider_propagates_registered_factory_key_error(monkeypatch: object) -> None:
    def broken_factory(base_url: str, options: dict[str, str]) -> ExampleProvider:
        raise KeyError("factory failure")

    monkeypatch.setitem(registry.PROVIDER_FACTORIES, "broken", broken_factory)

    with pytest.raises(KeyError, match="factory failure"):
        registry.create_provider("broken", "https://api.example.com", {})
