import json
import os
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from unittest.mock import Mock

import boto3
import pytest
import yaml
from moto import mock_aws

from scheduled_ingestor.core.http import HttpClient
from scheduled_ingestor.core.ingestion import IngestionService
from scheduled_ingestor.core.models import Observation
from scheduled_ingestor.core.provider import BaseProvider
from scheduled_ingestor.core.repository import ObservationRepository
from scheduled_ingestor.core.storage import TableSchema
from scheduled_ingestor.providers.open_meteo_provider import OpenMeteoProvider


class ExampleProvider(BaseProvider):
    def fetch(self, client: object, credential: str | None) -> object:
        return {"sample": "value"}

    def normalize(self, payload: object, observed_at: datetime) -> list[Observation]:
        return [
            Observation(
                provider="example",
                dataset="sample",
                subject="item",
                observed_at=observed_at,
                values={"reading": Decimal("1")},
            )
        ]


def test_provider_abc_accepts_complete_independent_adapter() -> None:
    provider = ExampleProvider()

    assert isinstance(provider, BaseProvider)
    assert provider.readiness_credential() is None


class MissingFetchProvider(BaseProvider):
    def normalize(self, payload: object, observed_at: datetime) -> list[Observation]:
        return []


class MissingNormalizeProvider(BaseProvider):
    def fetch(self, client: object, credential: str | None) -> object:
        return {}


@pytest.mark.parametrize(
    ("provider_class", "missing_method"),
    [
        (BaseProvider, "fetch"),
        (MissingFetchProvider, "fetch"),
        (MissingNormalizeProvider, "normalize"),
    ],
)
def test_provider_abc_rejects_incomplete_adapter(
    provider_class: type[BaseProvider], missing_method: str
) -> None:
    with pytest.raises(TypeError, match=missing_method):
        provider_class()


class StructuralProvider:
    def fetch(self, client: object, credential: str | None) -> object:
        return {}

    def normalize(self, payload: object, observed_at: datetime) -> list[Observation]:
        return []


def test_provider_abc_rejects_legacy_structural_adapter() -> None:
    assert not isinstance(StructuralProvider(), BaseProvider)


@mock_aws
def test_weather_provider_contract() -> None:
    provider = OpenMeteoProvider(
        "https://api.open-meteo.com",
        yaml.safe_load((Path(__file__).parents[1] / "fixtures" / "config.yaml").read_text())[
            "provider"
        ]["options"],
    )
    payload = json.loads(
        (Path(__file__).parents[1] / "fixtures" / "open_meteo_response.json").read_text(),
        parse_float=Decimal,
    )
    observations = provider.normalize(payload, datetime(2026, 10, 9, 13, tzinfo=UTC))
    assert isinstance(provider, BaseProvider)
    assert provider.readiness_credential() is None
    assert len(observations) == 1
    assert observations[0].values == {"temperature": Decimal("20.25"), "apparent": Decimal("19.5")}
    assert observations[0].observed_at == datetime(2026, 10, 9, 12, tzinfo=UTC)
    for observation in observations:
        assert observation.provider
        assert observation.dataset
        assert observation.subject
        assert observation.observed_at.tzinfo is not None
        assert observation.pk
        assert observation.sk

    table = boto3.resource("dynamodb").create_table(
        TableName="example-weather",
        KeySchema=[
            {"AttributeName": "date", "KeyType": "HASH"},
            {"AttributeName": "ts", "KeyType": "RANGE"},
        ],
        AttributeDefinitions=[
            {"AttributeName": "date", "AttributeType": "N"},
            {"AttributeName": "ts", "AttributeType": "N"},
        ],
        BillingMode="PAY_PER_REQUEST",
    )
    schema = TableSchema.from_mapping(json.loads(os.environ["STORAGE_SCHEMA_JSON"]))
    repository = ObservationRepository(table, 90, schema)
    http = Mock(spec=HttpClient)
    http.get_json.return_value = payload
    service = IngestionService(provider, http, repository)
    expected_result = {"status": "ok", "written": 1, "duplicates": 0}
    key = {"date": Decimal("20261009"), "ts": Decimal("1791547200")}

    assert service.run() == expected_result
    original = table.get_item(Key=key)["Item"]
    assert service.run() == expected_result
    assert table.get_item(Key=key)["Item"] == original

    table.put_item(Item={**original, "obsolete": "remove on replacement"})
    payload["current"]["temperature_2m"] = Decimal("99")
    assert service.run() == expected_result
    assert table.get_item(Key=key)["Item"] == {**original, "temperature": Decimal("99")}

    payload["current"]["time"] = "2026-10-09T11:59:59"
    assert service.run() == expected_result
    older_key = {"date": Decimal("20261009"), "ts": Decimal("1791547199")}
    older = table.get_item(Key=older_key)["Item"]
    assert older == {
        **original,
        **older_key,
        "temperature": Decimal("99"),
        "expts": original["expts"] - 1,
    }
    assert table.get_item(Key=key)["Item"]["temperature"] == Decimal("99")
