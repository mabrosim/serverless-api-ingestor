import importlib
import json
import logging
import os
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock

import pytest
from botocore.awsrequest import AWSPreparedRequest, AWSResponse
from botocore.httpsession import URLLib3Session

from scheduled_ingestor import registry
from scheduled_ingestor.core.budget import BudgetExceeded
from scheduled_ingestor.core.config import Settings
from scheduled_ingestor.core.models import Observation
from scheduled_ingestor.core.provider import BaseProvider


@pytest.fixture
def runtime_handler(monkeypatch: pytest.MonkeyPatch) -> object:
    monkeypatch.setenv("TABLE_NAME", "observations")
    monkeypatch.setenv("PROVIDER_NAME", "open_meteo")
    monkeypatch.setenv("API_BASE_URL", "https://api.open-meteo.com")
    monkeypatch.delenv("CREDENTIAL_PARAMETER_NAME", raising=False)
    monkeypatch.setattr("boto3.client", Mock())
    monkeypatch.setattr("boto3.resource", Mock())

    import scheduled_ingestor.handler as handler

    return importlib.reload(handler)


@pytest.fixture
def lambda_context() -> SimpleNamespace:
    return SimpleNamespace(
        function_name="test-handler",
        memory_limit_in_mb=128,
        invoked_function_arn="arn:aws:lambda:us-east-1:000000000000:function:test-handler",
        aws_request_id="test-request",
        get_remaining_time_in_millis=lambda: 20_000,
    )


class ExampleProvider(BaseProvider):
    def __init__(self) -> None:
        self.credential: str | None = None

    def fetch(self, client: object, credential: str | None) -> object:
        self.credential = credential
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


class RecordingRepository:
    def __init__(self) -> None:
        self.observations: list[Observation] = []

    def put(self, observation: Observation) -> bool:
        self.observations.append(observation)
        return True


def test_settings_rejects_provider_options_that_are_not_string_mapping(monkeypatch: object) -> None:
    monkeypatch.setenv("TABLE_NAME", "observations")
    monkeypatch.setenv("PROVIDER_NAME", "open_meteo")
    monkeypatch.setenv("API_BASE_URL", "https://api.example.com")
    monkeypatch.setenv("PROVIDER_OPTIONS_JSON", '["sample"]')

    with pytest.raises(ValueError, match="PROVIDER_OPTIONS_JSON"):
        Settings.from_environment()


def test_settings_removes_trailing_path_slash(monkeypatch: object) -> None:
    monkeypatch.setenv("TABLE_NAME", "observations")
    monkeypatch.setenv("PROVIDER_NAME", "open_meteo")
    monkeypatch.setenv("API_BASE_URL", "https://api.example.com/v1/")

    settings = Settings.from_environment()

    assert settings.api_base_url == "https://api.example.com/v1"


def test_settings_preserves_trailing_slash_in_query_value(monkeypatch: object) -> None:
    monkeypatch.setenv("TABLE_NAME", "observations")
    monkeypatch.setenv("PROVIDER_NAME", "open_meteo")
    monkeypatch.setenv("API_BASE_URL", "https://api.example.com?redirect=/")

    settings = Settings.from_environment()

    assert settings.api_base_url == "https://api.example.com?redirect=/"


def test_settings_loads_runtime_storage_schema_from_environment(monkeypatch: object) -> None:
    monkeypatch.setenv("TABLE_NAME", "observations")
    monkeypatch.setenv("PROVIDER_NAME", "open_meteo")
    monkeypatch.setenv("API_BASE_URL", "https://api.example.com")
    monkeypatch.setenv(
        "STORAGE_SCHEMA_JSON",
        json.dumps(
            {
                "partition_key": {"name": "day", "type": "N"},
                "sort_key": {"name": "sequence", "type": "S"},
                "ttl": {"attribute_name": "expires", "retention_days": 30},
                "fields": [
                    {"name": "day", "type": "N"},
                    {"name": "sequence", "type": "S"},
                    {"name": "expires", "type": "N"},
                    {"name": "temperature", "type": "N"},
                    {"name": "apparent", "type": "N"},
                ],
            }
        ),
    )

    settings = Settings.from_environment()

    assert settings.table_schema.partition_key.name == "day"
    assert settings.table_schema.sort_key.name == "sequence"
    assert settings.table_schema.ttl.attribute_name == "expires"
    assert settings.data_retention_days == 30


def test_settings_rejects_present_but_empty_runtime_storage_schema(monkeypatch: object) -> None:
    monkeypatch.setenv("TABLE_NAME", "observations")
    monkeypatch.setenv("PROVIDER_NAME", "open_meteo")
    monkeypatch.setenv("API_BASE_URL", "https://api.example.com")
    monkeypatch.setenv("STORAGE_SCHEMA_JSON", "")

    with pytest.raises(ValueError, match="STORAGE_SCHEMA_JSON"):
        Settings.from_environment()


def test_credential_is_loaded_once_and_cached(monkeypatch: object) -> None:
    monkeypatch.setenv("TABLE_NAME", "observations")
    monkeypatch.setenv("PROVIDER_NAME", "open_meteo")
    monkeypatch.setenv("API_BASE_URL", "https://api.example.com")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("CREDENTIAL_PARAMETER_NAME", raising=False)

    import scheduled_ingestor.handler as handler

    class FakeSsm:
        def __init__(self) -> None:
            self.calls = 0

        def get_parameter(self, **_: object) -> dict[str, dict[str, str]]:
            self.calls += 1
            return {"Parameter": {"Value": "EXAMPLE_SECRET"}}

    ssm = FakeSsm()
    monkeypatch.setattr(handler._ingestion, "credential_parameter_name", "/example/credential")
    monkeypatch.setattr(handler._ingestion, "ssm_client", ssm)
    monkeypatch.setattr(handler._ingestion, "_cached_credential", None)

    assert handler._ingestion._credential() == "EXAMPLE_SECRET"
    assert handler._ingestion._credential() == "EXAMPLE_SECRET"
    assert ssm.calls == 1


def test_lambda_handler_ingests_independent_provider(monkeypatch: object) -> None:
    monkeypatch.setenv("TABLE_NAME", "observations")
    monkeypatch.setenv("PROVIDER_NAME", "example")
    monkeypatch.setenv("API_BASE_URL", "https://api.example.com")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("CREDENTIAL_PARAMETER_NAME", raising=False)
    monkeypatch.setitem(
        registry.PROVIDER_FACTORIES,
        "example",
        lambda base_url, options: ExampleProvider(),
    )

    import scheduled_ingestor.handler as handler

    handler = importlib.reload(handler)
    provider = ExampleProvider()
    repository = RecordingRepository()
    monkeypatch.setattr(handler._ingestion, "provider", provider)
    monkeypatch.setattr(handler._ingestion, "repository", repository)

    context = SimpleNamespace(
        function_name="test-handler",
        memory_limit_in_mb=128,
        invoked_function_arn="arn:aws:lambda:us-east-1:000000000000:function:test-handler",
        aws_request_id="test-request",
        get_remaining_time_in_millis=lambda: 20_000,
    )
    result = handler.lambda_handler({}, context)

    assert result == {"status": "ok", "written": 1, "duplicates": 0}
    assert provider.credential is None
    assert len(repository.observations) == 1


def test_lambda_handler_ingests_weather_without_ssm(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("TABLE_NAME", "example-weather-observations")
    monkeypatch.setenv("PROVIDER_NAME", "open_meteo")
    monkeypatch.setenv("API_BASE_URL", "https://api.open-meteo.com")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "testing")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "testing")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.delenv("CREDENTIAL_PARAMETER_NAME", raising=False)

    import scheduled_ingestor.handler as handler

    handler = importlib.reload(handler)
    payload = json.loads(
        (Path(__file__).parents[1] / "fixtures" / "open_meteo_response.json").read_text(),
        parse_float=Decimal,
    )
    http = Mock()
    http.get_json.return_value = payload
    ssm = Mock()
    repository = RecordingRepository()
    monkeypatch.setattr(handler._ingestion, "http", http)
    monkeypatch.setattr(handler._ingestion, "ssm_client", ssm)
    monkeypatch.setattr(handler._ingestion, "repository", repository)
    context = SimpleNamespace(
        function_name="test-handler",
        memory_limit_in_mb=128,
        invoked_function_arn="arn:aws:lambda:us-east-1:000000000000:function:test-handler",
        aws_request_id="test-request",
        get_remaining_time_in_millis=lambda: 20_000,
    )

    result = handler.lambda_handler({}, context)

    assert result == {"status": "ok", "written": 1, "duplicates": 0}
    assert repository.observations[0].values == {
        "temperature": Decimal("20.25"),
        "apparent": Decimal("19.5"),
    }
    assert len({item.observed_at for item in repository.observations}) == 1
    ssm.get_parameter.assert_not_called()
    http.get_json.assert_called_once()
    assert http.get_json.call_args.kwargs == {}


def test_settings_rejects_missing_runtime_storage_schema(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("STORAGE_SCHEMA_JSON", raising=False)
    with pytest.raises(KeyError, match="STORAGE_SCHEMA_JSON"):
        Settings.from_environment()


def test_settings_rejects_selection_schema_mismatch(monkeypatch: pytest.MonkeyPatch) -> None:
    options = json.loads(os.environ["PROVIDER_OPTIONS_JSON"])
    options["measurements"][0]["name"] = "different"
    monkeypatch.setenv("PROVIDER_OPTIONS_JSON", json.dumps(options))
    with pytest.raises(ValueError, match="match runtime storage schema"):
        Settings.from_environment()


@pytest.mark.parametrize("written", [True, False])
def test_handler_preserves_result_and_operational_log_fields(
    runtime_handler: object,
    lambda_context: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    written: bool,
) -> None:
    handler = runtime_handler
    repository = Mock()
    repository.put.return_value = written
    monkeypatch.setattr(handler._ingestion, "provider", ExampleProvider())
    monkeypatch.setattr(handler._ingestion, "repository", repository)
    caplog.set_level(logging.INFO, logger=handler.logger.name)

    result = handler.lambda_handler({"raw": "SYNTHETIC_EVENT_VALUE"}, lambda_context)

    assert result == {"status": "ok", "written": int(written), "duplicates": int(not written)}
    assert isinstance(handler.logger, logging.Logger)
    assert len(caplog.records) == 1
    record = caplog.records[0]
    assert record.getMessage() == "Ingestion completed"
    assert record.provider == "open_meteo"
    assert record.observation_count == 1
    assert record.written_count == int(written)
    assert record.duplicate_count == int(not written)
    assert "SYNTHETIC_EVENT_VALUE" not in repr(record.__dict__)
    assert "sample" not in record.__dict__
    assert "credential" not in record.__dict__


def test_handler_reuses_lazy_credential_cache_across_invocations(
    runtime_handler: object, lambda_context: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    handler = runtime_handler
    provider = ExampleProvider()
    ssm = Mock()
    ssm.get_parameter.return_value = {"Parameter": {"Value": "SYNTHETIC_VALUE"}}
    monkeypatch.setattr(handler._ingestion, "provider", provider)
    monkeypatch.setattr(handler._ingestion, "repository", RecordingRepository())
    monkeypatch.setattr(handler._ingestion, "credential_parameter_name", "/example/credential")
    monkeypatch.setattr(handler._ingestion, "ssm_client", ssm)
    ssm.get_parameter.assert_not_called()

    handler.lambda_handler({}, lambda_context)
    handler.lambda_handler({}, lambda_context)

    ssm.get_parameter.assert_called_once_with(Name="/example/credential", WithDecryption=True)
    assert provider.credential == "SYNTHETIC_VALUE"


def test_handler_composition_omits_ssm_client_without_parameter(runtime_handler: object) -> None:
    assert runtime_handler._ingestion.ssm_client is None


def test_handler_root_debug_suppresses_real_sdk_secret_logs(
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
    lambda_context: SimpleNamespace,
) -> None:
    dependencies = ("boto3", "botocore", "urllib3")
    for name in dependencies:
        monkeypatch.setattr(logging.getLogger(name), "level", logging.NOTSET)
    caplog.set_level(logging.DEBUG)
    monkeypatch.setenv("TABLE_NAME", "observations")
    monkeypatch.setenv("PROVIDER_NAME", "open_meteo")
    monkeypatch.setenv("API_BASE_URL", "https://api.example.com")
    monkeypatch.setenv("CREDENTIAL_PARAMETER_NAME", "/example/credential")
    monkeypatch.setenv("AWS_SESSION_TOKEN", "SYNTHETIC_SESSION_TOKEN")
    monkeypatch.setenv("AWS_EC2_METADATA_DISABLED", "true")
    monkeypatch.setattr("boto3.DEFAULT_SESSION", None)
    monkeypatch.setattr("socket.getaddrinfo", Mock(side_effect=AssertionError("Unexpected DNS")))
    monkeypatch.setattr(
        "socket.socket.connect", Mock(side_effect=AssertionError("Unexpected network"))
    )
    requests: list[AWSPreparedRequest] = []
    body = json.dumps(
        {"Parameter": {"Type": "SecureString", "Value": "SYNTHETIC_DECRYPTED_SECRET"}}
    ).encode()

    def send(session: URLLib3Session, request: AWSPreparedRequest) -> AWSResponse:
        requests.append(request)
        raw = Mock()
        raw.stream.return_value = iter([body])
        return AWSResponse(request.url, 200, {"content-type": "application/x-amz-json-1.1"}, raw)

    monkeypatch.setattr(URLLib3Session, "send", send)

    import scheduled_ingestor.handler as handler

    handler = importlib.reload(handler)
    monkeypatch.setattr(handler._ingestion, "provider", ExampleProvider())
    monkeypatch.setattr(handler._ingestion, "repository", RecordingRepository())
    handler.logger.debug("Application debug remains enabled")
    result = handler.lambda_handler({}, lambda_context)

    assert result == {"status": "ok", "written": 1, "duplicates": 0}
    assert len(requests) == 1
    assert requests[0].headers["Authorization"].startswith(b"AWS4-HMAC-SHA256 ")
    assert requests[0].headers["X-Amz-Security-Token"] == b"SYNTHETIC_SESSION_TOKEN"
    assert json.loads(requests[0].body) == {
        "Name": "/example/credential",
        "WithDecryption": True,
    }
    assert handler._ingestion.provider.credential == "SYNTHETIC_DECRYPTED_SECRET"
    assert logging.getLogger().level == logging.DEBUG
    assert all(
        logging.getLogger(name).getEffectiveLevel() == logging.WARNING for name in dependencies
    )
    assert "Application debug remains enabled" in caplog.text
    assert not any(record.name.split(".")[0] in dependencies for record in caplog.records)
    for sensitive in (
        "Authorization",
        "X-Amz-Security-Token",
        "SYNTHETIC_SESSION_TOKEN",
        "SYNTHETIC_DECRYPTED_SECRET",
        "AWS4-HMAC-SHA256",
    ):
        assert sensitive not in caplog.text
    completion = next(record for record in caplog.records if record.msg == "Ingestion completed")
    assert completion.provider == "open_meteo"
    assert completion.observation_count == completion.written_count == 1
    assert completion.duplicate_count == 0

    caplog.clear()
    for name in dependencies:
        logging.getLogger(name).setLevel(logging.NOTSET)
    handler._ssm.get_parameter(Name="/example/credential", WithDecryption=True)
    assert "SYNTHETIC_SESSION_TOKEN" in caplog.text
    assert "SYNTHETIC_DECRYPTED_SECRET" in caplog.text
    assert "Authorization" in caplog.text
    assert {"botocore.auth", "botocore.endpoint", "botocore.parsers"} <= {
        record.name for record in caplog.records
    }


def test_handler_composes_configured_ssm_without_fetching_credential(
    runtime_handler: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CREDENTIAL_PARAMETER_NAME", "/example/credential")
    ssm = Mock()
    client = Mock(return_value=ssm)
    monkeypatch.setattr("boto3.client", client)

    handler = importlib.reload(runtime_handler)

    client.assert_called_once_with("ssm", config=handler._sdk_config)
    assert handler._ingestion.ssm_client is ssm
    ssm.get_parameter.assert_not_called()


def test_handler_configures_reused_sdk_clients_with_bounded_timeouts_and_no_retries(
    runtime_handler: object, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setenv("CREDENTIAL_PARAMETER_NAME", "/example/credential")
    client = Mock()
    resource = Mock()
    monkeypatch.setattr("boto3.client", client)
    monkeypatch.setattr("boto3.resource", resource)

    handler = importlib.reload(runtime_handler)

    client.assert_called_once_with("ssm", config=handler._sdk_config)
    resource.assert_called_once_with("dynamodb", config=handler._sdk_config)
    config = handler._sdk_config
    assert config.connect_timeout == pytest.approx(1.0)
    assert config.read_timeout == pytest.approx(2.0)
    assert config.retries == {"mode": "standard", "total_max_attempts": 1}
    assert handler._ingestion.ssm_client is client.return_value
    assert handler._repository.table is resource.return_value.Table.return_value


def test_handler_forwards_actual_remaining_time(
    runtime_handler: object, lambda_context: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    run = Mock(return_value={"status": "ok", "written": 0, "duplicates": 0})
    monkeypatch.setattr(runtime_handler._ingestion, "run", run)

    runtime_handler.lambda_handler({}, lambda_context)

    run.assert_called_once_with(lambda_context.get_remaining_time_in_millis)


def test_handler_short_invocation_budget_prevents_work_and_success_log(
    runtime_handler: object, lambda_context: SimpleNamespace, monkeypatch: pytest.MonkeyPatch
) -> None:
    lambda_context.get_remaining_time_in_millis = lambda: 1000
    provider = Mock(spec=BaseProvider)
    log = Mock()
    monkeypatch.setattr(runtime_handler._ingestion, "provider", provider)
    monkeypatch.setattr(runtime_handler.logger, "info", log)

    with pytest.raises(BudgetExceeded):
        runtime_handler.lambda_handler({}, lambda_context)

    provider.fetch.assert_not_called()
    log.assert_not_called()
    assert runtime_handler._http._budget is None


@pytest.mark.parametrize("stage", ["credential", "fetch", "normalize", "put"])
def test_handler_propagates_failures_without_success_log(
    runtime_handler: object,
    lambda_context: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    stage: str,
) -> None:
    handler = runtime_handler
    provider = Mock(spec=BaseProvider)
    provider.normalize.return_value = ExampleProvider().normalize(
        {}, datetime(2026, 10, 9, 12, tzinfo=UTC)
    )
    repository = Mock()
    failure = RuntimeError("synthetic failure")
    monkeypatch.setattr(handler._ingestion, "provider", provider)
    monkeypatch.setattr(handler._ingestion, "repository", repository)
    if stage == "credential":
        ssm = Mock()
        ssm.get_parameter.side_effect = failure
        monkeypatch.setattr(handler._ingestion, "credential_parameter_name", "/example/key")
        monkeypatch.setattr(handler._ingestion, "ssm_client", ssm)
    elif stage == "put":
        repository.put.side_effect = failure
    else:
        getattr(provider, stage).side_effect = failure
    log = Mock()
    monkeypatch.setattr(handler.logger, "info", log)

    with pytest.raises(RuntimeError) as error:
        handler.lambda_handler({}, lambda_context)

    assert error.value is failure
    log.assert_not_called()


def test_handler_logs_neither_event_payload_nor_credential(
    runtime_handler: object,
    lambda_context: SimpleNamespace,
    monkeypatch: pytest.MonkeyPatch,
    caplog: pytest.LogCaptureFixture,
) -> None:
    handler = runtime_handler
    ssm = Mock()
    ssm.get_parameter.return_value = {"Parameter": {"Value": "SYNTHETIC_CREDENTIAL_SENTINEL"}}
    monkeypatch.setattr(handler._ingestion, "provider", ExampleProvider())
    monkeypatch.setattr(handler._ingestion, "repository", RecordingRepository())
    monkeypatch.setattr(handler._ingestion, "credential_parameter_name", "/example/key")
    monkeypatch.setattr(handler._ingestion, "ssm_client", ssm)

    with caplog.at_level("INFO"):
        handler.lambda_handler({"payload": "SYNTHETIC_EVENT_SENTINEL"}, lambda_context)

    assert any(record.message == "Ingestion completed" for record in caplog.records)
    output = repr([record.__dict__ for record in caplog.records])
    assert "SYNTHETIC_CREDENTIAL_SENTINEL" not in output
    assert "SYNTHETIC_EVENT_SENTINEL" not in output
