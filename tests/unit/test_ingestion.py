import ast
import os
import signal
import time
from collections.abc import Callable
from datetime import UTC, datetime
from decimal import Decimal
from io import BytesIO
from pathlib import Path
from unittest.mock import Mock, call

import boto3
import pytest
import urllib3
from botocore.config import Config
from botocore.exceptions import HTTPClientError
from botocore.httpsession import URLLib3Session

from scheduled_ingestor.core.budget import BudgetExceeded, InvocationBudget
from scheduled_ingestor.core.http import HttpClient
from scheduled_ingestor.core.ingestion import IngestionService
from scheduled_ingestor.core.models import Observation
from scheduled_ingestor.core.provider import BaseProvider
from scheduled_ingestor.core.repository import ObservationRepository
from scheduled_ingestor.core.transport import bound_sdk_client


@pytest.fixture
def service() -> IngestionService:
    provider = Mock(spec=BaseProvider)
    provider.readiness_credential.return_value = "EXAMPLE_READINESS_ONLY"
    provider.fetch.return_value = {"sample": "value"}
    provider.normalize.return_value = [
        Observation(
            provider="example",
            dataset="sample",
            subject="item",
            observed_at=datetime(2026, 10, 9, 12, tzinfo=UTC),
            values={"reading": Decimal("1")},
        )
    ]
    repository = Mock(spec=ObservationRepository)
    repository.put.return_value = True
    return IngestionService(provider, Mock(spec=HttpClient), repository)


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Mock:
    timer = Mock(return_value=0.0)
    monkeypatch.setattr("scheduled_ingestor.core.budget.time.monotonic", timer)
    return timer


@pytest.mark.parametrize("written", [True, False])
def test_run_reports_written_or_duplicate_counts(service: IngestionService, written: bool) -> None:
    service.repository.put.return_value = written

    result = service.run()

    assert result == {"status": "ok", "written": int(written), "duplicates": int(not written)}
    service.provider.fetch.assert_called_once_with(service.http, None)
    payload, observed_at = service.provider.normalize.call_args.args
    assert payload is service.provider.fetch.return_value
    assert observed_at.tzinfo is UTC
    assert observed_at.microsecond == 0
    service.repository.put.assert_called_once_with(service.provider.normalize.return_value[0])


def test_run_counts_repeated_snapshots_as_writes(service: IngestionService) -> None:
    observations = service.provider.normalize.return_value * 3
    service.provider.normalize.return_value = observations
    service.repository.put.side_effect = [True, True, True]

    assert service.run() == {"status": "ok", "written": 3, "duplicates": 0}
    assert service.repository.put.call_args_list == [call(item) for item in observations]


def test_run_accepts_empty_observations(service: IngestionService) -> None:
    service.provider.normalize.return_value = []

    assert service.run() == {"status": "ok", "written": 0, "duplicates": 0}
    service.repository.put.assert_not_called()


@pytest.mark.parametrize("parameter", [None, ""])
def test_run_without_parameter_never_uses_ssm(
    service: IngestionService, parameter: str | None
) -> None:
    service.credential_parameter_name = parameter
    service.ssm_client = Mock()

    service.run()

    service.ssm_client.get_parameter.assert_not_called()
    service.provider.fetch.assert_called_once_with(service.http, None)
    service.provider.readiness_credential.assert_not_called()


def test_run_lazily_caches_credential_per_service(service: IngestionService) -> None:
    service.credential_parameter_name = "/test/ssm"
    service.ssm_client = Mock()
    service.ssm_client.get_parameter.return_value = {"Parameter": {"Value": "test-only"}}
    service.ssm_client.get_parameter.assert_not_called()

    service.run()
    service.run()

    service.ssm_client.get_parameter.assert_called_once_with(Name="/test/ssm", WithDecryption=True)
    assert service.provider.fetch.call_args_list == [
        call(service.http, "test-only"),
        call(service.http, "test-only"),
    ]
    service.provider.readiness_credential.assert_not_called()


def test_new_service_does_not_share_credential_cache(service: IngestionService) -> None:
    ssm = Mock()
    ssm.get_parameter.side_effect = [
        {"Parameter": {"Value": "FIRST_SYNTHETIC_VALUE"}},
        {"Parameter": {"Value": "SECOND_SYNTHETIC_VALUE"}},
    ]
    first = IngestionService(
        service.provider, service.http, service.repository, "/example/key", ssm
    )
    second = IngestionService(
        service.provider, service.http, service.repository, "/example/key", ssm
    )

    first.run()
    second.run()

    assert service.provider.fetch.call_args_list == [
        call(service.http, "FIRST_SYNTHETIC_VALUE"),
        call(service.http, "SECOND_SYNTHETIC_VALUE"),
    ]
    service.provider.readiness_credential.assert_not_called()


def test_run_rejects_configured_parameter_without_ssm(service: IngestionService) -> None:
    service.credential_parameter_name = "/example/credential"

    with pytest.raises(ValueError, match="requires an SSM client"):
        service.run()
    service.provider.fetch.assert_not_called()
    service.provider.readiness_credential.assert_not_called()


@pytest.mark.parametrize("stage", ["credential", "fetch", "normalize", "put"])
def test_run_propagates_processing_failures(service: IngestionService, stage: str) -> None:
    failure = RuntimeError("synthetic failure")
    if stage == "credential":
        service.credential_parameter_name = "/example/credential"
        service.ssm_client = Mock()
        service.ssm_client.get_parameter.side_effect = failure
    elif stage == "put":
        service.repository.put.side_effect = failure
    else:
        getattr(service.provider, stage).side_effect = failure

    with pytest.raises(RuntimeError) as error:
        service.run()

    assert error.value is failure
    service.provider.readiness_credential.assert_not_called()
    if stage in ("credential", "fetch"):
        service.provider.normalize.assert_not_called()
    if stage != "put":
        service.repository.put.assert_not_called()


def test_run_retries_failed_credential_fetch_without_caching_failure(
    service: IngestionService,
) -> None:
    service.credential_parameter_name = "/example/credential"
    service.ssm_client = Mock()
    service.ssm_client.get_parameter.side_effect = [
        RuntimeError("synthetic failure"),
        {"Parameter": {"Value": "SYNTHETIC_VALUE"}},
    ]

    with pytest.raises(RuntimeError, match="synthetic failure"):
        service.run()
    service.run()
    service.run()

    assert service.ssm_client.get_parameter.call_count == 2
    service.provider.fetch.assert_called_with(service.http, "SYNTHETIC_VALUE")
    service.provider.readiness_credential.assert_not_called()


def test_core_imports_never_depend_on_plugins_registry_or_handler() -> None:
    core = Path(__file__).parents[2] / "src" / "scheduled_ingestor" / "core"
    forbidden = {
        "scheduled_ingestor.providers",
        "scheduled_ingestor.registry",
        "scheduled_ingestor.handler",
    }
    violations = []
    for module in core.glob("*.py"):
        for node in ast.walk(ast.parse(module.read_text())):
            imports = []
            if isinstance(node, ast.Import):
                imports = [alias.name for alias in node.names]
            elif isinstance(node, ast.ImportFrom):
                imports = [node.module or ""]
                if node.module == "scheduled_ingestor":
                    imports.extend(f"scheduled_ingestor.{alias.name}" for alias in node.names)
            if any(
                name == prefix or name.startswith(f"{prefix}.")
                for name in imports
                for prefix in forbidden
            ):
                violations.append(module.name)
    assert violations == []


@pytest.mark.parametrize("remaining_ms", [0, 500, 4500])
def test_run_rejects_exhausted_budget_before_provider_or_credentials(
    service: IngestionService, remaining_ms: int
) -> None:
    service.credential_parameter_name = "/example/credential"
    service.ssm_client = Mock()

    with pytest.raises(BudgetExceeded):
        service.run(lambda: remaining_ms)

    service.ssm_client.get_parameter.assert_not_called()
    service.provider.fetch.assert_not_called()
    service.repository.put.assert_not_called()
    service.http.set_budget.assert_called_with(None)


def test_run_reserves_ssm_and_persistence_only_for_uncached_credential(
    service: IngestionService,
) -> None:
    service.credential_parameter_name = "/test/ssm"
    service.ssm_client = Mock()
    service.ssm_client.get_parameter.return_value = {"Parameter": {"Value": "test-only"}}

    with pytest.raises(BudgetExceeded):
        service.run(lambda: 7000)
    service.ssm_client.get_parameter.assert_not_called()
    service.run(lambda: 20_000)
    assert service.run(lambda: 7000) == {"status": "ok", "written": 1, "duplicates": 0}

    service.ssm_client.get_parameter.assert_called_once_with(Name="/test/ssm", WithDecryption=True)


def test_ssm_elapsed_operation_is_bounded_and_failed_read_is_not_cached(
    service: IngestionService, clock: Mock
) -> None:
    service.credential_parameter_name = "/example/credential"
    service.ssm_client = Mock()

    def delayed_parameter(**kwargs: object) -> dict[str, object]:
        clock.return_value += 3.1
        return {"Parameter": {"Value": "EXAMPLE_SECRET"}}

    service.ssm_client.get_parameter.side_effect = delayed_parameter

    with pytest.raises(BudgetExceeded):
        service.run(lambda: 20_000)

    assert service._cached_credential is None
    service.provider.fetch.assert_not_called()
    service.repository.put.assert_not_called()
    service.ssm_client.get_parameter.side_effect = None
    service.ssm_client.get_parameter.return_value = {"Parameter": {"Value": "EXAMPLE_SECRET"}}
    service.run(lambda: 20_000)
    assert service.ssm_client.get_parameter.call_count == 2


@pytest.mark.parametrize("stage", ["credential", "put"])
def test_sdk_timer_is_armed_and_timeout_propagates_without_cached_success(
    service: IngestionService, stage: str
) -> None:
    armed: list[float] = []

    def interrupt(*args: object, **kwargs: object) -> None:
        armed.append(signal.getitimer(signal.ITIMER_REAL)[0])
        signal.getsignal(signal.SIGALRM)(signal.SIGALRM, None)

    if stage == "credential":
        service.credential_parameter_name = "/example/credential"
        service.ssm_client = Mock()
        service.ssm_client.get_parameter.side_effect = interrupt
    else:
        service.repository.put.side_effect = interrupt

    with pytest.raises(BudgetExceeded):
        service.run(lambda: 20_000)

    assert len(armed) == 1
    assert 0 < armed[0] <= 3.0
    assert service._cached_credential is None
    service.http.set_budget.assert_called_with(None)
    assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)


def test_normalization_cannot_spend_persistence_reserve(
    service: IngestionService, clock: Mock
) -> None:
    observations = service.provider.normalize.return_value

    def delayed_normalize(*args: object) -> list[Observation]:
        clock.return_value = 16.0
        return observations

    service.provider.normalize.side_effect = delayed_normalize

    with pytest.raises(BudgetExceeded):
        service.run(lambda: 20_000)

    service.repository.put.assert_not_called()
    service.http.set_budget.assert_called_with(None)


def test_budget_checked_between_observations_preserves_partial_write_retry(
    service: IngestionService, clock: Mock
) -> None:
    observations = service.provider.normalize.return_value * 2
    service.provider.normalize.return_value = observations

    def delayed_fetch(*args: object) -> dict[str, str]:
        clock.return_value += 14.0
        return {"sample": "value"}

    def delayed_put(observation: Observation) -> bool:
        clock.return_value += 2.5
        return True

    service.provider.fetch.side_effect = delayed_fetch
    service.repository.put.side_effect = delayed_put

    with pytest.raises(BudgetExceeded):
        service.run(lambda: 20_000)

    service.repository.put.assert_called_once_with(observations[0])
    service.provider.fetch.side_effect = None
    service.repository.put.side_effect = [True, True]
    assert service.run(lambda: 20_000) == {"status": "ok", "written": 2, "duplicates": 0}
    bound_budgets = [entry.args[0] for entry in service.http.set_budget.call_args_list]
    assert isinstance(bound_budgets[0], InvocationBudget)
    assert isinstance(bound_budgets[2], InvocationBudget)
    assert bound_budgets[0] is not bound_budgets[2]
    assert bound_budgets[1] is bound_budgets[3] is None


def test_actual_http_policy_shares_service_persistence_reserve_and_resets_deadline(
    service: IngestionService, monkeypatch: pytest.MonkeyPatch, clock: Mock
) -> None:
    service.http = HttpClient()
    service.provider.fetch.side_effect = lambda client, credential: client.get_json(
        "https://api.example.com/data"
    )
    responses = [urllib3.HTTPResponse(body=BytesIO(b"{}"), preload_content=False) for _ in range(2)]
    transport = Mock(side_effect=responses)
    monkeypatch.setattr(urllib3.HTTPSConnectionPool, "_make_request", transport)

    assert service.run(lambda: 5000) == {"status": "ok", "written": 1, "duplicates": 0}
    assert transport.call_args.kwargs["timeout"].total == pytest.approx(0.5)
    assert service.http._budget is None
    clock.return_value = 30.0
    assert service.run(lambda: 20_000) == {"status": "ok", "written": 1, "duplicates": 0}
    assert transport.call_args.kwargs["timeout"].total == pytest.approx(4.0)
    assert service.http._budget is None


@pytest.mark.parametrize("stage", ["credential", "put"])
@pytest.mark.parametrize("io_stage", ["headers", "body"])
def test_kernel_sdk_expiry_crosses_botocore_boundary_without_success_or_retry(
    service: IngestionService,
    isolated_process: Callable[[Callable[[], None]], None],
    stage: str,
    io_stage: str,
) -> None:
    def check() -> None:
        client = boto3.client(
            "ssm" if stage == "credential" else "dynamodb",
            endpoint_url="https://sdk.example.com",
            config=Config(proxies={}, retries={"mode": "standard", "total_max_attempts": 1}),
        )
        read_fd, write_fd = os.pipe()
        stream = os.fdopen(read_fd, "rb", buffering=0)
        wrapped: list[Exception] = []
        original_send = URLLib3Session.send

        def observe_send(session: URLLib3Session, request: object) -> object:
            try:
                return original_send(session, request)
            except HTTPClientError as error:
                wrapped.append(error.kwargs["error"])
                raise

        def blocking_request(*args: object, **kwargs: object) -> urllib3.HTTPResponse:
            if io_stage == "headers":
                stream.read(1)
            return urllib3.HTTPResponse(body=stream, preload_content=False)

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(URLLib3Session, "send", observe_send)
            bound_sdk_client(client)
            patch.setattr("scheduled_ingestor.core.ingestion.SDK_OPERATION_SECONDS", 0.06)
            transport = Mock(side_effect=blocking_request)
            patch.setattr(urllib3.HTTPSConnectionPool, "_make_request", transport)
            if stage == "credential":
                service.credential_parameter_name = "/example/credential"
                service.ssm_client = client
            else:
                service.provider.normalize.return_value *= 2
                service.repository.put.side_effect = lambda observation: client.put_item(
                    TableName="observations", Item={"pk": {"S": "example"}}
                )
            previous = signal.getsignal(signal.SIGALRM)
            started = time.monotonic()
            try:
                with pytest.raises(BudgetExceeded):
                    service.run(lambda: 20_000)
                assert time.monotonic() - started < 0.3
                transport.assert_called_once()
                assert len(wrapped) == 1 and isinstance(wrapped[0], BudgetExceeded)
                assert service._cached_credential is None
                if stage == "credential":
                    service.provider.fetch.assert_not_called()
                    service.repository.put.assert_not_called()
                else:
                    assert service.repository.put.call_count == 1
                service.http.set_budget.assert_called_with(None)
                assert signal.getsignal(signal.SIGALRM) is previous
                assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
            finally:
                client.close()
                stream.close()
                os.close(write_fd)

    isolated_process(check)
