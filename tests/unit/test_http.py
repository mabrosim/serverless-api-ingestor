import ctypes
import json
import os
import signal
import socket
import time
from collections.abc import Callable
from decimal import Decimal
from io import BytesIO
from threading import Event, Thread
from threading import enumerate as enumerate_threads
from unittest.mock import Mock

import boto3
import pytest
import urllib3
from botocore.config import Config

from scheduled_ingestor.core.budget import BudgetExceeded, InvocationBudget
from scheduled_ingestor.core.http import HttpClient
from scheduled_ingestor.core.transport import bound_sdk_client


@pytest.fixture(autouse=True)
def no_real_sleep(monkeypatch: pytest.MonkeyPatch) -> Mock:
    sleeper = Mock()
    monkeypatch.setattr("scheduled_ingestor.core.http.time.sleep", sleeper)
    return sleeper


@pytest.fixture
def clock(monkeypatch: pytest.MonkeyPatch) -> Mock:
    timer = Mock(return_value=0.0)
    monkeypatch.setattr("scheduled_ingestor.core.budget.time.monotonic", timer)
    return timer


def test_get_json_preserves_decimal_precision() -> None:
    client = HttpClient()
    client._pool = FakePool(FakeResponse(body=b'{"value": 12.1234567890123456789}'))

    result = client.get_json("https://api.example.com/data")

    assert result["value"] == Decimal("12.1234567890123456789")
    assert isinstance(result["value"], Decimal)


@pytest.mark.parametrize("constant", ["NaN", "Infinity", "-Infinity"])
def test_get_json_rejects_nonstandard_constants(constant: str) -> None:
    response = FakeResponse(body=f'{{"value": {constant}}}'.encode())
    client = HttpClient()
    client._pool = FakePool(response)

    with pytest.raises(ValueError, match="nonstandard numeric constant"):
        client.get_json("https://api.example.com/data")

    assert response.closed and response.released


class FakeResponse:
    def __init__(self, status: int = 200, body: bytes | None = b"{}") -> None:
        self.status = status
        self.headers: dict[str, str] = {}
        self._body = body
        self.read_sizes: list[int] = []
        self.closed = False
        self.released = False

    @property
    def data(self) -> bytes:
        raise AssertionError("response.data must not be accessed")

    def read(self, size: int) -> bytes:
        self.read_sizes.append(size)
        if self._body is None:
            return b"x" * size
        return self._body

    def close(self) -> None:
        self.closed = True

    def release_conn(self) -> None:
        self.released = True


class FakePool:
    def __init__(self, response: FakeResponse) -> None:
        self.response = response
        self.request_args: tuple[object, ...] | None = None
        self.request_kwargs: dict[str, object] | None = None

    def request(self, *args: object, **kwargs: object) -> FakeResponse:
        self.request_args = args
        self.request_kwargs = kwargs
        return self.response


def test_get_json_decodes_json_and_releases_response() -> None:
    # Arrange
    response = FakeResponse(body=b'{"value": 42}')
    pool = FakePool(response)
    client = HttpClient()
    client._pool = pool

    # Act
    result = client.get_json("https://api.example.com/data", {"X-Test": "value"})

    # Assert
    assert result == {"value": 42}
    assert pool.request_args == ("GET", "https://api.example.com/data")
    timeout = pool.request_kwargs.pop("timeout")
    assert timeout.total == pytest.approx(4.0)
    assert timeout.connect_timeout == pytest.approx(2.0)
    assert timeout.read_timeout <= 3.0
    assert pool.request_kwargs == {
        "headers": {
            "User-Agent": "serverless-api-ingestor/0.1",
            "X-Test": "value",
        },
        "preload_content": False,
        "redirect": False,
        "retries": False,
    }
    assert response.read_sizes == [1_000_001]
    assert response.closed
    assert response.released


def test_get_json_accepts_response_exactly_at_size_limit() -> None:
    # Arrange
    prefix = b'{"payload":"'
    suffix = b'"}'
    body = prefix + b"x" * (1_000_000 - len(prefix) - len(suffix)) + suffix
    response = FakeResponse(body=body)
    client = HttpClient()
    client._pool = FakePool(response)

    # Act
    result = client.get_json("https://api.example.com/data")

    # Assert
    assert len(body) == 1_000_000
    assert result["payload"]
    assert response.read_sizes == [1_000_001]
    assert response.closed
    assert response.released


def test_get_json_rejects_oversized_stream_after_bounded_read() -> None:
    # Arrange
    response = FakeResponse(body=None)
    client = HttpClient()
    client._pool = FakePool(response)

    # Act and assert
    with pytest.raises(RuntimeError, match="exceeds 1 MB limit"):
        client.get_json("https://api.example.com/data")

    assert response.read_sizes == [1_000_001]
    assert response.closed
    assert response.released


def test_get_json_releases_response_on_http_error(monkeypatch: pytest.MonkeyPatch) -> None:
    # Arrange
    response = FakeResponse(status=503)
    client = HttpClient()
    client._pool = FakePool(response)
    monkeypatch.setattr("scheduled_ingestor.core.http.time.sleep", Mock())

    # Act and assert
    with pytest.raises(RuntimeError, match="Provider returned HTTP 503"):
        client.get_json("https://api.example.com/data")

    assert response.read_sizes == [1_000_001] * 3
    assert response.closed
    assert response.released


def test_get_json_rejects_redirect_without_following_and_releases_response() -> None:
    # Arrange
    response = FakeResponse(status=302)
    pool = FakePool(response)
    client = HttpClient()
    client._pool = pool

    # Act and assert
    with pytest.raises(RuntimeError, match="Provider returned HTTP 302"):
        client.get_json("https://api.example.com/data")

    assert pool.request_kwargs is not None
    assert pool.request_kwargs["redirect"] is False
    assert response.read_sizes == [1_000_001]
    assert response.closed
    assert response.released


def test_get_json_rejects_non_https_before_request() -> None:
    # Arrange
    pool = FakePool(FakeResponse())
    client = HttpClient()
    client._pool = pool

    # Act and assert
    with pytest.raises(ValueError, match="HTTPS"):
        client.get_json("http://api.example.com/data")

    assert pool.request_args is None


def test_get_json_releases_response_on_invalid_json() -> None:
    # Arrange
    response = FakeResponse(body=b"not-json")
    client = HttpClient()
    client._pool = FakePool(response)

    # Act and assert
    with pytest.raises(json.JSONDecodeError):
        client.get_json("https://api.example.com/data")

    assert response.closed
    assert response.released


def test_real_pool_never_automatically_drains_or_sleeps(monkeypatch: pytest.MonkeyPatch) -> None:
    responses = [
        urllib3.HTTPResponse(
            body=BytesIO(b"{}"),
            status=503,
            headers={"Retry-After": "21600"},
            preload_content=False,
        )
        for _ in range(3)
    ]
    transport = Mock(side_effect=responses)
    sleeper = Mock()
    monkeypatch.setattr("scheduled_ingestor.core.http.time.sleep", sleeper)
    monkeypatch.setattr(urllib3.HTTPSConnectionPool, "_make_request", transport)
    monkeypatch.setattr(
        urllib3.HTTPResponse, "drain_conn", Mock(side_effect=AssertionError("automatic drain"))
    )
    monkeypatch.setattr(urllib3.Retry, "sleep", Mock(side_effect=AssertionError("automatic sleep")))

    client = HttpClient()
    with pytest.raises(RuntimeError, match="Provider returned HTTP 503"):
        client.get_json("https://api.example.com/data")

    assert transport.call_count == 3
    assert transport.call_args.kwargs["retries"].total is False
    assert [args.args for args in sleeper.call_args_list] == [(1.0,), (1.0,)]


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_real_pool_retries_transient_status_then_preserves_success(
    monkeypatch: pytest.MonkeyPatch, no_real_sleep: Mock, status: int
) -> None:
    responses = [
        urllib3.HTTPResponse(body=BytesIO(b"{}"), status=status, preload_content=False),
        urllib3.HTTPResponse(body=BytesIO(b'{"value": 1.25}'), preload_content=False),
    ]
    transport = Mock(side_effect=responses)
    monkeypatch.setattr(urllib3.HTTPSConnectionPool, "_make_request", transport)

    result = HttpClient().get_json("https://api.example.com/data")

    assert result == {"value": Decimal("1.25")}
    assert transport.call_count == 2
    no_real_sleep.assert_called_once_with(0.2)
    assert all(response.closed and response._connection is None for response in responses)


@pytest.mark.parametrize("status", [200, 302, 400, 429, 500, 503])
def test_real_pool_bounds_every_response_body_before_retry(
    monkeypatch: pytest.MonkeyPatch, no_real_sleep: Mock, status: int
) -> None:
    raw = BytesIO(b"x" * 1_500_000)
    response = urllib3.HTTPResponse(
        body=raw, status=status, headers={"Retry-After": "21600"}, preload_content=False
    )
    read = Mock(wraps=response.read)
    monkeypatch.setattr(response, "read", read)
    transport = Mock(return_value=response)
    monkeypatch.setattr(urllib3.HTTPSConnectionPool, "_make_request", transport)
    monkeypatch.setattr(response, "drain_conn", Mock(side_effect=AssertionError("automatic drain")))

    client = HttpClient()
    with pytest.raises(RuntimeError, match="exceeds 1 MB limit"):
        client.get_json("https://api.example.com/data")

    read.assert_called_once_with(1_000_001)
    assert response._fp_bytes_read == 1_000_001
    assert response.closed and response._connection is None
    transport.assert_called_once()
    no_real_sleep.assert_not_called()


@pytest.mark.parametrize(
    ("header", "expected"),
    [
        ("21600", 1.0),
        ("Thu, 01 Jan 2099 00:00:00 GMT", 1.0),
        ("0", 0.0),
        ("not-a-date", 0.2),
        ("-1", 0.2),
        ("9" * 5000, 0.2),
    ],
)
def test_retry_after_is_capped_or_invalid_header_uses_backoff(
    monkeypatch: pytest.MonkeyPatch, header: str, expected: float
) -> None:
    monkeypatch.setattr("urllib3.util.retry.time.time", lambda: 0.0)

    assert HttpClient._retry_delay(header, 0) == pytest.approx(expected)


@pytest.mark.parametrize("failure_type", ["connect", "read", "protocol"])
def test_real_pool_transport_exhaustion_is_bounded_and_sanitized(
    monkeypatch: pytest.MonkeyPatch, no_real_sleep: Mock, failure_type: str
) -> None:
    errors = {
        "connect": urllib3.exceptions.ConnectTimeoutError("SYNTHETIC_PRIVATE_VALUE"),
        "read": urllib3.exceptions.ReadTimeoutError(None, "/example", "SYNTHETIC_PRIVATE_VALUE"),
        "protocol": urllib3.exceptions.ProtocolError("SYNTHETIC_PRIVATE_VALUE"),
    }
    transport = Mock(side_effect=errors[failure_type])
    monkeypatch.setattr(urllib3.HTTPSConnectionPool, "_make_request", transport)

    client = HttpClient()
    with pytest.raises(RuntimeError, match=r"^Provider HTTP request failed$") as caught:
        client.get_json("https://api.example.com/data")

    assert "SYNTHETIC_PRIVATE_VALUE" not in str(caught.value)
    assert caught.value.__suppress_context__
    assert transport.call_count == 3
    assert [args.args for args in no_real_sleep.call_args_list] == [(0.2,), (0.4,)]


def test_retry_delay_never_spends_persistence_reserve(clock: Mock, no_real_sleep: Mock) -> None:
    response = FakeResponse(status=429)
    response.headers["Retry-After"] = "21600"
    client = HttpClient()
    client._pool = FakePool(response)
    client.set_budget(InvocationBudget(lambda: 5000))

    with pytest.raises(BudgetExceeded, match="retry exceeds invocation time budget"):
        client.get_json("https://api.example.com/data")

    no_real_sleep.assert_not_called()
    assert response.closed and response.released
    assert client._pool.request_kwargs["timeout"].total == pytest.approx(0.5)


def test_real_body_elapsed_time_is_checked_not_only_socket_timeout(
    monkeypatch: pytest.MonkeyPatch, clock: Mock, no_real_sleep: Mock
) -> None:
    class TricklingBody(BytesIO):
        def read(self, size: int = -1) -> bytes:
            if self.tell() < len(self.getvalue()):
                clock.return_value += 4.1
            return super().read(size)

    responses = [
        urllib3.HTTPResponse(body=TricklingBody(b"{}"), preload_content=False) for _ in range(3)
    ]
    transport = Mock(side_effect=responses)
    monkeypatch.setattr(urllib3.HTTPSConnectionPool, "_make_request", transport)

    client = HttpClient()
    with pytest.raises(BudgetExceeded, match="HTTP time budget exhausted"):
        client.get_json("https://api.example.com/data")

    assert transport.call_count == 3
    assert all(response.closed and response._connection is None for response in responses)
    assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)


def test_timer_interrupts_real_response_read_and_releases_connection(
    monkeypatch: pytest.MonkeyPatch, no_real_sleep: Mock
) -> None:
    armed: list[float] = []

    class InterruptedBody(BytesIO):
        def read(self, size: int = -1) -> bytes:
            armed.append(signal.getitimer(signal.ITIMER_REAL)[0])
            signal.getsignal(signal.SIGALRM)(signal.SIGALRM, None)
            raise AssertionError("alarm did not interrupt read")

    responses = [
        urllib3.HTTPResponse(body=InterruptedBody(b"{}"), preload_content=False) for _ in range(3)
    ]
    monkeypatch.setattr(urllib3.HTTPSConnectionPool, "_make_request", Mock(side_effect=responses))
    previous_handler = signal.getsignal(signal.SIGALRM)

    client = HttpClient()
    with pytest.raises(BudgetExceeded, match="HTTP time budget exhausted"):
        client.get_json("https://api.example.com/data")

    assert all(0 < seconds <= 4 for seconds in armed)
    assert len(armed) == 3
    assert all(response.closed and response._connection is None for response in responses)
    assert signal.getsignal(signal.SIGALRM) is previous_handler
    assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)


def test_invocation_deadline_and_attempts_reset_but_not_between_fetches(clock: Mock) -> None:
    client = HttpClient()
    client._pool = FakePool(FakeResponse())
    first = InvocationBudget(lambda: 20_000)
    client.set_budget(first)
    for _ in range(3):
        assert client.get_json("https://api.example.com/data") == {}
    with pytest.raises(RuntimeError, match="attempt budget exhausted"):
        client.get_json("https://api.example.com/data")

    clock.return_value = 30.0
    with pytest.raises(BudgetExceeded):
        client.get_json("https://api.example.com/data")
    client.set_budget(InvocationBudget(lambda: 20_000))
    assert client.get_json("https://api.example.com/data") == {}
    client.set_budget(None)
    assert client.get_json("https://api.example.com/data") == {}


def test_remaining_callback_can_shorten_but_not_extend_deadline(clock: Mock) -> None:
    remaining = Mock(return_value=20_000)
    budget = InvocationBudget(remaining)
    clock.return_value = 10.0
    remaining.return_value = 30_000
    assert budget.check() == pytest.approx(9.5)
    remaining.return_value = 250

    with pytest.raises(BudgetExceeded):
        budget.check()


def test_nested_timer_restores_outer_deadline(clock: Mock) -> None:
    budget = InvocationBudget()
    with budget.limit(maximum=10.0):
        with budget.limit(maximum=4.0):
            clock.return_value = 2.0
        restored = signal.getitimer(signal.ITIMER_REAL)[0]
        assert 7.9 < restored <= 8.0
    assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)


def test_deadline_enforcement_refuses_unprotected_worker_thread() -> None:
    failures: list[Exception] = []

    def work() -> None:
        try:
            with InvocationBudget().limit():
                raise AssertionError("unprotected work started")
        except RuntimeError as error:
            failures.append(error)

    worker = Thread(target=work)
    worker.start()
    worker.join()

    assert len(failures) == 1
    assert str(failures[0]) == "Deadline enforcement requires the main thread"


@pytest.mark.parametrize("body", [b"SYNTHETIC_PRIVATE_VALUE", b"\xff"])
def test_malformed_body_errors_never_retain_raw_response(body: bytes) -> None:
    client = HttpClient()
    client._pool = FakePool(FakeResponse(body=body))

    with pytest.raises(ValueError) as caught:
        client.get_json("https://api.example.com/data")

    assert "SYNTHETIC_PRIVATE_VALUE" not in str(caught.value)
    assert getattr(caught.value, "doc", "") == ""
    assert caught.value.__suppress_context__


@pytest.mark.parametrize("stage", ["headers", "body"])
def test_kernel_expiry_crosses_real_urllib3_boundary_without_enclosing_retry(
    isolated_process: Callable[[Callable[[], None]], None], stage: str
) -> None:
    def check() -> None:
        read_fd, write_fd = os.pipe()
        stream = os.fdopen(read_fd, "rb", buffering=0)
        responses: list[urllib3.HTTPResponse] = []

        def blocking_request(*args: object, **kwargs: object) -> urllib3.HTTPResponse:
            if stage == "headers":
                stream.read(1)
            response = urllib3.HTTPResponse(body=stream, preload_content=False)
            responses.append(response)
            return response

        with pytest.MonkeyPatch.context() as patch:
            transport = Mock(side_effect=blocking_request)
            patch.setattr(urllib3.HTTPSConnectionPool, "_make_request", transport)
            client = HttpClient()
            previous = signal.getsignal(signal.SIGALRM)
            started = time.monotonic()
            try:
                with pytest.raises(BudgetExceeded), InvocationBudget().limit(maximum=0.06):
                    client.get_json("https://api.example.com/data")
                assert time.monotonic() - started < 0.3
                transport.assert_called_once()
                assert all(
                    response.closed and response._connection is None for response in responses
                )
                assert signal.getsignal(signal.SIGALRM) is previous
                assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
            finally:
                client._pool.clear()
                stream.close()
                os.close(write_fd)

    isolated_process(check)


@pytest.mark.parametrize("stage", ["headers", "body"])
def test_kernel_operation_timeout_retries_only_three_local_attempts(
    isolated_process: Callable[[Callable[[], None]], None], stage: str
) -> None:
    def check() -> None:
        streams: list[object] = []
        write_fds: list[int] = []
        responses: list[urllib3.HTTPResponse] = []

        def blocking_request(*args: object, **kwargs: object) -> urllib3.HTTPResponse:
            read_fd, write_fd = os.pipe()
            write_fds.append(write_fd)
            stream = os.fdopen(read_fd, "rb", buffering=0)
            streams.append(stream)
            if stage == "headers":
                stream.read(1)
            response = urllib3.HTTPResponse(body=stream, preload_content=False)
            responses.append(response)
            return response

        with pytest.MonkeyPatch.context() as patch:
            patch.setattr("scheduled_ingestor.core.http.ATTEMPT_SECONDS", 0.04)
            transport = Mock(side_effect=blocking_request)
            patch.setattr(urllib3.HTTPSConnectionPool, "_make_request", transport)
            client = HttpClient()
            started = time.monotonic()
            try:
                with pytest.raises(BudgetExceeded, match="HTTP time budget exhausted"):
                    client.get_json("https://api.example.com/data")
                assert transport.call_count == 3
                assert 0.1 < time.monotonic() - started < 0.4
                assert all(
                    response.closed and response._connection is None for response in responses
                )
                assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
            finally:
                client._pool.clear()
                for stream in streams:
                    stream.close()
                for write_fd in write_fds:
                    os.close(write_fd)

    isolated_process(check)


@pytest.mark.parametrize("transport", ["http", "ssm", "dynamodb"])
def test_native_resolver_deferral_returns_within_budget_without_late_transport(
    isolated_process: Callable[[Callable[[], None]], None],
    transport: str,
) -> None:
    """Defer alarms inside native lookup; caller must return without late connection work."""

    def check() -> None:
        if transport == "http":
            client = HttpClient()
            close = client._pool.clear
        else:
            client = boto3.client(
                transport,
                endpoint_url="https://sdk.example.com",
                config=Config(proxies={}, retries={"total_max_attempts": 1}),
            )
            bound_sdk_client(client)
            close = client.close

        def request() -> object:
            if transport == "http":
                return client.get_json("https://api.example.com/data")
            if transport == "ssm":
                return client.get_parameter(Name="/example/setting")
            return client.put_item(TableName="observations", Item={"pk": {"S": "example"}})

        read_fd, write_fd = os.pipe()
        release = Event()
        finished = Event()
        native_read = ctypes.CDLL(None).read
        native_read.argtypes = [ctypes.c_int, ctypes.c_void_p, ctypes.c_size_t]
        native_read.restype = ctypes.c_ssize_t
        calls: list[tuple[object, ...]] = []

        def stalled_resolver(*args: object) -> list[tuple[object, ...]]:
            calls.append(args)
            buffer = ctypes.create_string_buffer(1)
            previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
            try:
                native_read(read_fd, buffer, 1)
            finally:
                signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)
            finished.set()
            return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.1", 443))]

        def unblock() -> None:
            release.wait(0.75)
            os.write(write_fd, b"x")

        safety = Thread(target=unblock)
        safety.start()
        with pytest.MonkeyPatch.context() as patch:
            patch.setattr(socket, "getaddrinfo", stalled_resolver)
            connection = Mock(side_effect=AssertionError("post-deadline connection"))
            patch.setattr(socket, "socket", connection)
            previous = signal.getsignal(signal.SIGALRM)
            started = time.monotonic()
            try:
                with pytest.raises(BudgetExceeded), InvocationBudget().limit(maximum=0.06):
                    request()
                assert time.monotonic() - started < 0.18
                assert not finished.is_set()
                for _ in range(4):
                    with (
                        pytest.raises(BudgetExceeded, match="resolver unavailable"),
                        InvocationBudget().limit(maximum=0.1),
                    ):
                        request()
                assert time.monotonic() - started < 0.3
                assert len(calls) == 1
                workers = [
                    worker for worker in enumerate_threads() if worker.name == "ingestor-dns"
                ]
                assert len(workers) == 1
                release.set()
                assert finished.wait(0.5)
                for worker in workers:
                    worker.join(0.5)
                    assert not worker.is_alive()
                connection.assert_not_called()
                assert signal.getsignal(signal.SIGALRM) is previous
                assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
            finally:
                release.set()
                safety.join(0.5)
                for worker in enumerate_threads():
                    if worker.name == "ingestor-dns":
                        worker.join(0.5)
                close()
                os.close(read_fd)
                os.close(write_fd)

    isolated_process(check)
