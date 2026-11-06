import os
import signal
import socket
import time
import warnings
from collections.abc import Callable
from threading import Event, Thread
from threading import enumerate as enumerate_threads
from unittest.mock import Mock, call

import boto3
import pytest
import urllib3
from botocore.config import Config
from botocore.exceptions import HTTPClientError

from scheduled_ingestor.core import budget as budget_module
from scheduled_ingestor.core.budget import BudgetExceeded, InvocationBudget, operation_remaining
from scheduled_ingestor.core.http import HttpClient
from scheduled_ingestor.core.transport import _HTTPSConnection, _Resolver, bound_sdk_client


def test_invalid_dns_hostname_rejects_before_resolution_or_socket_creation(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = Mock(side_effect=AssertionError("invalid hostname reached DNS"))
    sockets = Mock(side_effect=AssertionError("invalid hostname opened socket"))
    monkeypatch.setattr("scheduled_ingestor.core.transport._resolver.resolve", resolver)
    monkeypatch.setattr(socket, "socket", sockets)
    connection = _HTTPSConnection("a" * 64 + ".example.com", 443)

    with pytest.raises(urllib3.exceptions.LocationParseError, match="Invalid DNS hostname"):
        connection._new_conn()

    resolver.assert_not_called()
    sockets.assert_not_called()


@pytest.mark.parametrize(
    ("failure", "expected", "message"),
    [
        (TimeoutError("synthetic timeout"), urllib3.exceptions.ConnectTimeoutError, "timed out"),
        (OSError("synthetic refusal"), urllib3.exceptions.NewConnectionError, "establish"),
    ],
)
def test_connection_failure_translates_error_and_closes_failed_socket(
    monkeypatch: pytest.MonkeyPatch,
    failure: OSError,
    expected: type[Exception],
    message: str,
) -> None:
    addresses = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.1", 443))]
    resolver = Mock(return_value=addresses)
    connected = Mock()
    connected.connect.side_effect = failure
    monkeypatch.setattr("scheduled_ingestor.core.transport._resolver.resolve", resolver)
    monkeypatch.setattr(socket, "socket", Mock(return_value=connected))
    connection = _HTTPSConnection("api.example.com", 443, timeout=1.0)

    with InvocationBudget().limit(maximum=1.0), pytest.raises(expected, match=message) as caught:
        connection._new_conn()

    assert caught.value.__cause__ is failure
    resolver.assert_called_once_with("api.example.com", 443, socket.AF_UNSPEC)
    connected.connect.assert_called_once_with(("192.0.2.1", 443))
    connected.close.assert_called_once()


def test_connection_preserves_dns_host_address_order_ipv6_scope_and_socket_options(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    addresses = [
        (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("2001:db8::1", 443, 0, 2)),
        (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.1", 443)),
    ]
    resolver = Mock(return_value=addresses)
    monkeypatch.setattr("scheduled_ingestor.core.transport._resolver.resolve", resolver)
    first, second = Mock(), Mock()
    first.connect.side_effect = OSError("synthetic connection refusal")
    sockets = Mock(side_effect=[first, second])
    monkeypatch.setattr(socket, "socket", sockets)
    connection = _HTTPSConnection("api.example.com.", 443, timeout=1.0)

    with InvocationBudget().limit(maximum=0.2):
        assert connection._new_conn() is second

    assert resolver.call_args.args[:2] == ("api.example.com.", 443)
    assert sockets.call_args_list == [
        call(socket.AF_INET6, socket.SOCK_STREAM, 6),
        call(socket.AF_INET, socket.SOCK_STREAM, 6),
    ]
    first.connect.assert_called_once_with(("2001:db8::1", 443, 0, 2))
    first.close.assert_called_once()
    second.connect.assert_called_once_with(("192.0.2.1", 443))
    second.settimeout.assert_called_once_with(1.0)
    second.setsockopt.assert_called_once_with(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)
    assert connection.host == "api.example.com"


@pytest.mark.parametrize("source_tail", [(), (17, 7)])
def test_source_bind_resolution_uses_same_budget_and_family(
    monkeypatch: pytest.MonkeyPatch,
    source_tail: tuple[int, ...],
) -> None:
    destination = [(socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("2001:db8::1", 443, 0, 2))]
    source = [(socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", 0, 0, 0))]
    lookup = Mock(side_effect=[destination, source])
    resolver = _Resolver()
    monkeypatch.setattr(socket, "getaddrinfo", lookup)
    monkeypatch.setattr("scheduled_ingestor.core.transport._resolver", resolver)
    native_socket = socket.socket
    expected_source = ("::1", 0, *source_tail) if source_tail else source[0][4]

    class BindOnlySocket(native_socket):
        def bind(self, address: tuple[object, ...]) -> None:
            assert address == expected_source
            super().bind(address)

        def connect(self, address: tuple[object, ...]) -> None:
            assert self.getsockname()[0] == "::1"

    monkeypatch.setattr(socket, "socket", BindOnlySocket)
    connection = _HTTPSConnection(
        "api.example.com", timeout=1.0, source_address=("source.example.com", 0, *source_tail)
    )

    with InvocationBudget().limit(maximum=0.2):
        connected = connection._new_conn()
    connected.close()

    assert lookup.call_args.args == (
        "source.example.com",
        0,
        socket.AF_INET6,
        socket.SOCK_STREAM,
        0,
        0,
    )
    assert lookup.call_count == 2
    assert not resolver._worker.is_alive()
    assert resolver._job.result is None


@pytest.mark.parametrize(
    ("family", "source"),
    [
        (socket.AF_INET, ("", 0)),
        (socket.AF_INET, ("127.0.0.1", 0)),
        (socket.AF_INET6, ("", 0)),
        (socket.AF_INET6, ("::1", 0)),
        (socket.AF_INET6, ("::", 0, 17, 7)),
        (socket.AF_INET6, ("::%7", 0, 17, 7)),
    ],
)
def test_literal_source_bind_matches_native_without_source_dns_or_network_connect(
    monkeypatch: pytest.MonkeyPatch, family: int, source: tuple[object, ...]
) -> None:
    native_socket = socket.socket
    with native_socket(family, socket.SOCK_STREAM, 6) as reference:
        reference.bind(source)
        expected = reference.getsockname()

    class BindOnlySocket(native_socket):
        def bind(self, address: tuple[object, ...]) -> None:
            assert address == source
            super().bind(address)

        def connect(self, address: tuple[object, ...]) -> None:
            actual = self.getsockname()
            assert actual[0] == expected[0]
            assert actual[2:] == expected[2:]
            assert actual[1] > 0
            assert self.getsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY) == 0
            assert self.getsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE) != 0

    destination = [(family, socket.SOCK_STREAM, 6, "", expected)]
    resolver = Mock(side_effect=[destination])
    monkeypatch.setattr("scheduled_ingestor.core.transport._resolver.resolve", resolver)
    monkeypatch.setattr(socket, "socket", BindOnlySocket)
    connection = _HTTPSConnection(
        "api.example.com",
        timeout=1.0,
        source_address=source,
        socket_options=[
            (socket.IPPROTO_TCP, socket.TCP_NODELAY, 0),
            (socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1),
        ],
    )

    with InvocationBudget().limit(maximum=0.2):
        connected = connection._new_conn()
    connected.close()
    resolver.assert_called_once()


@pytest.mark.parametrize("hostname", ["api.example.com.", "2001:db8::1"])
def test_real_https_connect_preserves_tls_hostname_and_verification(
    monkeypatch: pytest.MonkeyPatch, hostname: str
) -> None:
    connection = _HTTPSConnection(hostname, 443, timeout=1.0, cert_reqs="CERT_REQUIRED")
    connected = Mock()
    monkeypatch.setattr(connection, "_new_conn", Mock(return_value=connected))
    handshake = Mock(return_value=(connected, True))
    monkeypatch.setattr("urllib3.connection._ssl_wrap_socket_and_match_hostname", handshake)

    connection.connect()

    assert handshake.call_args.kwargs["server_hostname"] == hostname.rstrip(".")
    assert handshake.call_args.kwargs["cert_reqs"] == "CERT_REQUIRED"
    assert connection.is_verified


@pytest.mark.parametrize(
    "proxy", ["http://proxy.example.com:8080", "https://proxy.example.com:8443"]
)
@pytest.mark.parametrize("forwarding", [False, True])
def test_sdk_proxy_pools_preserve_routing_hosts_tls_config_and_reuse(
    proxy: str, forwarding: bool
) -> None:
    client = boto3.client(
        "ssm",
        endpoint_url="https://sdk.example.com",
        config=Config(
            proxies={"https": proxy},
            proxies_config={"proxy_use_forwarding_for_https": forwarding},
        ),
    )
    session = client._endpoint.http_session
    manager = session._manager
    timeout = session._timeout
    bound_sdk_client(client)
    try:
        assert session is client._endpoint.http_session
        assert session._manager is manager and session._timeout is timeout
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore",
                message="Passing ssl_context when use_forwarding_for_https",
                category=FutureWarning,
            )
            proxy_manager = session._get_proxy_manager(proxy)
        assert session._get_proxy_manager(proxy) is proxy_manager
        pool = proxy_manager.connection_from_url("https://sdk.example.com/path")
        connection = pool._new_conn()
        assert isinstance(connection, urllib3.connection.HTTPSConnection)
        assert connection._new_conn.__func__ is _HTTPSConnection._new_conn
        assert connection.proxy.host == "proxy.example.com"
        assert connection.proxy_config.use_forwarding_for_https is forwarding
        session._setup_ssl_cert(pool, "https://sdk.example.com", session._verify)
        assert pool.cert_reqs == "CERT_REQUIRED"
        expected = (
            "https://sdk.example.com/path" if forwarding and proxy.startswith("https:") else "/path"
        )
        assert session._get_request_target("https://sdk.example.com/path", proxy) == expected
    finally:
        client.close()


def test_sdk_budget_unwrap_preserves_unrelated_transport_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = boto3.client("ssm", config=Config(proxies={}))
    session = client._endpoint.http_session
    failure = HTTPClientError(error=ValueError("synthetic failure"))
    monkeypatch.setattr(session, "send", Mock(side_effect=failure))
    bound_sdk_client(client)
    try:
        with pytest.raises(HTTPClientError) as caught:
            session.send(Mock())
        assert caught.value is failure
    finally:
        client.close()


def test_resolver_requires_active_main_thread_scope() -> None:
    with pytest.raises(RuntimeError, match="active main-thread"):
        _Resolver().resolve("api.example.com", 443, socket.AF_UNSPEC)
    with pytest.raises(RuntimeError, match="active main-thread"):
        operation_remaining()


def test_interrupted_worker_launch_cannot_allocate_replacement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = _Resolver()
    worker = Mock()
    worker.is_alive.return_value = False
    worker.start.side_effect = BudgetExceeded("synthetic launch interruption")
    constructor = Mock(return_value=worker)
    monkeypatch.setattr("scheduled_ingestor.core.transport.Thread", constructor)

    with InvocationBudget().limit(maximum=0.2):
        with pytest.raises(BudgetExceeded, match="launch interruption"):
            resolver.resolve("api.example.com", 443, socket.AF_UNSPEC)
        with pytest.raises(BudgetExceeded, match="resolver unavailable"):
            resolver.resolve("api.example.com", 443, socket.AF_UNSPEC)
    constructor.assert_called_once()


@pytest.mark.parametrize(
    "result", [[], [(socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", 443, 0, 2))]]
)
def test_resolver_reuses_one_slot_only_after_worker_exit(
    monkeypatch: pytest.MonkeyPatch, result: list[tuple[object, ...]]
) -> None:
    resolver = _Resolver()
    lookup = Mock(return_value=result)
    monkeypatch.setattr(socket, "getaddrinfo", lookup)

    with InvocationBudget().limit(maximum=0.2):
        assert resolver.resolve("api.example.com.", 443, socket.AF_UNSPEC) == result
        assert not resolver._worker.is_alive()
        assert resolver._job.result is None
        assert resolver.resolve("api.example.com.", 443, socket.AF_UNSPEC) == result
        assert not resolver._worker.is_alive()
    assert lookup.call_args_list == [
        call("api.example.com.", 443, socket.AF_UNSPEC, socket.SOCK_STREAM, 0, 0),
        call("api.example.com.", 443, socket.AF_UNSPEC, socket.SOCK_STREAM, 0, 0),
    ]


def test_resolver_failure_preserves_dns_error_without_retaining_result(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    resolver = _Resolver()
    failure = socket.gaierror("synthetic resolution failure")
    monkeypatch.setattr(socket, "getaddrinfo", Mock(side_effect=failure))

    with InvocationBudget().limit(maximum=0.2), pytest.raises(socket.gaierror) as caught:
        resolver.resolve("api.example.com", 443, socket.AF_UNSPEC)
    assert caught.value is failure
    assert not resolver._worker.is_alive()
    assert resolver._job.error is None


def test_dns_wait_timeout_discards_late_result_and_reaps_worker_when_alarm_is_blocked(
    monkeypatch: pytest.MonkeyPatch,
    isolated_process: Callable[[Callable[[], None]], None],
) -> None:
    def check() -> None:
        resolver = _Resolver()
        read_fd, write_fd = os.pipe()
        entered = Event()
        result = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.1", 443))]

        def lookup(*args: object) -> list[tuple[object, ...]]:
            entered.set()
            assert os.read(read_fd, 1) == b"x"
            return result

        monkeypatch.setattr(socket, "getaddrinfo", lookup)
        previous_mask = signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
        try:
            with (
                pytest.raises(BudgetExceeded, match="Invocation time budget exhausted"),
                InvocationBudget().limit(maximum=0.06),
                pytest.raises(BudgetExceeded, match="DNS resolution time budget exhausted"),
            ):
                resolver.resolve("api.example.com", 443, socket.AF_UNSPEC)

            assert entered.is_set()
            worker, job = resolver._worker, resolver._job
            assert worker.is_alive() and not job.done.is_set()
            assert job.abandoned and job.result is None and job.error is None
            with (
                InvocationBudget().limit(maximum=0.2),
                pytest.raises(BudgetExceeded, match="resolver unavailable"),
            ):
                resolver.resolve("replacement.example.com", 443, socket.AF_UNSPEC)
            assert resolver._worker is worker and resolver._job is job
        finally:
            os.write(write_fd, b"x")
            if resolver._worker is not None:
                resolver._worker.join(0.5)
                assert not resolver._worker.is_alive()
            os.close(read_fd)
            os.close(write_fd)
            signal.setitimer(signal.ITIMER_REAL, 0)
            if signal.SIGALRM in signal.sigpending():
                assert signal.sigwait({signal.SIGALRM}) == signal.SIGALRM
            signal.pthread_sigmask(signal.SIG_SETMASK, previous_mask)

        assert job.done.is_set() and job.result is None and job.error is None
        assert budget_module._limits.get() == ()
        assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
        monkeypatch.setattr(socket, "getaddrinfo", Mock(return_value=result))
        with InvocationBudget().limit(maximum=0.2):
            assert resolver.resolve("recovered.example.com", 443, socket.AF_UNSPEC) == result
        assert not resolver._worker.is_alive()

    isolated_process(check)


def test_resolution_completion_after_deadline_cannot_open_socket(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = Mock(return_value=0.0)
    monkeypatch.setattr("scheduled_ingestor.core.budget.time.monotonic", clock)

    def delayed_resolution(*args: object) -> list[tuple[object, ...]]:
        clock.return_value = 1.0
        return [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.1", 443))]

    monkeypatch.setattr("scheduled_ingestor.core.transport._resolver.resolve", delayed_resolution)
    connected = Mock(side_effect=AssertionError("post-deadline connection"))
    monkeypatch.setattr(socket, "socket", connected)

    with pytest.raises(BudgetExceeded), InvocationBudget().limit(maximum=0.2):
        HttpClient()._pool.connection_from_url("https://api.example.com")._new_conn()._new_conn()
    connected.assert_not_called()


@pytest.mark.parametrize("expiry", ["join-timeout", "sigalrm"])
def test_completed_dns_worker_retirement_blocks_replacement_until_pipe_gated_exit(
    monkeypatch: pytest.MonkeyPatch,
    isolated_process: Callable[[Callable[[], None]], None],
    expiry: str,
) -> None:
    def check() -> None:
        resolver = _Resolver()
        result = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.1", 443))]
        lookup = Mock(return_value=result)
        monkeypatch.setattr(socket, "getaddrinfo", lookup)
        read_fd, write_fd = os.pipe()
        retirement_entered = Event()
        retirement_exited = Event()
        joins: list[float] = []
        alarms: list[int] = []
        original_expire = budget_module._expire

        def expire(signum: int, frame: object) -> None:
            assert retirement_entered.is_set() and not retirement_exited.is_set()
            assert joins
            alarms.append(signum)
            original_expire(signum, frame)

        class RetiringThread(Thread):
            def run(self) -> None:
                try:
                    super().run()
                    retirement_entered.set()
                    assert os.read(read_fd, 1) == b"x"
                finally:
                    retirement_exited.set()

            def join(self, timeout: float | None = None) -> None:
                assert timeout is not None and 0 < timeout <= 0.06
                assert retirement_entered.wait(timeout)
                assert resolver._job.done.is_set()
                assert resolver._job.result == result
                joins.append(timeout)
                if expiry == "sigalrm":
                    signal.setitimer(signal.ITIMER_REAL, timeout / 2)
                    super().join(timeout)
                else:
                    super().join(timeout / 2)

        constructor = Mock(side_effect=RetiringThread)
        monkeypatch.setattr("scheduled_ingestor.core.transport.Thread", constructor)
        monkeypatch.setattr(budget_module, "_expire", expire)
        try:
            started = time.monotonic()
            with pytest.raises(BudgetExceeded), InvocationBudget().limit(maximum=0.06):
                resolver.resolve("api.example.com", 443, socket.AF_UNSPEC)
            assert time.monotonic() - started < 0.18
            worker, job = resolver._worker, resolver._job
            assert worker.is_alive()
            assert job.done.is_set() and job.abandoned
            assert job.result is None and job.error is None
            assert alarms == ([signal.SIGALRM] if expiry == "sigalrm" else [])
            started = time.monotonic()
            for _ in range(4):
                with (
                    InvocationBudget().limit(maximum=0.06),
                    pytest.raises(BudgetExceeded, match="resolver unavailable"),
                ):
                    resolver.resolve("replacement.example.com", 443, socket.AF_UNSPEC)
                assert resolver._worker is worker and resolver._job is job
            assert time.monotonic() - started < 0.18
            constructor.assert_called_once()
            lookup.assert_called_once()
            assert [thread for thread in enumerate_threads() if thread.name == "ingestor-dns"] == [
                worker
            ]
        finally:
            os.write(write_fd, b"x")
            if resolver._worker is not None:
                Thread.join(resolver._worker, 0.5)
                assert not resolver._worker.is_alive()
            os.close(read_fd)
            os.close(write_fd)
        assert retirement_exited.is_set()
        monkeypatch.setattr("scheduled_ingestor.core.transport.Thread", Thread)
        with InvocationBudget().limit(maximum=0.2):
            assert resolver.resolve("recovered.example.com", 443, socket.AF_UNSPEC) == result
        assert resolver._worker is not worker and not resolver._worker.is_alive()
        assert resolver._job.result is None and resolver._job.error is None
        assert lookup.call_count == 2

    isolated_process(check)


@pytest.mark.parametrize("source", [("127.0.0.1", -1), ("source.example.com", 0)])
def test_invalid_source_failure_closes_socket_and_retires_resolver(
    monkeypatch: pytest.MonkeyPatch, source: tuple[str, int]
) -> None:
    resolver = _Resolver()
    destination = [(socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.1", 443))]
    lookup = Mock(side_effect=[destination, socket.gaierror("synthetic invalid source")])
    monkeypatch.setattr(socket, "getaddrinfo", lookup)
    monkeypatch.setattr("scheduled_ingestor.core.transport._resolver", resolver)
    native_socket = socket.socket
    created: list[socket.socket] = []

    class NoConnectSocket(native_socket):
        def __init__(self, *args: object) -> None:
            super().__init__(*args)
            created.append(self)

        def connect(self, address: tuple[object, ...]) -> None:
            pytest.fail("invalid source admitted network connect")

    monkeypatch.setattr(socket, "socket", NoConnectSocket)
    connection = _HTTPSConnection("api.example.com", timeout=1.0, source_address=source)
    expected_error = (
        OverflowError if source[0] == "127.0.0.1" else urllib3.exceptions.NameResolutionError
    )
    with InvocationBudget().limit(maximum=0.2), pytest.raises(expected_error):
        connection._new_conn()
    assert len(created) == 1 and created[0].fileno() == -1
    assert not resolver._worker.is_alive()
    assert resolver._job.done.is_set() and resolver._job.abandoned
    assert resolver._job.result is None and resolver._job.error is None
    monkeypatch.setattr(socket, "getaddrinfo", Mock(return_value=destination))
    with InvocationBudget().limit(maximum=0.2):
        assert resolver.resolve("recovered.example.com", 443, socket.AF_UNSPEC) == destination
