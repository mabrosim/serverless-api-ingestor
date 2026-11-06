import signal
import socket
import sys
from dataclasses import dataclass, field
from ipaddress import ip_address
from threading import Event, Thread
from typing import Any

import urllib3
from botocore.awsrequest import (
    AWSHTTPConnection,
    AWSHTTPConnectionPool,
    AWSHTTPSConnection,
    AWSHTTPSConnectionPool,
)
from botocore.exceptions import HTTPClientError
from urllib3.exceptions import (
    ConnectTimeoutError,
    LocationParseError,
    NameResolutionError,
    NewConnectionError,
)
from urllib3.util.connection import allowed_gai_family

from scheduled_ingestor.core.budget import BudgetExceeded, operation_remaining


@dataclass
class _Resolution:
    done: Event = field(default_factory=Event)
    result: list[Any] | None = None
    error: Exception | None = None
    abandoned: bool = False


class _Resolver:
    def __init__(self) -> None:
        self._worker: Thread | None = None
        self._job: _Resolution | None = None

    def resolve(self, host: str, port: int, family: int, flags: int = 0) -> list[Any]:
        operation_remaining()
        if self._job is not None and (
            not self._job.done.is_set() or (self._worker is not None and self._worker.is_alive())
        ):
            raise BudgetExceeded("DNS resolver unavailable while previous lookup finishes")
        job = _Resolution()
        self._job = job
        lookup = socket.getaddrinfo

        def work() -> None:
            signal.pthread_sigmask(signal.SIG_BLOCK, {signal.SIGALRM})
            try:
                result = lookup(host, port, family, socket.SOCK_STREAM, 0, flags)
                if not job.abandoned:
                    job.result = result
            except Exception as error:
                if not job.abandoned:
                    job.error = error.with_traceback(None)
            finally:
                job.done.set()

        self._worker = Thread(target=work, name="ingestor-dns", daemon=True)
        try:
            self._worker.start()
            if not job.done.wait(operation_remaining()):
                raise BudgetExceeded("DNS resolution time budget exhausted")
            self._worker.join(operation_remaining())
            if self._worker.is_alive():
                raise BudgetExceeded("DNS resolution time budget exhausted")
            operation_remaining()
            if job.error is not None:
                raise job.error
            return job.result or []
        finally:
            job.abandoned = True
            job.result = None
            job.error = None


_resolver = _Resolver()


class _BoundedConnection:
    def _new_conn(self) -> socket.socket:
        host = self._dns_host.strip("[]")
        try:
            host.encode("idna")
        except UnicodeError:
            raise LocationParseError("Invalid DNS hostname") from None
        try:
            addresses = _resolver.resolve(host, self.port, allowed_gai_family())
            connection = self._connect_addresses(addresses)
        except socket.gaierror as error:
            raise NameResolutionError(self.host, self, error) from error
        except TimeoutError as error:
            raise ConnectTimeoutError(self, "Connection timed out") from error
        except OSError as error:
            raise NewConnectionError(self, "Failed to establish a connection") from error
        sys.audit("http.client.connect", self, self.host, self.port)
        return connection

    def _connect_addresses(self, addresses: list[Any]) -> socket.socket:
        failure: OSError = OSError("getaddrinfo returns an empty list")
        for family, kind, protocol, _, address in addresses:
            operation_remaining()
            connection = socket.socket(family, kind, protocol)
            try:
                for option in self.socket_options or ():
                    connection.setsockopt(*option)
                connection.settimeout(self.timeout)
                if self.source_address:
                    self._bind_source(connection, family)
                operation_remaining()
                connection.connect(address)
                return connection
            except OSError as error:
                failure = error
                connection.close()
            except BaseException:
                connection.close()
                raise
        raise failure

    def _bind_source(self, connection: socket.socket, family: int) -> None:
        source = self.source_address
        source_host = source[0]
        if source_host:
            try:
                ip_address(source_host.partition("%")[0])
            except ValueError:
                sources = _resolver.resolve(source_host, source[1], family)
                source = sources[0][4]
                if len(self.source_address) > 2:
                    source = source[:2] + self.source_address[2:]
        connection.bind(source)


class _HTTPConnection(_BoundedConnection, urllib3.connection.HTTPConnection):
    pass


class _HTTPSConnection(_BoundedConnection, urllib3.connection.HTTPSConnection):
    pass


class _AWSHTTPConnection(_BoundedConnection, AWSHTTPConnection):
    pass


class _AWSHTTPSConnection(_BoundedConnection, AWSHTTPSConnection):
    pass


class _HTTPPool(urllib3.HTTPConnectionPool):
    ConnectionCls = _HTTPConnection


class _HTTPSPool(urllib3.HTTPSConnectionPool):
    ConnectionCls = _HTTPSConnection


class _AWSHTTPPool(AWSHTTPConnectionPool):
    ConnectionCls = _AWSHTTPConnection


class _AWSHTTPSPool(AWSHTTPSConnectionPool):
    ConnectionCls = _AWSHTTPSConnection


def bound_http_pool(pool: urllib3.PoolManager) -> None:
    """Install DNS-bounded connection classes on this reusable manager only.

    Args:
        pool: Newly composed provider pool; URLs, proxy routing and TLS stay unchanged.
    """
    pool.pool_classes_by_scheme = {"http": _HTTPPool, "https": _HTTPSPool}


def bound_sdk_client(client: Any) -> None:
    """Bound DNS on a newly composed SDK client without replacing its session/config.

    Args:
        client: Botocore client whose direct and proxy managers share bounded pool classes.
    """
    session = client._endpoint.http_session
    session._pool_classes_by_scheme = {"http": _AWSHTTPPool, "https": _AWSHTTPSPool}
    session._manager.pool_classes_by_scheme = session._pool_classes_by_scheme
    original_send = session.send

    def send(request: Any) -> Any:
        try:
            return original_send(request)
        except HTTPClientError as error:
            failure = error.kwargs.get("error")
            if isinstance(failure, BudgetExceeded):
                raise failure from None
            raise

    session.send = send
