import json
import time
from contextlib import suppress
from decimal import Decimal
from typing import Any
from urllib.parse import urlsplit

import urllib3

from scheduled_ingestor.core.budget import (
    PERSISTENCE_RESERVE_SECONDS,
    BudgetExceeded,
    InvocationBudget,
    OperationTimeout,
)
from scheduled_ingestor.core.transport import bound_http_pool

MAX_ATTEMPTS = 3
ATTEMPT_SECONDS = 4.0
MAX_RETRY_DELAY_SECONDS = 1.0
MAX_BODY_BYTES = 1_000_000
RETRYABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


def _reject_constant(value: str) -> None:
    raise ValueError("Provider JSON contains a nonstandard numeric constant")


class HttpClient:
    """Bounded JSON HTTP client with retries and explicit connection timeouts."""

    def __init__(self) -> None:
        self._pool = urllib3.PoolManager(
            timeout=urllib3.Timeout(connect=2.0, read=3.0),
            retries=False,
        )
        bound_http_pool(self._pool)
        self._budget: InvocationBudget | None = None
        self._attempts = 0

    def set_budget(self, budget: InvocationBudget | None) -> None:
        """Bind an invocation budget, or clear it after a reused service finishes.

        Args:
            budget: Shared invocation allowance; None enables standalone requests.
        """
        self._budget = budget
        self._attempts = 0

    def get_json(self, url: str, headers: dict[str, str] | None = None) -> Any:
        """Fetch and decode a bounded JSON response.

        Args:
            url: HTTPS provider endpoint.
            headers: Optional request headers.

        Returns:
            Decoded JSON payload.

        Raises:
            ValueError: If the provider URL is not HTTPS.
            RuntimeError: If HTTP attempts fail or any response exceeds the size limit.
            BudgetExceeded: If an attempt or invocation exhausts its time allowance.
            json.JSONDecodeError: If the response body is not valid JSON.
        """
        if urlsplit(url).scheme.lower() != "https":
            raise ValueError("Provider URL must use HTTPS")
        budget = self._budget or InvocationBudget()
        for attempt in range(MAX_ATTEMPTS):
            last_attempt = self._start_attempt(budget, attempt)
            status, body, retry_after = self._request_or_retry(url, headers, budget, last_attempt)
            if status is not None:
                if status < 300:
                    with budget.limit(PERSISTENCE_RESERVE_SECONDS):
                        return self._decode(body)
                if status not in RETRYABLE_STATUSES or last_attempt:
                    raise RuntimeError(f"Provider returned HTTP {status}")
            delay = self._retry_delay(retry_after, attempt)
            if delay >= budget.check(PERSISTENCE_RESERVE_SECONDS):
                raise BudgetExceeded("Provider retry exceeds invocation time budget")
            with budget.limit(PERSISTENCE_RESERVE_SECONDS):
                time.sleep(delay)

    def _start_attempt(self, budget: InvocationBudget, attempt: int) -> bool:
        budget.check(PERSISTENCE_RESERVE_SECONDS)
        if self._budget is not None:
            if self._attempts >= MAX_ATTEMPTS:
                raise RuntimeError("Provider HTTP attempt budget exhausted")
            self._attempts += 1
        return attempt == MAX_ATTEMPTS - 1 or self._attempts == MAX_ATTEMPTS

    def _request_or_retry(
        self,
        url: str,
        headers: dict[str, str] | None,
        budget: InvocationBudget,
        last_attempt: bool,
    ) -> tuple[int | None, bytes, str | None]:
        try:
            return self._request(url, headers, budget)
        except OperationTimeout:
            if last_attempt:
                raise BudgetExceeded("Provider HTTP time budget exhausted") from None
        except (urllib3.exceptions.HTTPError, OSError):
            if last_attempt:
                raise RuntimeError("Provider HTTP request failed") from None
        budget.check(PERSISTENCE_RESERVE_SECONDS)
        return None, b"", None

    def _request(
        self, url: str, headers: dict[str, str] | None, budget: InvocationBudget
    ) -> tuple[int, bytes, str | None]:
        seconds = min(ATTEMPT_SECONDS, budget.check(PERSISTENCE_RESERVE_SECONDS))
        with budget.limit(PERSISTENCE_RESERVE_SECONDS, ATTEMPT_SECONDS):
            response = self._pool.request(
                "GET",
                url,
                headers={"User-Agent": "serverless-api-ingestor/0.1", **(headers or {})},
                timeout=urllib3.Timeout(
                    total=seconds, connect=min(2.0, seconds), read=min(3.0, seconds)
                ),
                preload_content=False,
                redirect=False,
                retries=False,
            )
            try:
                body = response.read(MAX_BODY_BYTES + 1)
                if len(body) > MAX_BODY_BYTES:
                    raise RuntimeError("Provider response exceeds 1 MB limit")
                return response.status, body, response.headers.get("Retry-After")
            finally:
                try:
                    response.close()
                finally:
                    response.release_conn()

    @staticmethod
    def _retry_delay(header: str | None, attempt: int) -> float:
        delay = 0.2 * 2**attempt
        if header is not None:
            with suppress(urllib3.exceptions.InvalidHeader, ValueError, OverflowError):
                delay = urllib3.Retry().parse_retry_after(header)
        return min(MAX_RETRY_DELAY_SECONDS, max(0.0, delay))

    @staticmethod
    def _decode(body: bytes) -> Any:
        try:
            return json.loads(
                body.decode("utf-8"), parse_float=Decimal, parse_constant=_reject_constant
            )
        except json.JSONDecodeError:
            raise json.JSONDecodeError("Provider response is not valid JSON", "", 0) from None
        except UnicodeDecodeError:
            raise ValueError("Provider response is not UTF-8 JSON") from None
