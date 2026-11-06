import signal
import time
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
from threading import current_thread, main_thread

SHUTDOWN_RESERVE_SECONDS = 0.5
PERSISTENCE_RESERVE_SECONDS = 4.0
SDK_OPERATION_SECONDS = 3.0
BUDGET_EXHAUSTED_MESSAGE = "Invocation time budget exhausted"


class BudgetExceeded(RuntimeError):
    """The invocation or operation has exhausted its wall-clock allowance."""


class OperationTimeout(BudgetExceeded):
    """Only the innermost operation cap expired; its caller may retry."""


@dataclass
class _Limit:
    deadline: float
    retryable: bool
    failure: BudgetExceeded | None = None


_limits: ContextVar[tuple[_Limit, ...]] = ContextVar("invocation_limits", default=())


def _expire(signum: int, frame: object) -> None:
    scopes = _limits.get()
    owner = min(scopes, key=lambda scope: scope.deadline)
    failure_type = OperationTimeout if owner is scopes[-1] and owner.retryable else BudgetExceeded
    owner.failure = failure_type(BUDGET_EXHAUSTED_MESSAGE)
    raise owner.failure


def operation_remaining() -> float:
    """Return the active main-thread deadline allowance for a bounded resolver wait.

    Raises:
        BudgetExceeded: If an enclosing allowance expired.
        RuntimeError: If no protected main-thread operation is active.
    """
    scopes = _limits.get()
    if current_thread() is not main_thread() or not scopes:
        raise RuntimeError("Resolution requires an active main-thread deadline")
    for scope in scopes:
        if scope.failure is not None:
            raise scope.failure
    remaining = min(scope.deadline for scope in scopes) - time.monotonic()
    if remaining <= 0:
        raise BudgetExceeded(BUDGET_EXHAUSTED_MESSAGE)
    return remaining


class InvocationBudget:
    """Bound synchronous work on the Lambda main thread, including trickling I/O.

    Args:
        remaining_time: Lambda's remaining-milliseconds callback; standalone calls
            default to a fresh 20-second allowance.
    """

    def __init__(self, remaining_time: Callable[[], int] | None = None) -> None:
        self._remaining_time = remaining_time
        seconds = 20.0 if remaining_time is None else remaining_time() / 1000
        self._deadline = time.monotonic() + seconds - SHUTDOWN_RESERVE_SECONDS

    def check(self, reserve: float = 0.0) -> float:
        """Return available seconds or raise before starting unaffordable work.

        Args:
            reserve: Time withheld for subsequent work.

        Returns:
            Remaining wall-clock allowance after reserves.

        Raises:
            BudgetExceeded: If no allowance remains.
        """
        now = time.monotonic()
        for scope in _limits.get():
            if scope.failure is not None:
                raise scope.failure
            if scope.deadline <= now:
                scope.failure = BudgetExceeded(BUDGET_EXHAUSTED_MESSAGE)
                raise scope.failure
        remaining = self._deadline - now
        if self._remaining_time is not None:
            remaining = min(remaining, self._remaining_time() / 1000 - SHUTDOWN_RESERVE_SECONDS)
        available = remaining - reserve
        if available <= 0:
            raise BudgetExceeded(BUDGET_EXHAUSTED_MESSAGE)
        return available

    @contextmanager
    def limit(self, reserve: float = 0.0, maximum: float | None = None) -> Iterator[None]:
        """Interrupt blocking work rather than treating socket timeouts as deadlines.

        Args:
            reserve: Time withheld for subsequent work.
            maximum: Optional operation wall-clock cap in seconds.

        Yields:
            Control while a POSIX real-time timer enforces the allowance.

        Raises:
            BudgetExceeded: If the operation or invocation allowance expires.
            RuntimeError: If called outside the main thread.
        """
        available = self.check(reserve)
        seconds = available
        if maximum is not None:
            seconds = min(seconds, maximum)
        if current_thread() is not main_thread():
            raise RuntimeError("Deadline enforcement requires the main thread")
        previous_handler = signal.getsignal(signal.SIGALRM)
        previous_seconds, previous_interval = signal.getitimer(signal.ITIMER_REAL)
        scopes = _limits.get()
        if previous_interval > 0 or (
            previous_seconds > 0 and (not scopes or previous_handler is not _expire)
        ):
            raise RuntimeError("Unowned active deadline timer")
        if previous_seconds > 0:
            seconds = min(seconds, previous_seconds)
        started = time.monotonic()
        scope = _Limit(
            started + seconds,
            maximum is not None
            and maximum < available
            and (not scopes or maximum < previous_seconds),
        )
        token = _limits.set((*scopes, scope))
        signal.signal(signal.SIGALRM, _expire)
        try:
            signal.setitimer(signal.ITIMER_REAL, seconds)
            yield
            if time.monotonic() - started >= seconds:
                _expire(signal.SIGALRM, None)
            self.check(reserve)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            _limits.reset(token)
            signal.signal(signal.SIGALRM, previous_handler)
            remaining_previous = max(0.0, previous_seconds - (time.monotonic() - started))
            if remaining_previous > 0 and not any(parent.failure for parent in scopes):
                signal.setitimer(signal.ITIMER_REAL, remaining_previous, previous_interval)
            for expired in (*scopes, scope):
                if expired.failure is not None:
                    raise expired.failure
