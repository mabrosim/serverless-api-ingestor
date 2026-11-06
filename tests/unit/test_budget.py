import os
import signal
import time
from collections.abc import Callable
from unittest.mock import Mock

import pytest

from scheduled_ingestor.core import budget as budget_module
from scheduled_ingestor.core.budget import BudgetExceeded, InvocationBudget, operation_remaining


@pytest.mark.parametrize("operation", ["budget-check", "resolver-wait"])
def test_elapsed_scope_rejects_work_and_clears_failure_on_unwind(
    monkeypatch: pytest.MonkeyPatch,
    isolated_process: Callable[[Callable[[], None]], None],
    operation: str,
) -> None:
    def check() -> None:
        clock = Mock(return_value=0.0)
        monkeypatch.setattr(budget_module.time, "monotonic", clock)
        previous = signal.getsignal(signal.SIGALRM)
        budget = InvocationBudget()

        def exercise_expired_scope() -> None:
            with budget.limit(maximum=1.0):
                clock.return_value = 2.0
                fresh_budget = InvocationBudget()
                action = operation_remaining if operation == "resolver-wait" else fresh_budget.check
                with pytest.raises(BudgetExceeded, match="Invocation time budget exhausted"):
                    action()
                with pytest.raises(BudgetExceeded) as caught:
                    budget.check()
                repeated_budget = InvocationBudget()
                with pytest.raises(BudgetExceeded) as repeated:
                    repeated_budget.check()
                assert repeated.value is caught.value
                with pytest.raises(BudgetExceeded) as resolver_failure:
                    operation_remaining()
                assert resolver_failure.value is caught.value

        with pytest.raises(BudgetExceeded, match="Invocation time budget exhausted"):
            exercise_expired_scope()

        assert budget_module._limits.get() == ()
        assert signal.getsignal(signal.SIGALRM) is previous
        assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
        assert InvocationBudget().check() == pytest.approx(19.5)

    isolated_process(check)


def test_unowned_periodic_timer_is_rejected_without_changing_expiry(
    isolated_process: Callable[[Callable[[], None]], None],
) -> None:
    def check() -> None:
        read_fd, write_fd = os.pipe()
        expiries: list[float] = []
        previous = signal.getsignal(signal.SIGALRM)

        def expire(signum: int, frame: object) -> None:
            expiries.append(time.monotonic())
            os.write(write_fd, b"x")

        signal.signal(signal.SIGALRM, expire)
        signal.setitimer(signal.ITIMER_REAL, 0.08, 0.04)
        try:
            with (
                pytest.raises(RuntimeError, match="Unowned"),
                InvocationBudget().limit(maximum=0.01),
            ):
                raise AssertionError("work started under unowned timer")
            assert signal.getsignal(signal.SIGALRM) is expire
            assert signal.getitimer(signal.ITIMER_REAL)[1] == pytest.approx(0.04)
            assert os.read(read_fd, 1) == b"x"
            assert os.read(read_fd, 1) == b"x"
            assert len(expiries) == 2
            assert expiries[1] - expiries[0] == pytest.approx(0.04, abs=0.025)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
            os.close(read_fd)
            os.close(write_fd)

    isolated_process(check)


@pytest.mark.parametrize("outcome", ["success", "error", "expiry"])
def test_inactive_handler_is_restored_after_operation(
    isolated_process: Callable[[Callable[[], None]], None], outcome: str
) -> None:
    def check() -> None:
        previous = signal.getsignal(signal.SIGALRM)

        def handler(signum: int, frame: object) -> None:
            pass

        signal.signal(signal.SIGALRM, handler)
        read_fd, write_fd = os.pipe()
        try:
            expected = BudgetExceeded if outcome == "expiry" else ValueError
            if outcome == "success":
                with InvocationBudget().limit(maximum=0.04):
                    pass
            else:
                with pytest.raises(expected), InvocationBudget().limit(maximum=0.04):
                    if outcome == "error":
                        raise ValueError("synthetic failure")
                    os.read(read_fd, 1)
            assert signal.getsignal(signal.SIGALRM) is handler
            assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
            os.close(read_fd)
            os.close(write_fd)

    isolated_process(check)


def test_nested_kernel_expiry_stays_failed_if_inner_error_is_caught(
    isolated_process: Callable[[Callable[[], None]], None],
) -> None:
    def check() -> None:
        read_fd, write_fd = os.pipe()
        previous = signal.getsignal(signal.SIGALRM)
        budget = InvocationBudget()
        started = time.monotonic()
        try:
            with pytest.raises(BudgetExceeded), budget.limit(maximum=0.06):
                with pytest.raises(BudgetExceeded), budget.limit(maximum=0.2):
                    os.read(read_fd, 1)
                with budget.limit(maximum=0.2):
                    raise AssertionError("expired enclosing scope admitted work")
            assert time.monotonic() - started < 0.3
            assert signal.getsignal(signal.SIGALRM) is previous
            assert signal.getitimer(signal.ITIMER_REAL) == (0.0, 0.0)
        finally:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, previous)
            os.close(read_fd)
            os.close(write_fd)

    isolated_process(check)
