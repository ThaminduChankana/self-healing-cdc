"""Bounded retry with exponential backoff for transient infrastructure errors."""

from __future__ import annotations

import time
from typing import Callable, TypeVar

T = TypeVar("T")


class RetryExhaustedError(Exception):
    """Raised when all retry attempts failed. `last_error` holds the final cause."""

    def __init__(self, message: str, last_error: BaseException | None = None):
        super().__init__(message)
        self.last_error = last_error


def backoff_delay(attempt: int, base: float, maximum: float) -> float:
    """Delay before retry number `attempt` (1-based): base * 2^(attempt-1), capped."""
    if attempt < 1:
        return 0.0
    return min(base * (2 ** (attempt - 1)), maximum)


def retry_call(
    fn: Callable[[], T],
    *,
    attempts: int,
    base_delay: float,
    max_delay: float,
    retry_on: tuple[type[BaseException], ...] = (Exception,),
    on_retry: Callable[[int, BaseException, float], None] | None = None,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Call `fn` up to `attempts` times, sleeping with exponential backoff between tries.

    Only exceptions in `retry_on` are retried; anything else propagates at once.
    """
    if attempts < 1:
        raise ValueError("attempts must be >= 1")
    last: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except retry_on as exc:  # noqa: PERF203 - clarity over micro-optimisation
            last = exc
            if attempt == attempts:
                break
            delay = backoff_delay(attempt, base_delay, max_delay)
            if on_retry:
                on_retry(attempt, exc, delay)
            sleep(delay)
    raise RetryExhaustedError(f"Gave up after {attempts} attempts: {last}", last)
