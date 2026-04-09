from __future__ import annotations

import time
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")


class RetryService:
    def __init__(self, attempts: int = 3, base_delay_seconds: float = 0.1) -> None:
        self.attempts = attempts
        self.base_delay_seconds = base_delay_seconds

    def run(self, operation: Callable[[], T], on_retry: Callable[[int, Exception], None] | None = None) -> T:
        last_error: Exception | None = None
        for attempt in range(1, self.attempts + 1):
            try:
                return operation()
            except Exception as exc:  # noqa: BLE001
                last_error = exc
                if attempt == self.attempts:
                    raise
                if on_retry is not None:
                    on_retry(attempt, exc)
                time.sleep(self.base_delay_seconds * attempt)
        assert last_error is not None
        raise last_error
