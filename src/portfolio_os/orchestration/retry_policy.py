"""Retry policy for local batch orchestration."""

from __future__ import annotations

from collections.abc import Callable
from typing import TypeVar

from pydantic import BaseModel, Field


T = TypeVar("T")


class RetryPolicy(BaseModel):
    """Bounded retry policy for local candidate evaluation."""

    max_attempts: int = Field(default=1, ge=1)

    def run(self, func: Callable[[], T]) -> tuple[T, int]:
        """Run a callable with bounded retry attempts."""

        last_error: Exception | None = None
        for attempt in range(1, self.max_attempts + 1):
            try:
                return func(), attempt
            except Exception as exc:  # noqa: BLE001 - failure isolation is the point here.
                last_error = exc
        if last_error is None:
            raise RuntimeError("retry policy exhausted without an exception")
        raise last_error
