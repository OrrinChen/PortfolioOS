"""Structured trace event primitives."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


FORBIDDEN_TRACE_KEY_MARKERS = (
    "api-key",
    "apikey",
    "broker-output",
    "credential",
    "live-performance",
    "order",
    "password",
    "secret",
    "token",
    "trading-instruction",
)


class TraceEvent(BaseModel):
    """One JSONL-safe structured trace event."""

    event: str
    payload: dict[str, Any] = Field(default_factory=dict)
    ts: str

    @classmethod
    def create(
        cls,
        *,
        event: str,
        payload: dict[str, Any] | None = None,
        ts: str | None = None,
    ) -> "TraceEvent":
        """Create a sanitized trace event."""

        return cls(
            event=event,
            payload=sanitize_trace_payload(payload or {}),
            ts=ts or datetime.now(UTC).replace(microsecond=0).isoformat(),
        )


def sanitize_trace_payload(payload: dict[str, Any]) -> dict[str, Any]:
    """Drop sensitive or trading-output keys from a trace payload."""

    sanitized: dict[str, Any] = {}
    for key, value in payload.items():
        if _is_forbidden_key(key):
            continue
        cleaned = _sanitize_value(value)
        if cleaned is not None:
            sanitized[key] = cleaned
    return sanitized


def _sanitize_value(value: Any) -> Any:
    if isinstance(value, dict):
        cleaned = sanitize_trace_payload(value)
        return cleaned if cleaned else None
    if isinstance(value, list):
        cleaned_items = [_sanitize_value(item) for item in value]
        cleaned_items = [item for item in cleaned_items if item is not None]
        return cleaned_items if cleaned_items else None
    return value


def _is_forbidden_key(key: str) -> bool:
    normalized = key.lower().replace("_", "-").replace(" ", "-")
    return any(marker in normalized for marker in FORBIDDEN_TRACE_KEY_MARKERS)
