"""JSONL trace writing helpers."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any

from portfolio_os.observability.events import TraceEvent


class TraceWriter:
    """Append sanitized structured events to a JSONL file."""

    def __init__(self, path: str | Path):
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def write_event(self, event: TraceEvent) -> TraceEvent:
        """Append an already-built trace event and return it."""

        payload = event.model_dump(mode="json")
        line = json.dumps(payload, sort_keys=True, separators=(",", ":"), ensure_ascii=True)
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")
        return event

    def write(
        self,
        event: str,
        *,
        payload: dict[str, Any] | None = None,
        ts: str | None = None,
    ) -> TraceEvent:
        """Build, sanitize, and append one trace event."""

        return self.write_event(TraceEvent.create(event=event, payload=payload, ts=ts))
