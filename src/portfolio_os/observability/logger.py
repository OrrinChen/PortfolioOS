"""Optional structured trace logger wrapper."""

from __future__ import annotations

from typing import Any

from portfolio_os.observability.events import TraceEvent
from portfolio_os.observability.trace_writer import TraceWriter


class StructuredTraceLogger:
    """Thin logger that can be disabled by omitting a writer."""

    def __init__(self, writer: TraceWriter | None = None):
        self.writer = writer

    def emit(
        self,
        event: str,
        *,
        payload: dict[str, Any] | None = None,
        ts: str | None = None,
    ) -> TraceEvent | None:
        """Emit one trace event if tracing is enabled."""

        if self.writer is None:
            return None
        return self.writer.write(event, payload=payload, ts=ts)
