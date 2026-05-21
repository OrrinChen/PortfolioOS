"""Structured trace metrics."""

from __future__ import annotations

from collections import Counter

from pydantic import BaseModel, Field

from portfolio_os.observability.events import TraceEvent


class TraceSummary(BaseModel):
    """Small metrics summary for a trace stream."""

    total_events: int
    counts_by_event: dict[str, int] = Field(default_factory=dict)


def summarize_trace_events(events: list[TraceEvent]) -> TraceSummary:
    """Count trace events by event name."""

    counts = Counter(event.event for event in events)
    return TraceSummary(
        total_events=len(events),
        counts_by_event={name: counts[name] for name in sorted(counts)},
    )
