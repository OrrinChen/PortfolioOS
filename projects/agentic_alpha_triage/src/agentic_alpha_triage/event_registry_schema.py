"""Event registry schema for future Q1 data ingestion."""

from __future__ import annotations

from pydantic import BaseModel, field_validator


class EventRegistryEntry(BaseModel):
    """One timestamped event record eligible for later evaluation."""

    event_id: str
    symbol: str
    event_type: str
    event_available_timestamp: str
    source: str
    source_record_id: str | None = None

    @field_validator("event_id", "symbol", "event_type", "event_available_timestamp", "source")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        """Reject blank event fields."""

        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text
