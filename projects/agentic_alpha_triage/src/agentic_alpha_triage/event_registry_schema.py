"""Event registry schema for future Q1 data ingestion."""

from __future__ import annotations

from datetime import date, datetime
from pathlib import Path
from typing import Self

import yaml
from pydantic import BaseModel, Field, field_validator, model_validator


class EventRegistryEntry(BaseModel):
    """One timestamped event record eligible for later evaluation."""

    event_id: str
    symbol: str
    event_type: str
    event_available_timestamp: str
    anchor_trade_date: str | None = None
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

    @field_validator("anchor_trade_date")
    @classmethod
    def strip_optional_anchor_trade_date(cls, value: str | None) -> str | None:
        """Normalize optional anchor trade dates."""

        if value is None:
            return None
        text = str(value).strip()
        if not text:
            raise ValueError("anchor_trade_date cannot be blank")
        return text

    @model_validator(mode="after")
    def require_anchor_after_event_visibility(self) -> Self:
        """Reject examples whose trade anchor predates event visibility."""

        if self.anchor_trade_date is None:
            return self

        event_timestamp = _parse_datetime(self.event_available_timestamp, "event_available_timestamp")
        anchor_date = _parse_date(self.anchor_trade_date, "anchor_trade_date")
        if anchor_date < event_timestamp.date():
            raise ValueError("anchor_trade_date cannot be before event_available_timestamp date")
        return self


class EventRegistryExample(BaseModel):
    """Static event-registry example artifact for Q1 documentation."""

    registry_id: str
    description: str
    hypothesis_id: str
    hypothesis_path: str
    event_source: str
    events: list[EventRegistryEntry] = Field(min_length=1)

    @field_validator("registry_id", "description", "hypothesis_id", "hypothesis_path", "event_source")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        """Reject blank registry fields."""

        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text


def _parse_datetime(value: str, field_name: str) -> datetime:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field_name} cannot be blank")
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO timestamp") from exc


def _parse_date(value: str, field_name: str) -> date:
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field_name} cannot be blank")
    try:
        return date.fromisoformat(text)
    except ValueError as exc:
        raise ValueError(f"{field_name} must be an ISO date") from exc


def _load_yaml_mapping(path: Path) -> dict[str, object]:
    with path.open("r", encoding="utf-8") as handle:
        payload = yaml.safe_load(handle) or {}

    if not isinstance(payload, dict):
        raise ValueError(f"{path} must contain a YAML mapping")
    return payload


def _require_referenced_paths(path: Path, example: EventRegistryExample) -> None:
    resolved = (path.parent / example.hypothesis_path).resolve()
    if not resolved.exists():
        raise ValueError(f"Referenced hypothesis path does not exist: {example.hypothesis_path}")


def load_event_registry_example(path: str | Path) -> EventRegistryExample:
    """Load and validate one event-registry example YAML file."""

    resolved_path = Path(path)
    example = EventRegistryExample.model_validate(_load_yaml_mapping(resolved_path))
    _require_referenced_paths(resolved_path, example)
    return example


def load_event_registry_examples(examples_dir: str | Path) -> list[EventRegistryExample]:
    """Load all event-registry examples in a directory."""

    resolved_dir = Path(examples_dir)
    if not resolved_dir.exists():
        raise ValueError(f"Event registry example directory does not exist: {resolved_dir}")
    if not resolved_dir.is_dir():
        raise ValueError(f"Event registry example path is not a directory: {resolved_dir}")

    paths = sorted(resolved_dir.glob("*.yaml"))
    if not paths:
        raise ValueError(f"No event registry example YAML files found in {resolved_dir}")

    return [load_event_registry_example(path) for path in paths]
