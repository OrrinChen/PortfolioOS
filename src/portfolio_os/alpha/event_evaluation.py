"""Event-aware alpha evaluation contract helpers.

This module builds deterministic local evidence-plan artifacts for typed
event AlphaViews. It does not estimate realized returns, rank IC, orders, or
execution output.
"""

from __future__ import annotations

import csv
from datetime import datetime
import json
from pathlib import Path
from typing import Any, Literal, Sequence, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from portfolio_os.alpha.view_contract import AlphaView


EventLabelType = Literal["event_window", "to_next_announcement"]

EVENT_EVIDENCE_SCHEMA_VERSION = "event_evidence_bundle.v1"
EVENT_EVIDENCE_ARTIFACTS = (
    "event_evidence_bundle.json",
    "event_window_grid.csv",
    "event_half_life_summary.json",
    "event_overlap_diagnostics.json",
    "pit_visibility_report.json",
    "placebo_report.json",
)
EVENT_WINDOW_GRID_COLUMNS = [
    "alpha_view_id",
    "family_id",
    "label_name",
    "label_type",
    "horizon_label",
    "start_offset_days",
    "end_offset_days",
    "signal_date_field",
    "entry_rule",
    "next_event_field",
]


class EventEvidenceValidationError(ValueError):
    """Raised when an event-evidence contract cannot be built safely."""


class EventWindowLabel(BaseModel):
    """Event-time label such as SUE [+2,+22]."""

    model_config = ConfigDict(extra="forbid")

    label_type: Literal["event_window"] = "event_window"
    name: str
    start_offset_days: int
    end_offset_days: int

    @field_validator("name")
    @classmethod
    def require_name(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("event-window label name cannot be blank")
        return text

    @model_validator(mode="after")
    def validate_offsets(self) -> Self:
        if self.end_offset_days < self.start_offset_days:
            raise ValueError("end_offset_days cannot be before start_offset_days")
        return self


class ToNextAnnouncementLabel(BaseModel):
    """Label from a revision signal timestamp to the next announcement."""

    model_config = ConfigDict(extra="forbid")

    label_type: Literal["to_next_announcement"] = "to_next_announcement"
    name: str
    signal_date_field: str
    entry_rule: str
    next_event_field: str

    @field_validator("name", "signal_date_field", "entry_rule", "next_event_field")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("to-next-announcement label fields cannot be blank")
        return text


EventLabel = EventWindowLabel | ToNextAnnouncementLabel


class EventEvidenceBundle(BaseModel):
    """Deterministic event-evidence artifact bundle.

    Fields are artifact plans and contract diagnostics only. They intentionally
    omit realized performance, orders, broker output, and trading instructions.
    """

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["event_evidence_bundle.v1"] = EVENT_EVIDENCE_SCHEMA_VERSION
    alpha_view_id: str
    family_id: str
    mechanism_type: str
    horizon_type: str
    signal_timestamp: str
    visibility_timestamp: str
    tradable_timestamp: str
    anchor_event_timestamp: str
    event_window_grid: list[dict[str, Any]] = Field(default_factory=list)
    event_half_life_summary: dict[str, Any]
    event_overlap_diagnostics: dict[str, Any]
    pit_visibility_report: dict[str, Any]
    placebo_report: dict[str, Any]


def build_event_evidence_bundle(
    *,
    alpha_view: AlphaView,
    labels: Sequence[EventLabel],
    placebo_tests_required: Sequence[str],
    overlap_reference_family_ids: Sequence[str] | None = None,
) -> EventEvidenceBundle:
    """Build a deterministic local event-evidence bundle from one AlphaView."""

    label_list = list(labels)
    if not label_list:
        raise EventEvidenceValidationError("at least one event label is required")
    if alpha_view.mechanism_type != "event":
        raise EventEvidenceValidationError("event evidence requires an event mechanism AlphaView")
    if alpha_view.anchor_event_timestamp is None:
        raise EventEvidenceValidationError("event evidence requires anchor_event_timestamp")

    _validate_revision_source(alpha_view)
    _validate_label_horizon_alignment(alpha_view, label_list)

    event_window_grid = [_event_window_grid_row(alpha_view, label) for label in label_list]
    pit_visibility_report = _build_pit_visibility_report(alpha_view)
    placebo_report = {
        "alpha_view_id": alpha_view.alpha_view_id,
        "planned_tests": [str(test).strip() for test in placebo_tests_required if str(test).strip()],
        "status": "planned_only",
        "note": "This contract records required placebo/null tests; it does not compute realized alpha performance.",
    }
    return EventEvidenceBundle(
        alpha_view_id=alpha_view.alpha_view_id,
        family_id=alpha_view.family_id,
        mechanism_type=alpha_view.mechanism_type,
        horizon_type=alpha_view.horizon_type,
        signal_timestamp=_isoformat_z(alpha_view.signal_timestamp),
        visibility_timestamp=_isoformat_z(alpha_view.visibility_timestamp),
        tradable_timestamp=_isoformat_z(alpha_view.tradable_timestamp),
        anchor_event_timestamp=_isoformat_z(alpha_view.anchor_event_timestamp),
        event_window_grid=event_window_grid,
        event_half_life_summary=_build_half_life_summary(alpha_view),
        event_overlap_diagnostics=_build_overlap_diagnostics(alpha_view, overlap_reference_family_ids),
        pit_visibility_report=pit_visibility_report,
        placebo_report=placebo_report,
    )


def dump_event_evidence_bundle_json(bundle: EventEvidenceBundle) -> str:
    """Dump an event-evidence bundle as deterministic sorted JSON."""

    return _dump_json(bundle.model_dump(mode="json"))


def write_event_evidence_artifacts(bundle: EventEvidenceBundle, output_dir: str | Path) -> dict[str, Path]:
    """Write the standard Phase 36 local artifact set."""

    resolved_output_dir = Path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    artifacts = {
        "event_evidence_bundle.json": resolved_output_dir / "event_evidence_bundle.json",
        "event_window_grid.csv": resolved_output_dir / "event_window_grid.csv",
        "event_half_life_summary.json": resolved_output_dir / "event_half_life_summary.json",
        "event_overlap_diagnostics.json": resolved_output_dir / "event_overlap_diagnostics.json",
        "pit_visibility_report.json": resolved_output_dir / "pit_visibility_report.json",
        "placebo_report.json": resolved_output_dir / "placebo_report.json",
    }
    artifacts["event_evidence_bundle.json"].write_text(dump_event_evidence_bundle_json(bundle), encoding="utf-8")
    _write_event_window_grid_csv(artifacts["event_window_grid.csv"], bundle.event_window_grid)
    artifacts["event_half_life_summary.json"].write_text(
        _dump_json(bundle.event_half_life_summary),
        encoding="utf-8",
    )
    artifacts["event_overlap_diagnostics.json"].write_text(
        _dump_json(bundle.event_overlap_diagnostics),
        encoding="utf-8",
    )
    artifacts["pit_visibility_report.json"].write_text(
        _dump_json(bundle.pit_visibility_report),
        encoding="utf-8",
    )
    artifacts["placebo_report.json"].write_text(_dump_json(bundle.placebo_report), encoding="utf-8")
    return artifacts


def _validate_label_horizon_alignment(alpha_view: AlphaView, labels: Sequence[EventLabel]) -> None:
    label_types = {label.label_type for label in labels}
    if len(label_types) > 1:
        raise EventEvidenceValidationError("event evidence labels cannot mix horizon types")
    if "event_window" in label_types and alpha_view.horizon_type != "event_window":
        raise EventEvidenceValidationError("event_window labels require an event_window AlphaView horizon")
    if "to_next_announcement" in label_types and alpha_view.horizon_type != "to_next_event":
        raise EventEvidenceValidationError(
            "to_next_announcement labels require a to_next_event AlphaView horizon",
        )


def _validate_revision_source(alpha_view: AlphaView) -> None:
    family = alpha_view.family_id.upper()
    source = alpha_view.pit_safety_report.pit_source.strip()
    if ("REVISION" in family or "ANALYST" in family) and "fmp" in source.lower():
        raise EventEvidenceValidationError(
            "FMP analyst-estimate history is not a PIT-safe analyst revision source; use WRDS or another "
            "timestamped point-in-time source.",
        )


def _event_window_grid_row(alpha_view: AlphaView, label: EventLabel) -> dict[str, Any]:
    if isinstance(label, EventWindowLabel):
        return {
            "alpha_view_id": alpha_view.alpha_view_id,
            "family_id": alpha_view.family_id,
            "label_name": label.name,
            "label_type": label.label_type,
            "horizon_label": f"[+{label.start_offset_days},+{label.end_offset_days}]",
            "start_offset_days": label.start_offset_days,
            "end_offset_days": label.end_offset_days,
            "signal_date_field": "",
            "entry_rule": "",
            "next_event_field": "",
        }
    start_offset_days = alpha_view.holding_window.get("start_offset_days", "")
    return {
        "alpha_view_id": alpha_view.alpha_view_id,
        "family_id": alpha_view.family_id,
        "label_name": label.name,
        "label_type": label.label_type,
        "horizon_label": f"{label.signal_date_field} -> {label.entry_rule} -> {label.next_event_field}",
        "start_offset_days": start_offset_days,
        "end_offset_days": "",
        "signal_date_field": label.signal_date_field,
        "entry_rule": label.entry_rule,
        "next_event_field": label.next_event_field,
    }


def _build_half_life_summary(alpha_view: AlphaView) -> dict[str, Any]:
    return {
        "alpha_view_id": alpha_view.alpha_view_id,
        "decay_mode": alpha_view.decay_policy.get("mode", "unspecified"),
        "half_life_days": alpha_view.decay_policy.get("half_life_days", ""),
        "status": "planned_only",
        "todo": "Estimate event half-life only after leakage-safe realized event labels are supplied.",
    }


def _build_overlap_diagnostics(
    alpha_view: AlphaView,
    overlap_reference_family_ids: Sequence[str] | None,
) -> dict[str, Any]:
    references = [str(reference).strip() for reference in overlap_reference_family_ids or [] if str(reference).strip()]
    return {
        "alpha_view_id": alpha_view.alpha_view_id,
        "family_id": alpha_view.family_id,
        "reference_family_ids": references,
        "status": "planned_if_reference_available" if references else "not_requested",
        "todo": "Compute overlap and marginal-value diagnostics only from PIT-safe evidence bundles.",
    }


def _build_pit_visibility_report(alpha_view: AlphaView) -> dict[str, Any]:
    return {
        "alpha_view_id": alpha_view.alpha_view_id,
        "family_id": alpha_view.family_id,
        "signal_timestamp": _isoformat_z(alpha_view.signal_timestamp),
        "visibility_timestamp": _isoformat_z(alpha_view.visibility_timestamp),
        "tradable_timestamp": _isoformat_z(alpha_view.tradable_timestamp),
        "anchor_event_timestamp": _isoformat_z(alpha_view.anchor_event_timestamp),
        "pit_source": alpha_view.pit_safety_report.pit_source,
        "no_future_data": alpha_view.pit_safety_report.no_future_data,
        "visibility_not_after_tradable": alpha_view.visibility_timestamp <= alpha_view.tradable_timestamp,
        "anchor_not_after_tradable": alpha_view.anchor_event_timestamp <= alpha_view.tradable_timestamp
        if alpha_view.anchor_event_timestamp is not None
        else False,
    }


def _write_event_window_grid_csv(path: Path, rows: Sequence[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=EVENT_WINDOW_GRID_COLUMNS)
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in EVENT_WINDOW_GRID_COLUMNS})


def _dump_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _isoformat_z(value: datetime | None) -> str:
    if value is None:
        return ""
    text = value.isoformat()
    if text.endswith("+00:00"):
        return text[:-6] + "Z"
    return text
