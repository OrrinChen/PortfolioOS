"""Typed AlphaView contract.

AlphaView is a typed predictive claim. It is not an order, a trading
recommendation, or an opaque monthly alpha score.
"""

from __future__ import annotations

from datetime import datetime
import json
from pathlib import Path
from typing import Any, Literal, Self

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator

from portfolio_os.alpha.schema_versions import ALPHA_VIEW_SCHEMA_VERSION


MechanismType = Literal["event", "state_transition", "fixed_horizon", "residual_factor"]
HorizonType = Literal["event_window", "to_next_event", "rebalance_period", "state_exit"]
ExpectedReturnState = Literal["active_view", "no_view"]
AbstainMode = Literal["explicit_abstain"]

FORBIDDEN_ALPHA_VIEW_FIELDS = {
    "orders",
    "broker_output",
    "live_performance",
    "trading_recommendation",
    "trading_instruction",
    "hidden_q2_results",
}
FORWARD_RETURN_MARKERS = (
    "forward_return",
    "fwd_ret",
    "future_return",
    "realized_forward_return",
)


class AlphaViewValidationError(ValueError):
    """Raised when an AlphaView violates the contract boundary."""


class AbstainPolicy(BaseModel):
    """Policy for representing missing or stale alpha views."""

    model_config = ConfigDict(extra="forbid")

    mode: AbstainMode
    coverage_threshold: float | None = Field(default=None, ge=0.0, le=1.0)
    stale_after_days: int | None = Field(default=None, ge=0)


class CoverageMask(BaseModel):
    """Coverage metadata with explicit abstain semantics."""

    model_config = ConfigDict(extra="forbid")

    mode: AbstainMode
    covered_symbols: list[str] = Field(default_factory=list)
    uncovered_symbols: list[str] = Field(default_factory=list)

    @field_validator("covered_symbols", "uncovered_symbols")
    @classmethod
    def clean_symbols(cls, values: list[str]) -> list[str]:
        cleaned = [str(value).strip().upper() for value in values]
        if any(not value for value in cleaned):
            raise ValueError("symbol lists cannot contain blank entries")
        return cleaned


class ExpectedReturnEntry(BaseModel):
    """One symbol-level expected-return view."""

    model_config = ConfigDict(extra="forbid")

    state: ExpectedReturnState
    value: float | None = None
    reason: str | None = None
    source_column: str | None = None

    @model_validator(mode="after")
    def validate_state_semantics(self) -> Self:
        if self.state == "active_view" and self.value is None:
            raise ValueError("active_view entries require value")
        if self.state == "no_view":
            if self.value is not None:
                raise ValueError("no_view entries cannot carry value")
            if not self.reason:
                raise ValueError("no_view entries require reason")
        if self.source_column and _looks_like_forward_return(self.source_column):
            raise ValueError("forward-return leakage in expected_return_view source_column")
        return self


class PitSafetyReport(BaseModel):
    """Point-in-time safety metadata for one AlphaView."""

    model_config = ConfigDict(extra="forbid")

    no_future_data: bool
    visibility_not_after_tradable: bool
    anchor_not_before_visibility: bool
    pit_source: str

    @model_validator(mode="after")
    def require_safe_report(self) -> Self:
        failed = [
            name
            for name in (
                "no_future_data",
                "visibility_not_after_tradable",
                "anchor_not_before_visibility",
            )
            if getattr(self, name) is not True
        ]
        if failed:
            raise ValueError("PIT safety checks failed: " + ", ".join(failed))
        return self


class AlphaView(BaseModel):
    """Typed predictive alpha view contract."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["alpha_view.v1"] = ALPHA_VIEW_SCHEMA_VERSION
    alpha_view_id: str
    family_id: str
    mechanism_type: MechanismType
    universe_id: str
    signal_timestamp: datetime
    visibility_timestamp: datetime
    tradable_timestamp: datetime
    anchor_event_timestamp: datetime | None = None
    horizon_type: HorizonType
    holding_window: dict[str, Any]
    decay_policy: dict[str, Any]
    coverage_mask: CoverageMask
    abstain_policy: AbstainPolicy
    expected_return_view: dict[str, ExpectedReturnEntry] = Field(min_length=1)
    confidence_view: dict[str, Any]
    capacity_view: dict[str, Any]
    cost_sensitivity_view: dict[str, Any]
    pit_safety_report: PitSafetyReport
    provenance: dict[str, Any]

    @classmethod
    def validate_payload(cls, payload: dict[str, Any]) -> "AlphaView":
        """Validate a raw JSON-compatible AlphaView payload."""

        forbidden = sorted(_collect_keys(payload).intersection(FORBIDDEN_ALPHA_VIEW_FIELDS))
        if forbidden:
            raise AlphaViewValidationError("forbidden alpha view field: " + ", ".join(forbidden))
        if _contains_forward_return_marker(payload):
            raise AlphaViewValidationError("forward-return leakage in alpha view payload")
        try:
            return cls.model_validate(payload)
        except ValueError as exc:
            raise AlphaViewValidationError(str(exc)) from exc

    @field_validator("alpha_view_id", "family_id", "universe_id")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text

    @field_validator("expected_return_view")
    @classmethod
    def clean_expected_return_symbols(
        cls,
        value: dict[str, ExpectedReturnEntry],
    ) -> dict[str, ExpectedReturnEntry]:
        cleaned: dict[str, ExpectedReturnEntry] = {}
        for symbol, entry in value.items():
            normalized = str(symbol).strip().upper()
            if not normalized:
                raise ValueError("expected_return_view symbols cannot be blank")
            cleaned[normalized] = entry
        return cleaned

    @model_validator(mode="after")
    def validate_timestamps(self) -> Self:
        if self.visibility_timestamp > self.tradable_timestamp:
            raise ValueError("visibility_timestamp cannot be after tradable_timestamp")
        if self.anchor_event_timestamp is not None and self.anchor_event_timestamp > self.tradable_timestamp:
            raise ValueError("anchor_event_timestamp cannot be after tradable_timestamp")
        if self.mechanism_type == "event" and self.anchor_event_timestamp is None:
            raise ValueError("event alpha views require anchor_event_timestamp")
        return self


def load_alpha_view(path: str | Path) -> AlphaView:
    """Load and validate one AlphaView JSON file."""

    resolved_path = Path(path)
    payload = json.loads(resolved_path.read_text(encoding="utf-8"))
    if not isinstance(payload, dict):
        raise AlphaViewValidationError(f"{resolved_path} must contain a JSON object")
    return AlphaView.validate_payload(payload)


def dump_alpha_view_json(view: AlphaView) -> str:
    """Dump an AlphaView as deterministic sorted JSON."""

    payload = view.model_dump(mode="json")
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _collect_keys(payload: Any) -> set[str]:
    if isinstance(payload, dict):
        keys = set(payload)
        for value in payload.values():
            keys.update(_collect_keys(value))
        return keys
    if isinstance(payload, list):
        keys: set[str] = set()
        for value in payload:
            keys.update(_collect_keys(value))
        return keys
    return set()


def _contains_forward_return_marker(payload: Any) -> bool:
    if isinstance(payload, dict):
        return any(
            _looks_like_forward_return(str(key)) or _contains_forward_return_marker(value)
            for key, value in payload.items()
        )
    if isinstance(payload, list):
        return any(_contains_forward_return_marker(value) for value in payload)
    if isinstance(payload, str):
        return _looks_like_forward_return(payload)
    return False


def _looks_like_forward_return(value: str) -> bool:
    lowered = value.lower()
    return any(marker in lowered for marker in FORWARD_RETURN_MARKERS)
