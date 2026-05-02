"""Evidence bundle schemas for audit-ready decision evaluation."""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, Field, field_validator, model_validator


BundleStatus = Literal["ready", "rejected"]
CheckSeverity = Literal["info", "warning", "critical"]
PromotionEligibility = Literal[
    "eligible_for_promotion_review",
    "not_eligible",
    "needs_more_evidence",
]


class PitSafetyReport(BaseModel):
    """Point-in-time safety checks required before promotion review."""

    event_available_timestamp_present: bool
    signal_timestamp_present: bool
    anchor_trade_date_present: bool
    anchor_not_before_event_available: bool
    anchor_not_before_signal_timestamp: bool
    point_in_time_inputs_only: bool

    @model_validator(mode="after")
    def require_safe_timestamps(self) -> Self:
        """Reject bundles that fail timestamp or PIT-safety checks."""

        failed_checks = [
            _pit_failure_label(field_name)
            for field_name in (
                "event_available_timestamp_present",
                "signal_timestamp_present",
                "anchor_trade_date_present",
                "anchor_not_before_event_available",
                "anchor_not_before_signal_timestamp",
                "point_in_time_inputs_only",
            )
            if getattr(self, field_name) is not True
        ]
        if failed_checks:
            raise ValueError("timestamp/PIT safety checks failed: " + ", ".join(failed_checks))
        return self


class LeakageCheck(BaseModel):
    """One leakage check result recorded in an evidence bundle."""

    check_name: str
    passed: bool
    severity: CheckSeverity
    details: str

    @field_validator("check_name", "details")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text


class PlannedTest(BaseModel):
    """One planned evaluator test before any alpha promotion."""

    test_name: str
    description: str
    required: bool = True

    @field_validator("test_name", "description")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text


class EvidenceBundle(BaseModel):
    """Typed Q1 evidence package that remains separate from Q2 execution."""

    bundle_id: str
    hypothesis_id: str
    signal_id: str
    evaluation_id: str
    status: BundleStatus
    pit_safety: PitSafetyReport
    leakage_checks: list[LeakageCheck] = Field(min_length=1)
    required_columns: list[str] = Field(min_length=1)
    planned_tests: list[PlannedTest] = Field(min_length=1)
    coverage_requirements: list[str] = Field(default_factory=list)
    rejection_reasons: list[str] = Field(default_factory=list)
    promotion_eligibility: PromotionEligibility

    @field_validator("bundle_id", "hypothesis_id", "signal_id", "evaluation_id")
    @classmethod
    def require_non_empty_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("field cannot be blank")
        return text

    @field_validator("required_columns", "coverage_requirements", "rejection_reasons")
    @classmethod
    def clean_text_list(cls, values: list[str]) -> list[str]:
        cleaned = [str(value).strip() for value in values]
        if any(not value for value in cleaned):
            raise ValueError("list entries cannot be blank")
        return cleaned

    @model_validator(mode="after")
    def require_safe_bundle(self) -> Self:
        """Reject unsafe evidence bundles before promotion review."""

        forbidden_columns = [
            column
            for column in self.required_columns
            if _looks_like_forward_return_column(column)
        ]
        if forbidden_columns:
            raise ValueError("forward-return leakage in required_columns: " + ", ".join(forbidden_columns))

        failed_critical_checks = [
            check.check_name
            for check in self.leakage_checks
            if not check.passed and check.severity == "critical"
        ]
        if failed_critical_checks:
            raise ValueError("critical leakage checks failed: " + ", ".join(failed_critical_checks))

        if self.status == "ready" and self.rejection_reasons:
            raise ValueError("ready evidence bundle cannot include rejection_reasons")
        if self.status == "ready" and self.promotion_eligibility != "eligible_for_promotion_review":
            raise ValueError("ready evidence bundle must be eligible_for_promotion_review")
        return self


def _looks_like_forward_return_column(column: str) -> bool:
    lowered = column.lower()
    return (
        "forward_return" in lowered
        or lowered.startswith("fwd_ret")
        or lowered.startswith("future_return")
        or lowered.startswith("realized_forward_return")
    )


def _pit_failure_label(field_name: str) -> str:
    labels = {
        "anchor_not_before_event_available": "anchor_trade_date before event_available_timestamp",
        "anchor_not_before_signal_timestamp": "anchor_trade_date before signal timestamp",
    }
    return labels.get(field_name, field_name)
