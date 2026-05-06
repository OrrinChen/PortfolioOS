"""Schemas for WRDS PIT-labeled historical SUE event panels."""

from __future__ import annotations

from datetime import date, datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


SUE_HISTORICAL_EVENT_SCHEMA_VERSION = "sue_historical_event_row.v1"
SUE_HISTORICAL_COVERAGE_SCHEMA_VERSION = "sue_historical_coverage_report.v1"
SUE_HISTORICAL_LINEAGE_SCHEMA_VERSION = "sue_historical_data_lineage_manifest.v1"

SUE_HISTORICAL_EVENT_COLUMNS = [
    "event_id",
    "symbol",
    "permno",
    "ibes_ticker",
    "cusip",
    "fiscal_period",
    "announcement_date",
    "event_available_timestamp",
    "tradable_timestamp",
    "rebalance_date",
    "actual_eps",
    "expected_eps",
    "sue_value",
    "sue_definition",
    "estimate_snapshot_date",
    "price_anchor_date",
    "return_window_start",
    "return_window_end",
    "data_source",
    "link_method",
    "pit_safety_status",
    "diagnostic_only",
    "fetched_at",
]

FORWARD_RETURN_FEATURE_TOKENS = (
    "forward_return",
    "fwd_ret",
    "future_return",
    "realized_forward_return",
    "label_return",
)

MISLEADING_REPORT_CLAIMS = (
    "production approved",
    "paper ready",
    "paper-ready",
    "live-ready",
    "live ready",
    "live alpha orders",
    "broker execution",
    "order generation",
    "real historical sue alpha proven",
    "historical sue alpha proven",
    "guaranteed tradable alpha",
    "auto trading",
    "investment recommendation",
)


class SueHistoricalEventRow(BaseModel):
    """One PIT-labeled SUE event row."""

    model_config = ConfigDict(extra="forbid")

    schema_version: Literal["sue_historical_event_row.v1"] = SUE_HISTORICAL_EVENT_SCHEMA_VERSION
    event_id: str
    symbol: str
    permno: int | None = None
    ibes_ticker: str | None = None
    cusip: str | None = None
    fiscal_period: str
    announcement_date: date
    event_available_timestamp: datetime
    tradable_timestamp: datetime
    rebalance_date: date
    actual_eps: float | None = None
    expected_eps: float | None = None
    sue_value: float | None = None
    sue_definition: str
    estimate_snapshot_date: date | None = None
    price_anchor_date: date | None = None
    return_window_start: date
    return_window_end: date
    data_source: str
    link_method: str
    pit_safety_status: str
    diagnostic_only: bool = False
    fetched_at: datetime

    @field_validator("event_id", "symbol", "fiscal_period", "sue_definition", "data_source", "link_method", "pit_safety_status")
    @classmethod
    def require_text(cls, value: str) -> str:
        text = str(value).strip()
        if not text:
            raise ValueError("text fields cannot be blank")
        return text

    @field_validator("symbol", "ibes_ticker")
    @classmethod
    def normalize_upper(cls, value: str | None) -> str | None:
        if value is None:
            return None
        text = str(value).strip().upper()
        return text or None

    @model_validator(mode="after")
    def validate_pit_safety(self) -> "SueHistoricalEventRow":
        if self.event_available_timestamp > self.tradable_timestamp:
            raise ValueError("event_available_timestamp must be <= tradable_timestamp")
        if self.estimate_snapshot_date is None and not self.diagnostic_only:
            raise ValueError("missing estimate_snapshot_date requires diagnostic_only=true")
        if self.estimate_snapshot_date is not None:
            event_available_date = self.event_available_timestamp.date()
            if self.estimate_snapshot_date > event_available_date:
                raise ValueError("estimate_snapshot_date must be <= event_available_timestamp date")
        if self.return_window_start <= self.tradable_timestamp.date():
            raise ValueError("return_window_start must be after tradable_timestamp")
        if self.return_window_end < self.return_window_start:
            raise ValueError("return_window_end must be >= return_window_start")
        if self.expected_eps is None and not self.diagnostic_only:
            raise ValueError("missing expected_eps requires diagnostic_only=true")
        if self.actual_eps is None and not self.diagnostic_only:
            raise ValueError("missing actual_eps requires diagnostic_only=true")
        if self.sue_value is None and not self.diagnostic_only:
            raise ValueError("missing sue_value requires diagnostic_only=true")
        if (
            self.expected_eps == 0.0
            and self.diagnostic_only
            and "missing_estimate" in self.pit_safety_status
        ):
            raise ValueError("missing expected_eps cannot be encoded as zero SUE")
        if "FMP" in self.data_source.upper() and "SNAPSHOT" not in self.data_source.upper():
            raise ValueError("FMP frozen estimate history is not PIT-safe without snapshot history")
        return self


class SueHistoricalPanelConfig(BaseModel):
    """Builder configuration for historical SUE event panels."""

    model_config = ConfigDict(extra="forbid")

    mode: Literal["smoke", "full"] = "smoke"
    sample_event_count: int = Field(default=60, gt=0)
    fetched_at: str | None = None
    earnings_events_path: str | None = None
    estimate_snapshots_path: str | None = None
    security_links_path: str | None = None
    crsp_daily_path: str | None = None

    @model_validator(mode="after")
    def validate_full_mode_inputs(self) -> "SueHistoricalPanelConfig":
        if self.mode == "full":
            missing = [
                name
                for name, value in {
                    "earnings_events_path": self.earnings_events_path,
                    "estimate_snapshots_path": self.estimate_snapshots_path,
                    "security_links_path": self.security_links_path,
                    "crsp_daily_path": self.crsp_daily_path,
                }.items()
                if not value
            ]
            if missing:
                raise ValueError("full mode requires local WRDS extract paths: " + ", ".join(missing))
        return self


def validate_no_forward_return_feature_columns(columns: list[str] | tuple[str, ...]) -> None:
    """Reject forward-return labels in feature/input columns."""

    for column in columns:
        lowered = str(column).strip().lower()
        if lowered in {"return_window_start", "return_window_end"}:
            continue
        if any(token in lowered for token in FORWARD_RETURN_FEATURE_TOKENS):
            raise ValueError(f"forward-return feature column is forbidden: {column}")


def validate_sue_historical_report_language(text: str) -> None:
    """Reject misleading SUE panel report claims while allowing explicit non-claims."""

    lowered = str(text).lower()
    scrubbed = lowered
    allowed_phrases = [
        "it does not approve paper trading, live trading, broker workflows, orders, or production deployment.",
        "no broker workflow was added.",
        "no order workflow was added.",
        "no live trading workflow was added.",
        "no production approval is claimed.",
        "it does not prove paper readiness or production approval.",
    ]
    for phrase in allowed_phrases:
        scrubbed = scrubbed.replace(phrase, "")
    for claim in MISLEADING_REPORT_CLAIMS:
        if claim in scrubbed:
            raise ValueError(f"misleading SUE historical panel claim detected: {claim}")
