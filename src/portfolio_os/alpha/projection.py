"""Projection bridge from typed AlphaViews to rebalance expected returns.

The bridge prepares optimizer input panels from typed predictive claims. It
does not run PortfolioOS workflows, optimizers, brokers, or live data paths.
"""

from __future__ import annotations

import csv
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
from typing import Any, Sequence

from pydantic import BaseModel, ConfigDict, Field, field_validator

from portfolio_os.alpha.projection_diagnostics import build_projection_diagnostic_row
from portfolio_os.alpha.view_contract import AlphaView, ExpectedReturnEntry


PROJECTION_SCHEMA_VERSION = "alpha_projection.v2"
PROJECTION_ARTIFACTS = (
    "expected_return_panel.csv",
    "alpha_projection_manifest.json",
    "alpha_projection_diagnostics.json",
    "alpha_abstain_report.json",
)
EXPECTED_RETURN_PANEL_COLUMNS = [
    "date",
    "symbol",
    "expected_return",
    "active_alpha_views",
    "horizon_conversion",
    "decay_applied",
    "confidence_weight",
]
ABSTAIN_REPORT_COLUMNS = [
    "date",
    "symbol",
    "alpha_view_id",
    "family_id",
    "reason",
]


class AlphaProjectionValidationError(ValueError):
    """Raised when AlphaView projection cannot be built safely."""


class AlphaProjectionConfig(BaseModel):
    """Inputs for the Alpha Projection Bridge v2."""

    model_config = ConfigDict(extra="forbid")

    rebalance_dates: list[date] = Field(min_length=1)
    universe_symbols: list[str] = Field(min_length=1)
    risk_horizon_days: int = Field(gt=0)
    cost_assumptions: dict[str, Any] = Field(default_factory=dict)

    @field_validator("rebalance_dates", mode="before")
    @classmethod
    def parse_rebalance_dates(cls, values: Sequence[str | date | datetime]) -> list[date]:
        parsed: list[date] = []
        for value in values:
            if isinstance(value, datetime):
                parsed.append(value.date())
            elif isinstance(value, date):
                parsed.append(value)
            else:
                parsed.append(datetime.fromisoformat(str(value)).date())
        return parsed

    @field_validator("universe_symbols")
    @classmethod
    def normalize_symbols(cls, values: Sequence[str]) -> list[str]:
        cleaned = [str(value).strip().upper() for value in values if str(value).strip()]
        if not cleaned:
            raise ValueError("universe_symbols must contain at least one symbol")
        return sorted(dict.fromkeys(cleaned))


@dataclass(frozen=True)
class AlphaProjectionResult:
    """Projected expected-return panel plus audit artifacts."""

    expected_return_panel: list[dict[str, Any]]
    alpha_projection_manifest: dict[str, Any]
    alpha_projection_diagnostics: list[dict[str, Any]]
    alpha_abstain_report: list[dict[str, Any]]


def project_alpha_views_to_expected_returns(
    *,
    alpha_views: Sequence[AlphaView],
    config: AlphaProjectionConfig,
) -> AlphaProjectionResult:
    """Project typed AlphaViews into a rebalance-period expected-return panel."""

    views = list(alpha_views)
    if not views:
        raise AlphaProjectionValidationError("at least one AlphaView is required")
    _validate_projection_inputs(views)

    panel: list[dict[str, Any]] = []
    abstain_report: list[dict[str, Any]] = []
    diagnostics: list[dict[str, Any]] = []

    for rebalance_date in sorted(config.rebalance_dates):
        date_text = rebalance_date.isoformat()
        symbol_values: dict[str, float] = {}
        symbol_active_views: dict[str, list[str]] = {}
        symbol_horizon_conversions: dict[str, list[str]] = {}
        symbol_decay: dict[str, list[str]] = {}
        symbol_confidence: dict[str, list[str]] = {}
        active_views: list[str] = []
        abstained_views: list[str] = []
        horizon_diagnostics: list[dict[str, Any]] = []
        decay_diagnostics: list[dict[str, Any]] = []

        for view in sorted(views, key=lambda item: item.alpha_view_id):
            window = _active_window_for_view(view)
            if not _is_active_on(rebalance_date, window):
                abstained_views.append(view.alpha_view_id)
                for symbol in config.universe_symbols:
                    abstain_report.append(_abstain_row(date_text, symbol, view, "inactive_outside_view_window"))
                continue

            active_views.append(view.alpha_view_id)
            horizon_scale = _horizon_scale(view, rebalance_date, config.risk_horizon_days)
            decay_multiplier = _decay_multiplier(view, rebalance_date)
            confidence_weight = _confidence_weight(view)
            view_scale = float(horizon_scale * decay_multiplier * confidence_weight)
            horizon_diagnostics.append(
                {
                    "alpha_view_id": view.alpha_view_id,
                    "horizon_type": view.horizon_type,
                    "risk_horizon_days": config.risk_horizon_days,
                    "scale": round(horizon_scale, 12),
                }
            )
            decay_diagnostics.append(
                {
                    "alpha_view_id": view.alpha_view_id,
                    "decay_mode": str(view.decay_policy.get("mode", "unspecified")),
                    "multiplier": round(decay_multiplier, 12),
                }
            )

            for symbol in config.universe_symbols:
                entry = view.expected_return_view.get(symbol)
                if entry is None:
                    abstain_report.append(_abstain_row(date_text, symbol, view, "missing_expected_return_view"))
                    continue
                if entry.state == "no_view":
                    abstain_report.append(_abstain_row(date_text, symbol, view, entry.reason or "explicit_no_view"))
                    continue
                projected_value = _project_entry_value(entry, view_scale)
                symbol_values[symbol] = symbol_values.get(symbol, 0.0) + projected_value
                symbol_active_views.setdefault(symbol, []).append(view.alpha_view_id)
                symbol_horizon_conversions.setdefault(symbol, []).append(f"{view.alpha_view_id}:{horizon_scale:.12g}")
                symbol_decay.setdefault(symbol, []).append(f"{view.alpha_view_id}:{decay_multiplier:.12g}")
                symbol_confidence.setdefault(symbol, []).append(f"{view.alpha_view_id}:{confidence_weight:.12g}")

        for symbol in sorted(symbol_values):
            panel.append(
                {
                    "date": date_text,
                    "symbol": symbol,
                    "expected_return": round(float(symbol_values[symbol]), 12),
                    "active_alpha_views": "|".join(sorted(symbol_active_views.get(symbol, []))),
                    "horizon_conversion": "|".join(symbol_horizon_conversions.get(symbol, [])),
                    "decay_applied": "|".join(symbol_decay.get(symbol, [])),
                    "confidence_weight": "|".join(symbol_confidence.get(symbol, [])),
                }
            )

        diagnostics.append(
            build_projection_diagnostic_row(
                date=date_text,
                active_views=active_views,
                abstained_views=abstained_views,
                coverage_count=len(symbol_values),
                horizon_conversion=horizon_diagnostics,
                decay_applied=decay_diagnostics,
                final_expected_return_scale={symbol: round(float(value), 12) for symbol, value in sorted(symbol_values.items())},
            )
        )

    manifest = _build_projection_manifest(views, config, panel, diagnostics, abstain_report)
    return AlphaProjectionResult(
        expected_return_panel=panel,
        alpha_projection_manifest=manifest,
        alpha_projection_diagnostics=diagnostics,
        alpha_abstain_report=abstain_report,
    )


def write_alpha_projection_artifacts(result: AlphaProjectionResult, output_dir: str | Path) -> dict[str, Path]:
    """Write the standard Phase 37 Alpha Projection Bridge artifact set."""

    resolved_output_dir = Path(output_dir)
    resolved_output_dir.mkdir(parents=True, exist_ok=True)

    artifacts = {
        "expected_return_panel.csv": resolved_output_dir / "expected_return_panel.csv",
        "alpha_projection_manifest.json": resolved_output_dir / "alpha_projection_manifest.json",
        "alpha_projection_diagnostics.json": resolved_output_dir / "alpha_projection_diagnostics.json",
        "alpha_abstain_report.json": resolved_output_dir / "alpha_abstain_report.json",
    }
    _write_csv(artifacts["expected_return_panel.csv"], result.expected_return_panel, EXPECTED_RETURN_PANEL_COLUMNS)
    artifacts["alpha_projection_manifest.json"].write_text(
        _dump_json(result.alpha_projection_manifest),
        encoding="utf-8",
    )
    artifacts["alpha_projection_diagnostics.json"].write_text(
        _dump_json({"diagnostics": result.alpha_projection_diagnostics}),
        encoding="utf-8",
    )
    artifacts["alpha_abstain_report.json"].write_text(
        _dump_json({"abstain_report": result.alpha_abstain_report}),
        encoding="utf-8",
    )
    return artifacts


def _validate_projection_inputs(views: Sequence[AlphaView]) -> None:
    for view in views:
        if view.abstain_policy.mode != "explicit_abstain" or view.coverage_mask.mode != "explicit_abstain":
            raise AlphaProjectionValidationError("projection requires explicit_abstain semantics")


def _active_window_for_view(view: AlphaView) -> tuple[date, date]:
    if view.horizon_type == "event_window":
        if view.anchor_event_timestamp is None:
            raise AlphaProjectionValidationError("event_window views require anchor_event_timestamp")
        start_offset = int(view.holding_window.get("start_offset_days", 0))
        end_offset = int(view.holding_window.get("end_offset_days", start_offset))
        anchor_date = view.anchor_event_timestamp.date()
        start = _date_add_days(anchor_date, start_offset)
        end = _date_add_days(anchor_date, end_offset)
        return max(start, view.tradable_timestamp.date()), end
    if view.horizon_type == "to_next_event":
        next_event_value = view.holding_window.get("next_event_timestamp")
        if not next_event_value:
            raise AlphaProjectionValidationError("to_next_event views require holding_window.next_event_timestamp")
        next_event_ts = _parse_datetime(next_event_value)
        return view.tradable_timestamp.date(), next_event_ts.date()
    if view.horizon_type == "rebalance_period":
        stale_after_days = int(view.abstain_policy.stale_after_days or 0)
        return view.tradable_timestamp.date(), _date_add_days(view.tradable_timestamp.date(), stale_after_days)
    if view.horizon_type == "state_exit":
        stale_after_days = int(view.abstain_policy.stale_after_days or 0)
        return view.tradable_timestamp.date(), _date_add_days(view.tradable_timestamp.date(), stale_after_days)
    raise AlphaProjectionValidationError(f"unsupported horizon_type: {view.horizon_type}")


def _is_active_on(rebalance_date: date, window: tuple[date, date]) -> bool:
    return window[0] <= rebalance_date <= window[1]


def _horizon_scale(view: AlphaView, rebalance_date: date, risk_horizon_days: int) -> float:
    if view.horizon_type == "event_window":
        start_offset = int(view.holding_window.get("start_offset_days", 0))
        end_offset = int(view.holding_window.get("end_offset_days", start_offset))
        view_horizon_days = max(1, end_offset - start_offset + 1)
        return float(risk_horizon_days) / float(view_horizon_days)
    if view.horizon_type == "to_next_event":
        next_event_ts = _parse_datetime(view.holding_window["next_event_timestamp"])
        remaining_days = max(1, (next_event_ts.date() - rebalance_date).days)
        return float(risk_horizon_days) / float(remaining_days)
    return 1.0


def _decay_multiplier(view: AlphaView, rebalance_date: date) -> float:
    mode = str(view.decay_policy.get("mode", "none"))
    if mode == "event_half_life":
        half_life = float(view.decay_policy.get("half_life_days", 0.0))
        if half_life <= 0.0:
            raise AlphaProjectionValidationError("event_half_life decay requires positive half_life_days")
        days_since_tradable = max(0, (rebalance_date - view.tradable_timestamp.date()).days)
        return float(0.5 ** (float(days_since_tradable) / half_life))
    return 1.0


def _confidence_weight(view: AlphaView) -> float:
    raw_value = view.confidence_view.get("confidence_score", 1.0)
    value = float(raw_value)
    return max(0.0, min(1.0, value))


def _project_entry_value(entry: ExpectedReturnEntry, view_scale: float) -> float:
    if entry.value is None:
        raise AlphaProjectionValidationError("active expected-return entries require value")
    return float(entry.value) * float(view_scale)


def _abstain_row(date_text: str, symbol: str, view: AlphaView, reason: str) -> dict[str, str]:
    return {
        "date": date_text,
        "symbol": str(symbol).upper(),
        "alpha_view_id": view.alpha_view_id,
        "family_id": view.family_id,
        "reason": str(reason),
    }


def _build_projection_manifest(
    views: Sequence[AlphaView],
    config: AlphaProjectionConfig,
    panel: Sequence[dict[str, Any]],
    diagnostics: Sequence[dict[str, Any]],
    abstain_report: Sequence[dict[str, Any]],
) -> dict[str, Any]:
    payload = {
        "alpha_view_ids": [view.alpha_view_id for view in sorted(views, key=lambda item: item.alpha_view_id)],
        "cost_assumptions": config.cost_assumptions,
        "panel_row_count": len(panel),
        "diagnostic_row_count": len(diagnostics),
        "abstain_row_count": len(abstain_report),
        "rebalance_dates": [item.isoformat() for item in sorted(config.rebalance_dates)],
        "risk_horizon_days": config.risk_horizon_days,
        "schema_version": PROJECTION_SCHEMA_VERSION,
        "universe_symbols": config.universe_symbols,
    }
    payload["content_hash"] = hashlib.sha256(_dump_json(payload).encode("utf-8")).hexdigest()
    return payload


def _write_csv(path: Path, rows: Sequence[dict[str, Any]], columns: Sequence[str]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns))
        writer.writeheader()
        for row in rows:
            writer.writerow({column: row.get(column, "") for column in columns})


def _dump_json(payload: dict[str, Any]) -> str:
    return json.dumps(payload, indent=2, sort_keys=True) + "\n"


def _date_add_days(value: date, days: int) -> date:
    return value + timedelta(days=int(days))


def _parse_datetime(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        return value
    text = str(value)
    if text.endswith("Z"):
        text = text[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed
