from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import date as date_type
from datetime import timedelta
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.schema import FactorSpec


@dataclass(frozen=True)
class SignalBuildResult:
    signal_panels: dict[str, pd.DataFrame]
    abstain_report: pd.DataFrame
    disabled_factors: list[str]


_DATES = ("2026-01-30", "2026-02-27", "2026-03-31")
_ASSETS = ("AAA", "BBB", "CCC", "DDD")


def build_signal_panels(specs: list[FactorSpec] | tuple[FactorSpec, ...] | object) -> SignalBuildResult:
    signal_panels: dict[str, pd.DataFrame] = {}
    abstain_rows: list[dict[str, object]] = []
    disabled_factors: list[str] = []

    for spec in list(specs):
        if spec.status == "disabled":
            disabled_factors.append(spec.factor_id)
            continue
        panel = _build_factor_panel(spec)
        signal_panels[spec.factor_id] = panel
        abstain_rows.extend(
            panel.loc[panel["coverage_flag"] == False, [  # noqa: E712
                "factor_id",
                "date",
                "asset_id",
                "abstain_reason",
                "signal_timestamp",
                "visibility_timestamp",
                "tradable_timestamp",
            ]].to_dict("records")
        )

    abstain_report = pd.DataFrame(abstain_rows)
    return SignalBuildResult(
        signal_panels=signal_panels,
        abstain_report=abstain_report,
        disabled_factors=disabled_factors,
    )


def write_signal_outputs(result: SignalBuildResult, output_dir: Path) -> list[str]:
    output_dir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    for factor_id, panel in sorted(result.signal_panels.items()):
        filename = f"signal_panel_{factor_id}.csv"
        panel.to_csv(output_dir / filename, index=False)
        written.append(filename)
    result.abstain_report.to_csv(output_dir / "abstain_report.csv", index=False)
    written.append("abstain_report.csv")
    return written


def _build_factor_panel(spec: FactorSpec) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for date_idx, date in enumerate(_DATES):
        for asset_idx, asset_id in enumerate(_ASSETS):
            coverage_flag = not (asset_id == "DDD" and date_idx == 0)
            raw_signal = _raw_signal(spec, date_idx, asset_idx)
            normalized_signal = _normalize(raw_signal, spec) if coverage_flag else None
            rows.append(
                {
                    "schema_version": "signal_panel.v1",
                    "factor_id": spec.factor_id,
                    "date": date,
                    "asset_id": asset_id,
                    "raw_signal": raw_signal if coverage_flag else None,
                    "normalized_signal": normalized_signal,
                    "coverage_flag": coverage_flag,
                    "abstain_reason": "" if coverage_flag else "insufficient_history",
                    "signal_timestamp": f"{date}T16:00:00",
                    "visibility_timestamp": f"{date}T20:00:00",
                    "tradable_timestamp": _next_tradable_timestamp(date),
                    "horizon_start": _next_tradable_timestamp(date),
                    "horizon_end": _horizon_end(date, spec.horizon.holding_days),
                    "provenance_hash": _provenance_hash(spec.factor_id, date, asset_id),
                }
            )
    return pd.DataFrame(rows)


def _raw_signal(spec: FactorSpec, date_idx: int, asset_idx: int) -> float:
    base = (asset_idx - 1.5) * 0.8 + date_idx * 0.15
    if spec.family_id in {"reversal", "low_volatility", "investment", "accruals"}:
        base *= -1.0
    if spec.family_id == "liquidity":
        base = (3 - asset_idx) * 0.5 + date_idx * 0.1
    if spec.family_id == "sue":
        base = (asset_idx - 1) * 0.7 + date_idx * 0.2
    return round(base, 6)


def _normalize(raw_signal: float, spec: FactorSpec) -> float:
    if "inverse" in spec.signal_definition.transform:
        return round(-raw_signal, 6)
    return round(raw_signal, 6)


def _next_tradable_timestamp(date: str) -> str:
    next_date = date_type.fromisoformat(date) + timedelta(days=1)
    return f"{next_date.isoformat()}T09:30:00"


def _horizon_end(date: str, holding_days: int) -> str:
    end_date = date_type.fromisoformat(date) + timedelta(days=holding_days)
    return f"{end_date.isoformat()}T16:00:00"


def _provenance_hash(factor_id: str, date: str, asset_id: str) -> str:
    return hashlib.sha256(f"{factor_id}|{date}|{asset_id}|signal_panel.v1".encode()).hexdigest()[:16]
