from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd
import yaml


@dataclass(frozen=True)
class FactorAttributionWaterfallResult:
    waterfall_path: str
    waterfall_by_period_path: str
    report_path: str
    diagnostics_path: str
    factor_count: int
    period_row_count: int
    production_approval: bool
    live_trading: bool
    direct_q2_entry: bool
    not_alpha_evidence: bool


FACTOR_IDS = ("momentum_12_1", "reversal_5_1", "low_vol_60d")
MIN_HISTORY_DAYS = 252
COMPONENT_COLUMNS = (
    "intercept_contribution",
    "market_beta_contribution",
    "industry_contribution",
    "style_contribution",
    "residual_return",
)


def run_factor_attribution_waterfall(
    research_manifest_path: Path,
    residual_returns_path: Path,
    output_dir: Path,
) -> FactorAttributionWaterfallResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    waterfall_path = output_dir / "factor_attribution_waterfall.csv"
    by_period_path = output_dir / "factor_attribution_waterfall_by_period.csv"
    report_path = output_dir / "factor_attribution_report.md"
    diagnostics_path = output_dir / "factor_attribution_diagnostics.json"

    manifest = _load_yaml(research_manifest_path)
    prices = _normalize_prices(_load_research_csv(manifest, research_manifest_path, "prices"))
    benchmark = _normalize_benchmark(_load_research_csv(manifest, research_manifest_path, "benchmark"))
    residuals = _normalize_residuals(pd.read_csv(residual_returns_path, dtype={"asset_id": str}))

    by_period = _build_period_waterfall(prices, benchmark, residuals)
    waterfall = _summarize_waterfall(by_period)
    diagnostics = _diagnostics(
        waterfall=waterfall,
        by_period=by_period,
        research_manifest_path=research_manifest_path,
        residual_returns_path=residual_returns_path,
    )

    by_period.to_csv(by_period_path, index=False)
    waterfall.to_csv(waterfall_path, index=False)
    for row in waterfall.itertuples(index=False):
        factor_path = output_dir / f"factor_attribution_waterfall_{row.factor_id}.json"
        factor_path.write_text(json.dumps(_factor_payload(row), indent=2, sort_keys=True) + "\n", encoding="utf-8")
    diagnostics_path.write_text(json.dumps(diagnostics, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    report_path.write_text(_render_report(waterfall, diagnostics), encoding="utf-8")

    return FactorAttributionWaterfallResult(
        waterfall_path=str(waterfall_path),
        waterfall_by_period_path=str(by_period_path),
        report_path=str(report_path),
        diagnostics_path=str(diagnostics_path),
        factor_count=int(len(waterfall)),
        period_row_count=int(len(by_period)),
        production_approval=False,
        live_trading=False,
        direct_q2_entry=False,
        not_alpha_evidence=True,
    )


def _load_yaml(path: Path) -> dict[str, Any]:
    payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(payload, dict):
        raise ValueError(f"manifest must be a mapping: {path}")
    return payload


def _load_research_csv(manifest: Mapping[str, Any], manifest_path: Path, section: str) -> pd.DataFrame:
    payload = manifest.get(section)
    if not isinstance(payload, Mapping):
        raise ValueError(f"{section} section is required")
    raw_path = Path(str(payload.get("path", "")))
    path = _resolve_manifest_path(raw_path, manifest_path)
    return pd.read_csv(path, dtype={"asset_id": str, "permno": str, "gvkey": str})


def _resolve_manifest_path(raw_path: Path, manifest_path: Path) -> Path:
    if raw_path.is_absolute():
        return raw_path
    if raw_path.exists():
        return raw_path
    return manifest_path.parent / raw_path


def _normalize_prices(prices: pd.DataFrame) -> pd.DataFrame:
    normalized = prices.copy()
    if "asset_id" not in normalized.columns:
        normalized["asset_id"] = normalized["permno"].astype(str)
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["adjusted_close"] = pd.to_numeric(normalized["adjusted_close"], errors="coerce")
    normalized = normalized.dropna(subset=["asset_id", "date", "adjusted_close"]).sort_values(["asset_id", "date"])
    normalized["daily_return"] = normalized.groupby("asset_id")["adjusted_close"].pct_change()
    return normalized


def _normalize_benchmark(benchmark: pd.DataFrame) -> pd.DataFrame:
    normalized = benchmark.copy()
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["adjusted_close"] = pd.to_numeric(normalized["adjusted_close"], errors="coerce")
    return normalized.dropna(subset=["date", "adjusted_close"]).sort_values("date")


def _normalize_residuals(residuals: pd.DataFrame) -> pd.DataFrame:
    normalized = residuals.copy()
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    for column in ("date", "period_start", "period_end"):
        normalized[column] = pd.to_datetime(normalized[column], errors="coerce")
    for column in ("realized_return", *COMPONENT_COLUMNS):
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")
    normalized["coverage_flag"] = normalized["coverage_flag"].map(_as_bool)
    return normalized.dropna(subset=["date", "period_start", "period_end", "asset_id"])


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def _build_period_waterfall(prices: pd.DataFrame, benchmark: pd.DataFrame, residuals: pd.DataFrame) -> pd.DataFrame:
    benchmark_lookup = benchmark.set_index("date")["adjusted_close"].to_dict()
    rows: list[dict[str, object]] = []
    for signal_date, period_residuals in residuals.groupby("date", sort=True):
        signal_date = pd.Timestamp(signal_date)
        covered = period_residuals[period_residuals["coverage_flag"]].dropna(subset=["realized_return", *COMPONENT_COLUMNS])
        if covered.empty:
            continue
        period_start = pd.Timestamp(covered["period_start"].iloc[0])
        period_end = pd.Timestamp(covered["period_end"].iloc[0])
        signals = _signals_for_date(prices, signal_date)
        if signals.empty:
            continue
        frame = covered.merge(signals, on="asset_id", how="inner")
        if len(frame) < 3:
            continue
        qqq_return = _benchmark_return(benchmark_lookup, period_start, period_end)
        for factor_id in FACTOR_IDS:
            signal_column = f"{factor_id}_signal"
            if signal_column not in frame.columns:
                continue
            factor_frame = frame.dropna(subset=[signal_column, "realized_return", *COMPONENT_COLUMNS]).copy()
            if len(factor_frame) < 3:
                continue
            rows.append(_period_factor_row(factor_id, signal_date, period_start, period_end, factor_frame, signal_column, qqq_return))
    return pd.DataFrame(rows, columns=_period_columns())


def _signals_for_date(prices: pd.DataFrame, signal_date: pd.Timestamp) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for asset_id, history in prices[prices["date"] <= signal_date].groupby("asset_id", sort=False):
        history = history.sort_values("date")
        if len(history) <= MIN_HISTORY_DAYS:
            continue
        close = history["adjusted_close"].reset_index(drop=True)
        returns = history["daily_return"].reset_index(drop=True)
        rows.append(
            {
                "asset_id": str(asset_id),
                "momentum_12_1_signal": close.iloc[-22] / close.iloc[-253] - 1.0,
                "reversal_5_1_signal": -(close.iloc[-1] / close.iloc[-6] - 1.0),
                "low_vol_60d_signal": -float(returns.iloc[-60:].std()),
            }
        )
    return pd.DataFrame(rows)


def _period_factor_row(
    factor_id: str,
    signal_date: pd.Timestamp,
    period_start: pd.Timestamp,
    period_end: pd.Timestamp,
    factor_frame: pd.DataFrame,
    signal_column: str,
    qqq_return: float,
) -> dict[str, object]:
    gross = _spread(factor_frame, signal_column, "realized_return")
    intercept = _spread(factor_frame, signal_column, "intercept_contribution")
    market_beta = _spread(factor_frame, signal_column, "market_beta_contribution")
    industry = _spread(factor_frame, signal_column, "industry_contribution")
    style = _spread(factor_frame, signal_column, "style_contribution")
    residual = _spread(factor_frame, signal_column, "residual_return")
    beta_adjusted = gross - market_beta
    industry_adjusted = beta_adjusted - industry
    style_adjusted = industry_adjusted - style
    reconstruction_error = gross - (intercept + market_beta + industry + style + residual)
    status = _period_status(
        gross_spread=gross,
        qqq_relative_spread=gross - qqq_return,
        beta_adjusted_spread=beta_adjusted,
        full_residual_spread=residual,
    )
    return {
        "schema_version": "factor_attribution_waterfall_by_period.v1",
        "factor_id": factor_id,
        "date": _date_str(signal_date),
        "period_start": _date_str(period_start),
        "period_end": _date_str(period_end),
        "asset_count": int(len(factor_frame)),
        "tail_count": int(max(1, len(factor_frame) // 5)),
        "gross_spread": _round(gross),
        "qqq_return": _round(qqq_return),
        "qqq_relative_spread": _round(gross - qqq_return),
        "intercept_contribution_spread": _round(intercept),
        "market_beta_contribution_spread": _round(market_beta),
        "beta_adjusted_spread": _round(beta_adjusted),
        "industry_contribution_spread": _round(industry),
        "industry_adjusted_spread": _round(industry_adjusted),
        "style_proxy_contribution_spread": _round(style),
        "style_proxy_adjusted_spread": _round(style_adjusted),
        "full_residual_spread": _round(residual),
        "reconstruction_error": _round(reconstruction_error),
        "waterfall_status": status,
        "same_close_trading_used": False,
        "not_style_neutral_alpha": True,
        "not_alpha_evidence": True,
    }


def _spread(frame: pd.DataFrame, signal_column: str, value_column: str) -> float:
    ranked = frame.sort_values(signal_column)
    tail = max(1, len(ranked) // 5)
    return float(ranked.tail(tail)[value_column].mean() - ranked.head(tail)[value_column].mean())


def _period_status(
    gross_spread: float,
    qqq_relative_spread: float,
    beta_adjusted_spread: float,
    full_residual_spread: float,
) -> str:
    if qqq_relative_spread < 0.0 and beta_adjusted_spread < 0.0 and full_residual_spread > 0.0:
        return "style_proxy_conflict"
    if gross_spread > 0.0 and full_residual_spread <= 0.0:
        return "residual_not_positive"
    if full_residual_spread > 0.0:
        return "proxy_residual_positive"
    return "diagnostic_only"


def _benchmark_return(
    benchmark_lookup: Mapping[pd.Timestamp, float],
    period_start: pd.Timestamp,
    period_end: pd.Timestamp,
) -> float:
    start = benchmark_lookup.get(period_start)
    end = benchmark_lookup.get(period_end)
    if start is None or end is None or start == 0.0:
        return 0.0
    return float(end / start - 1.0)


def _summarize_waterfall(by_period: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    if by_period.empty:
        return pd.DataFrame(columns=_summary_columns())
    for factor_id, group in by_period.groupby("factor_id", sort=False):
        status = _summary_status(group)
        rows.append(
            {
                "schema_version": "factor_attribution_waterfall.v1",
                "factor_id": factor_id,
                "period_count": int(len(group)),
                "gross_spread_mean": _round(group["gross_spread"].mean()),
                "raw_spread_mean": _round(group["gross_spread"].mean()),
                "qqq_relative_spread_mean": _round(group["qqq_relative_spread"].mean()),
                "intercept_contribution_mean": _round(group["intercept_contribution_spread"].mean()),
                "market_beta_contribution_mean": _round(group["market_beta_contribution_spread"].mean()),
                "beta_adjusted_spread_mean": _round(group["beta_adjusted_spread"].mean()),
                "industry_contribution_mean": _round(group["industry_contribution_spread"].mean()),
                "industry_adjusted_spread_mean": _round(group["industry_adjusted_spread"].mean()),
                "style_proxy_contribution_mean": _round(group["style_proxy_contribution_spread"].mean()),
                "style_proxy_adjusted_spread_mean": _round(group["style_proxy_adjusted_spread"].mean()),
                "full_residual_spread_mean": _round(group["full_residual_spread"].mean()),
                "reconstruction_error_mean": _round(group["reconstruction_error"].abs().mean()),
                "style_proxy_conflict_count": int(group["waterfall_status"].eq("style_proxy_conflict").sum()),
                "waterfall_status": status,
                "redundancy_gate_allowed": False,
                "allowed_next_layer": "strict_residual_closeout_only",
                "not_style_neutral_alpha": True,
                "not_alpha_evidence": True,
                "production_approval": False,
                "direct_q2_entry": False,
            }
        )
    return pd.DataFrame(rows, columns=_summary_columns())


def _summary_status(group: pd.DataFrame) -> str:
    if group["waterfall_status"].eq("style_proxy_conflict").any():
        return "style_proxy_conflict"
    residual_mean = float(group["full_residual_spread"].mean())
    gross_mean = float(group["gross_spread"].mean())
    if gross_mean > 0.0 and residual_mean <= 0.0:
        return "residual_not_positive"
    if residual_mean > 0.0:
        return "proxy_residual_positive"
    return "diagnostic_only"


def _factor_payload(row: object) -> dict[str, object]:
    return {
        "schema_version": "factor_attribution_waterfall_factor.v1",
        "factor_id": str(row.factor_id),
        "waterfall_status": str(row.waterfall_status),
        "gross_spread_mean": float(row.gross_spread_mean),
        "qqq_relative_spread_mean": float(row.qqq_relative_spread_mean),
        "beta_adjusted_spread_mean": float(row.beta_adjusted_spread_mean),
        "industry_adjusted_spread_mean": float(row.industry_adjusted_spread_mean),
        "style_proxy_adjusted_spread_mean": float(row.style_proxy_adjusted_spread_mean),
        "full_residual_spread_mean": float(row.full_residual_spread_mean),
        "redundancy_gate_allowed": False,
        "allowed_next_layer": "strict_residual_closeout_only",
        "terminology": {
            "full_residual_spread": "factor sleeve spread after configured proxy risk-model components",
            "forbidden_claim": "configured proxy residual is not style-neutral alpha",
        },
        "non_claims": _non_claims(),
    }


def _diagnostics(
    waterfall: pd.DataFrame,
    by_period: pd.DataFrame,
    research_manifest_path: Path,
    residual_returns_path: Path,
) -> dict[str, object]:
    conflict_factors = (
        waterfall.loc[waterfall["waterfall_status"].eq("style_proxy_conflict"), "factor_id"].astype(str).tolist()
        if not waterfall.empty
        else []
    )
    return {
        "schema_version": "factor_attribution_waterfall_diagnostics.v1",
        "model_use": "factor_sleeve_ex_post_attribution_only",
        "research_manifest_path": str(research_manifest_path),
        "residual_returns_path": str(residual_returns_path),
        "factor_count": int(len(waterfall)),
        "period_row_count": int(len(by_period)),
        "style_proxy_conflict_factors": conflict_factors,
        "max_abs_reconstruction_error": _round(by_period["reconstruction_error"].abs().max()) if not by_period.empty else None,
        "component_order": [
            "gross_spread",
            "market_beta_contribution",
            "industry_contribution",
            "style_proxy_contribution",
            "full_residual_spread",
        ],
        "terminology": {
            "proxy_residual": "residual after the configured proxy risk model only",
            "forbidden_claim": "proxy residual is not style-neutral alpha",
        },
        "non_claims": {
            **_non_claims(),
            "residual_is_style_neutral": False,
            "residual_is_tradeable_prediction": False,
        },
    }


def _render_report(waterfall: pd.DataFrame, diagnostics: Mapping[str, object]) -> str:
    lines = [
        "# Factor Attribution Waterfall",
        "",
        "This is ex-post factor-sleeve attribution under the configured proxy risk model.",
        "The residual is not style-neutral alpha and is not a tradeable prediction.",
        "",
        "No production approval, no paper canary, no live trading, no security orders, no allocator entry, and no direct Q2 entry.",
        "",
    ]
    if waterfall.empty:
        lines.extend(["No factor waterfalls were produced.", ""])
        return "\n".join(lines)
    lines.extend(["## Factor Summary", ""])
    for row in waterfall.itertuples(index=False):
        lines.append(
            f"- `{row.factor_id}`: status `{row.waterfall_status}`, gross={row.gross_spread_mean:.6f}, "
            f"QQQ-relative={row.qqq_relative_spread_mean:.6f}, beta-adjusted={row.beta_adjusted_spread_mean:.6f}, "
            f"industry-adjusted={row.industry_adjusted_spread_mean:.6f}, "
            f"style-proxy-adjusted={row.style_proxy_adjusted_spread_mean:.6f}, "
            f"full residual={row.full_residual_spread_mean:.6f}."
        )
    conflict_factors = diagnostics.get("style_proxy_conflict_factors", [])
    if conflict_factors:
        lines.extend(
            [
                "",
                "## Benchmark / Beta Conflicts",
                "",
                (
                    "A positive proxy residual remains diagnostic only when benchmark/beta readouts are negative. "
                    "These rows are marked as style proxy conflict and must go to strict residual closeout."
                ),
                "",
            ]
        )
        for factor_id in conflict_factors:
            lines.append(f"- `{factor_id}` is a style proxy conflict.")
    lines.append("")
    return "\n".join(lines)


def _non_claims() -> dict[str, bool]:
    return {
        "not_alpha_evidence": True,
        "production_approval": False,
        "paper_canary": False,
        "live_trading": False,
        "security_orders": False,
        "direct_q2_entry": False,
        "allocator_entry": False,
    }


def _round(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), 10)


def _date_str(value: object) -> str:
    return pd.Timestamp(value).date().isoformat()


def _period_columns() -> list[str]:
    return [
        "schema_version",
        "factor_id",
        "date",
        "period_start",
        "period_end",
        "asset_count",
        "tail_count",
        "gross_spread",
        "qqq_return",
        "qqq_relative_spread",
        "intercept_contribution_spread",
        "market_beta_contribution_spread",
        "beta_adjusted_spread",
        "industry_contribution_spread",
        "industry_adjusted_spread",
        "style_proxy_contribution_spread",
        "style_proxy_adjusted_spread",
        "full_residual_spread",
        "reconstruction_error",
        "waterfall_status",
        "same_close_trading_used",
        "not_style_neutral_alpha",
        "not_alpha_evidence",
    ]


def _summary_columns() -> list[str]:
    return [
        "schema_version",
        "factor_id",
        "period_count",
        "gross_spread_mean",
        "raw_spread_mean",
        "qqq_relative_spread_mean",
        "intercept_contribution_mean",
        "market_beta_contribution_mean",
        "beta_adjusted_spread_mean",
        "industry_contribution_mean",
        "industry_adjusted_spread_mean",
        "style_proxy_contribution_mean",
        "style_proxy_adjusted_spread_mean",
        "full_residual_spread_mean",
        "reconstruction_error_mean",
        "style_proxy_conflict_count",
        "waterfall_status",
        "redundancy_gate_allowed",
        "allowed_next_layer",
        "not_style_neutral_alpha",
        "not_alpha_evidence",
        "production_approval",
        "direct_q2_entry",
    ]
