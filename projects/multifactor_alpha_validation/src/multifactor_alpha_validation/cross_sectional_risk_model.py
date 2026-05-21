from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Mapping

import numpy as np
import pandas as pd
import yaml


@dataclass(frozen=True)
class CrossSectionalRiskModelResult:
    returns_by_period_path: str
    coefficients_path: str
    residual_returns_path: str
    fit_diagnostics_path: str
    period_count: int
    residual_return_count: int
    production_approval: bool
    live_trading: bool
    direct_q2_entry: bool
    not_alpha_evidence: bool


REQUIRED_EXPOSURES = (
    "sector",
    "industry",
    "trailing_market_beta_252d",
    "log_market_cap",
    "liquidity_adv_60d",
    "residual_volatility_60d",
    "short_term_reversal_5d",
    "medium_term_momentum_12_1",
)
STYLE_EXPOSURES = (
    "log_market_cap",
    "liquidity_adv_60d",
    "residual_volatility_60d",
    "short_term_reversal_5d",
    "medium_term_momentum_12_1",
    "fundamental_book_to_market",
    "fundamental_profitability_roa",
    "fundamental_asset_growth",
)
MIN_ASSETS = 3
RIDGE_ALPHA = 1e-4


def run_cross_sectional_risk_model(
    research_manifest_path: Path,
    exposure_panel_path: Path,
    output_dir: Path,
    ridge_alpha: float = RIDGE_ALPHA,
    min_assets: int = MIN_ASSETS,
) -> CrossSectionalRiskModelResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    returns_path = output_dir / "risk_model_returns_by_period.csv"
    coefficients_path = output_dir / "risk_model_exposure_coefficients.csv"
    residuals_path = output_dir / "risk_model_residual_returns.csv"
    diagnostics_path = output_dir / "risk_model_fit_diagnostics.json"

    manifest = _load_yaml(research_manifest_path)
    prices = _normalize_prices(_load_research_csv(manifest, research_manifest_path, "prices"))
    exposures = _normalize_exposures(pd.read_csv(exposure_panel_path, dtype={"asset_id": str}))

    residual_rows: list[dict[str, object]] = []
    coefficient_rows: list[dict[str, object]] = []
    period_rows: list[dict[str, object]] = []
    instability_rows: list[dict[str, object]] = []
    missing_exposure_row_count = 0

    exposure_dates = sorted(pd.Timestamp(value) for value in exposures["date"].dropna().unique())
    for index, signal_date in enumerate(exposure_dates[:-1]):
        next_rebalance_date = exposure_dates[index + 1]
        period_exposures = exposures[exposures["date"].eq(signal_date)].copy()
        if period_exposures.empty:
            continue
        tradable_date = _period_tradable_date(period_exposures)
        if tradable_date is None:
            continue
        realized = _period_returns(prices, tradable_date, next_rebalance_date)
        if realized.empty:
            continue
        wide = _wide_exposures(period_exposures)
        frame = realized.merge(wide, on="asset_id", how="left")
        required_reason = frame.apply(_required_abstain_reason, axis=1)
        frame["coverage_flag"] = required_reason.eq("")
        frame["abstain_reason"] = required_reason
        missing_exposure_row_count += int((~frame["coverage_flag"]).sum())

        fit_frame = frame[frame["coverage_flag"]].copy()
        if len(fit_frame) < min_assets:
            residual_rows.extend(
                _unfitted_rows(
                    signal_date=signal_date,
                    period_start=tradable_date,
                    period_end=next_rebalance_date,
                    frame=frame,
                    fallback_reason="insufficient_cross_sectional_assets",
                )
            )
            period_rows.append(
                _period_summary(
                    signal_date,
                    tradable_date,
                    next_rebalance_date,
                    frame,
                    fitted=pd.DataFrame(),
                    condition_number=np.nan,
                    ridge_alpha=ridge_alpha,
                    attribution_complete=False,
                    instability_reason="insufficient_cross_sectional_assets",
                )
            )
            instability_rows.append(
                {
                    "date": _date_str(signal_date),
                    "reason": "insufficient_cross_sectional_assets",
                    "asset_count": int(len(frame)),
                    "used_asset_count": int(len(fit_frame)),
                    "design_column_count": 0,
                    "condition_number": None,
                }
            )
            continue

        design = _build_design_matrix(fit_frame)
        if design["x"].shape[1] == 0:
            continue
        model = _fit_ridge(design["x"], fit_frame["realized_return"].to_numpy(dtype=float), ridge_alpha)
        fitted = fit_frame.copy()
        _add_attribution_columns(fitted, design, model["coef"])
        period_instability_reason = _instability_reason(design["x"], model["condition_number"])
        if period_instability_reason:
            instability_rows.append(
                {
                    "date": _date_str(signal_date),
                    "reason": period_instability_reason,
                    "asset_count": int(len(frame)),
                    "used_asset_count": int(len(fit_frame)),
                    "design_column_count": int(design["x"].shape[1]),
                    "condition_number": round(float(model["condition_number"]), 6)
                    if pd.notna(model["condition_number"])
                    else None,
                }
            )
        residual_rows.extend(
            _fitted_rows(
                signal_date=signal_date,
                period_start=tradable_date,
                period_end=next_rebalance_date,
                fitted=fitted,
            )
        )
        residual_rows.extend(
            _unfitted_rows(
                signal_date=signal_date,
                period_start=tradable_date,
                period_end=next_rebalance_date,
                frame=frame[~frame["coverage_flag"]],
                fallback_reason="",
            )
        )
        coefficient_rows.extend(
            _coefficient_rows(
                signal_date=signal_date,
                period_start=tradable_date,
                period_end=next_rebalance_date,
                feature_names=design["feature_names"],
                feature_types=design["feature_types"],
                coef=model["coef"],
            )
        )
        period_rows.append(
            _period_summary(
                signal_date,
                tradable_date,
                next_rebalance_date,
                frame,
                fitted=fitted,
                condition_number=model["condition_number"],
                ridge_alpha=ridge_alpha,
                attribution_complete=True,
                instability_reason=period_instability_reason,
            )
        )

    residuals = pd.DataFrame(residual_rows, columns=_residual_columns())
    coefficients = pd.DataFrame(coefficient_rows, columns=_coefficient_columns())
    returns = pd.DataFrame(period_rows, columns=_period_columns())
    diagnostics = _diagnostics(
        returns=returns,
        residuals=residuals,
        coefficients=coefficients,
        instability_rows=instability_rows,
        missing_exposure_row_count=missing_exposure_row_count,
        ridge_alpha=ridge_alpha,
        research_manifest_path=research_manifest_path,
        exposure_panel_path=exposure_panel_path,
    )

    residuals.to_csv(residuals_path, index=False)
    coefficients.to_csv(coefficients_path, index=False)
    returns.to_csv(returns_path, index=False)
    diagnostics_path.write_text(json.dumps(diagnostics, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return CrossSectionalRiskModelResult(
        returns_by_period_path=str(returns_path),
        coefficients_path=str(coefficients_path),
        residual_returns_path=str(residuals_path),
        fit_diagnostics_path=str(diagnostics_path),
        period_count=int(len(returns)),
        residual_return_count=int(len(residuals)),
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
    return normalized.dropna(subset=["asset_id", "date", "adjusted_close"]).sort_values(["date", "asset_id"])


def _normalize_exposures(exposures: pd.DataFrame) -> pd.DataFrame:
    normalized = exposures.copy()
    normalized["asset_id"] = normalized["asset_id"].astype(str)
    normalized["date"] = pd.to_datetime(normalized["date"], errors="coerce")
    normalized["tradable_timestamp"] = pd.to_datetime(normalized["tradable_timestamp"], errors="coerce")
    normalized["coverage_flag"] = normalized["coverage_flag"].map(_as_bool)
    return normalized.dropna(subset=["asset_id", "date", "exposure_name"])


def _as_bool(value: object) -> bool:
    if isinstance(value, bool):
        return value
    return str(value).strip().lower() in {"true", "1", "yes"}


def _period_tradable_date(period_exposures: pd.DataFrame) -> pd.Timestamp | None:
    values = period_exposures["tradable_timestamp"].dropna().unique()
    if len(values) == 0:
        return None
    return pd.Timestamp(min(values))


def _period_returns(prices: pd.DataFrame, period_start: pd.Timestamp, period_end: pd.Timestamp) -> pd.DataFrame:
    start = prices[prices["date"].eq(period_start)][["asset_id", "adjusted_close"]].rename(
        columns={"adjusted_close": "start_close"}
    )
    end = prices[prices["date"].eq(period_end)][["asset_id", "adjusted_close"]].rename(
        columns={"adjusted_close": "end_close"}
    )
    merged = start.merge(end, on="asset_id", how="inner")
    if merged.empty:
        return pd.DataFrame(columns=["asset_id", "realized_return"])
    merged["realized_return"] = merged["end_close"] / merged["start_close"] - 1.0
    return merged[["asset_id", "realized_return"]]


def _wide_exposures(period_exposures: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for asset_id, group in period_exposures.groupby("asset_id", sort=False):
        row: dict[str, object] = {"asset_id": str(asset_id)}
        for exposure_name, exposure_rows in group.groupby("exposure_name", sort=False):
            exposure_row = exposure_rows.iloc[-1]
            name = str(exposure_name)
            covered = bool(exposure_row["coverage_flag"])
            row[f"{name}__covered"] = covered
            row[f"{name}__abstain_reason"] = str(exposure_row.get("abstain_reason", "") or "")
            row[name] = exposure_row.get("exposure_value") if covered else np.nan
        rows.append(row)
    return pd.DataFrame(rows)


def _required_abstain_reason(row: pd.Series) -> str:
    missing: list[str] = []
    for exposure in REQUIRED_EXPOSURES:
        if not bool(row.get(f"{exposure}__covered", False)):
            missing.append(exposure)
            continue
        value = row.get(exposure)
        if exposure not in {"sector", "industry"} and pd.isna(pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0]):
            missing.append(exposure)
    if not missing:
        return ""
    return f"missing_required_exposure:{missing[0]}"


def _build_design_matrix(frame: pd.DataFrame) -> dict[str, object]:
    working = frame.copy()
    design_parts: list[np.ndarray] = [np.ones((len(working), 1))]
    feature_names: list[str] = ["intercept"]
    feature_types: list[str] = ["intercept"]

    beta = pd.to_numeric(working["trailing_market_beta_252d"], errors="coerce").fillna(0.0)
    design_parts.append(beta.to_numpy(dtype=float).reshape(-1, 1))
    feature_names.append("trailing_market_beta_252d")
    feature_types.append("market_beta")

    industry = working["industry"].fillna("unknown_industry").astype(str)
    industry_dummies = pd.get_dummies(industry, drop_first=True, dtype=float)
    for column in industry_dummies.columns:
        design_parts.append(industry_dummies[column].to_numpy(dtype=float).reshape(-1, 1))
        feature_names.append(f"industry:{column}")
        feature_types.append("industry")

    for exposure in STYLE_EXPOSURES:
        if exposure not in working.columns:
            continue
        series = pd.to_numeric(working[exposure], errors="coerce")
        if series.notna().sum() < MIN_ASSETS or float(series.dropna().std(ddof=0)) == 0.0:
            continue
        transformed = np.log1p(series.clip(lower=0.0)) if exposure == "liquidity_adv_60d" else series
        zscore = _zscore(transformed.fillna(transformed.median()))
        design_parts.append(zscore.to_numpy(dtype=float).reshape(-1, 1))
        feature_names.append(exposure)
        feature_types.append("style")

    x = np.column_stack(design_parts)
    return {"x": x, "feature_names": feature_names, "feature_types": feature_types}


def _fit_ridge(x: np.ndarray, y: np.ndarray, ridge_alpha: float) -> dict[str, object]:
    penalty = np.eye(x.shape[1]) * ridge_alpha
    penalty[0, 0] = 0.0
    coef = np.linalg.solve(x.T @ x + penalty, x.T @ y)
    condition_number = float(np.linalg.cond(x)) if x.size else np.nan
    return {"coef": coef, "condition_number": condition_number}


def _add_attribution_columns(fitted: pd.DataFrame, design: Mapping[str, object], coef: np.ndarray) -> None:
    x = design["x"]
    feature_types = list(design["feature_types"])
    assert isinstance(x, np.ndarray)
    contribution = x * coef.reshape(1, -1)
    fitted["intercept_contribution"] = _sum_contributions(contribution, feature_types, "intercept")
    fitted["market_beta_contribution"] = _sum_contributions(contribution, feature_types, "market_beta")
    fitted["industry_contribution"] = _sum_contributions(contribution, feature_types, "industry")
    fitted["style_contribution"] = _sum_contributions(contribution, feature_types, "style")
    fitted["fitted_return"] = contribution.sum(axis=1)
    fitted["residual_return"] = fitted["realized_return"].to_numpy(dtype=float) - fitted["fitted_return"].to_numpy(dtype=float)


def _sum_contributions(contribution: np.ndarray, feature_types: list[str], feature_type: str) -> np.ndarray:
    indices = [index for index, value in enumerate(feature_types) if value == feature_type]
    if not indices:
        return np.zeros(contribution.shape[0])
    return contribution[:, indices].sum(axis=1)


def _instability_reason(x: np.ndarray, condition_number: float) -> str:
    if x.shape[1] >= x.shape[0]:
        return "underdetermined_or_saturated_cross_section"
    if pd.notna(condition_number) and condition_number > 1e8:
        return "high_condition_number"
    return ""


def _fitted_rows(
    signal_date: pd.Timestamp,
    period_start: pd.Timestamp,
    period_end: pd.Timestamp,
    fitted: pd.DataFrame,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in fitted.itertuples(index=False):
        rows.append(
            {
                "schema_version": "cross_sectional_risk_model_residual.v1",
                "date": _date_str(signal_date),
                "period_start": _date_str(period_start),
                "period_end": _date_str(period_end),
                "asset_id": str(row.asset_id),
                "realized_return": _round(row.realized_return),
                "intercept_contribution": _round(row.intercept_contribution),
                "market_beta_contribution": _round(row.market_beta_contribution),
                "industry_contribution": _round(row.industry_contribution),
                "style_contribution": _round(row.style_contribution),
                "fitted_return": _round(row.fitted_return),
                "residual_return": _round(row.residual_return),
                "coverage_flag": True,
                "abstain_reason": "",
                "not_tradeable_prediction": True,
                "not_alpha_evidence": True,
            }
        )
    return rows


def _unfitted_rows(
    signal_date: pd.Timestamp,
    period_start: pd.Timestamp,
    period_end: pd.Timestamp,
    frame: pd.DataFrame,
    fallback_reason: str,
) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for row in frame.itertuples(index=False):
        reason = str(getattr(row, "abstain_reason", "") or fallback_reason)
        rows.append(
            {
                "schema_version": "cross_sectional_risk_model_residual.v1",
                "date": _date_str(signal_date),
                "period_start": _date_str(period_start),
                "period_end": _date_str(period_end),
                "asset_id": str(row.asset_id),
                "realized_return": _round(getattr(row, "realized_return", np.nan)),
                "intercept_contribution": np.nan,
                "market_beta_contribution": np.nan,
                "industry_contribution": np.nan,
                "style_contribution": np.nan,
                "fitted_return": np.nan,
                "residual_return": np.nan,
                "coverage_flag": False,
                "abstain_reason": reason,
                "not_tradeable_prediction": True,
                "not_alpha_evidence": True,
            }
        )
    return rows


def _coefficient_rows(
    signal_date: pd.Timestamp,
    period_start: pd.Timestamp,
    period_end: pd.Timestamp,
    feature_names: list[str],
    feature_types: list[str],
    coef: np.ndarray,
) -> list[dict[str, object]]:
    return [
        {
            "schema_version": "cross_sectional_risk_model_coefficient.v1",
            "date": _date_str(signal_date),
            "period_start": _date_str(period_start),
            "period_end": _date_str(period_end),
            "coefficient_name": feature_name,
            "coefficient_type": feature_type,
            "coefficient_value": _round(value),
            "not_tradeable_prediction": True,
            "not_alpha_evidence": True,
        }
        for feature_name, feature_type, value in zip(feature_names, feature_types, coef, strict=True)
    ]


def _period_summary(
    signal_date: pd.Timestamp,
    period_start: pd.Timestamp,
    period_end: pd.Timestamp,
    frame: pd.DataFrame,
    fitted: pd.DataFrame,
    condition_number: float,
    ridge_alpha: float,
    attribution_complete: bool,
    instability_reason: str,
) -> dict[str, object]:
    if fitted.empty:
        r_squared = np.nan
        values = {
            "intercept_contribution_mean": np.nan,
            "market_beta_contribution_mean": np.nan,
            "industry_contribution_mean": np.nan,
            "style_contribution_mean": np.nan,
            "fitted_return_mean": np.nan,
            "residual_return_mean": np.nan,
        }
    else:
        y = fitted["realized_return"].to_numpy(dtype=float)
        residual = fitted["residual_return"].to_numpy(dtype=float)
        total = float(np.sum((y - y.mean()) ** 2))
        r_squared = 1.0 - float(np.sum(residual**2)) / total if total > 0.0 else np.nan
        values = {
            "intercept_contribution_mean": _round(fitted["intercept_contribution"].mean()),
            "market_beta_contribution_mean": _round(fitted["market_beta_contribution"].mean()),
            "industry_contribution_mean": _round(fitted["industry_contribution"].mean()),
            "style_contribution_mean": _round(fitted["style_contribution"].mean()),
            "fitted_return_mean": _round(fitted["fitted_return"].mean()),
            "residual_return_mean": _round(fitted["residual_return"].mean()),
        }
    return {
        "schema_version": "cross_sectional_risk_model_returns_by_period.v1",
        "date": _date_str(signal_date),
        "period_start": _date_str(period_start),
        "period_end": _date_str(period_end),
        "asset_count": int(len(frame)),
        "used_asset_count": int(len(fitted)),
        "abstain_asset_count": int(len(frame) - len(fitted)),
        **values,
        "r_squared": _round(r_squared),
        "condition_number": _round(condition_number),
        "ridge_alpha": ridge_alpha,
        "attribution_complete": bool(attribution_complete),
        "instability_reason": instability_reason,
        "not_tradeable_prediction": True,
        "not_alpha_evidence": True,
    }


def _diagnostics(
    returns: pd.DataFrame,
    residuals: pd.DataFrame,
    coefficients: pd.DataFrame,
    instability_rows: list[dict[str, object]],
    missing_exposure_row_count: int,
    ridge_alpha: float,
    research_manifest_path: Path,
    exposure_panel_path: Path,
) -> dict[str, object]:
    return {
        "schema_version": "cross_sectional_risk_model_diagnostics.v1",
        "model_use": "ex_post_attribution_only",
        "research_manifest_path": str(research_manifest_path),
        "exposure_panel_path": str(exposure_panel_path),
        "period_count": int(len(returns)),
        "residual_return_count": int(len(residuals)),
        "coefficient_count": int(len(coefficients)),
        "missing_exposure_row_count": int(missing_exposure_row_count),
        "instability_period_count": int(len(instability_rows)),
        "regression_instability_reported": True,
        "instability_report": instability_rows,
        "ridge_alpha": ridge_alpha,
        "required_exposures": list(REQUIRED_EXPOSURES),
        "style_exposures_considered": list(STYLE_EXPOSURES),
        "component_groups": ["intercept", "market_beta", "industry", "style", "residual"],
        "terminology": {
            "residual_return": "realized return left after configured ex-post proxy risk model components",
            "forbidden_claim": "configured proxy residual is not style neutral alpha",
        },
        "non_claims": {
            "not_alpha_evidence": True,
            "residual_is_tradeable_prediction": False,
            "production_approval": False,
            "live_trading": False,
            "security_orders": False,
            "direct_q2_entry": False,
            "allocator_entry": False,
        },
    }


def _zscore(series: pd.Series) -> pd.Series:
    std = float(series.std(ddof=0))
    if std == 0.0 or pd.isna(std):
        return pd.Series(0.0, index=series.index)
    return (series - float(series.mean())) / std


def _round(value: object) -> float | None:
    if value is None or pd.isna(value):
        return None
    return round(float(value), 10)


def _date_str(value: object) -> str:
    return pd.Timestamp(value).date().isoformat()


def _residual_columns() -> list[str]:
    return [
        "schema_version",
        "date",
        "period_start",
        "period_end",
        "asset_id",
        "realized_return",
        "intercept_contribution",
        "market_beta_contribution",
        "industry_contribution",
        "style_contribution",
        "fitted_return",
        "residual_return",
        "coverage_flag",
        "abstain_reason",
        "not_tradeable_prediction",
        "not_alpha_evidence",
    ]


def _coefficient_columns() -> list[str]:
    return [
        "schema_version",
        "date",
        "period_start",
        "period_end",
        "coefficient_name",
        "coefficient_type",
        "coefficient_value",
        "not_tradeable_prediction",
        "not_alpha_evidence",
    ]


def _period_columns() -> list[str]:
    return [
        "schema_version",
        "date",
        "period_start",
        "period_end",
        "asset_count",
        "used_asset_count",
        "abstain_asset_count",
        "intercept_contribution_mean",
        "market_beta_contribution_mean",
        "industry_contribution_mean",
        "style_contribution_mean",
        "fitted_return_mean",
        "residual_return_mean",
        "r_squared",
        "condition_number",
        "ridge_alpha",
        "attribution_complete",
        "instability_reason",
        "not_tradeable_prediction",
        "not_alpha_evidence",
    ]
