"""Mechanism-distinct price-volume formulas for FD FactorSpec v2."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass
from typing import Callable

import numpy as np
import pandas as pd


FORMULA_VERSION = "price_volume_29_mechanism_v2"
EPSILON = 1e-12


@dataclass(frozen=True)
class FormulaFrame:
    """Formula outputs with fallback audit fields."""

    values: pd.DataFrame
    fallback_used: pd.DataFrame
    fallback_reason: pd.DataFrame
    research_evidence_quality: pd.DataFrame


@dataclass(frozen=True)
class FormulaSpec:
    """Formula registry entry."""

    factor_id: str
    mechanism_family: str
    formula_summary: str
    raw_value_definition: str
    oriented_score_definition: str
    required_inputs: list[str]
    fallback_policy: str
    compute: Callable[..., FormulaFrame]

    @property
    def formula_version(self) -> str:
        return FORMULA_VERSION

    @property
    def formula_hash(self) -> str:
        payload = "|".join(
            [
                self.factor_id,
                self.formula_version,
                self.formula_summary,
                self.raw_value_definition,
                self.oriented_score_definition,
                ",".join(self.required_inputs),
                self.fallback_policy,
            ]
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:16]


def as_formula_frame(values: pd.DataFrame, fallback_used: bool = False, fallback_reason: str = "") -> FormulaFrame:
    """Wrap raw formula values in default audit frames."""

    used = pd.DataFrame(fallback_used, index=values.index, columns=values.columns)
    reason = pd.DataFrame(fallback_reason, index=values.index, columns=values.columns)
    quality_value = "degraded" if fallback_used else "standard"
    quality = pd.DataFrame(quality_value, index=values.index, columns=values.columns)
    return FormulaFrame(values=values, fallback_used=used, fallback_reason=reason, research_evidence_quality=quality)


def compute_log_momentum(close: pd.DataFrame, lookback_days: int, skip_days: int) -> pd.DataFrame:
    shifted = close.shift(skip_days)
    base = close.shift(lookback_days + skip_days)
    ratio = shifted.where(shifted > 0.0).div(base.where(base > 0.0))
    return np.log(ratio)


def compute_sector_neutral_residual_momentum(
    close: pd.DataFrame,
    classifications: pd.DataFrame,
    lookback_days: int,
    skip_days: int,
) -> FormulaFrame:
    momentum = compute_log_momentum(close, lookback_days=lookback_days, skip_days=skip_days)
    class_map = _classification_map(classifications)
    values = momentum.copy() * np.nan
    fallback_used = pd.DataFrame(False, index=momentum.index, columns=momentum.columns)
    fallback_reason = pd.DataFrame("", index=momentum.index, columns=momentum.columns)
    quality = pd.DataFrame("standard", index=momentum.index, columns=momentum.columns)

    for date, row in momentum.iterrows():
        available = row.dropna()
        if available.empty:
            continue
        group_keys: dict[str, str] = {}
        for asset_id in available.index.astype(str):
            group, reason = class_map.get(asset_id, ("__cross_section__", "missing_sector_and_industry"))
            group_keys[asset_id] = group
            if reason:
                fallback_used.at[date, asset_id] = True
                fallback_reason.at[date, asset_id] = reason
                quality.at[date, asset_id] = "degraded"
        groups = pd.Series(group_keys)
        for group in groups.unique():
            members = groups[groups == group].index.tolist()
            baseline = float(available.reindex(members).median())
            values.loc[date, members] = available.reindex(members) - baseline
    return FormulaFrame(values=values, fallback_used=fallback_used, fallback_reason=fallback_reason, research_evidence_quality=quality)


def compute_log_price_trend_quality(close: pd.DataFrame, window_days: int, skip_days: int) -> pd.DataFrame:
    shifted = np.log(close.shift(skip_days).where(close.shift(skip_days) > 0.0))
    return shifted.rolling(window_days, min_periods=window_days).apply(_linear_trend_quality_array, raw=True)


def compute_price_to_high_distance(close: pd.DataFrame, window_days: int, skip_days: int) -> pd.DataFrame:
    shifted = close.shift(skip_days)
    rolling_high = shifted.rolling(window_days, min_periods=window_days).max()
    return np.log(shifted.div(rolling_high))


def compute_window_max_drawdown_severity(close: pd.DataFrame, window_days: int, skip_days: int) -> pd.DataFrame:
    shifted = close.shift(skip_days)
    return shifted.rolling(window_days, min_periods=window_days).apply(_max_drawdown_array, raw=True)


def compute_vol_adjusted_overshoot_reversal(
    close: pd.DataFrame,
    recent_days: int,
    prior_days: int,
    skip_days: int,
) -> pd.DataFrame:
    shifted = close.shift(skip_days)
    recent = np.log(shifted.div(close.shift(recent_days + skip_days)))
    prior = np.log(close.shift(recent_days + skip_days).div(close.shift(recent_days + prior_days + skip_days)))
    sigma = np.log(close).diff().shift(skip_days).rolling(recent_days, min_periods=recent_days).std() * np.sqrt(recent_days)
    prior_z = _robust_cross_sectional_zscore(prior)
    extension_gate = np.maximum(0.0, np.sign(recent) * prior_z).clip(upper=3.0) / 3.0
    score = -recent.div(sigma.where(sigma > EPSILON)) * extension_gate
    return score.replace([np.inf, -np.inf], np.nan)


def compute_liquidity_shock(
    close: pd.DataFrame,
    volume: pd.DataFrame,
    recent_days: int,
    baseline_days: int,
    skip_days: int,
) -> pd.DataFrame:
    dollar_volume = close * volume
    recent_adv = dollar_volume.shift(skip_days).rolling(recent_days, min_periods=recent_days).mean()
    baseline_adv = dollar_volume.shift(skip_days + recent_days).rolling(baseline_days, min_periods=baseline_days).mean()
    return np.log(recent_adv.div(baseline_adv.where(baseline_adv > 0.0))).replace([np.inf, -np.inf], np.nan)


def compute_persistent_dollar_volume_capacity(
    close: pd.DataFrame,
    volume: pd.DataFrame,
    window_days: int,
    skip_days: int,
) -> pd.DataFrame:
    dollar_volume = close * volume
    adv = dollar_volume.shift(skip_days).rolling(window_days, min_periods=window_days).mean()
    return np.log(adv.where(adv > 0.0)).replace([np.inf, -np.inf], np.nan)


def compute_abnormal_turnover_zscore(
    turnover_or_volume: pd.DataFrame,
    recent_days: int,
    baseline_days: int,
    skip_days: int,
) -> pd.DataFrame:
    recent = turnover_or_volume.shift(skip_days).rolling(recent_days, min_periods=recent_days).mean()
    baseline_window = turnover_or_volume.shift(skip_days + recent_days).rolling(baseline_days, min_periods=baseline_days)
    baseline = baseline_window.mean()
    baseline_std = baseline_window.std()
    return recent.sub(baseline).div(baseline_std.where(baseline_std > EPSILON)).replace([np.inf, -np.inf], np.nan)


def compute_turnover_trend_persistence(turnover_or_volume: pd.DataFrame, window_days: int, skip_days: int) -> pd.DataFrame:
    shifted = np.log1p(turnover_or_volume.shift(skip_days).where(turnover_or_volume.shift(skip_days) >= 0.0))
    return shifted.rolling(window_days, min_periods=window_days).apply(_linear_trend_quality_array, raw=True)


def compute_ema_gap(close: pd.DataFrame, window_days: int, skip_days: int) -> pd.DataFrame:
    shifted = close.shift(skip_days)
    ema = shifted.ewm(span=max(window_days, 2), adjust=False).mean()
    return np.log(shifted.div(ema))


def compute_range_ratio(close: pd.DataFrame, window_days: int, skip_days: int) -> pd.DataFrame:
    shifted = close.shift(skip_days)
    high = shifted.rolling(window_days, min_periods=window_days).max()
    low = shifted.rolling(window_days, min_periods=window_days).min()
    return high.sub(low).div(shifted.where(shifted > 0.0))


def compute_realized_volatility(close: pd.DataFrame, window_days: int, skip_days: int) -> pd.DataFrame:
    returns = np.log(close).diff()
    return returns.shift(skip_days).rolling(window_days, min_periods=window_days).std() * np.sqrt(window_days)


def compute_volume_momentum(volume: pd.DataFrame, lookback_days: int, skip_days: int) -> pd.DataFrame:
    shifted = volume.shift(skip_days)
    base = volume.shift(lookback_days + skip_days)
    ratio = shifted.where(shifted > 0.0).div(base.where(base > 0.0))
    return np.log(ratio).replace([np.inf, -np.inf], np.nan)


def _classification_map(classifications: pd.DataFrame) -> dict[str, tuple[str, str]]:
    if classifications.empty:
        return {}
    frame = classifications.copy()
    frame["asset_id"] = frame["asset_id"].astype(str)
    rows: dict[str, tuple[str, str]] = {}
    for row in frame.drop_duplicates("asset_id", keep="last").itertuples(index=False):
        sector = str(getattr(row, "sector", "") or "").strip()
        industry = str(getattr(row, "industry", "") or "").strip()
        asset_id = str(getattr(row, "asset_id"))
        if sector and sector.lower() != "nan":
            rows[asset_id] = (f"sector:{sector}", "")
        elif industry and industry.lower() != "nan":
            rows[asset_id] = (f"industry:{industry}", "missing_sector_used_industry")
        else:
            rows[asset_id] = ("__cross_section__", "missing_sector_and_industry")
    return rows


def _robust_cross_sectional_zscore(frame: pd.DataFrame) -> pd.DataFrame:
    median = frame.median(axis=1)
    centered = frame.sub(median, axis=0)
    mad = centered.abs().median(axis=1)
    std = frame.std(axis=1)
    scale = (1.4826 * mad).where(mad > EPSILON, std)
    scaled = centered.div(scale.where(scale > EPSILON, np.nan), axis=0)
    return scaled.clip(lower=-6.0, upper=6.0)


def _linear_trend_quality_array(values: np.ndarray) -> float:
    if len(values) < 3 or np.any(~np.isfinite(values)):
        return np.nan
    y = values.astype("float64")
    x = np.arange(len(y), dtype="float64")
    x_centered = x - x.mean()
    y_centered = y - y.mean()
    sxx = float(np.dot(x_centered, x_centered))
    if sxx <= EPSILON:
        return np.nan
    beta = float(np.dot(x_centered, y_centered) / sxx)
    fitted = y.mean() + beta * x_centered
    residual = y - fitted
    sse = float(np.dot(residual, residual))
    sst = float(np.dot(y_centered, y_centered))
    if sst <= EPSILON:
        return 0.0
    r_squared = max(0.0, 1.0 - sse / sst)
    dof = len(y) - 2
    if dof <= 0:
        return np.nan
    sigma2 = sse / dof
    if sigma2 <= EPSILON:
        beta_tstat = np.sign(beta) * 1_000.0
    else:
        beta_tstat = beta / np.sqrt(sigma2 / sxx)
    return float(beta_tstat * r_squared)


def _max_drawdown_array(values: np.ndarray) -> float:
    if len(values) < 2 or np.any(~np.isfinite(values)) or np.any(values <= 0.0):
        return np.nan
    running_peak = np.maximum.accumulate(values)
    drawdown = 1.0 - values / running_peak
    return float(np.max(drawdown))
