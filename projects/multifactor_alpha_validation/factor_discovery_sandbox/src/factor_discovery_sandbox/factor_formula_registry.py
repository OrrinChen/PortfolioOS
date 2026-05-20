"""Stable FactorSpec v2 formula registry for the FD sandbox."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Callable

import pandas as pd

from .factor_formulas_v2 import (
    FORMULA_VERSION,
    FormulaFrame,
    FormulaSpec,
    as_formula_frame,
    compute_abnormal_turnover_zscore,
    compute_ema_gap,
    compute_liquidity_shock,
    compute_log_momentum,
    compute_log_price_trend_quality,
    compute_persistent_dollar_volume_capacity,
    compute_price_to_high_distance,
    compute_range_ratio,
    compute_realized_volatility,
    compute_sector_neutral_residual_momentum,
    compute_turnover_trend_persistence,
    compute_vol_adjusted_overshoot_reversal,
    compute_volume_momentum,
    compute_window_max_drawdown_severity,
)


TRADING_DAYS_PER_MONTH = 21
NEGATIVE_ORIENTATION_FACTORS = {
    "volatility_1m",
    "volatility_2m",
    "volatility_3m",
    "volatility_6m",
    "drawdown_3m",
    "drawdown_12m",
    "range_1m",
    "range_3m",
}


@dataclass(frozen=True)
class FormulaInputs:
    """Aligned daily inputs for FormulaSpec v2 computation."""

    close: pd.DataFrame
    volume: pd.DataFrame
    classifications: pd.DataFrame
    shares_float: pd.DataFrame | None = None
    shares_outstanding: pd.DataFrame | None = None


def compute_factor_frame(factor_id: str, inputs: FormulaInputs) -> FormulaFrame:
    """Compute a registered raw-value frame for ``factor_id``."""

    spec = FACTOR_FORMULA_REGISTRY[factor_id]
    return spec.compute(inputs)


def orient_factor_values(factor_id: str, raw_values: pd.DataFrame) -> pd.DataFrame:
    """Convert raw formula values into higher-is-better oriented scores."""

    if factor_id in NEGATIVE_ORIENTATION_FACTORS:
        return -raw_values
    return raw_values


def required_lookback_days(factor_id: str) -> int:
    """Return the minimum daily history needed by the v2 formula."""

    return REQUIRED_LOOKBACK_DAYS[factor_id]


def required_inputs_for_factor(factor_id: str) -> list[str]:
    return list(FACTOR_FORMULA_REGISTRY[factor_id].required_inputs)


def _turnover_base(inputs: FormulaInputs) -> tuple[pd.DataFrame, bool, str]:
    if inputs.shares_float is not None and inputs.shares_float.notna().any().any():
        return inputs.volume.div(inputs.shares_float.where(inputs.shares_float > 0.0)), False, ""
    if inputs.shares_outstanding is not None and inputs.shares_outstanding.notna().any().any():
        return inputs.volume.div(inputs.shares_outstanding.where(inputs.shares_outstanding > 0.0)), True, "missing_shares_float_used_shares_outstanding"
    return inputs.volume, True, "fallback_to_volume_based_proxy"


def _with_turnover_fallback(values: pd.DataFrame, fallback_used: bool, fallback_reason: str) -> FormulaFrame:
    return as_formula_frame(values, fallback_used=fallback_used, fallback_reason=fallback_reason)


def _momentum_spec(factor_id: str, months: int) -> FormulaSpec:
    days = months * TRADING_DAYS_PER_MONTH
    return _spec(
        factor_id=factor_id,
        family="price_momentum",
        summary=f"{months}m log price momentum using adjusted close and one-month signal skip.",
        raw=f"log(P_t-skip / P_t-skip-{days})",
        oriented="raw log momentum; higher means stronger prior price trend",
        inputs=["adjusted_close"],
        fallback="no fallback; insufficient price history is explicit_abstain",
        compute=lambda inputs, d=days: as_formula_frame(compute_log_momentum(inputs.close, d, TRADING_DAYS_PER_MONTH)),
    )


def _reversal_spec(factor_id: str, recent_days: int, prior_days: int) -> FormulaSpec:
    return _spec(
        factor_id=factor_id,
        family="overshoot_reversal",
        summary="Vol-adjusted overshoot reversal gated by prior-trend extension.",
        raw=(
            "-recent_return / realized_sigma * clipped prior-trend extension gate, "
            "where the prior trend is robust cross-sectionally z-scored"
        ),
        oriented="raw reversal score; higher means stronger overshoot-reversal candidate",
        inputs=["adjusted_close"],
        fallback="no fallback; missing recent, prior, or volatility window is explicit_abstain",
        compute=lambda inputs, r=recent_days, p=prior_days: as_formula_frame(
            compute_vol_adjusted_overshoot_reversal(inputs.close, r, p, TRADING_DAYS_PER_MONTH)
        ),
    )


def _volatility_spec(factor_id: str, months: int) -> FormulaSpec:
    days = months * TRADING_DAYS_PER_MONTH
    return _spec(
        factor_id=factor_id,
        family="risk_volatility",
        summary=f"{months}m realized log-return volatility.",
        raw=f"std(daily_log_returns, {days} trading days) * sqrt({days})",
        oriented="negative raw volatility; higher oriented score means lower realized volatility",
        inputs=["adjusted_close"],
        fallback="no fallback; insufficient price history is explicit_abstain",
        compute=lambda inputs, d=days: as_formula_frame(compute_realized_volatility(inputs.close, d, TRADING_DAYS_PER_MONTH)),
    )


def _volume_momentum_spec(factor_id: str, months: int) -> FormulaSpec:
    days = months * TRADING_DAYS_PER_MONTH
    return _spec(
        factor_id=factor_id,
        family="liquidity_volume",
        summary=f"{months}m volume momentum using daily share volume.",
        raw=f"log(V_t-skip / V_t-skip-{days})",
        oriented="raw volume momentum; higher means stronger volume expansion",
        inputs=["volume"],
        fallback="no fallback; insufficient volume history is explicit_abstain",
        compute=lambda inputs, d=days: as_formula_frame(compute_volume_momentum(inputs.volume, d, TRADING_DAYS_PER_MONTH)),
    )


def _price_to_high_spec(factor_id: str, months: int) -> FormulaSpec:
    days = months * TRADING_DAYS_PER_MONTH
    return _spec(
        factor_id=factor_id,
        family="price_position",
        summary=f"Current adjusted close distance from the {months}m rolling high.",
        raw=f"log(P_t-skip / rolling_high_{days})",
        oriented="raw log distance; values closer to zero are nearer the window high",
        inputs=["adjusted_close"],
        fallback="no fallback; insufficient price history is explicit_abstain",
        compute=lambda inputs, d=days: as_formula_frame(compute_price_to_high_distance(inputs.close, d, TRADING_DAYS_PER_MONTH)),
    )


def _drawdown_spec(factor_id: str, months: int) -> FormulaSpec:
    days = months * TRADING_DAYS_PER_MONTH
    return _spec(
        factor_id=factor_id,
        family="path_fragility",
        summary=f"{months}m window maximum peak-to-trough drawdown severity.",
        raw=f"max(1 - P_d / running_peak_d) inside the trailing {days}-day window",
        oriented="negative raw drawdown severity; higher oriented score means less path damage",
        inputs=["adjusted_close"],
        fallback="no fallback; insufficient price history is explicit_abstain",
        compute=lambda inputs, d=days: as_formula_frame(
            compute_window_max_drawdown_severity(inputs.close, d, TRADING_DAYS_PER_MONTH)
        ),
    )


def _trend_spec(factor_id: str, months: int) -> FormulaSpec:
    days = months * TRADING_DAYS_PER_MONTH
    return _spec(
        factor_id=factor_id,
        family="trend_quality",
        summary=f"{months}m log-price OLS trend quality.",
        raw=f"slope_tstat * R_squared from OLS(log_price, time_index) over {days} trading days",
        oriented="raw signed trend-quality score; higher means smoother positive trend",
        inputs=["adjusted_close"],
        fallback="no fallback; insufficient price history is explicit_abstain",
        compute=lambda inputs, d=days: as_formula_frame(compute_log_price_trend_quality(inputs.close, d, TRADING_DAYS_PER_MONTH)),
    )


def _ema_gap_spec(factor_id: str, months: int) -> FormulaSpec:
    days = months * TRADING_DAYS_PER_MONTH
    return _spec(
        factor_id=factor_id,
        family="trend_following",
        summary=f"Adjusted close gap versus a {months}m exponential moving average.",
        raw=f"log(P_t-skip / EMA_{days})",
        oriented="raw EMA gap; higher means price is further above smoothed trend",
        inputs=["adjusted_close"],
        fallback="no fallback; insufficient price history is explicit_abstain",
        compute=lambda inputs, d=days: as_formula_frame(compute_ema_gap(inputs.close, d, TRADING_DAYS_PER_MONTH)),
    )


def _range_spec(factor_id: str, months: int) -> FormulaSpec:
    days = months * TRADING_DAYS_PER_MONTH
    return _spec(
        factor_id=factor_id,
        family="risk_volatility",
        summary=f"{months}m rolling high-low range ratio.",
        raw=f"(rolling_high_{days} - rolling_low_{days}) / P_t-skip",
        oriented="negative raw range ratio; higher oriented score means lower realized price range",
        inputs=["adjusted_close"],
        fallback="no fallback; insufficient price history is explicit_abstain",
        compute=lambda inputs, d=days: as_formula_frame(compute_range_ratio(inputs.close, d, TRADING_DAYS_PER_MONTH)),
    )


def _spec(
    factor_id: str,
    family: str,
    summary: str,
    raw: str,
    oriented: str,
    inputs: list[str],
    fallback: str,
    compute: Callable[[FormulaInputs], FormulaFrame],
) -> FormulaSpec:
    return FormulaSpec(
        factor_id=factor_id,
        mechanism_family=family,
        formula_summary=summary,
        raw_value_definition=raw,
        oriented_score_definition=oriented,
        required_inputs=inputs,
        fallback_policy=fallback,
        compute=compute,
    )


FACTOR_FORMULA_REGISTRY: dict[str, FormulaSpec] = {
    "momentum_1m": _momentum_spec("momentum_1m", 1),
    "momentum_2m": _momentum_spec("momentum_2m", 2),
    "momentum_3m": _momentum_spec("momentum_3m", 3),
    "momentum_6m": _momentum_spec("momentum_6m", 6),
    "momentum_9m": _momentum_spec("momentum_9m", 9),
    "momentum_12m": _momentum_spec("momentum_12m", 12),
    "reversal_1m": _reversal_spec("reversal_1m", 21, 105),
    "reversal_2m": _reversal_spec("reversal_2m", 42, 210),
    "volatility_1m": _volatility_spec("volatility_1m", 1),
    "volatility_2m": _volatility_spec("volatility_2m", 2),
    "volatility_3m": _volatility_spec("volatility_3m", 3),
    "volatility_6m": _volatility_spec("volatility_6m", 6),
    "liquidity_dollar_volume_1m": _spec(
        factor_id="liquidity_dollar_volume_1m",
        family="liquidity_shock",
        summary="Short-term dollar-volume shock versus a trailing 6m baseline that excludes the recent 1m window.",
        raw="log(ADV_1m / ADV_6m_baseline_excluding_recent_1m)",
        oriented="raw liquidity shock; higher means recent dollar volume is unusually elevated",
        inputs=["adjusted_close", "volume"],
        fallback="no fallback; missing price or volume windows are explicit_abstain",
        compute=lambda inputs: as_formula_frame(
            compute_liquidity_shock(inputs.close, inputs.volume, 21, 126, TRADING_DAYS_PER_MONTH)
        ),
    ),
    "liquidity_dollar_volume_3m": _spec(
        factor_id="liquidity_dollar_volume_3m",
        family="capacity_level",
        summary="Persistent 3m dollar-volume capacity level.",
        raw="log(mean(adjusted_close * volume over trailing 63 trading days))",
        oriented="raw persistent capacity proxy; higher means larger dollar-volume capacity",
        inputs=["adjusted_close", "volume"],
        fallback="no fallback; missing price or volume windows are explicit_abstain",
        compute=lambda inputs: as_formula_frame(
            compute_persistent_dollar_volume_capacity(inputs.close, inputs.volume, 63, TRADING_DAYS_PER_MONTH)
        ),
    ),
    "turnover_1m": _spec(
        factor_id="turnover_1m",
        family="turnover_shock",
        summary="Abnormal 1m turnover z-score versus a trailing 6m baseline excluding the recent 1m window.",
        raw="(TO_1m - TO_6m_baseline_excluding_recent_1m) / TO_6m_std",
        oriented="raw abnormal turnover score; higher means unusually elevated short-term turnover",
        inputs=["volume", "shares_float_or_shares_outstanding"],
        fallback="shares_float -> shares_outstanding -> volume-based proxy; fallback is audited as degraded evidence",
        compute=lambda inputs: _compute_turnover_1m(inputs),
    ),
    "turnover_3m": _spec(
        factor_id="turnover_3m",
        family="turnover_trend",
        summary="3m turnover trend persistence using OLS trend quality on log1p(turnover).",
        raw="slope_tstat * R_squared from OLS(log1p(turnover), time_index) over 63 trading days",
        oriented="raw signed turnover-trend score; higher means persistently improving trading activity",
        inputs=["volume", "shares_float_or_shares_outstanding"],
        fallback="shares_float -> shares_outstanding -> volume-based proxy; fallback is audited as degraded evidence",
        compute=lambda inputs: _compute_turnover_3m(inputs),
    ),
    "volume_momentum_1m": _volume_momentum_spec("volume_momentum_1m", 1),
    "volume_momentum_3m": _volume_momentum_spec("volume_momentum_3m", 3),
    "price_to_high_3m": _price_to_high_spec("price_to_high_3m", 3),
    "price_to_high_12m": _price_to_high_spec("price_to_high_12m", 12),
    "drawdown_3m": _drawdown_spec("drawdown_3m", 3),
    "drawdown_12m": _drawdown_spec("drawdown_12m", 12),
    "trend_slope_3m": _trend_spec("trend_slope_3m", 3),
    "trend_slope_6m": _trend_spec("trend_slope_6m", 6),
    "ema_gap_1m": _ema_gap_spec("ema_gap_1m", 1),
    "ema_gap_3m": _ema_gap_spec("ema_gap_3m", 3),
    "range_1m": _range_spec("range_1m", 1),
    "range_3m": _range_spec("range_3m", 3),
    "residual_momentum_6m": _spec(
        factor_id="residual_momentum_6m",
        family="sector_neutral_residual_momentum",
        summary="6m momentum residualized by same-date same-sector median momentum.",
        raw="log(P_t-skip / P_t-skip-126) - median_same_sector_6m_log_momentum",
        oriented="raw sector-neutral residual momentum; higher means stronger within-group momentum",
        inputs=["adjusted_close", "pit_sector_or_industry"],
        fallback="PIT sector -> PIT industry -> cross-section median; fallback is audited as degraded evidence",
        compute=lambda inputs: compute_sector_neutral_residual_momentum(
            inputs.close,
            inputs.classifications,
            lookback_days=126,
            skip_days=TRADING_DAYS_PER_MONTH,
        ),
    ),
}


REQUIRED_LOOKBACK_DAYS: dict[str, int] = {
    "momentum_1m": 21,
    "momentum_2m": 42,
    "momentum_3m": 63,
    "momentum_6m": 126,
    "momentum_9m": 189,
    "momentum_12m": 252,
    "reversal_1m": 126,
    "reversal_2m": 252,
    "volatility_1m": 21,
    "volatility_2m": 42,
    "volatility_3m": 63,
    "volatility_6m": 126,
    "liquidity_dollar_volume_1m": 147,
    "liquidity_dollar_volume_3m": 63,
    "turnover_1m": 147,
    "turnover_3m": 63,
    "volume_momentum_1m": 21,
    "volume_momentum_3m": 63,
    "price_to_high_3m": 63,
    "price_to_high_12m": 252,
    "drawdown_3m": 63,
    "drawdown_12m": 252,
    "trend_slope_3m": 63,
    "trend_slope_6m": 126,
    "ema_gap_1m": 21,
    "ema_gap_3m": 63,
    "range_1m": 21,
    "range_3m": 63,
    "residual_momentum_6m": 126,
}


def _compute_turnover_1m(inputs: FormulaInputs) -> FormulaFrame:
    turnover, fallback_used, fallback_reason = _turnover_base(inputs)
    values = compute_abnormal_turnover_zscore(turnover, 21, 126, TRADING_DAYS_PER_MONTH)
    return _with_turnover_fallback(values, fallback_used=fallback_used, fallback_reason=fallback_reason)


def _compute_turnover_3m(inputs: FormulaInputs) -> FormulaFrame:
    turnover, fallback_used, fallback_reason = _turnover_base(inputs)
    values = compute_turnover_trend_persistence(turnover, 63, TRADING_DAYS_PER_MONTH)
    return _with_turnover_fallback(values, fallback_used=fallback_used, fallback_reason=fallback_reason)


__all__ = [
    "FACTOR_FORMULA_REGISTRY",
    "FORMULA_VERSION",
    "FormulaInputs",
    "compute_factor_frame",
    "orient_factor_values",
    "required_inputs_for_factor",
    "required_lookback_days",
]
