from __future__ import annotations

import numpy as np
import pandas as pd

from factor_discovery_sandbox.factor_formulas_v2 import (
    compute_abnormal_turnover_zscore,
    compute_liquidity_shock,
    compute_log_momentum,
    compute_log_price_trend_quality,
    compute_persistent_dollar_volume_capacity,
    compute_price_to_high_distance,
    compute_sector_neutral_residual_momentum,
    compute_turnover_trend_persistence,
    compute_vol_adjusted_overshoot_reversal,
    compute_window_max_drawdown_severity,
)


def test_residual_momentum_uses_sector_median_not_qqq_constant() -> None:
    close = pd.DataFrame(
        {
            "A": _growth_path(100.0, 0.0030, 160),
            "B": _growth_path(100.0, 0.0010, 160),
            "C": _growth_path(100.0, 0.0020, 160),
            "D": _growth_path(100.0, -0.0010, 160),
        },
        index=pd.bdate_range("2020-01-01", periods=160),
    )
    classifications = pd.DataFrame(
        {
            "asset_id": ["A", "B", "C", "D"],
            "sector": ["tech", "tech", "health", "health"],
            "industry": ["software", "hardware", "biotech", "services"],
        }
    )

    residual = compute_sector_neutral_residual_momentum(close, classifications, lookback_days=126, skip_days=1)
    momentum = compute_log_momentum(close, lookback_days=126, skip_days=1)

    date = close.index[-1]
    assert residual.values.loc[date].rank().corr(momentum.loc[date].rank(), method="spearman") < 0.999
    assert residual.fallback_reason.loc[date].eq("").all()


def test_trend_slope_rewards_smooth_trend_not_endpoint_return() -> None:
    dates = pd.bdate_range("2020-01-01", periods=80)
    smooth = np.linspace(100.0, 130.0, len(dates))
    choppy = smooth.copy()
    choppy_window = np.linspace(smooth[-64], smooth[-1], 64)
    choppy_window[1:-1] += np.where(np.arange(62) % 2 == 0, 22.0, -22.0)
    choppy[-64:] = choppy_window
    close = pd.DataFrame({"smooth": smooth, "choppy": choppy}, index=dates)

    trend = compute_log_price_trend_quality(close, window_days=63, skip_days=0)
    momentum = compute_log_momentum(close, lookback_days=63, skip_days=0)

    date = dates[-1]
    assert np.isclose(momentum.loc[date, "smooth"], momentum.loc[date, "choppy"], atol=0.02)
    assert trend.loc[date, "smooth"] > trend.loc[date, "choppy"]


def test_drawdown_uses_peak_to_trough_not_current_high_distance() -> None:
    dates = pd.bdate_range("2020-01-01", periods=63)
    asset_a = np.linspace(100.0, 90.0, len(dates))
    asset_b = np.array([100.0, 50.0, *np.linspace(50.0, 90.0, len(dates) - 2)])
    close = pd.DataFrame({"slow": asset_a, "crash_rebound": asset_b}, index=dates)

    price_to_high = compute_price_to_high_distance(close, window_days=63, skip_days=0)
    drawdown = compute_window_max_drawdown_severity(close, window_days=63, skip_days=0)

    date = dates[-1]
    assert np.isclose(price_to_high.loc[date, "slow"], price_to_high.loc[date, "crash_rebound"], atol=0.02)
    assert drawdown.loc[date, "crash_rebound"] > drawdown.loc[date, "slow"]


def test_reversal_uses_volatility_and_prior_trend_condition() -> None:
    dates = pd.bdate_range("2020-01-01", periods=160)
    close = pd.DataFrame(
        {
            "extended_low_vol": _piecewise_path(100.0, [(-0.0010, 126), (-0.0030, 21), (0.0, 13)]),
            "no_prior_low_vol": _piecewise_path(100.0, [(0.0, 126), (-0.0030, 21), (0.0, 13)]),
            "positive_prior": _piecewise_path(100.0, [(0.0010, 126), (-0.0030, 21), (0.0, 13)]),
            "extended_high_vol": _piecewise_path(100.0, [(-0.0010, 126), (-0.0030, 21), (0.0, 13)], noise=0.03),
        },
        index=dates,
    )

    reversal = compute_vol_adjusted_overshoot_reversal(close, recent_days=21, prior_days=105, skip_days=1)
    momentum = -compute_log_momentum(close, lookback_days=21, skip_days=1)
    date = dates[-1]

    assert reversal.loc[date, "extended_low_vol"] > reversal.loc[date, "no_prior_low_vol"]
    assert reversal.loc[date, "extended_low_vol"] > reversal.loc[date, "extended_high_vol"]
    assert reversal.loc[date].rank().corr(momentum.loc[date].rank(), method="spearman") < 0.999


def test_liquidity_shock_is_distinct_from_capacity_level() -> None:
    dates = pd.bdate_range("2020-01-01", periods=180)
    close = pd.DataFrame({"capacity": 100.0, "shock": 100.0}, index=dates)
    volume = pd.DataFrame(
        {
            "capacity": np.full(len(dates), 1_000_000.0),
            "shock": np.r_[np.full(len(dates) - 21, 200_000.0), np.full(21, 900_000.0)],
        },
        index=dates,
    )

    shock = compute_liquidity_shock(close, volume, recent_days=21, baseline_days=126, skip_days=1)
    capacity = compute_persistent_dollar_volume_capacity(close, volume, window_days=63, skip_days=1)
    date = dates[-1]

    assert capacity.loc[date, "capacity"] > capacity.loc[date, "shock"]
    assert shock.loc[date, "shock"] > shock.loc[date, "capacity"]


def test_turnover_shock_is_distinct_from_turnover_trend_persistence() -> None:
    dates = pd.bdate_range("2020-01-01", periods=180)
    baseline = 100_000.0 + 8_000.0 * np.sin(np.arange(len(dates) - 21) / 3.0)
    volume = pd.DataFrame(
        {
            "spike": np.r_[baseline, np.full(21, 900_000.0)],
            "trend": np.linspace(100_000.0, 1_600_000.0, len(dates)),
        },
        index=dates,
    )

    turnover_shock = compute_abnormal_turnover_zscore(volume, recent_days=21, baseline_days=126, skip_days=1)
    turnover_trend = compute_turnover_trend_persistence(volume, window_days=63, skip_days=1)
    date = dates[-1]

    assert turnover_shock.loc[date, "spike"] > turnover_shock.loc[date, "trend"]
    assert turnover_trend.loc[date, "trend"] > turnover_trend.loc[date, "spike"]


def _growth_path(start: float, daily_return: float, periods: int) -> np.ndarray:
    return start * np.cumprod(np.full(periods, 1.0 + daily_return))


def _piecewise_path(start: float, segments: list[tuple[float, int]], noise: float = 0.0) -> np.ndarray:
    values = [start]
    index = 0
    for daily_return, length in segments:
        for _ in range(length):
            adjustment = noise if index % 2 == 0 else -noise
            values.append(values[-1] * (1.0 + daily_return + adjustment))
            index += 1
    return np.asarray(values[1:])
