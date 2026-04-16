"""Walk-forward alpha bridge from accepted research signal to backtest expected returns."""

from __future__ import annotations

from dataclasses import dataclass
from statistics import NormalDist
from pathlib import Path

import numpy as np
import pandas as pd

from portfolio_os.alpha.acceptance import AlphaRecipeConfig
from portfolio_os.alpha.research import (
    build_alpha_ic_frame,
    build_alpha_research_frame,
    build_alpha_score_frame,
    load_alpha_returns_panel,
)
from portfolio_os.domain.errors import InputValidationError


_PRIMARY_SIGNAL_NAME = "blended_alpha"
_ANNUALIZATION_FACTOR = 252.0
_NORMAL_DIST = NormalDist()
_ACCEPTED_RECIPE = AlphaRecipeConfig(
    recipe_name="alt_momentum_4_1",
    reversal_lookback_days=21,
    momentum_lookback_days=84,
    momentum_skip_days=21,
    forward_horizon_days=5,
    reversal_weight=0.0,
    momentum_weight=1.0,
    quantiles=5,
    min_assets_per_date=20,
)


@dataclass
class AlphaRebalanceSnapshot:
    """Walk-forward alpha snapshot for one rebalance date."""

    rebalance_date: pd.Timestamp
    signal_frame: pd.DataFrame
    ic_frame: pd.DataFrame
    current_cross_section: pd.DataFrame
    alpha_only_target_weights: dict[str, float]


def _quantile_buckets(scores: pd.Series, *, quantiles: int) -> pd.Series:
    """Bucket one cross-section into deterministic quantiles."""

    return np.ceil(scores.rank(method="first", pct=True) * quantiles).clip(1, quantiles)


def build_alpha_only_target_weights(
    frame: pd.DataFrame,
    *,
    quantiles: int,
    score_column: str = "alpha_score",
) -> dict[str, float]:
    """Convert one alpha cross-section into top-quantile equal-weight targets."""

    work = frame.loc[:, ["ticker", score_column]].copy()
    work = work.dropna(subset=[score_column]).sort_values(["ticker"]).reset_index(drop=True)
    if work.empty:
        return {}
    quantile_bucket = _quantile_buckets(work[score_column], quantiles=quantiles)
    winners = work.loc[quantile_bucket == quantiles, "ticker"].astype(str).tolist()
    if not winners:
        return {str(ticker): 0.0 for ticker in work["ticker"].astype(str).tolist()}
    weight = 1.0 / float(len(winners))
    return {
        str(ticker): (weight if str(ticker) in winners else 0.0)
        for ticker in work["ticker"].astype(str).tolist()
    }


def _cross_sectional_rank_pct(scores: pd.Series) -> pd.Series:
    """Return clipped percentile ranks for inverse-normal conversion."""

    return scores.rank(method="first", pct=True).clip(lower=1e-6, upper=1.0 - 1e-6)


def _inverse_normal_series(rank_pct: pd.Series) -> pd.Series:
    """Map percentile ranks into inverse-normal scores."""

    return rank_pct.map(lambda value: float(_NORMAL_DIST.inv_cdf(float(value))))


def _rank_ic_t_stat(history_ic: pd.Series) -> float:
    """Estimate t-stat for the trailing rank-IC mean."""

    clean = pd.to_numeric(history_ic, errors="coerce").dropna().astype(float)
    if len(clean) < 2:
        return 0.0
    std = float(clean.std(ddof=1))
    if std <= 0.0:
        return 0.0
    return float(clean.mean() / (std / np.sqrt(float(len(clean)))))


def _deannualize_return(annualized_return: float, horizon_days: int) -> float:
    """Convert an annualized return estimate into one rebalance-period return."""

    if horizon_days <= 0:
        raise InputValidationError("decision_horizon_days must be positive.")
    return float((1.0 + float(annualized_return)) ** (float(horizon_days) / _ANNUALIZATION_FACTOR) - 1.0)


def build_alpha_snapshot_for_rebalance(
    *,
    returns_file: str | Path,
    rebalance_date: str | pd.Timestamp,
    quantiles: int = 5,
    min_evaluation_dates: int = 20,
    zscore_winsor_limit: float = 3.0,
    t_stat_full_confidence: float = 3.0,
    max_abs_expected_return: float = 0.30,
    decision_horizon_days: int | None = None,
) -> AlphaRebalanceSnapshot:
    """Build one walk-forward alpha snapshot aligned to a rebalance date."""

    rebalance_ts = pd.Timestamp(rebalance_date).normalize()
    effective_horizon_days = int(decision_horizon_days or _ACCEPTED_RECIPE.forward_horizon_days)
    if effective_horizon_days <= 0:
        raise InputValidationError("decision_horizon_days must be positive.")
    returns_panel = load_alpha_returns_panel(returns_file)
    returns_panel = returns_panel.loc[returns_panel.index <= rebalance_ts].copy()
    if returns_panel.empty:
        raise InputValidationError("No returns history is available on or before the rebalance date.")

    score_frame = build_alpha_score_frame(
        returns_panel,
        reversal_lookback_days=_ACCEPTED_RECIPE.reversal_lookback_days,
        momentum_lookback_days=_ACCEPTED_RECIPE.momentum_lookback_days,
        momentum_skip_days=_ACCEPTED_RECIPE.momentum_skip_days,
        reversal_weight=_ACCEPTED_RECIPE.reversal_weight,
        momentum_weight=_ACCEPTED_RECIPE.momentum_weight,
    )
    signal_frame = build_alpha_research_frame(
        returns_panel,
        reversal_lookback_days=_ACCEPTED_RECIPE.reversal_lookback_days,
        momentum_lookback_days=_ACCEPTED_RECIPE.momentum_lookback_days,
        momentum_skip_days=_ACCEPTED_RECIPE.momentum_skip_days,
        forward_horizon_days=_ACCEPTED_RECIPE.forward_horizon_days,
        reversal_weight=_ACCEPTED_RECIPE.reversal_weight,
        momentum_weight=_ACCEPTED_RECIPE.momentum_weight,
    )
    ic_frame = build_alpha_ic_frame(
        signal_frame,
        min_assets_per_date=_ACCEPTED_RECIPE.min_assets_per_date,
        quantiles=quantiles,
    )
    ic_frame = (
        ic_frame.loc[ic_frame["signal_name"] == _PRIMARY_SIGNAL_NAME]
        .copy()
        .sort_values("date")
        .reset_index(drop=True)
    )

    current_cross_section = (
        score_frame.loc[score_frame["date"] == rebalance_ts.strftime("%Y-%m-%d")]
        .copy()
        .sort_values("ticker")
        .reset_index(drop=True)
    )
    if current_cross_section.empty:
        raise InputValidationError("No accepted-recipe alpha scores are available on the rebalance date.")

    current_cross_section["alpha_rank_pct"] = _cross_sectional_rank_pct(current_cross_section["alpha_score"])
    current_cross_section["alpha_zscore"] = _inverse_normal_series(current_cross_section["alpha_rank_pct"]).clip(
        lower=-float(zscore_winsor_limit),
        upper=float(zscore_winsor_limit),
    )
    current_cross_section["alpha_zscore"] = (
        current_cross_section["alpha_zscore"] - float(current_cross_section["alpha_zscore"].mean())
    )
    current_cross_section["quantile"] = _quantile_buckets(
        current_cross_section["alpha_score"],
        quantiles=quantiles,
    ).astype(int)

    history = ic_frame.loc[pd.to_datetime(ic_frame["date"]) < rebalance_ts].copy()
    if len(history) < int(min_evaluation_dates):
        confidence = 0.0
        annualized_spread = 0.0
    else:
        t_stat = _rank_ic_t_stat(history["rank_ic"])
        confidence = float(np.clip(t_stat / float(t_stat_full_confidence), 0.0, 1.0))
        annualized_spread = float(
            max(float(history["top_bottom_spread"].mean()), 0.0)
            * (_ANNUALIZATION_FACTOR / float(_ACCEPTED_RECIPE.forward_horizon_days))
        )
    period_spread = _deannualize_return(annualized_spread, effective_horizon_days)

    top_mask = current_cross_section["quantile"] == int(quantiles)
    bottom_mask = current_cross_section["quantile"] == 1
    z_gap = float(
        current_cross_section.loc[top_mask, "alpha_zscore"].mean()
        - current_cross_section.loc[bottom_mask, "alpha_zscore"].mean()
    )
    if not np.isfinite(z_gap) or z_gap <= 1e-6:
        z_gap = 1e-6

    current_cross_section["expected_return"] = (
        confidence
        * period_spread
        * current_cross_section["alpha_zscore"]
        / z_gap
    ).clip(lower=-float(max_abs_expected_return), upper=float(max_abs_expected_return))
    current_cross_section["signal_strength_confidence"] = float(confidence)
    current_cross_section["annualized_top_bottom_spread"] = float(annualized_spread)
    current_cross_section["period_top_bottom_spread"] = float(period_spread)
    current_cross_section["decision_horizon_days"] = int(effective_horizon_days)

    return AlphaRebalanceSnapshot(
        rebalance_date=rebalance_ts,
        signal_frame=signal_frame,
        ic_frame=history.reset_index(drop=True),
        current_cross_section=current_cross_section,
        alpha_only_target_weights=build_alpha_only_target_weights(current_cross_section, quantiles=quantiles),
    )
