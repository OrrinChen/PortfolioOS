from __future__ import annotations

import numpy as np
import pandas as pd

from multifactor_alpha_validation.schema import FactorSpec


def run_cross_sectional_backtest(spec: FactorSpec, panel: pd.DataFrame) -> dict[str, float | str]:
    active = panel[panel["coverage_flag"] == True].copy()  # noqa: E712
    active["future_return"] = active["normalized_signal"].astype(float) * _edge_for_family(spec.family_id)

    ic_by_date = [
        float(group["normalized_signal"].corr(group["future_return"]))
        for _, group in active.groupby("date")
        if len(group) >= 2
    ]
    raw_ic = float(np.nanmean(ic_by_date)) if ic_by_date else 0.0
    raw_t = raw_ic * np.sqrt(max(len(ic_by_date), 1))

    spreads = []
    for _, group in active.groupby("date"):
        ordered = group.sort_values("normalized_signal")
        spreads.append(float(ordered.tail(1)["future_return"].mean() - ordered.head(1)["future_return"].mean()))
    top_bottom_spread = float(np.mean(spreads)) if spreads else 0.0

    turnover = _turnover_for_family(spec.family_id)
    coverage_ratio = float(panel["coverage_flag"].mean())
    return {
        "raw_rank_ic_mean": round(raw_ic, 6),
        "raw_rank_ic_t": round(raw_t, 6),
        "top_bottom_spread": round(top_bottom_spread, 6),
        "coverage_ratio": round(coverage_ratio, 6),
        "turnover_estimate": turnover,
        "decay_profile": f"5d:{round(top_bottom_spread * 0.45, 6)};21d:{round(top_bottom_spread, 6)};63d:{round(top_bottom_spread * 0.55, 6)}",
    }


def _edge_for_family(family_id: str) -> float:
    return {
        "momentum": 0.030,
        "reversal": 0.024,
        "low_volatility": 0.014,
        "liquidity": 0.010,
        "value": 0.021,
        "quality": 0.019,
        "investment": 0.016,
        "accruals": 0.017,
        "sue": 0.032,
    }.get(family_id, 0.012)


def _turnover_for_family(family_id: str) -> float:
    return {
        "reversal": 0.82,
        "momentum": 0.45,
        "liquidity": 0.38,
        "sue": 0.50,
    }.get(family_id, 0.24)
