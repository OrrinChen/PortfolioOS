from __future__ import annotations

import numpy as np
import pandas as pd

from factor_discovery_sandbox.factor_weighting_estimators import (
    estimate_equal_weight_all,
    estimate_family_equal_weight,
    estimate_ridge_weighting,
    estimate_shrunk_rolling_icir,
)
from factor_discovery_sandbox.teaching_baseline import FACTOR_NAMES


def test_equal_weight_all_uses_all_29_factors() -> None:
    weights = estimate_equal_weight_all(FACTOR_NAMES)

    assert weights["factor_id"].nunique() == 29
    assert weights["weight"].ne(0.0).all()
    assert np.isclose(weights["weight"].sum(), 1.0)
    assert weights["not_alpha_evidence"].eq(True).all()
    assert weights["direct_q2_entry_allowed"].eq(False).all()


def test_family_equal_weight_does_not_overweight_large_families() -> None:
    metadata = pd.DataFrame(
        {
            "factor_id": ["mom_a", "mom_b", "mom_c", "resid_a"],
            "mechanism_family": ["momentum", "momentum", "momentum", "residual"],
        }
    )

    weights = estimate_family_equal_weight(metadata)
    family_weight = weights.groupby("mechanism_family")["weight"].sum()

    assert np.isclose(family_weight["momentum"], family_weight["residual"])
    assert np.isclose(weights.loc[weights["factor_id"] == "mom_a", "weight"].iloc[0], 1.0 / 6.0)
    assert np.isclose(weights.loc[weights["factor_id"] == "resid_a", "weight"].iloc[0], 1.0 / 2.0)


def test_shrunk_icir_reduces_weight_norm_vs_current_icir() -> None:
    current = pd.DataFrame(
        {
            "factor_id": ["a", "b", "c"],
            "rolling_icir": [2.0, -1.0, 0.5],
            "history_observation_count": [3, 3, 3],
        }
    )

    signed = estimate_shrunk_rolling_icir(current, shrink_lambda=9.0, signed=True)
    nonnegative = estimate_shrunk_rolling_icir(current, shrink_lambda=9.0, signed=False)

    current_norm = float(np.linalg.norm(current["rolling_icir"].to_numpy()))
    signed_norm = float(np.linalg.norm(signed["raw_shrunk_weight"].to_numpy()))
    assert signed_norm < current_norm
    assert (nonnegative["weight"] >= 0.0).all()
    assert np.isclose(nonnegative["weight"].sum(), 1.0)


def test_ridge_weighting_uses_only_past_dates() -> None:
    factor_panel, targets = _ridge_fixture()
    rebalance_date = pd.Timestamp("2020-06-30")

    baseline = estimate_ridge_weighting(
        factor_panel=factor_panel,
        targets=targets,
        rebalance_date=rebalance_date,
        horizon_months=1,
        factor_ids=["factor_a", "factor_b"],
        train_window_months=4,
        ridge_alpha=1.0,
    )
    mutated_targets = targets.copy()
    mutated_targets.loc[mutated_targets["rebalance_date"] >= rebalance_date, "forward_excess_return"] = 999.0
    mutated = estimate_ridge_weighting(
        factor_panel=factor_panel,
        targets=mutated_targets,
        rebalance_date=rebalance_date,
        horizon_months=1,
        factor_ids=["factor_a", "factor_b"],
        train_window_months=4,
        ridge_alpha=1.0,
    )

    assert baseline["uses_full_sample_icir"].eq(False).all()
    assert baseline["future_normalization_used"].eq(False).all()
    assert baseline["post_period_factor_selection_used"].eq(False).all()
    assert (pd.to_datetime(baseline["estimation_window_end"]) < rebalance_date).all()
    assert (pd.to_datetime(baseline["return_visibility_cutoff"]) < rebalance_date).all()
    assert np.allclose(baseline["weight"], mutated["weight"])


def test_no_full_sample_icir_or_future_normalization() -> None:
    factor_panel, targets = _ridge_fixture()
    weights = estimate_ridge_weighting(
        factor_panel=factor_panel,
        targets=targets,
        rebalance_date=pd.Timestamp("2020-06-30"),
        horizon_months=1,
        factor_ids=["factor_a", "factor_b"],
        train_window_months=4,
        ridge_alpha=1.0,
    )

    assert weights["uses_full_sample_icir"].eq(False).all()
    assert weights["future_universe_used"].eq(False).all()
    assert weights["future_normalization_used"].eq(False).all()
    assert weights["post_period_factor_selection_used"].eq(False).all()


def _ridge_fixture() -> tuple[pd.DataFrame, pd.DataFrame]:
    dates = pd.to_datetime(
        ["2020-01-31", "2020-02-28", "2020-03-31", "2020-04-30", "2020-05-29", "2020-06-30", "2020-07-31"]
    )
    assets = ["A", "B", "C", "D"]
    factor_rows = []
    target_rows = []
    for date_index, date in enumerate(dates):
        for asset_index, asset in enumerate(assets):
            value_a = float(asset_index - 1.5)
            value_b = float(1.5 - asset_index)
            target = value_a * 0.02 + date_index * 0.0001
            for factor_id, value in [("factor_a", value_a), ("factor_b", value_b)]:
                factor_rows.append(
                    {
                        "rebalance_date": date,
                        "asset_id": asset,
                        "factor_id": factor_id,
                        "normalized_value": value,
                        "coverage_status": "active_view",
                        "mechanism_family": "family_a" if factor_id == "factor_a" else "family_b",
                        "known_correlation_family": "family_a" if factor_id == "factor_a" else "family_b",
                    }
                )
            target_rows.append(
                {
                    "rebalance_date": date,
                    "asset_id": asset,
                    "horizon_months": 1,
                    "period": "validation" if date_index < 4 else "test",
                    "forward_excess_return": target,
                    "forward_return_available": True,
                    "target_return_visible_timestamp": (date + pd.Timedelta(days=20)).isoformat(),
                }
            )
    return pd.DataFrame(factor_rows), pd.DataFrame(target_rows)
