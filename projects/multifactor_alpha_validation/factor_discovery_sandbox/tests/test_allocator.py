from __future__ import annotations

from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.allocator import run_allocator


def test_allocator_writes_shrinkage_weights_and_zero_weight_attribution(tmp_path: Path) -> None:
    result = run_allocator(tmp_path)

    assert {
        "posterior_factor_mu.csv",
        "factor_covariance_shrunk.csv",
        "allocator_weights.csv",
        "zero_weight_attribution.csv",
    } == {path.name for path in result.artifacts.values()}
    assert result.summary["production_strategy_claimed"] is False
    assert result.summary["sign_flip_sanity_check_passed"] is True
    assert result.summary["scale_response_sanity_check_passed"] is True

    posterior = pd.read_csv(tmp_path / "posterior_factor_mu.csv")
    assert {"factor", "raw_mu", "posterior_mu", "shrinkage_intensity"}.issubset(posterior.columns)
    assert posterior["factor"].nunique() == 29
    assert posterior["shrinkage_intensity"].between(0, 1).all()

    covariance = pd.read_csv(tmp_path / "factor_covariance_shrunk.csv", index_col=0)
    assert covariance.shape == (29, 29)
    assert (covariance.values.diagonal() > 0).all()

    weights = pd.read_csv(tmp_path / "allocator_weights.csv")
    assert {"factor", "allocator_weight", "cluster_id", "production_strategy_claimed"}.issubset(weights.columns)
    assert weights["allocator_weight"].ge(0).all()
    assert round(weights["allocator_weight"].sum(), 10) == 1.0
    assert set(weights["production_strategy_claimed"]) == {False}

    zero = pd.read_csv(tmp_path / "zero_weight_attribution.csv")
    allowed_reasons = {
        "low_posterior_alpha",
        "high_redundancy",
        "cluster_dominated",
        "high_turnover",
        "high_cost_drag",
        "capacity_limited",
        "no_view",
        "insufficient_evidence",
    }
    assert {"factor", "zero_weight_reason"}.issubset(zero.columns)
    assert zero["zero_weight_reason"].notna().all()
    assert set(zero["zero_weight_reason"]).issubset(allowed_reasons)
    assert set(zero["factor"]).issubset(set(weights.loc[weights["allocator_weight"] == 0, "factor"]))
