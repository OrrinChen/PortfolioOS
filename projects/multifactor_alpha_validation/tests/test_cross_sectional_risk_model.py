from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from multifactor_alpha_validation.cross_sectional_risk_model import run_cross_sectional_risk_model
from multifactor_alpha_validation.risk_exposure_store import run_pit_exposure_store

from test_risk_exposure_store import _write_fundamentals_bundle, _write_research_bundle


def test_cross_sectional_risk_model_outputs_component_and_residual_attribution(tmp_path: Path) -> None:
    research_manifest = _write_research_bundle(tmp_path / "research")
    fundamentals_manifest = _write_fundamentals_bundle(tmp_path / "fundamentals")
    exposures = run_pit_exposure_store(research_manifest, fundamentals_manifest, tmp_path / "risk_model")

    result = run_cross_sectional_risk_model(
        research_manifest_path=research_manifest,
        exposure_panel_path=Path(exposures.exposure_panel_path),
        output_dir=tmp_path / "risk_attribution",
    )

    returns_by_period = pd.read_csv(result.returns_by_period_path)
    coefficients = pd.read_csv(result.coefficients_path)
    residuals = pd.read_csv(result.residual_returns_path)
    diagnostics = json.loads(Path(result.fit_diagnostics_path).read_text(encoding="utf-8"))

    assert result.production_approval is False
    assert result.direct_q2_entry is False
    assert result.not_alpha_evidence is True
    assert result.period_count > 0
    assert result.residual_return_count == len(residuals)

    assert {
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
    }.issubset(residuals.columns)
    covered = residuals[residuals["coverage_flag"].astype(bool)]
    assert not covered.empty
    reconstructed = (
        covered["intercept_contribution"]
        + covered["market_beta_contribution"]
        + covered["industry_contribution"]
        + covered["style_contribution"]
        + covered["residual_return"]
    )
    assert np.allclose(covered["realized_return"], reconstructed, atol=1e-8)
    assert set(residuals["not_tradeable_prediction"].astype(bool)) == {True}
    assert set(residuals["not_alpha_evidence"].astype(bool)) == {True}

    assert {"intercept", "market_beta", "industry", "style"} <= set(coefficients["coefficient_type"])
    assert returns_by_period["residual_return_mean"].notna().all()
    assert returns_by_period["attribution_complete"].isin([True, False]).all()

    assert diagnostics["schema_version"] == "cross_sectional_risk_model_diagnostics.v1"
    assert diagnostics["non_claims"]["residual_is_tradeable_prediction"] is False
    assert diagnostics["non_claims"]["production_approval"] is False
    assert diagnostics["model_use"] == "ex_post_attribution_only"
    assert "style_neutral_alpha" not in Path(result.fit_diagnostics_path).read_text(encoding="utf-8")


def test_cross_sectional_risk_model_marks_missing_exposure_as_abstain(tmp_path: Path) -> None:
    research_manifest = _write_research_bundle(tmp_path / "research")
    fundamentals_manifest = _write_fundamentals_bundle(tmp_path / "fundamentals")
    exposure_result = run_pit_exposure_store(research_manifest, fundamentals_manifest, tmp_path / "risk_model")
    exposure_panel_path = Path(exposure_result.exposure_panel_path)
    exposure_panel = pd.read_csv(exposure_panel_path)
    missing_mask = (
        exposure_panel["asset_id"].astype(str).eq("10002")
        & exposure_panel["exposure_name"].eq("trailing_market_beta_252d")
    )
    exposure_panel.loc[missing_mask, "coverage_flag"] = False
    exposure_panel.loc[missing_mask, "exposure_value"] = np.nan
    exposure_panel.loc[missing_mask, "abstain_reason"] = "forced_missing_beta_for_test"
    exposure_panel.to_csv(exposure_panel_path, index=False)

    result = run_cross_sectional_risk_model(
        research_manifest_path=research_manifest,
        exposure_panel_path=exposure_panel_path,
        output_dir=tmp_path / "risk_attribution",
    )

    residuals = pd.read_csv(result.residual_returns_path)
    diagnostics = json.loads(Path(result.fit_diagnostics_path).read_text(encoding="utf-8"))
    missing_asset = residuals[residuals["asset_id"].astype(str).eq("10002")]

    assert not missing_asset.empty
    assert set(missing_asset["coverage_flag"].astype(bool)) == {False}
    assert "missing_required_exposure:trailing_market_beta_252d" in set(missing_asset["abstain_reason"])
    assert missing_asset["residual_return"].isna().all()
    assert diagnostics["missing_exposure_row_count"] >= len(missing_asset)
    assert diagnostics["regression_instability_reported"] is True
