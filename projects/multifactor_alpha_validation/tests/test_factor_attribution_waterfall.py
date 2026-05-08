from __future__ import annotations

import json
from pathlib import Path

import numpy as np
import pandas as pd

from multifactor_alpha_validation.cross_sectional_risk_model import run_cross_sectional_risk_model
from multifactor_alpha_validation.factor_attribution_waterfall import run_factor_attribution_waterfall
from multifactor_alpha_validation.risk_exposure_store import run_pit_exposure_store

from test_risk_exposure_store import _write_fundamentals_bundle, _write_research_bundle


def test_factor_attribution_waterfall_decomposes_factor_sleeves(tmp_path: Path) -> None:
    research_manifest = _write_research_bundle(tmp_path / "research")
    fundamentals_manifest = _write_fundamentals_bundle(tmp_path / "fundamentals")
    exposures = run_pit_exposure_store(research_manifest, fundamentals_manifest, tmp_path / "risk_model")
    risk_model = run_cross_sectional_risk_model(
        research_manifest_path=research_manifest,
        exposure_panel_path=Path(exposures.exposure_panel_path),
        output_dir=tmp_path / "risk_model",
    )

    result = run_factor_attribution_waterfall(
        research_manifest_path=research_manifest,
        residual_returns_path=Path(risk_model.residual_returns_path),
        output_dir=tmp_path / "waterfall",
    )

    waterfall = pd.read_csv(result.waterfall_path)
    by_period = pd.read_csv(result.waterfall_by_period_path)
    diagnostics = json.loads(Path(result.diagnostics_path).read_text(encoding="utf-8"))
    report = Path(result.report_path).read_text(encoding="utf-8").lower()

    assert result.production_approval is False
    assert result.direct_q2_entry is False
    assert result.not_alpha_evidence is True
    assert set(waterfall["factor_id"]) == {"momentum_12_1", "reversal_5_1", "low_vol_60d"}
    assert {
        "gross_spread_mean",
        "qqq_relative_spread_mean",
        "market_beta_contribution_mean",
        "beta_adjusted_spread_mean",
        "industry_contribution_mean",
        "industry_adjusted_spread_mean",
        "style_proxy_contribution_mean",
        "style_proxy_adjusted_spread_mean",
        "full_residual_spread_mean",
        "waterfall_status",
        "not_style_neutral_alpha",
        "not_alpha_evidence",
    }.issubset(waterfall.columns)
    assert set(waterfall["not_style_neutral_alpha"].astype(bool)) == {True}
    assert set(waterfall["not_alpha_evidence"].astype(bool)) == {True}

    reconstructed = (
        by_period["intercept_contribution_spread"]
        + by_period["market_beta_contribution_spread"]
        + by_period["industry_contribution_spread"]
        + by_period["style_proxy_contribution_spread"]
        + by_period["full_residual_spread"]
    )
    assert np.allclose(by_period["gross_spread"], reconstructed, atol=1e-8)
    assert by_period["same_close_trading_used"].eq(False).all()
    assert by_period["not_alpha_evidence"].eq(True).all()

    for factor_id in waterfall["factor_id"]:
        factor_json = tmp_path / "waterfall" / f"factor_attribution_waterfall_{factor_id}.json"
        assert factor_json.exists()
        payload = json.loads(factor_json.read_text(encoding="utf-8"))
        assert payload["factor_id"] == factor_id
        assert payload["non_claims"]["production_approval"] is False
        assert payload["non_claims"]["not_alpha_evidence"] is True

    assert diagnostics["schema_version"] == "factor_attribution_waterfall_diagnostics.v1"
    assert diagnostics["non_claims"]["production_approval"] is False
    assert diagnostics["non_claims"]["residual_is_style_neutral"] is False
    assert "style_neutral_alpha" not in Path(result.diagnostics_path).read_text(encoding="utf-8")
    assert "not style-neutral alpha" in report


def test_factor_attribution_waterfall_flags_benchmark_beta_style_proxy_conflict(tmp_path: Path) -> None:
    research_manifest = _write_research_bundle(tmp_path / "research")
    prices = pd.read_csv(tmp_path / "research" / "adjusted_price_volume_panel.csv", dtype={"asset_id": str})
    dates = pd.to_datetime(prices["date"]).sort_values().drop_duplicates().tolist()
    signal_date = pd.Timestamp("2020-12-31")
    period_start = min(date for date in dates if date > signal_date)
    period_end = pd.Timestamp("2021-01-29")
    signal_order = _momentum_order(prices, signal_date)
    bottom_asset = signal_order[0]
    top_asset = signal_order[-1]

    benchmark = pd.read_csv(tmp_path / "research" / "qqq_benchmark_panel.csv")
    start_mask = benchmark["date"].eq(period_start.date().isoformat())
    end_mask = benchmark["date"].eq(period_end.date().isoformat())
    start_price = float(benchmark.loc[start_mask, "adjusted_close"].iloc[0])
    benchmark.loc[end_mask, "adjusted_close"] = start_price * 1.20
    benchmark.to_csv(tmp_path / "research" / "qqq_benchmark_panel.csv", index=False)

    rows = []
    for asset_id in sorted(prices["asset_id"].astype(str).unique()):
        if asset_id == top_asset:
            realized = 0.10
            market_beta = 0.20
            residual = 0.03
        elif asset_id == bottom_asset:
            realized = 0.00
            market_beta = 0.00
            residual = 0.00
        else:
            realized = 0.02
            market_beta = 0.02
            residual = 0.00
        rows.append(
            {
                "schema_version": "cross_sectional_risk_model_residual.v1",
                "date": signal_date.date().isoformat(),
                "period_start": period_start.date().isoformat(),
                "period_end": period_end.date().isoformat(),
                "asset_id": asset_id,
                "realized_return": realized,
                "intercept_contribution": 0.0,
                "market_beta_contribution": market_beta,
                "industry_contribution": 0.0,
                "style_contribution": realized - market_beta - residual,
                "fitted_return": realized - residual,
                "residual_return": residual,
                "coverage_flag": True,
                "abstain_reason": "",
                "not_tradeable_prediction": True,
                "not_alpha_evidence": True,
            }
        )
    residual_path = tmp_path / "forced_residual_returns.csv"
    pd.DataFrame(rows).to_csv(residual_path, index=False)

    result = run_factor_attribution_waterfall(research_manifest, residual_path, tmp_path / "waterfall")

    waterfall = pd.read_csv(result.waterfall_path)
    momentum = waterfall[waterfall["factor_id"].eq("momentum_12_1")].iloc[0]
    assert momentum["gross_spread_mean"] > 0
    assert momentum["qqq_relative_spread_mean"] < 0
    assert momentum["beta_adjusted_spread_mean"] < 0
    assert momentum["full_residual_spread_mean"] > 0
    assert momentum["waterfall_status"] == "style_proxy_conflict"
    assert bool(momentum["redundancy_gate_allowed"]) is False

    diagnostics = json.loads(Path(result.diagnostics_path).read_text(encoding="utf-8"))
    assert "momentum_12_1" in diagnostics["style_proxy_conflict_factors"]
    report = Path(result.report_path).read_text(encoding="utf-8").lower()
    assert "benchmark/beta" in report
    assert "style proxy conflict" in report


def _momentum_order(prices: pd.DataFrame, signal_date: pd.Timestamp) -> list[str]:
    rows: list[tuple[str, float]] = []
    prices["date"] = pd.to_datetime(prices["date"])
    for asset_id, history in prices[prices["date"] <= signal_date].groupby("asset_id"):
        history = history.sort_values("date")
        close = pd.to_numeric(history["adjusted_close"], errors="coerce").reset_index(drop=True)
        rows.append((str(asset_id), float(close.iloc[-22] / close.iloc[-253] - 1.0)))
    return [asset_id for asset_id, _ in sorted(rows, key=lambda item: item[1])]
