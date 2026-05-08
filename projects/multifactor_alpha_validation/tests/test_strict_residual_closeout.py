from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from multifactor_alpha_validation.strict_residual_closeout import run_strict_residual_closeout


def test_strict_residual_closeout_blocks_style_proxy_conflicts(tmp_path: Path) -> None:
    input_dir = tmp_path / "r12"
    input_dir.mkdir()
    _write_waterfall(
        input_dir,
        [
            {
                "factor_id": "momentum_12_1",
                "period_count": 48,
                "gross_spread_mean": 0.02,
                "qqq_relative_spread_mean": -0.01,
                "beta_adjusted_spread_mean": -0.005,
                "industry_adjusted_spread_mean": 0.004,
                "style_proxy_adjusted_spread_mean": 0.006,
                "full_residual_spread_mean": 0.006,
                "style_proxy_conflict_count": 12,
                "waterfall_status": "style_proxy_conflict",
                "redundancy_gate_allowed": False,
                "not_style_neutral_alpha": True,
                "not_alpha_evidence": True,
            },
            {
                "factor_id": "low_vol_60d",
                "period_count": 48,
                "gross_spread_mean": -0.01,
                "qqq_relative_spread_mean": -0.02,
                "beta_adjusted_spread_mean": -0.012,
                "industry_adjusted_spread_mean": -0.010,
                "style_proxy_adjusted_spread_mean": 0.003,
                "full_residual_spread_mean": 0.003,
                "style_proxy_conflict_count": 9,
                "waterfall_status": "style_proxy_conflict",
                "redundancy_gate_allowed": False,
                "not_style_neutral_alpha": True,
                "not_alpha_evidence": True,
            },
        ],
    )
    _write_by_period(
        input_dir,
        [
            *(_period_rows("momentum_12_1", "style_proxy_conflict", residual=0.01, count=12)),
            *(_period_rows("momentum_12_1", "proxy_residual_positive", residual=0.01, count=30)),
            *(_period_rows("momentum_12_1", "residual_not_positive", residual=-0.01, count=6)),
            *(_period_rows("low_vol_60d", "style_proxy_conflict", residual=0.01, count=9)),
            *(_period_rows("low_vol_60d", "diagnostic_only", residual=-0.01, count=39)),
        ],
    )

    result = run_strict_residual_closeout(input_dir, tmp_path / "r13")

    decisions = pd.read_csv(result.decision_table_path)
    assert result.ready_for_redundancy_count == 0
    assert result.production_approval is False
    assert result.direct_q2_entry is False
    assert set(decisions["closeout_status"]) == {"style_proxy_conflict"}
    assert decisions["redundancy_gate_allowed"].eq(False).all()
    assert decisions["allocator_entry_allowed"].eq(False).all()
    assert decisions["not_style_neutral_alpha"].eq(True).all()
    assert decisions["not_alpha_evidence"].eq(True).all()
    assert decisions["decision_reason"].str.contains("positive proxy residual").any()

    diagnostics = json.loads(Path(result.diagnostics_path).read_text(encoding="utf-8"))
    assert diagnostics["schema_version"] == "strict_residual_closeout_diagnostics.v1"
    assert diagnostics["ready_for_redundancy_count"] == 0
    assert set(diagnostics["blocked_factors"]) == {"momentum_12_1", "low_vol_60d"}

    registry = yaml.safe_load(Path(result.registry_update_path).read_text(encoding="utf-8"))
    assert registry["schema_version"] == "factor_registry_risk_model_update.v1"
    assert registry["non_claims"]["production_approval"] is False
    assert {row["registry_status"] for row in registry["factors"]} == {"style_proxy_conflict"}

    report = Path(result.report_path).read_text(encoding="utf-8").lower()
    assert "strict residual evidence closeout" in report
    assert "positive proxy residual" in report
    assert "not style-neutral alpha" in report
    assert "redundancy gate remains blocked" in report


def test_strict_residual_closeout_allows_only_clean_stable_residual(tmp_path: Path) -> None:
    input_dir = tmp_path / "r12"
    input_dir.mkdir()
    _write_waterfall(
        input_dir,
        [
            {
                "factor_id": "clean_factor",
                "period_count": 36,
                "gross_spread_mean": 0.012,
                "qqq_relative_spread_mean": 0.004,
                "beta_adjusted_spread_mean": 0.006,
                "industry_adjusted_spread_mean": 0.005,
                "style_proxy_adjusted_spread_mean": 0.004,
                "full_residual_spread_mean": 0.004,
                "style_proxy_conflict_count": 0,
                "waterfall_status": "proxy_residual_positive",
                "redundancy_gate_allowed": False,
                "not_style_neutral_alpha": True,
                "not_alpha_evidence": True,
            },
            {
                "factor_id": "raw_only_factor",
                "period_count": 36,
                "gross_spread_mean": 0.015,
                "qqq_relative_spread_mean": 0.004,
                "beta_adjusted_spread_mean": 0.003,
                "industry_adjusted_spread_mean": 0.002,
                "style_proxy_adjusted_spread_mean": -0.001,
                "full_residual_spread_mean": -0.001,
                "style_proxy_conflict_count": 0,
                "waterfall_status": "residual_not_positive",
                "redundancy_gate_allowed": False,
                "not_style_neutral_alpha": True,
                "not_alpha_evidence": True,
            },
        ],
    )
    _write_by_period(
        input_dir,
        [
            *(_period_rows("clean_factor", "proxy_residual_positive", residual=0.01, count=30)),
            *(_period_rows("clean_factor", "diagnostic_only", residual=-0.002, count=6)),
            *(_period_rows("raw_only_factor", "residual_not_positive", residual=-0.01, count=36)),
        ],
    )

    result = run_strict_residual_closeout(input_dir, tmp_path / "r13")

    decisions = pd.read_csv(result.decision_table_path).set_index("factor_id")
    assert result.ready_for_redundancy_count == 1
    assert decisions.loc["clean_factor", "closeout_status"] == "ready_for_redundancy_gate"
    assert bool(decisions.loc["clean_factor", "redundancy_gate_allowed"]) is True
    assert decisions.loc["raw_only_factor", "closeout_status"] == "insufficient_residual_evidence"
    assert bool(decisions.loc["raw_only_factor", "redundancy_gate_allowed"]) is False
    assert "residual" in decisions.loc["raw_only_factor", "decision_reason"]


def _write_waterfall(path: Path, rows: list[dict[str, object]]) -> None:
    pd.DataFrame(rows).to_csv(path / "factor_attribution_waterfall.csv", index=False)


def _write_by_period(path: Path, rows: list[dict[str, object]]) -> None:
    pd.DataFrame(rows).to_csv(path / "factor_attribution_waterfall_by_period.csv", index=False)


def _period_rows(factor_id: str, status: str, residual: float, count: int) -> list[dict[str, object]]:
    rows: list[dict[str, object]] = []
    for index in range(count):
        rows.append(
            {
                "factor_id": factor_id,
                "date": f"2020-{(index % 12) + 1:02d}-28",
                "asset_count": 100,
                "gross_spread": residual + 0.01,
                "qqq_relative_spread": residual,
                "beta_adjusted_spread": residual,
                "industry_adjusted_spread": residual,
                "style_proxy_adjusted_spread": residual,
                "full_residual_spread": residual,
                "waterfall_status": status,
                "same_close_trading_used": False,
                "not_style_neutral_alpha": True,
                "not_alpha_evidence": True,
            }
        )
    return rows
