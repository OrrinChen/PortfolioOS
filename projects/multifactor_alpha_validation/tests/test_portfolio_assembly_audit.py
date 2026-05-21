from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.portfolio_assembly_audit import run_portfolio_assembly_audit


def test_portfolio_assembly_audit_reclassifies_observed_subset_and_writes_diagnostics(tmp_path: Path) -> None:
    risk_dir = tmp_path / "risk_model"
    validation_dir = tmp_path / "portfolio_validation"
    risk_dir.mkdir()
    validation_dir.mkdir()
    pool_path = risk_dir / "soft_resurrected_component_pool.csv"
    component_path = risk_dir / "component_candidate_table.csv"
    observations_path = tmp_path / "real_oos_observations.csv"
    _write_component_pool(pool_path)
    _write_component_table(component_path)
    _write_oos_observations(observations_path)
    _write_r15_outputs(validation_dir, gross_return=0.02, net_return=-0.01, decision_state="portfolio_component_pool_fails_cost")

    result = run_portfolio_assembly_audit(
        component_pool_path=pool_path,
        component_candidate_path=component_path,
        portfolio_validation_dir=validation_dir,
        oos_observation_path=observations_path,
        output_dir=validation_dir,
    )

    audit = json.loads(Path(result.audit_path).read_text(encoding="utf-8"))
    coverage = pd.read_csv(result.coverage_report_path)
    direction = pd.read_csv(result.direction_audit_path)
    waterfall = pd.read_csv(result.gross_to_net_waterfall_path)
    role_report = pd.read_csv(result.role_aware_ensemble_report_path)
    report = Path(result.reclassification_report_path).read_text(encoding="utf-8").lower()

    assert audit["original_decision_state"] == "portfolio_component_pool_fails_cost"
    assert audit["reclassified_decision_state"] == "observed_subset_turnover_killed"
    assert audit["component_pool_validation_state"] == "component_pool_unavailable_coverage_gap"
    assert audit["observed_component_count"] == 2
    assert audit["unavailable_component_count"] == 1
    assert audit["coverage_ratio"] < 0.8
    assert audit["or_optimizer_used"] is False
    assert audit["security_level_portfolio_construction_used"] is False
    assert audit["non_claims"]["production_approval"] is False

    assert set(coverage["factor_id"]) == {"momentum_12_1", "low_vol_60d", "value_bm", "hard_blocked_factor"}
    assert set(direction["factor_id"]) == {"momentum_12_1", "low_vol_60d"}
    assert direction.set_index("factor_id").loc["low_vol_60d", "expected_long_leg"] == "low_volatility_assets"
    assert set(waterfall["layer"]) == {"gross_return", "estimated_cost_drag", "cost_adjusted_return"}
    assert "return_driver_plus_hedge_80_20" in set(role_report["ensemble_id"])
    assert "momentum_plus_low_vol" in set(role_report["ensemble_id"])
    assert "observed subset" in report
    assert "not full component pool failure" in report
    assert "or remains blocked" in report


def test_portfolio_assembly_audit_distinguishes_gross_failure_from_cost_failure(tmp_path: Path) -> None:
    risk_dir = tmp_path / "risk_model"
    validation_dir = tmp_path / "portfolio_validation"
    risk_dir.mkdir()
    validation_dir.mkdir()
    pool_path = risk_dir / "soft_resurrected_component_pool.csv"
    component_path = risk_dir / "component_candidate_table.csv"
    observations_path = tmp_path / "real_oos_observations.csv"
    _write_component_pool(pool_path)
    _write_component_table(component_path)
    _write_oos_observations(observations_path)
    _write_r15_outputs(validation_dir, gross_return=-0.02, net_return=-0.03, decision_state="portfolio_component_pool_fails_cost")

    result = run_portfolio_assembly_audit(
        component_pool_path=pool_path,
        component_candidate_path=component_path,
        portfolio_validation_dir=validation_dir,
        oos_observation_path=observations_path,
        output_dir=validation_dir,
    )

    audit = json.loads(Path(result.audit_path).read_text(encoding="utf-8"))
    waterfall = pd.read_csv(result.gross_to_net_waterfall_path)

    assert audit["reclassified_decision_state"] == "observed_subset_fails_gross"
    assert audit["gross_failure"] is True
    assert audit["cost_killed_after_positive_gross"] is False
    assert waterfall.set_index("layer").loc["gross_return", "annualized_return"] == -0.02


def test_portfolio_assembly_audit_allows_component_pool_diagnosis_when_coverage_is_sufficient(
    tmp_path: Path,
) -> None:
    risk_dir = tmp_path / "risk_model"
    validation_dir = tmp_path / "portfolio_validation"
    risk_dir.mkdir()
    validation_dir.mkdir()
    pool_path = risk_dir / "soft_resurrected_component_pool.csv"
    component_path = risk_dir / "component_candidate_table.csv"
    observations_path = tmp_path / "real_oos_observations.csv"
    _write_component_pool(pool_path)
    _write_component_table(component_path)
    _write_oos_observations(observations_path)
    _write_r15_outputs(
        validation_dir,
        gross_return=-0.02,
        net_return=-0.03,
        decision_state="portfolio_component_pool_fails_cost",
        available_component_ids=["low_vol_60d", "momentum_12_1", "value_bm"],
        unavailable_component_ids=[],
    )

    result = run_portfolio_assembly_audit(
        component_pool_path=pool_path,
        component_candidate_path=component_path,
        portfolio_validation_dir=validation_dir,
        oos_observation_path=observations_path,
        output_dir=validation_dir,
    )

    audit = json.loads(Path(result.audit_path).read_text(encoding="utf-8"))
    report = Path(result.reclassification_report_path).read_text(encoding="utf-8").lower()

    assert audit["component_pool_validation_state"] == "component_pool_observation_sufficient"
    assert audit["component_pool_decision_allowed"] is True
    assert audit["reclassified_decision_state"] == "component_pool_fails_gross"
    assert "current observed component pool" in report
    assert "not full component pool failure" not in report


def _write_component_pool(path: Path) -> None:
    pd.DataFrame(
        [
            _pool_row("momentum_12_1", "momentum", "eligible_benchmark_premia_component", "style_premia_return_driver", True),
            _pool_row("low_vol_60d", "low_vol", "eligible_hedge_component", "hedge_or_diversifier_component", True),
            _pool_row("value_bm", "value", "eligible_fundamental_premia_component", "fundamental_premia_component", True),
            _pool_row("hard_blocked_factor", "blocked", "blocked_component", "hard_blocked_component", False),
        ]
    ).to_csv(path, index=False)


def _write_component_table(path: Path) -> None:
    pd.DataFrame(
        [
            {
                "factor_id": "momentum_12_1",
                "component_status": "eligible_benchmark_premia_component",
                "component_role": "style_premia_return_driver",
                "portfolio_validation_allowed": True,
            },
            {
                "factor_id": "low_vol_60d",
                "component_status": "eligible_hedge_component",
                "component_role": "hedge_or_diversifier_component",
                "portfolio_validation_allowed": True,
            },
        ]
    ).to_csv(path, index=False)


def _write_oos_observations(path: Path) -> None:
    rows: list[dict[str, object]] = []
    dates = pd.date_range("2020-01-31", periods=6, freq="ME")
    for i, date in enumerate(dates):
        qqq_return = 0.015 + i * 0.001
        rows.append(
            {
                "factor_id": "momentum_12_1",
                "rebalance_date": date.date().isoformat(),
                "rank_ic": 0.05,
                "gross_spread": 0.018,
                "net_spread": 0.012,
                "qqq_return": qqq_return,
                "qqq_relative_spread": 0.018 - qqq_return,
                "beta_adjusted_spread": 0.004,
                "sector_adjusted_spread": 0.004,
                "style_adjusted_spread": 0.004,
                "cost_drag": 0.006,
            }
        )
        rows.append(
            {
                "factor_id": "low_vol_60d",
                "rebalance_date": date.date().isoformat(),
                "rank_ic": -0.01,
                "gross_spread": 0.002,
                "net_spread": -0.003,
                "qqq_return": qqq_return,
                "qqq_relative_spread": 0.002 - qqq_return,
                "beta_adjusted_spread": 0.001,
                "sector_adjusted_spread": 0.001,
                "style_adjusted_spread": 0.001,
                "cost_drag": 0.005,
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_r15_outputs(
    validation_dir: Path,
    *,
    gross_return: float,
    net_return: float,
    decision_state: str,
    available_component_ids: list[str] | None = None,
    unavailable_component_ids: list[str] | None = None,
) -> None:
    available_component_ids = available_component_ids or ["low_vol_60d", "momentum_12_1"]
    unavailable_component_ids = unavailable_component_ids if unavailable_component_ids is not None else ["value_bm"]
    pd.DataFrame(
        [
            {
                "ensemble_id": "equal_weight_all_components",
                "annualized_return": gross_return,
                "cost_adjusted_return": net_return,
                "QQQ_relative_return": -0.08,
                "beta": -0.25,
                "beta_adjusted_return": 0.01,
                "Sharpe": -0.2,
                "max_drawdown": -0.25,
                "turnover": 0.2,
                "full_sample_weights_used": False,
                "uses_unrestricted_optimizer": False,
            }
        ]
    ).to_csv(validation_dir / "portfolio_ensemble_oos_report.csv", index=False)
    (validation_dir / "ensemble_validation_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "portfolio_validation_summary.v1",
                "decision_state": decision_state,
                "input_component_count": 4,
                "available_component_count": len(available_component_ids),
                "unavailable_component_count": len(unavailable_component_ids),
                "hard_blocked_component_count": 1,
                "available_component_ids": available_component_ids,
                "unavailable_component_ids": unavailable_component_ids,
                "full_sample_weights_used": False,
                "or_optimizer_used": False,
                "security_level_portfolio_construction_used": False,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _pool_row(
    factor_id: str,
    family_id: str,
    component_status: str,
    component_role: str,
    eligible: bool,
) -> dict[str, object]:
    return {
        "factor_id": factor_id,
        "family_id": family_id,
        "component_status": component_status,
        "component_role": component_role,
        "component_pool_eligible": eligible,
        "portfolio_validation_allowed": eligible,
        "filter_class": "soft_resurrected" if eligible else "hard_excluded",
        "not_alpha_evidence": True,
    }
