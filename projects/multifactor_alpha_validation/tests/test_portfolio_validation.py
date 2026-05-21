from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.portfolio_validation import run_portfolio_ensemble_validation


def test_portfolio_validation_uses_soft_pool_excludes_hard_blocks_and_includes_placebos(tmp_path: Path) -> None:
    input_dir = tmp_path / "risk_model"
    input_dir.mkdir()
    output_dir = tmp_path / "portfolio_validation"
    pool_path = input_dir / "soft_resurrected_component_pool.csv"
    component_path = input_dir / "component_candidate_table.csv"
    observation_path = tmp_path / "real_oos_observations.csv"
    waterfall_path = tmp_path / "factor_attribution_waterfall_by_period.csv"

    _write_component_pool(pool_path)
    _write_component_table(component_path)
    _write_observations(observation_path)
    _write_waterfall(waterfall_path)

    result = run_portfolio_ensemble_validation(
        component_pool_path=pool_path,
        component_candidate_path=component_path,
        oos_observation_path=observation_path,
        waterfall_by_period_path=waterfall_path,
        output_dir=output_dir,
    )

    report = pd.read_csv(result.portfolio_ensemble_oos_report_path)
    baselines = pd.read_csv(result.ensemble_vs_baselines_path)
    random_placebo = pd.read_csv(result.random_weight_placebo_report_path)
    permuted_placebo = pd.read_csv(result.permuted_signal_placebo_report_path)
    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))

    assert result.input_component_count == 4
    assert result.available_component_count == 2
    assert result.hard_blocked_component_count == 1
    assert result.unavailable_component_count == 1
    assert set(summary["available_component_ids"]) == {"momentum_12_1", "reversal_5_1"}
    assert "hard_blocked_factor" not in set(summary["available_component_ids"])
    assert "value_bm" in set(summary["unavailable_component_ids"])
    assert summary["full_sample_weights_used"] is False
    assert summary["or_optimizer_used"] is False
    assert summary["decision_state"] == "portfolio_component_pool_is_benchmark_exposure"
    assert summary["non_claims"]["production_approval"] is False

    expected_ensembles = {
        "equal_weight_all_components",
        "equal_weight_by_cluster",
        "inverse_vol_ensemble",
        "simple_shrinkage_ensemble",
        "current_three_factor_component_ensemble",
        "best_single_factor",
        "QQQ_benchmark",
        "random_weight_placebo",
        "permuted_signal_placebo",
    }
    assert expected_ensembles <= set(report["ensemble_id"])
    assert expected_ensembles <= set(baselines["ensemble_id"])
    assert bool(report["full_sample_weights_used"].any()) is False
    assert bool(report["uses_unrestricted_optimizer"].any()) is False
    assert not random_placebo.empty
    assert not permuted_placebo.empty


def test_portfolio_validation_writes_structured_unavailable_outputs_without_fabricating_returns(tmp_path: Path) -> None:
    input_dir = tmp_path / "risk_model"
    input_dir.mkdir()
    pool_path = input_dir / "soft_resurrected_component_pool.csv"
    _write_component_pool(pool_path)

    result = run_portfolio_ensemble_validation(
        component_pool_path=pool_path,
        component_candidate_path=input_dir / "missing_component_table.csv",
        oos_observation_path=input_dir / "missing_observations.csv",
        waterfall_by_period_path=input_dir / "missing_waterfall.csv",
        output_dir=tmp_path / "portfolio_validation",
    )

    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    report = pd.read_csv(result.portfolio_ensemble_oos_report_path)

    assert result.validation_status == "unavailable"
    assert summary["decision_state"] == "portfolio_component_pool_inconclusive"
    assert summary["unavailable_reason"] == "missing_oos_observations"
    assert summary["fabricated_returns"] is False
    assert report.empty


def _write_component_pool(path: Path) -> None:
    rows = [
        _pool_row("momentum_12_1", "momentum", "soft_resurrected", True, "eligible_benchmark_premia_component"),
        _pool_row("reversal_5_1", "reversal", "soft_resurrected", True, "eligible_benchmark_premia_component"),
        _pool_row("value_bm", "value", "soft_resurrected", True, "eligible_fundamental_premia_component"),
        _pool_row("hard_blocked_factor", "blocked", "hard_excluded", False, "blocked_component"),
    ]
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_component_table(path: Path) -> None:
    rows = [
        {
            "factor_id": "momentum_12_1",
            "component_status": "eligible_benchmark_premia_component",
            "component_role": "style_premia_return_driver",
            "portfolio_validation_allowed": True,
        },
        {
            "factor_id": "reversal_5_1",
            "component_status": "eligible_benchmark_premia_component",
            "component_role": "style_premia_return_driver",
            "portfolio_validation_allowed": True,
        },
        {
            "factor_id": "hard_blocked_factor",
            "component_status": "blocked_component",
            "component_role": "hard_blocked_component",
            "portfolio_validation_allowed": False,
        },
    ]
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_observations(path: Path) -> None:
    rows: list[dict[str, object]] = []
    dates = pd.date_range("2020-01-31", periods=8, freq="ME")
    for i, date in enumerate(dates):
        qqq_return = 0.01 + i * 0.0005
        for factor_id, spread_offset in {
            "momentum_12_1": 0.0002,
            "reversal_5_1": -0.0001,
            "hard_blocked_factor": 0.05,
        }.items():
            gross_spread = qqq_return + spread_offset
            rows.append(
                {
                    "factor_id": factor_id,
                    "rebalance_date": date.date().isoformat(),
                    "history_cutoff_date": (date - pd.Timedelta(days=1)).date().isoformat(),
                    "full_sample_icir_used": False,
                    "prior_history_only": True,
                    "gross_spread": gross_spread,
                    "net_spread": gross_spread - 0.001,
                    "qqq_return": qqq_return,
                    "qqq_relative_spread": gross_spread - qqq_return,
                    "beta_adjusted_spread": gross_spread - qqq_return,
                    "sector_adjusted_spread": gross_spread - qqq_return,
                    "style_adjusted_spread": gross_spread - qqq_return,
                    "cost_drag": 0.001,
                    "asset_count": 50,
                }
            )
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_waterfall(path: Path) -> None:
    rows: list[dict[str, object]] = []
    for row in pd.read_csv(path.parent / "real_oos_observations.csv").itertuples(index=False):
        rows.append(
            {
                "factor_id": row.factor_id,
                "date": row.rebalance_date,
                "gross_spread": row.gross_spread,
                "qqq_return": row.qqq_return,
                "qqq_relative_spread": row.qqq_relative_spread,
                "beta_adjusted_spread": row.beta_adjusted_spread,
                "industry_adjusted_spread": row.sector_adjusted_spread,
                "style_proxy_adjusted_spread": row.style_adjusted_spread,
                "full_residual_spread": row.style_adjusted_spread,
                "waterfall_status": "benchmark_exposure",
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)


def _pool_row(
    factor_id: str,
    family_id: str,
    filter_class: str,
    eligible: bool,
    component_status: str,
) -> dict[str, object]:
    return {
        "factor_id": factor_id,
        "family_id": family_id,
        "filter_class": filter_class,
        "component_pool_eligible": eligible,
        "component_status": component_status,
        "component_role": "style_premia_return_driver",
        "portfolio_validation_allowed": eligible,
        "hard_exclusion_reason": "" if eligible else "blocked_pit_failure",
        "not_alpha_evidence": True,
        "production_approval": False,
        "live_trading": False,
        "security_orders": False,
        "direct_q2_entry": False,
    }
