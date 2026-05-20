from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.portfolio_contribution import run_post_portfolio_contribution


def test_portfolio_contribution_ablation_classifies_components_without_unlocking_or(tmp_path: Path) -> None:
    risk_dir = tmp_path / "risk_model"
    validation_dir = tmp_path / "portfolio_validation"
    output_dir = tmp_path / "portfolio_contribution"
    risk_dir.mkdir()
    validation_dir.mkdir()
    pool_path = risk_dir / "soft_resurrected_component_pool.csv"
    observations_path = tmp_path / "real_oos_observations.csv"
    _write_component_pool(pool_path)
    _write_observations(observations_path)
    _write_r15_summary(validation_dir)

    result = run_post_portfolio_contribution(
        component_pool_path=pool_path,
        oos_observation_path=observations_path,
        portfolio_validation_dir=validation_dir,
        output_dir=output_dir,
    )

    factor_report = pd.read_csv(result.factor_ablation_report_path).set_index("factor_id")
    cluster_report = pd.read_csv(result.cluster_ablation_report_path)
    role_report = pd.read_csv(result.factor_role_contribution_path)
    regime_report = pd.read_csv(result.contribution_by_regime_path)
    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    markdown = Path(result.report_path).read_text(encoding="utf-8").lower()

    assert result.validation_status == "evaluated"
    assert summary["observed_component_count"] == 3
    assert summary["or_optimizer_used"] is False
    assert summary["security_level_portfolio_construction_used"] is False
    assert summary["non_claims"]["production_approval"] is False

    assert factor_report.loc["reversal_5_1", "component_decision"] == "cost_negative_component"
    assert factor_report.loc["reversal_5_1", "contribution_to_cost_adjusted_return"] < 0.0
    assert factor_report.loc["momentum_12_1", "contribution_to_cost_adjusted_return"] > 0.0
    assert factor_report.loc["low_vol_60d", "component_decision"] in {
        "hedge_component",
        "diversifier_component",
        "regime_specific_component",
    }

    assert "reversal" in set(cluster_report["cluster_id"])
    assert "style_premia_return_driver" in set(role_report["component_role"])
    assert {"QQQ_up", "QQQ_down"} <= set(regime_report["regime"])
    assert "not alpha evidence" in markdown
    assert "or remains locked" in markdown


def test_portfolio_contribution_writes_structured_unavailable_without_fabricating_returns(tmp_path: Path) -> None:
    risk_dir = tmp_path / "risk_model"
    validation_dir = tmp_path / "portfolio_validation"
    risk_dir.mkdir()
    validation_dir.mkdir()
    pool_path = risk_dir / "soft_resurrected_component_pool.csv"
    _write_component_pool(pool_path)

    result = run_post_portfolio_contribution(
        component_pool_path=pool_path,
        oos_observation_path=tmp_path / "missing_observations.csv",
        portfolio_validation_dir=validation_dir,
        output_dir=tmp_path / "portfolio_contribution",
    )

    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    factor_report = pd.read_csv(result.factor_ablation_report_path)

    assert result.validation_status == "unavailable"
    assert summary["decision_state"] == "portfolio_contribution_unavailable"
    assert summary["unavailable_reason"] == "missing_oos_observations"
    assert summary["fabricated_returns"] is False
    assert factor_report.empty


def _write_component_pool(path: Path) -> None:
    pd.DataFrame(
        [
            _pool_row("momentum_12_1", "momentum", "style_premia_return_driver", "eligible_benchmark_premia_component"),
            _pool_row("reversal_5_1", "reversal", "style_premia_return_driver", "eligible_benchmark_premia_component"),
            _pool_row("low_vol_60d", "low_vol", "hedge_or_diversifier_component", "eligible_hedge_component"),
            _pool_row("hard_blocked_factor", "blocked", "hard_blocked_component", "blocked_component", eligible=False),
        ]
    ).to_csv(path, index=False)


def _write_observations(path: Path) -> None:
    rows: list[dict[str, object]] = []
    dates = pd.date_range("2020-01-31", periods=8, freq="ME")
    for index, date in enumerate(dates):
        qqq_return = 0.03 if index % 2 == 0 else -0.03
        rows.extend(
            [
                _observation("momentum_12_1", date, gross=0.020, net=0.018, qqq_return=qqq_return),
                _observation("reversal_5_1", date, gross=0.014, net=-0.010, qqq_return=qqq_return),
                _observation("low_vol_60d", date, gross=-0.004 if qqq_return > 0 else 0.018, net=-0.005 if qqq_return > 0 else 0.017, qqq_return=qqq_return),
                _observation("hard_blocked_factor", date, gross=0.500, net=0.500, qqq_return=qqq_return),
            ]
        )
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_r15_summary(path: Path) -> None:
    (path / "ensemble_validation_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "portfolio_validation_summary.v1",
                "decision_state": "portfolio_component_pool_fails_cost",
                "available_component_ids": ["momentum_12_1", "reversal_5_1", "low_vol_60d"],
                "unavailable_component_ids": ["sue_event_reference"],
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
    component_role: str,
    component_status: str,
    *,
    eligible: bool = True,
) -> dict[str, object]:
    return {
        "factor_id": factor_id,
        "family_id": family_id,
        "cluster_id": family_id,
        "filter_class": "soft_resurrected" if eligible else "hard_excluded",
        "component_pool_eligible": eligible,
        "component_status": component_status,
        "component_role": component_role,
        "portfolio_validation_allowed": eligible,
        "not_alpha_evidence": True,
        "production_approval": False,
        "live_trading": False,
        "security_orders": False,
        "direct_q2_entry": False,
    }


def _observation(factor_id: str, date: pd.Timestamp, *, gross: float, net: float, qqq_return: float) -> dict[str, object]:
    return {
        "schema_version": "real_rolling_oos_observation.v1",
        "factor_id": factor_id,
        "rebalance_date": date.date().isoformat(),
        "history_cutoff_date": (date - pd.Timedelta(days=1)).date().isoformat(),
        "signal_date": date.date().isoformat(),
        "tradable_date": (date + pd.Timedelta(days=1)).date().isoformat(),
        "same_close_trading_used": False,
        "full_sample_icir_used": False,
        "prior_history_only": True,
        "gross_spread": gross,
        "net_spread": net,
        "qqq_return": qqq_return,
        "qqq_relative_spread": net - qqq_return,
        "beta_adjusted_spread": net - 0.5 * qqq_return,
        "sector_adjusted_spread": net,
        "style_adjusted_spread": net,
        "cost_drag": gross - net,
        "asset_count": 50,
        "not_alpha_evidence": True,
    }
