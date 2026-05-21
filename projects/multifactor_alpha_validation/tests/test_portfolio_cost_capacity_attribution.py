from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.portfolio_cost_capacity import run_portfolio_cost_capacity_attribution


def test_cost_capacity_attribution_reports_component_cost_drag_without_unlocking_or(tmp_path: Path) -> None:
    risk_dir = tmp_path / "risk_model"
    validation_dir = tmp_path / "portfolio_validation"
    contribution_dir = tmp_path / "portfolio_contribution"
    output_dir = tmp_path / "portfolio_cost_capacity"
    risk_dir.mkdir()
    validation_dir.mkdir()
    contribution_dir.mkdir()
    pool_path = risk_dir / "soft_resurrected_component_pool.csv"
    observations_path = tmp_path / "real_oos_observations.csv"
    _write_component_pool(pool_path)
    _write_observations(observations_path)
    _write_r15_summary(validation_dir)
    _write_r16_summary(contribution_dir)

    result = run_portfolio_cost_capacity_attribution(
        component_pool_path=pool_path,
        oos_observation_path=observations_path,
        portfolio_validation_dir=validation_dir,
        portfolio_contribution_dir=contribution_dir,
        output_dir=output_dir,
    )

    attribution = pd.read_csv(result.component_cost_capacity_attribution_path).set_index("factor_id")
    frontier = pd.read_csv(result.capacity_frontier_path)
    stress = pd.read_csv(result.cost_stress_report_path)
    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    markdown = Path(result.report_path).read_text(encoding="utf-8").lower()

    assert result.validation_status == "evaluated"
    assert result.decision_state == "cost_capacity_attribution_diagnostic_only"
    assert summary["observed_component_count"] == 3
    assert summary["security_level_adv_available"] is False
    assert summary["capacity_model_scope"] == "component_proxy_only"
    assert summary["or_optimizer_used"] is False
    assert summary["security_level_portfolio_construction_used"] is False
    assert summary["direct_q2_entry"] is False
    assert summary["non_claims"]["production_approval"] is False

    assert attribution.loc["costly_reversal", "component_cost_capacity_decision"] == "cost_toxic_component"
    assert attribution.loc["costly_reversal", "cost_drag_to_gross_ratio"] > 1.0
    assert attribution.loc["momentum_12_1", "component_cost_capacity_decision"] in {
        "cost_capacity_watch_component",
        "cost_capacity_ok_component",
    }
    assert attribution.loc["thin_component", "component_cost_capacity_decision"] == "capacity_fragile_component"

    assert {"aum_usd", "participation_cap", "capacity_proxy_status", "component_id"}.issubset(frontier.columns)
    assert set(frontier["capacity_proxy_status"]) == {"proxy_only_missing_security_level_adv"}
    assert {"base_cost", "cost_2x", "cost_3x"} <= set(stress["cost_stress_scenario"])
    assert "diagnostic only" in markdown
    assert "or remains locked" in markdown
    assert "security-level adv unavailable" in markdown


def test_cost_capacity_attribution_writes_unavailable_without_fabricating_capacity(tmp_path: Path) -> None:
    risk_dir = tmp_path / "risk_model"
    validation_dir = tmp_path / "portfolio_validation"
    contribution_dir = tmp_path / "portfolio_contribution"
    risk_dir.mkdir()
    validation_dir.mkdir()
    contribution_dir.mkdir()
    pool_path = risk_dir / "soft_resurrected_component_pool.csv"
    _write_component_pool(pool_path)

    result = run_portfolio_cost_capacity_attribution(
        component_pool_path=pool_path,
        oos_observation_path=tmp_path / "missing_observations.csv",
        portfolio_validation_dir=validation_dir,
        portfolio_contribution_dir=contribution_dir,
        output_dir=tmp_path / "portfolio_cost_capacity",
    )

    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    attribution = pd.read_csv(result.component_cost_capacity_attribution_path)

    assert result.validation_status == "unavailable"
    assert summary["decision_state"] == "cost_capacity_attribution_unavailable"
    assert summary["unavailable_reason"] == "missing_oos_observations"
    assert summary["fabricated_capacity"] is False
    assert summary["security_level_adv_available"] is False
    assert attribution.empty


def _write_component_pool(path: Path) -> None:
    pd.DataFrame(
        [
            _pool_row("momentum_12_1", "momentum", "style_premia_return_driver"),
            _pool_row("costly_reversal", "reversal", "style_premia_return_driver"),
            _pool_row("thin_component", "microcap", "fundamental_premia_component"),
            _pool_row("hard_blocked_factor", "blocked", "hard_blocked_component", eligible=False),
        ]
    ).to_csv(path, index=False)


def _write_observations(path: Path) -> None:
    rows: list[dict[str, object]] = []
    dates = pd.date_range("2020-01-31", periods=6, freq="ME")
    for date in dates:
        rows.extend(
            [
                _observation("momentum_12_1", date, gross=0.020, net=0.016, asset_count=80),
                _observation("costly_reversal", date, gross=0.010, net=-0.008, asset_count=75),
                _observation("thin_component", date, gross=0.018, net=0.014, asset_count=4),
                _observation("hard_blocked_factor", date, gross=0.500, net=0.500, asset_count=1),
            ]
        )
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_r15_summary(path: Path) -> None:
    (path / "ensemble_validation_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "portfolio_validation_summary.v1",
                "decision_state": "component_pool_fails_gross",
                "available_component_ids": ["momentum_12_1", "costly_reversal", "thin_component"],
                "unavailable_component_ids": ["sue_event_reference"],
                "or_optimizer_used": False,
                "security_level_portfolio_construction_used": False,
            },
            indent=2,
        )
        + "\n",
        encoding="utf-8",
    )


def _write_r16_summary(path: Path) -> None:
    (path / "portfolio_contribution_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "portfolio_contribution_summary.v1",
                "decision_state": "portfolio_contribution_diagnostic_only",
                "observed_component_count": 3,
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
    *,
    eligible: bool = True,
) -> dict[str, object]:
    return {
        "factor_id": factor_id,
        "family_id": family_id,
        "cluster_id": family_id,
        "filter_class": "soft_resurrected" if eligible else "hard_excluded",
        "component_pool_eligible": eligible,
        "component_status": "eligible_component" if eligible else "blocked_component",
        "component_role": component_role,
        "portfolio_validation_allowed": eligible,
        "not_alpha_evidence": True,
        "production_approval": False,
        "live_trading": False,
        "security_orders": False,
        "direct_q2_entry": False,
    }


def _observation(
    factor_id: str,
    date: pd.Timestamp,
    *,
    gross: float,
    net: float,
    asset_count: int,
) -> dict[str, object]:
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
        "qqq_return": 0.002,
        "qqq_relative_spread": net - 0.002,
        "beta_adjusted_spread": net - 0.001,
        "sector_adjusted_spread": net,
        "style_adjusted_spread": net,
        "cost_drag": gross - net,
        "asset_count": asset_count,
        "not_alpha_evidence": True,
    }
