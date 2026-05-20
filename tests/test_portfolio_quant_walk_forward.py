from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from portfolio_os.backtest.engine import run_backtest
from portfolio_os.backtest.portfolio_quant import run_portfolio_quant_walk_forward


def _write_portfolio_quant_manifest(project_root: Path, tmp_path: Path) -> Path:
    source_manifest = project_root / "data" / "backtest_samples" / "manifest_us_expanded.yaml"
    payload = yaml.safe_load(source_manifest.read_text(encoding="utf-8"))
    payload["baselines"] = ["naive_pro_rata", "buy_and_hold", "cost_unaware_rebalance"]
    payload["portfolio_quant"] = {
        "enabled": True,
        "rebalance_frequency": "monthly",
        "include_cost_unaware_baseline": True,
        "benchmark_strategy": "naive_pro_rata",
        "policy": {
            "turnover_cap": 0.35,
            "max_drawdown_limit": -0.20,
            "cvar_alpha": 0.05,
            "min_cvar_limit": -0.03,
            "exposure_check": "if_available",
        },
    }
    manifest_path = tmp_path / "portfolio_quant_manifest.yaml"
    manifest_path.write_text(
        yaml.safe_dump(payload, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    return manifest_path


def test_backtest_supports_cost_unaware_rebalance_baseline(project_root: Path, tmp_path: Path) -> None:
    manifest_path = _write_portfolio_quant_manifest(project_root, tmp_path)

    result = run_backtest(manifest_path)

    assert "cost_unaware_rebalance" in set(result.nav_series["strategy"])
    assert "cost_unaware_rebalance" in result.summary["strategies"]
    assert "optimizer_vs_cost_unaware_ending_nav_delta" in result.summary["comparison"]
    assert result.summary["strategies"]["cost_unaware_rebalance"]["total_transaction_cost"] >= 0.0
    assert (
        result.summary["strategies"]["optimizer"]["ending_nav"]
        != result.summary["strategies"]["cost_unaware_rebalance"]["ending_nav"]
    )
    assert set(result.period_attribution["strategy"]) >= {
        "optimizer",
        "naive_pro_rata",
        "buy_and_hold",
        "cost_unaware_rebalance",
    }


def test_portfolio_quant_walk_forward_writes_resume_ready_artifacts(project_root: Path, tmp_path: Path) -> None:
    manifest_path = _write_portfolio_quant_manifest(project_root, tmp_path)
    output_dir = tmp_path / "portfolio_quant_output"

    result = run_portfolio_quant_walk_forward(manifest_path=manifest_path, output_dir=output_dir)

    expected_files = {
        "portfolio_quant_summary.json",
        "portfolio_quant_nav_curve.csv",
        "portfolio_quant_drawdown_curve.csv",
        "portfolio_quant_turnover_distribution.csv",
        "portfolio_quant_cost_attribution.csv",
        "portfolio_quant_multi_snapshot_replay.csv",
        "portfolio_quant_strategy_comparison.csv",
        "portfolio_quant_policy_breaches.csv",
        "portfolio_quant_report.md",
    }
    assert expected_files <= {path.name for path in output_dir.iterdir()}

    summary = json.loads((output_dir / "portfolio_quant_summary.json").read_text(encoding="utf-8"))
    turnover_distribution = pd.read_csv(output_dir / "portfolio_quant_turnover_distribution.csv")
    cost_attribution = pd.read_csv(output_dir / "portfolio_quant_cost_attribution.csv")
    multi_snapshot = pd.read_csv(output_dir / "portfolio_quant_multi_snapshot_replay.csv")
    drawdown_curve = pd.read_csv(output_dir / "portfolio_quant_drawdown_curve.csv")
    comparison = pd.read_csv(output_dir / "portfolio_quant_strategy_comparison.csv")
    policy_breaches = pd.read_csv(output_dir / "portfolio_quant_policy_breaches.csv")
    report_text = (output_dir / "portfolio_quant_report.md").read_text(encoding="utf-8")

    assert result.summary_path == output_dir / "portfolio_quant_summary.json"
    assert summary["metadata"]["evaluation_type"] == "historical_walk_forward_portfolio_quant"
    assert summary["metadata"]["rebalance_frequency"] == "monthly"
    assert summary["metadata"]["not_alpha_research"] is True
    assert summary["policy"]["turnover_cap"] == 0.35
    assert summary["policy"]["max_drawdown_limit"] == -0.20
    assert summary["policy"]["min_cvar_limit"] == -0.03
    assert summary["downstream_flags"] == {
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        "optimizer_alpha_input_opened": False,
        "alpha_registry_update_allowed": False,
        "paper_ready": False,
        "live_ready": False,
        "broker_order_path_opened": False,
        "production_approval_claimed": False,
    }
    assert {
        "equal_weight",
        "mean_variance",
        "risk_parity",
        "cost_unaware_rebalance",
        "portfolio_os_cost_aware_rebalance",
    } <= set(
        summary["strategies"]
    )
    assert "cvar_5" in summary["strategies"]["portfolio_os_cost_aware_rebalance"]
    assert summary["policy"]["exposure_status"] in {"evaluated_from_industry", "unavailable"}
    assert summary["policy"]["exposure_result_fabricated"] is False
    assert "max_exposure_drift" in summary["strategies"]["portfolio_os_cost_aware_rebalance"]

    assert {"strategy", "turnover_mean", "turnover_median", "turnover_p95", "turnover_max"} <= set(
        turnover_distribution.columns
    )
    assert {
        "strategy",
        "commission_cost",
        "spread_cost",
        "slippage_cost",
        "total_transaction_cost",
    } <= set(cost_attribution.columns)
    assert (cost_attribution["total_transaction_cost"] >= 0.0).all()
    assert {
        "date",
        "strategy",
        "turnover",
        "commission_cost",
        "spread_cost",
        "slippage_cost",
        "total_transaction_cost",
    } <= set(multi_snapshot.columns)
    assert set(multi_snapshot["strategy"]) >= {
        "cost_unaware_rebalance",
        "portfolio_os_cost_aware_rebalance",
    }
    assert "drawdown" in drawdown_curve.columns
    assert float(drawdown_curve["drawdown"].min()) <= 0.0
    assert {"comparison", "ending_nav_delta", "total_cost_delta", "total_turnover_delta"} <= set(
        comparison.columns
    )
    assert "portfolio_os_cost_aware_vs_cost_unaware" in set(comparison["comparison"])
    assert {"policy_name", "strategy", "breached"} <= set(policy_breaches.columns)
    assert {"turnover_cap", "max_drawdown_limit", "min_cvar_limit", "exposure_drift"} <= set(
        policy_breaches["policy_name"]
    )

    forbidden_text = report_text.lower()
    for forbidden in (
        "alpha passed",
        "q2-ready",
        "paper-ready",
        "live-ready",
        "production-ready",
        "broker order",
    ):
        assert forbidden not in forbidden_text
    assert "Portfolio Quant Walk-Forward Report" in report_text
