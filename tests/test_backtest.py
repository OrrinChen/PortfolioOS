from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml
from typer.testing import CliRunner

from portfolio_os.api.cli import backtest_app, backtest_sweep_app, risk_sweep_app
from portfolio_os.backtest.engine import run_backtest
from portfolio_os.backtest.manifest import load_backtest_manifest
from portfolio_os.backtest.sweep import run_backtest_cost_sweep, run_backtest_risk_sweep


def _write_backtest_fixture(tmp_path: Path) -> Path:
    base_dir = tmp_path / "backtest_fixture"
    base_dir.mkdir(parents=True, exist_ok=True)

    returns_path = base_dir / "returns_long.csv"
    market_path = base_dir / "market_snapshot.csv"
    holdings_path = base_dir / "initial_holdings.csv"
    target_path = base_dir / "target_weights.csv"
    reference_path = base_dir / "reference.csv"
    portfolio_state_path = base_dir / "portfolio_state.yaml"
    config_path = base_dir / "config.yaml"
    constraints_path = base_dir / "constraints.yaml"
    execution_profile_path = base_dir / "execution_profile.yaml"
    manifest_path = base_dir / "manifest.yaml"

    returns_rows = [
        ("2026-01-29", "AAA", 0.00),
        ("2026-01-29", "BBB", 0.00),
        ("2026-01-30", "AAA", 0.01),
        ("2026-01-30", "BBB", 0.02),
        ("2026-02-02", "AAA", 0.00),
        ("2026-02-02", "BBB", 0.01),
        ("2026-02-27", "AAA", -0.01),
        ("2026-02-27", "BBB", 0.03),
        ("2026-03-02", "AAA", 0.02),
        ("2026-03-02", "BBB", 0.00),
        ("2026-03-30", "AAA", 0.00),
        ("2026-03-30", "BBB", -0.02),
        ("2026-03-31", "AAA", 0.01),
        ("2026-03-31", "BBB", 0.01),
    ]
    pd.DataFrame(returns_rows, columns=["date", "ticker", "return"]).to_csv(returns_path, index=False)
    pd.DataFrame(
        [
            {
                "ticker": "AAA",
                "close": 11.0,
                "vwap": 11.0,
                "adv_shares": 1_000_000,
                "tradable": True,
                "upper_limit_hit": False,
                "lower_limit_hit": False,
            },
            {
                "ticker": "BBB",
                "close": 21.0,
                "vwap": 21.0,
                "adv_shares": 1_000_000,
                "tradable": True,
                "upper_limit_hit": False,
                "lower_limit_hit": False,
            },
        ]
    ).to_csv(market_path, index=False)
    pd.DataFrame(
        [
            {"ticker": "AAA", "quantity": 100},
            {"ticker": "BBB", "quantity": 0},
        ]
    ).to_csv(holdings_path, index=False)
    pd.DataFrame(
        [
            {"ticker": "AAA", "target_weight": 0.4},
            {"ticker": "BBB", "target_weight": 0.4},
        ]
    ).to_csv(target_path, index=False)
    pd.DataFrame(
        [
            {"ticker": "AAA", "industry": "Technology", "blacklist_buy": False, "blacklist_sell": False},
            {"ticker": "BBB", "industry": "Healthcare", "blacklist_buy": False, "blacklist_sell": False},
        ]
    ).to_csv(reference_path, index=False)
    portfolio_state_path.write_text(
        "\n".join(
            [
                "account_id: test_backtest",
                "as_of_date: '2026-01-29'",
                "available_cash: 5000.0",
                "min_cash_buffer: 0.0",
                "account_type: us_pilot",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    config_path.write_text(
        "\n".join(
            [
                "project:",
                "  name: PortfolioOS",
                '  disclaimer: "Auxiliary decision-support tool only. Not investment advice."',
                "trading:",
                "  market: us",
                "  lot_size: 1",
                "  allow_fractional_shares_in_optimizer: true",
                "fees:",
                "  commission_rate: 0.0003",
                "  transfer_fee_rate: 0.0",
                "  stamp_duty_rate: 0.0",
                "slippage:",
                "  k: 0.015",
                "objective_weights:",
                "  risk_term: 1.0",
                "  tracking_error: 1.0",
                "  transaction_cost: 1.0",
                "  target_deviation: 100000.0",
                "  transaction_fee: 1.0",
                "  turnover_penalty: 0.03",
                "  slippage_penalty: 1.0",
                "risk_model:",
                "  enabled: false",
                "solver:",
                "  name: CLARABEL",
                "  max_iters: 5000",
                "  eps: 0.0001",
                "reporting:",
                "  top_weight_changes: 5",
                "  top_findings: 10",
                "simulation:",
                "  mode: impact_aware",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    constraints_path.write_text(
        "\n".join(
            [
                "single_name_max_weight: 0.8",
                "industry_bounds: {}",
                "max_turnover: 1.0",
                "min_order_notional: 1.0",
                "participation_limit: 1.0",
                "cash_non_negative: true",
                "double_ten:",
                "  enabled: false",
                "single_name_guardrail:",
                "  enabled: false",
                "factor_bounds: {}",
                "no_trade_zone:",
                "  enabled: false",
                "severity_policy:",
                "  blocked_trade: BREACH",
                "  unresolved_risk: BREACH",
                "  manager_aggregate: INFO",
                "  remediation_note: INFO",
                "report_labels:",
                "  mandate_type: us_backtest",
                "  audience: pm",
                "  strategy_tag: US_BACKTEST",
                "blocked_trade_policy:",
                "  treat_as_blocking: true",
                "  cleared_if_removed: true",
                "  export_requires_blocking_checks_cleared: true",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    execution_profile_path.write_text(
        "\n".join(
            [
                "urgency: low",
                "slice_ratio: 0.25",
                "max_child_orders: 4",
                "backtest_fixed_half_spread_bps: 5.0",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    manifest_path.write_text(
        "\n".join(
            [
                "name: test_monthly_backtest",
                "description: synthetic monthly backtest fixture",
                f"returns_file: {returns_path}",
                f"market_snapshot: {market_path}",
                f"initial_holdings: {holdings_path}",
                f"target_weights: {target_path}",
                f"reference: {reference_path}",
                f"portfolio_state: {portfolio_state_path}",
                f"config: {config_path}",
                f"constraints: {constraints_path}",
                f"execution_profile: {execution_profile_path}",
                "rebalance:",
                "  frequency: monthly",
                "baselines:",
                "  - naive_pro_rata",
                "  - buy_and_hold",
            ]
        )
        + "\n",
        encoding="utf-8",
    )
    return manifest_path


def _write_risk_enabled_backtest_fixture(tmp_path: Path) -> Path:
    manifest_path = _write_backtest_fixture(tmp_path)
    fixture_dir = manifest_path.parent
    config_path = fixture_dir / "config.yaml"
    returns_path = fixture_dir / "returns_long.csv"
    factor_exposure_path = fixture_dir / "factor_exposure.csv"

    pd.DataFrame(
        [
            {"ticker": "AAA", "factor": "AAA_factor", "exposure": 1.0},
            {"ticker": "AAA", "factor": "BBB_factor", "exposure": 0.0},
            {"ticker": "BBB", "factor": "AAA_factor", "exposure": 0.0},
            {"ticker": "BBB", "factor": "BBB_factor", "exposure": 1.0},
        ]
    ).to_csv(factor_exposure_path, index=False)

    config_payload = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    config_payload["risk_model"] = {
        "enabled": True,
        "integration_mode": "augment",
        "returns_path": str(returns_path.resolve()),
        "factor_exposure_path": str(factor_exposure_path.resolve()),
        "lookback_days": 7,
        "min_history_days": 5,
        "estimator": "sample",
    }
    config_path.write_text(
        yaml.safe_dump(config_payload, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    return manifest_path


def test_load_backtest_manifest_reads_monthly_us_expanded_sample(project_root: Path) -> None:
    manifest = load_backtest_manifest(
        project_root / "data" / "backtest_samples" / "manifest_us_expanded.yaml"
    )

    assert manifest.name == "us_expanded_monthly"
    assert manifest.rebalance.frequency == "monthly"
    assert manifest.market_snapshot.exists()


def test_run_backtest_produces_optimizer_naive_and_buy_hold_nav(tmp_path: Path) -> None:
    manifest_path = _write_backtest_fixture(tmp_path)

    result = run_backtest(manifest_path)

    assert {"optimizer", "naive_pro_rata", "buy_and_hold"} <= set(result.nav_series["strategy"])
    assert result.summary["rebalance_count"] == 2
    assert result.summary["strategies"]["optimizer"]["total_turnover"] >= 0.0
    assert result.summary["strategies"]["buy_and_hold"]["rebalance_count"] == 0
    assert result.summary["strategies"]["optimizer"]["annualized_return"] != 0.0
    assert "optimizer_vs_naive_ending_nav_delta" in result.summary["comparison"]
    assert set(result.period_attribution["strategy"]) == {"optimizer", "naive_pro_rata", "buy_and_hold"}
    assert {
        "period_index",
        "start_date",
        "fill_date",
        "end_date",
        "holding_pnl",
        "active_trading_pnl",
        "trading_cost_pnl",
        "period_pnl",
        "period_return",
        "gross_traded_notional",
        "turnover",
    } <= set(result.period_attribution.columns)
    optimizer_rows = result.period_attribution.loc[result.period_attribution["strategy"] == "optimizer"].copy()
    assert len(optimizer_rows) == 2
    assert (
        optimizer_rows["period_pnl"]
        - optimizer_rows["holding_pnl"]
        - optimizer_rows["active_trading_pnl"]
        - optimizer_rows["trading_cost_pnl"]
    ).abs().max() < 1e-8
    buy_and_hold_rows = result.period_attribution.loc[
        result.period_attribution["strategy"] == "buy_and_hold"
    ].copy()
    assert (buy_and_hold_rows["active_trading_pnl"].abs() < 1e-8).all()
    assert (buy_and_hold_rows["trading_cost_pnl"].abs() < 1e-8).all()
    assert "# Backtest Report" in result.report_markdown


def test_backtest_cli_writes_json_and_nav_series(tmp_path: Path) -> None:
    manifest_path = _write_backtest_fixture(tmp_path)
    output_dir = tmp_path / "backtest_output"
    runner = CliRunner()

    result = runner.invoke(
        backtest_app,
        [
            "--manifest",
            str(manifest_path),
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "backtest_results.json").exists()
    assert (output_dir / "nav_series.csv").exists()
    assert (output_dir / "period_attribution.csv").exists()
    assert (output_dir / "backtest_report.md").exists()
    with (output_dir / "backtest_results.json").open("r", encoding="utf-8") as handle:
        payload = json.load(handle)
    nav_frame = pd.read_csv(output_dir / "nav_series.csv")
    attribution_frame = pd.read_csv(output_dir / "period_attribution.csv")
    report_text = (output_dir / "backtest_report.md").read_text(encoding="utf-8")

    assert payload["summary"]["rebalance_count"] == 2
    assert "comparison" in payload["summary"]
    assert set(nav_frame["strategy"]) == {"optimizer", "naive_pro_rata", "buy_and_hold"}
    assert set(attribution_frame["strategy"]) == {"optimizer", "naive_pro_rata", "buy_and_hold"}
    assert "# Backtest Report" in report_text


def test_run_backtest_cost_sweep_scales_cost_bundle(tmp_path: Path) -> None:
    manifest_path = _write_backtest_fixture(tmp_path)
    output_dir = tmp_path / "sweep_output"

    result = run_backtest_cost_sweep(
        manifest_path=manifest_path,
        output_dir=output_dir,
        cost_bundle_multipliers=[0.5, 1.0],
    )

    assert list(result.summary_frame["cost_bundle_multiplier"]) == [0.5, 1.0]
    assert len(result.run_results) == 2
    first_run = result.run_results[0]
    assert first_run.scaled_objective_weights["transaction_cost"] == 0.5
    assert first_run.scaled_objective_weights["transaction_fee"] == 0.5
    assert first_run.scaled_objective_weights["turnover_penalty"] == 0.015
    assert first_run.scaled_objective_weights["slippage_penalty"] == 0.5
    assert (output_dir / "sweep_summary.csv").exists()
    assert (output_dir / "efficient_frontier_report.md").exists()


def test_backtest_sweep_cli_writes_summary_and_report(tmp_path: Path) -> None:
    manifest_path = _write_backtest_fixture(tmp_path)
    output_dir = tmp_path / "sweep_cli_output"
    runner = CliRunner()

    result = runner.invoke(
        backtest_sweep_app,
        [
            "--manifest",
            str(manifest_path),
            "--output-dir",
            str(output_dir),
            "--cost-bundle-multiplier",
            "0.5",
            "--cost-bundle-multiplier",
            "1.0",
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "sweep_summary.csv").exists()
    assert (output_dir / "efficient_frontier_report.md").exists()
    summary_frame = pd.read_csv(output_dir / "sweep_summary.csv")
    report_text = (output_dir / "efficient_frontier_report.md").read_text(encoding="utf-8")

    assert list(summary_frame["cost_bundle_multiplier"]) == [0.5, 1.0]
    assert "# Cost Bundle Sweep Report" in report_text


def test_run_backtest_cost_sweep_preserves_relative_risk_model_paths(
    project_root: Path,
    tmp_path: Path,
) -> None:
    manifest_path = project_root / "data" / "backtest_samples" / "manifest_us_expanded.yaml"
    output_dir = tmp_path / "risk_path_sweep_output"

    result = run_backtest_cost_sweep(
        manifest_path=manifest_path,
        output_dir=output_dir,
        cost_bundle_multipliers=[1.0],
    )

    scaled_config = yaml.safe_load(result.run_results[0].scaled_config_path.read_text(encoding="utf-8"))

    assert Path(scaled_config["risk_model"]["returns_path"]).is_absolute()
    assert Path(scaled_config["risk_model"]["factor_exposure_path"]).is_absolute()


def test_run_backtest_risk_sweep_scales_risk_term(tmp_path: Path) -> None:
    manifest_path = _write_risk_enabled_backtest_fixture(tmp_path)
    output_dir = tmp_path / "risk_sweep_output"

    result = run_backtest_risk_sweep(
        manifest_path=manifest_path,
        output_dir=output_dir,
        risk_aversion_multipliers=[1.0, 100.0],
    )

    assert list(result.summary_frame["risk_aversion_multiplier"]) == [1.0, 100.0]
    assert len(result.run_results) == 2
    first_run = result.run_results[0]
    second_run = result.run_results[1]
    assert first_run.scaled_objective_weights["risk_term"] == 1.0
    assert second_run.scaled_objective_weights["risk_term"] == 100.0
    assert (output_dir / "risk_sweep_summary.csv").exists()
    assert (output_dir / "risk_aversion_frontier_report.md").exists()
    assert (output_dir / "risk_sweep_manifest.json").exists()


def test_risk_sweep_cli_writes_summary_and_report(tmp_path: Path) -> None:
    manifest_path = _write_risk_enabled_backtest_fixture(tmp_path)
    output_dir = tmp_path / "risk_sweep_cli_output"
    runner = CliRunner()

    result = runner.invoke(
        risk_sweep_app,
        [
            "--manifest",
            str(manifest_path),
            "--output-dir",
            str(output_dir),
            "--risk-aversion-multiplier",
            "1.0",
            "--risk-aversion-multiplier",
            "100.0",
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "risk_sweep_summary.csv").exists()
    assert (output_dir / "risk_aversion_frontier_report.md").exists()
    summary_frame = pd.read_csv(output_dir / "risk_sweep_summary.csv")
    report_text = (output_dir / "risk_aversion_frontier_report.md").read_text(encoding="utf-8")

    assert list(summary_frame["risk_aversion_multiplier"]) == [1.0, 100.0]
    assert "# Risk Aversion Sweep Report" in report_text


def test_run_backtest_risk_sweep_preserves_relative_risk_model_paths(
    project_root: Path,
    tmp_path: Path,
) -> None:
    manifest_path = project_root / "data" / "backtest_samples" / "manifest_us_expanded.yaml"
    output_dir = tmp_path / "risk_path_risk_sweep_output"

    result = run_backtest_risk_sweep(
        manifest_path=manifest_path,
        output_dir=output_dir,
        risk_aversion_multipliers=[1.0],
    )

    scaled_config = yaml.safe_load(result.run_results[0].scaled_config_path.read_text(encoding="utf-8"))

    assert Path(scaled_config["risk_model"]["returns_path"]).is_absolute()
    assert Path(scaled_config["risk_model"]["factor_exposure_path"]).is_absolute()


def test_risk_sweep_summary_frame_contains_volatility_column(tmp_path: Path) -> None:
    manifest_path = _write_risk_enabled_backtest_fixture(tmp_path)
    output_dir = tmp_path / "risk_sweep_vol_output"

    result = run_backtest_risk_sweep(
        manifest_path=manifest_path,
        output_dir=output_dir,
        risk_aversion_multipliers=[1.0, 100.0],
    )

    assert "annualized_volatility" in result.summary_frame.columns
    assert (result.summary_frame["annualized_volatility"] >= 0.0).all()


def test_risk_sweep_does_not_modify_cost_weights(tmp_path: Path) -> None:
    manifest_path = _write_risk_enabled_backtest_fixture(tmp_path)
    output_dir = tmp_path / "risk_sweep_cost_weight_output"

    result = run_backtest_risk_sweep(
        manifest_path=manifest_path,
        output_dir=output_dir,
        risk_aversion_multipliers=[100.0],
    )

    scaled_config = yaml.safe_load(result.run_results[0].scaled_config_path.read_text(encoding="utf-8"))
    objective_weights = scaled_config["objective_weights"]

    assert objective_weights["risk_term"] == 100.0
    assert objective_weights["tracking_error"] == 1.0
    assert objective_weights["transaction_cost"] == 1.0
    assert objective_weights["transaction_fee"] == 1.0
    assert objective_weights["turnover_penalty"] == 0.03
    assert objective_weights["slippage_penalty"] == 1.0
