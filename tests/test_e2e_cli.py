from __future__ import annotations

import json
from shutil import copytree

import pandas as pd
from typer.testing import CliRunner

from portfolio_os.api.cli import (
    app,
    approval_app,
    build_market_app,
    build_reference_app,
    build_snapshot_app,
    build_target_app,
    execution_app,
    replay_app,
    scenario_app,
)
from portfolio_os.data.providers.mock import MOCK_MARKET_DATA, MOCK_REFERENCE_DATA
from portfolio_os.domain.errors import ProviderPermissionError


def _state_transition_daily_cli_fixture() -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    shares_by_ticker = {
        "000001": 10_000_000.0,
        "000002": 10_000_000.0,
        "000003": 10_000_000.0,
        "000004": 10_000_000.0,
        "000005": 10_000_000.0,
    }
    industry_by_ticker = {
        "000001": "Industrials",
        "000002": "Industrials",
        "000003": "Consumer",
        "000004": "Consumer",
        "000005": "Technology",
    }
    daily_spec = {
        "2026-03-30": {
            "000001": {"close": 6.50, "amount": 7_000_000},
            "000002": {"close": 5.70, "amount": 6_000_000},
            "000003": {"close": 7.50, "amount": 8_000_000},
            "000004": {"close": 8.30, "amount": 9_000_000},
            "000005": {"close": 4.80, "amount": 4_000_000},
        },
        "2026-03-31": {
            "000001": {"close": 6.80, "amount": 7_200_000},
            "000002": {"close": 5.90, "amount": 6_200_000},
            "000003": {"close": 7.70, "amount": 8_200_000},
            "000004": {"close": 8.60, "amount": 9_200_000},
            "000005": {"close": 4.90, "amount": 4_200_000},
        },
        "2026-04-01": {
            "000001": {
                "open": 6.90,
                "high": 7.70,
                "low": 6.85,
                "close": 7.70,
                "upper_limit_price": 7.70,
                "amount": 7_400_000,
            },
            "000002": {
                "open": 5.95,
                "high": 6.20,
                "low": 5.90,
                "close": 6.00,
                "upper_limit_price": 6.49,
                "amount": 6_400_000,
            },
            "000003": {
                "open": 7.75,
                "high": 8.10,
                "low": 7.70,
                "close": 8.00,
                "upper_limit_price": 8.47,
                "amount": 8_400_000,
            },
            "000004": {
                "open": 8.70,
                "high": 9.90,
                "low": 8.60,
                "close": 9.45,
                "upper_limit_price": 9.90,
                "amount": 9_400_000,
            },
            "000005": {
                "open": 4.95,
                "high": 5.10,
                "low": 4.90,
                "close": 5.00,
                "upper_limit_price": 5.39,
                "amount": 4_400_000,
            },
        },
        "2026-04-02": {
            "000001": {"close": 7.90, "amount": 7_500_000},
            "000002": {"close": 6.05, "amount": 6_450_000},
            "000003": {"close": 8.10, "amount": 8_450_000},
            "000004": {"close": 9.10, "amount": 9_450_000},
            "000005": {"close": 5.05, "amount": 4_450_000},
        },
    }
    for date_value, ticker_map in daily_spec.items():
        for ticker, values in ticker_map.items():
            close = float(values["close"])
            rows.append(
                {
                    "date": date_value,
                    "ticker": ticker,
                    "open": float(values.get("open", close * 0.99)),
                    "high": float(values.get("high", close * 1.01)),
                    "low": float(values.get("low", close * 0.98)),
                    "close": close,
                    "volume": 1_000_000,
                    "amount": float(values["amount"]),
                    "upper_limit_price": float(values.get("upper_limit_price", close * 1.10)),
                    "lower_limit_price": float(values.get("lower_limit_price", close * 0.90)),
                    "tradable": "true",
                    "industry": industry_by_ticker[ticker],
                    "issuer_total_shares": shares_by_ticker[ticker],
                }
            )
    return pd.DataFrame(rows)


def test_cli_produces_expected_artifacts(sample_context: dict, tmp_path) -> None:
    runner = CliRunner()
    project_root = sample_context["project_root"]
    sample_dir = sample_context["sample_dir"]
    output_dir = tmp_path / "demo_run"

    result = runner.invoke(
        app,
        [
            "--holdings",
            str(sample_dir / "holdings_example.csv"),
            "--target",
            str(sample_dir / "target_example.csv"),
            "--market",
            str(sample_dir / "market_example.csv"),
            "--reference",
            str(sample_dir / "reference_example.csv"),
            "--portfolio-state",
            str(sample_dir / "portfolio_state_example.yaml"),
            "--constraints",
            str(project_root / "config" / "constraints" / "public_fund.yaml"),
            "--config",
            str(project_root / "config" / "default.yaml"),
            "--execution-profile",
            str(project_root / "config" / "execution" / "conservative.yaml"),
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    orders_path = output_dir / "orders.csv"
    orders_oms_path = output_dir / "orders_oms.csv"
    audit_path = output_dir / "audit.json"
    summary_path = output_dir / "summary.md"
    benchmark_json_path = output_dir / "benchmark_comparison.json"
    benchmark_markdown_path = output_dir / "benchmark_comparison.md"

    assert orders_path.exists()
    assert orders_oms_path.exists()
    assert audit_path.exists()
    assert summary_path.exists()
    assert benchmark_json_path.exists()
    assert benchmark_markdown_path.exists()

    orders = pd.read_csv(orders_path)
    orders_oms = pd.read_csv(orders_oms_path)
    with audit_path.open("r", encoding="utf-8") as handle:
        audit = json.load(handle)
    with benchmark_json_path.open("r", encoding="utf-8") as handle:
        benchmark = json.load(handle)
    summary = summary_path.read_text(encoding="utf-8")
    benchmark_markdown = benchmark_markdown_path.read_text(encoding="utf-8")

    assert not orders.empty
    assert not orders_oms.empty
    assert set(orders["side"]) >= {"BUY", "SELL"}
    assert audit["findings"]
    required_finding_fields = {
        "code",
        "category",
        "severity",
        "ticker",
        "message",
        "rule_source",
        "blocking",
        "repair_status",
        "details",
    }
    assert required_finding_fields.issubset(audit["findings"][0].keys())
    assert "cash_before" in summary
    assert "turnover" in summary
    assert "Findings By Category" in summary
    assert "Findings By Severity" in summary
    assert "Blocked Reason Summary" in summary
    assert "Repair Reason Summary" in summary
    assert "benchmark_summary" in json.dumps(audit["summary"])
    assert {
        "account_id",
        "ticker",
        "side",
        "quantity",
        "price_type",
        "limit_price",
        "estimated_price",
        "estimated_notional",
        "urgency",
        "strategy_tag",
        "basket_id",
        "reason",
        "blocking_checks_cleared",
    }.issubset(set(orders_oms.columns))
    strategy_names = {item["strategy_name"] for item in benchmark["strategies"]}
    assert strategy_names == {
        "naive_target_rebalance",
        "cost_unaware_rebalance",
        "portfolio_os_rebalance",
    }
    assert "Strategy Table" in benchmark_markdown
    assert "PortfolioOS vs Naive" in benchmark_markdown


def test_cli_surfaces_data_quality_warning_in_audit(sample_context: dict, tmp_path) -> None:
    runner = CliRunner()
    project_root = sample_context["project_root"]
    sample_dir = sample_context["sample_dir"]
    work_dir = tmp_path / "warning_case"
    work_dir.mkdir()
    output_dir = work_dir / "output"

    holdings_path = work_dir / "holdings.csv"
    target_path = work_dir / "target.csv"
    market_path = work_dir / "market.csv"
    reference_path = work_dir / "reference.csv"
    portfolio_state_path = work_dir / "portfolio_state.yaml"

    holdings_path.write_text((sample_dir / "holdings_example.csv").read_text(encoding="utf-8"), encoding="utf-8")
    target_path.write_text((sample_dir / "target_example.csv").read_text(encoding="utf-8"), encoding="utf-8")
    market_path.write_text((sample_dir / "market_example.csv").read_text(encoding="utf-8"), encoding="utf-8")
    portfolio_state_path.write_text((sample_dir / "portfolio_state_example.yaml").read_text(encoding="utf-8"), encoding="utf-8")

    reference_frame = pd.read_csv(sample_dir / "reference_example.csv", dtype={"ticker": str})
    reference_frame["benchmark_weight"] = 0.25
    reference_frame.to_csv(reference_path, index=False)

    result = runner.invoke(
        app,
        [
            "--holdings",
            str(holdings_path),
            "--target",
            str(target_path),
            "--market",
            str(market_path),
            "--reference",
            str(reference_path),
            "--portfolio-state",
            str(portfolio_state_path),
            "--constraints",
            str(project_root / "config" / "constraints" / "public_fund.yaml"),
            "--config",
            str(project_root / "config" / "default.yaml"),
            "--execution-profile",
            str(project_root / "config" / "execution" / "conservative.yaml"),
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    with (output_dir / "audit.json").open("r", encoding="utf-8") as handle:
        audit = json.load(handle)
    data_quality_findings = [
        finding for finding in audit["findings"] if finding["category"] == "data_quality"
    ]
    assert data_quality_findings
    assert any(finding["code"] == "benchmark_weight_total_anomaly" for finding in data_quality_findings)


def test_cli_supports_import_profile(project_root, tmp_path) -> None:
    runner = CliRunner()
    sample_dir = project_root / "data" / "import_profile_samples" / "custodian_style_a"
    output_dir = tmp_path / "mapped_demo_run"

    result = runner.invoke(
        app,
        [
            "--holdings",
            str(sample_dir / "holdings.csv"),
            "--target",
            str(sample_dir / "target.csv"),
            "--market",
            str(sample_dir / "market.csv"),
            "--reference",
            str(sample_dir / "reference.csv"),
            "--portfolio-state",
            str(project_root / "data" / "sample" / "portfolio_state_example.yaml"),
            "--constraints",
            str(project_root / "config" / "constraints" / "public_fund.yaml"),
            "--config",
            str(project_root / "config" / "default.yaml"),
            "--execution-profile",
            str(project_root / "config" / "execution" / "conservative.yaml"),
            "--import-profile",
            str(project_root / "config" / "import_profiles" / "custodian_style_a.yaml"),
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    with (output_dir / "audit.json").open("r", encoding="utf-8") as handle:
        audit = json.load(handle)
    with (output_dir / "run_manifest.json").open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    assert audit["inputs"]["import_profile"]["sha256"]
    assert manifest["import_profile"]


def test_paper_calibration_cli_produces_expected_artifacts(tmp_path) -> None:
    runner = CliRunner()
    output_dir = tmp_path / "paper_calibration_cli"

    result = runner.invoke(
        app,
        [
            "paper-calibration",
            "--ticker",
            "SPY",
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "target.csv").exists()
    assert (output_dir / "paper_calibration_manifest.json").exists()
    assert (output_dir / "paper_calibration_payload.json").exists()
    assert (output_dir / "paper_calibration_report.md").exists()


def test_paper_calibration_cli_repeat_uses_separate_run_directories(tmp_path, monkeypatch) -> None:
    runner = CliRunner()
    output_dir = tmp_path / "paper_calibration_repeat_cli"
    called_output_dirs: list[str] = []

    def _fake_dry_run(**kwargs):
        target_dir = kwargs["output_dir"]
        called_output_dirs.append(str(target_dir))
        target_dir.mkdir(parents=True, exist_ok=True)
        for name in [
            "target.csv",
            "paper_calibration_manifest.json",
            "paper_calibration_payload.json",
            "paper_calibration_report.md",
        ]:
            (target_dir / name).write_text("demo", encoding="utf-8")

        class _Result:
            target_path = str(target_dir / "target.csv")
            manifest_path = str(target_dir / "paper_calibration_manifest.json")
            payload_path = str(target_dir / "paper_calibration_payload.json")
            report_path = str(target_dir / "paper_calibration_report.md")

        return _Result()

    monkeypatch.setattr("portfolio_os.api.cli.run_paper_calibration_dry_run", _fake_dry_run)
    monkeypatch.setattr("portfolio_os.api.cli.time.sleep", lambda _seconds: None)

    result = runner.invoke(
        app,
        [
            "paper-calibration",
            "--ticker",
            "SPY",
            "--output-dir",
            str(output_dir),
            "--repeat",
            "3",
            "--interval-seconds",
            "0",
        ],
    )

    assert result.exit_code == 0, result.output
    assert called_output_dirs == [
        str(output_dir / "run_001"),
        str(output_dir / "run_002"),
        str(output_dir / "run_003"),
    ]


def test_paper_calibration_aggregate_cli_produces_summary(tmp_path) -> None:
    runner = CliRunner()
    input_root = tmp_path / "aggregate_inputs"
    output_dir = tmp_path / "aggregate_outputs"

    run_dir = input_root / "paper_calibration_live_2026-04-16" / "run_001"
    run_dir.mkdir(parents=True, exist_ok=True)
    pd.DataFrame(
        [
            {
                "ticker": "SPY",
                "captured_at_utc": "2026-04-16T14:30:00+00:00",
                "latest_trade_price": 500.0,
                "latest_trade_at_utc": "2026-04-16T14:29:59+00:00",
                "bid_price": 499.95,
                "ask_price": 500.05,
                "mid_price": 500.0,
                "spread_bps": 2.0,
                "reference_price": 500.0,
                "reference_price_source": "mid_price",
            }
        ]
    ).to_csv(run_dir / "pretrade_reference_snapshot.csv", index=False)
    pd.DataFrame(
        [
            {
                "sample_id": "run_001",
                "ticker": "SPY",
                "direction": "buy",
                "requested_qty": 1.0,
                "filled_qty": 1.0,
                "avg_fill_price": 500.05,
                "reference_price": 500.0,
                "estimated_price": 500.0,
                "requested_notional": 500.0,
                "filled_notional": 500.05,
                "fill_ratio": 1.0,
                "status": "filled",
                "reject_reason": "",
                "broker_order_id": "order-1",
                "submitted_at_utc": "2026-04-16T14:30:04+00:00",
                "terminal_at_utc": "2026-04-16T14:30:04+00:00",
                "latency_seconds": 0.0,
                "poll_count": 1,
                "timeout_cancelled": False,
                "cancel_requested": False,
                "cancel_acknowledged": False,
                "avg_fill_price_fallback_used": False,
                "status_history": "[]",
            }
        ]
    ).to_csv(run_dir / "alpaca_fill_orders.csv", index=False)

    result = runner.invoke(
        app,
        [
            "paper-calibration-aggregate",
            "--input-root",
            str(input_root),
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "drift_observations.csv").exists()
    assert (output_dir / "drift_summary.md").exists()


def test_promotion_registry_cli_produces_expected_outputs(tmp_path) -> None:
    runner = CliRunner()
    input_root = tmp_path / "promotion_bundles"
    output_dir = tmp_path / "promotion_registry"

    def _write_bundle(bundle_name: str, bundle_id: str, research_line: str) -> None:
        bundle_dir = input_root / bundle_name
        artifacts_dir = bundle_dir / "artifacts"
        artifacts_dir.mkdir(parents=True, exist_ok=True)
        for name in [
            "signal_audit_summary.json",
            "combo_summary.json",
            "MEMORY.md",
            "ledger.md",
        ]:
            target_path = artifacts_dir / name
            if target_path.suffix == ".json":
                target_path.write_text("{}", encoding="utf-8")
            else:
                target_path.write_text("# artifact\n", encoding="utf-8")
        manifest = {
            "contract_type": "portfolio_os_research_promotion_bundle",
            "contract_version": "1.0",
            "bundle_id": bundle_id,
            "created_at": "2026-04-15T12:00:00Z",
            "research_line": research_line,
            "candidate_status": "stage3_candidate_not_promoted",
            "thesis": {
                "summary": f"{research_line} candidate",
                "universe_name": f"{research_line}_dynamic_universe",
            },
            "signals": [
                {
                    "name": "signal_a",
                    "stage_bucket": "partially_real",
                    "audit_summary_path": "artifacts/signal_audit_summary.json",
                }
            ],
            "combo": {
                "summary_path": "artifacts/combo_summary.json",
                "eligible_for_stage4": False,
                "blocking_reason": "needs more stability",
                "full_sample_ir": 0.12,
                "second_half_ir": 0.03,
            },
            "artifacts": {
                "memory_path": "artifacts/MEMORY.md",
                "ledger_path": "artifacts/ledger.md",
            },
        }
        (bundle_dir / "promotion_bundle.json").write_text(
            json.dumps(manifest, indent=2),
            encoding="utf-8",
        )

    _write_bundle("bundle_a", "ashare_bundle", "ashare")
    _write_bundle("bundle_b", "us_bundle", "us")

    result = runner.invoke(
        app,
        [
            "promotion-registry",
            "--input-root",
            str(input_root),
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "promotion_registry.csv").exists()
    assert (output_dir / "promotion_registry_manifest.json").exists()
    assert (output_dir / "promotion_registry_summary.md").exists()


def test_replay_cli_produces_suite_outputs(project_root, replay_manifest_path, tmp_path) -> None:
    runner = CliRunner()
    output_dir = tmp_path / "replay_demo"

    result = runner.invoke(
        replay_app,
        [
            "--manifest",
            str(replay_manifest_path),
            "--constraints",
            str(project_root / "config" / "constraints" / "public_fund.yaml"),
            "--config",
            str(project_root / "config" / "default.yaml"),
            "--execution-profile",
            str(project_root / "config" / "execution" / "conservative.yaml"),
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    suite_results_path = output_dir / "suite_results.json"
    suite_summary_path = output_dir / "suite_summary.md"
    sample_results_dir = output_dir / "sample_results"

    assert suite_results_path.exists()
    assert suite_summary_path.exists()
    assert sample_results_dir.exists()

    with suite_results_path.open("r", encoding="utf-8") as handle:
        suite_results = json.load(handle)
    suite_summary = suite_summary_path.read_text(encoding="utf-8")

    assert suite_results["suite"]["sample_count"] == 5
    assert len(suite_results["samples"]) == 5
    assert "Strategy Overview" in suite_summary
    assert "PortfolioOS vs Naive" in suite_summary
    for sample in suite_results["samples"]:
        sample_name = sample["sample_name"]
        assert (sample_results_dir / sample_name / "benchmark_comparison.json").exists()
        assert (sample_results_dir / sample_name / "benchmark_comparison.md").exists()


def test_scenario_cli_produces_decision_pack(project_root, scenario_manifest_path, tmp_path) -> None:
    runner = CliRunner()
    output_dir = tmp_path / "scenario_demo"

    result = runner.invoke(
        scenario_app,
        [
            "--manifest",
            str(scenario_manifest_path),
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    comparison_json_path = output_dir / "scenario_comparison.json"
    comparison_md_path = output_dir / "scenario_comparison.md"
    decision_pack_path = output_dir / "decision_pack.md"
    scenario_results_dir = output_dir / "scenario_results"

    assert comparison_json_path.exists()
    assert comparison_md_path.exists()
    assert decision_pack_path.exists()
    assert scenario_results_dir.exists()

    with comparison_json_path.open("r", encoding="utf-8") as handle:
        comparison = json.load(handle)
    comparison_md = comparison_md_path.read_text(encoding="utf-8")
    decision_pack = decision_pack_path.read_text(encoding="utf-8")

    assert comparison["labels"]["recommended_scenario"]
    assert comparison["ranking"]
    assert "cross_scenario_explanation" in comparison
    assert "recommendation_diagnostics" in comparison
    assert "Scenario Table" in comparison_md
    assert "Recommended Scenario" in comparison_md
    assert "score_gap_to_second" in comparison_md
    assert "Decision Pack" in decision_pack
    assert "why_not_second_best" in decision_pack
    assert "Trade-Off Versus Named Alternatives" in decision_pack


def test_approval_cli_produces_final_execution_package(scenario_output_dir, tmp_path) -> None:
    runner = CliRunner()
    request_path = tmp_path / "approval_request.yaml"
    output_dir = tmp_path / "approval_demo"
    request_path.write_text(
        "\n".join(
            [
                "name: demo_approval_request",
                "description: Approve a final execution package from scenario analysis",
                f"scenario_output_dir: {scenario_output_dir}",
                "selected_scenario: public_conservative",
                "decision_maker: pm_demo",
                "decision_role: portfolio_manager",
                "rationale: Choose the conservative public scenario as the baseline executable package.",
                "acknowledged_warning_codes: []",
                "handoff:",
                "  trader: trader_demo",
                "  reviewer: risk_demo",
                "  compliance_contact: compliance_demo",
                "tags:",
                "  - demo",
                "  - approval",
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        approval_app,
        [
            "--request",
            str(request_path),
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "approval_record.json").exists()
    assert (output_dir / "approval_summary.md").exists()
    assert (output_dir / "freeze_manifest.json").exists()
    assert (output_dir / "handoff_checklist.md").exists()
    assert (output_dir / "final_orders.csv").exists()
    assert (output_dir / "final_orders_oms.csv").exists()
    assert (output_dir / "final_audit.json").exists()
    assert (output_dir / "final_summary.md").exists()


def test_approval_template_cli_generates_draft_request(scenario_output_dir, tmp_path) -> None:
    runner = CliRunner()
    output_path = tmp_path / "approval_request_template.yaml"

    result = runner.invoke(
        approval_app,
        [
            "template",
            "--scenario-output-dir",
            str(scenario_output_dir),
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert output_path.exists()
    payload = output_path.read_text(encoding="utf-8")
    assert "acknowledged_warning_codes:" in payload
    assert "override_reason_code:" in payload


def test_approval_cli_supports_controlled_override_on_blocking_findings(scenario_output_dir, tmp_path) -> None:
    runner = CliRunner()
    scenario_copy = tmp_path / "scenario_copy_override"
    copytree(scenario_output_dir, scenario_copy)
    audit_path = scenario_copy / "scenario_results" / "public_conservative" / "audit.json"
    with audit_path.open("r", encoding="utf-8") as handle:
        audit_payload = json.load(handle)
    audit_payload["findings"].append(
        {
            "code": "synthetic_blocking_cli_test",
            "category": "risk",
            "severity": "BREACH",
            "ticker": "600519",
            "message": "Synthetic blocking finding for CLI override test.",
            "rule_source": "test",
            "blocking": True,
            "repair_status": "unresolved",
            "details": {},
        }
    )
    with audit_path.open("w", encoding="utf-8") as handle:
        json.dump(audit_payload, handle, indent=2, ensure_ascii=False)

    request_path = tmp_path / "approval_request_override.yaml"
    output_dir = tmp_path / "approval_override_demo"
    request_path.write_text(
        "\n".join(
            [
                "name: demo_approval_request_override",
                "description: Approve with controlled override",
                f"scenario_output_dir: {scenario_copy}",
                "selected_scenario: public_conservative",
                "decision_maker: pm_demo",
                "decision_role: portfolio_manager",
                "rationale: Controlled override for pilot continuity.",
                "acknowledged_warning_codes: []",
                "override:",
                "  enabled: true",
                "  reason: blocking findings accepted under controlled pilot override",
                "  override_reason_code: workflow_continuity",
                "  approver: risk_head_demo",
                "  approved_at: 2026-03-24T09:30:00+00:00",
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        approval_app,
        [
            "--request",
            str(request_path),
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    with (output_dir / "approval_record.json").open("r", encoding="utf-8") as handle:
        approval_record = json.load(handle)
    assert approval_record["approval_status"] == "approved_with_override"
    assert approval_record["override_used"] is True
    assert approval_record["override"]["enabled"] is True
    assert (output_dir / "final_orders.csv").exists()
    assert (output_dir / "final_orders_oms.csv").exists()
    assert (output_dir / "final_audit.json").exists()
    assert (output_dir / "final_summary.md").exists()


def test_execution_cli_produces_execution_outputs(project_root, approval_output_dir, tmp_path) -> None:
    runner = CliRunner()
    request_path = tmp_path / "execution_request.yaml"
    output_dir = tmp_path / "execution_demo"
    request_path.write_text(
        "\n".join(
            [
                "name: demo_execution_request",
                "description: Simulate intraday execution for a frozen final basket",
                f"artifact_dir: {approval_output_dir}",
                "input_orders: final_orders_oms.csv",
                "simulation:",
                "  mode: participation_twap",
                "  bucket_count: 5",
            ]
        ),
        encoding="utf-8",
    )

    result = runner.invoke(
        execution_app,
        [
            "--request",
            str(request_path),
            "--calibration-profile",
            str(project_root / "config" / "calibration_profiles" / "balanced_day.yaml"),
            "--output-dir",
            str(output_dir),
        ],
    )

    assert result.exit_code == 0, result.output
    execution_report_json_path = output_dir / "execution_report.json"
    execution_report_md_path = output_dir / "execution_report.md"
    execution_fills_path = output_dir / "execution_fills.csv"
    execution_child_orders_path = output_dir / "execution_child_orders.csv"
    handoff_checklist_path = output_dir / "handoff_checklist.md"

    assert execution_report_json_path.exists()
    assert execution_report_md_path.exists()
    assert execution_fills_path.exists()
    assert execution_child_orders_path.exists()
    assert handoff_checklist_path.exists()

    with execution_report_json_path.open("r", encoding="utf-8") as handle:
        report = json.load(handle)
    report_md = execution_report_md_path.read_text(encoding="utf-8")
    fills_frame = pd.read_csv(execution_fills_path)

    assert report["portfolio_summary"]["fill_rate"] >= 0.0
    assert report["source_artifacts"]["audit"]["sha256"]
    assert report["bucket_curve"]["bucket_count"] == 5
    assert report["resolved_calibration"]["selected_profile"]["source"] == "cli"
    assert report["stress_test"]["enabled"] is True
    assert "Execution Summary" in report_md
    assert "Calibration" in report_md
    assert "Stress Comparison" in report_md
    assert "Worst 3 Orders" in report_md
    assert not fills_frame.empty


def test_builder_clis_run_and_outputs_feed_main_cli(project_root, tmp_path) -> None:
    runner = CliRunner()
    generated_dir = tmp_path / "generated"
    generated_dir.mkdir()
    market_output = generated_dir / "market.csv"
    reference_output = generated_dir / "reference.csv"
    target_output = generated_dir / "target.csv"

    market_result = runner.invoke(
        build_market_app,
        [
            "--tickers-file",
            str(project_root / "data" / "sample" / "tickers.txt"),
            "--as-of-date",
            "2026-03-23",
            "--provider",
            "mock",
            "--output",
            str(market_output),
        ],
    )
    reference_result = runner.invoke(
        build_reference_app,
        [
            "--tickers-file",
            str(project_root / "data" / "sample" / "tickers.txt"),
            "--as-of-date",
            "2026-03-23",
            "--provider",
            "mock",
            "--overlay",
            str(project_root / "data" / "sample" / "reference_overlay_example.csv"),
            "--output",
            str(reference_output),
        ],
    )
    target_result = runner.invoke(
        build_target_app,
        [
            "--index-code",
            "000300.SH",
            "--as-of-date",
            "2026-03-23",
            "--provider",
            "mock",
            "--output",
            str(target_output),
        ],
    )

    assert market_result.exit_code == 0, market_result.output
    assert reference_result.exit_code == 0, reference_result.output
    assert target_result.exit_code == 0, target_result.output
    assert market_output.exists()
    assert reference_output.exists()
    assert target_output.exists()
    assert (generated_dir / "target_manifest.json").exists()

    main_output_dir = tmp_path / "main_from_generated"
    main_result = runner.invoke(
        app,
        [
            "--holdings",
            str(project_root / "data" / "sample" / "holdings_example.csv"),
            "--target",
            str(target_output),
            "--market",
            str(market_output),
            "--reference",
            str(reference_output),
            "--portfolio-state",
            str(project_root / "data" / "sample" / "portfolio_state_example.yaml"),
            "--constraints",
            str(project_root / "config" / "constraints" / "public_fund.yaml"),
            "--config",
            str(project_root / "config" / "default.yaml"),
            "--execution-profile",
            str(project_root / "config" / "execution" / "conservative.yaml"),
            "--output-dir",
            str(main_output_dir),
            "--skip-benchmarks",
        ],
    )

    assert main_result.exit_code == 0, main_result.output
    assert (main_output_dir / "orders.csv").exists()
    assert (main_output_dir / "orders_oms.csv").exists()
    assert (main_output_dir / "audit.json").exists()


def test_snapshot_builder_cli_runs_and_snapshot_outputs_feed_main_cli(project_root, tmp_path) -> None:
    runner = CliRunner()
    snapshot_dir = tmp_path / "snapshot_bundle"
    snapshot_result = runner.invoke(
        build_snapshot_app,
        [
            "--tickers-file",
            str(project_root / "data" / "sample" / "tickers.txt"),
            "--index-code",
            "000300.SH",
            "--as-of-date",
            "2026-03-23",
            "--provider",
            "mock",
            "--reference-overlay",
            str(project_root / "data" / "sample" / "reference_overlay_example.csv"),
            "--output-dir",
            str(snapshot_dir),
        ],
    )

    assert snapshot_result.exit_code == 0, snapshot_result.output
    assert (snapshot_dir / "market.csv").exists()
    assert (snapshot_dir / "reference.csv").exists()
    assert (snapshot_dir / "target.csv").exists()
    assert (snapshot_dir / "market_manifest.json").exists()
    assert (snapshot_dir / "reference_manifest.json").exists()
    assert (snapshot_dir / "target_manifest.json").exists()
    assert (snapshot_dir / "snapshot_manifest.json").exists()

    main_output_dir = tmp_path / "main_from_snapshot_bundle"
    main_result = runner.invoke(
        app,
        [
            "--holdings",
            str(project_root / "data" / "sample" / "holdings_example.csv"),
            "--target",
            str(snapshot_dir / "target.csv"),
            "--market",
            str(snapshot_dir / "market.csv"),
            "--reference",
            str(snapshot_dir / "reference.csv"),
            "--portfolio-state",
            str(project_root / "data" / "sample" / "portfolio_state_example.yaml"),
            "--constraints",
            str(project_root / "config" / "constraints" / "public_fund.yaml"),
            "--config",
            str(project_root / "config" / "default.yaml"),
            "--execution-profile",
            str(project_root / "config" / "execution" / "conservative.yaml"),
            "--output-dir",
            str(main_output_dir),
            "--skip-benchmarks",
        ],
    )

    assert main_result.exit_code == 0, main_result.output
    assert (main_output_dir / "orders.csv").exists()


def test_snapshot_builder_cli_surfaces_alternative_path_on_permission_failure(project_root, tmp_path, monkeypatch) -> None:
    class PermissionLimitedProvider:
        provider_name = "tushare"
        provider_metadata = {
            "provider_token_source": "cli",
            "approximation_notes": {"market": [], "reference": [], "target": []},
        }

        def get_daily_market_snapshot(self, tickers, as_of_date):
            _ = as_of_date
            return [MOCK_MARKET_DATA[ticker] for ticker in tickers]

        def get_reference_snapshot(self, tickers, as_of_date):
            _ = as_of_date
            return [MOCK_REFERENCE_DATA[ticker] for ticker in tickers]

        def get_index_weights(self, index_code, as_of_date):
            _ = index_code
            _ = as_of_date
            raise ProviderPermissionError("index_weight permission denied")

        def get_capability_report(self, feed_name):
            if feed_name == "target":
                return {
                    "provider_capability_status": "unavailable",
                    "fallback_notes": ["index_weight_permission_missing"],
                    "permission_notes": ["index_weight_permission_missing"],
                    "recommended_alternative_path": "provide_target_csv_and_continue",
                }
            return {
                "provider_capability_status": "available",
                "fallback_notes": [],
                "permission_notes": [],
                "recommended_alternative_path": None,
            }

    monkeypatch.setattr(
        "portfolio_os.api.cli.get_data_provider",
        lambda name, provider_token=None: PermissionLimitedProvider(),
    )

    runner = CliRunner()
    snapshot_dir = tmp_path / "snapshot_permission_failure"
    result = runner.invoke(
        build_snapshot_app,
        [
            "--tickers-file",
            str(project_root / "data" / "sample" / "tickers.txt"),
            "--index-code",
            "000300.SH",
            "--as-of-date",
            "2026-03-23",
            "--provider",
            "tushare",
            "--provider-token",
            "demo_token",
            "--reference-overlay",
            str(project_root / "data" / "sample" / "reference_overlay_example.csv"),
            "--output-dir",
            str(snapshot_dir),
        ],
    )

    assert result.exit_code == 1
    assert "Provide target.csv from the client side and continue." in result.output
    assert (snapshot_dir / "market.csv").exists()
    assert (snapshot_dir / "reference.csv").exists()
    assert (snapshot_dir / "target_manifest.json").exists()
    with (snapshot_dir / "snapshot_manifest.json").open("r", encoding="utf-8") as handle:
        manifest = json.load(handle)
    assert manifest["steps"]["target"]["build_status"] == "failed_permission"


def test_builder_generated_inputs_can_run_sample_02_without_optimizer_infeasible(project_root, tmp_path) -> None:
    runner = CliRunner()
    replay_sample_dir = project_root / "data" / "replay_samples" / "sample_02"
    generated_dir = tmp_path / "generated_sample_02"
    generated_dir.mkdir(parents=True, exist_ok=True)

    tickers_path = generated_dir / "tickers.txt"
    tickers = pd.concat(
        [
            pd.read_csv(replay_sample_dir / "holdings.csv", dtype={"ticker": str})["ticker"],
            pd.read_csv(replay_sample_dir / "target.csv", dtype={"ticker": str})["ticker"],
        ]
    ).dropna().astype(str).drop_duplicates()
    tickers_path.write_text("\n".join(sorted(tickers.tolist())) + "\n", encoding="utf-8")

    market_output = generated_dir / "market.csv"
    reference_output = generated_dir / "reference.csv"
    build_market_result = runner.invoke(
        build_market_app,
        [
            "--tickers-file",
            str(tickers_path),
            "--as-of-date",
            "2026-03-19",
            "--provider",
            "mock",
            "--output",
            str(market_output),
        ],
    )
    build_reference_result = runner.invoke(
        build_reference_app,
        [
            "--tickers-file",
            str(tickers_path),
            "--as-of-date",
            "2026-03-19",
            "--provider",
            "mock",
            "--overlay",
            str(replay_sample_dir / "reference.csv"),
            "--output",
            str(reference_output),
        ],
    )
    assert build_market_result.exit_code == 0, build_market_result.output
    assert build_reference_result.exit_code == 0, build_reference_result.output

    output_dir = tmp_path / "sample_02_main"
    main_result = runner.invoke(
        app,
        [
            "--holdings",
            str(replay_sample_dir / "holdings.csv"),
            "--target",
            str(replay_sample_dir / "target.csv"),
            "--market",
            str(market_output),
            "--reference",
            str(reference_output),
            "--portfolio-state",
            str(replay_sample_dir / "portfolio_state.yaml"),
            "--constraints",
            str(project_root / "config" / "constraints" / "public_fund.yaml"),
            "--config",
            str(project_root / "config" / "default.yaml"),
            "--execution-profile",
            str(project_root / "config" / "execution" / "conservative.yaml"),
            "--output-dir",
            str(output_dir),
        ],
    )

    assert main_result.exit_code == 0, main_result.output
    with (output_dir / "audit.json").open("r", encoding="utf-8") as handle:
        audit_payload = json.load(handle)
    finding_codes = {finding["code"] for finding in audit_payload["findings"]}
    assert "locked_single_name_above_limit" in finding_codes
    assert (output_dir / "orders.csv").exists()


def test_state_transition_pilot_cli_writes_expected_artifacts(tmp_path) -> None:
    runner = CliRunner()
    daily_path = tmp_path / "state_transition_daily.csv"
    output_dir = tmp_path / "state_transition_pilot_cli"
    _state_transition_daily_cli_fixture().to_csv(daily_path, index=False)

    result = runner.invoke(
        app,
        [
            "state-transition-pilot",
            "--daily-panel",
            str(daily_path),
            "--output-dir",
            str(output_dir),
            "--lookback-days",
            "2",
            "--null-seed",
            "7",
            "--null-seed",
            "8",
            "--null-seed",
            "9",
        ],
    )

    assert result.exit_code == 0, result.output
    assert (output_dir / "expression_frame.csv").exists()
    assert (output_dir / "control_comparison.csv").exists()
    assert (output_dir / "placebo_comparison.csv").exists()
    assert (output_dir / "null_pool.csv").exists()
    assert (output_dir / "null_summary.csv").exists()
    assert (output_dir / "pilot_read_frame.csv").exists()
    assert (output_dir / "summary.json").exists()
    assert (output_dir / "note.md").exists()
    assert "summary.json" in result.output


def test_build_state_transition_panel_cli_writes_expected_artifacts(tmp_path, monkeypatch) -> None:
    class _FakeStateTransitionProvider:
        provider_name = "fake_state_transition"
        provider_metadata = {
            "provider_token_source": "cli",
            "approximation_notes": {
                "state_transition_daily_panel": [
                    "industry and issuer_total_shares are treated as static end-date reference fields."
                ]
            },
        }

        def get_state_transition_daily_panel(self, tickers, start_date, end_date):
            _ = (tickers, start_date, end_date)
            return pd.DataFrame(
                [
                    {
                        "date": "2026-04-01",
                        "ticker": "000001",
                        "open": 10.0,
                        "high": 11.0,
                        "low": 9.9,
                        "close": 11.0,
                        "volume": 1_000_000.0,
                        "amount": 10_500_000.0,
                        "upper_limit_price": 11.0,
                        "lower_limit_price": 9.0,
                        "tradable": True,
                        "industry": "Industrials",
                        "issuer_total_shares": 10_000_000.0,
                    }
                ]
            )

        def get_capability_report(self, feed_name: str):
            _ = feed_name
            return {
                "provider_capability_status": "available",
                "fallback_notes": [],
                "fallback_chain_used": [],
                "data_source_mix": ["fake_state_transition"],
                "permission_notes": [],
                "recommended_alternative_path": None,
            }

    monkeypatch.setattr(
        "portfolio_os.api.cli.get_data_provider",
        lambda name, provider_token=None: _FakeStateTransitionProvider(),
    )

    runner = CliRunner()
    tickers_path = tmp_path / "tickers.txt"
    output_path = tmp_path / "state_transition_daily_panel.csv"
    tickers_path.write_text("000001\n", encoding="utf-8")

    result = runner.invoke(
        app,
        [
            "build-state-transition-panel",
            "--tickers-file",
            str(tickers_path),
            "--start-date",
            "2026-04-01",
            "--end-date",
            "2026-04-02",
            "--provider",
            "tushare",
            "--output",
            str(output_path),
        ],
    )

    assert result.exit_code == 0, result.output
    assert output_path.exists()
    assert (tmp_path / "state_transition_daily_panel_manifest.json").exists()
    assert "state_transition_daily_panel.csv" in result.output
