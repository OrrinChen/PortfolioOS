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
