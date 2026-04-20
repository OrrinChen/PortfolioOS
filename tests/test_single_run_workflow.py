from __future__ import annotations

import json
from pathlib import Path
from types import SimpleNamespace

import pandas as pd
from typer.testing import CliRunner

from portfolio_os.api import cli as cli_module
from portfolio_os.api.cli import app
from portfolio_os.workflow.single_run import run_single_rebalance


def test_run_single_rebalance_preserves_data_quality_findings_in_audit_payload(
    sample_context: dict,
    tmp_path: Path,
) -> None:
    project_root = sample_context["project_root"]
    sample_dir = sample_context["sample_dir"]
    work_dir = tmp_path / "warning_case"
    work_dir.mkdir()

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

    result = run_single_rebalance(
        holdings=holdings_path,
        target=target_path,
        market=market_path,
        reference=reference_path,
        portfolio_state=portfolio_state_path,
        constraints=project_root / "config" / "constraints" / "public_fund.yaml",
        config=project_root / "config" / "default.yaml",
        execution_profile=project_root / "config" / "execution" / "conservative.yaml",
        import_profile=None,
        skip_benchmarks=False,
        run_id="workflow_test",
        created_at="2026-03-30T00:00:00+00:00",
    )

    data_quality_findings = [
        finding for finding in result.audit_payload["findings"] if finding["category"] == "data_quality"
    ]
    assert data_quality_findings
    assert any(finding["code"] == "benchmark_weight_total_anomaly" for finding in data_quality_findings)


def test_run_single_rebalance_supports_import_profile_mapping(project_root: Path) -> None:
    sample_dir = project_root / "data" / "import_profile_samples" / "custodian_style_a"

    result = run_single_rebalance(
        holdings=sample_dir / "holdings.csv",
        target=sample_dir / "target.csv",
        market=sample_dir / "market.csv",
        reference=sample_dir / "reference.csv",
        portfolio_state=project_root / "data" / "sample" / "portfolio_state_example.yaml",
        constraints=project_root / "config" / "constraints" / "public_fund.yaml",
        config=project_root / "config" / "default.yaml",
        execution_profile=project_root / "config" / "execution" / "conservative.yaml",
        import_profile=project_root / "config" / "import_profiles" / "custodian_style_a.yaml",
        skip_benchmarks=False,
        run_id="workflow_import_profile_test",
        created_at="2026-03-30T00:00:00+00:00",
    )

    assert result.audit_payload["inputs"]["import_profile"]["path"]
    assert result.benchmark_payload is not None


def test_main_cli_delegates_to_run_single_rebalance(sample_context: dict, tmp_path: Path, monkeypatch) -> None:
    project_root = sample_context["project_root"]
    sample_dir = sample_context["sample_dir"]
    output_dir = tmp_path / "delegated_run"
    runner = CliRunner()
    captured: dict[str, object] = {}

    def _fake_run_single_rebalance(**kwargs):
        captured.update(kwargs)
        return SimpleNamespace(
            rebalance_run=SimpleNamespace(basket=object(), findings=[]),
            app_config=object(),
            audit_payload={"findings": [], "summary": {}},
            summary_markdown="# summary\n",
            benchmark_payload=None,
            benchmark_markdown=None,
        )

    def _fake_prepare_run_artifacts(_output_dir):
        base = Path(_output_dir)
        base.mkdir(parents=True, exist_ok=True)
        return SimpleNamespace(
            run_id="delegated_run_id",
            created_at="2026-03-30T00:00:00+00:00",
            orders_path=str(base / "orders.csv"),
            orders_oms_path=str(base / "orders_oms.csv"),
            audit_path=str(base / "audit.json"),
            summary_path=str(base / "summary.md"),
            benchmark_json_path=str(base / "benchmark_comparison.json"),
            benchmark_markdown_path=str(base / "benchmark_comparison.md"),
            manifest_path=str(base / "run_manifest.json"),
        )

    monkeypatch.setattr(cli_module, "run_single_rebalance", _fake_run_single_rebalance, raising=False)
    monkeypatch.setattr(cli_module, "prepare_run_artifacts", _fake_prepare_run_artifacts)
    monkeypatch.setattr(
        cli_module,
        "export_basket_csv",
        lambda basket, path: Path(path).write_text("ticker,side,quantity\n", encoding="utf-8"),
    )
    monkeypatch.setattr(
        cli_module,
        "export_basket_oms_csv",
        lambda **kwargs: Path(kwargs["path"]).write_text("ticker,side,quantity\n", encoding="utf-8"),
    )
    monkeypatch.setattr(
        cli_module,
        "write_json",
        lambda path, payload: Path(path).write_text(json.dumps(payload), encoding="utf-8"),
    )
    monkeypatch.setattr(
        cli_module,
        "write_text",
        lambda path, payload: Path(path).write_text(payload, encoding="utf-8"),
    )

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
    assert captured["holdings"] == sample_dir / "holdings_example.csv"
    assert Path(output_dir / "orders.csv").exists()
    assert Path(output_dir / "orders_oms.csv").exists()
    assert Path(output_dir / "audit.json").exists()
    assert Path(output_dir / "summary.md").exists()
