from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pandas as pd
import yaml


def _load_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_risk_ab_comparison.py"
    spec = importlib.util.spec_from_file_location("run_risk_ab_comparison_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_dashboard(path: Path, rows: list[dict[str, str]]) -> None:
    frame = pd.DataFrame(rows)
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)


def test_write_overlay_file_contains_expected_fields(tmp_path: Path) -> None:
    module = _load_module()
    risk_inputs_dir = tmp_path / "risk_inputs"
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text("date,ticker,return\n", encoding="utf-8")
    (risk_inputs_dir / "factor_exposure.csv").write_text("ticker,factor,exposure\n", encoding="utf-8")

    overlay_path = module.write_overlay_file(tmp_path, 0.1, risk_inputs_dir)
    payload = yaml.safe_load(overlay_path.read_text(encoding="utf-8"))

    assert payload["risk_model"]["enabled"] is True
    assert payload["risk_model"]["integration_mode"] == "replace"
    assert payload["risk_model"]["estimator"] == "ledoit_wolf"
    assert payload["objective_weights"]["risk_term"] == 0.1
    assert payload["objective_weights"]["tracking_error"] == 0.0
    assert payload["objective_weights"]["transaction_cost"] == 0.0
    assert payload["risk_model"]["returns_path"].endswith("returns_long.csv")
    assert "market" not in payload
    assert "lot_size" not in payload
    assert "data_provider" not in payload


def test_render_ab_report_contains_summary_values(tmp_path: Path) -> None:
    module = _load_module()
    config = module.ABConfig(
        baseline_dashboard=tmp_path / "baseline.csv",
        start_date="2026-01-30",
        end_date="2026-03-20",
        phase="phase_1",
        market="cn",
        risk_inputs_dir=tmp_path / "risk_inputs",
        w5_values=[0.05],
        cool_down=0.0,
        max_failures=5,
        output_dir=tmp_path / "out",
    )
    baseline_daily = pd.DataFrame(
        [
            {"date": "2026-02-09", "cost": 1.0, "turnover": 0.2, "override": 0.0},
            {"date": "2026-02-10", "cost": 0.9, "turnover": 0.1, "override": 1.0},
        ]
    )
    run_daily = pd.DataFrame(
        [
            {"date": "2026-02-09", "cost": 0.95, "turnover": 0.18, "override": 0.0},
            {"date": "2026-02-10", "cost": 0.92, "turnover": 0.11, "override": 1.0},
        ]
    )
    run_result = module.ReplayRunResult(
        w5_value=0.05,
        status="completed",
        return_code=0,
        tracking_dir=tmp_path / "tracking",
        dashboard_path=tmp_path / "tracking" / "pilot_dashboard.csv",
        progress_status="completed",
        metrics={
            "rows": 2,
            "success_rate": 1.0,
            "override_rate_avg": 0.5,
            "cost_better_ratio_avg": 0.935,
            "turnover_avg": 0.145,
            "solver_time_avg": None,
            "clarabel_convergence_rate": 1.0,
        },
        daily=run_daily,
        stderr="",
    )

    report = module.render_ab_report(
        config=config,
        risk_input_meta={"ticker_count": 45, "actual_trading_days": 243},
        baseline_metrics={
            "rows": 2,
            "success_rate": 1.0,
            "override_rate_avg": 0.5,
            "cost_better_ratio_avg": 0.95,
            "turnover_avg": 0.15,
            "solver_time_avg": None,
            "clarabel_convergence_rate": 1.0,
        },
        baseline_daily=baseline_daily,
        run_results=[run_result],
    )

    assert "# Risk Model A/B Comparison Report" in report
    assert "| Success rate | 100.00% | 100.00% |" in report
    assert "## Daily Detail: risk_term_weight=0.05" in report
    assert "risk_term_weight=0.05: success_rate unchanged" in report


def test_run_risk_ab_continues_when_first_w5_fails(tmp_path: Path) -> None:
    module = _load_module()
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    _write_dashboard(
        baseline_dashboard,
        [
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "solver_primary": "CLARABEL",
                "notes": "historical_replay_2026-02-09",
            },
            {
                "date": "2026-02-10",
                "as_of_date": "2026-02-10",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "1",
                "cost_better_ratio": "0.9",
                "solver_primary": "CLARABEL",
                "notes": "historical_replay_2026-02-10",
            },
        ],
    )
    risk_inputs_dir = tmp_path / "risk_inputs"
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text(
        "date,ticker,return\n2026-01-01,AAA,0.01\n",
        encoding="utf-8",
    )

    config = module.ABConfig(
        baseline_dashboard=baseline_dashboard,
        start_date="2026-02-09",
        end_date="2026-02-10",
        phase="phase_1",
        market="cn",
        risk_inputs_dir=risk_inputs_dir,
        w5_values=[0.05, 0.1],
        cool_down=0.0,
        max_failures=5,
        output_dir=tmp_path / "output",
    )

    replay_commands: list[list[str]] = []

    def _fake_runner(command: list[str], *, cwd: Path):
        _ = cwd
        if command[1].endswith("pilot_ops.py") and command[2] == "init":
            output_dir = Path(command[command.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")
        if command[1].endswith("pilot_historical_replay.py"):
            replay_commands.append(list(command))
            output_dir = Path(command[command.index("--output-dir") + 1])
            overlay_path = Path(command[command.index("--config-overlay") + 1])
            if "0_05" in overlay_path.as_posix():
                return subprocess.CompletedProcess(command, 1, stdout="", stderr="simulated failure")
            _write_dashboard(
                output_dir / "pilot_dashboard.csv",
                [
                    {
                        "date": "2026-02-09",
                        "as_of_date": "2026-02-09",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "override_count": "0",
                        "cost_better_ratio": "1.0",
                        "solver_primary": "CLARABEL",
                        "notes": "historical_replay_2026-02-09",
                    }
                ],
            )
            (output_dir / "replay_progress.json").write_text(
                json.dumps({"status": "completed"}),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    rc = module.run_risk_ab_comparison(config, runner=_fake_runner, logger=lambda _msg: None)

    report_path = config.output_dir / "risk_ab_report.md"
    report_text = report_path.read_text(encoding="utf-8")
    assert rc == 0
    assert len(replay_commands) == 2
    for replay_command in replay_commands:
        assert "--ab-flow" in replay_command
        assert "--require-eligibility-gate" in replay_command
    assert "## Comparison Eligibility" in report_text
    assert "- status: ineligible" in report_text
    assert "## Daily Detail: risk_term_weight=0.05" in report_text
    assert "status: failed" in report_text
    assert "not suitable for strategy decision-making" in report_text
    assert "## Daily Detail: risk_term_weight=0.1" in report_text


def test_run_risk_ab_replay_command_includes_market_us(tmp_path: Path) -> None:
    module = _load_module()
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    _write_dashboard(
        baseline_dashboard,
        [
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "solver_primary": "CLARABEL",
                "notes": "historical_replay_2026-02-09",
            }
        ],
    )
    risk_inputs_dir = tmp_path / "risk_inputs"
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text(
        "date,ticker,return\n2026-01-01,AAPL,0.01\n",
        encoding="utf-8",
    )

    config = module.ABConfig(
        baseline_dashboard=baseline_dashboard,
        start_date="2026-02-09",
        end_date="2026-02-09",
        phase="phase_1",
        market="us",
        risk_inputs_dir=risk_inputs_dir,
        w5_values=[0.1],
        cool_down=0.0,
        max_failures=5,
        output_dir=tmp_path / "output",
    )

    replay_commands: list[list[str]] = []

    def _fake_runner(command: list[str], *, cwd: Path):
        _ = cwd
        if command[1].endswith("pilot_ops.py") and command[2] == "init":
            output_dir = Path(command[command.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")
        if command[1].endswith("pilot_historical_replay.py"):
            replay_commands.append(list(command))
            output_dir = Path(command[command.index("--output-dir") + 1])
            _write_dashboard(
                output_dir / "pilot_dashboard.csv",
                [
                    {
                        "date": "2026-02-09",
                        "as_of_date": "2026-02-09",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "override_count": "0",
                        "cost_better_ratio": "1.0",
                        "solver_primary": "CLARABEL",
                        "notes": "historical_replay_2026-02-09",
                    }
                ],
            )
            (output_dir / "replay_progress.json").write_text(
                json.dumps({"status": "completed"}),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    rc = module.run_risk_ab_comparison(config, runner=_fake_runner, logger=lambda _msg: None)

    assert rc == 0
    assert len(replay_commands) == 1
    replay_command = replay_commands[0]
    assert "--market" in replay_command
    assert replay_command[replay_command.index("--market") + 1] == "us"
    assert "--real-sample" not in replay_command


def test_run_risk_ab_can_skip_replay_eligibility_gate(tmp_path: Path) -> None:
    module = _load_module()
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    _write_dashboard(
        baseline_dashboard,
        [
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "solver_primary": "CLARABEL",
                "notes": "historical_replay_2026-02-09",
            }
        ],
    )
    risk_inputs_dir = tmp_path / "risk_inputs"
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text(
        "date,ticker,return\n2026-01-01,AAA,0.01\n",
        encoding="utf-8",
    )

    config = module.ABConfig(
        baseline_dashboard=baseline_dashboard,
        start_date="2026-02-09",
        end_date="2026-02-09",
        phase="phase_1",
        market="us",
        risk_inputs_dir=risk_inputs_dir,
        w5_values=[0.1],
        cool_down=0.0,
        max_failures=5,
        output_dir=tmp_path / "output",
        replay_require_eligibility_gate=False,
    )
    replay_commands: list[list[str]] = []

    def _fake_runner(command: list[str], *, cwd: Path):
        _ = cwd
        if command[1].endswith("pilot_ops.py") and command[2] == "init":
            output_dir = Path(command[command.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(command, 0, stdout="ok", stderr="")
        if command[1].endswith("pilot_historical_replay.py"):
            replay_commands.append(list(command))
            output_dir = Path(command[command.index("--output-dir") + 1])
            _write_dashboard(
                output_dir / "pilot_dashboard.csv",
                [
                    {
                        "date": "2026-02-09",
                        "as_of_date": "2026-02-09",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "override_count": "0",
                        "cost_better_ratio": "1.0",
                        "solver_primary": "CLARABEL",
                        "notes": "historical_replay_2026-02-09",
                    }
                ],
            )
            (output_dir / "replay_progress.json").write_text(json.dumps({"status": "completed"}), encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    rc = module.run_risk_ab_comparison(config, runner=_fake_runner, logger=lambda _msg: None)

    assert rc == 0
    assert len(replay_commands) == 1
    replay_command = replay_commands[0]
    assert "--ab-flow" in replay_command
    assert "--require-eligibility-gate" not in replay_command


def test_report_warns_on_baseline_duplicate_business_dates(tmp_path: Path) -> None:
    module = _load_module()
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    _write_dashboard(
        baseline_dashboard,
        [
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.00",
                "turnover": "0.12",
                "notes": "historical_replay_2026-02-09",
            },
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "1",
                "cost_better_ratio": "0.98",
                "turnover": "0.13",
                "notes": "historical_replay_2026-02-09",
            },
            {
                "date": "2026-02-10",
                "as_of_date": "2026-02-10",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.01",
                "turnover": "0.10",
                "notes": "historical_replay_2026-02-10",
            },
        ],
    )
    risk_inputs_dir = tmp_path / "risk_inputs"
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text(
        "date,ticker,return\n2026-01-01,AAA,0.01\n",
        encoding="utf-8",
    )
    config = module.ABConfig(
        baseline_dashboard=baseline_dashboard,
        start_date="2026-02-09",
        end_date="2026-02-10",
        phase="phase_1",
        market="cn",
        risk_inputs_dir=risk_inputs_dir,
        w5_values=[0.1],
        cool_down=0.0,
        max_failures=5,
        output_dir=tmp_path / "out",
    )

    def _fake_runner(command: list[str], *, cwd: Path):
        _ = cwd
        if command[1].endswith("pilot_ops.py") and command[2] == "init":
            output_dir = Path(command[command.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command[1].endswith("pilot_historical_replay.py"):
            output_dir = Path(command[command.index("--output-dir") + 1])
            _write_dashboard(
                output_dir / "pilot_dashboard.csv",
                [
                    {
                        "date": "2026-02-09",
                        "as_of_date": "2026-02-09",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "override_count": "0",
                        "cost_better_ratio": "1.00",
                        "turnover": "0.11",
                        "notes": "historical_replay_2026-02-09",
                    },
                    {
                        "date": "2026-02-10",
                        "as_of_date": "2026-02-10",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "override_count": "0",
                        "cost_better_ratio": "1.00",
                        "turnover": "0.09",
                        "notes": "historical_replay_2026-02-10",
                    },
                ],
            )
            (output_dir / "replay_progress.json").write_text(
                json.dumps({"status": "completed"}),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    rc = module.run_risk_ab_comparison(config, runner=_fake_runner, logger=lambda _msg: None)
    report_text = (config.output_dir / "risk_ab_report.md").read_text(encoding="utf-8")
    assert rc == 0
    assert "## Data Quality Warnings" in report_text
    assert "Baseline contains duplicate business dates" in report_text


def test_report_warns_on_baseline_and_variant_date_mismatch(tmp_path: Path) -> None:
    module = _load_module()
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    _write_dashboard(
        baseline_dashboard,
        [
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "turnover": "0.10",
                "notes": "historical_replay_2026-02-09",
            },
            {
                "date": "2026-02-10",
                "as_of_date": "2026-02-10",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "turnover": "0.10",
                "notes": "historical_replay_2026-02-10",
            },
        ],
    )
    risk_inputs_dir = tmp_path / "risk_inputs"
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text(
        "date,ticker,return\n2026-01-01,AAA,0.01\n",
        encoding="utf-8",
    )
    config = module.ABConfig(
        baseline_dashboard=baseline_dashboard,
        start_date="2026-02-09",
        end_date="2026-02-10",
        phase="phase_1",
        market="cn",
        risk_inputs_dir=risk_inputs_dir,
        w5_values=[0.1],
        cool_down=0.0,
        max_failures=5,
        output_dir=tmp_path / "out",
    )

    def _fake_runner(command: list[str], *, cwd: Path):
        _ = cwd
        if command[1].endswith("pilot_ops.py") and command[2] == "init":
            output_dir = Path(command[command.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command[1].endswith("pilot_historical_replay.py"):
            output_dir = Path(command[command.index("--output-dir") + 1])
            _write_dashboard(
                output_dir / "pilot_dashboard.csv",
                [
                    {
                        "date": "2026-02-09",
                        "as_of_date": "2026-02-09",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "override_count": "0",
                        "cost_better_ratio": "1.0",
                        "turnover": "0.10",
                        "notes": "historical_replay_2026-02-09",
                    }
                ],
            )
            (output_dir / "replay_progress.json").write_text(
                json.dumps({"status": "completed"}),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    rc = module.run_risk_ab_comparison(config, runner=_fake_runner, logger=lambda _msg: None)
    report_text = (config.output_dir / "risk_ab_report.md").read_text(encoding="utf-8")
    assert rc == 0
    assert "## Data Quality Warnings" in report_text
    assert "date coverage mismatch vs baseline" in report_text


def test_report_degrades_missing_metrics_to_na_and_warns(tmp_path: Path) -> None:
    module = _load_module()
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    _write_dashboard(
        baseline_dashboard,
        [
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "notes": "historical_replay_2026-02-09",
            }
        ],
    )
    risk_inputs_dir = tmp_path / "risk_inputs"
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text(
        "date,ticker,return\n2026-01-01,AAA,0.01\n",
        encoding="utf-8",
    )
    config = module.ABConfig(
        baseline_dashboard=baseline_dashboard,
        start_date="2026-02-09",
        end_date="2026-02-09",
        phase="phase_1",
        market="cn",
        risk_inputs_dir=risk_inputs_dir,
        w5_values=[0.1],
        cool_down=0.0,
        max_failures=5,
        output_dir=tmp_path / "out",
    )

    def _fake_runner(command: list[str], *, cwd: Path):
        _ = cwd
        if command[1].endswith("pilot_ops.py") and command[2] == "init":
            output_dir = Path(command[command.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command[1].endswith("pilot_historical_replay.py"):
            output_dir = Path(command[command.index("--output-dir") + 1])
            _write_dashboard(
                output_dir / "pilot_dashboard.csv",
                [
                    {
                        "date": "2026-02-09",
                        "as_of_date": "2026-02-09",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "notes": "historical_replay_2026-02-09",
                    }
                ],
            )
            (output_dir / "replay_progress.json").write_text(
                json.dumps({"status": "completed"}),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    rc = module.run_risk_ab_comparison(config, runner=_fake_runner, logger=lambda _msg: None)
    report_text = (config.output_dir / "risk_ab_report.md").read_text(encoding="utf-8")
    assert rc == 0
    assert "## Data Quality Warnings" in report_text
    assert "missing metric families" in report_text
    assert "| Cost better ratio (avg) | N/A | N/A |" in report_text


def test_report_marks_ineligible_on_date_coverage_mismatch(tmp_path: Path) -> None:
    module = _load_module()
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    _write_dashboard(
        baseline_dashboard,
        [
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "turnover": "0.10",
                "notes": "historical_replay_2026-02-09",
            },
            {
                "date": "2026-02-10",
                "as_of_date": "2026-02-10",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "turnover": "0.10",
                "notes": "historical_replay_2026-02-10",
            },
        ],
    )
    risk_inputs_dir = tmp_path / "risk_inputs"
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text(
        "date,ticker,return\n2026-01-01,AAA,0.01\n",
        encoding="utf-8",
    )
    config = module.ABConfig(
        baseline_dashboard=baseline_dashboard,
        start_date="2026-02-09",
        end_date="2026-02-10",
        phase="phase_1",
        market="cn",
        risk_inputs_dir=risk_inputs_dir,
        w5_values=[0.1],
        cool_down=0.0,
        max_failures=5,
        output_dir=tmp_path / "out",
    )

    def _fake_runner(command: list[str], *, cwd: Path):
        _ = cwd
        if command[1].endswith("pilot_ops.py") and command[2] == "init":
            output_dir = Path(command[command.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command[1].endswith("pilot_historical_replay.py"):
            output_dir = Path(command[command.index("--output-dir") + 1])
            _write_dashboard(
                output_dir / "pilot_dashboard.csv",
                [
                    {
                        "date": "2026-02-09",
                        "as_of_date": "2026-02-09",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "override_count": "0",
                        "cost_better_ratio": "1.0",
                        "turnover": "0.10",
                        "notes": "historical_replay_2026-02-09",
                    }
                ],
            )
            (output_dir / "replay_progress.json").write_text(
                json.dumps({"status": "completed"}),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    rc = module.run_risk_ab_comparison(config, runner=_fake_runner, logger=lambda _msg: None)
    report_text = (config.output_dir / "risk_ab_report.md").read_text(encoding="utf-8")
    assert rc == 0
    assert "## Comparison Eligibility" in report_text
    assert "- status: ineligible" in report_text
    assert "date mismatch" in report_text


def test_report_suppresses_directional_conclusion_when_ineligible(tmp_path: Path) -> None:
    module = _load_module()
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    _write_dashboard(
        baseline_dashboard,
        [
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "turnover": "0.10",
                "notes": "historical_replay_2026-02-09",
            },
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "1",
                "cost_better_ratio": "0.9",
                "turnover": "0.11",
                "notes": "historical_replay_2026-02-09",
            },
        ],
    )
    risk_inputs_dir = tmp_path / "risk_inputs"
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text(
        "date,ticker,return\n2026-01-01,AAA,0.01\n",
        encoding="utf-8",
    )
    config = module.ABConfig(
        baseline_dashboard=baseline_dashboard,
        start_date="2026-02-09",
        end_date="2026-02-09",
        phase="phase_1",
        market="cn",
        risk_inputs_dir=risk_inputs_dir,
        w5_values=[0.1],
        cool_down=0.0,
        max_failures=5,
        output_dir=tmp_path / "out",
    )

    def _fake_runner(command: list[str], *, cwd: Path):
        _ = cwd
        if command[1].endswith("pilot_ops.py") and command[2] == "init":
            output_dir = Path(command[command.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command[1].endswith("pilot_historical_replay.py"):
            output_dir = Path(command[command.index("--output-dir") + 1])
            _write_dashboard(
                output_dir / "pilot_dashboard.csv",
                [
                    {
                        "date": "2026-02-09",
                        "as_of_date": "2026-02-09",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "override_count": "0",
                        "cost_better_ratio": "1.0",
                        "turnover": "0.10",
                        "notes": "historical_replay_2026-02-09",
                    }
                ],
            )
            (output_dir / "replay_progress.json").write_text(
                json.dumps({"status": "completed"}),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    rc = module.run_risk_ab_comparison(config, runner=_fake_runner, logger=lambda _msg: None)
    report_text = (config.output_dir / "risk_ab_report.md").read_text(encoding="utf-8")
    assert rc == 0
    assert "not suitable for strategy decision-making" in report_text
    assert " improved " not in report_text
    assert " worsened " not in report_text
    assert " unchanged " not in report_text


def test_duplicate_date_dedup_rule_is_deterministic(tmp_path: Path) -> None:
    module = _load_module()
    dashboard_path = tmp_path / "baseline" / "pilot_dashboard.csv"
    _write_dashboard(
        dashboard_path,
        [
            {
                "date": "2026-02-10",
                "as_of_date": "2026-02-10",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "notes": "historical_replay_2026-02-10",
            },
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "5",
                "cost_better_ratio": "0.8",
                "notes": "historical_replay_2026-02-09",
            },
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "1",
                "cost_better_ratio": "1.2",
                "notes": "historical_replay_2026-02-09",
            },
        ],
    )
    window = module.load_dashboard_window(
        dashboard_path,
        start_date="2026-02-09",
        end_date="2026-02-10",
    )
    assert window["date"].to_list() == ["2026-02-09", "2026-02-10"]
    # 2026-02-09 duplicate rows keep-last (original file order), deterministic.
    row_0209 = window[window["date"] == "2026-02-09"].iloc[0]
    assert str(row_0209["override_count"]) == "1"
    assert str(row_0209["cost_better_ratio"]) == "1.2"


def test_require_eligible_returns_nonzero_when_ineligible(tmp_path: Path) -> None:
    module = _load_module()
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    _write_dashboard(
        baseline_dashboard,
        [
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "turnover": "0.10",
                "notes": "historical_replay_2026-02-09",
            },
            {
                "date": "2026-02-10",
                "as_of_date": "2026-02-10",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "turnover": "0.10",
                "notes": "historical_replay_2026-02-10",
            },
        ],
    )
    risk_inputs_dir = tmp_path / "risk_inputs"
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text(
        "date,ticker,return\n2026-01-01,AAA,0.01\n",
        encoding="utf-8",
    )
    config = module.ABConfig(
        baseline_dashboard=baseline_dashboard,
        start_date="2026-02-09",
        end_date="2026-02-10",
        phase="phase_1",
        market="cn",
        risk_inputs_dir=risk_inputs_dir,
        w5_values=[0.1],
        cool_down=0.0,
        max_failures=5,
        output_dir=tmp_path / "out",
        require_eligible=True,
    )

    def _fake_runner(command: list[str], *, cwd: Path):
        _ = cwd
        if command[1].endswith("pilot_ops.py") and command[2] == "init":
            output_dir = Path(command[command.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command[1].endswith("pilot_historical_replay.py"):
            output_dir = Path(command[command.index("--output-dir") + 1])
            _write_dashboard(
                output_dir / "pilot_dashboard.csv",
                [
                    {
                        "date": "2026-02-09",
                        "as_of_date": "2026-02-09",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "override_count": "0",
                        "cost_better_ratio": "1.0",
                        "turnover": "0.10",
                        "notes": "historical_replay_2026-02-09",
                    }
                ],
            )
            (output_dir / "replay_progress.json").write_text(json.dumps({"status": "completed"}), encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    rc = module.run_risk_ab_comparison(config, runner=_fake_runner, logger=lambda _msg: None)
    assert rc != 0
    assert rc == 2
    assert (config.output_dir / "comparison_eligibility.json").exists()


def test_require_eligible_returns_zero_when_eligible(tmp_path: Path) -> None:
    module = _load_module()
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    _write_dashboard(
        baseline_dashboard,
        [
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "turnover": "0.10",
                "notes": "historical_replay_2026-02-09",
            }
        ],
    )
    risk_inputs_dir = tmp_path / "risk_inputs"
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text(
        "date,ticker,return\n2026-01-01,AAA,0.01\n",
        encoding="utf-8",
    )
    config = module.ABConfig(
        baseline_dashboard=baseline_dashboard,
        start_date="2026-02-09",
        end_date="2026-02-09",
        phase="phase_1",
        market="cn",
        risk_inputs_dir=risk_inputs_dir,
        w5_values=[0.1],
        cool_down=0.0,
        max_failures=5,
        output_dir=tmp_path / "out",
        require_eligible=True,
    )

    def _fake_runner(command: list[str], *, cwd: Path):
        _ = cwd
        if command[1].endswith("pilot_ops.py") and command[2] == "init":
            output_dir = Path(command[command.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command[1].endswith("pilot_historical_replay.py"):
            output_dir = Path(command[command.index("--output-dir") + 1])
            _write_dashboard(
                output_dir / "pilot_dashboard.csv",
                [
                    {
                        "date": "2026-02-09",
                        "as_of_date": "2026-02-09",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "override_count": "1",
                        "cost_better_ratio": "0.9",
                        "turnover": "0.11",
                        "notes": "historical_replay_2026-02-09",
                    }
                ],
            )
            (output_dir / "replay_progress.json").write_text(json.dumps({"status": "completed"}), encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    rc = module.run_risk_ab_comparison(config, runner=_fake_runner, logger=lambda _msg: None)
    assert rc == 0
    payload = json.loads((config.output_dir / "comparison_eligibility.json").read_text(encoding="utf-8"))
    assert payload["eligible"] is True


def test_machine_readable_eligibility_fields_match_report(tmp_path: Path) -> None:
    module = _load_module()
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    _write_dashboard(
        baseline_dashboard,
        [
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "turnover": "0.10",
                "notes": "historical_replay_2026-02-09",
            },
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "1",
                "cost_better_ratio": "0.9",
                "turnover": "0.11",
                "notes": "historical_replay_2026-02-09",
            },
        ],
    )
    risk_inputs_dir = tmp_path / "risk_inputs"
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text(
        "date,ticker,return\n2026-01-01,AAA,0.01\n",
        encoding="utf-8",
    )
    config = module.ABConfig(
        baseline_dashboard=baseline_dashboard,
        start_date="2026-02-09",
        end_date="2026-02-09",
        phase="phase_1",
        market="cn",
        risk_inputs_dir=risk_inputs_dir,
        w5_values=[0.1],
        cool_down=0.0,
        max_failures=5,
        output_dir=tmp_path / "out",
    )

    def _fake_runner(command: list[str], *, cwd: Path):
        _ = cwd
        if command[1].endswith("pilot_ops.py") and command[2] == "init":
            output_dir = Path(command[command.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command[1].endswith("pilot_historical_replay.py"):
            output_dir = Path(command[command.index("--output-dir") + 1])
            _write_dashboard(
                output_dir / "pilot_dashboard.csv",
                [
                    {
                        "date": "2026-02-09",
                        "as_of_date": "2026-02-09",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "override_count": "0",
                        "cost_better_ratio": "1.0",
                        "turnover": "0.10",
                        "notes": "historical_replay_2026-02-09",
                    }
                ],
            )
            (output_dir / "replay_progress.json").write_text(json.dumps({"status": "completed"}), encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    rc = module.run_risk_ab_comparison(config, runner=_fake_runner, logger=lambda _msg: None)
    assert rc == 0
    report_text = (config.output_dir / "risk_ab_report.md").read_text(encoding="utf-8")
    payload = json.loads((config.output_dir / "comparison_eligibility.json").read_text(encoding="utf-8"))
    assert set(payload.keys()) == {"eligible", "reasons", "baseline_quality_flags"}
    assert isinstance(payload["eligible"], bool)
    assert isinstance(payload["reasons"], list)
    assert isinstance(payload["baseline_quality_flags"], list)
    status_text = "eligible" if payload["eligible"] else "ineligible"
    assert f"- status: {status_text}" in report_text
    for reason in payload["reasons"]:
        assert f"- reason: {reason}" in report_text
    for flag in payload["baseline_quality_flags"]:
        assert f"- baseline_quality_flag: {flag}" in report_text


def test_main_parses_require_eligible_flag(tmp_path: Path) -> None:
    module = _load_module()
    captured: dict[str, bool] = {}

    original_run = module.run_risk_ab_comparison

    def _fake_run(config, *, runner=None, logger=None):  # noqa: ANN001
        _ = runner
        _ = logger
        captured["require_eligible"] = bool(config.require_eligible)
        return 0

    module.run_risk_ab_comparison = _fake_run
    try:
        rc = module.main(
            [
                "--baseline-dashboard",
                str(tmp_path / "baseline.csv"),
                "--start-date",
                "2026-02-09",
                "--end-date",
                "2026-02-10",
                "--risk-inputs-dir",
                str(tmp_path / "risk_inputs"),
                "--output-dir",
                str(tmp_path / "out"),
                "--require-eligible",
            ]
        )
    finally:
        module.run_risk_ab_comparison = original_run

    assert rc == 0
    assert captured["require_eligible"] is True


def test_main_parses_real_sample_flag(tmp_path: Path) -> None:
    module = _load_module()
    captured: dict[str, bool] = {}

    original_run = module.run_risk_ab_comparison

    def _fake_run(config, *, runner=None, logger=None):  # noqa: ANN001
        _ = runner
        _ = logger
        captured["real_sample"] = bool(config.real_sample)
        return 0

    module.run_risk_ab_comparison = _fake_run
    try:
        rc = module.main(
            [
                "--baseline-dashboard",
                str(tmp_path / "baseline.csv"),
                "--start-date",
                "2026-02-09",
                "--end-date",
                "2026-02-10",
                "--risk-inputs-dir",
                str(tmp_path / "risk_inputs"),
                "--output-dir",
                str(tmp_path / "out"),
                "--real-sample",
            ]
        )
    finally:
        module.run_risk_ab_comparison = original_run

    assert rc == 0
    assert captured["real_sample"] is True


def test_main_parses_skip_replay_eligibility_gate_flag(tmp_path: Path) -> None:
    module = _load_module()
    captured: dict[str, bool] = {}

    original_run = module.run_risk_ab_comparison

    def _fake_run(config, *, runner=None, logger=None):  # noqa: ANN001
        _ = runner
        _ = logger
        captured["replay_require_eligibility_gate"] = bool(config.replay_require_eligibility_gate)
        return 0

    module.run_risk_ab_comparison = _fake_run
    try:
        rc = module.main(
            [
                "--baseline-dashboard",
                str(tmp_path / "baseline.csv"),
                "--start-date",
                "2026-02-09",
                "--end-date",
                "2026-02-10",
                "--risk-inputs-dir",
                str(tmp_path / "risk_inputs"),
                "--output-dir",
                str(tmp_path / "out"),
                "--skip-replay-eligibility-gate",
            ]
        )
    finally:
        module.run_risk_ab_comparison = original_run

    assert rc == 0
    assert captured["replay_require_eligibility_gate"] is False


def test_main_parses_risk_term_and_fixed_weights(tmp_path: Path) -> None:
    module = _load_module()
    captured: dict[str, object] = {}

    original_run = module.run_risk_ab_comparison

    def _fake_run(config, *, runner=None, logger=None):  # noqa: ANN001
        _ = runner
        _ = logger
        captured["w5_values"] = list(config.w5_values)
        captured["risk_integration_mode"] = str(config.risk_integration_mode)
        captured["tracking_error_weight"] = float(config.tracking_error_weight)
        captured["transaction_cost_weight"] = float(config.transaction_cost_weight)
        return 0

    module.run_risk_ab_comparison = _fake_run
    try:
        rc = module.main(
            [
                "--baseline-dashboard",
                str(tmp_path / "baseline.csv"),
                "--start-date",
                "2026-02-09",
                "--end-date",
                "2026-02-10",
                "--risk-inputs-dir",
                str(tmp_path / "risk_inputs"),
                "--output-dir",
                str(tmp_path / "out"),
                "--risk-term-values",
                "100,300,1000",
                "--risk-integration-mode",
                "augment",
                "--tracking-error-weight",
                "0",
                "--transaction-cost-weight",
                "0",
            ]
        )
    finally:
        module.run_risk_ab_comparison = original_run

    assert rc == 0
    assert captured["w5_values"] == [100.0, 300.0, 1000.0]
    assert captured["risk_integration_mode"] == "augment"
    assert captured["tracking_error_weight"] == 0.0
    assert captured["transaction_cost_weight"] == 0.0


def test_v2_report_and_manifest_are_generated_with_required_sections(tmp_path: Path) -> None:
    module = _load_module()
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    _write_dashboard(
        baseline_dashboard,
        [
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "turnover": "0.10",
                "notes": "historical_replay_2026-02-09",
            },
            {
                "date": "2026-02-10",
                "as_of_date": "2026-02-10",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "1",
                "cost_better_ratio": "0.9",
                "turnover": "0.12",
                "notes": "historical_replay_2026-02-10",
            },
        ],
    )
    risk_inputs_dir = tmp_path / "risk_inputs"
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text(
        "date,ticker,return\n2026-01-01,AAA,0.01\n2026-02-06,AAA,0.02\n",
        encoding="utf-8",
    )
    (risk_inputs_dir / "risk_inputs_manifest.json").write_text(
        json.dumps(
                {
                    "ticker_count": 1,
                    "actual_trading_days": 40,
                    "date_range": ["2026-01-01", "2026-02-06"],
                }
            ),
        encoding="utf-8",
    )
    config = module.ABConfig(
        baseline_dashboard=baseline_dashboard,
        start_date="2026-02-09",
        end_date="2026-02-10",
        phase="phase_1",
        market="cn",
        risk_inputs_dir=risk_inputs_dir,
        w5_values=[0.1],
        cool_down=0.0,
        max_failures=5,
        output_dir=tmp_path / "out",
    )

    def _fake_runner(command: list[str], *, cwd: Path):
        _ = cwd
        if command[1].endswith("pilot_ops.py") and command[2] == "init":
            output_dir = Path(command[command.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command[1].endswith("pilot_historical_replay.py"):
            output_dir = Path(command[command.index("--output-dir") + 1])
            _write_dashboard(
                output_dir / "pilot_dashboard.csv",
                [
                    {
                        "date": "2026-02-09",
                        "as_of_date": "2026-02-09",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "override_count": "0",
                        "cost_better_ratio": "1.0",
                        "turnover": "0.11",
                        "notes": "historical_replay_2026-02-09",
                    },
                    {
                        "date": "2026-02-10",
                        "as_of_date": "2026-02-10",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "override_count": "0",
                        "cost_better_ratio": "1.0",
                        "turnover": "0.11",
                        "notes": "historical_replay_2026-02-10",
                    },
                ],
            )
            (output_dir / "replay_progress.json").write_text(json.dumps({"status": "completed"}), encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    rc = module.run_risk_ab_comparison(config, runner=_fake_runner, logger=lambda _msg: None)
    assert rc == 0

    v2_report_path = config.output_dir / "risk_ab_v2_report.md"
    manifest_path = config.output_dir / "experiment_manifest.json"
    assert v2_report_path.exists()
    assert manifest_path.exists()

    report_text = v2_report_path.read_text(encoding="utf-8")
    assert "## Eligibility & Data Quality Gate" in report_text
    assert "## Paired Daily Delta" in report_text
    assert "## Statistical Confidence" in report_text
    assert "## Decision Readiness" in report_text
    assert "## Recommendation" in report_text

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["experiment_version"].startswith("risk_ab_v2")
    assert manifest["market"] == "cn"
    assert manifest["start_date"] == "2026-02-09"
    assert manifest["end_date"] == "2026-02-10"
    assert manifest["time_alignment"]["aligned"] is True


def test_time_alignment_violation_marks_ineligible_in_v2(tmp_path: Path) -> None:
    module = _load_module()
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    _write_dashboard(
        baseline_dashboard,
        [
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "turnover": "0.10",
                "notes": "historical_replay_2026-02-09",
            }
        ],
    )
    risk_inputs_dir = tmp_path / "risk_inputs"
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text(
        "date,ticker,return\n2026-02-09,AAA,0.01\n",
        encoding="utf-8",
    )
    (risk_inputs_dir / "risk_inputs_manifest.json").write_text(
        json.dumps({"date_range": ["2026-01-01", "2026-02-09"]}),
        encoding="utf-8",
    )
    config = module.ABConfig(
        baseline_dashboard=baseline_dashboard,
        start_date="2026-02-09",
        end_date="2026-02-09",
        phase="phase_1",
        market="cn",
        risk_inputs_dir=risk_inputs_dir,
        w5_values=[0.1],
        cool_down=0.0,
        max_failures=5,
        output_dir=tmp_path / "out",
    )

    def _fake_runner(command: list[str], *, cwd: Path):
        _ = cwd
        if command[1].endswith("pilot_ops.py") and command[2] == "init":
            output_dir = Path(command[command.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command[1].endswith("pilot_historical_replay.py"):
            output_dir = Path(command[command.index("--output-dir") + 1])
            _write_dashboard(
                output_dir / "pilot_dashboard.csv",
                [
                    {
                        "date": "2026-02-09",
                        "as_of_date": "2026-02-09",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "override_count": "0",
                        "cost_better_ratio": "1.0",
                        "turnover": "0.10",
                        "notes": "historical_replay_2026-02-09",
                    }
                ],
            )
            (output_dir / "replay_progress.json").write_text(json.dumps({"status": "completed"}), encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    rc = module.run_risk_ab_comparison(config, runner=_fake_runner, logger=lambda _msg: None)
    assert rc == 0

    eligibility_payload = json.loads((config.output_dir / "comparison_eligibility.json").read_text(encoding="utf-8"))
    assert eligibility_payload["eligible"] is False
    assert any("time alignment" in str(reason).lower() for reason in eligibility_payload["reasons"])

    v2_report_text = (config.output_dir / "risk_ab_v2_report.md").read_text(encoding="utf-8")
    assert "Decision Readiness: Ineligible" in v2_report_text
    assert "not suitable for strategy decision-making" in v2_report_text
    assert " improved " not in v2_report_text
    assert " worsened " not in v2_report_text
    assert " unchanged " not in v2_report_text


def test_v2_report_matches_comparison_eligibility_json(tmp_path: Path) -> None:
    module = _load_module()
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    _write_dashboard(
        baseline_dashboard,
        [
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "turnover": "0.10",
                "notes": "historical_replay_2026-02-09",
            },
            {
                "date": "2026-02-09",
                "as_of_date": "2026-02-09",
                "mode": "nightly",
                "nightly_status": "pass",
                "override_count": "1",
                "cost_better_ratio": "0.9",
                "turnover": "0.11",
                "notes": "historical_replay_2026-02-09",
            },
        ],
    )
    risk_inputs_dir = tmp_path / "risk_inputs"
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text(
        "date,ticker,return\n2026-02-08,AAA,0.01\n",
        encoding="utf-8",
    )
    (risk_inputs_dir / "risk_inputs_manifest.json").write_text(
        json.dumps({"date_range": ["2026-01-01", "2026-02-08"]}),
        encoding="utf-8",
    )
    config = module.ABConfig(
        baseline_dashboard=baseline_dashboard,
        start_date="2026-02-09",
        end_date="2026-02-09",
        phase="phase_1",
        market="cn",
        risk_inputs_dir=risk_inputs_dir,
        w5_values=[0.1],
        cool_down=0.0,
        max_failures=5,
        output_dir=tmp_path / "out",
    )

    def _fake_runner(command: list[str], *, cwd: Path):
        _ = cwd
        if command[1].endswith("pilot_ops.py") and command[2] == "init":
            output_dir = Path(command[command.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        if command[1].endswith("pilot_historical_replay.py"):
            output_dir = Path(command[command.index("--output-dir") + 1])
            _write_dashboard(
                output_dir / "pilot_dashboard.csv",
                [
                    {
                        "date": "2026-02-09",
                        "as_of_date": "2026-02-09",
                        "mode": "nightly",
                        "nightly_status": "pass",
                        "override_count": "0",
                        "cost_better_ratio": "1.0",
                        "turnover": "0.10",
                        "notes": "historical_replay_2026-02-09",
                    }
                ],
            )
            (output_dir / "replay_progress.json").write_text(json.dumps({"status": "completed"}), encoding="utf-8")
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    rc = module.run_risk_ab_comparison(config, runner=_fake_runner, logger=lambda _msg: None)
    assert rc == 0

    payload = json.loads((config.output_dir / "comparison_eligibility.json").read_text(encoding="utf-8"))
    v2_report_text = (config.output_dir / "risk_ab_v2_report.md").read_text(encoding="utf-8")
    decision_text = "Eligible" if payload["eligible"] else "Ineligible"
    assert f"Decision Readiness: {decision_text}" in v2_report_text
    for reason in payload["reasons"]:
        assert f"- reason: {reason}" in v2_report_text


def test_build_daily_tradeoff_frame_exports_required_columns() -> None:
    module = _load_module()
    baseline_daily = pd.DataFrame(
        [
            {
                "date": "2026-02-09",
                "estimated_total_cost_daily": 100.0,
                "target_deviation_improvement_daily": 0.0400,
            },
            {
                "date": "2026-02-10",
                "estimated_total_cost_daily": 110.0,
                "target_deviation_improvement_daily": 0.0410,
            },
        ]
    )
    variant_daily = pd.DataFrame(
        [
            {
                "date": "2026-02-09",
                "estimated_total_cost_daily": 97.0,
                "target_deviation_improvement_daily": 0.0395,
            },
            {
                "date": "2026-02-10",
                "estimated_total_cost_daily": 108.0,
                "target_deviation_improvement_daily": 0.0400,
            },
        ]
    )
    run_results = [
        module.ReplayRunResult(
            w5_value=10000.0,
            status="completed",
            return_code=0,
            tracking_dir=Path("C:/tmp/tracking"),
            dashboard_path=Path("C:/tmp/tracking/pilot_dashboard.csv"),
            progress_status="completed",
            metrics={},
            daily=variant_daily,
            stderr="",
        )
    ]

    tradeoff = module.build_daily_tradeoff_frame(
        baseline_daily=baseline_daily,
        run_results=run_results,
        required_weights=[10000.0],
    )

    assert list(tradeoff.columns) == [
        "date",
        "weight",
        "baseline_estimated_total_cost_daily",
        "variant_estimated_total_cost_daily",
        "delta_cost_daily",
        "baseline_target_deviation_improvement_daily",
        "variant_target_deviation_improvement_daily",
        "delta_target_deviation_daily",
    ]
    first = tradeoff.iloc[0].to_dict()
    assert first["weight"] == 10000.0
    assert first["baseline_estimated_total_cost_daily"] == 100.0
    assert first["variant_estimated_total_cost_daily"] == 97.0
    assert first["delta_cost_daily"] == -3.0
    assert abs(float(first["delta_target_deviation_daily"]) + 0.0005) < 1e-12


def test_evaluate_threshold_policies_selects_best_feasible_weight() -> None:
    module = _load_module()
    core_rows = [
        {
            "group": "baseline",
            "risk_term_weight": 0.0,
            "status": "completed",
            "order_count_total": 1000,
            "gross_traded_notional_total": 1000000.0,
            "estimated_total_cost_total": 10000.0,
            "target_deviation_improvement_mean": 0.0400,
        },
        {
            "group": "variant",
            "risk_term_weight": 10000.0,
            "status": "completed",
            "order_count_total": 980,
            "gross_traded_notional_total": 990000.0,
            "estimated_total_cost_total": 9800.0,
            "estimated_total_cost_total_delta": -200.0,
            "estimated_total_cost_total_delta_pct": -0.02,
            "target_deviation_improvement_mean": 0.0398,
            "target_deviation_improvement_mean_delta": -0.0002,
            "target_deviation_improvement_mean_delta_pct": -0.005,
            "order_count_total_delta": -20.0,
            "order_count_total_delta_pct": -0.02,
            "gross_traded_notional_total_delta": -10000.0,
            "gross_traded_notional_total_delta_pct": -0.01,
        },
        {
            "group": "variant",
            "risk_term_weight": 15000.0,
            "status": "completed",
            "order_count_total": 960,
            "gross_traded_notional_total": 980000.0,
            "estimated_total_cost_total": 9700.0,
            "estimated_total_cost_total_delta": -300.0,
            "estimated_total_cost_total_delta_pct": -0.03,
            "target_deviation_improvement_mean": 0.0390,
            "target_deviation_improvement_mean_delta": -0.0010,
            "target_deviation_improvement_mean_delta_pct": -0.025,
            "order_count_total_delta": -40.0,
            "order_count_total_delta_pct": -0.04,
            "gross_traded_notional_total_delta": -20000.0,
            "gross_traded_notional_total_delta_pct": -0.02,
        },
        {
            "group": "variant",
            "risk_term_weight": 20000.0,
            "status": "completed",
            "order_count_total": 940,
            "gross_traded_notional_total": 970000.0,
            "estimated_total_cost_total": 9600.0,
            "estimated_total_cost_total_delta": -400.0,
            "estimated_total_cost_total_delta_pct": -0.04,
            "target_deviation_improvement_mean": 0.0385,
            "target_deviation_improvement_mean_delta": -0.0015,
            "target_deviation_improvement_mean_delta_pct": -0.0375,
            "order_count_total_delta": -60.0,
            "order_count_total_delta_pct": -0.06,
            "gross_traded_notional_total_delta": -30000.0,
            "gross_traded_notional_total_delta_pct": -0.03,
        },
    ]

    policy_results = module.evaluate_threshold_policies(core_rows)
    by_policy = {item["policy"]: item for item in policy_results}
    assert by_policy["Policy A"]["recommended_risk_term_weight"] == 10000.0
    assert by_policy["Policy B"]["recommended_risk_term_weight"] == 10000.0
    assert by_policy["Policy C"]["recommended_risk_term_weight"] == 15000.0
