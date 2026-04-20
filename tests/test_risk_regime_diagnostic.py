from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import subprocess
import sys

import pandas as pd
import yaml


def _load_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_risk_regime_diagnostic.py"
    spec = importlib.util.spec_from_file_location("run_risk_regime_diagnostic_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_baseline_dashboard(path: Path, *, date: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    pd.DataFrame([{"date": date, "run_root": str(path.parent / "baseline_run")}]).to_csv(path, index=False)


def _write_sample_audit(
    *,
    path: Path,
    mode: str,
    component_payload: dict[str, dict[str, float]],
    gross_traded_notional: float,
    estimated_total_cost: float,
    order_count: int,
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "summary": {
            "gross_traded_notional": gross_traded_notional,
            "estimated_total_cost": estimated_total_cost,
        },
        "orders": [{"ticker": f"T{i}", "quantity": 1} for i in range(order_count)],
        "optimization_metadata": {
            "objective_decomposition": {
                "mode": mode,
                "objective_value": 1.0,
                "abs_weighted_sum": 1.0,
                "components": component_payload,
            }
        },
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _build_runner(date: str):
    def _fake_runner(command: list[str], *, cwd: Path):
        _ = cwd
        if command[1].endswith("pilot_ops.py") and command[2] == "init":
            output_dir = Path(command[command.index("--output-dir") + 1])
            output_dir.mkdir(parents=True, exist_ok=True)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        if command[1].endswith("pilot_historical_replay.py"):
            output_dir = Path(command[command.index("--output-dir") + 1])
            overlay_path = Path(command[command.index("--config-overlay") + 1])
            overlay = yaml.safe_load(overlay_path.read_text(encoding="utf-8"))
            enabled = bool(overlay["risk_model"]["enabled"])
            risk_term = float(overlay["objective_weights"]["risk_term"])

            cell_label = f"{'on' if enabled else 'off'}_{str(risk_term).replace('.', '_')}"
            run_root = output_dir / f"run_{cell_label}"
            sample_ids = ["sample_us_01", "sample_us_02"]
            for idx, sample_id in enumerate(sample_ids):
                audit_path = run_root / "samples" / sample_id / "main" / "audit.json"
                if enabled:
                    components = {
                        "risk_term": {
                            "raw_value": 100.0 + idx,
                            "weight": risk_term,
                            "weighted_value": (100.0 + idx) * risk_term,
                            "share_abs_weighted": 0.98,
                        },
                        "tracking_error": {
                            "raw_value": 1.0,
                            "weight": 1.0,
                            "weighted_value": 1.0,
                            "share_abs_weighted": 0.01,
                        },
                        "transaction_cost": {
                            "raw_value": 1.0,
                            "weight": 1.0,
                            "weighted_value": 1.0,
                            "share_abs_weighted": 0.01,
                        },
                    }
                    mode = "risk"
                else:
                    components = {
                        "target_deviation": {
                            "raw_value": 2.0,
                            "weight": 1.0,
                            "weighted_value": 2.0,
                            "share_abs_weighted": 0.4,
                        },
                        "transaction_fee": {
                            "raw_value": 1.0,
                            "weight": 1.0,
                            "weighted_value": 1.0,
                            "share_abs_weighted": 0.2,
                        },
                        "turnover_penalty": {
                            "raw_value": 1.0,
                            "weight": 1.0,
                            "weighted_value": 1.0,
                            "share_abs_weighted": 0.2,
                        },
                        "slippage_penalty": {
                            "raw_value": 1.0,
                            "weight": 1.0,
                            "weighted_value": 1.0,
                            "share_abs_weighted": 0.2,
                        },
                    }
                    mode = "legacy"
                _write_sample_audit(
                    path=audit_path,
                    mode=mode,
                    component_payload=components,
                    gross_traded_notional=1000.0 + idx,
                    estimated_total_cost=10.0 + idx,
                    order_count=1 if enabled else 0,
                )

            pd.DataFrame([{"date": date, "run_root": str(run_root)}]).to_csv(
                output_dir / "pilot_dashboard.csv",
                index=False,
            )
            (output_dir / "replay_progress.json").write_text(
                json.dumps({"status": "completed"}),
                encoding="utf-8",
            )
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

        return subprocess.CompletedProcess(command, 0, stdout="", stderr="")

    return _fake_runner


def test_risk_regime_diagnostic_outputs_and_2x4_grid(tmp_path: Path) -> None:
    module = _load_module()
    date = "2026-03-20"
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    risk_inputs_dir = tmp_path / "risk_inputs"
    output_dir = tmp_path / "diag_out"
    _write_baseline_dashboard(baseline_dashboard, date=date)
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text("date,ticker,return\n", encoding="utf-8")

    config = module.DiagnosticConfig(
        baseline_dashboard=baseline_dashboard,
        date=date,
        phase="phase_1",
        market="us",
        risk_inputs_dir=risk_inputs_dir,
        risk_term_values=[0.001, 0.01, 0.1, 1.0],
        tracking_error_weight=1.0,
        output_dir=output_dir,
        real_sample=False,
        skip_replay_eligibility_gate=True,
        include_risk_term_zero_anchor=False,
    )

    rc = module.run_risk_regime_diagnostic(config, runner=_build_runner(date), logger=lambda _msg: None)
    assert rc == 0

    csv_path = output_dir / "diagnostic_matrix.csv"
    json_path = output_dir / "diagnostic_matrix.json"
    md_path = output_dir / "diagnostic_report.md"
    manifest_path = output_dir / "diagnostic_manifest.json"
    assert csv_path.exists()
    assert json_path.exists()
    assert md_path.exists()
    assert manifest_path.exists()

    frame = pd.read_csv(csv_path, encoding="utf-8-sig")
    assert len(frame) == 8
    required_columns = {
        "cell_id",
        "risk_model_enabled",
        "risk_term_weight",
        "order_count_total",
        "gross_traded_notional_total",
        "estimated_total_cost_total",
        "risk_term_share_abs_weighted_mean",
    }
    assert required_columns.issubset(set(frame.columns))

    payload = json.loads(json_path.read_text(encoding="utf-8"))
    assert len(payload["rows"]) == 8


def test_risk_regime_diagnostic_anchor_adds_one_cell(tmp_path: Path) -> None:
    module = _load_module()
    date = "2026-03-20"
    baseline_dashboard = tmp_path / "baseline" / "pilot_dashboard.csv"
    risk_inputs_dir = tmp_path / "risk_inputs"
    output_dir = tmp_path / "diag_out"
    _write_baseline_dashboard(baseline_dashboard, date=date)
    risk_inputs_dir.mkdir(parents=True, exist_ok=True)
    (risk_inputs_dir / "returns_long.csv").write_text("date,ticker,return\n", encoding="utf-8")

    config = module.DiagnosticConfig(
        baseline_dashboard=baseline_dashboard,
        date=date,
        phase="phase_1",
        market="us",
        risk_inputs_dir=risk_inputs_dir,
        risk_term_values=[0.001, 0.01, 0.1, 1.0],
        tracking_error_weight=1.0,
        output_dir=output_dir,
        real_sample=False,
        skip_replay_eligibility_gate=True,
        include_risk_term_zero_anchor=True,
    )

    rc = module.run_risk_regime_diagnostic(config, runner=_build_runner(date), logger=lambda _msg: None)
    assert rc == 0

    payload = json.loads((output_dir / "diagnostic_matrix.json").read_text(encoding="utf-8"))
    rows = payload["rows"]
    assert len(rows) == 9
    anchor_rows = [row for row in rows if bool(row.get("is_anchor"))]
    assert len(anchor_rows) == 1
    anchor = anchor_rows[0]
    assert bool(anchor["risk_model_enabled"]) is True
    assert float(anchor["risk_term_weight"]) == 0.0

