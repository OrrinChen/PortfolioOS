from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path

import pandas as pd


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "run_portfolioos_demo.py"
DEMO_PYTHONPATH = ":".join(
    [
        "src",
        "projects/audit_report/src",
        "projects/agentic_alpha_triage/src",
        "projects/evidence_bundle/src",
        "projects/promotion_gate/src",
        "projects/execution_aware_optimizer/src",
    ]
)


def test_one_command_demo_writes_expected_local_artifacts(tmp_path: Path) -> None:
    output_dir = tmp_path / "demo"
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONPATH"] = DEMO_PYTHONPATH

    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--output-dir", str(output_dir)],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    expected_files = {
        "q1_summary.json",
        "evidence_bundle.json",
        "promotion_decision.json",
        "q2_execution_matrix.csv",
        "audit_report.md",
        "run_manifest.json",
        "trace.jsonl",
        "dashboard.html",
    }
    assert expected_files.issubset({path.name for path in output_dir.iterdir()})

    q1_summary = _read_json(output_dir / "q1_summary.json")
    decisions = _read_json(output_dir / "promotion_decision.json")
    q2_matrix = pd.read_csv(output_dir / "q2_execution_matrix.csv")
    audit_report = (output_dir / "audit_report.md").read_text(encoding="utf-8")
    trace_text = (output_dir / "trace.jsonl").read_text(encoding="utf-8")

    assert q1_summary["ready_count"] == 1
    assert q1_summary["rejected_count"] == 1
    assert decisions["promoted_like_case"]["decision"] == "promote_to_execution_eval"
    assert decisions["rejected_leakage_case"]["decision"] == "reject"
    assert set(q2_matrix["status"]) == {"unavailable"}
    assert "Q2 execution evaluation: skipped because promotion decision is `reject`." in audit_report
    assert "report_written" in trace_text
    assert "live_services" not in trace_text.lower()


def test_make_demo_target_uses_portfolioos_demo_script() -> None:
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")

    assert "scripts/run_portfolioos_demo.py" in makefile
    assert "--output-dir outputs/demo" in makefile


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload
