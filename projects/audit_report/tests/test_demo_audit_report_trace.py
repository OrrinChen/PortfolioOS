from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[3]
SCRIPT_PATH = REPO_ROOT / "projects" / "audit_report" / "scripts" / "build_demo_audit_report.py"
MANIFEST_PATH = REPO_ROOT / "projects" / "audit_report" / "examples" / "demo_audit_manifest.yaml"
FORBIDDEN_TRACE_TERMS = {
    "api_key",
    "broker_output",
    "live_performance",
    "order",
    "password",
    "secret",
    "token",
    "trading_instruction",
}


def test_demo_audit_report_cli_writes_structured_trace_jsonl(tmp_path: Path) -> None:
    output_path = tmp_path / "demo_audit_report.md"
    provenance_path = tmp_path / "demo_run_manifest.json"
    trace_path = tmp_path / "trace.jsonl"
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"

    result = subprocess.run(
        [
            sys.executable,
            str(SCRIPT_PATH),
            "--manifest",
            str(MANIFEST_PATH),
            "--output",
            str(output_path),
            "--provenance-output",
            str(provenance_path),
            "--trace-jsonl",
            str(trace_path),
        ],
        cwd=REPO_ROOT,
        env=env,
        check=False,
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    events = [json.loads(line) for line in trace_path.read_text(encoding="utf-8").splitlines()]
    event_names = [event["event"] for event in events]

    for expected_event in (
        "bundle_loaded",
        "schema_validated",
        "promotion_decision_created",
        "q2_scenario_unavailable",
        "report_written",
    ):
        assert expected_event in event_names
    assert output_path.exists()
    assert provenance_path.exists()
    assert _has_no_forbidden_trace_terms(trace_path)


def _has_no_forbidden_trace_terms(path: Path) -> bool:
    raw = path.read_text(encoding="utf-8").lower()
    return all(term not in raw for term in FORBIDDEN_TRACE_TERMS)
