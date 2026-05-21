from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "run_portfolioos_demo_v2.py"
DEMO_V2_PYTHONPATH = ":".join(
    [
        "src",
        "projects/typed_alpha_pilot/src",
        "projects/evidence_bundle/src",
        "projects/promotion_gate/src",
        "projects/execution_aware_optimizer/src",
    ]
)


EXPECTED_SCHEMA_VERSIONS = {
    "alpha_view": "alpha_view.v1",
    "event_evidence": "event_evidence_bundle.v1",
    "projection_manifest": "alpha_projection.v2",
    "promotion_decision_v2": "promotion_decision.v2",
    "q2_input_contract_v2": "q2_input_contract.v2",
    "q2_typed_matrix": "q2_typed_matrix.v1",
    "paper_overlay_readiness": "paper_overlay_readiness.v1",
    "typed_alpha_release_manifest": "typed_alpha_release_manifest.v1",
}


def test_typed_alpha_schema_versions_are_locked() -> None:
    from portfolio_os.alpha.schema_versions import TYPED_ALPHA_SCHEMA_VERSIONS

    assert TYPED_ALPHA_SCHEMA_VERSIONS == EXPECTED_SCHEMA_VERSIONS


def test_demo_v2_release_candidate_artifacts_have_schema_versions(tmp_path: Path) -> None:
    output_dir = _run_demo_v2(tmp_path)

    expected_files = {
        "typed_alpha_release_manifest.json",
        "us_sue_event_alpha_view.json",
        "us_sue_event_evidence_bundle.json",
        "us_sue_projection_manifest.json",
        "us_sue_promotion_decision_v2.json",
        "us_sue_q2_matrix.csv",
        "paper_overlay_calibration_summary.json",
        "us_sue_audit_report.md",
        "dashboard_v2.html",
    }
    assert expected_files.issubset({path.name for path in output_dir.iterdir()})

    alpha_view = _read_json(output_dir / "us_sue_event_alpha_view.json")
    event_evidence = _read_json(output_dir / "us_sue_event_evidence_bundle.json")
    projection_manifest = _read_json(output_dir / "us_sue_projection_manifest.json")
    promotion_decision = _read_json(output_dir / "us_sue_promotion_decision_v2.json")
    paper_summary = _read_json(output_dir / "paper_overlay_calibration_summary.json")
    release_manifest = _read_json(output_dir / "typed_alpha_release_manifest.json")

    assert alpha_view["schema_version"] == EXPECTED_SCHEMA_VERSIONS["alpha_view"]
    assert event_evidence["schema_version"] == EXPECTED_SCHEMA_VERSIONS["event_evidence"]
    assert projection_manifest["schema_version"] == EXPECTED_SCHEMA_VERSIONS["projection_manifest"]
    assert promotion_decision["schema_version"] == EXPECTED_SCHEMA_VERSIONS["promotion_decision_v2"]
    assert (
        promotion_decision["q2_allowed_inputs"]["schema_version"]
        == EXPECTED_SCHEMA_VERSIONS["q2_input_contract_v2"]
    )
    assert paper_summary["schema_version"] == EXPECTED_SCHEMA_VERSIONS["paper_overlay_readiness"]
    assert release_manifest["schema_version"] == EXPECTED_SCHEMA_VERSIONS["typed_alpha_release_manifest"]
    assert release_manifest["schema_versions"] == EXPECTED_SCHEMA_VERSIONS

    with (output_dir / "us_sue_q2_matrix.csv").open(newline="", encoding="utf-8") as handle:
        first_row = next(csv.DictReader(handle))
    assert first_row["schema_version"] == EXPECTED_SCHEMA_VERSIONS["q2_typed_matrix"]
    assert first_row["status"] == "unavailable"
    assert first_row["gross_to_net_retention"] == ""
    assert first_row["turnover"] == ""
    assert first_row["cost_drag"] == ""


def test_demo_v2_release_candidate_manifest_links_typed_alpha_chain(tmp_path: Path) -> None:
    output_dir = _run_demo_v2(tmp_path)
    release_manifest = _read_json(output_dir / "typed_alpha_release_manifest.json")

    assert release_manifest["run_id"] == "demo_v2"
    assert release_manifest["status"] == "release_candidate_local_only"
    assert release_manifest["production_alpha_approved"] is False
    assert release_manifest["live_trading_enabled"] is False
    assert release_manifest["broker_routes_enabled"] is False
    assert release_manifest["typed_alpha_chain"] == [
        "AlphaView",
        "Event Evidence",
        "Projection Manifest",
        "Promotion Gate v2",
        "Q2 Typed Matrix",
        "Paper Overlay Readiness",
        "Demo v2 Dashboard",
    ]
    assert release_manifest["content_hash"]


def test_typed_alpha_release_candidate_notes_exist_and_avoid_overclaims() -> None:
    release_note = REPO_ROOT / "docs" / "releases" / "typed_alpha_v0_1_release_candidate.md"
    text = release_note.read_text(encoding="utf-8")

    assert "Typed Alpha v0.1 Release Candidate" in text
    assert "make validate" in text
    assert "make demo-v2" in text
    for phrase in (
        "no production alpha approval",
        "no live trading",
        "no broker integration",
        "no order generation",
        "no realized alpha performance",
    ):
        assert phrase in text


def _run_demo_v2(tmp_path: Path) -> Path:
    output_dir = tmp_path / "demo_v2"
    env = os.environ.copy()
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    env["PYTHONPATH"] = DEMO_V2_PYTHONPATH
    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--output-dir", str(output_dir)],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )
    assert result.returncode == 0, result.stderr
    return output_dir


def _read_json(path: Path) -> dict[str, object]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload
