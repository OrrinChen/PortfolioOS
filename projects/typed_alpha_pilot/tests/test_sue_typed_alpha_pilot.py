from __future__ import annotations

import csv
import json
from pathlib import Path

from portfolio_os.alpha.view_contract import load_alpha_view
from typed_alpha_pilot.pilot import run_sue_typed_alpha_pilot


REPO_ROOT = Path(__file__).resolve().parents[3]


def test_sue_typed_alpha_pilot_writes_full_local_artifact_chain(tmp_path: Path) -> None:
    result = run_sue_typed_alpha_pilot(
        output_dir=tmp_path,
        alpha_view_path=REPO_ROOT / "projects" / "alpha_view_contract" / "examples" / "event_sue_pead_view.json",
        evidence_bundle_path=REPO_ROOT / "projects" / "evidence_bundle" / "examples" / "valid_bundle.yaml",
    )

    assert set(result.artifacts) >= {
        "us_sue_event_alpha_view.json",
        "us_sue_event_evidence_bundle.json",
        "us_sue_projection_panel.csv",
        "us_sue_q2_matrix.csv",
        "us_sue_audit_report.md",
    }
    for path in result.artifacts.values():
        assert path.exists()

    assert load_alpha_view(result.artifacts["us_sue_event_alpha_view.json"]).alpha_view_id == "AV-US-SUE-PEAD-001"
    event_bundle = json.loads(result.artifacts["us_sue_event_evidence_bundle.json"].read_text(encoding="utf-8"))
    assert event_bundle["schema_version"] == "event_evidence_bundle.v1"

    with result.artifacts["us_sue_projection_panel.csv"].open(newline="", encoding="utf-8") as handle:
        projection_rows = list(csv.DictReader(handle))
    assert {row["symbol"] for row in projection_rows} == {"AAPL", "MSFT"}

    with result.artifacts["us_sue_q2_matrix.csv"].open(newline="", encoding="utf-8") as handle:
        q2_rows = list(csv.DictReader(handle))
    assert q2_rows
    assert q2_rows[0]["status"] == "unavailable"
    assert q2_rows[0]["gross_to_net_retention"] == ""
    assert q2_rows[0]["turnover"] == ""
    assert q2_rows[0]["active_name_count"] == "2"
    assert q2_rows[0]["abstain_count"] == "1"


def test_sue_typed_alpha_pilot_report_is_not_production_approval(tmp_path: Path) -> None:
    result = run_sue_typed_alpha_pilot(
        output_dir=tmp_path,
        alpha_view_path=REPO_ROOT / "projects" / "alpha_view_contract" / "examples" / "event_sue_pead_view.json",
        evidence_bundle_path=REPO_ROOT / "projects" / "evidence_bundle" / "examples" / "valid_bundle.yaml",
    )

    report = result.artifacts["us_sue_audit_report.md"].read_text(encoding="utf-8")

    assert "SUE Typed Alpha Pilot" in report
    assert "integration benchmark, not production approval" in report
    assert "Promotion Gate v2 decision: `promote_to_execution_eval`" in report
    assert "Q2 typed matrix status: `unavailable`" in report
    assert "No live trading instruction" in report
