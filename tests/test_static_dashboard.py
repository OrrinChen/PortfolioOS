from __future__ import annotations

import json
from pathlib import Path

from portfolio_os.dashboard import render_static_dashboard


def test_static_dashboard_renders_read_only_artifact_sections(tmp_path: Path) -> None:
    artifact_root = tmp_path / "artifacts"
    _write_json(artifact_root / "batch_summary.json", {"results": [{"candidate_id": "candidate-a"}]})
    _write_json(artifact_root / "q1_summary.json", {"ready_count": 1, "rejected_count": 1})
    _write_json(artifact_root / "promotion_decision.json", {"decision": "promote_to_execution_eval"})
    _write_text(artifact_root / "q2_execution_matrix.csv", "scenario_id,status\ns1,unavailable\n")
    _write_text(artifact_root / "cost_sensitivity.csv", "cost_bps,status\n5,unavailable\n")
    _write_text(artifact_root / "audit_report.md", "# Audit Report\n")
    _write_json(artifact_root / "run_manifest.json", {"run_id": "demo"})
    output_path = tmp_path / "dashboard.html"

    rendered = render_static_dashboard(artifact_root=artifact_root, output_path=output_path)
    html = output_path.read_text(encoding="utf-8")

    assert rendered == output_path
    for heading in (
        "Candidate List",
        "Q1 Status",
        "Promotion Decision",
        "Q2 Execution Matrix",
        "Cost Sensitivity",
        "Audit Report",
        "Reproducibility Manifest",
    ):
        assert heading in html
    assert "candidate-a" in html
    assert "promote_to_execution_eval" in html
    assert _is_read_only_html(html)


def test_static_dashboard_handles_missing_artifacts_as_unavailable(tmp_path: Path) -> None:
    output_path = tmp_path / "dashboard.html"

    render_static_dashboard(artifact_root=tmp_path / "empty", output_path=output_path)
    html = output_path.read_text(encoding="utf-8")

    assert "Artifact not available" in html
    assert _is_read_only_html(html)


def _is_read_only_html(html: str) -> bool:
    lowered = html.lower()
    forbidden = ("<form", "method=", "post", "trade", "broker", "order")
    return all(term not in lowered for term in forbidden)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
