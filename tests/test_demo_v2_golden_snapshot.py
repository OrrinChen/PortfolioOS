from __future__ import annotations

import json
import os
import subprocess
import sys
from pathlib import Path
from typing import Any


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "run_portfolioos_demo_v2.py"
GOLDEN_PATH = REPO_ROOT / "tests" / "golden" / "demo_v2_expected_manifest.json"
DEMO_V2_PYTHONPATH = ":".join(
    [
        "src",
        "projects/typed_alpha_pilot/src",
        "projects/evidence_bundle/src",
        "projects/promotion_gate/src",
        "projects/execution_aware_optimizer/src",
    ]
)


def test_demo_v2_matches_golden_artifact_shape_and_sections(tmp_path: Path) -> None:
    golden = _read_json(GOLDEN_PATH)
    output_dir = _run_demo_v2(tmp_path)

    actual_artifacts = {path.name for path in output_dir.iterdir() if path.is_file()}
    assert set(golden["required_artifacts"]).issubset(actual_artifacts)

    release_manifest = _read_json(output_dir / "typed_alpha_release_manifest.json")
    assert set(golden["required_manifest_keys"]).issubset(release_manifest)
    assert release_manifest["typed_alpha_chain"] == golden["required_chain"]
    assert release_manifest["status"] == "release_candidate_local_only"
    assert str(output_dir) not in json.dumps(release_manifest, sort_keys=True)

    dashboard = (output_dir / "dashboard_v2.html").read_text(encoding="utf-8")
    for heading in golden["required_dashboard_sections"]:
        assert heading in dashboard
    assert _has_no_workflow_controls(dashboard)


def test_demo_v2_golden_snapshot_preserves_unavailable_row_semantics(tmp_path: Path) -> None:
    output_dir = _run_demo_v2(tmp_path)
    q2_matrix = (output_dir / "us_sue_q2_matrix.csv").read_text(encoding="utf-8")

    assert "unavailable" in q2_matrix
    assert "typed Q2 execution adapter is not implemented" in q2_matrix
    for fabricated_metric in (
        "gross_to_net_retention,0.",
        "turnover,0.",
        "cost_drag,0.",
    ):
        assert fabricated_metric not in q2_matrix


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


def _read_json(path: Path) -> dict[str, Any]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert isinstance(payload, dict)
    return payload


def _has_no_workflow_controls(html: str) -> bool:
    lowered = html.lower()
    forbidden = ("<form", "method=", "post", "/broker", "/order", "/trade", "/live", "submit")
    return all(term not in lowered for term in forbidden)
