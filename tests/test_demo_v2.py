from __future__ import annotations

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


def test_demo_v2_writes_typed_alpha_artifacts_and_dashboard(tmp_path: Path) -> None:
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
    expected = {
        "us_sue_event_alpha_view.json",
        "us_sue_event_evidence_bundle.json",
        "us_sue_projection_panel.csv",
        "us_sue_projection_diagnostics.json",
        "us_sue_abstain_report.json",
        "us_sue_q2_matrix.csv",
        "paper_overlay_calibration_summary.json",
        "paper_overlay_readiness.md",
        "dashboard_v2.html",
    }
    assert expected.issubset({path.name for path in output_dir.iterdir()})

    html = (output_dir / "dashboard_v2.html").read_text(encoding="utf-8")
    for heading in (
        "Typed Alpha View",
        "Event Evidence",
        "Projection Diagnostics",
        "Abstain Report",
        "Q2 Typed Alpha Matrix",
        "Paper Overlay Calibration",
    ):
        assert heading in html
    assert _is_read_only_html(html)


def test_make_demo_v2_target_uses_demo_v2_script() -> None:
    makefile = (REPO_ROOT / "Makefile").read_text(encoding="utf-8")

    assert "demo-v2" in makefile
    assert "scripts/run_portfolioos_demo_v2.py" in makefile


def _is_read_only_html(html: str) -> bool:
    lowered = html.lower()
    forbidden = ("<form", "method=", "post", "trade", "broker", "order")
    return all(term not in lowered for term in forbidden)
