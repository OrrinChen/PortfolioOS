from __future__ import annotations

import subprocess
import sys
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
SCRIPT_PATH = REPO_ROOT / "scripts" / "build_typed_alpha_closeout_report.py"
COMMITTED_REPORT = REPO_ROOT / "reports" / "typed_alpha_closeout_report.md"


def test_typed_alpha_closeout_report_builder_writes_required_sections(tmp_path: Path) -> None:
    output_path = tmp_path / "typed_alpha_closeout_report.md"
    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--output", str(output_path)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    text = output_path.read_text(encoding="utf-8")
    for heading in (
        "# Typed Alpha Closeout Report",
        "## Scope",
        "## What This Proves",
        "## What This Does Not Prove",
        "## Known Limitations",
        "## Reproducibility Commands",
        "## Next Allowed Work",
    ):
        assert heading in text
    for non_claim in (
        "no live alpha approval",
        "no production trading approval",
        "no broker integration approval",
        "no order generation",
        "no realized alpha performance claim",
    ):
        assert non_claim in text
    assert "Q2 typed rows may remain unavailable" in text
    assert "paper overlay readiness is environment calibration only" in text


def test_committed_typed_alpha_closeout_report_matches_builder(tmp_path: Path) -> None:
    output_path = tmp_path / "typed_alpha_closeout_report.md"
    result = subprocess.run(
        [sys.executable, str(SCRIPT_PATH), "--output", str(output_path)],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0, result.stderr
    assert COMMITTED_REPORT.read_text(encoding="utf-8") == output_path.read_text(encoding="utf-8")


def test_typed_alpha_closeout_report_avoids_approval_language() -> None:
    text = COMMITTED_REPORT.read_text(encoding="utf-8").lower()

    forbidden_claims = (
        "production alpha approved",
        "production trading approved",
        "broker integration approved",
        "realized alpha performance result",
        "live alpha orders approved",
    )
    for forbidden_claim in forbidden_claims:
        assert forbidden_claim not in text
