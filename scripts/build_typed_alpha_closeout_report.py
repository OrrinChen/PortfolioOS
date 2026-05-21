#!/usr/bin/env python3
"""Build the deterministic typed-alpha closeout report."""

from __future__ import annotations

import argparse
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT = REPO_ROOT / "reports" / "typed_alpha_closeout_report.md"


def main() -> None:
    parser = argparse.ArgumentParser(description="Build the typed-alpha closeout report.")
    parser.add_argument("--output", default=str(DEFAULT_OUTPUT))
    args = parser.parse_args()

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(build_typed_alpha_closeout_report(), encoding="utf-8")
    print(f"typed_alpha_closeout_report: {output_path}")


def build_typed_alpha_closeout_report() -> str:
    """Return the deterministic Phase 35-42 closeout markdown."""

    return "\n".join(
        [
            "# Typed Alpha Closeout Report",
            "",
            "## Scope",
            "",
            "Phase 35-42 implemented the typed-alpha research-to-Q2 local artifact chain:",
            "",
            "- AlphaView contract",
            "- event-aware evaluation contract",
            "- Alpha Projection Bridge v2",
            "- Promotion Gate v2",
            "- Q2 typed alpha execution matrix",
            "- paper overlay readiness lane",
            "- local SUE integration benchmark",
            "- demo-v2 static dashboard",
            "",
            "Phase 43-44 then locked schema versions, release-candidate artifacts, and golden demo-v2 shape checks.",
            "",
            "## What This Proves",
            "",
            "- Typed alpha artifacts can be represented deterministically.",
            "- Event alpha can be separated from fixed-horizon alpha.",
            "- Projection artifacts can produce optimizer-facing expected-return panels with explicit abstain rows.",
            "- Promotion Gate v2 can preserve the Q1/Q2 boundary while emitting only a typed input contract.",
            "- Q2 typed matrix can report unavailable rows without fabricating returns.",
            "- Paper overlay readiness can be documented as execution-environment calibration only.",
            "- Demo-v2 can present the full local audit chain with a release manifest and schema-version lock.",
            "",
            "## What This Does Not Prove",
            "",
            "- no live alpha approval",
            "- no production trading approval",
            "- no broker integration approval",
            "- no order generation",
            "- no realized alpha performance claim",
            "- no real SUE deployment",
            "- no claim that unavailable Q2 rows are performance results",
            "- no claim that paper overlay calibration validates alpha",
            "",
            "## Known Limitations",
            "",
            "- Q2 typed rows may remain unavailable until a typed execution adapter exists.",
            "- paper overlay readiness is environment calibration only.",
            "- The SUE pilot is an integration benchmark, not production approval.",
            "- Demo-v2 uses local fixtures and ignored local outputs under `outputs/demo_v2/`.",
            "- Schema migration support is limited to the current v0.1 version lock.",
            "",
            "## Reproducibility Commands",
            "",
            "```bash",
            "make validate",
            "make demo-v2",
            "PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_typed_alpha_release_candidate.py tests/test_demo_v2_golden_snapshot.py -q",
            "```",
            "",
            "## Next Allowed Work",
            "",
            "- dashboard readability polish",
            "- read-only artifact browsing hardening",
            "- closeout documentation cleanup",
            "- schema migration support if a v2 contract is intentionally introduced",
            "",
        ]
    )


if __name__ == "__main__":
    main()
