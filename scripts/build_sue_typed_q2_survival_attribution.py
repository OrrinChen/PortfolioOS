"""Build the SUE execution-survival attribution report from Phase 50 artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path

from execution_aware_optimizer.sue_execution_survival_attribution import (
    build_sue_execution_survival_attribution,
    load_sue_survival_result,
    write_sue_execution_survival_attribution_artifacts,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build SUE execution-survival attribution artifacts.")
    parser.add_argument(
        "--survival-result",
        default=str(REPO_ROOT / "outputs" / "sue_typed_q2_survival" / "sue_typed_q2_survival_result.json"),
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "sue_typed_q2_survival"),
    )
    parser.add_argument(
        "--report",
        default=str(REPO_ROOT / "reports" / "sue_typed_q2_survival_attribution.md"),
    )
    args = parser.parse_args()

    result = load_sue_survival_result(args.survival_result)
    attribution = build_sue_execution_survival_attribution(result)
    artifacts = write_sue_execution_survival_attribution_artifacts(
        attribution,
        output_dir=args.output_dir,
        report_path=args.report,
    )
    print(f"decision_label={attribution.decision_label}")
    print(f"primary_stop_layer={attribution.primary_stop_layer}")
    print(f"phase52_revision_marginal_value_should_proceed={attribution.phase52_revision_marginal_value_should_proceed}")
    for name, path in artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
