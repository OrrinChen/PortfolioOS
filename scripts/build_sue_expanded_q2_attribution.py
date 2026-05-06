"""Build expanded SUE Q2 attribution report from Phase 56A artifacts."""

from __future__ import annotations

import argparse
from pathlib import Path

from execution_aware_optimizer.sue_expanded_typed_q2_survival import (
    load_sue_expanded_result,
    write_sue_expanded_q2_attribution_artifacts,
)


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Build expanded SUE typed-Q2 attribution artifacts.")
    parser.add_argument(
        "--result",
        default=str(
            REPO_ROOT
            / "outputs"
            / "sue_expanded_typed_q2_survival"
            / "sue_expanded_typed_q2_survival_result.json"
        ),
    )
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "outputs" / "sue_expanded_typed_q2_survival"))
    parser.add_argument("--report-path", default=str(REPO_ROOT / "reports" / "sue_expanded_typed_q2_survival_report.md"))
    args = parser.parse_args()

    result = load_sue_expanded_result(args.result)
    artifacts = write_sue_expanded_q2_attribution_artifacts(
        result,
        output_dir=args.output_dir,
        report_path=args.report_path,
    )
    print(f"survival_status={result.survival_status}")
    print(f"event_count={result.event_count}")
    print(f"q2_observed_rows={result.q2_observed_rows}")
    print(f"q2_unavailable_rows={result.q2_unavailable_rows}")
    for name, path in artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
