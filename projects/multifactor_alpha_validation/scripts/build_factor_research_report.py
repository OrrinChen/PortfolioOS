from __future__ import annotations

import argparse
from pathlib import Path

from _pipeline import build_pipeline
from multifactor_alpha_validation.reports import write_research_report


def main() -> None:
    parser = argparse.ArgumentParser(description="Build multifactor research report.")
    parser.add_argument("--spec-dir", type=Path, default=Path("projects/multifactor_alpha_validation/factor_specs"))
    parser.add_argument("--output-dir", type=Path, default=Path("reports"))
    args = parser.parse_args()

    *_, report, _dashboard = build_pipeline(args.spec_dir)
    path = write_research_report(report, args.output_dir)
    print(f"factor_research_report_built path={path}")


if __name__ == "__main__":
    main()
