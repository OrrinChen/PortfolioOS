from __future__ import annotations

import argparse
from pathlib import Path

from _pipeline import build_pipeline
from multifactor_alpha_validation.registry import write_factor_registry
from multifactor_alpha_validation.reports import write_release_manifest, write_research_report
from multifactor_alpha_validation.dashboard import write_factor_dashboard


def main() -> None:
    parser = argparse.ArgumentParser(description="Build multifactor release manifest.")
    parser.add_argument("--spec-dir", type=Path, default=Path("projects/multifactor_alpha_validation/factor_specs"))
    parser.add_argument("--registry-dir", type=Path, default=Path("outputs/factor_registry"))
    parser.add_argument("--report-dir", type=Path, default=Path("reports"))
    parser.add_argument("--dashboard-dir", type=Path, default=Path("outputs/factor_dashboard"))
    parser.add_argument("--output-dir", type=Path, default=Path("outputs/factor_release"))
    args = parser.parse_args()

    *_, survival, registry, report, dashboard = build_pipeline(args.spec_dir)
    write_factor_registry(registry, args.registry_dir)
    report_path = write_research_report(report, args.report_dir)
    dashboard_path = write_factor_dashboard(dashboard, args.dashboard_dir)
    manifest = write_release_manifest(
        args.output_dir,
        [
            args.registry_dir / "factor_registry.yaml",
            args.registry_dir / "factor_decision_table.csv",
            report_path,
            dashboard_path,
        ],
        survival,
    )
    print(f"factor_release_manifest_built path={manifest}")


if __name__ == "__main__":
    main()

