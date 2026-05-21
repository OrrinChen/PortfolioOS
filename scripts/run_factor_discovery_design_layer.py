"""Write the FD-D0 Factor Discovery design-layer contract."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.factor_design import write_factor_design_layer_spec


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "factor_discovery" / "design_layer"
DEFAULT_REPORT = REPO_ROOT / "reports" / "factor_discovery_design_layer_report.md"


def main() -> None:
    parser = argparse.ArgumentParser(description="Write Factor Discovery design-layer contract artifacts.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--report", default=str(DEFAULT_REPORT))
    args = parser.parse_args()

    result = write_factor_design_layer_spec(output_dir=args.output_dir, report_path=args.report)
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(
        "design_layer_required_before_formula="
        f"{str(result.summary['design_layer_required_before_formula']).lower()}"
    )
    print(f"allocator_entry_allowed={str(result.summary['allocator_entry_allowed']).lower()}")
    print(f"q1_entry_allowed={str(result.summary['q1_entry_allowed']).lower()}")
    print(f"q2_entry_allowed={str(result.summary['q2_entry_allowed']).lower()}")
    print(f"alpha_registry_update_allowed={str(result.summary['alpha_registry_update_allowed']).lower()}")
    print(f"direct_q2_entry_allowed={str(result.summary['direct_q2_entry_allowed']).lower()}")
    print(f"not_alpha_evidence={str(result.summary['not_alpha_evidence']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
