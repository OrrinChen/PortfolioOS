"""Run D2-INSIDER-01 no-formula observability fixture."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.insider_disclosure_d2 import run_insider_disclosure_d2


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "factor_discovery" / "insider_disclosure" / "d2"


def main() -> None:
    parser = argparse.ArgumentParser(description="Write D2 insider disclosure observability artifacts.")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()

    result = run_insider_disclosure_d2(output_dir=args.output_dir)
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"coupling_group={result.summary['coupling_group']}")
    print(f"event_count={result.summary['event_count']}")
    print(f"event_month_count={result.summary['event_month_count']}")
    print(f"overall_decision={result.summary['overall_decision']}")
    print(f"allow_d3_charter_for={','.join(result.summary['allow_d3_charter_for'])}")
    print(f"formula_score_written={str(result.summary['formula_score_written']).lower()}")
    print(f"measurement_spec_written={str(result.summary['measurement_spec_written']).lower()}")
    print(f"q1_entry_allowed={str(result.summary['q1_entry_allowed']).lower()}")
    print(f"q2_entry_allowed={str(result.summary['q2_entry_allowed']).lower()}")
    print(f"alpha_registry_update_allowed={str(result.summary['alpha_registry_update_allowed']).lower()}")
    print(f"production_approval_claimed={str(result.summary['production_approval_claimed']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
