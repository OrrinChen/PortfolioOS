"""Run D2-INSIDER-01R local Form 4 extraction and observability replay."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.insider_disclosure_d2_real import run_real_form4_observability


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_DIR = REPO_ROOT / "data" / "cache" / "sec_form4_insider_disclosure"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "factor_discovery" / "insider_disclosure" / "d2_real"


def main() -> None:
    parser = argparse.ArgumentParser(description="Write D2 real Form 4 observability artifacts.")
    parser.add_argument("--source-dir", default=str(DEFAULT_SOURCE_DIR))
    parser.add_argument("--market-data", default="")
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--start-offset", type=int, default=0)
    parser.add_argument("--max-files", type=int, default=None)
    args = parser.parse_args()

    result = run_real_form4_observability(
        source_dir=args.source_dir,
        market_data_path=args.market_data or None,
        output_dir=args.output_dir,
        start_offset=args.start_offset,
        max_files=args.max_files,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"source_type={result.summary['source_type']}")
    print(f"real_data_status={result.summary['real_data_status']}")
    print(f"network_used={str(result.summary['network_used']).lower()}")
    print(f"source_index_total_count={result.summary.get('source_index_total_count', 0)}")
    print(f"source_index_start_offset={result.summary.get('source_index_start_offset', 0)}")
    print(f"source_index_max_files={result.summary.get('source_index_max_files', '')}")
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
        if path.exists():
            print(f"{name}={path}")


if __name__ == "__main__":
    main()
