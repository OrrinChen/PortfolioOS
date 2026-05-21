"""Run D3-INSIDER-02 open-market insider buying SignalBuilder."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.insider_disclosure_d3_signal_builder import (
    run_open_market_buying_signal_builder,
)


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_EVENT_REGISTRY = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "insider_disclosure"
    / "d2_real_archive_batched_aggregate"
    / "insider_event_market_join.csv"
)
DEFAULT_MEASUREMENT_SPEC = (
    REPO_ROOT
    / "projects"
    / "multifactor_alpha_validation"
    / "factor_discovery_sandbox"
    / "factor_specs"
    / "insider_disclosure_2023"
    / "open_market_insider_buying_post_2023_v0.yaml"
)
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT / "outputs" / "factor_discovery" / "insider_disclosure" / "d3_open_market_buying_v0"
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Write D3 open-market insider buying signal artifacts.")
    parser.add_argument("--event-registry", default=str(DEFAULT_EVENT_REGISTRY))
    parser.add_argument("--measurement-spec", default=str(DEFAULT_MEASUREMENT_SPEC))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    args = parser.parse_args()

    result = run_open_market_buying_signal_builder(
        event_registry_path=args.event_registry,
        measurement_spec_path=args.measurement_spec,
        output_dir=args.output_dir,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"measurement_spec_id={result.summary['measurement_spec_id']}")
    print(f"event_count={result.summary['event_count']}")
    print(f"signal_row_count={result.summary['signal_row_count']}")
    print(f"active_signal_count={result.summary['active_signal_count']}")
    print(f"no_view_count={result.summary['no_view_count']}")
    print(f"transaction_code_scope={result.summary['transaction_code_scope']}")
    print(f"private_purchase_filter_status={result.summary['private_purchase_filter_status']}")
    print(f"signal_panel_written={str(result.summary['signal_panel_written']).lower()}")
    print(f"expected_return_panel_written={str(result.summary['expected_return_panel_written']).lower()}")
    print(f"q1_entry_allowed={str(result.summary['q1_entry_allowed']).lower()}")
    print(f"q2_entry_allowed={str(result.summary['q2_entry_allowed']).lower()}")
    print(f"alpha_registry_update_allowed={str(result.summary['alpha_registry_update_allowed']).lower()}")
    print(f"production_approval_claimed={str(result.summary['production_approval_claimed']).lower()}")
    for name, path in result.artifacts.items():
        if path.exists():
            print(f"{name}={path}")


if __name__ == "__main__":
    main()
