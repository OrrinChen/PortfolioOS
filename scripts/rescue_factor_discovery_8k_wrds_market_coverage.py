"""Run WRDS CRSP market coverage rescue for D2-8K-01R priority events."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "projects" / "multifactor_alpha_validation" / "factor_discovery_sandbox" / "src"))

from factor_discovery_sandbox.eightk_wrds_market_rescue import run_eightk_wrds_market_rescue  # noqa: E402


DEFAULT_EVENT_REGISTRY = REPO_ROOT / "outputs" / "factor_discovery" / "8k_subtype" / "d2_real" / "eightk_event_registry_real.csv"
DEFAULT_OUTPUT_PATH = REPO_ROOT / "data" / "cache" / "factor_discovery_8k" / "wrds_priority_8k_price_rescue.csv"
DEFAULT_MANIFEST_PATH = REPO_ROOT / "outputs" / "factor_discovery" / "8k_subtype" / "d2_real" / "wrds_market_rescue_manifest.json"


def main() -> None:
    parser = argparse.ArgumentParser(description="Write bounded WRDS CRSP price cache for D2-8K-01R priority events.")
    parser.add_argument("--event-registry", default=str(DEFAULT_EVENT_REGISTRY))
    parser.add_argument("--output-path", default=str(DEFAULT_OUTPUT_PATH))
    parser.add_argument("--manifest-path", default=str(DEFAULT_MANIFEST_PATH))
    parser.add_argument("--wrds-username", default=None)
    parser.add_argument("--wrds-max-date", default="2024-12-31")
    parser.add_argument("--max-events", type=int, default=None)
    args = parser.parse_args()

    import wrds

    wrds_username = args.wrds_username or os.environ.get("WRDS_USERNAME")
    connection_kwargs = {"wrds_username": wrds_username} if wrds_username else {}
    connection = wrds.Connection(**connection_kwargs)
    try:
        result = run_eightk_wrds_market_rescue(
            event_registry_path=args.event_registry,
            output_path=args.output_path,
            manifest_path=args.manifest_path,
            connection=connection,
            wrds_max_date=args.wrds_max_date,
            max_events=args.max_events,
        )
    finally:
        connection.close()

    print(f"status={result['status']}")
    print(f"eligible_event_count={result['eligible_event_count']}")
    print(f"skipped_after_wrds_max_date={result['skipped_after_wrds_max_date']}")
    print(f"linked_permno_count={result['linked_permno_count']}")
    print(f"row_count={result['row_count']}")
    print(f"output_path={result['output_path']}")
    print(f"manifest_path={args.manifest_path}")
    print(f"q1_entry_allowed={str(result['q1_entry_allowed']).lower()}")
    print(f"q2_entry_allowed={str(result['q2_entry_allowed']).lower()}")
    print(f"production_approval_claimed={str(result['production_approval_claimed']).lower()}")


if __name__ == "__main__":
    main()
