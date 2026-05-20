"""Extract WRDS timestamp-source files for SUE H1E.5 enrichment."""

from __future__ import annotations

import argparse
import os
from pathlib import Path
import sys


REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from portfolio_os.alpha.sue_timestamp_source_extract import (  # noqa: E402
    extract_wrds_sue_timestamp_sources,
    load_sue_timestamp_source_extract_config,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Extract SUE timestamp sources from WRDS.")
    parser.add_argument("--config", default="configs/wrds_sue_timestamp_sources.yaml")
    parser.add_argument("--wrds-username", default=None)
    args = parser.parse_args()

    if args.wrds_username:
        os.environ["WRDS_USERNAME"] = args.wrds_username

    import wrds

    config = load_sue_timestamp_source_extract_config(args.config)
    wrds_username = args.wrds_username or os.environ.get("WRDS_USERNAME")
    connection = wrds.Connection(wrds_username=wrds_username) if wrds_username else wrds.Connection()
    try:
        result = extract_wrds_sue_timestamp_sources(config, connection=connection)
    finally:
        connection.close()

    print("status=completed")
    print(f"event_count={result['event_count']}")
    print(f"ibes_actuals_matched_events={result['ibes_actuals_matched_events']}")
    print(f"compustat_rdq_matched_events={result['compustat_rdq_matched_events']}")
    print(f"q2_evaluation_ran={result['q2_evaluation_ran']}")
    print(f"optimizer_path_evaluation_ran={result['optimizer_path_evaluation_ran']}")
    print(f"production_approval_claimed={result['production_approval_claimed']}")
    print(f"ibes_actuals_output_path={result['ibes_actuals_output_path']}")
    print(f"compustat_quarterly_output_path={result['compustat_quarterly_output_path']}")


if __name__ == "__main__":
    main()
