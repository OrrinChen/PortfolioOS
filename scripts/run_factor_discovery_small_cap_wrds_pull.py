"""Pull the WRDS CRSP daily bundle for FD small-cap candidate-family research."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from factor_discovery_sandbox.wrds_small_cap_pull import run_wrds_small_cap_pull


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "data" / "cache" / "wrds_multifactor" / "small_cap_us_daily"


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull WRDS CRSP data for FD small-cap family research.")
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "outputs" / "factor_discovery" / "small_cap"))
    parser.add_argument("--start-date", default="2020-01-01")
    parser.add_argument("--end-date", default="2024-12-31")
    parser.add_argument("--price-start-date", default="2019-01-01")
    parser.add_argument("--date-chunk-years", type=int, default=1)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument(
        "--wrds-username",
        default=None,
        help="Optional WRDS username for this process only; credentials must remain outside the repo.",
    )
    args = parser.parse_args()

    if args.wrds_username:
        os.environ["WRDS_USERNAME"] = args.wrds_username

    result = run_wrds_small_cap_pull(
        output_root=args.output_root,
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        price_start_date=args.price_start_date,
        date_chunk_years=args.date_chunk_years,
        refresh=args.refresh,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"dataset_id={result.summary['dataset_id']}")
    print(f"manifest_path={result.summary['manifest_path']}")
    print(f"research_start={result.summary['research_start']}")
    print(f"research_end={result.summary['research_end']}")
    print(f"price_start={result.summary['price_start']}")
    print(f"universe_rows={result.summary['universe_rows']}")
    print(f"price_rows={result.summary['price_rows']}")
    print(f"benchmark_rows={result.summary['benchmark_rows']}")
    print(f"delisting_rows={result.summary['delisting_rows']}")
    print(f"allocator_entry_allowed={str(result.summary['allocator_entry_allowed']).lower()}")
    print(f"q1_entry_allowed={str(result.summary['q1_entry_allowed']).lower()}")
    print(f"q2_entry_allowed={str(result.summary['q2_entry_allowed']).lower()}")
    print(f"direct_q2_entry_allowed={str(result.summary['direct_q2_entry_allowed']).lower()}")
    print(f"not_alpha_evidence={str(result.summary['not_alpha_evidence']).lower()}")
    for name, path in result.raw_files.items():
        print(f"raw_{name}={path}")
    for name, path in result.standardized_files.items():
        print(f"standardized_{name}={path}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
