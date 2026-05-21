"""Pull the WRDS Compustat/CCM PIT quality bundle for FD small-cap research."""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from factor_discovery_sandbox.wrds_small_cap_quality_pull import run_wrds_small_cap_quality_pull


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_RESEARCH_MANIFEST = (
    REPO_ROOT
    / "data"
    / "cache"
    / "wrds_multifactor"
    / "small_cap_us_daily"
    / "standardized"
    / "research_mode_dataset_manifest.yaml"
)
DEFAULT_OUTPUT_ROOT = REPO_ROOT / "data" / "cache" / "wrds_multifactor" / "small_cap_quality"


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull WRDS CCM/Compustat quality scores for FD small-cap research.")
    parser.add_argument("--research-manifest", default=str(DEFAULT_RESEARCH_MANIFEST))
    parser.add_argument("--output-root", default=str(DEFAULT_OUTPUT_ROOT))
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "outputs" / "factor_discovery" / "small_cap"))
    parser.add_argument("--start-date", default="2020-01-01")
    parser.add_argument("--end-date", default="2024-12-31")
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument(
        "--wrds-username",
        default=None,
        help="Optional WRDS username for this process only; credentials must remain outside the repo.",
    )
    args = parser.parse_args()

    if args.wrds_username:
        os.environ["WRDS_USERNAME"] = args.wrds_username

    result = run_wrds_small_cap_quality_pull(
        research_manifest_path=args.research_manifest,
        output_root=args.output_root,
        output_dir=args.output_dir,
        start_date=args.start_date,
        end_date=args.end_date,
        refresh=args.refresh,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"dataset_id={result.summary['dataset_id']}")
    print(f"research_manifest_path={result.summary['research_manifest_path']}")
    print(f"quality_manifest_path={result.summary['quality_manifest_path']}")
    print(f"ccm_link_rows={result.summary['ccm_link_rows']}")
    print(f"fundamental_rows={result.summary['fundamental_rows']}")
    print(f"quality_score_rows={result.summary['quality_score_rows']}")
    print(f"quality_covered_assets={result.summary['quality_covered_assets']}")
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
