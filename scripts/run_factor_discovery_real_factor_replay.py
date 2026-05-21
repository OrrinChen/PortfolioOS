"""Run FD-R3 real daily FactorSpec replay."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.real_factor_replay import run_real_factor_replay


REPO_ROOT = Path(__file__).resolve().parents[1]


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Factor Discovery FD-R3 real daily factor replay.")
    parser.add_argument(
        "--manifest",
        default=str(
            REPO_ROOT
            / "data"
            / "cache"
            / "wrds_multifactor"
            / "nasdaq100_daily_full10"
            / "standardized"
            / "research_mode_dataset_manifest.yaml"
        ),
    )
    parser.add_argument(
        "--output-dir",
        default=str(REPO_ROOT / "outputs" / "factor_discovery" / "real_data_daily" / "fd_r3"),
    )
    parser.add_argument(
        "--factor-spec-dir",
        default=str(
            REPO_ROOT
            / "projects"
            / "multifactor_alpha_validation"
            / "factor_discovery_sandbox"
            / "factor_specs"
            / "price_volume_29"
        ),
    )
    args = parser.parse_args()

    result = run_real_factor_replay(args.manifest, args.output_dir, args.factor_spec_dir)
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"dataset_frequency={result.summary['dataset_frequency']}")
    print(f"factor_count={result.summary['factor_count']}")
    print(f"formula_version={result.summary['formula_version']}")
    print(f"signal_date_count={result.summary['signal_date_count']}")
    print(f"row_count={result.summary['row_count']}")
    print(f"active_view_rows={result.summary['active_view_rows']}")
    print(f"explicit_abstain_rows={result.summary['explicit_abstain_rows']}")
    print(f"factor_ranking_ran={str(result.summary['factor_ranking_ran']).lower()}")
    print(f"allocator_ran={str(result.summary['allocator_ran']).lower()}")
    print(f"alpha_success_claimed={str(result.summary['alpha_success_claimed']).lower()}")
    print(f"direct_q2_entry_allowed={str(result.summary['direct_q2_entry_allowed']).lower()}")
    print(f"parquet_written={str(result.summary['parquet_written']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
