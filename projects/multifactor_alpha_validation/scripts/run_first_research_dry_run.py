from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.research_dry_run import run_first_research_dry_run


def main() -> None:
    parser = argparse.ArgumentParser(description="Run the first minimal PIT-ready multifactor research dry run.")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path(
            "projects/multifactor_alpha_validation/fixtures/research_dataset/research_mode_dataset_manifest_fixture.yaml"
        ),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/research_dry_run"),
    )
    args = parser.parse_args()

    result = run_first_research_dry_run(args.manifest, args.output_dir)
    print(
        "multifactor_first_research_dry_run "
        f"preflight_ready={str(result.preflight_ready).lower()} "
        f"factor_count={len(result.factor_ids)} "
        f"same_close_trading_used={str(result.same_close_trading_used).lower()} "
        f"allocator_ran={str(result.allocator_ran).lower()}"
    )


if __name__ == "__main__":
    main()
