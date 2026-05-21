from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.data_contract import run_research_mode_preflight


def main() -> None:
    parser = argparse.ArgumentParser(description="Run multifactor research-mode PIT preflight.")
    parser.add_argument(
        "--manifest",
        type=Path,
        default=Path("projects/multifactor_alpha_validation/configs/research_mode_preflight_local_proxy.yaml"),
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/multifactor_alpha_validation/research_mode_preflight"),
    )
    parser.add_argument(
        "--require-ready",
        action="store_true",
        help="Exit nonzero when research-mode readiness is blocked.",
    )
    args = parser.parse_args()

    result = run_research_mode_preflight(args.manifest, args.output_dir)
    status = "ready" if result.research_mode_ready else "blocked"
    print(
        "multifactor_research_mode_preflight "
        f"status={status} "
        f"blocker_count={len(result.blockers)} "
        f"output_dir={args.output_dir}"
    )
    if args.require_ready and not result.research_mode_ready:
        raise SystemExit(2)


if __name__ == "__main__":
    main()
