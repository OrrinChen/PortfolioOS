from __future__ import annotations

import argparse
from pathlib import Path

from multifactor_alpha_validation.wrds_ingest import (
    run_wrds_multifactor_ingest,
    validate_wrds_query_config,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Pull WRDS data into the multifactor research-mode contract.")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path("projects/multifactor_alpha_validation/configs/wrds_multifactor_query_template.yaml"),
    )
    parser.add_argument("--base-dir", type=Path, default=Path("."))
    parser.add_argument("--require-ready", action="store_true")
    parser.add_argument(
        "--check-config-only",
        action="store_true",
        help="Validate config shape without opening a WRDS connection.",
    )
    args = parser.parse_args()

    if args.check_config_only:
        payload, _ = validate_wrds_query_config(args.config)
        print(
            "wrds_multifactor_config_valid "
            f"query_count={len(payload['queries'])} "
            "credentials_in_config=false"
        )
        return

    result = run_wrds_multifactor_ingest(
        args.config,
        base_dir=args.base_dir,
        require_ready=args.require_ready,
    )
    status = "ready" if result.preflight.research_mode_ready else "blocked"
    print(
        "wrds_multifactor_ingest "
        f"status={status} "
        f"manifest={result.manifest_path} "
        f"blocker_count={len(result.preflight.blockers)}"
    )


if __name__ == "__main__":
    main()
