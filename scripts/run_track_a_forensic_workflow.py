#!/usr/bin/env python3
"""Run the Track A forensic workflow fixture or a supplied config."""

from __future__ import annotations

import argparse
import json
from pathlib import Path

from factor_discovery_sandbox.track_a_forensic_workflow import (
    run_track_a_forensic_workflow,
    write_fixture_config,
)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Track A forensic research workflow.")
    parser.add_argument("--config", type=Path, default=None, help="Existing track_a_run/v1 config.")
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("outputs/track_a/sector_neutral_residual_momentum/fixture"),
        help="Output directory used when generating the deterministic fixture config.",
    )
    parser.add_argument("--run-id", default="sector-neutral-residual-momentum-fixture")
    args = parser.parse_args()

    config_path = args.config
    if config_path is None:
        config_path = write_fixture_config(args.output_dir, run_id=args.run_id)
    result = run_track_a_forensic_workflow(config_path)
    print(
        json.dumps(
            {
                "decision": result.decision["decision"],
                "primary_reason": result.decision["primary_reason"],
                "output_dir": str(result.output_dir),
                "artifact_count": len(result.artifacts),
            },
            sort_keys=True,
        ),
    )


if __name__ == "__main__":
    main()
