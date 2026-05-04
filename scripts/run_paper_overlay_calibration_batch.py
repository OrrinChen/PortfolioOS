"""Build paper overlay readiness artifacts from local observation CSV data."""

from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from portfolio_os.paper.overlay_readiness import (
    assess_paper_overlay_readiness,
    write_paper_overlay_readiness_artifacts,
)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--observations", required=True, help="Local paper drift observations CSV.")
    parser.add_argument("--output-dir", required=True, help="Directory for readiness artifacts.")
    parser.add_argument("--requested-sample-count", type=int, default=50)
    parser.add_argument("--max-validated-participation-rate", type=float, default=0.001)
    args = parser.parse_args()

    observations = pd.read_csv(args.observations)
    result = assess_paper_overlay_readiness(
        observations=observations,
        requested_sample_count=args.requested_sample_count,
        max_validated_participation_rate=args.max_validated_participation_rate,
    )
    artifacts = write_paper_overlay_readiness_artifacts(result, Path(args.output_dir))
    for name, path in artifacts.items():
        print(f"{name}: {path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
