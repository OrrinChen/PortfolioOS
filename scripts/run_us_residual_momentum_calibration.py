"""Run the first US residual momentum calibration-family slice."""

from __future__ import annotations

import argparse
import sys
from datetime import date
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from portfolio_os.alpha.discovery_calibration import run_us_residual_momentum_calibration_from_files


REPO_ROOT = ROOT
DEFAULT_RETURNS_FILE = REPO_ROOT / "data" / "risk_inputs_us_expanded" / "returns_long.csv"
DEFAULT_UNIVERSE_REFERENCE_FILE = REPO_ROOT / "data" / "universe" / "us_universe_reference.csv"
DEFAULT_OUTPUT_DIR = REPO_ROOT / "outputs" / "us_residual_momentum_calibration" / date.today().isoformat()


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--returns-file", default=str(DEFAULT_RETURNS_FILE))
    parser.add_argument("--universe-reference-file", default=str(DEFAULT_UNIVERSE_REFERENCE_FILE))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--random-seed", type=int, default=7)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    result = run_us_residual_momentum_calibration_from_files(
        returns_file=args.returns_file,
        universe_reference_file=args.universe_reference_file,
        output_dir=args.output_dir,
        random_seed=args.random_seed,
    )
    print(f"wrote calibration artifacts to {result.output_dir}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
