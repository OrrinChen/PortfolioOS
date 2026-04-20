from __future__ import annotations

import argparse
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from portfolio_os.execution.slippage_calibration import (
    calibrate_slippage,
    create_synthetic_slippage_calibration_fixture,
    write_slippage_calibration_artifacts,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Calibrate the PortfolioOS slippage model from Alpaca fill telemetry.")
    parser.add_argument("--fill-collection-root", type=Path, default=None, help="Root directory containing alpaca_fill_manifest.json files.")
    parser.add_argument("--source-run-root", type=Path, default=None, help="Optional pilot_validation run root used to recover ADV and source market data.")
    parser.add_argument("--output-dir", type=Path, required=True, help="Parent directory for the calibration run output.")
    parser.add_argument("--alpha", type=float, default=0.6, help="Fixed alpha used for the power-law model.")
    parser.add_argument("--min-filled-orders", type=int, default=20, help="Minimum number of filled orders required before default rollout is considered.")
    parser.add_argument("--min-participation-span", type=float, default=10.0, help="Minimum participation-rate span (percentage points) required before default rollout is considered.")
    parser.add_argument("--update-default-config", action="store_true", help="Write a preview overlay only; never overwrite repository default config files.")
    parser.add_argument(
        "--synthetic",
        action="store_true",
        help="Generate a deterministic offline fill-collection fixture and run calibration against it.",
    )
    parser.add_argument(
        "--synthetic-fixture-dir",
        type=Path,
        default=None,
        help="Optional directory for the generated synthetic fixture; defaults to <output-dir>/_synthetic_fixture.",
    )
    parser.add_argument("--synthetic-positive-count", type=int, default=24, help="Number of positive-signal synthetic fills.")
    parser.add_argument("--synthetic-negative-count", type=int, default=4, help="Number of negative-signal synthetic fills.")
    parser.add_argument("--synthetic-true-k", type=float, default=0.02, help="Ground-truth k used to generate synthetic fills.")
    parser.add_argument(
        "--synthetic-no-missing-adv",
        action="store_true",
        help="Disable the synthetic missing-ADV row used to test fit-eligibility guards.",
    )
    parser.add_argument(
        "--synthetic-no-timeout",
        action="store_true",
        help="Disable the synthetic timeout order used to test non-filled telemetry handling.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    fill_collection_root = args.fill_collection_root.resolve() if args.fill_collection_root is not None else None
    source_run_root = args.source_run_root.resolve() if args.source_run_root is not None else None
    synthetic_fixture = None
    if bool(args.synthetic):
        synthetic_fixture_dir = (
            args.synthetic_fixture_dir.resolve()
            if args.synthetic_fixture_dir is not None
            else (output_dir / "_synthetic_fixture").resolve()
        )
        synthetic_fixture = create_synthetic_slippage_calibration_fixture(
            output_dir=synthetic_fixture_dir,
            positive_count=int(args.synthetic_positive_count),
            negative_count=int(args.synthetic_negative_count),
            include_missing_adv=not bool(args.synthetic_no_missing_adv),
            include_timeout=not bool(args.synthetic_no_timeout),
            true_k=float(args.synthetic_true_k),
            alpha=float(args.alpha),
        )
        fill_collection_root = synthetic_fixture.fill_collection_root
        source_run_root = synthetic_fixture.source_run_root

    if fill_collection_root is None:
        parser.error("--fill-collection-root is required unless --synthetic is used.")

    result = calibrate_slippage(
        fill_collection_root=fill_collection_root,
        output_dir=output_dir,
        source_run_root=source_run_root,
        alpha=float(args.alpha),
        min_filled_orders=int(args.min_filled_orders),
        min_participation_span=float(args.min_participation_span),
        update_default_config=bool(args.update_default_config),
    )
    run_id = f"slippage_calibration_us_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}_{uuid4().hex[:8]}"
    run_dir = output_dir / run_id
    run_dir.mkdir(parents=True, exist_ok=True)
    artifact_paths = write_slippage_calibration_artifacts(result=result, output_dir=run_dir)
    if synthetic_fixture is not None:
        print(f"synthetic_fixture_manifest: {synthetic_fixture.manifest_path}")
        print(f"synthetic_fill_collection_root: {synthetic_fixture.fill_collection_root}")
        print(f"synthetic_source_run_root: {synthetic_fixture.source_run_root}")
        print(f"synthetic_expected_k: {synthetic_fixture.expected_k}")
    for label, path in artifact_paths.items():
        print(f"{label}: {path}")
    print(f"recommendation: {result.summary.get('recommendation')}")
    print(f"candidate_k: {result.summary.get('candidate_k')}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
