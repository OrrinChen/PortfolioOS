"""Run FD-S4.2 small-cap slow / capacity-filtered diagnostic rerun."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.small_cap_s4_2_diagnostic import run_small_cap_s4_2_diagnostic


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = (
    REPO_ROOT
    / "data"
    / "cache"
    / "wrds_multifactor"
    / "small_cap_us_daily"
    / "standardized"
    / "research_mode_dataset_manifest.yaml"
)
DEFAULT_SOURCE_DIR = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "small_cap"
    / "family_candidates"
    / "quality_residual_momentum"
)
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "small_cap"
    / "family_candidates"
    / "quality_residual_momentum_s4_2"
)
DEFAULT_TARGET_CACHE_DIR = REPO_ROOT / "outputs" / "factor_discovery" / "small_cap" / "target_cache"
DEFAULT_REPORT = REPO_ROOT / "reports" / "factor_discovery_small_cap_s4_2_slow_capacity_filtered_diagnostic.md"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run FD-S4.2 slow/capacity-filtered diagnostic.")
    parser.add_argument("--manifest", default=str(DEFAULT_MANIFEST))
    parser.add_argument("--source-signal-panel", default=str(DEFAULT_SOURCE_DIR / "monthly_signal_panel_cache.csv"))
    parser.add_argument("--source-target-panel", default=str(DEFAULT_SOURCE_DIR / "forward_target_panel_cache.csv"))
    parser.add_argument("--target-cache-output-dir", default=str(DEFAULT_TARGET_CACHE_DIR))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--report", default=str(DEFAULT_REPORT))
    args = parser.parse_args()

    result = run_small_cap_s4_2_diagnostic(
        source_signal_panel_path=args.source_signal_panel,
        source_target_panel_path=args.source_target_panel,
        target_cache_output_dir=args.target_cache_output_dir,
        output_dir=args.output_dir,
        report_path=args.report,
        manifest_path=args.manifest,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"decision_label={result.summary['decision_label']}")
    print(f"six_month_target_available={str(result.summary['six_month_target_available']).lower()}")
    print(f"allocator_entry_allowed={str(result.summary['allocator_entry_allowed']).lower()}")
    print(f"q1_entry_allowed={str(result.summary['q1_entry_allowed']).lower()}")
    print(f"q2_entry_allowed={str(result.summary['q2_entry_allowed']).lower()}")
    print(f"alpha_registry_update_allowed={str(result.summary['alpha_registry_update_allowed']).lower()}")
    print(f"direct_q2_entry_allowed={str(result.summary['direct_q2_entry_allowed']).lower()}")
    print(f"not_alpha_evidence={str(result.summary['not_alpha_evidence']).lower()}")
    for name, path in result.artifacts.items():
        print(f"{name}={path}")


if __name__ == "__main__":
    main()
