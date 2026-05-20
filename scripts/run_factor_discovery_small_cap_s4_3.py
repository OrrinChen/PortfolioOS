"""Run FD-S4.3 small-cap capacity-filtered live-signal preregistration gate."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.small_cap_s4_3_preregistration import run_small_cap_s4_3_preregistration


REPO_ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_DIR = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "small_cap"
    / "family_candidates"
    / "quality_residual_momentum"
)
DEFAULT_S4_2_DIR = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "small_cap"
    / "family_candidates"
    / "quality_residual_momentum_s4_2"
)
DEFAULT_TARGET_CACHE = (
    REPO_ROOT / "outputs" / "factor_discovery" / "small_cap" / "target_cache" / "forward_returns_1m_3m_6m.csv"
)
DEFAULT_OUTPUT_DIR = (
    REPO_ROOT
    / "outputs"
    / "factor_discovery"
    / "small_cap"
    / "family_candidates"
    / "capacity_filtered_live_s4_3"
)
DEFAULT_REPORT = REPO_ROOT / "reports" / "factor_discovery_small_cap_capacity_filtered_live_s4_3.md"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run FD-S4.3 capacity-filtered live-signal gate.")
    parser.add_argument("--source-signal-panel", default=str(DEFAULT_SOURCE_DIR / "monthly_signal_panel_cache.csv"))
    parser.add_argument("--source-target-cache", default=str(DEFAULT_TARGET_CACHE))
    parser.add_argument("--s4-2-decision", default=str(DEFAULT_S4_2_DIR / "s4_2_decision.json"))
    parser.add_argument("--s4-2-grid", default=str(DEFAULT_S4_2_DIR / "slow_signal_validation_grid.csv"))
    parser.add_argument("--output-dir", default=str(DEFAULT_OUTPUT_DIR))
    parser.add_argument("--report", default=str(DEFAULT_REPORT))
    args = parser.parse_args()

    result = run_small_cap_s4_3_preregistration(
        source_signal_panel_path=args.source_signal_panel,
        source_target_cache_path=args.source_target_cache,
        s4_2_decision_path=args.s4_2_decision,
        s4_2_grid_path=args.s4_2_grid,
        output_dir=args.output_dir,
        report_path=args.report,
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"decision_label={result.summary['decision_label']}")
    print(f"confirmation_available={str(result.summary['confirmation_available']).lower()}")
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
