from __future__ import annotations

import argparse
from pathlib import Path

from portfolio_os.alpha.package_audit import build_real_alpha_package_audit
from portfolio_os.alpha.research import load_alpha_returns_panel
from portfolio_os.backtest.engine import run_backtest
from portfolio_os.backtest.manifest import load_backtest_manifest
from portfolio_os.storage.snapshots import write_json, write_text


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_MANIFEST = ROOT / "data" / "backtest_samples" / "manifest_us_expanded_alpha_phase_1_5.yaml"
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "real_alpha_package_audit_2026-04-16"


def main() -> None:
    parser = argparse.ArgumentParser(description="Audit the current real alpha package on an existing backtest manifest.")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args()

    manifest_path = args.manifest.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    manifest = load_backtest_manifest(manifest_path)
    result = run_backtest(manifest.manifest_path)
    if result.alpha_panel is None:
        raise ValueError("Manifest did not produce an alpha_panel; real alpha package audit requires alpha_model.enabled = true.")

    returns_panel = load_alpha_returns_panel(manifest.returns_file)
    audit = build_real_alpha_package_audit(
        rebalance_schedule=result.summary["rebalance_schedule"],
        alpha_panel=result.alpha_panel,
        period_attribution=result.period_attribution,
        returns_panel=returns_panel,
    )

    audit.coverage_frame.to_csv(output_dir / "coverage_by_rebalance.csv", index=False)
    audit.mapping_frame.to_csv(output_dir / "mapping_by_active_rebalance.csv", index=False)
    audit.thickness_frame.to_csv(output_dir / "thickness_by_active_period.csv", index=False)

    summary_payload = {
        "manifest_path": str(manifest.manifest_path),
        "alpha_model": result.summary.get("alpha_model", {}),
        "comparison": result.summary.get("comparison", {}),
        **audit.summary_payload,
    }
    write_json(output_dir / "real_alpha_package_audit_summary.json", summary_payload)
    write_text(output_dir / "real_alpha_package_audit.md", audit.report_markdown)

    print(f"[real-alpha-audit] wrote artifacts to {output_dir}")
    print(
        "[real-alpha-audit] "
        f"ready={summary_payload['coverage']['alpha_ready_count']}/{summary_payload['coverage']['rebalance_count']}, "
        f"active={summary_payload['coverage']['alpha_active_count']}, "
        f"mean_rank_ic={summary_payload['mapping']['mean_rank_ic']:.4f}, "
        f"net_active_pnl={summary_payload['thickness']['net_active_pnl']:.2f}"
    )


if __name__ == "__main__":
    main()
