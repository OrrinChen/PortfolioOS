from __future__ import annotations

import argparse
import subprocess
import sys
from datetime import date
from pathlib import Path
from typing import Any

import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = ROOT / "src"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(SRC_ROOT) not in sys.path:
    sys.path.insert(0, str(SRC_ROOT))

from portfolio_os.alpha.package_audit import build_counterfactual_alpha_panel
from portfolio_os.alpha.signal_consistency_diagnostic import build_signal_consistency_report
from portfolio_os.backtest.engine import run_backtest
from portfolio_os.backtest.manifest import load_backtest_manifest
from portfolio_os.storage.snapshots import write_json, write_text
from scripts import run_us_long_horizon_signal_extension as long_horizon_script
from scripts.run_real_alpha_package_audit import DEFAULT_MANIFEST

DEFAULT_OUTPUT_DIR = ROOT / "outputs" / f"signal_consistency_diagnostic_{date.today().isoformat()}"
COUNTERFACTUAL_FORWARD_HORIZON_DAYS = 5
COUNTERFACTUAL_MAX_ABS_EXPECTED_RETURN = 0.30


def _git_head_metadata(repo_root: Path) -> dict[str, Any]:
    sha = subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo_root, text=True).strip()
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], cwd=repo_root, text=True).strip())
    return {
        "portfolioos_head_sha": f"{sha}-dirty" if dirty else sha,
        "working_tree_clean": not dirty,
    }


def _extract_active_view(alpha_panel: pd.DataFrame) -> pd.DataFrame:
    work = alpha_panel.copy()
    work["date"] = pd.to_datetime(work["date"]).dt.normalize()
    active_dates = (
        work.groupby("date")["expected_return"]
        .apply(lambda values: bool(pd.to_numeric(values, errors="coerce").abs().gt(1e-15).any()))
    )
    selected_dates = set(active_dates.loc[active_dates].index.tolist())
    return work.loc[work["date"].isin(selected_dates)].copy().reset_index(drop=True)


def _build_production_views(manifest_path: Path) -> dict[str, pd.DataFrame]:
    manifest = load_backtest_manifest(manifest_path)
    result = run_backtest(manifest.manifest_path)
    if result.alpha_panel is None:
        raise ValueError("Manifest did not produce an alpha_panel; signal consistency diagnostic requires alpha_model.enabled = true.")

    baseline = _extract_active_view(result.alpha_panel)
    signed_panel, _ = build_counterfactual_alpha_panel(
        alpha_panel=result.alpha_panel,
        negative_spread_mode="signed_spread",
        forward_horizon_days=COUNTERFACTUAL_FORWARD_HORIZON_DAYS,
        max_abs_expected_return=COUNTERFACTUAL_MAX_ABS_EXPECTED_RETURN,
    )
    signed_spread = _extract_active_view(signed_panel)
    return {"baseline": baseline, "signed_spread": signed_spread}


def _build_canonical_cross_section() -> pd.DataFrame:
    requested_tickers = long_horizon_script._load_universe_tickers(long_horizon_script.UNIVERSE_TICKERS_PATH)
    close_panel = long_horizon_script._download_close_panel(requested_tickers)
    returns_panel = close_panel.pct_change().dropna(how="all")
    horizon_frame_map = long_horizon_script._build_horizon_frame_map(returns_panel)
    operational_frame = horizon_frame_map["21d"]
    static_labels = long_horizon_script._load_static_label_frame(requested_tickers)
    shares_panel = long_horizon_script._load_shares_panel(requested_tickers, close_panel.index)
    return long_horizon_script._build_operational_cross_section_frame(
        returns_panel=returns_panel,
        close_panel=close_panel,
        operational_frame=operational_frame,
        static_labels=static_labels,
        shares_panel=shares_panel,
    )


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Compare production alpha outputs against the canonical long-horizon cross-section.")
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR)
    args = parser.parse_args(argv)

    manifest_path = args.manifest.resolve()
    output_dir = args.output_dir.resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    production_views = _build_production_views(manifest_path)
    canonical_cross_section = _build_canonical_cross_section()

    metadata = {
        **_git_head_metadata(ROOT),
        "manifest_path": str(manifest_path),
        "canonical_builder_path": str((ROOT / "scripts" / "run_us_long_horizon_signal_extension.py").resolve()),
        "canonical_builder_name": "_build_operational_cross_section_frame",
        "canonical_signal_spec": dict(long_horizon_script.CANONICAL_SIGNAL_SPEC),
        "canonical_data_source": "yfinance adjusted close proxy",
        "counterfactual_negative_spread_mode": "signed_spread",
        "counterfactual_forward_horizon_days": COUNTERFACTUAL_FORWARD_HORIZON_DAYS,
        "counterfactual_max_abs_expected_return": COUNTERFACTUAL_MAX_ABS_EXPECTED_RETURN,
    }

    report = build_signal_consistency_report(
        canonical_cross_section=canonical_cross_section,
        production_views=production_views,
        metadata=metadata,
    )

    report.per_month_frame.to_csv(output_dir / "signal_consistency_per_month.csv", index=False)
    report.pooled_summary_frame.to_csv(output_dir / "signal_consistency_pooled_summary.csv", index=False)
    write_json(
        output_dir / "signal_consistency_summary.json",
        {
            "metadata": report.metadata,
            "pooled_summary": report.pooled_summary_frame.to_dict(orient="records"),
            "per_month_row_count": int(len(report.per_month_frame)),
        },
    )
    write_text(output_dir / "signal_consistency_note.md", report.to_markdown())

    print(f"[signal-consistency] wrote artifacts to {output_dir}")


if __name__ == "__main__":
    main()
