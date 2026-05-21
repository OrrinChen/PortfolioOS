"""Run FD-R4.1 / FD-R5.2 weighting reliability gate."""

from __future__ import annotations

import argparse
from pathlib import Path

from factor_discovery_sandbox.weighting_reliability import run_weighting_reliability_gate


REPO_ROOT = Path(__file__).resolve().parents[1]
REAL_DATA_DAILY = REPO_ROOT / "outputs" / "factor_discovery" / "real_data_daily"


def main() -> None:
    parser = argparse.ArgumentParser(description="Run Factor Discovery weighting reliability gate.")
    parser.add_argument("--factor-panel", default=str(REAL_DATA_DAILY / "fd_r3" / "real_factor_panel.csv"))
    parser.add_argument("--rolling-weights", default=str(REAL_DATA_DAILY / "fd_r4" / "rolling_icir_real.csv"))
    parser.add_argument("--oos-score-panel", default=str(REAL_DATA_DAILY / "fd_r4" / "oos_factor_score_panel_real.csv"))
    parser.add_argument("--placebo-report", default=str(REAL_DATA_DAILY / "fd_r5" / "placebo_report.csv"))
    parser.add_argument("--output-dir", default=str(REPO_ROOT / "outputs" / "factor_discovery" / "research_mode"))
    parser.add_argument("--report", default=str(REPO_ROOT / "reports" / "factor_discovery_weighting_reliability_report.md"))
    parser.add_argument("--train-window-months", type=int, default=36)
    parser.add_argument("--shrink-lambda", dest="shrink_lambdas", type=float, action="append", default=None)
    parser.add_argument("--ridge-alpha", dest="ridge_alphas", type=float, action="append", default=None)
    args = parser.parse_args()

    result = run_weighting_reliability_gate(
        factor_panel_path=args.factor_panel,
        rolling_weights_path=args.rolling_weights,
        oos_score_panel_path=args.oos_score_panel,
        placebo_report_path=args.placebo_report,
        output_dir=args.output_dir,
        report_path=args.report,
        train_window_months=args.train_window_months,
        shrink_lambdas=tuple(args.shrink_lambdas or [6.0, 12.0, 24.0]),
        ridge_alphas=tuple(args.ridge_alphas or [1.0, 10.0]),
    )
    print(f"schema_version={result.summary['schema_version']}")
    print(f"stage={result.summary['stage']}")
    print(f"decision={result.summary['decision']}")
    print(f"rolling_icir_overfit_noise_failure={str(result.summary['rolling_icir_overfit_noise_failure']).lower()}")
    print(f"estimator_count={result.summary['estimator_count']}")
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
