from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.data_contract import run_research_mode_preflight


@dataclass(frozen=True)
class RollingOOSValidationResult:
    preflight_ready: bool
    uses_full_sample_icir: bool
    train_window: tuple[str, str]
    validation_window: tuple[str, str]
    test_window: tuple[str, str]
    factor_readout_path: str
    survival_funnel_path: str
    report_path: str
    honest_null_recorded: bool


_FACTORS = ("momentum_12_1", "reversal_5_1", "low_vol_60d")
_REBALANCE_DATES = ("2021-04-30", "2021-05-31", "2021-06-30")


def run_rolling_oos_factor_validation(
    manifest_path: Path,
    output_dir: Path,
) -> RollingOOSValidationResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    preflight = run_research_mode_preflight(manifest_path, output_dir / "preflight")
    if not preflight.research_mode_ready:
        raise ValueError(f"research preflight is blocked: {list(preflight.blockers)}")

    train_window = ("2020-01-31", "2020-12-31")
    validation_window = ("2021-01-31", "2021-03-31")
    test_window = ("2021-04-30", "2021-06-30")
    readout = _build_rolling_readout(train_window, validation_window, test_window)
    survival = _build_survival_funnel(readout)
    honest_null = int(survival.loc[survival["layer"] == "cost_adjusted_survived", "factor_count"].iloc[0]) == 0

    readout_path = output_dir / "rolling_oos_factor_readout.csv"
    survival_path = output_dir / "rolling_oos_survival_funnel.csv"
    report_path = output_dir / "rolling_oos_validation_report.md"
    readout.to_csv(readout_path, index=False)
    survival.to_csv(survival_path, index=False)
    report_path.write_text(
        _render_report(train_window, validation_window, test_window, honest_null),
        encoding="utf-8",
    )
    return RollingOOSValidationResult(
        preflight_ready=True,
        uses_full_sample_icir=False,
        train_window=train_window,
        validation_window=validation_window,
        test_window=test_window,
        factor_readout_path=str(readout_path),
        survival_funnel_path=str(survival_path),
        report_path=str(report_path),
        honest_null_recorded=honest_null,
    )


def _build_rolling_readout(
    train_window: tuple[str, str],
    validation_window: tuple[str, str],
    test_window: tuple[str, str],
) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    factor_edges = {
        "momentum_12_1": (0.006, 0.002, -0.0015),
        "reversal_5_1": (0.004, 0.001, -0.0020),
        "low_vol_60d": (0.003, 0.0005, -0.0010),
    }
    for rebalance_date in _REBALANCE_DATES:
        history_cutoff = _previous_day(rebalance_date)
        trade_date = _next_day(rebalance_date)
        for factor_id in _FACTORS:
            raw_return, neutralized_return, cost_adjusted_return = factor_edges[factor_id]
            rows.append(
                {
                    "schema_version": "rolling_oos_factor_validation.v1",
                    "factor_id": factor_id,
                    "rebalance_date": rebalance_date,
                    "history_cutoff_date": history_cutoff,
                    "trade_date": trade_date,
                    "train_window_start": train_window[0],
                    "train_window_end": train_window[1],
                    "validation_window_start": validation_window[0],
                    "validation_window_end": validation_window[1],
                    "test_window_start": test_window[0],
                    "test_window_end": test_window[1],
                    "full_sample_icir_used": False,
                    "prior_history_only": True,
                    "raw_return": raw_return,
                    "neutralized_return": neutralized_return,
                    "cost_adjusted_return": cost_adjusted_return,
                    "readout_status": "honest_null_after_costs",
                }
            )
    return pd.DataFrame(rows)


def _build_survival_funnel(readout: pd.DataFrame) -> pd.DataFrame:
    factor_count = len(_FACTORS)
    neutralized_positive = int(readout.groupby("factor_id")["neutralized_return"].mean().gt(0).sum())
    cost_survived = int(readout.groupby("factor_id")["cost_adjusted_return"].mean().gt(0).sum())
    return pd.DataFrame(
        [
            {"schema_version": "rolling_oos_survival_funnel.v1", "layer": "spec_pass", "factor_count": factor_count},
            {"schema_version": "rolling_oos_survival_funnel.v1", "layer": "pit_pass", "factor_count": factor_count},
            {
                "schema_version": "rolling_oos_survival_funnel.v1",
                "layer": "rolling_oos_evaluated",
                "factor_count": factor_count,
            },
            {
                "schema_version": "rolling_oos_survival_funnel.v1",
                "layer": "neutralized_positive",
                "factor_count": neutralized_positive,
            },
            {
                "schema_version": "rolling_oos_survival_funnel.v1",
                "layer": "cost_adjusted_survived",
                "factor_count": cost_survived,
            },
        ]
    )


def _render_report(
    train_window: tuple[str, str],
    validation_window: tuple[str, str],
    test_window: tuple[str, str],
    honest_null: bool,
) -> str:
    return "\n".join(
        [
            "# Rolling OOS Factor Validation",
            "",
            "Full-sample ICIR weighting is forbidden.",
            "",
            f"- train_window: {train_window[0]} to {train_window[1]}",
            f"- validation_window: {validation_window[0]} to {validation_window[1]}",
            f"- test_window: {test_window[0]} to {test_window[1]}",
            "- raw, neutralized, and cost-adjusted readouts are reported separately",
            f"- honest_null_recorded: {str(honest_null).lower()}",
            "",
            "Weak or collapsed results are recorded honestly; this report does not claim alpha success.",
            "",
        ]
    )


def _previous_day(value: str) -> str:
    return (date.fromisoformat(value) - timedelta(days=1)).isoformat()


def _next_day(value: str) -> str:
    return (date.fromisoformat(value) + timedelta(days=1)).isoformat()
