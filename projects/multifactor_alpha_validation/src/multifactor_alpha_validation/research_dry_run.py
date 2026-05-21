from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from multifactor_alpha_validation.alpha_view_mapper import write_alpha_view_outputs
from multifactor_alpha_validation.data_contract import run_research_mode_preflight
from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.q1_evidence import build_q1_evidence, write_q1_evidence_outputs
from multifactor_alpha_validation.signal_builders import build_signal_panels, write_signal_outputs


@dataclass(frozen=True)
class ResearchDryRunResult:
    preflight_ready: bool
    factor_ids: tuple[str, ...]
    signal_timestamp_check_passed: bool
    same_close_trading_used: bool
    allocator_ran: bool
    signal_output_dir: str
    alpha_view_output_dir: str
    q1_output_dir: str
    benchmark_attribution_path: str
    report_path: str
    allocator_output_path: str | None


_ALLOWED_DRY_RUN_FACTORS = ("momentum_12_1", "reversal_5_1", "low_vol_60d")


def run_first_research_dry_run(
    manifest_path: Path,
    output_dir: Path,
    factor_spec_dir: Path = Path("projects/multifactor_alpha_validation/factor_specs"),
) -> ResearchDryRunResult:
    output_dir.mkdir(parents=True, exist_ok=True)
    preflight = run_research_mode_preflight(manifest_path, output_dir / "preflight")
    if not preflight.research_mode_ready:
        raise ValueError(f"research preflight is blocked: {list(preflight.blockers)}")

    specs = [spec for spec in load_factor_specs(factor_spec_dir) if spec.factor_id in _ALLOWED_DRY_RUN_FACTORS]
    specs.sort(key=lambda spec: _ALLOWED_DRY_RUN_FACTORS.index(spec.factor_id))
    signals = build_signal_panels(specs)

    signal_dir = output_dir / "signals"
    alpha_view_dir = output_dir / "alpha_views"
    q1_dir = output_dir / "q1_evidence"
    benchmark_path = output_dir / "benchmark_attribution.csv"
    report_path = output_dir / "first_research_dry_run_report.md"

    write_signal_outputs(signals, signal_dir)
    write_alpha_view_outputs(specs, signals.signal_panels, alpha_view_dir)
    q1 = build_q1_evidence(specs, signals.signal_panels)
    write_q1_evidence_outputs(q1, q1_dir)
    benchmark = _build_benchmark_readout(q1.factor_evidence_table)
    benchmark.to_csv(benchmark_path, index=False)

    timestamp_check = _timestamp_check(signals.signal_panels)
    same_close_used = _same_close_used(signals.signal_panels)
    report_path.write_text(
        _render_dry_run_report(preflight.research_mode_ready, specs, timestamp_check, same_close_used),
        encoding="utf-8",
    )
    return ResearchDryRunResult(
        preflight_ready=preflight.research_mode_ready,
        factor_ids=tuple(spec.factor_id for spec in specs),
        signal_timestamp_check_passed=timestamp_check,
        same_close_trading_used=same_close_used,
        allocator_ran=False,
        signal_output_dir=str(signal_dir),
        alpha_view_output_dir=str(alpha_view_dir),
        q1_output_dir=str(q1_dir),
        benchmark_attribution_path=str(benchmark_path),
        report_path=str(report_path),
        allocator_output_path=None,
    )


def _build_benchmark_readout(evidence: pd.DataFrame) -> pd.DataFrame:
    rows: list[dict[str, object]] = []
    for row in evidence.itertuples(index=False):
        rows.append(
            {
                "schema_version": "research_dry_run_benchmark_attribution.v1",
                "factor_id": row.factor_id,
                "raw_return": round(float(row.top_bottom_spread), 6),
                "qqq_relative_return": round(float(row.benchmark_relative_spread), 6),
                "beta_adjusted_return": round(float(row.beta_adjusted_spread), 6),
                "readout_status": "dry_run_fixture_not_alpha_evidence",
            }
        )
    return pd.DataFrame(rows)


def _timestamp_check(signal_panels: dict[str, pd.DataFrame]) -> bool:
    for panel in signal_panels.values():
        if not (panel["visibility_timestamp"] <= panel["tradable_timestamp"]).all():
            return False
    return True


def _same_close_used(signal_panels: dict[str, pd.DataFrame]) -> bool:
    for panel in signal_panels.values():
        signal_dates = panel["signal_timestamp"].astype(str).str.slice(0, 10)
        tradable_dates = panel["tradable_timestamp"].astype(str).str.slice(0, 10)
        if (signal_dates == tradable_dates).any():
            return True
    return False


def _render_dry_run_report(
    preflight_ready: bool,
    specs: list[object],
    timestamp_check: bool,
    same_close_used: bool,
) -> str:
    payload = {
        "preflight_ready": preflight_ready,
        "factor_ids": [getattr(spec, "factor_id") for spec in specs],
        "signal_timestamp_check_passed": timestamp_check,
        "same_close_trading_used": same_close_used,
        "allocator": "allocator not run",
        "non_claim": "This dry run does not claim alpha success.",
    }
    return "\n".join(
        [
            "# First Research Dry Run",
            "",
            "This local PIT-ready fixture dry run does not claim alpha success.",
            "",
            "Allocator not run; allowed layers are signal builder, AlphaView mapper, Q1 evidence, and benchmark attribution.",
            "",
            "```json",
            json.dumps(payload, indent=2, sort_keys=True),
            "```",
            "",
        ]
    )
