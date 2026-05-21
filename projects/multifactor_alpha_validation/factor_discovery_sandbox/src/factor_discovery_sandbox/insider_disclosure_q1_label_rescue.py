"""Narrow Q1 label coverage rescue for D3 insider buying signals."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.insider_disclosure_q1_evidence import (
    run_open_market_buying_q1_evidence_review,
)


SUMMARY_SCHEMA_VERSION = "insider_open_market_buying_q1_label_rescue_summary.v1"
STAGE = "Q1-INSIDER-01A"

FORBIDDEN_PRICE_COLUMN_PATTERNS = (
    "expected_return",
    "forward_return",
    "future_return",
    "alpha_score",
    "optimizer",
    "portfolio",
    "q2_",
    "broker",
    "order",
    "production",
    "trading_instruction",
)

DOWNSTREAM_FLAGS = {
    "q2_entry_allowed": False,
    "optimizer_entry_allowed": False,
    "alpha_registry_update_allowed": False,
    "paper_ready": False,
    "live_ready": False,
    "broker_order_path_opened": False,
    "production_approval_claimed": False,
    "expected_return_panel_written": False,
}


@dataclass(frozen=True)
class InsiderQ1LabelCoverageRescueResult:
    """Artifacts and summary for Q1-INSIDER-01A."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def run_open_market_buying_q1_label_coverage_rescue(
    signal_panel_path: str | Path,
    baseline_price_panel_path: str | Path,
    output_dir: str | Path,
    benchmark_panel_path: str | Path | None = None,
    extra_price_panel_paths: list[str | Path] | tuple[str | Path, ...] | None = None,
    minimum_active_event_clusters: int = 500,
    minimum_event_month_count: int = 24,
    minimum_label_coverage_share: float = 0.75,
) -> InsiderQ1LabelCoverageRescueResult:
    """Merge label-only price coverage, then rerun the existing Q1 review."""

    signal_path = Path(signal_panel_path)
    baseline_price_path = Path(baseline_price_panel_path)
    benchmark_path = Path(benchmark_panel_path) if benchmark_panel_path else None
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    signal_hash_before = _sha256_file(signal_path)
    baseline_price = _read_price_like_panel(baseline_price_path, panel_name="baseline price panel")
    extra_frames: list[pd.DataFrame] = []
    extra_rows = 0
    for extra_path in extra_price_panel_paths or []:
        frame = _read_price_like_panel(Path(extra_path), panel_name=f"extra price panel {extra_path}")
        extra_rows += int(len(frame))
        extra_frames.append(frame)

    rescued_price = _merge_price_panels(baseline_price, extra_frames)
    rescued_price.to_csv(artifacts["rescued_price_panel"], index=False)

    rescued_q1 = run_open_market_buying_q1_evidence_review(
        signal_panel_path=signal_path,
        price_panel_path=artifacts["rescued_price_panel"],
        benchmark_panel_path=benchmark_path,
        output_dir=artifacts["rescued_q1_output_dir"],
        minimum_active_event_clusters=minimum_active_event_clusters,
        minimum_event_month_count=minimum_event_month_count,
        minimum_label_coverage_share=minimum_label_coverage_share,
    )

    signal_hash_after = _sha256_file(signal_path)
    rescued_label_coverage = float(rescued_q1.summary.get("label_coverage_share", 0.0) or 0.0)
    coverage_rescue_status = (
        "sufficient_label_coverage"
        if rescued_label_coverage >= minimum_label_coverage_share
        else "blocked_data_coverage_after_rescue"
    )
    summary = {
        "schema_version": SUMMARY_SCHEMA_VERSION,
        "stage": STAGE,
        "signal_panel_path": str(signal_path),
        "baseline_price_panel_path": str(baseline_price_path),
        "extra_price_panel_paths": [str(path) for path in extra_price_panel_paths or []],
        "benchmark_panel_path": str(benchmark_path) if benchmark_path else "",
        "signal_panel_hash_before": signal_hash_before,
        "signal_panel_hash_after": signal_hash_after,
        "signal_panel_hash_unchanged": signal_hash_before == signal_hash_after,
        "baseline_price_rows": int(len(baseline_price)),
        "extra_price_rows": int(extra_rows),
        "merged_extra_price_rows": int(max(0, len(rescued_price) - len(baseline_price))),
        "rescued_price_rows": int(len(rescued_price)),
        "rescued_q1_decision": rescued_q1.summary.get("q1_decision", ""),
        "rescued_q1_result_interpretation": rescued_q1.summary.get("q1_result_interpretation", ""),
        "rescued_label_coverage_share": rescued_label_coverage,
        "minimum_label_coverage_share": minimum_label_coverage_share,
        "coverage_rescue_status": coverage_rescue_status,
        "recommended_next_step": (
            "rerun_q1_with_fixed_d3_signal"
            if coverage_rescue_status == "sufficient_label_coverage"
            else "stop_q1_until_additional_price_label_source_is_available"
        ),
        "rescued_observed_primary_label_clusters": rescued_q1.summary.get("observed_primary_label_clusters", 0),
        "rescued_active_event_clusters": rescued_q1.summary.get("active_event_clusters", 0),
        "rescued_observed_event_month_count": rescued_q1.summary.get("observed_event_month_count", 0),
        "label_rescue_scope": "price_return_label_coverage_only",
        "measurement_spec_modified": False,
        "formula_modified": False,
        "signal_construction_modified": False,
        "no_view_not_zero_alpha": True,
        "promotion_gate_allowed": bool(rescued_q1.summary.get("promotion_gate_allowed", False)),
        **DOWNSTREAM_FLAGS,
    }
    _write_json(artifacts["label_coverage_rescue_summary"], summary)
    artifacts["q1_label_coverage_rescue_report"].write_text(
        _render_report(summary, rescued_q1.artifacts),
        encoding="utf-8",
    )
    return InsiderQ1LabelCoverageRescueResult(
        summary=summary,
        artifacts={
            **artifacts,
            "rescued_q1_decision_summary": rescued_q1.artifacts["q1_decision_summary"],
        },
    )


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "rescued_price_panel": output_path / "rescued_price_panel.csv",
        "rescued_q1_output_dir": output_path / "rescued_q1",
        "label_coverage_rescue_summary": output_path / "label_coverage_rescue_summary.json",
        "q1_label_coverage_rescue_report": output_path / "q1_label_coverage_rescue_report.md",
    }


def _read_price_like_panel(path: Path, panel_name: str) -> pd.DataFrame:
    frame = pd.read_csv(path)
    forbidden = _forbidden_price_columns(frame.columns)
    if forbidden:
        raise ValueError(f"forbidden price panel columns in {panel_name}: {', '.join(forbidden)}")
    required = {"ticker", "date"}
    missing = sorted(required - set(frame.columns))
    if missing:
        raise ValueError(f"{panel_name} missing required columns: {', '.join(missing)}")
    return frame


def _forbidden_price_columns(columns: pd.Index) -> list[str]:
    forbidden: list[str] = []
    for column in columns:
        lower = str(column).lower()
        if any(pattern in lower for pattern in FORBIDDEN_PRICE_COLUMN_PATTERNS):
            forbidden.append(str(column))
    return forbidden


def _merge_price_panels(baseline_price: pd.DataFrame, extra_frames: list[pd.DataFrame]) -> pd.DataFrame:
    if not extra_frames:
        merged = baseline_price.copy()
    else:
        merged = pd.concat([baseline_price, *extra_frames], ignore_index=True, sort=False)
    merged["_ticker_sort_key"] = merged["ticker"].astype(str).str.upper()
    merged["_date_sort_key"] = pd.to_datetime(merged["date"], errors="coerce")
    merged = merged.sort_values(["_ticker_sort_key", "_date_sort_key"], kind="mergesort")
    merged = merged.drop_duplicates(subset=["_ticker_sort_key", "_date_sort_key"], keep="last")
    merged = merged.drop(columns=["_ticker_sort_key", "_date_sort_key"])
    return merged.reset_index(drop=True)


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.write_text(json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def _render_report(summary: dict[str, object], q1_artifacts: dict[str, Path]) -> str:
    return "\n".join(
        [
            "# Q1-INSIDER-01A Label Coverage Rescue",
            "",
            "This is a label coverage rescue only. It only merges additional price/return label coverage and reruns the existing Q1 evidence review.",
            "",
            "It does not modify the D3 signal, MeasurementSpec, formula, role weights, cluster logic, holding windows, or event construction.",
            "",
            "It does not generate expected_return_panel artifacts and does not open Q2, optimizer, Alpha Registry, paper, broker, order, live, or production workflows.",
            "",
            "Missing label coverage remains explicit unavailable/no_view context and is not encoded as zero alpha.",
            "",
            "## Rescue Summary",
            "",
            f"- rescued_q1_decision: `{summary['rescued_q1_decision']}`",
            f"- coverage_rescue_status: `{summary['coverage_rescue_status']}`",
            f"- rescued_label_coverage_share: `{summary['rescued_label_coverage_share']}`",
            f"- minimum_label_coverage_share: `{summary['minimum_label_coverage_share']}`",
            f"- rescued_observed_primary_label_clusters: `{summary['rescued_observed_primary_label_clusters']}`",
            f"- rescued_active_event_clusters: `{summary['rescued_active_event_clusters']}`",
            f"- signal_panel_hash_unchanged: `{summary['signal_panel_hash_unchanged']}`",
            f"- measurement_spec_modified: `{summary['measurement_spec_modified']}`",
            f"- formula_modified: `{summary['formula_modified']}`",
            "",
            "## Downstream Boundary",
            "",
            f"- promotion_gate_allowed: `{summary['promotion_gate_allowed']}`",
            f"- q2_entry_allowed: `{summary['q2_entry_allowed']}`",
            f"- optimizer_entry_allowed: `{summary['optimizer_entry_allowed']}`",
            f"- alpha_registry_update_allowed: `{summary['alpha_registry_update_allowed']}`",
            f"- production_approval_claimed: `{summary['production_approval_claimed']}`",
            "",
            "## Rescued Q1 Artifacts",
            "",
            f"- q1_decision_summary: `{q1_artifacts['q1_decision_summary']}`",
            f"- q1_evidence_report: `{q1_artifacts['q1_open_market_buying_evidence_report']}`",
            "",
        ],
    )
