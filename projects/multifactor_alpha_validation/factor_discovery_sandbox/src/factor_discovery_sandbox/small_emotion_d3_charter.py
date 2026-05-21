"""D3 charter freezer for small-cap emotion top-pocket candidates."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import pandas as pd
import yaml

from .small_emotion_exploratory_sweep import EXPLORATORY_GUARDS


CANDIDATE_ID = "small_cap_up_shock_reversal_post_1_22_v0"
SHARPENED_CANDIDATE_ID = "small_cap_sharpened_up_shock_reversal_post_1_22_v0"


@dataclass(frozen=True)
class SmallEmotionD3CharterResult:
    """D3 charter output for one frozen small-emotion candidate."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def write_small_emotion_d3_charter(
    *,
    freeze_review_path: str | Path,
    top_pocket_summary_path: str | Path,
    chunk_metrics_path: str | Path,
    output_dir: str | Path,
) -> SmallEmotionD3CharterResult:
    """Write a D3 charter from an approved E0 top-pocket replay."""

    freeze_path = Path(freeze_review_path)
    summary_path = Path(top_pocket_summary_path)
    metrics_path = Path(chunk_metrics_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    freeze_review = json.loads(freeze_path.read_text(encoding="utf-8"))
    top_summary = json.loads(summary_path.read_text(encoding="utf-8"))
    metrics = pd.read_csv(metrics_path) if metrics_path.exists() else pd.DataFrame()
    if not bool(freeze_review.get("candidate_can_be_reviewed_for_d3_freeze", False)):
        raise ValueError("top-pocket replay is not eligible for D3 charter freeze")

    candidate_id = _candidate_id(top_summary)
    charter = _charter(
        freeze_review=freeze_review,
        top_summary=top_summary,
        metrics=metrics,
        candidate_id=candidate_id,
    )
    artifacts["d3_candidate_charter"].write_text(
        yaml.safe_dump(charter, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    manifest = _manifest(
        charter_path=artifacts["d3_candidate_charter"],
        freeze_review_path=freeze_path,
        top_pocket_summary_path=summary_path,
        chunk_metrics_path=metrics_path,
        candidate_id=candidate_id,
    )
    artifacts["d3_charter_manifest"].write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary = _summary(charter=charter, manifest=manifest)
    artifacts["d3_charter_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["d3_charter_report"].write_text(_report(summary, charter), encoding="utf-8")
    return SmallEmotionD3CharterResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "d3_candidate_charter": output_path / "d3_candidate_charter.yaml",
        "d3_charter_manifest": output_path / "d3_charter_manifest.json",
        "d3_charter_summary": output_path / "d3_charter_summary.json",
        "d3_charter_report": output_path / "d3_charter_report.md",
    }


def _candidate_id(top_summary: dict[str, object]) -> str:
    if top_summary.get("stage") == "E0-SMALL-EMOTION-04A":
        return SHARPENED_CANDIDATE_ID
    return CANDIDATE_ID


def _charter(
    *,
    freeze_review: dict[str, object],
    top_summary: dict[str, object],
    metrics: pd.DataFrame,
    candidate_id: str,
) -> dict[str, object]:
    candidate = dict(freeze_review["candidate"])  # type: ignore[index]
    return {
        "schema_version": "small_emotion_d3_candidate_charter.v1",
        "stage": "D3-SMALL-EMOTION-03",
        "candidate_id": candidate_id,
        "candidate_family": "small_cap_shock_conditioned_emotion_liquidity",
        "source_stage": top_summary.get("stage", "E0-SMALL-EMOTION-02A"),
        "thesis": (
            "Small-cap names with abnormal-volume positive shocks may overshoot "
            "because retail attention and liquidity demand chase the shock; the "
            "frozen candidate tests whether the post-shock path reverses over the "
            "predeclared window."
        ),
        "candidate": {
            "mechanism": candidate.get("mechanism"),
            "expected_direction": "negative_post_shock_abnormal_return",
            "shock_threshold": float(candidate.get("shock_threshold", 0.0)),
            "volume_spike_threshold": float(candidate.get("volume_spike_threshold", 0.0)),
            "prior_5d_min_return": candidate.get("prior_5d_min_return", ""),
            "prior_20d_min_return": candidate.get("prior_20d_min_return", ""),
            "close_location_filter": candidate.get("close_location_filter", "all"),
            "low_price_filter": candidate.get("low_price_filter", "all"),
            "market_cap_bucket": candidate.get("market_cap_bucket"),
            "liquidity_filter": candidate.get("liquidity_filter"),
            "spread_filter": candidate.get("spread_filter", "all"),
            "regime_filter": candidate.get("regime_filter", "all"),
            "stale_filter": candidate.get("stale_filter"),
            "adv_min_dollars": float(candidate.get("adv_min_dollars", 0.0)),
            "primary_window": candidate.get("window"),
            "signal_state": "candidate_charter_only_no_signal_built",
        },
        "universe_contract": {
            "security_scope": "US common stocks where local PIT daily data is available",
            "market_cap_scope": "small-cap universe used by E0 sweep",
            "exclude_or_no_view": [
                "missing adjusted close or return",
                "missing volume or dollar volume",
                "missing benchmark return",
                "missing ADV capacity input",
                "zero-volume or stale rows blocked by preparation guards",
                "below frozen ADV threshold",
            ],
        },
        "timestamp_contract": {
            "signal_anchor": "shock_trading_date_close",
            "tradable_interpretation": "next_trading_day_after_shock_close_for_future_measurement_spec",
            "label_window": candidate.get("window"),
            "same_close_trading_allowed": False,
            "forward_return_fields_allowed_in_features": False,
        },
        "coverage_policy": {
            "missing_coverage": "no_view_not_zero_alpha",
            "no_view_rows_ranked": False,
            "coverage_as_alpha_allowed": False,
        },
        "e0_replay_evidence": {
            "aggregate_active_event_count": int(top_summary.get("aggregate_active_event_count", 0) or 0),
            "observed_chunk_count": int(top_summary.get("observed_chunk_count", 0) or 0),
            "positive_chunk_count": int(top_summary.get("positive_chunk_count", 0) or 0),
            "weighted_mean_directional_return": float(top_summary.get("weighted_mean_directional_return", 0.0) or 0.0),
            "weighted_hit_rate": float(top_summary.get("weighted_hit_rate", 0.0) or 0.0),
            "chunk_rows": int(len(metrics)),
            "interpretation": "in_sample_exploratory_pocket_only_not_alpha_evidence",
        },
        "hard_falsifiers": [
            "shifted_date_placebo",
            "same_coverage_random_placebo",
            "large_cap_matched_shock_placebo",
            "stale_price_matched_placebo",
            "adv_capacity_matched_placebo",
            "pre_event_dominance",
            "cost_liquidity_pregate_failure",
            "single_chunk_or_single_month_concentration",
            "microcap_only_or_illiquid_only_dependency",
        ],
        "allowed_next_steps": [
            "write_measurement_spec_draft_for_this_candidate_only",
            "build_signal_panel_only_after_measurement_spec_freeze",
            "run_q1_only_after_signal_panel_and_falsifier_pack_exist",
        ],
        "forbidden_next_steps": [
            "direct_q1_without_measurement_spec",
            "q2_entry",
            "expected_return_panel",
            "optimizer_or_portfolio_input",
            "alpha_registry_promotion",
            "paper_live_broker_order_production_workflow",
        ],
        "downstream_boundaries": {
            "d3_charter_written": True,
            "measurement_spec_written": False,
            "formula_score_written": False,
            "q1_entry_allowed": False,
            "q2_entry_allowed": False,
            "expected_return_panel_written": False,
            "optimizer_entry_allowed": False,
            "portfolio_construction_allowed": False,
            "alpha_registry_update_allowed": False,
            "paper_ready": False,
            "live_ready": False,
            "broker_order_path_opened": False,
            "production_approval_claimed": False,
        },
        "risk_disclosure": {
            "selection_bias_risk": "high",
            "overfit_search_used": True,
            "requires_oos_and_placebo_after_freeze": True,
            "not_alpha_evidence": True,
        },
    }


def _manifest(
    *,
    charter_path: Path,
    freeze_review_path: Path,
    top_pocket_summary_path: Path,
    chunk_metrics_path: Path,
    candidate_id: str,
) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_d3_charter_manifest.v1",
        "stage": "D3-SMALL-EMOTION-03",
        "candidate_id": candidate_id,
        "d3_candidate_charter_path": str(charter_path),
        "source_freeze_review_path": str(freeze_review_path),
        "source_top_pocket_summary_path": str(top_pocket_summary_path),
        "source_chunk_metrics_path": str(chunk_metrics_path),
        "candidate_charter_hash": _file_hash(charter_path),
        "source_freeze_review_hash": _file_hash(freeze_review_path),
        "source_top_pocket_summary_hash": _file_hash(top_pocket_summary_path),
        "source_chunk_metrics_hash": _file_hash(chunk_metrics_path) if chunk_metrics_path.exists() else "",
        "measurement_spec_written": False,
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        **EXPLORATORY_GUARDS,
    }


def _summary(*, charter: dict[str, object], manifest: dict[str, object]) -> dict[str, object]:
    boundaries = charter["downstream_boundaries"]  # type: ignore[index]
    return {
        "schema_version": "small_emotion_d3_charter_summary.v1",
        "stage": "D3-SMALL-EMOTION-03",
        "candidate_id": charter["candidate_id"],
        "d3_charter_written": True,
        "candidate_charter_hash": manifest["candidate_charter_hash"],
        "allowed_next_step": "write_measurement_spec_draft_for_this_candidate_only",
        **boundaries,  # type: ignore[arg-type]
        "not_alpha_evidence": True,
        "no_view_not_zero_alpha": True,
    }


def _report(summary: dict[str, object], charter: dict[str, object]) -> str:
    candidate = charter["candidate"]  # type: ignore[index]
    evidence = charter["e0_replay_evidence"]  # type: ignore[index]
    return "\n".join(
        [
            "# D3-SMALL-EMOTION-03 Candidate Charter",
            "",
            "This freezes one exploratory small-cap shock candidate for possible MeasurementSpec drafting. It is not alpha evidence and does not open Q1, Q2, optimizer, portfolio, Alpha Registry, paper, broker, order, live, or production workflows.",
            "",
            f"- candidate_id: {summary['candidate_id']}",
            f"- mechanism: {candidate['mechanism']}",
            f"- primary_window: {candidate['primary_window']}",
            f"- aggregate_active_event_count: {evidence['aggregate_active_event_count']}",
            f"- positive_chunk_count: {evidence['positive_chunk_count']}",
            f"- weighted_mean_directional_return: {evidence['weighted_mean_directional_return']}",
            f"- measurement_spec_written: {summary['measurement_spec_written']}",
            f"- q1_entry_allowed: {summary['q1_entry_allowed']}",
            "",
        ]
    )


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()
