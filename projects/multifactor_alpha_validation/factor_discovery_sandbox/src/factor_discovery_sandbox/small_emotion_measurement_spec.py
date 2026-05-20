"""Freeze MeasurementSpec for small-cap emotion candidates."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from pathlib import Path

import yaml

from .small_emotion_exploratory_sweep import EXPLORATORY_GUARDS


STAGE = "D4-SMALL-EMOTION-04"


@dataclass(frozen=True)
class SmallEmotionMeasurementSpecResult:
    """Frozen MeasurementSpec output."""

    summary: dict[str, object]
    artifacts: dict[str, Path]


def write_small_emotion_measurement_spec(
    *,
    charter_path: str | Path,
    output_dir: str | Path,
) -> SmallEmotionMeasurementSpecResult:
    """Freeze a MeasurementSpec from one D3 small-emotion charter."""

    source = Path(charter_path)
    output_path = Path(output_dir)
    output_path.mkdir(parents=True, exist_ok=True)
    artifacts = _artifact_paths(output_path)

    charter = yaml.safe_load(source.read_text(encoding="utf-8"))
    spec = _measurement_spec(charter)
    artifacts["measurement_spec"].write_text(
        yaml.safe_dump(spec, sort_keys=False, allow_unicode=False),
        encoding="utf-8",
    )
    manifest = _manifest(
        measurement_spec_path=artifacts["measurement_spec"],
        source_charter_path=source,
        measurement_spec_id=str(spec["measurement_spec_id"]),
    )
    artifacts["measurement_spec_manifest"].write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    summary = _summary(spec, manifest)
    artifacts["measurement_spec_summary"].write_text(
        json.dumps(summary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    artifacts["measurement_spec_report"].write_text(_report(summary, spec), encoding="utf-8")
    return SmallEmotionMeasurementSpecResult(summary=summary, artifacts=artifacts)


def _artifact_paths(output_path: Path) -> dict[str, Path]:
    return {
        "measurement_spec": output_path / "measurement_spec.yaml",
        "measurement_spec_manifest": output_path / "measurement_spec_manifest.json",
        "measurement_spec_summary": output_path / "measurement_spec_summary.json",
        "measurement_spec_report": output_path / "measurement_spec_report.md",
    }


def _measurement_spec(charter: dict[str, object]) -> dict[str, object]:
    candidate = dict(charter["candidate"])  # type: ignore[index]
    candidate_id = str(charter["candidate_id"])
    return {
        "schema_version": "small_emotion_measurement_spec.v1",
        "stage": STAGE,
        "measurement_spec_id": candidate_id,
        "candidate_id": candidate_id,
        "candidate_family": charter.get("candidate_family", "small_cap_shock_conditioned_emotion_liquidity"),
        "source_charter_stage": charter.get("stage", ""),
        "status": "frozen_measurement_spec_not_q1_evidence",
        "thesis": charter.get("thesis", ""),
        "signal_definition": {
            "mechanism": candidate.get("mechanism"),
            "expected_direction": candidate.get("expected_direction"),
            "signal_anchor": "shock_trading_date_close",
            "tradable_timestamp_policy": "next_trading_day_after_shock_close",
            "features_allowed": [
                "same-day shock return",
                "abnormal volume using trailing observations",
                "prior 5-day return using data before shock day",
                "market regime using trailing benchmark returns",
                "market cap bucket",
                "ADV/liquidity/spread filters",
            ],
            "features_forbidden": [
                "forward returns",
                "future labels",
                "Q1 evidence outputs",
                "optimizer outputs",
                "portfolio returns",
            ],
            "filters": {
                "shock_threshold": float(candidate.get("shock_threshold", 0.0)),
                "volume_spike_threshold": float(candidate.get("volume_spike_threshold", 0.0)),
                "prior_5d_min_return": _optional_float(candidate.get("prior_5d_min_return")),
                "prior_20d_min_return": _optional_float(candidate.get("prior_20d_min_return")),
                "close_location_filter": candidate.get("close_location_filter", "all"),
                "low_price_filter": candidate.get("low_price_filter", "all"),
                "market_cap_bucket": candidate.get("market_cap_bucket", "all_small_cap"),
                "liquidity_filter": candidate.get("liquidity_filter", "all"),
                "spread_filter": candidate.get("spread_filter", "all"),
                "regime_filter": candidate.get("regime_filter", "all"),
                "adv_min_dollars": float(candidate.get("adv_min_dollars", 0.0)),
            },
            "signal_value": {
                "active_signal": -1.0,
                "meaning": "negative expected post-shock abnormal return for qualified up-shock events",
                "missing_or_unqualified": "no_view",
            },
        },
        "label_contract": {
            "primary_window": candidate.get("primary_window"),
            "diagnostic_windows": ["post_1_5", "post_1_10", "post_1_22", "post_1_44"],
            "pre_event_audit_windows": ["pre_5_1", "pre_10_1", "pre_20_1"],
            "label_start_after_signal_anchor": True,
            "label_fields_forbidden_in_features": True,
        },
        "coverage_policy": {
            "missing_signal_policy": "no_view_not_zero_alpha",
            "unqualified_events_policy": "no_view",
            "no_view_rows_ranked": False,
            "coverage_as_alpha_allowed": False,
        },
        "hard_falsifiers": list(charter.get("hard_falsifiers", [])),
        "required_q1_checks": [
            "time_split_oos",
            "shifted_date_placebo",
            "same_coverage_random_placebo",
            "large_cap_matched_shock_placebo",
            "stale_price_matched_placebo",
            "adv_capacity_matched_placebo",
            "pre_event_dominance",
            "cost_liquidity_pregate",
            "single_month_or_single_issuer_concentration",
        ],
        "downstream_boundaries": {
            "measurement_spec_written": True,
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
            "selection_bias_risk": "extreme",
            "overfit_search_used": True,
            "not_alpha_evidence": True,
            "q1_required_before_any_promotion": True,
        },
    }


def _optional_float(value: object) -> float | None:
    if value in {"", None}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _manifest(*, measurement_spec_path: Path, source_charter_path: Path, measurement_spec_id: str) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_measurement_spec_manifest.v1",
        "stage": STAGE,
        "measurement_spec_id": measurement_spec_id,
        "measurement_spec_path": str(measurement_spec_path),
        "source_charter_path": str(source_charter_path),
        "measurement_spec_hash": _file_hash(measurement_spec_path),
        "source_charter_hash": _file_hash(source_charter_path),
        "q1_entry_allowed": False,
        "q2_entry_allowed": False,
        **EXPLORATORY_GUARDS,
    }


def _summary(spec: dict[str, object], manifest: dict[str, object]) -> dict[str, object]:
    boundaries = dict(spec["downstream_boundaries"])  # type: ignore[arg-type]
    return {
        "schema_version": "small_emotion_measurement_spec_summary.v1",
        "stage": STAGE,
        "measurement_spec_id": spec["measurement_spec_id"],
        "measurement_spec_hash": manifest["measurement_spec_hash"],
        "source_charter_hash": manifest["source_charter_hash"],
        **boundaries,
        "not_alpha_evidence": True,
        "no_view_not_zero_alpha": True,
    }


def _report(summary: dict[str, object], spec: dict[str, object]) -> str:
    filters = spec["signal_definition"]["filters"]  # type: ignore[index]
    return "\n".join(
        [
            "# D4-SMALL-EMOTION-04 MeasurementSpec",
            "",
            "This freezes the selected small-cap emotion measurement definition. It is not Q1 evidence and opens no Q2, optimizer, portfolio, Alpha Registry, paper, broker, order, live, or production workflow.",
            "",
            f"- measurement_spec_id: {summary['measurement_spec_id']}",
            f"- mechanism: {spec['signal_definition']['mechanism']}",  # type: ignore[index]
            f"- primary_window: {spec['label_contract']['primary_window']}",  # type: ignore[index]
            f"- filters: {filters}",
            f"- q1_entry_allowed: {summary['q1_entry_allowed']}",
            f"- q2_entry_allowed: {summary['q2_entry_allowed']}",
            "",
        ]
    )


def _file_hash(path: Path) -> str:
    digest = hashlib.sha256()
    digest.update(path.read_bytes())
    return digest.hexdigest()
