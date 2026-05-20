from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.small_emotion_promotion_gate import run_small_emotion_promotion_gate


def test_promotion_gate_requires_full_replay_hash_and_writes_audits(tmp_path: Path) -> None:
    spec_path = _write_measurement_spec(tmp_path / "measurement_spec.yaml")
    spec_hash = hashlib.sha256(spec_path.read_bytes()).hexdigest()
    q1_dir = _write_q1_outputs(tmp_path / "q1", source_row_limit=None)
    search_grid = _write_search_grid(tmp_path / "search_grid.csv")

    result = run_small_emotion_promotion_gate(
        measurement_spec_path=spec_path,
        q1_output_dir=q1_dir,
        output_dir=tmp_path / "pg",
        required_measurement_spec_hash=spec_hash,
        search_grid_path=search_grid,
    )

    assert result.summary["schema_version"] == "small_emotion_promotion_gate_summary.v1"
    assert result.summary["stage"] == "PG-SMALL-EMOTION-01"
    assert result.summary["measurement_spec_hash"] == spec_hash
    assert result.summary["full_no_cap_q1_required"] is True
    assert result.summary["full_no_cap_q1_observed"] is True
    assert result.summary["q1_decision"] == "passed_q1_research_review"
    assert result.summary["promotion_decision"] in {
        "promote_to_q2_candidate",
        "promising_needs_full_replay_or_breadth",
        "reject_overfit_or_data_artifact",
    }
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["production_approval_claimed"] is False

    for artifact_name in {
        "search_burden_audit",
        "tail_concentration_audit",
        "data_anomaly_audit",
        "cost_liquidity_gate",
        "time_breadth_audit",
        "promotion_decision_summary",
        "promotion_gate_report",
    }:
        assert result.artifacts[artifact_name].exists()

    search = json.loads(result.artifacts["search_burden_audit"].read_text(encoding="utf-8"))
    assert search["search_grid_row_count"] == 2
    assert search["sweep_adjusted_placebo_status"] in {"pass", "warning", "fail"}

    tail = json.loads(result.artifacts["tail_concentration_audit"].read_text(encoding="utf-8"))
    assert tail["observed_primary_label_count"] == 4
    assert tail["issuer_concentration_max_share"] <= 0.5
    assert tail["sector_status"] == "available"

    anomaly = json.loads(result.artifacts["data_anomaly_audit"].read_text(encoding="utf-8"))
    assert anomaly["stale_event_count"] == 0
    assert anomaly["zero_volume_event_count"] == 0
    assert anomaly["delisting_event_count"] == 0

    cost = pd.read_csv(result.artifacts["cost_liquidity_gate"])
    assert {"adv_participation_25k_p95", "spread_proxy_p95", "entry_exit_timing"}.issubset(set(cost["metric"]))

    report = result.artifacts["promotion_gate_report"].read_text(encoding="utf-8").lower()
    assert "promotion gate only" in report
    for forbidden in ["paper ready", "live trading", "broker execution", "order generation", "production approved"]:
        assert forbidden not in report


def test_promotion_gate_stops_when_q1_is_bounded_smoke_only(tmp_path: Path) -> None:
    spec_path = _write_measurement_spec(tmp_path / "measurement_spec.yaml")
    spec_hash = hashlib.sha256(spec_path.read_bytes()).hexdigest()
    q1_dir = _write_q1_outputs(tmp_path / "q1_bounded", source_row_limit=750_000)

    result = run_small_emotion_promotion_gate(
        measurement_spec_path=spec_path,
        q1_output_dir=q1_dir,
        output_dir=tmp_path / "pg_bounded",
        required_measurement_spec_hash=spec_hash,
    )

    assert result.summary["promotion_decision"] == "bounded_smoke_only"
    assert result.summary["stop_reason"] == "bounded_smoke_only_not_promoted"
    assert result.summary["promotion_gate_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False


def test_promotion_gate_rejects_when_full_q1_fails_falsifiers(tmp_path: Path) -> None:
    spec_path = _write_measurement_spec(tmp_path / "measurement_spec.yaml")
    spec_hash = hashlib.sha256(spec_path.read_bytes()).hexdigest()
    q1_dir = _write_q1_outputs(
        tmp_path / "q1_full_failed",
        source_row_limit=None,
        q1_decision="blocked_placebo_dominance",
        falsifier_dominates=True,
    )

    result = run_small_emotion_promotion_gate(
        measurement_spec_path=spec_path,
        q1_output_dir=q1_dir,
        output_dir=tmp_path / "pg_full_failed",
        required_measurement_spec_hash=spec_hash,
    )

    assert result.summary["full_no_cap_q1_observed"] is True
    assert result.summary["promotion_decision"] == "reject_overfit_or_data_artifact"
    assert result.summary["stop_reason"] == "q1_failed_full_replay"
    assert result.summary["promotion_gate_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False


def test_promotion_gate_uses_measurement_spec_primary_window(tmp_path: Path) -> None:
    spec_path = _write_measurement_spec(tmp_path / "measurement_spec_post_1_10.yaml", primary_window="post_1_10")
    spec_hash = hashlib.sha256(spec_path.read_bytes()).hexdigest()
    q1_dir = _write_q1_outputs(tmp_path / "q1_post_1_10", source_row_limit=None, primary_window="post_1_10")

    result = run_small_emotion_promotion_gate(
        measurement_spec_path=spec_path,
        q1_output_dir=q1_dir,
        output_dir=tmp_path / "pg_post_1_10",
        required_measurement_spec_hash=spec_hash,
    )

    tail = json.loads(result.artifacts["tail_concentration_audit"].read_text(encoding="utf-8"))
    assert tail["observed_primary_label_count"] == 4
    assert result.summary["primary_window"] == "post_1_10"


def _write_measurement_spec(path: Path, *, primary_window: str = "post_1_22") -> Path:
    path.write_text(
        "\n".join(
            [
                "schema_version: small_emotion_measurement_spec.v1",
                f"measurement_spec_id: small_cap_sharpened_up_shock_reversal_{primary_window}_v0",
                "signal_definition:",
                "  mechanism: up_shock_reversal",
                "  filters:",
                "    shock_threshold: 0.05",
                "    volume_spike_threshold: 1.5",
                "    prior_5d_min_return: 0.2",
                "    market_cap_bucket: micro",
                "    regime_filter: market_up_20d",
                "    adv_min_dollars: 250000.0",
                "label_contract:",
                f"  primary_window: {primary_window}",
                "",
            ]
        ),
        encoding="utf-8",
    )
    return path


def _write_q1_outputs(
    path: Path,
    *,
    source_row_limit: int | None,
    q1_decision: str = "passed_q1_research_review",
    falsifier_dominates: bool = False,
    primary_window: str = "post_1_22",
) -> Path:
    path.mkdir(parents=True)
    data_coverage = {
        "schema_version": "small_emotion_data_coverage_report.v1",
        "data_status": "available",
        "price_row_count": 5_977_606 if source_row_limit is None else source_row_limit,
        "source_row_limit": source_row_limit,
    }
    (path / "data_coverage_report.json").write_text(json.dumps(data_coverage), encoding="utf-8")
    summary = {
        "schema_version": "small_emotion_q1_oos_summary.v1",
        "stage": "Q1-SMALL-EMOTION-01",
        "measurement_spec_id": f"small_cap_sharpened_up_shock_reversal_{primary_window}_v0",
        "q1_decision": q1_decision,
        "active_event_count": 4,
        "observed_primary_label_count": 4,
        "event_month_count": 4,
        "mean_primary_directional_return": 0.08,
        "oos_test_mean_directional_return": 0.07,
        "falsifier_dominance_count": int(falsifier_dominates),
        "promotion_gate_allowed": q1_decision == "passed_q1_research_review",
        "q2_entry_allowed": False,
        "optimizer_entry_allowed": False,
        "expected_return_panel_written": False,
        "alpha_registry_update_allowed": False,
        "production_approval_claimed": False,
        "no_view_not_zero_alpha": True,
    }
    (path / "q1_decision_summary.json").write_text(json.dumps(summary), encoding="utf-8")
    pd.DataFrame(
        [
            _event("e1", "A", "AAA", "2022-01-04", "2022-01", "tech", 300_000, 0.04, 100_000_000),
            _event("e2", "B", "BBB", "2022-02-04", "2022-02", "health", 450_000, 0.05, 120_000_000),
            _event("e3", "C", "CCC", "2022-03-04", "2022-03", "tech", 550_000, 0.06, 150_000_000),
            _event("e4", "D", "DDD", "2022-04-04", "2022-04", "energy", 800_000, 0.04, 200_000_000),
        ]
    ).to_csv(path / "q1_event_panel.csv", index=False)
    pd.DataFrame(
        [
            _label("e1", "A", "2022-01-04", "2022-01", 0.12, window=primary_window),
            _label("e2", "B", "2022-02-04", "2022-02", 0.07, window=primary_window),
            _label("e3", "C", "2022-03-04", "2022-03", 0.05, window=primary_window),
            _label("e4", "D", "2022-04-04", "2022-04", 0.08, window=primary_window),
        ]
    ).to_csv(path / "q1_window_return_panel.csv", index=False)
    pd.DataFrame(
        [
            {"falsifier_name": "shift_minus_5", "live_mean_directional_return": 0.08, "falsifier_mean_directional_return": -0.01, "falsifier_dominates_live": False},
            {
                "falsifier_name": "same_coverage_random",
                "live_mean_directional_return": 0.08,
                "falsifier_mean_directional_return": 0.09 if falsifier_dominates else 0.01,
                "falsifier_dominates_live": falsifier_dominates,
            },
        ]
    ).to_csv(path / "q1_falsifier_report.csv", index=False)
    pd.DataFrame(
        [{"guard_name": "pre_event_dominance", "guard_breached": False, "observed_value": -0.03}]
    ).to_csv(path / "q1_policy_guard_report.csv", index=False)
    return path


def _event(
    event_id: str,
    asset_id: str,
    ticker: str,
    date: str,
    month: str,
    sector: str,
    adv20: float,
    spread: float,
    market_cap: float,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "asset_id": asset_id,
        "ticker": ticker,
        "date": date,
        "event_month": month,
        "sector": sector,
        "industry": sector,
        "market_cap": market_cap,
        "adv20": adv20,
        "bid_ask_spread": spread,
        "volume": 100_000,
        "zero_volume": False,
        "stale_roll_5": 0,
        "delisting_within_label_window": False,
        "shock_return": 0.06,
        "directional_return": 0.08,
        "no_view_not_zero_alpha": True,
    }


def _label(
    event_id: str,
    asset_id: str,
    date: str,
    month: str,
    directional: float,
    *,
    window: str = "post_1_22",
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "asset_id": asset_id,
        "date": date,
        "event_month": month,
        "window": window,
        "label_status": "observed",
        "directional_return": directional,
        "abnormal_return": -directional,
    }


def _write_search_grid(path: Path) -> Path:
    pd.DataFrame(
        [
            {"candidate": "chosen", "mean_directional_return": 0.08, "hit_rate": 0.75},
            {"candidate": "placebo_like", "mean_directional_return": 0.01, "hit_rate": 0.50},
        ]
    ).to_csv(path, index=False)
    return path
