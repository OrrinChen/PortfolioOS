from __future__ import annotations

import json
from pathlib import Path

import yaml

from factor_discovery_sandbox.small_emotion_measurement_spec import write_small_emotion_measurement_spec


def test_measurement_spec_freezes_sharpened_charter_without_downstream_artifacts(tmp_path: Path) -> None:
    charter_path = tmp_path / "d3_candidate_charter.yaml"
    charter_path.write_text(
        yaml.safe_dump(
            {
                "schema_version": "small_emotion_d3_candidate_charter.v1",
                "stage": "D3-SMALL-EMOTION-03",
                "candidate_id": "small_cap_sharpened_up_shock_reversal_post_1_22_v0",
                "candidate": {
                    "mechanism": "up_shock_reversal",
                    "expected_direction": "negative_post_shock_abnormal_return",
                    "shock_threshold": 0.05,
                    "volume_spike_threshold": 1.5,
                    "prior_5d_min_return": 0.2,
                    "prior_20d_min_return": "",
                    "close_location_filter": "all",
                    "low_price_filter": "all",
                    "market_cap_bucket": "micro",
                    "liquidity_filter": "all",
                    "spread_filter": "all",
                    "regime_filter": "market_up_20d",
                    "adv_min_dollars": 250000.0,
                    "primary_window": "post_1_22",
                },
                "timestamp_contract": {
                    "signal_anchor": "shock_trading_date_close",
                    "tradable_interpretation": "next_trading_day_after_shock_close_for_future_measurement_spec",
                },
                "coverage_policy": {"missing_coverage": "no_view_not_zero_alpha"},
                "hard_falsifiers": ["shifted_date_placebo", "same_coverage_random_placebo"],
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    result = write_small_emotion_measurement_spec(
        charter_path=charter_path,
        output_dir=tmp_path / "spec",
    )

    assert result.summary["schema_version"] == "small_emotion_measurement_spec_summary.v1"
    assert result.summary["stage"] == "D4-SMALL-EMOTION-04"
    assert result.summary["measurement_spec_id"] == "small_cap_sharpened_up_shock_reversal_post_1_22_v0"
    assert result.summary["measurement_spec_written"] is True
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False

    spec = yaml.safe_load(result.artifacts["measurement_spec"].read_text(encoding="utf-8"))
    assert spec["measurement_spec_id"] == "small_cap_sharpened_up_shock_reversal_post_1_22_v0"
    assert spec["signal_definition"]["mechanism"] == "up_shock_reversal"
    assert spec["signal_definition"]["filters"]["prior_5d_min_return"] == 0.2
    assert spec["signal_definition"]["filters"]["market_cap_bucket"] == "micro"
    assert spec["label_contract"]["primary_window"] == "post_1_22"
    assert spec["coverage_policy"]["missing_signal_policy"] == "no_view_not_zero_alpha"
    assert spec["downstream_boundaries"]["expected_return_panel_written"] is False

    manifest = json.loads(result.artifacts["measurement_spec_manifest"].read_text(encoding="utf-8"))
    assert manifest["measurement_spec_hash"]
    assert manifest["source_charter_hash"]
    assert not (tmp_path / "spec" / "expected_return_panel.csv").exists()
    assert not (tmp_path / "spec" / "signal_panel.csv").exists()
