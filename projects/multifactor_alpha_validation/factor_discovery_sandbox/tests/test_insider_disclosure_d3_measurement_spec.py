from __future__ import annotations

from pathlib import Path

import yaml


SPEC_PATH = Path(
    "projects/multifactor_alpha_validation/factor_discovery_sandbox/"
    "factor_specs/insider_disclosure_2023/open_market_insider_buying_post_2023_v0.yaml",
)


def test_d3_insider_measurement_spec_freezes_only_open_market_buy_after_real_d2_gate() -> None:
    payload = yaml.safe_load(SPEC_PATH.read_text(encoding="utf-8"))

    assert payload["schema_version"] == "track_a_measurement_spec.v1"
    assert payload["measurement_spec_id"] == "open_market_insider_buying_post_2023_v0"
    assert payload["status"] == "d3_measurement_spec_frozen_after_real_d2_observable"
    assert payload["source_d2_observability_decision"]["overall_decision"] == "observable"
    assert payload["source_d2_observability_decision"]["allow_d3_charter_for"] == [
        "open_market_insider_buying_post_2023",
    ]
    assert payload["source_d2_observability_decision"]["open_market_buy_gate"]["passed"] is True
    assert payload["source_d2_observability_decision"]["open_market_buy_gate"]["covered_count"] == 1458
    assert payload["source_d2_observability_decision"]["open_market_buy_gate"]["coverage_share"] == 0.810901

    primary = payload["primary_measurement"]
    assert primary["include_transaction_codes"] == ["P"]
    assert primary["expected_direction"] == "positive"
    assert primary["planned_sell_policy"] == "no_view_diagnostic_only"
    assert primary["discretionary_sell_policy"] == "out_of_scope_contrast_only"
    assert "S" not in primary["include_transaction_codes"]
    assert payload["timestamp_contract"]["return_anchor"] == "tradable_timestamp"
    assert payload["timestamp_contract"]["transaction_date_policy"] == "never_return_anchor"
    assert payload["no_view_policy"]["missing_coverage"] == "explicit_no_view_abstain_not_zero"
    assert payload["formula_score_written"] is False
    assert payload["q1_entry_allowed"] is False
    assert payload["q2_entry_allowed"] is False
    assert payload["production_approval_claimed"] is False

    text = SPEC_PATH.read_text(encoding="utf-8").lower()
    for forbidden in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "alpha passed",
        "q2-ready",
        "tradable alpha",
        "deployable",
        "ready for allocation",
    ]:
        assert forbidden not in text
