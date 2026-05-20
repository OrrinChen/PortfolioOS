from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from factor_discovery_sandbox.insider_disclosure_d3_signal_builder import (
    run_open_market_buying_signal_builder,
)


SPEC_PATH = Path(
    "projects/multifactor_alpha_validation/factor_discovery_sandbox/"
    "factor_specs/insider_disclosure_2023/open_market_insider_buying_post_2023_v0.yaml",
)


def test_open_market_buying_signal_builder_writes_d3_artifacts_without_q1_or_q2(tmp_path: Path) -> None:
    events_path = _write_event_registry_fixture(tmp_path / "insider_event_market_join.csv")

    result = run_open_market_buying_signal_builder(
        event_registry_path=events_path,
        measurement_spec_path=SPEC_PATH,
        output_dir=tmp_path / "d3",
    )

    assert result.summary["schema_version"] == "insider_open_market_buying_signal_builder_summary.v1"
    assert result.summary["stage"] == "D3-INSIDER-02"
    assert result.summary["measurement_spec_id"] == "open_market_insider_buying_post_2023_v0"
    assert result.summary["signal_panel_written"] is True
    assert result.summary["q1_entry_allowed"] is False
    assert result.summary["q2_entry_allowed"] is False
    assert result.summary["expected_return_panel_written"] is False
    assert result.summary["production_approval_claimed"] is False
    assert result.summary["transaction_code_scope"] == "open_market_or_private_purchase"
    assert result.summary["private_purchase_filter_status"] == "unavailable_from_form4_code_only"

    signal_panel = pd.read_csv(result.artifacts["signal_panel"]).fillna("")
    required_columns = {
        "issuer_id",
        "ticker",
        "cik",
        "event_id",
        "accession_number",
        "filing_accepted_ts",
        "tradable_ts",
        "signal_date",
        "event_subset",
        "transaction_code",
        "rule_10b5_1_flag",
        "role_bucket",
        "buy_dollar_value",
        "market_cap_at_event",
        "buy_value_pct",
        "distinct_buying_insider_count",
        "cluster_weight",
        "holding_change_ratio",
        "role_weight",
        "raw_buy_conviction",
        "winsorized_raw_buy_conviction",
        "normalized_signal",
        "coverage_state",
        "no_view_reason",
        "measurement_spec_hash",
        "source_manifest_hash",
        "transaction_code_scope",
        "private_purchase_filter_status",
    }
    assert required_columns.issubset(signal_panel.columns)

    active = signal_panel[signal_panel["coverage_state"] == "active"]
    no_view = signal_panel[signal_panel["coverage_state"] == "no_view"]
    assert len(active) == 2
    assert active["normalized_signal"].astype(str).ne("").all()
    assert no_view["normalized_signal"].astype(str).eq("").all()
    assert no_view["no_view_reason"].astype(str).ne("").all()
    assert signal_panel["transaction_code_scope"].eq("open_market_or_private_purchase").all()

    coverage = pd.read_csv(result.artifacts["coverage_abstain_panel"]).fillna("")
    assert "missing_market_join_or_price_volume_controls" in set(coverage["no_view_reason"])
    assert "not_code_p_primary_measurement" in set(coverage["no_view_reason"])
    assert coverage["no_view_not_zero_alpha"].eq(True).all()

    falsifier_pack = json.loads(result.artifacts["hard_falsifier_input_pack"].read_text(encoding="utf-8"))
    assert falsifier_pack["q1_entry_allowed"] is False
    assert "shifted_filing_dates" in falsifier_pack["hard_falsifiers"]
    assert falsifier_pack["source_signal_panel"].endswith("signal_panel.csv")

    report = result.artifacts["d3_open_market_buying_signal_builder_report"].read_text(encoding="utf-8").lower()
    assert "not alpha evidence" in report
    assert "form 4 code p means open-market or private purchase" in report
    for forbidden in [
        "production approved",
        "paper ready",
        "live trading",
        "broker execution",
        "order generation",
        "alpha passed",
        "q2-ready",
        "tradable alpha",
    ]:
        assert forbidden not in report


def test_open_market_buying_signal_builder_rejects_forward_return_and_car_inputs(tmp_path: Path) -> None:
    events_path = _write_event_registry_fixture(tmp_path / "leaky_event_registry.csv")
    events = pd.read_csv(events_path)
    events["forward_return_22d"] = 0.12
    events["car_post_1_22"] = 0.08
    events.to_csv(events_path, index=False)

    try:
        run_open_market_buying_signal_builder(
            event_registry_path=events_path,
            measurement_spec_path=SPEC_PATH,
            output_dir=tmp_path / "d3_leaky",
        )
    except ValueError as exc:
        message = str(exc)
    else:  # pragma: no cover - proves the hard guard fired
        raise AssertionError("expected forward-return/CAR input guard to reject the registry")

    assert "forbidden input columns" in message
    assert "forward_return_22d" in message
    assert "car_post_1_22" in message


def _write_event_registry_fixture(path: Path) -> Path:
    rows = [
        {
            "event_id": "buy_a_1",
            "issuer_cik": "1001",
            "ticker": "BUYA",
            "accession_number": "0000000000-24-000001",
            "form_type": "4",
            "filing_accepted_ts": "2024-05-01T18:00:00+00:00",
            "visibility_timestamp": "2024-05-01T18:00:00+00:00",
            "tradable_timestamp": "2024-05-02T13:30:00+00:00",
            "reporting_owner_cik": "2001",
            "reporting_owner_name_hash": "owner_a",
            "role_bucket": "cfo",
            "is_director": True,
            "is_officer": True,
            "officer_title_bucket": "CFO",
            "is_10pct_owner": False,
            "transaction_code": "P",
            "acquired_disposed": "A",
            "transaction_date": "2024-04-30",
            "transaction_shares": 1000,
            "transaction_price": 20.0,
            "transaction_dollar_value": 20_000.0,
            "security_title": "Common Stock",
            "is_derivative": False,
            "ownership_direct_or_indirect": "D",
            "post_transaction_holding": 11_000,
            "rule_10b5_1_flag": "",
            "plan_adoption_date": "",
            "event_subset": "open_market_buy",
            "event_cluster_id": "BUYA_2024-05-02",
            "market_cap_at_event": 1_000_000_000,
            "adv_20d": 5_000_000,
            "spread_proxy": 0.001,
            "sector": "technology",
            "size_bucket": "large",
            "liquidity_bucket": "high",
            "coverage_state": "covered",
            "no_view_reason": "",
            "diagnostic_only": False,
        },
        {
            "event_id": "buy_a_2",
            "issuer_cik": "1001",
            "ticker": "BUYA",
            "accession_number": "0000000000-24-000002",
            "form_type": "4",
            "filing_accepted_ts": "2024-05-01T19:00:00+00:00",
            "visibility_timestamp": "2024-05-01T19:00:00+00:00",
            "tradable_timestamp": "2024-05-02T13:30:00+00:00",
            "reporting_owner_cik": "2002",
            "reporting_owner_name_hash": "owner_b",
            "role_bucket": "director",
            "is_director": True,
            "is_officer": False,
            "officer_title_bucket": "Director",
            "is_10pct_owner": False,
            "transaction_code": "P",
            "acquired_disposed": "A",
            "transaction_date": "2024-04-30",
            "transaction_shares": 500,
            "transaction_price": 22.0,
            "transaction_dollar_value": 11_000.0,
            "security_title": "Common Stock",
            "is_derivative": False,
            "ownership_direct_or_indirect": "D",
            "post_transaction_holding": 4500,
            "rule_10b5_1_flag": "",
            "plan_adoption_date": "",
            "event_subset": "open_market_buy",
            "event_cluster_id": "BUYA_2024-05-02",
            "market_cap_at_event": 1_000_000_000,
            "adv_20d": 5_000_000,
            "spread_proxy": 0.001,
            "sector": "technology",
            "size_bucket": "large",
            "liquidity_bucket": "high",
            "coverage_state": "covered",
            "no_view_reason": "",
            "diagnostic_only": False,
        },
        {
            "event_id": "buy_b_missing_market",
            "issuer_cik": "1002",
            "ticker": "BUYB",
            "accession_number": "0000000000-24-000003",
            "form_type": "4",
            "filing_accepted_ts": "2024-05-03T18:00:00+00:00",
            "visibility_timestamp": "2024-05-03T18:00:00+00:00",
            "tradable_timestamp": "2024-05-06T13:30:00+00:00",
            "reporting_owner_cik": "2003",
            "reporting_owner_name_hash": "owner_c",
            "role_bucket": "ceo",
            "is_director": True,
            "is_officer": True,
            "officer_title_bucket": "CEO",
            "is_10pct_owner": False,
            "transaction_code": "P",
            "acquired_disposed": "A",
            "transaction_date": "2024-05-02",
            "transaction_shares": 750,
            "transaction_price": 18.0,
            "transaction_dollar_value": 13_500.0,
            "security_title": "Common Stock",
            "is_derivative": False,
            "ownership_direct_or_indirect": "D",
            "post_transaction_holding": 0,
            "rule_10b5_1_flag": "",
            "plan_adoption_date": "",
            "event_subset": "open_market_buy",
            "event_cluster_id": "BUYB_2024-05-06",
            "market_cap_at_event": 0,
            "adv_20d": 0,
            "spread_proxy": 0,
            "sector": "",
            "size_bucket": "",
            "liquidity_bucket": "",
            "coverage_state": "no_view",
            "no_view_reason": "missing_market_join_or_price_volume_controls",
            "diagnostic_only": False,
        },
        {
            "event_id": "sell_control",
            "issuer_cik": "1003",
            "ticker": "SELL",
            "accession_number": "0000000000-24-000004",
            "form_type": "4",
            "filing_accepted_ts": "2024-05-04T18:00:00+00:00",
            "visibility_timestamp": "2024-05-04T18:00:00+00:00",
            "tradable_timestamp": "2024-05-06T13:30:00+00:00",
            "reporting_owner_cik": "2004",
            "reporting_owner_name_hash": "owner_d",
            "role_bucket": "cfo",
            "is_director": True,
            "is_officer": True,
            "officer_title_bucket": "CFO",
            "is_10pct_owner": False,
            "transaction_code": "S",
            "acquired_disposed": "D",
            "transaction_date": "2024-05-02",
            "transaction_shares": 750,
            "transaction_price": 18.0,
            "transaction_dollar_value": 13_500.0,
            "security_title": "Common Stock",
            "is_derivative": False,
            "ownership_direct_or_indirect": "D",
            "post_transaction_holding": 5000,
            "rule_10b5_1_flag": False,
            "plan_adoption_date": "",
            "event_subset": "discretionary_sell",
            "event_cluster_id": "SELL_2024-05-06",
            "market_cap_at_event": 1_000_000_000,
            "adv_20d": 5_000_000,
            "spread_proxy": 0.001,
            "sector": "technology",
            "size_bucket": "large",
            "liquidity_bucket": "high",
            "coverage_state": "covered",
            "no_view_reason": "",
            "diagnostic_only": False,
        },
    ]
    pd.DataFrame(rows).to_csv(path, index=False)
    return path
