from __future__ import annotations

from pathlib import Path

from multifactor_alpha_validation.alpha_view_mapper import map_signal_panel_to_alpha_view
from multifactor_alpha_validation.factor_library import load_factor_specs
from multifactor_alpha_validation.signal_builders import build_signal_panels, write_signal_outputs


PROJECT_ROOT = Path(__file__).resolve().parents[1]
SPEC_DIR = PROJECT_ROOT / "factor_specs"


def test_enabled_factors_emit_standard_signal_panels_with_timestamp_fields() -> None:
    specs = load_factor_specs(SPEC_DIR)
    result = build_signal_panels(specs)

    enabled_or_reference = [spec.factor_id for spec in specs if spec.status in {"enabled", "reference"}]
    assert sorted(result.signal_panels) == sorted(enabled_or_reference)

    required_columns = {
        "schema_version",
        "factor_id",
        "date",
        "asset_id",
        "raw_signal",
        "normalized_signal",
        "coverage_flag",
        "abstain_reason",
        "signal_timestamp",
        "visibility_timestamp",
        "tradable_timestamp",
        "horizon_start",
        "horizon_end",
        "provenance_hash",
    }
    for panel in result.signal_panels.values():
        assert required_columns.issubset(panel.columns)
        assert (panel["visibility_timestamp"] <= panel["tradable_timestamp"]).all()


def test_missing_coverage_becomes_abstain_not_zero_alpha() -> None:
    result = build_signal_panels(load_factor_specs(SPEC_DIR))

    abstain_rows = result.abstain_report
    assert not abstain_rows.empty
    assert set(abstain_rows["abstain_reason"]) == {"insufficient_history"}

    for panel in result.signal_panels.values():
        missing = panel[panel["coverage_flag"] == False]  # noqa: E712
        assert not missing.empty
        assert missing["abstain_reason"].eq("insufficient_history").all()
        assert missing["normalized_signal"].isna().all()


def test_alpha_view_mapper_preserves_fixed_horizon_event_and_disabled_semantics() -> None:
    specs = {spec.factor_id: spec for spec in load_factor_specs(SPEC_DIR)}
    result = build_signal_panels(specs.values())

    momentum_view = map_signal_panel_to_alpha_view(specs["momentum_12_1"], result.signal_panels["momentum_12_1"])
    assert momentum_view["view_type"] == "fixed_horizon"
    assert momentum_view["horizon"]["holding_days"] == 21
    assert momentum_view["no_view_is_not_zero_alpha"] is True
    assert momentum_view["active_view_count"] > 0
    assert momentum_view["abstain_count"] > 0

    sue_view = map_signal_panel_to_alpha_view(specs["sue_event_reference"], result.signal_panels["sue_event_reference"])
    assert sue_view["view_type"] == "event_reference"
    assert sue_view["factor_id"] == "sue_event_reference"

    assert "analyst_revision_disabled" in result.disabled_factors
    assert "analyst_revision_disabled" not in result.signal_panels


def test_signal_outputs_are_written(tmp_path: Path) -> None:
    result = build_signal_panels(load_factor_specs(SPEC_DIR))
    written = write_signal_outputs(result, tmp_path)

    assert (tmp_path / "abstain_report.csv").exists()
    assert (tmp_path / "signal_panel_momentum_12_1.csv").exists()
    assert "signal_panel_momentum_12_1.csv" in written
