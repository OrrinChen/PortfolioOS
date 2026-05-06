from __future__ import annotations

import json
from pathlib import Path

import yaml

from multifactor_alpha_validation.external_source_adapter import (
    validate_external_pit_dataset_source,
)


WRDS_CONFIG = Path("projects/multifactor_alpha_validation/configs/wrds_nasdaq100_research_mode.yaml")


def test_wrds_source_adapter_writes_manifest_mapping_validation_and_readiness(tmp_path: Path) -> None:
    result = validate_external_pit_dataset_source(WRDS_CONFIG, tmp_path / "source_adapter")

    assert result.status == "ready"
    assert result.blockers == ()
    assert Path(result.dataset_source_manifest_path).exists()
    assert Path(result.source_field_mapping_path).exists()
    assert Path(result.dataset_ingest_validation_path).exists()
    assert Path(result.dataset_readiness_path).exists()

    validation = json.loads(Path(result.dataset_ingest_validation_path).read_text())
    assert validation["ingest_executed"] is False
    assert validation["credentials_embedded"] is False
    assert validation["raw_data_committed"] is False
    assert validation["checks"]["historical_universe_membership"]["passed"] is True
    assert validation["checks"]["adjusted_price_volume"]["passed"] is True
    assert validation["checks"]["qqq_benchmark"]["passed"] is True
    assert validation["checks"]["delisting_handling"]["passed"] is True

    manifest = yaml.safe_load(Path(result.dataset_source_manifest_path).read_text())
    assert manifest["schema_version"] == "external_pit_dataset_source_manifest.v1"
    assert manifest["source_type"] == "wrds"
    assert manifest["connection_policy"]["credentials_in_repo"] is False
    assert manifest["paths"]["raw_output_dir"].startswith("data/cache/")

    mapping = yaml.safe_load(Path(result.source_field_mapping_path).read_text())
    assert mapping["schema_version"] == "source_field_mapping.v1"
    assert "historical_universe_membership" in mapping["artifacts"]
    assert "adjusted_price_volume_panel" in mapping["artifacts"]


def test_external_source_adapter_blocks_current_constituent_or_yfinance_config(tmp_path: Path) -> None:
    config = {
        "schema_version": "wrds_multifactor_query_config.v1",
        "raw_output_dir": "data/cache/wrds_multifactor/bad/raw",
        "standardized_output_dir": "data/cache/wrds_multifactor/bad/standardized",
        "preflight_output_dir": "outputs/multifactor_alpha_validation/bad_preflight",
        "queries": {
            "historical_universe_membership": {"sql": "select * from yfinance_current_constituents"},
            "adjusted_price_volume_panel": {"sql": "select date, prc as close, vol as volume from crsp.dsf"},
            "qqq_benchmark_panel": {"sql": "select date, adjusted_close from crsp.dsf"},
            "delisting_returns": {"sql": "select permno from crsp.dsf"},
        },
    }

    result = validate_external_pit_dataset_source(config, tmp_path / "source_adapter")

    assert result.status == "blocked"
    assert "historical universe membership source is not proven PIT/historical" in result.blockers
    assert "price-volume query does not prove adjusted OHLCV output" in result.blockers
    assert "benchmark query does not prove QQQ adjusted benchmark coverage" in result.blockers
    assert "delisting/inactive handling query is missing required fields" in result.blockers


def test_external_source_adapter_rejects_configs_with_credentials(tmp_path: Path) -> None:
    config = {
        "schema_version": "wrds_multifactor_query_config.v1",
        "password": "do-not-commit-this",
        "queries": {
            "historical_universe_membership": {"sql": "select * from comp.idxcst_his"},
            "adjusted_price_volume_panel": {"sql": "select adjusted_close, adjusted_open, volume from x"},
            "qqq_benchmark_panel": {"sql": "select 'QQQ' as benchmark, adjusted_close from x"},
            "delisting_returns": {"sql": "select delisting_date, delisting_return from x"},
        },
    }

    result = validate_external_pit_dataset_source(config, tmp_path / "source_adapter")

    assert result.status == "blocked"
    assert "WRDS query config must not contain credentials" in result.blockers
