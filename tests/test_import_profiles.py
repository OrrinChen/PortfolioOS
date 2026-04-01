from __future__ import annotations

from pathlib import Path

import pytest

from portfolio_os.data.import_profiles import load_import_profile
from portfolio_os.data.market import load_market_snapshot
from portfolio_os.data.portfolio import load_holdings, load_target_weights
from portfolio_os.data.reference import load_reference_snapshot
from portfolio_os.domain.errors import InputValidationError


def test_standard_schema_loads_without_import_profile(project_root: Path) -> None:
    sample_dir = project_root / "data" / "sample"

    holdings = load_holdings(sample_dir / "holdings_example.csv")
    targets = load_target_weights(sample_dir / "target_example.csv")
    market = load_market_snapshot(
        sample_dir / "market_example.csv",
        required_tickers=["600519", "300750"],
    )
    reference = load_reference_snapshot(
        sample_dir / "reference_example.csv",
        required_tickers=["600519", "300750"],
    )

    assert holdings[0].ticker == "600519"
    assert targets[0].target_weight == pytest.approx(0.08)
    assert market.rows[0].ticker
    assert reference.rows[0].industry


def test_import_profile_maps_external_columns_into_standard_models(project_root: Path) -> None:
    sample_dir = project_root / "data" / "import_profile_samples" / "custodian_style_a"
    profile = load_import_profile(project_root / "config" / "import_profiles" / "custodian_style_a.yaml")

    holdings = load_holdings(sample_dir / "holdings.csv", import_profile=profile)
    targets = load_target_weights(sample_dir / "target.csv", import_profile=profile)
    market = load_market_snapshot(
        sample_dir / "market.csv",
        required_tickers=["600519", "601012", "600276", "000858"],
        import_profile=profile,
    )
    reference = load_reference_snapshot(
        sample_dir / "reference.csv",
        required_tickers=["600519", "601012", "000333"],
        import_profile=profile,
    )

    assert holdings[0].ticker == "600519"
    assert holdings[0].quantity == 180
    assert targets[0].target_weight == pytest.approx(0.08)
    market_rows = {row.ticker: row for row in market.rows}
    assert market_rows["600276"].tradable is False
    assert market_rows["000858"].upper_limit_hit is True
    reference_rows = {row.ticker: row for row in reference.rows}
    assert reference_rows["601012"].blacklist_buy is True
    assert reference_rows["000333"].benchmark_weight == pytest.approx(0.04)


def test_import_profile_raises_when_required_mapped_column_is_missing(tmp_path: Path, project_root: Path) -> None:
    holdings_path = tmp_path / "holdings.csv"
    holdings_path.write_text(
        "\n".join(
            [
                "security_code,avg_cost_cny",
                "600519,1650",
            ]
        ),
        encoding="utf-8",
    )
    profile = load_import_profile(project_root / "config" / "import_profiles" / "custodian_style_a.yaml")

    with pytest.raises(InputValidationError, match="position_qty"):
        load_holdings(holdings_path, import_profile=profile)
