from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import yaml

from multifactor_alpha_validation.component_oos_observation_expansion import (
    run_component_oos_observation_expansion,
)


def test_component_oos_observation_expansion_adds_price_volume_and_lagged_fundamental_components(
    tmp_path: Path,
) -> None:
    daily_manifest, fundamentals_manifest = _write_research_inputs(tmp_path)
    source_observations = tmp_path / "source_observations.csv"
    component_pool = tmp_path / "soft_resurrected_component_pool.csv"
    _write_source_observations(source_observations)
    _write_component_pool(component_pool)

    result = run_component_oos_observation_expansion(
        source_observation_path=source_observations,
        component_pool_path=component_pool,
        daily_manifest_path=daily_manifest,
        fundamentals_manifest_path=fundamentals_manifest,
        output_dir=tmp_path / "expanded",
    )

    observations = pd.read_csv(result.observation_path)
    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    enablement = pd.read_csv(result.enablement_report_path).set_index("factor_id")

    expected_generated = {
        "liquidity_turnover",
        "value_bm",
        "profitability_quality",
        "investment_asset_growth",
        "accruals",
    }
    assert expected_generated <= set(observations["factor_id"])
    assert "sue_event_reference" not in set(observations["factor_id"])
    assert set(summary["generated_factor_ids"]) == expected_generated
    assert summary["observed_factor_count_after_expansion"] == 6
    assert summary["unavailable_factor_ids_after_expansion"] == ["sue_event_reference"]
    assert summary["fabricated_returns"] is False
    assert summary["full_sample_icir_used"] is False
    assert summary["prior_history_only"] is True
    assert result.production_approval is False
    assert result.direct_q2_entry is False

    generated = observations[observations["factor_id"].isin(expected_generated)]
    assert set(generated["same_close_trading_used"]) == {False}
    assert set(generated["full_sample_icir_used"]) == {False}
    assert set(generated["prior_history_only"]) == {True}
    assert (pd.to_datetime(generated["tradable_date"]) > pd.to_datetime(generated["signal_date"])).all()

    assert enablement.loc["value_bm", "enablement_status"] == "generated_lagged_fundamental_oos_observations"
    assert int(enablement.loc["value_bm", "reporting_lag_days"]) == 90
    assert enablement.loc["sue_event_reference", "enablement_status"] == "unavailable_missing_event_visibility_path"


def test_component_oos_observation_expansion_keeps_fundamentals_unavailable_without_manifest(
    tmp_path: Path,
) -> None:
    daily_manifest, _fundamentals_manifest = _write_research_inputs(tmp_path)
    source_observations = tmp_path / "source_observations.csv"
    component_pool = tmp_path / "soft_resurrected_component_pool.csv"
    _write_source_observations(source_observations)
    _write_component_pool(component_pool)

    result = run_component_oos_observation_expansion(
        source_observation_path=source_observations,
        component_pool_path=component_pool,
        daily_manifest_path=daily_manifest,
        fundamentals_manifest_path=tmp_path / "missing_fundamentals_manifest.yaml",
        output_dir=tmp_path / "expanded",
    )

    observations = pd.read_csv(result.observation_path)
    summary = json.loads(Path(result.summary_path).read_text(encoding="utf-8"))
    enablement = pd.read_csv(result.enablement_report_path).set_index("factor_id")

    assert "liquidity_turnover" in set(observations["factor_id"])
    assert "value_bm" not in set(observations["factor_id"])
    assert summary["generated_factor_ids"] == ["liquidity_turnover"]
    assert "value_bm" in summary["unavailable_factor_ids_after_expansion"]
    assert enablement.loc["value_bm", "enablement_status"] == "unavailable_missing_fundamentals_manifest"
    assert summary["fabricated_returns"] is False


def _write_research_inputs(tmp_path: Path) -> tuple[Path, Path]:
    daily_root = tmp_path / "daily"
    fund_root = tmp_path / "fundamentals"
    daily_root.mkdir()
    fund_root.mkdir()
    dates = pd.bdate_range("2018-01-02", "2020-06-30")
    assets = [
        ("10001", "1001", 0.0005, 0.08),
        ("10002", "1002", 0.0003, 0.04),
        ("10003", "1003", -0.0001, 0.07),
        ("10004", "1004", 0.0007, 0.03),
        ("10005", "1005", 0.0002, 0.06),
        ("10006", "1006", -0.0002, 0.05),
    ]
    price_rows: list[dict[str, object]] = []
    universe_rows: list[dict[str, object]] = []
    annual_rows: list[dict[str, object]] = []
    for asset_index, (asset_id, gvkey, drift, fundamental_edge) in enumerate(assets):
        price = 20.0 + asset_index
        shrout = 1000.0 + asset_index * 20.0
        for i, date in enumerate(dates):
            price *= 1.0 + drift + ((i % 7) - 3) * 0.0001
            volume = 100000.0 + asset_index * 5000.0 + (i % 11) * 100.0
            price_rows.append(
                {
                    "asset_id": asset_id,
                    "permno": asset_id,
                    "ticker": f"T{asset_index}",
                    "date": date.date().isoformat(),
                    "adjusted_open": price,
                    "adjusted_close": price,
                    "volume": volume,
                    "dlycap": price * shrout,
                    "shrout": shrout,
                    "adjusted_price_convention": "crsp_daily_adjusted_fixture",
                }
            )
        universe_rows.append(
            {
                "asset_id": asset_id,
                "permno": asset_id,
                "ticker": f"T{asset_index}",
                "gvkey": gvkey,
                "membership_start": "2017-01-01",
                "membership_end": "2020-12-31",
                "sector": f"sector_{asset_index % 3}",
                "industry": f"industry_{asset_index % 2}",
                "source_is_pit": True,
            }
        )
        for year, asset_multiplier in [(2017, 0.9), (2018, 1.0), (2019, 1.08)]:
            at = 100.0 * asset_multiplier * (1.0 + asset_index * 0.02)
            annual_rows.append(
                {
                    "gvkey": gvkey,
                    "datadate": f"{year}-12-31",
                    "ceq": at * (0.4 + fundamental_edge),
                    "at": at,
                    "oibdp": at * (0.08 + fundamental_edge),
                    "ib": at * (0.04 + fundamental_edge / 2.0),
                    "oancf": at * (0.03 + fundamental_edge / 3.0),
                    "mkvalt": at * 1.5,
                    "visibility_timestamp": f"{year + 1}-04-30",
                    "tradable_timestamp": f"{year + 1}-05-01",
                    "coverage_flag": True,
                    "not_alpha_evidence": True,
                }
            )
    benchmark_rows = [
        {"date": date.date().isoformat(), "adjusted_close": 100.0 * (1.0 + i * 0.0002), "volume": 1000000.0}
        for i, date in enumerate(dates)
    ]
    prices_path = daily_root / "adjusted_price_volume_panel.csv"
    universe_path = daily_root / "historical_universe_membership.csv"
    benchmark_path = daily_root / "qqq_benchmark_panel.csv"
    annual_path = fund_root / "annual_fundamentals_panel.csv"
    quarterly_path = fund_root / "quarterly_fundamentals_panel.csv"
    ccm_path = fund_root / "ccm_link_history.csv"
    pd.DataFrame(price_rows).to_csv(prices_path, index=False)
    pd.DataFrame(universe_rows).to_csv(universe_path, index=False)
    pd.DataFrame(benchmark_rows).to_csv(benchmark_path, index=False)
    pd.DataFrame(annual_rows).to_csv(annual_path, index=False)
    pd.DataFrame(annual_rows).to_csv(quarterly_path, index=False)
    pd.DataFrame(
        [
            {"gvkey": gvkey, "asset_id": asset_id, "linkdt": "2017-01-01", "linkenddt": "2020-12-31"}
            for asset_id, gvkey, _drift, _edge in assets
        ]
    ).to_csv(ccm_path, index=False)
    daily_manifest = tmp_path / "research_mode_dataset_manifest.yaml"
    fundamentals_manifest = tmp_path / "fundamentals_manifest.yaml"
    daily_manifest.write_text(
        yaml.safe_dump(
            {
                "prices": {"path": str(prices_path)},
                "universe": {"path": str(universe_path)},
                "benchmark": {"path": str(benchmark_path)},
                "timestamp_policy": {"allow_same_close_trading": False},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    fundamentals_manifest.write_text(
        yaml.safe_dump(
            {
                "paths": {
                    "annual_fundamentals_panel": str(annual_path),
                    "quarterly_fundamentals_panel": str(quarterly_path),
                    "ccm_link_history": str(ccm_path),
                },
                "timestamp_policy": {
                    "annual_visibility": "datadate_plus_120d_lag",
                    "same_close_trading": False,
                },
                "non_claims": {"not_alpha_evidence": True},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    return daily_manifest, fundamentals_manifest


def _write_source_observations(path: Path) -> None:
    rows = []
    for date in pd.date_range("2020-01-31", "2020-05-29", freq="ME"):
        rows.append(
            {
                "schema_version": "real_rolling_oos_observation.v1",
                "factor_id": "momentum_12_1",
                "rebalance_date": date.date().isoformat(),
                "history_cutoff_date": (date - pd.Timedelta(days=1)).date().isoformat(),
                "signal_date": date.date().isoformat(),
                "tradable_date": (date + pd.Timedelta(days=1)).date().isoformat(),
                "horizon_end_date": (date + pd.offsets.MonthEnd(1)).date().isoformat(),
                "same_close_trading_used": False,
                "full_sample_icir_used": False,
                "prior_history_only": True,
                "rank_ic": 0.01,
                "gross_spread": 0.002,
                "qqq_return": 0.001,
                "qqq_relative_spread": 0.001,
                "beta_adjusted_spread": 0.0015,
                "sector_adjusted_rank_ic": 0.01,
                "sector_adjusted_spread": 0.001,
                "sector_adjusted_status": "observed",
                "style_adjusted_rank_ic": 0.01,
                "style_adjusted_spread": 0.001,
                "style_adjusted_status": "observed_size_liquidity_volatility_proxy",
                "style_model_scope": "size_liquidity_volatility_proxy",
                "net_spread": 0.001,
                "sector_adjusted_net_spread": 0.0,
                "style_adjusted_net_spread": 0.0,
                "cost_drag": 0.001,
                "asset_count": 6,
                "not_alpha_evidence": True,
            }
        )
    pd.DataFrame(rows).to_csv(path, index=False)


def _write_component_pool(path: Path) -> None:
    factors = [
        ("momentum_12_1", "momentum"),
        ("liquidity_turnover", "liquidity"),
        ("value_bm", "value"),
        ("profitability_quality", "quality"),
        ("investment_asset_growth", "investment"),
        ("accruals", "accruals"),
        ("sue_event_reference", "sue"),
    ]
    pd.DataFrame(
        [
            {
                "factor_id": factor_id,
                "family_id": family_id,
                "filter_class": "soft_resurrected",
                "component_pool_eligible": True,
                "component_status": "eligible_component",
                "component_role": "component",
                "portfolio_validation_allowed": True,
                "not_alpha_evidence": True,
                "production_approval": False,
                "direct_q2_entry": False,
            }
            for factor_id, family_id in factors
        ]
    ).to_csv(path, index=False)
