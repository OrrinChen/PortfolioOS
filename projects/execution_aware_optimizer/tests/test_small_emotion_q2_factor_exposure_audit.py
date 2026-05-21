from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from execution_aware_optimizer.small_emotion_q2_factor_exposure_audit import (
    run_small_emotion_q2_factor_exposure_audit,
)


def test_q2_factor_exposure_audit_writes_beta_residual_artifacts_without_downstream_paths(tmp_path: Path) -> None:
    q2_complete_dir, q1_panels, price_panel, benchmark_panel = _write_inputs(tmp_path)

    result = run_small_emotion_q2_factor_exposure_audit(
        q2_complete_dir=q2_complete_dir,
        q1_event_panels={"rank2": q1_panels[0]},
        q1_window_panels={"rank2": q1_panels[1]},
        price_panel_path=price_panel,
        benchmark_panel_path=benchmark_panel,
        output_dir=tmp_path / "audit",
        minimum_event_count=3,
        beta_lookback_days=4,
    )

    assert result.summary["schema_version"] == "small_emotion_q2_factor_exposure_audit_summary.v1"
    assert result.summary["stage"] == "Q2-SMALL-EMOTION-06"
    assert result.summary["candidate_count"] == 1
    assert result.summary["factor_exposure_audit_run"] is True
    assert result.summary["orders_written"] is False
    assert result.summary["portfolio_construction_artifact_written"] is False
    assert result.summary["alpha_registry_update_allowed"] is False
    assert result.summary["production_approval_claimed"] is False

    exposure_panel = pd.read_csv(result.artifacts["exposure_panel"])
    assert {
        "candidate_name",
        "event_id",
        "directional_return",
        "benchmark_return",
        "trailing_beta_60d",
        "trailing_volatility_60d",
        "log_market_cap",
        "log_dollar_volume",
        "prior_5d_return",
        "prior_20d_return",
    } <= set(exposure_panel.columns)
    assert exposure_panel["trailing_beta_60d"].notna().all()
    assert bool(exposure_panel["no_view_not_zero_alpha"].all()) is True

    residual = pd.read_csv(result.artifacts["beta_residual_matrix"])
    assert residual.loc[0, "candidate_name"] == "rank2"
    assert residual.loc[0, "audit_decision"] == "beta_residual_passed"
    assert residual.loc[0, "factor_adjusted_alpha"] > 0.0
    assert residual.loc[0, "factor_adjusted_alpha_t_stat"] > 0.0
    assert bool(residual.loc[0, "orders_written"]) is False

    loadings = pd.read_csv(result.artifacts["factor_loading_matrix"])
    assert {"candidate_name", "factor_name", "loading", "t_stat"} <= set(loadings.columns)
    assert "trailing_beta_60d" in set(loadings["factor_name"])

    policy = pd.read_csv(result.artifacts["policy_gate"])
    assert {"minimum_event_count", "positive_factor_adjusted_alpha"} <= set(policy["policy_name"])

    report = result.artifacts["report"].read_text(encoding="utf-8")
    assert "Q2 factor exposure / beta residual audit only" in report
    assert "production approval: not claimed" in report
    assert "paper-ready" not in report.lower()


def test_q2_factor_exposure_audit_blocks_when_price_panel_missing(tmp_path: Path) -> None:
    q2_complete_dir, q1_panels, _price_panel, benchmark_panel = _write_inputs(tmp_path)

    result = run_small_emotion_q2_factor_exposure_audit(
        q2_complete_dir=q2_complete_dir,
        q1_event_panels={"rank2": q1_panels[0]},
        q1_window_panels={"rank2": q1_panels[1]},
        price_panel_path=tmp_path / "missing_price.csv",
        benchmark_panel_path=benchmark_panel,
        output_dir=tmp_path / "audit_missing",
        minimum_event_count=3,
    )

    residual = pd.read_csv(result.artifacts["beta_residual_matrix"])
    assert residual.loc[0, "audit_decision"] == "blocked_missing_factor_inputs"
    assert result.summary["beta_residual_passed_count"] == 0


def _write_inputs(tmp_path: Path) -> tuple[Path, tuple[Path, Path], Path, Path]:
    q2_complete_dir = tmp_path / "q2_complete"
    q1_dir = tmp_path / "q1"
    q2_complete_dir.mkdir()
    q1_dir.mkdir()

    pd.DataFrame(
        [
            {
                "candidate_name": "rank2",
                "measurement_spec_id": "small_emotion_rank2_v0",
                "measurement_spec_hash": "hash-rank2",
                "primary_window": "post_1_22",
                "q2_complete_decision": "completed_q2_execution_survival",
                "orders_written": False,
                "portfolio_construction_allowed": False,
                "alpha_registry_update_allowed": False,
                "broker_order_path_opened": False,
                "production_approval_claimed": False,
                "no_view_not_zero_alpha": True,
            }
        ]
    ).to_csv(q2_complete_dir / "small_emotion_q2_complete_matrix.csv", index=False)
    (q2_complete_dir / "small_emotion_q2_complete_summary.json").write_text(
        json.dumps({"schema_version": "small_emotion_q2_complete_summary.v1"}, sort_keys=True),
        encoding="utf-8",
    )

    event_rows = [
        _event_row("e1", 101, "AAA", "2021-01-06", 10_000_000, 1_000_000, 0.01, 0.10, 0.20),
        _event_row("e2", 102, "BBB", "2021-01-07", 20_000_000, 2_000_000, 0.02, 0.20, 0.30),
        _event_row("e3", 103, "CCC", "2021-01-08", 30_000_000, 3_000_000, 0.03, 0.30, 0.40),
    ]
    window_rows = [
        _window_row("e1", 101, "AAA", "2021-01-06", 0.11, 0.01, 0.10),
        _window_row("e2", 102, "BBB", "2021-01-07", 0.12, 0.01, 0.11),
        _window_row("e3", 103, "CCC", "2021-01-08", 0.13, 0.01, 0.12),
    ]
    event_panel = q1_dir / "q1_event_panel.csv"
    window_panel = q1_dir / "q1_window_return_panel.csv"
    pd.DataFrame(event_rows).to_csv(event_panel, index=False)
    pd.DataFrame(window_rows).to_csv(window_panel, index=False)

    price_panel = tmp_path / "price_panel.csv"
    benchmark_panel = tmp_path / "benchmark_panel.csv"
    price_rows = []
    benchmark_rows = []
    dates = pd.date_range("2020-12-30", "2021-01-08", freq="D")
    for idx, date in enumerate(dates):
        date_text = date.strftime("%Y-%m-%d")
        bench_ret = 0.001 * (idx + 1)
        benchmark_rows.append({"date": date_text, "benchmark": "IWM", "return": bench_ret})
        for asset_id, ticker, multiplier in [(101, "AAA", 1.0), (102, "BBB", 1.2), (103, "CCC", 0.8)]:
            price_rows.append(
                {
                    "asset_id": asset_id,
                    "ticker": ticker,
                    "date": date_text,
                    "return": multiplier * bench_ret + 0.001,
                    "adjusted_close": 10.0,
                    "volume": 100_000.0,
                    "dollar_volume": 1_000_000.0,
                    "market_cap": 10_000_000.0 * multiplier,
                }
            )
    pd.DataFrame(price_rows).to_csv(price_panel, index=False)
    pd.DataFrame(benchmark_rows).to_csv(benchmark_panel, index=False)
    return q2_complete_dir, (event_panel, window_panel), price_panel, benchmark_panel


def _event_row(
    event_id: str,
    asset_id: int,
    ticker: str,
    date: str,
    market_cap: float,
    dollar_volume: float,
    spread: float,
    prior_5d: float,
    prior_20d: float,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "asset_id": asset_id,
        "ticker": ticker,
        "date": date,
        "event_month": date[:7],
        "signal_state": "active",
        "shock_return": 0.15,
        "abs_shock_return": 0.15,
        "abnormal_volume": 2.0,
        "prior_5d_return": prior_5d,
        "prior_20d_return": prior_20d,
        "sector": "technology",
        "industry": "sic_1234",
        "market_cap": market_cap,
        "market_cap_bucket": "small",
        "dollar_volume": dollar_volume,
        "bid_ask_spread": spread,
        "adv20": dollar_volume,
        "liquidity_bucket": "high",
        "spread_bucket": "tight",
        "no_view_not_zero_alpha": True,
    }


def _window_row(
    event_id: str,
    asset_id: int,
    ticker: str,
    date: str,
    asset_return: float,
    benchmark_return: float,
    directional_return: float,
) -> dict[str, object]:
    return {
        "event_id": event_id,
        "asset_id": asset_id,
        "ticker": ticker,
        "date": date,
        "event_month": date[:7],
        "window": "post_1_22",
        "label_status": "observed",
        "asset_return": asset_return,
        "benchmark_return": benchmark_return,
        "abnormal_return": asset_return - benchmark_return,
        "directional_return": directional_return,
        "no_view_not_zero_alpha": True,
    }
