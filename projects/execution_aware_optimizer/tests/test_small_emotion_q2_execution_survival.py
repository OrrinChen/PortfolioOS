from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

from execution_aware_optimizer.small_emotion_q2_execution_survival import (
    run_small_emotion_q2_execution_survival,
)


def test_q2_survival_writes_cost_capacity_holding_and_optimizer_probe(tmp_path: Path) -> None:
    intake_dir = _write_q2_intake(
        tmp_path / "q2_intake",
        [
            _panel_row("rank2", "e1", "AAA", "2021-01-04", -0.18, 1_000_000, 0.02, 10.0),
            _panel_row("rank2", "e2", "BBB", "2021-01-05", -0.18, 2_000_000, 0.03, 20.0),
            _panel_row("rank2", "e3", "CCC", "2021-01-06", None, None, None, None, state="no_view"),
        ],
    )

    result = run_small_emotion_q2_execution_survival(
        q2_intake_dir=intake_dir,
        output_dir=tmp_path / "q2_survival",
    )

    assert result.summary["schema_version"] == "small_emotion_q2_execution_survival_summary.v1"
    assert result.summary["stage"] == "Q2-SMALL-EMOTION-02"
    assert result.summary["candidate_count"] == 1
    assert result.summary["survival_passed_count"] == 1
    assert result.summary["optimizer_entry_allowed"] is False
    assert result.summary["portfolio_construction_allowed"] is False
    assert result.summary["broker_order_path_opened"] is False
    assert result.summary["production_approval_claimed"] is False

    matrix = pd.read_csv(result.artifacts["survival_matrix"])
    assert matrix.loc[0, "survival_decision"] == "execution_survival_passed"
    assert matrix.loc[0, "optimizer_input_probe_status"] == "staged_optimizer_input_ready"
    assert matrix.loc[0, "active_expected_return_rows"] == 2
    assert matrix.loc[0, "no_view_rows_excluded"] == 1

    costs = pd.read_csv(result.artifacts["cost_capacity_report"])
    assert {"participation_25k_p95", "participation_100k_p95", "net_directional_return_25k_median"}.issubset(
        set(costs["metric"])
    )
    assert set(costs["status"]) == {"pass"}

    holding = pd.read_csv(result.artifacts["holding_path"])
    assert holding["active_positions"].max() > 0
    assert holding["entries"].sum() == 2

    probe = pd.read_csv(result.artifacts["optimizer_input_probe"])
    assert {"date", "ticker", "expected_return", "expected_return_source", "close", "adv_shares"}.issubset(
        probe.columns
    )
    assert probe["expected_return"].notna().all()
    assert (probe["expected_return"] < 0.0).all()


def test_q2_survival_blocks_cost_toxic_candidate(tmp_path: Path) -> None:
    intake_dir = _write_q2_intake(
        tmp_path / "q2_intake_cost_toxic",
        [_panel_row("rank1", "e1", "TOX", "2021-01-04", -0.05, 20_000, 0.40, 2.0)],
    )

    result = run_small_emotion_q2_execution_survival(
        q2_intake_dir=intake_dir,
        output_dir=tmp_path / "q2_survival_cost_toxic",
    )

    matrix = pd.read_csv(result.artifacts["survival_matrix"])
    assert matrix.loc[0, "survival_decision"] == "cost_capacity_failed"
    assert result.summary["survival_passed_count"] == 0


def _write_q2_intake(path: Path, rows: list[dict[str, object]]) -> Path:
    path.mkdir(parents=True)
    pd.DataFrame(rows).to_csv(path / "small_emotion_q2_expected_return_panel.csv", index=False)
    pd.DataFrame(
        [
            {
                "candidate_name": rows[0]["candidate_name"],
                "measurement_spec_id": "spec_" + str(rows[0]["candidate_name"]),
                "q2_status": "opened_q2_candidate",
                "primary_window": rows[0]["primary_window"],
                "q2_expected_return_rows": len(rows),
            }
        ]
    ).to_csv(path / "small_emotion_q2_candidate_matrix.csv", index=False)
    (path / "small_emotion_q2_candidate_summary.json").write_text(
        json.dumps(
            {
                "schema_version": "small_emotion_q2_candidate_intake_summary.v1",
                "opened_q2_candidate_count": 1,
                "q2_entry_allowed": True,
                "optimizer_entry_allowed": False,
                "portfolio_construction_allowed": False,
            }
        ),
        encoding="utf-8",
    )
    return path


def _panel_row(
    candidate: str,
    event_id: str,
    ticker: str,
    date: str,
    expected_return: float | None,
    adv20: float | None,
    spread: float | None,
    close: float | None,
    *,
    state: str = "active",
) -> dict[str, object]:
    return {
        "schema_version": "small_emotion_q2_expected_return_panel.v1",
        "stage": "Q2-SMALL-EMOTION-01",
        "candidate_name": candidate,
        "measurement_spec_id": "spec_" + candidate,
        "measurement_spec_hash": "hash-" + candidate,
        "date": date,
        "symbol": ticker,
        "asset_id": "asset-" + ticker,
        "event_id": event_id,
        "primary_window": "post_1_22",
        "signal_state": state,
        "signal_value": -1.0 if state == "active" else "",
        "expected_return": expected_return if expected_return is not None else "",
        "adv20": adv20 if adv20 is not None else "",
        "bid_ask_spread": spread if spread is not None else "",
        "adjusted_close": close if close is not None else "",
        "volume": 100_000 if state == "active" else "",
        "market_cap": 100_000_000 if state == "active" else "",
        "industry": "fixture",
        "sector": "fixture",
        "active_alpha_views": "spec_" + candidate,
        "q2_status": "opened_q2_candidate" if state == "active" else "no_view",
        "no_view_not_zero_alpha": True,
    }
