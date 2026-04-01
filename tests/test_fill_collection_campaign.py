from __future__ import annotations

import csv
import importlib.util
import json
import sys
from pathlib import Path

import pandas as pd
import pytest
import yaml

from portfolio_os.execution import fill_collection_campaign as campaign


def _load_pilot_ops_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "pilot_ops.py"
    spec = importlib.util.spec_from_file_location("pilot_ops_campaign_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _write_orders_oms_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=["sample_id", "ticker", "direction", "quantity", "estimated_price", "price_limit"],
        )
        writer.writeheader()
        writer.writerows(rows)


def _write_execution_report(path: Path, per_order_results: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {"per_order_results": per_order_results}
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_source_run_root(root: Path) -> Path:
    sample_root = root / "samples" / "sample_01"
    _write_orders_oms_csv(
        sample_root / "approval" / "final_orders_oms.csv",
        [
            {
                "sample_id": "sample_01",
                "ticker": "AAPL",
                "direction": "buy",
                "quantity": 100,
                "estimated_price": 100.0,
                "price_limit": "",
            },
            {
                "sample_id": "sample_01",
                "ticker": "MSFT",
                "direction": "sell",
                "quantity": 300,
                "estimated_price": 100.0,
                "price_limit": "",
            },
        ],
    )
    _write_execution_report(
        sample_root / "execution" / "execution_report.json",
        [
            {
                "ticker": "AAPL",
                "side": "BUY",
                "ordered_quantity": 100,
                "filled_quantity": 100,
                "filled_notional": 10000.0,
                "average_fill_price": 100.0,
                "participation_limit_used": 0.001,
                "bucket_results": [
                    {
                        "status": "filled",
                        "bucket_available_volume": 100000.0,
                        "bucket_participation_limit": 0.2,
                    }
                ],
            },
            {
                "ticker": "MSFT",
                "side": "SELL",
                "ordered_quantity": 300,
                "filled_quantity": 300,
                "filled_notional": 30000.0,
                "average_fill_price": 100.0,
                "participation_limit_used": 0.003,
                "bucket_results": [
                    {
                        "status": "filled",
                        "bucket_available_volume": 100000.0,
                        "bucket_participation_limit": 0.2,
                    }
                ],
            },
        ],
    )
    return root


def _write_bucket_plan(path: Path, source_root_name: str) -> None:
    payload = {
        "targets": [
            {
                "name": "buy_low",
                "side": "buy",
                "participation_min": 0.001,
                "participation_max": 0.005,
                "notional_min": 5000,
                "notional_max": 25000,
                "min_filled_orders": 1,
            },
            {
                "name": "sell_mid",
                "side": "sell",
                "participation_min": 0.001,
                "participation_max": 0.005,
                "notional_min": 25000,
                "notional_max": 100000,
                "min_filled_orders": 1,
            },
        ],
        "selection": {
            "source_run_roots": [source_root_name, source_root_name],
            "source_orders_oms": [],
            "scale_factors": [1.0, 2.0],
            "prefer_mixed_side_baskets": True,
            "max_tasks_per_target": 8,
        },
    }
    path.write_text(yaml.safe_dump(payload, sort_keys=False), encoding="utf-8")


def _make_seed_task_bundle() -> tuple[pd.DataFrame, dict[str, pd.DataFrame]]:
    tasks = pd.DataFrame(
        [
            {
                "task_id": "task_0001",
                "task_hash": "hash_1",
                "source_sample_id": "sample_1",
                "source_path": "C:/tmp/sample_1.csv",
                "source_root": "C:/tmp/source_root",
                "scale_factor": 1.0,
                "target_name": "buy_low",
                "target_side": "buy",
                "target_participation_min": 0.001,
                "target_participation_max": 0.005,
                "target_notional_min": 5000.0,
                "target_notional_max": 25000.0,
                "side_scope": "buy-only",
                "side_scope_allowed": True,
                "selected": False,
                "status": "planned",
                "selection_reason": "",
                "run_dir": "",
                "input_orders_oms_path": "",
                "matched_orders_estimate": 1,
                "matched_orders_total_estimate": 1,
                "candidate_order_count": 1,
                "estimated_total_notional": 100.0,
                "primary_ticker": "AAA",
                "candidate_tickers_json": json.dumps(["AAA"]),
                "mixed_side": False,
                "missing_adv_count": 0,
                "missing_reference_price_count": 0,
                "estimated_participation_min": 0.001,
                "estimated_participation_max": 0.002,
                "estimated_notional_min": 100.0,
                "estimated_notional_max": 100.0,
                "broker_state_available": True,
                "broker_precheck_submittable_count": 1,
                "broker_precheck_buy_submittable_count": 1,
                "broker_precheck_sell_submittable_count": 0,
                "broker_precheck_blocked_order_count": 0,
                "broker_precheck_blocked_reason_counts_json": "{}",
                "broker_precheck_blocked_side_counts_json": "{}",
                "broker_precheck_status": "ready",
                "match_details_json": json.dumps([{"target_name": "buy_low", "match_count": 1}]),
                "task_priority": 1.0,
            },
            {
                "task_id": "task_0002",
                "task_hash": "hash_2",
                "source_sample_id": "sample_1",
                "source_path": "C:/tmp/sample_1.csv",
                "source_root": "C:/tmp/source_root",
                "scale_factor": 1.0,
                "target_name": "buy_low",
                "target_side": "buy",
                "target_participation_min": 0.001,
                "target_participation_max": 0.005,
                "target_notional_min": 5000.0,
                "target_notional_max": 25000.0,
                "side_scope": "buy-only",
                "side_scope_allowed": True,
                "selected": False,
                "status": "planned",
                "selection_reason": "",
                "run_dir": "",
                "input_orders_oms_path": "",
                "matched_orders_estimate": 1,
                "matched_orders_total_estimate": 1,
                "candidate_order_count": 1,
                "estimated_total_notional": 120.0,
                "primary_ticker": "BBB",
                "candidate_tickers_json": json.dumps(["BBB"]),
                "mixed_side": False,
                "missing_adv_count": 0,
                "missing_reference_price_count": 0,
                "estimated_participation_min": 0.001,
                "estimated_participation_max": 0.003,
                "estimated_notional_min": 120.0,
                "estimated_notional_max": 120.0,
                "broker_state_available": True,
                "broker_precheck_submittable_count": 1,
                "broker_precheck_buy_submittable_count": 1,
                "broker_precheck_sell_submittable_count": 0,
                "broker_precheck_blocked_order_count": 0,
                "broker_precheck_blocked_reason_counts_json": "{}",
                "broker_precheck_blocked_side_counts_json": "{}",
                "broker_precheck_status": "ready",
                "match_details_json": json.dumps([{"target_name": "buy_low", "match_count": 1}]),
                "task_priority": 1.0,
            },
            {
                "task_id": "task_0003",
                "task_hash": "hash_3",
                "source_sample_id": "sample_1",
                "source_path": "C:/tmp/sample_1.csv",
                "source_root": "C:/tmp/source_root",
                "scale_factor": 1.0,
                "target_name": "buy_low",
                "target_side": "buy",
                "target_participation_min": 0.001,
                "target_participation_max": 0.005,
                "target_notional_min": 5000.0,
                "target_notional_max": 25000.0,
                "side_scope": "buy-only",
                "side_scope_allowed": True,
                "selected": False,
                "status": "planned",
                "selection_reason": "",
                "run_dir": "",
                "input_orders_oms_path": "",
                "matched_orders_estimate": 1,
                "matched_orders_total_estimate": 1,
                "candidate_order_count": 1,
                "estimated_total_notional": 300.0,
                "primary_ticker": "CCC",
                "candidate_tickers_json": json.dumps(["CCC"]),
                "mixed_side": False,
                "missing_adv_count": 0,
                "missing_reference_price_count": 0,
                "estimated_participation_min": 0.001,
                "estimated_participation_max": 0.004,
                "estimated_notional_min": 300.0,
                "estimated_notional_max": 300.0,
                "broker_state_available": True,
                "broker_precheck_submittable_count": 1,
                "broker_precheck_buy_submittable_count": 1,
                "broker_precheck_sell_submittable_count": 0,
                "broker_precheck_blocked_order_count": 0,
                "broker_precheck_blocked_reason_counts_json": "{}",
                "broker_precheck_blocked_side_counts_json": "{}",
                "broker_precheck_status": "ready",
                "match_details_json": json.dumps([{"target_name": "buy_low", "match_count": 1}]),
                "task_priority": 1.0,
            },
            {
                "task_id": "task_0004",
                "task_hash": "hash_4",
                "source_sample_id": "sample_1",
                "source_path": "C:/tmp/sample_1.csv",
                "source_root": "C:/tmp/source_root",
                "scale_factor": 1.0,
                "target_name": "sell_mid",
                "target_side": "sell",
                "target_participation_min": 0.001,
                "target_participation_max": 0.005,
                "target_notional_min": 25000.0,
                "target_notional_max": 100000.0,
                "side_scope": "buy-only",
                "side_scope_allowed": False,
                "selected": False,
                "status": "planned",
                "selection_reason": "",
                "run_dir": "",
                "input_orders_oms_path": "",
                "matched_orders_estimate": 1,
                "matched_orders_total_estimate": 1,
                "candidate_order_count": 1,
                "estimated_total_notional": 150.0,
                "primary_ticker": "SELL",
                "candidate_tickers_json": json.dumps(["SELL"]),
                "mixed_side": False,
                "missing_adv_count": 0,
                "missing_reference_price_count": 0,
                "estimated_participation_min": 0.001,
                "estimated_participation_max": 0.004,
                "estimated_notional_min": 150.0,
                "estimated_notional_max": 150.0,
                "broker_state_available": True,
                "broker_precheck_submittable_count": 0,
                "broker_precheck_buy_submittable_count": 0,
                "broker_precheck_sell_submittable_count": 0,
                "broker_precheck_blocked_order_count": 0,
                "broker_precheck_blocked_reason_counts_json": "{}",
                "broker_precheck_blocked_side_counts_json": "{}",
                "broker_precheck_status": "blocked",
                "match_details_json": json.dumps([{"target_name": "sell_mid", "match_count": 1}]),
                "task_priority": 1.0,
            },
        ]
    )
    task_inputs = {
        "task_0001": pd.DataFrame(
            [
                {
                    "sample_id": "sample_1",
                    "ticker": "AAA",
                    "direction": "buy",
                    "quantity": 1.0,
                    "requested_qty": 1.0,
                    "price_limit": 100.0,
                    "estimated_price": 100.0,
                    "requested_notional": 100.0,
                    "estimated_participation": 0.001,
                    "source_order_index": 0,
                }
            ]
        ),
        "task_0002": pd.DataFrame(
            [
                {
                    "sample_id": "sample_1",
                    "ticker": "BBB",
                    "direction": "buy",
                    "quantity": 1.0,
                    "requested_qty": 1.0,
                    "price_limit": 120.0,
                    "estimated_price": 120.0,
                    "requested_notional": 120.0,
                    "estimated_participation": 0.003,
                    "source_order_index": 0,
                }
            ]
        ),
        "task_0003": pd.DataFrame(
            [
                {
                    "sample_id": "sample_1",
                    "ticker": "CCC",
                    "direction": "buy",
                    "quantity": 1.0,
                    "requested_qty": 1.0,
                    "price_limit": 300.0,
                    "estimated_price": 300.0,
                    "requested_notional": 300.0,
                    "estimated_participation": 0.004,
                    "source_order_index": 0,
                }
            ]
        ),
        "task_0004": pd.DataFrame(
            [
                {
                    "sample_id": "sample_1",
                    "ticker": "SELL",
                    "direction": "sell",
                    "quantity": 1.0,
                    "requested_qty": 1.0,
                    "price_limit": 150.0,
                    "estimated_price": 150.0,
                    "requested_notional": 150.0,
                    "estimated_participation": 0.004,
                    "source_order_index": 0,
                }
            ]
        ),
    }
    return tasks, task_inputs


def test_bucket_plan_parsing_and_task_generation_dedupe(tmp_path: Path) -> None:
    source_root = _write_source_run_root(tmp_path / "pilot_validation_001")
    plan_path = tmp_path / "bucket_plan.yaml"
    _write_bucket_plan(plan_path, source_root.name)

    plan = campaign.load_bucket_plan(plan_path)
    source_baskets = campaign.load_campaign_sources(
        source_run_roots=plan["selection"]["source_run_roots"],
        source_orders_oms=[],
    )
    assert len(source_baskets) == 1

    tasks, task_inputs = campaign.generate_campaign_tasks(plan=plan, source_baskets=source_baskets, side_scope="both")
    assert len(tasks) == 2
    assert set(tasks["scale_factor"]) == {1.0, 2.0}
    assert int(tasks["matched_orders_total_estimate"].max()) >= 2
    assert int(tasks["selected"].sum()) == 1
    assert len(task_inputs) == 2
    assert tasks.loc[tasks["selected"], "status"].iloc[0] == "selected"


def test_buy_only_side_scope_excludes_sell_tasks(tmp_path: Path) -> None:
    source_root = _write_source_run_root(tmp_path / "pilot_validation_001")
    plan_path = tmp_path / "bucket_plan.yaml"
    _write_bucket_plan(plan_path, source_root.name)

    plan = campaign.load_bucket_plan(plan_path)
    source_baskets = campaign.load_campaign_sources(
        source_run_roots=plan["selection"]["source_run_roots"],
        source_orders_oms=[],
    )

    tasks, _task_inputs = campaign.generate_campaign_tasks(plan=plan, source_baskets=source_baskets, side_scope="buy-only")
    assert not tasks.empty
    assert set(tasks["target_side"].astype(str).str.lower()) == {"buy"}
    assert int(tasks["selected"].sum()) == 1
    assert "sell" not in set(tasks["target_side"].astype(str).str.lower())


def test_seed_inventory_selection_prefers_low_notional_buy_tasks_and_respects_budget(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    tasks, task_inputs = _make_seed_task_bundle()
    tasks = tasks.loc[[2, 1, 0, 3]].reset_index(drop=True)
    broker_state_snapshot = {"account": {"buying_power": 1000.0, "cash": 1000.0}, "positions": []}
    monkeypatch.setattr(campaign, "_collect_tradable_alpaca_symbols", lambda symbols: {str(symbol).strip().upper() for symbol in symbols})

    selected_tasks, _selected_inputs, seed_summary = campaign._apply_seed_inventory_selection(
        tasks=tasks,
        task_inputs=task_inputs,
        broker_state_snapshot=broker_state_snapshot,
        max_seed_notional=250.0,
        max_seed_orders=3,
    )

    selected_ids = selected_tasks.loc[selected_tasks["selected"].astype(bool), "task_id"].tolist()
    assert set(selected_ids) == {"task_0001", "task_0002"}
    assert "task_0003" not in selected_ids
    assert int(selected_tasks["selected"].sum()) == 2
    assert seed_summary["seed_inventory_mode"] is True
    assert seed_summary["seed_selected_task_count"] == 2
    assert seed_summary["seed_selected_order_count"] == 2
    assert seed_summary["seed_selected_notional"] == 220.0
    assert seed_summary["seed_preset_status"] == "seed_inventory_successful"
    assert selected_tasks.loc[selected_tasks["task_id"] == "task_0004", "selection_reason"].iloc[0] == "side_scope_excluded"


def test_seed_inventory_selection_skips_untradable_broker_assets(monkeypatch: pytest.MonkeyPatch) -> None:
    tasks, task_inputs = _make_seed_task_bundle()
    broker_state_snapshot = {"account": {"buying_power": 1000.0, "cash": 1000.0}, "positions": []}
    monkeypatch.setattr(campaign, "_collect_tradable_alpaca_symbols", lambda symbols: {"BBB"})

    selected_tasks, _selected_inputs, seed_summary = campaign._apply_seed_inventory_selection(
        tasks=tasks,
        task_inputs=task_inputs,
        broker_state_snapshot=broker_state_snapshot,
        max_seed_notional=250.0,
        max_seed_orders=3,
    )

    selected_ids = selected_tasks.loc[selected_tasks["selected"].astype(bool), "task_id"].tolist()
    assert selected_ids == ["task_0002"]
    assert selected_tasks.loc[selected_tasks["task_id"] == "task_0001", "selection_reason"].iloc[0] == "broker_asset_not_tradable"
    assert seed_summary["seed_asset_not_tradable_task_count"] >= 1
    assert seed_summary["seed_preset_status"] == "seed_inventory_successful"


def test_seed_inventory_selection_stops_when_buying_power_is_zero(monkeypatch: pytest.MonkeyPatch) -> None:
    tasks, task_inputs = _make_seed_task_bundle()
    broker_state_snapshot = {"account": {"buying_power": 0.0, "cash": 0.0}, "positions": []}
    monkeypatch.setattr(campaign, "_collect_tradable_alpaca_symbols", lambda symbols: {str(symbol).strip().upper() for symbol in symbols})

    selected_tasks, _selected_inputs, seed_summary = campaign._apply_seed_inventory_selection(
        tasks=tasks,
        task_inputs=task_inputs,
        broker_state_snapshot=broker_state_snapshot,
        max_seed_notional=250.0,
        max_seed_orders=3,
    )

    assert int(selected_tasks["selected"].sum()) == 0
    assert seed_summary["seed_preset_status"] == "seed_inventory_limited_by_buying_power"
    assert seed_summary["seed_budget_limited_task_count"] >= 1
    assert seed_summary["seed_excluded_not_relevant_task_count"] == 1


def test_reduce_positions_selection_prioritizes_current_inventory_and_releases_notional() -> None:
    broker_state_snapshot = {
        "account": {"buying_power": 0.0, "cash": -3500.0},
        "positions": [
            {"ticker": "MSFT", "quantity": 20.0, "market_value": 4000.0, "current_price": 200.0},
            {"ticker": "GOOGL", "quantity": 20.0, "market_value": 6000.0, "current_price": 300.0},
            {"ticker": "AAPL", "quantity": 40.0, "market_value": 4000.0, "current_price": 100.0},
            {"ticker": "NVDA", "quantity": 5.0, "market_value": 1000.0, "current_price": 200.0},
        ],
    }

    tasks, task_inputs, reduction_summary = campaign._build_reduce_positions_selection(
        broker_state_snapshot=broker_state_snapshot,
    )

    selected = tasks.loc[tasks["selected"].astype(bool)].reset_index(drop=True)
    assert selected["primary_ticker"].tolist()[:3] == ["AAPL", "MSFT", "GOOGL"]
    assert selected.loc[0, "reduction_requested_qty"] == 40.0
    assert selected.loc[1, "reduction_requested_qty"] == 20.0
    assert selected.loc[2, "reduction_requested_qty"] == 1.0
    assert selected.loc[3, "primary_ticker"] == "NVDA"
    assert selected.loc[3, "reduction_requested_qty"] == 1.0
    assert reduction_summary["reduction_mode"] is True
    assert reduction_summary["reduction_selected_task_count"] == 4
    assert reduction_summary["reduction_selected_order_count"] == 4
    assert reduction_summary["reduction_selected_tickers"][:4] == ["AAPL", "MSFT", "GOOGL", "NVDA"]
    assert reduction_summary["reduction_preset_status"] == "reduction_successful"
    assert len(task_inputs) == 4


def test_seed_inventory_campaign_run_writes_seed_manifest_and_report(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source_root = _write_source_run_root(tmp_path / "pilot_validation_seed")
    plan_path = tmp_path / "bucket_plan.yaml"
    _write_bucket_plan(plan_path, source_root.name)
    output_dir = tmp_path / "campaign_outputs"
    calls: list[dict[str, object]] = []
    monkeypatch.setattr(campaign, "_collect_tradable_alpaca_symbols", lambda symbols: {str(symbol).strip().upper() for symbol in symbols})

    def _fake_runner(**kwargs):
        calls.append(dict(kwargs))
        input_path = Path(kwargs["orders_oms"])
        run_parent = Path(kwargs["output_dir"])
        run_dir = run_parent / f"seed_run_{len(calls):02d}"
        run_dir.mkdir(parents=True, exist_ok=True)
        input_df = pd.read_csv(input_path)
        assert len(input_df) == 1
        assert "price_limit" in input_df.columns
        assert "extended_hours" in input_df.columns
        assert input_df["price_limit"].notna().all()
        assert input_df["extended_hours"].astype(bool).all()
        assert (pd.to_numeric(input_df["price_limit"], errors="coerce") > pd.to_numeric(input_df["estimated_price"], errors="coerce")).all()
        price_limits = pd.to_numeric(input_df["price_limit"], errors="coerce")
        assert (price_limits.round(2) == price_limits).all()
        output_df = input_df[["sample_id", "ticker", "direction", "requested_qty", "estimated_price"]].copy()
        output_df["filled_qty"] = output_df["requested_qty"]
        output_df["avg_fill_price"] = output_df["estimated_price"] + output_df["direction"].map({"buy": 1.0, "sell": -1.0}).fillna(0.0)
        output_df["requested_notional"] = output_df["requested_qty"] * output_df["estimated_price"]
        output_df["filled_notional"] = output_df["filled_qty"] * output_df["avg_fill_price"]
        output_df["fill_ratio"] = 1.0
        output_df["status"] = "filled"
        output_df["reject_reason"] = ""
        output_df["broker_order_id"] = [f"seed_{len(calls)}_{index}" for index in range(len(output_df))]
        output_df["submitted_at_utc"] = "2026-03-26T17:00:00+00:00"
        output_df["terminal_at_utc"] = "2026-03-26T17:00:01+00:00"
        output_df["latency_seconds"] = 1.0
        output_df["poll_count"] = 1
        output_df["timeout_cancelled"] = False
        output_df["cancel_requested"] = False
        output_df["cancel_acknowledged"] = False
        output_df["avg_fill_price_fallback_used"] = False
        output_df["source_path"] = str(input_path)
        output_df["status_history"] = "[]"
        output_df.to_csv(run_dir / "alpaca_fill_orders.csv", index=False, encoding="utf-8")
        (run_dir / "alpaca_fill_events.csv").write_text("sample_id,ticker\n", encoding="utf-8")
        (run_dir / "alpaca_fill_manifest.json").write_text(
            json.dumps(
                {
                    "submitted_count": int(len(output_df)),
                    "filled_count": int((output_df["filled_qty"] > 0).sum()),
                    "partial_count": 0,
                    "unfilled_count": 0,
                    "rejected_count": 0,
                    "timeout_cancelled_count": 0,
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (run_dir / "execution_result.json").write_text(
            json.dumps({"submitted_count": int(len(output_df))}, ensure_ascii=False),
            encoding="utf-8",
        )
        (run_dir / "execution_result.csv").write_text("{}", encoding="utf-8")
        (run_dir / "reconciliation_report.json").write_text("{}", encoding="utf-8")
        return run_dir

    monkeypatch.setattr(campaign.fill_collection, "generate_run_id", lambda **_kwargs: "alpaca_fill_campaign_20260326T170500_seed")
    monkeypatch.setattr(campaign.fill_collection, "is_us_market_open", lambda **_kwargs: False)

    broker_state_snapshot = {
        "account": {"buying_power": 1000.0, "cash": 1000.0},
        "positions": [],
    }

    campaign_root = campaign.run_fill_collection_campaign(
        bucket_plan_file=plan_path,
        output_dir=output_dir,
        run_root=None,
        source_orders_oms=[],
        timeout_seconds=5.0,
        poll_interval_seconds=0.1,
        max_runs=20,
        force_outside_market_hours=True,
        notes="seed-inventory-test",
        campaign_preset="seed-inventory",
        side_scope="buy-only",
        broker_state_snapshot=broker_state_snapshot,
        max_seed_notional=250.0,
        max_seed_orders=1,
        collection_runner=_fake_runner,
    )

    assert len(calls) == 1
    manifest = json.loads((campaign_root / "alpaca_fill_campaign_manifest.json").read_text(encoding="utf-8"))
    report = (campaign_root / "alpaca_fill_campaign_report.md").read_text(encoding="utf-8")
    assert manifest["campaign_preset"] == "seed-inventory"
    assert manifest["seed_inventory_mode"] is True
    assert manifest["side_scope"] == "buy-only"
    assert manifest["seed_extended_hours_price_limit_multiplier"] >= 1.0
    assert manifest["selected_buy_task_count"] == 1
    assert manifest["seed_selected_order_count"] == 1
    assert manifest["submitted_buy_order_count"] >= 1
    assert manifest["filled_buy_order_count"] >= 1
    assert manifest["recommendation"] == "seed_inventory_successful"
    assert "seed_inventory_successful" in report
    assert "seed_inventory_mode: true" in report
    assert "excluded because not sellable/not relevant" in report


def test_reduce_positions_campaign_run_writes_manifest_report_and_tasks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    plan_path = tmp_path / "bucket_plan.yaml"
    _write_bucket_plan(plan_path, "unused_source_root")
    output_dir = tmp_path / "campaign_outputs"
    calls: list[dict[str, object]] = []

    def _fake_runner(**kwargs):
        calls.append(dict(kwargs))
        input_path = Path(kwargs["orders_oms"])
        run_parent = Path(kwargs["output_dir"])
        run_dir = run_parent / f"reduce_run_{len(calls):02d}"
        run_dir.mkdir(parents=True, exist_ok=True)
        input_df = pd.read_csv(input_path)
        output_df = input_df[["sample_id", "ticker", "direction", "requested_qty", "estimated_price"]].copy()
        output_df["filled_qty"] = output_df["requested_qty"]
        output_df["avg_fill_price"] = output_df["estimated_price"]
        output_df["requested_notional"] = output_df["requested_qty"] * output_df["estimated_price"]
        output_df["filled_notional"] = output_df["filled_qty"] * output_df["avg_fill_price"]
        output_df["fill_ratio"] = 1.0
        output_df["status"] = "filled"
        output_df["reject_reason"] = ""
        output_df["broker_order_id"] = [f"reduce_{len(calls)}_{index}" for index in range(len(output_df))]
        output_df["submitted_at_utc"] = "2026-03-26T18:00:00+00:00"
        output_df["terminal_at_utc"] = "2026-03-26T18:00:01+00:00"
        output_df["latency_seconds"] = 1.0
        output_df["poll_count"] = 1
        output_df["timeout_cancelled"] = False
        output_df["cancel_requested"] = False
        output_df["cancel_acknowledged"] = False
        output_df["avg_fill_price_fallback_used"] = False
        output_df["source_path"] = str(input_path)
        output_df["status_history"] = "[]"
        output_df.to_csv(run_dir / "alpaca_fill_orders.csv", index=False, encoding="utf-8")
        (run_dir / "alpaca_fill_events.csv").write_text("sample_id,ticker\n", encoding="utf-8")
        (run_dir / "alpaca_fill_manifest.json").write_text(
            json.dumps(
                {
                    "submitted_count": int(len(output_df)),
                    "filled_count": int((output_df["filled_qty"] > 0).sum()),
                    "partial_count": 0,
                    "unfilled_count": 0,
                    "rejected_count": 0,
                    "timeout_cancelled_count": 0,
                },
                ensure_ascii=False,
            ),
            encoding="utf-8",
        )
        (run_dir / "execution_result.json").write_text(
            json.dumps({"submitted_count": int(len(output_df))}, ensure_ascii=False),
            encoding="utf-8",
        )
        (run_dir / "execution_result.csv").write_text("{}", encoding="utf-8")
        (run_dir / "reconciliation_report.json").write_text("{}", encoding="utf-8")
        return run_dir

    monkeypatch.setattr(campaign.fill_collection, "generate_run_id", lambda **_kwargs: "alpaca_fill_campaign_20260326T170700_reduce")
    monkeypatch.setattr(campaign.fill_collection, "is_us_market_open", lambda **_kwargs: True)

    broker_state_snapshot = {
        "account": {"buying_power": 0.0, "cash": -3500.0},
        "positions": [
            {"ticker": "AAPL", "quantity": 40.0, "market_value": 4000.0, "current_price": 100.0},
            {"ticker": "MSFT", "quantity": 20.0, "market_value": 4000.0, "current_price": 200.0},
            {"ticker": "GOOGL", "quantity": 20.0, "market_value": 6000.0, "current_price": 300.0},
        ],
    }

    campaign_root = campaign.run_fill_collection_campaign(
        bucket_plan_file=plan_path,
        output_dir=output_dir,
        run_root=None,
        source_orders_oms=[],
        timeout_seconds=5.0,
        poll_interval_seconds=0.1,
        max_runs=20,
        force_outside_market_hours=False,
        notes="reduce-positions-test",
        campaign_preset="reduce-positions",
        side_scope="buy-only",
        broker_state_snapshot=broker_state_snapshot,
        collection_runner=_fake_runner,
    )

    assert len(calls) == 3
    manifest = json.loads((campaign_root / "alpaca_fill_campaign_manifest.json").read_text(encoding="utf-8"))
    report = (campaign_root / "alpaca_fill_campaign_report.md").read_text(encoding="utf-8")
    assert manifest["campaign_preset"] == "reduce-positions"
    assert manifest["reduction_mode"] is True
    assert manifest["side_scope"] == "sell-only"
    assert manifest["selected_sell_task_count"] >= 1
    assert manifest["submitted_sell_order_count"] >= 1
    assert manifest["filled_sell_order_count"] >= 1
    assert "reduction_successful" in manifest["recommendation"]
    assert "reduction_mode: true" in report
    assert "reduction_target_notional" in report
    assert "AAPL" in report


def test_sell_only_side_scope_without_inventory_blocks_sell_tasks(tmp_path: Path) -> None:
    source_root = _write_source_run_root(tmp_path / "pilot_validation_001")
    plan_path = tmp_path / "bucket_plan.yaml"
    _write_bucket_plan(plan_path, source_root.name)

    plan = campaign.load_bucket_plan(plan_path)
    source_baskets = campaign.load_campaign_sources(
        source_run_roots=plan["selection"]["source_run_roots"],
        source_orders_oms=[],
    )
    broker_state_snapshot = {"account": {"buying_power": 1000000.0, "cash": 1000000.0}, "positions": []}

    tasks, _task_inputs = campaign.generate_campaign_tasks(
        plan=plan,
        source_baskets=source_baskets,
        side_scope="sell-only",
        broker_state_snapshot=broker_state_snapshot,
    )
    assert not tasks.empty
    assert set(tasks["target_side"].astype(str).str.lower()) == {"sell"}
    assert int(tasks["selected"].sum()) == 0
    blocked_reasons = [json.loads(value or "{}") for value in tasks["broker_precheck_blocked_reason_counts_json"].tolist()]
    assert any(item.get("no_broker_position_for_sell", 0) > 0 for item in blocked_reasons)


def test_missing_adv_excludes_candidate_tasks(tmp_path: Path) -> None:
    orders_path = tmp_path / "orders_oms.csv"
    _write_orders_oms_csv(
        orders_path,
        [
            {
                "sample_id": "direct_01",
                "ticker": "AAPL",
                "direction": "buy",
                "quantity": 100,
                "estimated_price": 100.0,
                "price_limit": "",
            }
        ],
    )
    plan_path = tmp_path / "bucket_plan.yaml"
    plan_path.write_text(
        yaml.safe_dump(
            {
                "targets": [
                    {
                        "name": "buy_low",
                        "side": "buy",
                        "participation_min": 0.001,
                        "participation_max": 0.005,
                        "notional_min": 5000,
                        "notional_max": 25000,
                        "min_filled_orders": 1,
                    }
                ],
                "selection": {
                    "source_run_roots": [],
                    "source_orders_oms": [],
                    "scale_factors": [1.0],
                    "prefer_mixed_side_baskets": True,
                    "max_tasks_per_target": 8,
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    plan = campaign.load_bucket_plan(plan_path)
    source_baskets = campaign.load_campaign_sources(source_run_roots=[], source_orders_oms=[orders_path])
    assert source_baskets[0]["orders"]["adv_shares"].isna().all()

    tasks, task_inputs = campaign.generate_campaign_tasks(plan=plan, source_baskets=source_baskets)
    assert tasks.empty
    assert not task_inputs


def test_bucket_plan_rejects_invalid_ranges(tmp_path: Path) -> None:
    plan_path = tmp_path / "bucket_plan.yaml"
    plan_path.write_text(
        yaml.safe_dump(
            {
                "targets": [
                    {
                        "name": "bad_bucket",
                        "side": "buy",
                        "participation_min": 0.01,
                        "participation_max": 0.005,
                        "notional_min": 5000,
                        "notional_max": 25000,
                        "min_filled_orders": 3,
                    }
                ],
                "selection": {"scale_factors": [1.0]},
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="participation range"):
        campaign.load_bucket_plan(plan_path)


def test_campaign_run_writes_manifest_report_and_tasks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source_root = _write_source_run_root(tmp_path / "pilot_validation_002")
    plan_path = tmp_path / "bucket_plan.yaml"
    _write_bucket_plan(plan_path, source_root.name)
    output_dir = tmp_path / "campaign_outputs"
    calls: list[dict[str, object]] = []

    def _fake_runner(**kwargs):
        calls.append(dict(kwargs))
        input_path = Path(kwargs["orders_oms"])
        run_parent = Path(kwargs["output_dir"])
        run_dir = run_parent / f"run_{len(calls):02d}"
        run_dir.mkdir(parents=True, exist_ok=True)
        input_df = pd.read_csv(input_path)
        output_df = input_df[["sample_id", "ticker", "direction", "requested_qty", "estimated_price"]].copy()
        output_df["filled_qty"] = output_df["requested_qty"]
        output_df["avg_fill_price"] = output_df["estimated_price"] + output_df["direction"].map({"buy": 1.0, "sell": -1.0}).fillna(0.0)
        output_df["requested_notional"] = output_df["requested_qty"] * output_df["estimated_price"]
        output_df["filled_notional"] = output_df["filled_qty"] * output_df["avg_fill_price"]
        output_df["fill_ratio"] = 1.0
        output_df["status"] = "filled"
        output_df["reject_reason"] = ""
        output_df["broker_order_id"] = [f"oid_{len(calls)}_{index}" for index in range(len(output_df))]
        output_df["submitted_at_utc"] = "2026-03-26T17:00:00+00:00"
        output_df["terminal_at_utc"] = "2026-03-26T17:00:01+00:00"
        output_df["latency_seconds"] = 1.0
        output_df["poll_count"] = 1
        output_df["timeout_cancelled"] = False
        output_df["cancel_requested"] = False
        output_df["cancel_acknowledged"] = False
        output_df["avg_fill_price_fallback_used"] = False
        output_df["source_path"] = str(input_path)
        output_df["status_history"] = "[]"
        output_df.to_csv(run_dir / "alpaca_fill_orders.csv", index=False, encoding="utf-8")
        (run_dir / "alpaca_fill_events.csv").write_text("sample_id,ticker\n", encoding="utf-8")
        (run_dir / "alpaca_fill_manifest.json").write_text("{}", encoding="utf-8")
        (run_dir / "execution_result.json").write_text("{}", encoding="utf-8")
        (run_dir / "execution_result.csv").write_text("{}", encoding="utf-8")
        (run_dir / "reconciliation_report.json").write_text("{}", encoding="utf-8")
        return run_dir

    monkeypatch.setattr(campaign.fill_collection, "generate_run_id", lambda **_kwargs: "alpaca_fill_campaign_20260326T170000_deadbeef")
    monkeypatch.setattr(campaign.fill_collection, "is_us_market_open", lambda **_kwargs: True)

    broker_state_snapshot = {
        "account": {"buying_power": 1000000.0, "cash": 1000000.0},
        "positions": [{"ticker": "MSFT", "quantity": 1000.0, "market_value": 100000.0}],
    }

    campaign_root = campaign.run_fill_collection_campaign(
        bucket_plan_file=plan_path,
        output_dir=output_dir,
        run_root=None,
        source_orders_oms=[],
        timeout_seconds=5.0,
        poll_interval_seconds=0.1,
        max_runs=20,
        force_outside_market_hours=False,
        notes="campaign-test",
        side_scope="both",
        broker_state_snapshot=broker_state_snapshot,
        collection_runner=_fake_runner,
    )

    assert len(calls) == 1
    manifest = json.loads((campaign_root / "alpaca_fill_campaign_manifest.json").read_text(encoding="utf-8"))
    assert manifest["recommendation"] == "ready_for_slippage_calibration"
    assert manifest["side_scope"] == "both"
    assert manifest["selected_buy_task_count"] >= 0
    assert manifest["selected_sell_task_count"] >= 0
    assert "blocked_by_reason" in manifest
    assert "blocked_by_side" in manifest
    assert "broker_state_snapshot" in manifest
    assert manifest["bucket_coverage"]["buy_low"]["filled_order_count"] >= 1
    assert manifest["bucket_coverage"]["sell_mid"]["filled_order_count"] >= 1
    assert (campaign_root / "alpaca_fill_campaign_report.md").exists()
    assert "Coverage is wide enough to rerun slippage calibration." in (campaign_root / "alpaca_fill_campaign_report.md").read_text(encoding="utf-8")
    tasks = pd.read_csv(campaign_root / "alpaca_fill_campaign_tasks.csv")
    assert int(tasks["selected"].sum()) == 1
    assert {"completed", "skipped"}.issuperset(set(tasks["status"].astype(str)))


def test_campaign_run_reports_continue_when_no_candidate_tasks(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    orders_path = tmp_path / "orders_oms.csv"
    _write_orders_oms_csv(
        orders_path,
        [
            {
                "sample_id": "direct_01",
                "ticker": "AAPL",
                "direction": "buy",
                "quantity": 100,
                "estimated_price": 100.0,
                "price_limit": "",
            }
        ],
    )
    plan_path = tmp_path / "bucket_plan.yaml"
    plan_path.write_text(
        yaml.safe_dump(
            {
                "targets": [
                    {
                        "name": "buy_low",
                        "side": "buy",
                        "participation_min": 0.001,
                        "participation_max": 0.005,
                        "notional_min": 5000,
                        "notional_max": 25000,
                        "min_filled_orders": 1,
                    },
                    {
                        "name": "sell_mid",
                        "side": "sell",
                        "participation_min": 0.001,
                        "participation_max": 0.005,
                        "notional_min": 25000,
                        "notional_max": 100000,
                        "min_filled_orders": 1,
                    },
                ],
                "selection": {
                    "source_run_roots": [],
                    "source_orders_oms": [],
                    "scale_factors": [1.0, 2.0],
                    "prefer_mixed_side_baskets": True,
                    "max_tasks_per_target": 8,
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )
    output_dir = tmp_path / "campaign_outputs"

    def _should_not_run(**_kwargs):
        raise AssertionError("runner should not be called when no tasks are selected")

    monkeypatch.setattr(campaign.fill_collection, "generate_run_id", lambda **_kwargs: "alpaca_fill_campaign_20260326T170000_deadbeef")
    monkeypatch.setattr(campaign.fill_collection, "is_us_market_open", lambda **_kwargs: True)

    campaign_root = campaign.run_fill_collection_campaign(
        bucket_plan_file=plan_path,
        output_dir=output_dir,
        run_root=None,
        source_orders_oms=[orders_path],
        timeout_seconds=5.0,
        poll_interval_seconds=0.1,
        max_runs=20,
        force_outside_market_hours=False,
        notes="campaign-test",
        side_scope="buy-only",
        collection_runner=_should_not_run,
    )

    manifest = json.loads((campaign_root / "alpaca_fill_campaign_manifest.json").read_text(encoding="utf-8"))
    assert manifest["recommendation"] == "continue_campaign"
    assert manifest["missing_buckets"]
    assert all(
        item["reason"] in {"side_scope_excluded", "no_candidate_tasks", "missing_adv", "no_selected_tasks", "insufficient_filled_orders"}
        for item in manifest["missing_buckets"]
    )
    assert manifest["coverage_summary"]["candidate_missing_adv_order_count"] > 0
    report = (campaign_root / "alpaca_fill_campaign_report.md").read_text(encoding="utf-8")
    assert "observed_side_counts" in report
    assert "side_scope_excluded" in report


def test_campaign_run_reports_broker_precheck_blockers(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    source_root = _write_source_run_root(tmp_path / "pilot_validation_003")
    plan_path = tmp_path / "bucket_plan.yaml"
    _write_bucket_plan(plan_path, source_root.name)
    output_dir = tmp_path / "campaign_outputs"

    monkeypatch.setattr(campaign.fill_collection, "generate_run_id", lambda **_kwargs: "alpaca_fill_campaign_20260326T170100_feedface")
    monkeypatch.setattr(campaign.fill_collection, "is_us_market_open", lambda **_kwargs: True)

    broker_state_snapshot = {"account": {"buying_power": 0.0, "cash": 0.0}, "positions": []}

    campaign_root = campaign.run_fill_collection_campaign(
        bucket_plan_file=plan_path,
        output_dir=output_dir,
        run_root=None,
        source_orders_oms=[],
        timeout_seconds=5.0,
        poll_interval_seconds=0.1,
        max_runs=20,
        force_outside_market_hours=False,
        notes="campaign-test",
        side_scope="both",
        broker_state_snapshot=broker_state_snapshot,
        collection_runner=lambda **_kwargs: (_ for _ in ()).throw(AssertionError("runner should not be called when broker state blocks all tasks")),
    )

    manifest = json.loads((campaign_root / "alpaca_fill_campaign_manifest.json").read_text(encoding="utf-8"))
    assert manifest["side_scope"] == "both"
    assert manifest["selected_buy_task_count"] == 0
    assert manifest["selected_sell_task_count"] == 0
    assert manifest["blocked_by_reason"]["buying_power_budget_exhausted"] > 0
    assert manifest["blocked_by_reason"]["no_broker_position_for_sell"] > 0
    assert manifest["blocked_by_side"]["buy"] > 0
    assert manifest["blocked_by_side"]["sell"] > 0
    report = (campaign_root / "alpaca_fill_campaign_report.md").read_text(encoding="utf-8")
    assert "blocked_by_reason" in report
    assert "both not ready" in report or "both ready" in report
