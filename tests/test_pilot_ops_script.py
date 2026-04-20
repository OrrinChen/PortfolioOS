from __future__ import annotations

import csv
import importlib.util
import json
from pathlib import Path
import sys

import pandas as pd
import pytest
import yaml

from portfolio_os.execution.models import ExecutionResult, OrderExecutionRecord, ReconciliationReport


def _load_pilot_ops_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "pilot_ops.py"
    spec = importlib.util.spec_from_file_location("pilot_ops_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


class _FakeFillCollectionAdapter:
    def __init__(self, *args, **kwargs) -> None:
        _ = args
        _ = kwargs
        self._positions = pd.DataFrame(
            [
                {"ticker": "MSFT", "quantity": 10.0, "market_value": 1100.0},
            ],
            columns=["ticker", "quantity", "market_value"],
        )

    def query_clock(self) -> dict[str, object]:
        return {"is_open": True}

    def query_account(self) -> dict[str, float]:
        return {"buying_power": 100000.0}

    def query_positions(self) -> pd.DataFrame:
        return self._positions.copy()

    def submit_orders_with_telemetry(self, orders_df: pd.DataFrame) -> ExecutionResult:
        records: list[OrderExecutionRecord] = []
        for index, row in enumerate(orders_df.to_dict(orient="records")):
            qty = float(row["quantity"])
            if index % 2 == 0:
                status = "filled"
                filled_qty = qty
                avg_fill_price = 100.0
                terminal_at_utc = f"2026-03-26T14:00:{index:02d}+00:00"
                submitted_at_utc = f"2026-03-26T13:59:{index:02d}+00:00"
                status_history = [
                    {
                        "event_type": "terminal",
                        "event_at_utc": terminal_at_utc,
                        "status": status,
                        "filled_qty": filled_qty,
                        "filled_avg_price": avg_fill_price,
                        "reject_reason": None,
                    }
                ]
            else:
                status = "partially_filled"
                filled_qty = qty / 2.0
                avg_fill_price = 110.0
                terminal_at_utc = f"2026-03-26T14:00:{index:02d}+00:00"
                submitted_at_utc = f"2026-03-26T13:59:{index:02d}+00:00"
                status_history = [
                    {
                        "event_type": "poll",
                        "event_at_utc": submitted_at_utc,
                        "status": "new",
                        "filled_qty": 0.0,
                        "filled_avg_price": None,
                        "reject_reason": None,
                    },
                    {
                        "event_type": "terminal",
                        "event_at_utc": terminal_at_utc,
                        "status": status,
                        "filled_qty": filled_qty,
                        "filled_avg_price": avg_fill_price,
                        "reject_reason": None,
                    },
                ]
            records.append(
                OrderExecutionRecord(
                    ticker=str(row["ticker"]),
                    direction=str(row["direction"]),
                    requested_qty=qty,
                    filled_qty=filled_qty,
                    avg_fill_price=avg_fill_price,
                    status=status,
                    reject_reason=None,
                    order_id=f"alpaca-{index + 1}",
                    broker_order_id=f"alpaca-{index + 1}",
                    submitted_at_utc=submitted_at_utc,
                    terminal_at_utc=terminal_at_utc,
                    poll_count=2 if index % 2 else 1,
                    timeout_cancelled=False,
                    cancel_requested=False,
                    cancel_acknowledged=False,
                    status_history=status_history,
                )
            )
        return ExecutionResult(
            orders=records,
            submitted_count=len(records),
            filled_count=sum(1 for record in records if record.status == "filled"),
            partial_count=sum(1 for record in records if record.status == "partially_filled"),
            unfilled_count=0,
            rejected_count=0,
            timeout_cancelled_count=0,
        )

    def reconcile(self, expected_positions: pd.DataFrame) -> ReconciliationReport:
        details = []
        for row in expected_positions.to_dict(orient="records"):
            details.append(
                {
                    "ticker": row.get("ticker", ""),
                    "expected_quantity": row.get("expected_quantity"),
                    "actual_quantity": row.get("expected_quantity"),
                    "quantity_diff": 0.0,
                    "expected_value": row.get("expected_value"),
                    "actual_value": row.get("expected_value"),
                    "value_diff": 0.0,
                }
            )
        return ReconciliationReport(
            matched_count=len(details),
            mismatched_count=0,
            missing_in_broker=[],
            missing_in_system=[],
            details=[],
        )


def _write_orders_oms_csv(path: Path, rows: list[dict[str, object]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sample_id",
                "ticker",
                "direction",
                "quantity",
                "estimated_price",
                "price_limit",
            ],
        )
        writer.writeheader()
        writer.writerows(rows)


def test_run_ops_accepts_as_of_date_without_real_sample(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    run_root = tmp_path / "pilot_validation_20260324_000001"
    run_root.mkdir(parents=True, exist_ok=True)

    monkeypatch.setattr(module, "_run_validation", lambda **_kwargs: (0, run_root))
    monkeypatch.setattr(module, "_load_sample_rows", lambda _run_root: [])
    monkeypatch.setattr(module, "_load_provider_report", lambda _run_root: {})

    rc = module.run_ops(
        mode="nightly",
        phase="phase_1",
        reviewer_input=None,
        real_sample=False,
        rebalance_triggered=False,
        incident_id="",
        notes="historical_replay_2026-03-20",
        as_of_date="2026-03-20",
        output_dir=tmp_path / "tracking",
    )

    assert rc == 0
    rows = list(
        csv.DictReader((tmp_path / "tracking" / "pilot_dashboard.csv").open("r", encoding="utf-8-sig", newline=""))
    )
    assert rows[-1]["date"] == "2026-03-20"
    assert rows[-1]["as_of_date"] == "2026-03-20"


def test_run_validation_forwards_real_feed_as_of_date(monkeypatch, tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    run_root = tmp_path / "pilot_validation_20260324_120000"
    run_root.mkdir(parents=True, exist_ok=True)
    captured: dict[str, list[str]] = {}

    class _Done:
        def __init__(self):
            self.returncode = 0
            self.stdout = f"Validation completed. Root: {run_root}\n"
            self.stderr = ""

    def _fake_run(command, **kwargs):
        _ = kwargs
        captured["command"] = list(command)
        return _Done()

    monkeypatch.setattr(module.subprocess, "run", _fake_run)
    rc, resolved_root = module._run_validation(
        mode="nightly",
        reviewer_input=None,
        real_sample=True,
        market="cn",
        as_of_date="2026-03-20",
    )

    assert rc == 0
    assert resolved_root == run_root
    assert "--market" in captured["command"]
    assert "--real-feed-as-of-date" in captured["command"]
    idx = captured["command"].index("--real-feed-as-of-date")
    assert captured["command"][idx + 1] == "2026-03-20"


def test_run_validation_forwards_config_overlay(monkeypatch, tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    run_root = tmp_path / "pilot_validation_20260324_121000"
    run_root.mkdir(parents=True, exist_ok=True)
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text("risk_model:\n  enabled: true\n", encoding="utf-8")
    captured: dict[str, list[str]] = {}

    class _Done:
        def __init__(self):
            self.returncode = 0
            self.stdout = f"Validation completed. Root: {run_root}\n"
            self.stderr = ""

    def _fake_run(command, **kwargs):
        _ = kwargs
        captured["command"] = list(command)
        return _Done()

    monkeypatch.setattr(module.subprocess, "run", _fake_run)
    rc, resolved_root = module._run_validation(
        mode="nightly",
        reviewer_input=None,
        real_sample=True,
        market="cn",
        config_overlay=overlay_path,
        as_of_date="2026-03-20",
    )

    assert rc == 0
    assert resolved_root == run_root
    assert "--config-overlay" in captured["command"]
    idx = captured["command"].index("--config-overlay")
    assert captured["command"][idx + 1] == str(overlay_path)


def test_ensure_dashboard_schema_upgrades_legacy_header(tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    dashboard = tmp_path / "pilot_dashboard.csv"
    dashboard.write_text(
        "date,phase,nightly_status,release_status,rebalance_triggered,override_count,cost_better_ratio,primary_feed_success,fallback_activated,solver_primary,blocked_untradeable_count,incident_id,notes\n"
        "2026-03-24,phase_1,pass,,false,0,1.0,true,,CLARABEL,1,,legacy\n",
        encoding="utf-8",
    )

    module._ensure_dashboard_schema(dashboard)

    with dashboard.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        headers = reader.fieldnames or []

    assert headers == module.DASHBOARD_HEADERS
    assert len(rows) == 1
    assert rows[0]["date"] == "2026-03-24"
    assert rows[0]["phase"] == "phase_1"
    assert rows[0]["notes"] == "legacy"
    assert rows[0]["mode"] == ""


def test_generate_go_nogo_report_outputs_dual_windows(tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    dashboard = tmp_path / "pilot_dashboard.csv"
    incident = tmp_path / "incident_register.csv"
    output = tmp_path / "go_nogo_status.md"

    with dashboard.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=module.DASHBOARD_HEADERS)
        writer.writeheader()
        writer.writerow(
            {
                "date": "2026-03-20",
                "phase": "phase_1",
                "mode": "nightly",
                "run_root": "outputs/pilot_validation_a",
                "as_of_date": "2026-03-20",
                "nightly_status": "pass",
                "release_status": "",
                "release_gate_passed": "",
                "rebalance_triggered": "false",
                "artifact_chain_complete": "true",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "primary_feed_success": "true",
                "fallback_activated": "",
                "solver_primary": "CLARABEL",
                "blocked_untradeable_count": "0",
                "static_count": "5",
                "real_count": "1",
                "full_chain_success_static": "5",
                "full_chain_success_real": "1",
                "override_used_static": "0",
                "score_gap_ge_001_static": "5",
                "cost_better_ratio_static": "1.0",
                "solver_fallback_used_static": "0",
                "solver_sample_count_static": "5",
                "mean_order_reasonableness_static": "4.0",
                "mean_findings_explainability_static": "4.0",
                "mean_execution_credibility_static": "4.0",
                "execution_residual_risk_consistent": "true",
                "provider_blockers_count": "0",
                "incident_id": "",
                "notes": "",
            }
        )
        writer.writerow(
            {
                "date": "2026-03-21",
                "phase": "phase_1",
                "mode": "release",
                "run_root": "outputs/pilot_validation_b",
                "as_of_date": "2026-03-21",
                "nightly_status": "pass",
                "release_status": "passed",
                "release_gate_passed": "true",
                "rebalance_triggered": "true",
                "artifact_chain_complete": "true",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "primary_feed_success": "true",
                "fallback_activated": "akshare",
                "solver_primary": "CLARABEL",
                "blocked_untradeable_count": "1",
                "static_count": "5",
                "real_count": "1",
                "full_chain_success_static": "5",
                "full_chain_success_real": "1",
                "override_used_static": "0",
                "score_gap_ge_001_static": "5",
                "cost_better_ratio_static": "1.0",
                "solver_fallback_used_static": "0",
                "solver_sample_count_static": "5",
                "mean_order_reasonableness_static": "4.0",
                "mean_findings_explainability_static": "4.0",
                "mean_execution_credibility_static": "4.0",
                "execution_residual_risk_consistent": "true",
                "provider_blockers_count": "0",
                "incident_id": "",
                "notes": "",
            }
        )

    with incident.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "incident_id",
                "date",
                "severity",
                "category",
                "description",
                "root_cause",
                "resolution",
                "time_to_resolve_hours",
                "recurrence",
                "linked_kpi_impact",
            ],
        )
        writer.writeheader()

    module.generate_go_nogo_report(
        dashboard_path=dashboard,
        incident_path=incident,
        output_path=output,
        as_of_date="2026-03-21",
        window_trading_days=20,
    )

    rendered = output.read_text(encoding="utf-8")
    assert "## Window: rolling_20" in rendered
    assert "## Window: pilot_to_date" in rendered
    assert rendered.count("C10_reviewer_mean_scores") == 2


def test_generate_go_nogo_report_with_legacy_rows_marks_insufficient(tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    dashboard = tmp_path / "pilot_dashboard.csv"
    incident = tmp_path / "incident_register.csv"
    output = tmp_path / "go_nogo_status.md"

    dashboard.write_text(
        "date,phase,nightly_status,release_status,rebalance_triggered,override_count,cost_better_ratio,primary_feed_success,fallback_activated,solver_primary,blocked_untradeable_count,incident_id,notes\n"
        "2026-03-24,phase_1,pass,,false,0,1.0,true,,CLARABEL,1,,legacy\n",
        encoding="utf-8",
    )
    incident.write_text(
        "incident_id,date,severity,category,description,root_cause,resolution,time_to_resolve_hours,recurrence,linked_kpi_impact\n",
        encoding="utf-8",
    )

    module.generate_go_nogo_report(
        dashboard_path=dashboard,
        incident_path=incident,
        output_path=output,
        as_of_date="2026-03-24",
        window_trading_days=20,
    )

    rendered = output.read_text(encoding="utf-8")
    assert module.STATUS_INSUFFICIENT in rendered


def test_prepare_orders_for_alpaca_uses_estimated_price_when_limit_is_nan() -> None:
    module = _load_pilot_ops_module()
    orders_df = pd.DataFrame(
        [
            {
                "ticker": "AAPL",
                "direction": "buy",
                "quantity": 1000.0,
                "price_limit": float("nan"),
                "estimated_price": 200.0,
            },
            {
                "ticker": "MSFT",
                "direction": "buy",
                "quantity": 10.0,
                "price_limit": float("nan"),
                "estimated_price": 300.0,
            },
        ]
    )
    broker_positions = pd.DataFrame(columns=["ticker", "quantity"])
    prepared, precheck = module._prepare_orders_for_alpaca(
        orders_df=orders_df,
        broker_positions_before=broker_positions,
        account_payload={"buying_power": 100000.0},
    )

    assert len(prepared) == 2
    assert precheck["submitted_count"] == 2
    submitted = {
        str(row["ticker"]): float(row["quantity"])
        for row in prepared.to_dict(orient="records")
    }
    assert submitted["AAPL"] == 394.0
    assert submitted["MSFT"] == 3.0
    assert precheck["clipped_count"] >= 1
    assert any(
        str(item.get("reason")) == "clipped_to_buying_power_budget"
        for item in list(precheck.get("clipped_orders") or [])
    )


def test_run_alpaca_execution_cycle_writes_reconciliation_when_precheck_empty(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = _load_pilot_ops_module()
    run_root = tmp_path / "pilot_validation_20260324_999999"
    approval_dir = run_root / "samples" / "real_sample_01" / "approval"
    execution_dir = run_root / "samples" / "real_sample_01" / "execution"
    approval_dir.mkdir(parents=True, exist_ok=True)
    execution_dir.mkdir(parents=True, exist_ok=True)

    orders_csv = approval_dir / "final_orders_oms.csv"
    orders_csv.write_text(
        "\n".join(
            [
                "account_id,ticker,side,quantity,price_type,limit_price,estimated_price,estimated_notional,urgency,strategy_tag,basket_id,reason,blocking_checks_cleared",
                "us_sample_01,AAPL,BUY,10,VWAP_REF,,190.0,1900.0,low,US_REBALANCE,basket,reason,True",
            ]
        )
        + "\n",
        encoding="utf-8",
    )

    class _ReconcileResult:
        def to_dict(self):
            return {
                "matched_count": 0,
                "mismatched_count": 0,
                "missing_in_broker": [],
                "missing_in_system": [],
                "details": [],
            }

    class _FakeAdapter:
        def __init__(self) -> None:
            pass

        def query_positions(self) -> pd.DataFrame:
            return pd.DataFrame(columns=["ticker", "quantity", "market_value"])

        def query_account(self) -> dict[str, float]:
            return {"buying_power": 0.0}

        def reconcile(self, expected_positions: pd.DataFrame):
            _ = expected_positions
            return _ReconcileResult()

        def submit_orders(self, orders_df: pd.DataFrame):
            raise AssertionError("submit_orders should not be called when precheck yields empty basket")

    monkeypatch.setattr(module, "AlpacaAdapter", _FakeAdapter)
    rows = [
        {
            "sample_id": "real_sample_01",
            "approval_success": "true",
            "execution_success": "true",
        }
    ]

    module._run_alpaca_execution_cycle(run_root=run_root, rows=rows)

    execution_result_path = execution_dir / "execution_result.json"
    reconciliation_path = execution_dir / "reconciliation_report.json"
    assert execution_result_path.exists()
    assert reconciliation_path.exists()

    execution_payload = json.loads(execution_result_path.read_text(encoding="utf-8"))
    reconciliation_payload = json.loads(reconciliation_path.read_text(encoding="utf-8"))
    assert execution_payload["submitted_count"] == 0
    assert reconciliation_payload["mismatched_count"] == 0


def test_run_alpaca_fill_collection_fails_fast_without_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = _load_pilot_ops_module()
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)

    with pytest.raises(RuntimeError, match="Missing Alpaca credentials"):
        module._run_alpaca_fill_collection(
            run_root=tmp_path / "missing_run_root",
            orders_oms=tmp_path / "missing_orders.csv",
            output_dir=tmp_path / "out",
            market="us",
            broker="alpaca",
            timeout_seconds=1.0,
            poll_interval_seconds=0.1,
            notes="",
            force_outside_market_hours=True,
        )


def test_main_collect_fills_campaign_fails_fast_without_credentials(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = _load_pilot_ops_module()
    monkeypatch.delenv("ALPACA_API_KEY", raising=False)
    monkeypatch.delenv("ALPACA_SECRET_KEY", raising=False)

    with pytest.raises(RuntimeError, match="Missing Alpaca credentials"):
        module.main(
            [
                "collect-fills-campaign",
                "--bucket-plan-file",
                str(tmp_path / "bucket_plan.yaml"),
                "--output-dir",
                str(tmp_path / "campaign_outputs"),
        ]
    )


def test_main_collect_fills_campaign_forwards_side_scope_and_broker_snapshot(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = _load_pilot_ops_module()
    captured: dict[str, object] = {}
    fake_snapshot = {
        "captured_at_utc": "2026-03-26T12:00:00+00:00",
        "account": {"buying_power": 1234.0, "cash": 1234.0},
        "positions": pd.DataFrame([{"ticker": "MSFT", "quantity": 10.0, "market_value": 1000.0}]),
    }

    def _fake_collect_snapshot(**_kwargs):
        return fake_snapshot

    def _fake_run_fill_collection_campaign(**kwargs):
        captured.update(kwargs)
        return tmp_path / "campaign_run"

    monkeypatch.setenv("ALPACA_API_KEY", "test-api-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test-secret-key")
    monkeypatch.setattr(module, "_collect_alpaca_broker_state_snapshot", _fake_collect_snapshot)
    monkeypatch.setattr(module.fill_collection_campaign, "run_fill_collection_campaign", _fake_run_fill_collection_campaign)

    rc = module.main(
        [
            "collect-fills-campaign",
            "--bucket-plan-file",
            str(tmp_path / "bucket_plan.yaml"),
            "--output-dir",
            str(tmp_path / "campaign_outputs"),
            "--campaign-preset",
            "seed-inventory",
            "--max-seed-notional",
            "250",
            "--max-seed-orders",
            "2",
            "--side-scope",
            "buy-only",
        ]
    )

    assert rc == 0
    assert captured["campaign_preset"] == "seed-inventory"
    assert captured["side_scope"] == "buy-only"
    assert captured["max_seed_notional"] == 250.0
    assert captured["max_seed_orders"] == 2
    assert captured["broker_state_snapshot"] == fake_snapshot


def test_main_collect_fills_campaign_writes_reduction_broker_state_report(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = _load_pilot_ops_module()
    before_snapshot = {
        "captured_at_utc": "2026-03-26T12:00:00+00:00",
        "account": {"buying_power": 0.0, "cash": -3500.0, "equity": 10000.0},
        "positions": pd.DataFrame([{"ticker": "AAPL", "quantity": 40.0, "market_value": 4000.0}]),
    }
    after_snapshot = {
        "captured_at_utc": "2026-03-26T12:30:00+00:00",
        "account": {"buying_power": 2500.0, "cash": 2500.0, "equity": 10000.0},
        "positions": pd.DataFrame([{"ticker": "MSFT", "quantity": 20.0, "market_value": 4000.0}]),
    }
    snapshots = [before_snapshot, after_snapshot]
    campaign_root = tmp_path / "campaign_root"
    campaign_root.mkdir(parents=True, exist_ok=True)
    manifest = {
        "campaign_run_id": "alpaca_fill_campaign_20260326T170700_reduce",
        "broker": "alpaca",
        "market": "us",
        "campaign_preset": "reduce-positions",
        "side_scope": "sell-only",
        "reduction_selected_tickers": ["AAPL"],
        "reduction_selected_quantities": [10.0],
        "reduction_selected_notionals": [1000.0],
        "submitted_sell_order_count": 1,
        "filled_sell_order_count": 1,
    }
    (campaign_root / "alpaca_fill_campaign_manifest.json").write_text(json.dumps(manifest), encoding="utf-8")

    def _fake_collect_snapshot(**_kwargs):
        return snapshots.pop(0)

    def _fake_run_fill_collection_campaign(**_kwargs):
        return campaign_root

    monkeypatch.setenv("ALPACA_API_KEY", "test-api-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test-secret-key")
    monkeypatch.setattr(module, "_collect_alpaca_broker_state_snapshot", _fake_collect_snapshot)
    monkeypatch.setattr(module.fill_collection_campaign, "run_fill_collection_campaign", _fake_run_fill_collection_campaign)

    rc = module.main(
        [
            "collect-fills-campaign",
            "--bucket-plan-file",
            str(tmp_path / "bucket_plan.yaml"),
            "--output-dir",
            str(tmp_path / "campaign_outputs"),
            "--campaign-preset",
            "reduce-positions",
        ]
    )

    assert rc == 0
    report_json = json.loads((campaign_root / "broker_state_report.json").read_text(encoding="utf-8"))
    report_md = (campaign_root / "broker_state_report.md").read_text(encoding="utf-8")
    assert report_json["reduction_result"] == "reduction successful"
    assert report_json["ready_for_buy_only"] is True
    assert report_json["sold_tickers"] == ["AAPL"]
    assert report_json["buying_power_delta"] > 0
    assert "reduction successful" in report_md


def test_main_inspect_broker_state_writes_report_and_recommends_reduce_positions(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = _load_pilot_ops_module()
    fake_snapshot = {
        "captured_at_utc": "2026-03-26T12:00:00+00:00",
        "account": {
            "buying_power": 0.0,
            "cash": -97157.46,
            "equity": 95083.91,
            "portfolio_value": 95083.91,
            "account_type": "paper",
        },
        "positions": pd.DataFrame(
            [
                {"ticker": "AAPL", "quantity": 188.0, "market_value": 18800.0, "unrealized_pnl": 123.0},
                {"ticker": "MSFT", "quantity": 121.0, "market_value": 24200.0, "unrealized_pnl": -42.0},
            ]
        ),
    }

    monkeypatch.setenv("ALPACA_API_KEY", "test-api-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test-secret-key")
    monkeypatch.setattr(module.fill_collection, "generate_run_id", lambda **_kwargs: "broker_state_inspection_20260326T170000_seed")
    monkeypatch.setattr(module, "_collect_alpaca_broker_state_snapshot", lambda **_kwargs: fake_snapshot)

    rc = module.main(
        [
            "inspect-broker-state",
            "--output-dir",
            str(tmp_path / "inspections"),
            "--notes",
            "manual-state-check",
        ]
    )

    assert rc == 0
    run_dir = tmp_path / "inspections" / "broker_state_inspection_20260326T170000_seed"
    report_json = json.loads((run_dir / "broker_state_report.json").read_text(encoding="utf-8"))
    report_md = (run_dir / "broker_state_report.md").read_text(encoding="utf-8")
    assert report_json["recommended_next_action"] == "reduce positions"
    assert report_json["feasible_routes"]["sell_only_campaign"] is True
    assert report_json["feasible_routes"]["buy_only_campaign"] is False
    assert report_json["positions_summary"]["positions_count"] == 2
    assert "reduce positions" in report_md
    assert "sell_only_campaign" in report_md


def test_main_off_hours_prep_writes_reports_and_minimal_validation_plan(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = _load_pilot_ops_module()
    fake_snapshot = {
        "captured_at_utc": "2026-03-26T12:00:00+00:00",
        "account": {
            "buying_power": 2000000.0,
            "cash": 1000000.0,
            "equity": 2000000.0,
            "portfolio_value": 2000000.0,
            "account_type": "paper",
        },
        "positions": pd.DataFrame(
            [
                {"ticker": "AAPL", "quantity": 10.0, "market_value": 1500.0, "unrealized_pnl": 25.0},
            ]
        ),
        "open_orders": [
            {
                "order_id": "buy-1",
                "ticker": "AAPL",
                "direction": "buy",
                "order_type": "limit",
                "time_in_force": "day",
                "status": "open",
                "quantity": 10,
                "filled_qty": 0,
                "submitted_at": "2026-03-26T06:00:00+00:00",
            }
        ],
    }
    prep_dir = tmp_path / "slippage_prep"
    bucket_plan_file = tmp_path / "bucket_plan.yaml"
    bucket_plan_file.write_text("selection:\n  source_run_roots: []\n  source_orders_oms: []\ntargets: []\n", encoding="utf-8")

    monkeypatch.setenv("ALPACA_API_KEY", "test-api-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test-secret-key")
    monkeypatch.setattr(module.fill_collection, "generate_run_id", lambda **_kwargs: "off_hours_prep_20260326T170000_seed")
    monkeypatch.setattr(module, "_collect_alpaca_broker_state_snapshot", lambda **_kwargs: fake_snapshot)
    monkeypatch.setattr(module.fill_collection, "is_us_market_open", lambda **_kwargs: False)
    monkeypatch.setattr(
        module.slippage_calibration,
        "prepare_slippage_calibration_prep",
        lambda **_kwargs: {
            "prep_root": prep_dir,
            "fill_collection_dir": prep_dir / "fill_collection",
            "dataset_dir": prep_dir / "dataset",
            "residuals_dir": prep_dir / "residuals",
            "candidate_overlay_dir": prep_dir / "candidate_overlay",
            "diagnostics_dir": prep_dir / "diagnostics",
            "slippage_calibration_prep_manifest": prep_dir / "slippage_calibration_prep_manifest.json",
            "slippage_calibration_prep_checklist": prep_dir / "slippage_calibration_prep_checklist.md",
        },
    )

    rc = module.main(
        [
            "off-hours-prep",
            "--output-dir",
            str(tmp_path / "off_hours"),
            "--bucket-plan-file",
            str(bucket_plan_file),
            "--notes",
            "manual-off-hours-prep",
        ]
    )

    assert rc == 0
    run_dir = tmp_path / "off_hours" / "off_hours_prep_20260326T170000_seed"
    report_json = json.loads((run_dir / "off_hours_prep_manifest.json").read_text(encoding="utf-8"))
    report_md = (run_dir / "off_hours_prep_report.md").read_text(encoding="utf-8")
    plan_yaml = yaml.safe_load((run_dir / "tomorrow_minimal_validation_plan.yaml").read_text(encoding="utf-8"))
    assert report_json["prep_mode"] == "read_only_off_hours_prep"
    assert report_json["broker_state_inspection"]["open_orders_summary"]["open_orders_count"] == 1
    assert report_json["tomorrow_minimal_validation_plan"]["campaign_preset"] == "minimal-buy-validation"
    assert report_json["tomorrow_minimal_validation_plan"]["side_scope"] == "buy-only"
    assert report_json["tomorrow_minimal_validation_plan"]["max_seed_orders"] == 1
    assert plan_yaml["campaign_preset"] == "minimal-buy-validation"
    assert "Slippage Calibration Prep" in report_md
    assert "Tomorrow Minimal Validation Plan" in report_md
    assert "open_orders_count" in report_md


def test_build_parser_accepts_calibrate_slippage_command(tmp_path: Path) -> None:
    module = _load_pilot_ops_module()

    args = module.build_parser().parse_args(
        [
            "calibrate-slippage",
            "--fill-collection-root",
            str(tmp_path / "fills"),
            "--source-run-root",
            str(tmp_path / "source"),
            "--output-dir",
            str(tmp_path / "output"),
            "--alpha",
            "0.7",
            "--min-filled-orders",
            "12",
            "--min-participation-span",
            "8.5",
        ]
    )

    assert args.command == "calibrate-slippage"
    assert args.fill_collection_root == tmp_path / "fills"
    assert args.source_run_root == tmp_path / "source"
    assert args.output_dir == tmp_path / "output"
    assert args.alpha == pytest.approx(0.7)
    assert args.min_filled_orders == 12
    assert args.min_participation_span == pytest.approx(8.5)


def test_main_calibrate_slippage_runs_tca_pipeline(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    captured: dict[str, object] = {}
    artifact_dir = tmp_path / "tca_run"
    artifact_dir.mkdir(parents=True, exist_ok=True)

    class _FakeResult:
        def __init__(self) -> None:
            self.summary = {
                "candidate_k": 0.021,
                "overlay_readiness": "sufficient",
                "next_recommended_action": "apply_as_paper_overlay",
            }

    def _fake_calibrate_slippage(**kwargs):
        captured["calibrate_kwargs"] = kwargs
        return _FakeResult()

    def _fake_write_artifacts(*, result, output_dir):
        captured["write_result"] = result.summary
        captured["write_output_dir"] = output_dir
        report = Path(output_dir) / "slippage_calibration_report.md"
        summary = Path(output_dir) / "slippage_calibration.json"
        report.write_text("# report\n", encoding="utf-8")
        summary.write_text(json.dumps(result.summary), encoding="utf-8")
        return {
            "slippage_calibration_report": report,
            "slippage_calibration_json": summary,
        }

    monkeypatch.setattr(module.slippage_calibration, "calibrate_slippage", _fake_calibrate_slippage)
    monkeypatch.setattr(module.slippage_calibration, "write_slippage_calibration_artifacts", _fake_write_artifacts)

    rc = module.main(
        [
            "calibrate-slippage",
            "--fill-collection-root",
            str(tmp_path / "fills"),
            "--source-run-root",
            str(tmp_path / "source"),
            "--output-dir",
            str(artifact_dir),
            "--alpha",
            "0.7",
            "--min-filled-orders",
            "12",
            "--min-participation-span",
            "8.5",
        ]
    )

    assert rc == 0
    assert captured["calibrate_kwargs"]["fill_collection_root"] == tmp_path / "fills"
    assert captured["calibrate_kwargs"]["source_run_root"] == tmp_path / "source"
    assert captured["calibrate_kwargs"]["output_dir"] == artifact_dir
    assert captured["calibrate_kwargs"]["alpha"] == pytest.approx(0.7)
    assert captured["calibrate_kwargs"]["min_filled_orders"] == 12
    assert captured["calibrate_kwargs"]["min_participation_span"] == pytest.approx(8.5)
    assert captured["write_result"]["overlay_readiness"] == "sufficient"
    assert captured["write_result"]["next_recommended_action"] == "apply_as_paper_overlay"


def test_main_pre_submit_check_uses_snapshot_file_and_writes_artifacts(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    module = _load_pilot_ops_module()
    orders_path = tmp_path / "sample_us_01_orders_oms.csv"
    _write_orders_oms_csv(
        orders_path,
        [
            {
                "sample_id": "sample_us_01",
                "ticker": "AAPL",
                "direction": "buy",
                "quantity": 10,
                "estimated_price": 100.0,
                "price_limit": "",
            },
            {
                "sample_id": "sample_us_01",
                "ticker": "MSFT",
                "direction": "sell",
                "quantity": 5,
                "estimated_price": 200.0,
                "price_limit": "",
            },
        ],
    )
    snapshot_path = tmp_path / "broker_state_report.json"
    snapshot_path.write_text(
        json.dumps(
            {
                "inspected_at_utc": "2026-03-26T12:00:00+00:00",
                "account_snapshot": {
                    "status": "ACTIVE",
                    "buying_power": 1500.0,
                    "cash": 2000.0,
                    "equity": 2500.0,
                    "account_type": "paper",
                    "trading_blocked": False,
                    "transfers_blocked": False,
                },
                "positions": [
                    {"ticker": "MSFT", "quantity": 2.0, "market_value": 400.0, "unrealized_pnl": 10.0},
                ],
                "open_orders": [
                    {
                        "order_id": "buy-1",
                        "ticker": "QQQ",
                        "direction": "buy",
                        "order_type": "limit",
                        "time_in_force": "day",
                        "status": "open",
                        "quantity": 1,
                        "filled_qty": 0,
                        "submitted_at": "2026-03-26T06:00:00+00:00",
                    }
                ],
                "broker_state_snapshot": {
                    "captured_at_utc": "2026-03-26T12:00:00+00:00",
                    "positions": [{"ticker": "MSFT", "quantity": 2.0}],
                    "open_orders": [{"ticker": "QQQ", "direction": "buy"}],
                },
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8-sig",
    )

    monkeypatch.setattr(module.fill_collection, "generate_run_id", lambda **_kwargs: "pre_submission_check_20260326T170000_seed")
    rc = module.main(
        [
            "pre-submit-check",
            "--orders-oms",
            str(orders_path),
            "--broker-state-snapshot",
            str(snapshot_path),
            "--output-dir",
            str(tmp_path / "pre_submit"),
            "--notes",
            "sample-us-check",
        ]
    )

    assert rc == 0
    run_dir = tmp_path / "pre_submit" / "pre_submission_check_20260326T170000_seed"
    report_json = json.loads((run_dir / "pre_submission_check.json").read_text(encoding="utf-8"))
    report_md = (run_dir / "pre_submission_check.md").read_text(encoding="utf-8")
    summary_csv = pd.read_csv(run_dir / "basket_precheck_summary.csv")
    prepared_orders = pd.read_csv(run_dir / "prepared_orders" / "sample_us_01_prepared_orders.csv")

    assert report_json["snapshot_source"] == "file"
    assert report_json["overall_recommendation"] == "review_required"
    assert report_json["open_orders_summary"]["open_orders_count"] == 1
    assert report_json["basket_checks"][0]["recommendation"] == "review_open_orders_before_submit"
    assert report_json["basket_checks"][0]["clipped_reason_counts"]["clipped_to_available_position"] == 1
    assert int(summary_csv.loc[0, "submitted_order_count"]) == 2
    assert len(prepared_orders) == 2
    assert float(prepared_orders.loc[prepared_orders["ticker"] == "MSFT", "quantity"].iloc[0]) == 2.0
    assert "review_open_orders_before_submit" in report_md
    assert "sample_us_01" in report_md


def test_run_ops_blocks_decision_flow_when_comparison_ineligible(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    module = _load_pilot_ops_module()
    run_root = tmp_path / "pilot_validation_20260325_111111"
    evaluation_dir = run_root / "evaluation"
    evaluation_dir.mkdir(parents=True, exist_ok=True)

    sample_csv = evaluation_dir / "sample_assessment.csv"
    with sample_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sample_id",
                "market_builder_success",
                "reference_builder_success",
                "main_flow_success",
                "scenario_success",
                "approval_success",
                "execution_success",
                "benchmark_json_path",
                "execution_report_path",
                "scenario_comparison_path",
                "approval_record_path",
                "main_audit_path",
                "cost_difference",
                "order_reasonableness_score",
                "findings_explainability_score",
                "execution_credibility_score",
                "override_used",
                "scenario_score_gap",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "sample_id": "sample_01",
                "market_builder_success": "true",
                "reference_builder_success": "true",
                "main_flow_success": "true",
                "scenario_success": "true",
                "approval_success": "true",
                "execution_success": "true",
                "benchmark_json_path": "x",
                "execution_report_path": "x",
                "scenario_comparison_path": "x",
                "approval_record_path": "x",
                "main_audit_path": "x",
                "cost_difference": "1.0",
                "order_reasonableness_score": "4",
                "findings_explainability_score": "4",
                "execution_credibility_score": "4",
                "override_used": "false",
                "scenario_score_gap": "0.02",
            }
        )

    (evaluation_dir / "provider_capability_report.json").write_text("{}", encoding="utf-8")
    (evaluation_dir / "comparison_eligibility.json").write_text(
        json.dumps(
            {
                "eligible": False,
                "reasons": ["baseline duplicate dates detected", "w5=0.1: date mismatch vs baseline"],
                "baseline_quality_flags": ["baseline_duplicate_dates"],
            }
        ),
        encoding="utf-8",
    )

    monkeypatch.setattr(module, "_run_validation", lambda **_kwargs: (0, run_root))
    rc = module.run_ops(
        mode="release",
        phase="phase_2",
        reviewer_input=tmp_path / "reviewer.csv",
        real_sample=False,
        rebalance_triggered=False,
        incident_id="",
        notes="",
        output_dir=tmp_path / "tracking",
    )

    assert rc == module.EXIT_CODE_ELIGIBILITY_INELIGIBLE
    captured = capsys.readouterr()
    assert module.ELIGIBILITY_LOG_INELIGIBLE in captured.out
    dashboard_path = tmp_path / "tracking" / "pilot_dashboard.csv"
    row = list(csv.DictReader(dashboard_path.open("r", encoding="utf-8-sig", newline="")))[-1]
    assert row["comparison_eligibility_status"] == "INELIGIBLE"
    assert row["comparison_eligibility_reason_count"] == "2"
    assert row["release_status"] == "ineligible"
    assert row["release_gate_passed"] == "false"


def test_run_ops_keeps_decision_flow_open_when_comparison_eligible(monkeypatch: pytest.MonkeyPatch, tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    run_root = tmp_path / "pilot_validation_20260325_111222"
    evaluation_dir = run_root / "evaluation"
    evaluation_dir.mkdir(parents=True, exist_ok=True)

    sample_csv = evaluation_dir / "sample_assessment.csv"
    with sample_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sample_id",
                "market_builder_success",
                "reference_builder_success",
                "main_flow_success",
                "scenario_success",
                "approval_success",
                "execution_success",
                "benchmark_json_path",
                "execution_report_path",
                "scenario_comparison_path",
                "approval_record_path",
                "main_audit_path",
                "cost_difference",
                "order_reasonableness_score",
                "findings_explainability_score",
                "execution_credibility_score",
                "override_used",
                "scenario_score_gap",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "sample_id": "sample_01",
                "market_builder_success": "true",
                "reference_builder_success": "true",
                "main_flow_success": "true",
                "scenario_success": "true",
                "approval_success": "true",
                "execution_success": "true",
                "benchmark_json_path": "x",
                "execution_report_path": "x",
                "scenario_comparison_path": "x",
                "approval_record_path": "x",
                "main_audit_path": "x",
                "cost_difference": "1.0",
                "order_reasonableness_score": "4",
                "findings_explainability_score": "4",
                "execution_credibility_score": "4",
                "override_used": "false",
                "scenario_score_gap": "0.02",
            }
        )

    (evaluation_dir / "provider_capability_report.json").write_text("{}", encoding="utf-8")
    (evaluation_dir / "comparison_eligibility.json").write_text(
        json.dumps({"eligible": True, "reasons": [], "baseline_quality_flags": []}),
        encoding="utf-8",
    )

    monkeypatch.setattr(module, "_run_validation", lambda **_kwargs: (0, run_root))
    rc = module.run_ops(
        mode="release",
        phase="phase_2",
        reviewer_input=tmp_path / "reviewer.csv",
        real_sample=False,
        rebalance_triggered=False,
        incident_id="",
        notes="",
        output_dir=tmp_path / "tracking",
    )

    assert rc == 0
    dashboard_path = tmp_path / "tracking" / "pilot_dashboard.csv"
    row = list(csv.DictReader(dashboard_path.open("r", encoding="utf-8-sig", newline="")))[-1]
    assert row["comparison_eligibility_status"] == "ELIGIBLE"
    assert row["comparison_eligibility_reason_count"] == "0"
    assert row["release_status"] == "passed"
    assert row["release_gate_passed"] == "true"


def test_generate_go_nogo_report_aggregates_eligibility_reason_count(tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    dashboard = tmp_path / "pilot_dashboard.csv"
    incident = tmp_path / "incident_register.csv"
    output = tmp_path / "go_nogo_status.md"

    with dashboard.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=module.DASHBOARD_HEADERS)
        writer.writeheader()
        writer.writerow(
            {
                "date": "2026-03-21",
                "phase": "phase_2",
                "mode": "release",
                "run_root": "outputs/pilot_validation_x",
                "as_of_date": "2026-03-21",
                "nightly_status": "pass",
                "release_status": "ineligible",
                "release_gate_passed": "false",
                "rebalance_triggered": "false",
                "artifact_chain_complete": "true",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "primary_feed_success": "true",
                "fallback_activated": "",
                "solver_primary": "CLARABEL",
                "blocked_untradeable_count": "0",
                "static_count": "5",
                "real_count": "1",
                "full_chain_success_static": "5",
                "full_chain_success_real": "1",
                "override_used_static": "0",
                "score_gap_ge_001_static": "5",
                "cost_better_ratio_static": "1.0",
                "solver_fallback_used_static": "0",
                "solver_sample_count_static": "5",
                "mean_order_reasonableness_static": "4.0",
                "mean_findings_explainability_static": "4.0",
                "mean_execution_credibility_static": "4.0",
                "execution_residual_risk_consistent": "true",
                "provider_blockers_count": "0",
                "comparison_eligibility_status": "INELIGIBLE",
                "comparison_eligibility_reason_count": "2",
                "incident_id": "",
                "notes": "",
            }
        )

    incident.write_text(
        "incident_id,date,severity,category,description,root_cause,resolution,time_to_resolve_hours,recurrence,linked_kpi_impact\n",
        encoding="utf-8",
    )

    module.generate_go_nogo_report(
        dashboard_path=dashboard,
        incident_path=incident,
        output_path=output,
        as_of_date="2026-03-21",
        window_trading_days=20,
    )
    rendered = output.read_text(encoding="utf-8")
    assert "C11_comparison_eligibility_gate" in rendered
    assert "reasons_total=2" in rendered


def test_generate_go_nogo_report_marks_c11_waive_when_only_not_available(tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    dashboard = tmp_path / "pilot_dashboard.csv"
    incident = tmp_path / "incident_register.csv"
    output = tmp_path / "go_nogo_status.md"

    with dashboard.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=module.DASHBOARD_HEADERS)
        writer.writeheader()
        writer.writerow(
            {
                "date": "2026-03-22",
                "phase": "phase_2",
                "mode": "release",
                "run_root": "outputs/pilot_validation_na",
                "as_of_date": "2026-03-22",
                "nightly_status": "pass",
                "release_status": "passed",
                "release_gate_passed": "true",
                "rebalance_triggered": "false",
                "artifact_chain_complete": "true",
                "override_count": "0",
                "cost_better_ratio": "1.0",
                "primary_feed_success": "true",
                "fallback_activated": "",
                "solver_primary": "CLARABEL",
                "blocked_untradeable_count": "0",
                "static_count": "5",
                "real_count": "1",
                "full_chain_success_static": "5",
                "full_chain_success_real": "1",
                "override_used_static": "0",
                "score_gap_ge_001_static": "5",
                "cost_better_ratio_static": "1.0",
                "solver_fallback_used_static": "0",
                "solver_sample_count_static": "5",
                "mean_order_reasonableness_static": "4.0",
                "mean_findings_explainability_static": "4.0",
                "mean_execution_credibility_static": "4.0",
                "execution_residual_risk_consistent": "true",
                "provider_blockers_count": "0",
                "comparison_eligibility_status": "NOT_AVAILABLE",
                "comparison_eligibility_reason_count": "0",
                "incident_id": "",
                "notes": "",
            }
        )

    incident.write_text(
        "incident_id,date,severity,category,description,root_cause,resolution,time_to_resolve_hours,recurrence,linked_kpi_impact\n",
        encoding="utf-8",
    )

    module.generate_go_nogo_report(
        dashboard_path=dashboard,
        incident_path=incident,
        output_path=output,
        as_of_date="2026-03-22",
        window_trading_days=20,
    )
    rendered = output.read_text(encoding="utf-8")
    assert "C11_comparison_eligibility_gate" in rendered
    assert "| WAIVE |" in rendered


def test_go_nogo_c11_aggregation_is_consistent_with_dashboard_rows(tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    dashboard = tmp_path / "pilot_dashboard.csv"
    incident = tmp_path / "incident_register.csv"
    output = tmp_path / "go_nogo_status.md"

    rows = [
        {
            "date": "2026-03-23",
            "phase": "phase_2",
            "mode": "release",
            "run_root": "outputs/pilot_validation_a",
            "as_of_date": "2026-03-23",
            "nightly_status": "pass",
            "release_status": "passed",
            "release_gate_passed": "true",
            "rebalance_triggered": "false",
            "artifact_chain_complete": "true",
            "override_count": "0",
            "cost_better_ratio": "1.0",
            "primary_feed_success": "true",
            "fallback_activated": "",
            "solver_primary": "CLARABEL",
            "blocked_untradeable_count": "0",
            "static_count": "5",
            "real_count": "1",
            "full_chain_success_static": "5",
            "full_chain_success_real": "1",
            "override_used_static": "0",
            "score_gap_ge_001_static": "5",
            "cost_better_ratio_static": "1.0",
            "solver_fallback_used_static": "0",
            "solver_sample_count_static": "5",
            "mean_order_reasonableness_static": "4.0",
            "mean_findings_explainability_static": "4.0",
            "mean_execution_credibility_static": "4.0",
            "execution_residual_risk_consistent": "true",
            "provider_blockers_count": "0",
            "comparison_eligibility_status": "ELIGIBLE",
            "comparison_eligibility_reason_count": "0",
            "incident_id": "",
            "notes": "",
        },
        {
            "date": "2026-03-24",
            "phase": "phase_2",
            "mode": "release",
            "run_root": "outputs/pilot_validation_b",
            "as_of_date": "2026-03-24",
            "nightly_status": "ineligible",
            "release_status": "ineligible",
            "release_gate_passed": "false",
            "rebalance_triggered": "false",
            "artifact_chain_complete": "true",
            "override_count": "0",
            "cost_better_ratio": "1.0",
            "primary_feed_success": "true",
            "fallback_activated": "",
            "solver_primary": "CLARABEL",
            "blocked_untradeable_count": "0",
            "static_count": "5",
            "real_count": "1",
            "full_chain_success_static": "5",
            "full_chain_success_real": "1",
            "override_used_static": "0",
            "score_gap_ge_001_static": "5",
            "cost_better_ratio_static": "1.0",
            "solver_fallback_used_static": "0",
            "solver_sample_count_static": "5",
            "mean_order_reasonableness_static": "4.0",
            "mean_findings_explainability_static": "4.0",
            "mean_execution_credibility_static": "4.0",
            "execution_residual_risk_consistent": "true",
            "provider_blockers_count": "0",
            "comparison_eligibility_status": "INELIGIBLE",
            "comparison_eligibility_reason_count": "2",
            "incident_id": "",
            "notes": "",
        },
        {
            "date": "2026-03-25",
            "phase": "phase_2",
            "mode": "release",
            "run_root": "outputs/pilot_validation_c",
            "as_of_date": "2026-03-25",
            "nightly_status": "ineligible",
            "release_status": "ineligible",
            "release_gate_passed": "false",
            "rebalance_triggered": "false",
            "artifact_chain_complete": "true",
            "override_count": "0",
            "cost_better_ratio": "1.0",
            "primary_feed_success": "true",
            "fallback_activated": "",
            "solver_primary": "CLARABEL",
            "blocked_untradeable_count": "0",
            "static_count": "5",
            "real_count": "1",
            "full_chain_success_static": "5",
            "full_chain_success_real": "1",
            "override_used_static": "0",
            "score_gap_ge_001_static": "5",
            "cost_better_ratio_static": "1.0",
            "solver_fallback_used_static": "0",
            "solver_sample_count_static": "5",
            "mean_order_reasonableness_static": "4.0",
            "mean_findings_explainability_static": "4.0",
            "mean_execution_credibility_static": "4.0",
            "execution_residual_risk_consistent": "true",
            "provider_blockers_count": "0",
            "comparison_eligibility_status": "INVALID",
            "comparison_eligibility_reason_count": "1",
            "incident_id": "",
            "notes": "",
        },
    ]

    with dashboard.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=module.DASHBOARD_HEADERS)
        writer.writeheader()
        writer.writerows(rows)

    incident.write_text(
        "incident_id,date,severity,category,description,root_cause,resolution,time_to_resolve_hours,recurrence,linked_kpi_impact\n",
        encoding="utf-8",
    )

    module.generate_go_nogo_report(
        dashboard_path=dashboard,
        incident_path=incident,
        output_path=output,
        as_of_date="2026-03-25",
        window_trading_days=20,
    )
    rendered = output.read_text(encoding="utf-8")
    assert "C11_comparison_eligibility_gate" in rendered
    assert "eligible=1/3" in rendered
    assert "ineligible=1/3" in rendered
    assert "invalid=1/3" in rendered
    assert "not_available=0/3" in rendered
    assert "reasons_total=3" in rendered
    assert "| FAIL |" in rendered


def test_eligibility_json_contract_maps_to_status_and_exit_code_consistently(tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    run_root = tmp_path / "pilot_validation_eligibility_contract"
    evaluation_dir = run_root / "evaluation"
    evaluation_dir.mkdir(parents=True, exist_ok=True)

    (evaluation_dir / "comparison_eligibility.json").write_text(
        json.dumps(
            {
                "eligible": False,
                "reasons": ["baseline duplicate dates detected", "w5=0.1: date mismatch vs baseline"],
                "baseline_quality_flags": ["baseline_duplicate_dates"],
            }
        ),
        encoding="utf-8",
    )

    payload, load_error = module._load_comparison_eligibility(run_root)
    status, reasons, flags = module._resolve_comparison_eligibility(
        payload,
        load_error=load_error,
        ab_flow=True,
        require_eligibility_gate=True,
    )
    exit_code, _log = module._eligibility_gate_outcome(
        status=status,
        ab_flow=True,
        require_eligibility_gate=True,
    )

    assert status == module.COMPARISON_ELIGIBILITY_INELIGIBLE
    assert len(reasons) == 2
    assert len(flags) == 1
    assert exit_code == module.EXIT_CODE_ELIGIBILITY_INELIGIBLE


def test_run_ops_blocks_when_ab_require_gate_and_eligibility_file_missing(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    module = _load_pilot_ops_module()
    run_root = tmp_path / "pilot_validation_20260325_111333"
    evaluation_dir = run_root / "evaluation"
    evaluation_dir.mkdir(parents=True, exist_ok=True)

    sample_csv = evaluation_dir / "sample_assessment.csv"
    with sample_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sample_id",
                "market_builder_success",
                "reference_builder_success",
                "main_flow_success",
                "scenario_success",
                "approval_success",
                "execution_success",
                "benchmark_json_path",
                "execution_report_path",
                "scenario_comparison_path",
                "approval_record_path",
                "main_audit_path",
                "cost_difference",
                "order_reasonableness_score",
                "findings_explainability_score",
                "execution_credibility_score",
                "override_used",
                "scenario_score_gap",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "sample_id": "sample_01",
                "market_builder_success": "true",
                "reference_builder_success": "true",
                "main_flow_success": "true",
                "scenario_success": "true",
                "approval_success": "true",
                "execution_success": "true",
                "benchmark_json_path": "x",
                "execution_report_path": "x",
                "scenario_comparison_path": "x",
                "approval_record_path": "x",
                "main_audit_path": "x",
                "cost_difference": "1.0",
                "order_reasonableness_score": "4",
                "findings_explainability_score": "4",
                "execution_credibility_score": "4",
                "override_used": "false",
                "scenario_score_gap": "0.02",
            }
        )

    (evaluation_dir / "provider_capability_report.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(module, "_run_validation", lambda **_kwargs: (0, run_root))
    rc = module.run_ops(
        mode="release",
        phase="phase_2",
        reviewer_input=tmp_path / "reviewer.csv",
        real_sample=False,
        rebalance_triggered=False,
        incident_id="",
        notes="",
        output_dir=tmp_path / "tracking",
        ab_flow=True,
        require_eligibility_gate=True,
    )
    assert rc == module.EXIT_CODE_ELIGIBILITY_MISSING
    captured = capsys.readouterr()
    assert module.ELIGIBILITY_LOG_MISSING in captured.out
    row = list(
        csv.DictReader((tmp_path / "tracking" / "pilot_dashboard.csv").open("r", encoding="utf-8-sig", newline=""))
    )[-1]
    assert row["comparison_eligibility_status"] == module.COMPARISON_ELIGIBILITY_NOT_AVAILABLE
    assert row["comparison_eligibility_reason_count"] == "1"


def test_run_ops_marks_invalid_when_eligibility_json_malformed(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    module = _load_pilot_ops_module()
    run_root = tmp_path / "pilot_validation_20260325_111444"
    evaluation_dir = run_root / "evaluation"
    evaluation_dir.mkdir(parents=True, exist_ok=True)

    sample_csv = evaluation_dir / "sample_assessment.csv"
    with sample_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sample_id",
                "market_builder_success",
                "reference_builder_success",
                "main_flow_success",
                "scenario_success",
                "approval_success",
                "execution_success",
                "benchmark_json_path",
                "execution_report_path",
                "scenario_comparison_path",
                "approval_record_path",
                "main_audit_path",
                "cost_difference",
                "order_reasonableness_score",
                "findings_explainability_score",
                "execution_credibility_score",
                "override_used",
                "scenario_score_gap",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "sample_id": "sample_01",
                "market_builder_success": "true",
                "reference_builder_success": "true",
                "main_flow_success": "true",
                "scenario_success": "true",
                "approval_success": "true",
                "execution_success": "true",
                "benchmark_json_path": "x",
                "execution_report_path": "x",
                "scenario_comparison_path": "x",
                "approval_record_path": "x",
                "main_audit_path": "x",
                "cost_difference": "1.0",
                "order_reasonableness_score": "4",
                "findings_explainability_score": "4",
                "execution_credibility_score": "4",
                "override_used": "false",
                "scenario_score_gap": "0.02",
            }
        )

    (evaluation_dir / "provider_capability_report.json").write_text("{}", encoding="utf-8")
    (evaluation_dir / "comparison_eligibility.json").write_text("{bad json", encoding="utf-8")
    monkeypatch.setattr(module, "_run_validation", lambda **_kwargs: (0, run_root))
    rc = module.run_ops(
        mode="release",
        phase="phase_2",
        reviewer_input=tmp_path / "reviewer.csv",
        real_sample=False,
        rebalance_triggered=False,
        incident_id="",
        notes="",
        output_dir=tmp_path / "tracking",
    )
    assert rc == module.EXIT_CODE_ELIGIBILITY_INVALID
    captured = capsys.readouterr()
    assert module.ELIGIBILITY_LOG_INVALID in captured.out
    row = list(
        csv.DictReader((tmp_path / "tracking" / "pilot_dashboard.csv").open("r", encoding="utf-8-sig", newline=""))
    )[-1]
    assert row["comparison_eligibility_status"] == module.COMPARISON_ELIGIBILITY_INVALID
    assert row["comparison_eligibility_reason_count"] == "1"


def test_run_ops_non_ab_flow_missing_eligibility_does_not_block(
    monkeypatch: pytest.MonkeyPatch, tmp_path: Path
) -> None:
    module = _load_pilot_ops_module()
    run_root = tmp_path / "pilot_validation_20260325_111555"
    evaluation_dir = run_root / "evaluation"
    evaluation_dir.mkdir(parents=True, exist_ok=True)

    sample_csv = evaluation_dir / "sample_assessment.csv"
    with sample_csv.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=[
                "sample_id",
                "market_builder_success",
                "reference_builder_success",
                "main_flow_success",
                "scenario_success",
                "approval_success",
                "execution_success",
                "benchmark_json_path",
                "execution_report_path",
                "scenario_comparison_path",
                "approval_record_path",
                "main_audit_path",
                "cost_difference",
                "order_reasonableness_score",
                "findings_explainability_score",
                "execution_credibility_score",
                "override_used",
                "scenario_score_gap",
            ],
        )
        writer.writeheader()
        writer.writerow(
            {
                "sample_id": "sample_01",
                "market_builder_success": "true",
                "reference_builder_success": "true",
                "main_flow_success": "true",
                "scenario_success": "true",
                "approval_success": "true",
                "execution_success": "true",
                "benchmark_json_path": "x",
                "execution_report_path": "x",
                "scenario_comparison_path": "x",
                "approval_record_path": "x",
                "main_audit_path": "x",
                "cost_difference": "1.0",
                "order_reasonableness_score": "4",
                "findings_explainability_score": "4",
                "execution_credibility_score": "4",
                "override_used": "false",
                "scenario_score_gap": "0.02",
            }
        )

    (evaluation_dir / "provider_capability_report.json").write_text("{}", encoding="utf-8")
    monkeypatch.setattr(module, "_run_validation", lambda **_kwargs: (0, run_root))
    rc = module.run_ops(
        mode="release",
        phase="phase_2",
        reviewer_input=tmp_path / "reviewer.csv",
        real_sample=False,
        rebalance_triggered=False,
        incident_id="",
        notes="",
        output_dir=tmp_path / "tracking",
        ab_flow=False,
        require_eligibility_gate=True,
    )
    assert rc == 0
    row = list(
        csv.DictReader((tmp_path / "tracking" / "pilot_dashboard.csv").open("r", encoding="utf-8-sig", newline=""))
    )[-1]
    assert row["comparison_eligibility_status"] == module.COMPARISON_ELIGIBILITY_NOT_AVAILABLE
    assert row["comparison_eligibility_reason_count"] == "0"


def test_collect_fills_subcommand_parses_arguments() -> None:
    module = _load_pilot_ops_module()
    args = module.build_parser().parse_args(
        [
            "collect-fills",
            "--run-root",
            "C:/tmp/run_root",
            "--orders-oms",
            "C:/tmp/orders.csv",
            "--output-dir",
            "C:/tmp/output",
            "--notes",
            "fill-telemetry",
        ]
    )

    assert args.command == "collect-fills"
    assert str(args.orders_oms).replace("\\", "/").endswith("C:/tmp/orders.csv")
    assert str(args.run_root).replace("\\", "/").endswith("C:/tmp/run_root")
    assert str(args.output_dir).replace("\\", "/").endswith("C:/tmp/output")
    assert args.market == "us"
    assert args.broker == "alpaca"
    assert args.timeout_seconds == 300.0
    assert args.poll_interval_seconds == 1.0


def test_collect_fills_campaign_subcommand_parses_arguments() -> None:
    module = _load_pilot_ops_module()
    args = module.build_parser().parse_args(
        [
            "collect-fills-campaign",
            "--bucket-plan-file",
            "C:/tmp/bucket_plan.yaml",
            "--output-dir",
            "C:/tmp/campaign_output",
            "--run-root",
            "C:/tmp/run_root",
            "--source-orders-oms",
            "C:/tmp/orders_01.csv",
            "C:/tmp/orders_02.csv",
            "--max-runs",
            "5",
            "--notes",
            "campaign",
        ]
    )

    assert args.command == "collect-fills-campaign"
    assert str(args.bucket_plan_file).replace("\\", "/").endswith("C:/tmp/bucket_plan.yaml")
    assert str(args.output_dir).replace("\\", "/").endswith("C:/tmp/campaign_output")
    assert str(args.run_root).replace("\\", "/").endswith("C:/tmp/run_root")
    assert [str(path).replace("\\", "/").split("/")[-1] for path in args.source_orders_oms] == ["orders_01.csv", "orders_02.csv"]
    assert args.max_runs == 5
    assert args.market == "us"
    assert args.broker == "alpaca"


def test_collect_fills_from_orders_oms_writes_artifacts(tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    orders_oms = tmp_path / "orders_oms.csv"
    run_root = tmp_path / "run_root"
    run_root.mkdir(parents=True, exist_ok=True)
    _write_orders_oms_csv(
        orders_oms,
        [
            {
                "sample_id": "basket_01",
                "ticker": "AAPL",
                "direction": "buy",
                "quantity": 2,
                "estimated_price": 100.0,
                "price_limit": "",
            },
            {
                "sample_id": "basket_01",
                "ticker": "MSFT",
                "direction": "sell",
                "quantity": 4,
                "estimated_price": 110.0,
                "price_limit": "",
            },
        ],
    )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(module.fill_collection, "generate_run_id", lambda **_kwargs: "orders_oms_run")
    monkeypatch.setenv("ALPACA_API_KEY", "test-api-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test-secret-key")
    try:
        run_dir = module._run_alpaca_fill_collection(
            run_root=run_root,
            orders_oms=orders_oms,
            output_dir=tmp_path / "output",
            market="us",
            broker="alpaca",
            timeout_seconds=5.0,
            poll_interval_seconds=0.1,
            notes="direct-orders",
            force_outside_market_hours=False,
            adapter_factory=_FakeFillCollectionAdapter,
        )
    finally:
        monkeypatch.undo()

    assert run_dir == (tmp_path / "output" / "orders_oms_run")
    manifest_path = run_dir / "alpaca_fill_manifest.json"
    orders_path = run_dir / "alpaca_fill_orders.csv"
    events_path = run_dir / "alpaca_fill_events.csv"
    assert manifest_path.exists()
    assert orders_path.exists()
    assert events_path.exists()
    assert (run_dir / "broker_account_before.json").exists()
    assert (run_dir / "broker_account_after.json").exists()
    assert (run_dir / "broker_positions_before.csv").exists()
    assert (run_dir / "broker_positions_after.csv").exists()
    assert (run_dir / "reconciliation_report.json").exists()
    assert (run_dir / "execution_result.json").exists()
    assert (run_dir / "execution_result.csv").exists()
    assert (run_dir / "alpaca_fill_summary.md").exists()

    manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert manifest["source_type"] == "orders_oms"
    assert manifest["order_count"] == 2
    assert manifest["submitted_count"] == 2
    assert manifest["filled_count"] == 1
    assert manifest["partial_count"] == 1
    assert manifest["has_any_filled_orders"] is True
    assert manifest["fill_rate"] == pytest.approx(0.5)
    assert manifest["status_mix"]["filled"] == 1
    assert manifest["status_mix"]["partially_filled"] == 1

    order_frame = pd.read_csv(orders_path)
    filled_mask = pd.to_numeric(order_frame["filled_qty"], errors="coerce").fillna(0.0) > 0
    weighted_avg = float(
        pd.to_numeric(order_frame.loc[filled_mask, "filled_notional"], errors="coerce").fillna(0.0).sum()
        / pd.to_numeric(order_frame.loc[filled_mask, "filled_qty"], errors="coerce").fillna(0.0).sum()
    )
    assert manifest["avg_fill_price_mean"] == pytest.approx(weighted_avg)
    assert manifest["latency_seconds_mean"] == pytest.approx(float(order_frame["latency_seconds"].mean()))
    assert {"submitted_at_utc", "terminal_at_utc", "latency_seconds", "poll_count", "timeout_cancelled"}.issubset(
        set(order_frame.columns)
    )
    assert len(order_frame) == 2


def test_collect_fills_from_run_root_writes_artifacts(tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    run_root = tmp_path / "run_root"
    approval_dir = run_root / "samples" / "sample_01" / "approval"
    approval_dir.mkdir(parents=True, exist_ok=True)
    _write_orders_oms_csv(
        approval_dir / "final_orders_oms.csv",
        [
            {
                "sample_id": "sample_01",
                "ticker": "AAPL",
                "direction": "buy",
                "quantity": 1,
                "estimated_price": 100.0,
                "price_limit": "",
            }
        ],
    )

    monkeypatch = pytest.MonkeyPatch()
    monkeypatch.setattr(module.fill_collection, "generate_run_id", lambda **_kwargs: "run_root_run")
    monkeypatch.setenv("ALPACA_API_KEY", "test-api-key")
    monkeypatch.setenv("ALPACA_SECRET_KEY", "test-secret-key")
    try:
        run_dir = module._run_alpaca_fill_collection(
            run_root=run_root,
            orders_oms=None,
            output_dir=tmp_path / "output",
            market="us",
            broker="alpaca",
            timeout_seconds=5.0,
            poll_interval_seconds=0.1,
            notes="run-root",
            force_outside_market_hours=False,
            adapter_factory=_FakeFillCollectionAdapter,
        )
    finally:
        monkeypatch.undo()

    manifest = json.loads((run_dir / "alpaca_fill_manifest.json").read_text(encoding="utf-8"))
    assert manifest["source_type"] == "run_root"
    assert manifest["order_count"] == 1
    assert manifest["submitted_count"] == 1
    assert manifest["filled_count"] == 1
    assert manifest["has_any_filled_orders"] is True


def test_build_parser_includes_collect_fills_campaign_arguments() -> None:
    module = _load_pilot_ops_module()
    parser = module.build_parser()
    campaign_parser = parser._subparsers._group_actions[0].choices["collect-fills-campaign"]
    option_strings = {action.dest for action in campaign_parser._actions}
    assert {
        "bucket_plan_file",
        "output_dir",
        "market",
        "broker",
        "campaign_preset",
        "run_root",
        "source_orders_oms",
        "timeout_seconds",
        "poll_interval_seconds",
        "max_runs",
        "max_seed_notional",
        "max_seed_orders",
        "side_scope",
        "force_outside_market_hours",
        "notes",
    }.issubset(option_strings)


def test_build_parser_defaults_collect_fills_campaign_side_scope() -> None:
    module = _load_pilot_ops_module()
    args = module.build_parser().parse_args(
        [
            "collect-fills-campaign",
            "--bucket-plan-file",
            "C:/tmp/bucket_plan.yaml",
            "--output-dir",
            "C:/tmp/campaign_output",
        ]
    )
    assert args.command == "collect-fills-campaign"
    assert args.side_scope == "buy-only"
    assert args.campaign_preset == "coverage"


def test_build_parser_parses_seed_inventory_preset_and_caps() -> None:
    module = _load_pilot_ops_module()
    args = module.build_parser().parse_args(
        [
            "collect-fills-campaign",
            "--bucket-plan-file",
            "C:/tmp/bucket_plan.yaml",
            "--output-dir",
            "C:/tmp/campaign_output",
            "--campaign-preset",
            "seed-inventory",
            "--max-seed-notional",
            "250",
            "--max-seed-orders",
            "2",
        ]
    )
    assert args.command == "collect-fills-campaign"
    assert args.campaign_preset == "seed-inventory"
    assert args.max_seed_notional == 250.0
    assert args.max_seed_orders == 2


def test_build_parser_parses_reduce_positions_preset() -> None:
    module = _load_pilot_ops_module()
    args = module.build_parser().parse_args(
        [
            "collect-fills-campaign",
            "--bucket-plan-file",
            "C:/tmp/bucket_plan.yaml",
            "--output-dir",
            "C:/tmp/campaign_output",
            "--campaign-preset",
            "reduce-positions",
        ]
    )
    assert args.command == "collect-fills-campaign"
    assert args.campaign_preset == "reduce-positions"


def test_build_parser_parses_minimal_buy_validation_command() -> None:
    module = _load_pilot_ops_module()
    args = module.build_parser().parse_args(
        [
            "collect-fills-minimal-buy",
            "--output-dir",
            "C:/tmp/minimal_buy_output",
            "--timeout-seconds",
            "60",
            "--poll-interval-seconds",
            "1",
        ]
    )
    assert args.command == "collect-fills-minimal-buy"
    assert str(args.output_dir).replace("\\", "/").endswith("C:/tmp/minimal_buy_output")
    assert args.timeout_seconds == 60.0
    assert args.poll_interval_seconds == 1.0


def test_build_parser_includes_inspect_broker_state_arguments() -> None:
    module = _load_pilot_ops_module()
    parser = module.build_parser()
    inspect_parser = parser._subparsers._group_actions[0].choices["inspect-broker-state"]
    option_strings = {action.dest for action in inspect_parser._actions}
    assert {"output_dir", "notes"}.issubset(option_strings)


def test_build_parser_includes_off_hours_prep_arguments() -> None:
    module = _load_pilot_ops_module()
    parser = module.build_parser()
    off_hours_parser = parser._subparsers._group_actions[0].choices["off-hours-prep"]
    option_strings = {action.dest for action in off_hours_parser._actions}
    assert {"output_dir", "bucket_plan_file", "max_seed_notional", "max_seed_orders", "notes"}.issubset(option_strings)


def test_build_parser_defaults_inspect_broker_state_output_dir() -> None:
    module = _load_pilot_ops_module()
    args = module.build_parser().parse_args(["inspect-broker-state"])
    assert args.command == "inspect-broker-state"
    assert Path(args.output_dir) == module.BROKER_STATE_INSPECTION_DIR


def test_build_parser_defaults_off_hours_prep_output_dir() -> None:
    module = _load_pilot_ops_module()
    args = module.build_parser().parse_args(["off-hours-prep"])
    assert args.command == "off-hours-prep"
    assert Path(args.output_dir) == module.OFF_HOURS_PREP_DIR


def test_build_parser_parses_off_hours_prep_minimal_validation_plan_args() -> None:
    module = _load_pilot_ops_module()
    args = module.build_parser().parse_args(
        [
            "off-hours-prep",
            "--bucket-plan-file",
            "C:/tmp/bucket_plan.yaml",
            "--max-seed-notional",
            "1",
            "--max-seed-orders",
            "1",
        ]
    )
    assert args.command == "off-hours-prep"
    assert Path(args.bucket_plan_file) == Path("C:/tmp/bucket_plan.yaml")
    assert args.max_seed_notional == 1.0
    assert args.max_seed_orders == 1


def test_build_parser_includes_generate_fill_collection_batch_arguments() -> None:
    module = _load_pilot_ops_module()
    parser = module.build_parser()
    batch_parser = parser._subparsers._group_actions[0].choices["generate-fill-collection-batch"]
    option_strings = {action.dest for action in batch_parser._actions}
    assert {
        "market_file",
        "output_dir",
        "participation_buckets",
        "orders_per_bucket",
        "side_scope",
        "sample_id",
        "notes",
    }.issubset(option_strings)


def test_parse_participation_bucket_values_accepts_percent_strings() -> None:
    module = _load_pilot_ops_module()
    values = module._parse_participation_bucket_values("0.01%,0.1%,1%,5%")
    assert values == [0.0001, 0.001, 0.01, 0.05]


def test_generate_fill_collection_batch_plan_floors_whole_shares_and_records_actual_participation() -> None:
    module = _load_pilot_ops_module()
    market_frame = pd.DataFrame(
        [
            {
                "ticker": "RKLB",
                "close": 20.0,
                "adv_shares": 19.0,
                "tradable": True,
            }
        ]
    )

    batch_frame, orders_frame, summary = module._generate_fill_collection_batch_plan(
        market_frame=market_frame,
        participation_buckets=[0.125],
        orders_per_bucket=1,
        side_scope="buy-only",
        sample_id="batch_test",
    )

    assert len(batch_frame) == 1
    assert len(orders_frame) == 1
    assert int(batch_frame.loc[0, "quantity"]) == 2
    assert float(batch_frame.loc[0, "actual_participation"]) == pytest.approx(2.0 / 19.0)
    assert batch_frame.loc[0, "target_participation_bucket"]
    assert batch_frame.loc[0, "direction"] == "buy"
    assert int(summary["generated_order_count"]) == 1


def test_generate_fill_collection_batch_plan_respects_side_scope_and_filters_untradable() -> None:
    module = _load_pilot_ops_module()
    market_frame = pd.DataFrame(
        [
            {
                "ticker": "CHEAP",
                "close": 1.0,
                "adv_shares": 1000.0,
                "tradable": False,
            },
            {
                "ticker": "SAFE",
                "close": 10.0,
                "adv_shares": 1000.0,
                "tradable": True,
            },
        ]
    )

    batch_frame, _orders_frame, summary = module._generate_fill_collection_batch_plan(
        market_frame=market_frame,
        participation_buckets=[0.1],
        orders_per_bucket=1,
        side_scope="both",
        sample_id="batch_test",
    )

    assert set(batch_frame["direction"].tolist()) == {"buy", "sell"}
    assert set(batch_frame["ticker"].tolist()) == {"SAFE"}
    assert int(summary["skipped_untradable_count"]) == 1


def test_generate_fill_collection_batch_plan_spreads_tickers_across_buckets_when_available() -> None:
    module = _load_pilot_ops_module()
    market_frame = pd.DataFrame(
        [
            {"ticker": "AAA", "close": 10.0, "adv_shares": 1000.0, "tradable": True},
            {"ticker": "BBB", "close": 11.0, "adv_shares": 1010.0, "tradable": True},
        ]
    )

    batch_frame, _orders_frame, _summary = module._generate_fill_collection_batch_plan(
        market_frame=market_frame,
        participation_buckets=[0.1, 0.2],
        orders_per_bucket=1,
        side_scope="buy-only",
        sample_id="batch_test",
    )

    assert batch_frame.loc[0, "ticker"] == "AAA"
    assert batch_frame.loc[1, "ticker"] == "BBB"


def test_build_parser_parses_fill_collection_batch_constraints() -> None:
    module = _load_pilot_ops_module()
    args = module.build_parser().parse_args(
        [
            "generate-fill-collection-batch",
            "--market-file",
            "C:/tmp/market.csv",
            "--broker-positions-file",
            "C:/tmp/positions.csv",
            "--buying-power",
            "1234.5",
            "--side-scope",
            "sell-only",
        ]
    )
    assert args.command == "generate-fill-collection-batch"
    assert Path(args.broker_positions_file) == Path("C:/tmp/positions.csv")
    assert args.buying_power == 1234.5
    assert args.side_scope == "sell-only"


def test_generate_fill_collection_batch_plan_sell_side_uses_only_broker_positions_and_caps_quantity() -> None:
    module = _load_pilot_ops_module()
    market_frame = pd.DataFrame(
        [
            {"ticker": "AAA", "close": 10.0, "adv_shares": 1000.0, "tradable": True},
            {"ticker": "BBB", "close": 12.0, "adv_shares": 1000.0, "tradable": True},
        ]
    )
    positions_frame = pd.DataFrame(
        [
            {"ticker": "AAA", "shares": 30},
        ]
    )

    batch_frame, _orders_frame, summary = module._generate_fill_collection_batch_plan(
        market_frame=market_frame,
        participation_buckets=[0.05],
        orders_per_bucket=2,
        side_scope="sell-only",
        sample_id="batch_test",
        broker_positions_frame=positions_frame,
    )

    assert set(batch_frame["ticker"].tolist()) == {"AAA"}
    assert set(batch_frame["direction"].tolist()) == {"sell"}
    assert int(batch_frame.loc[0, "quantity"]) == 30
    assert summary["broker_positions_source"] == "provided"


def test_generate_fill_collection_batch_plan_buy_side_respects_buying_power_budget_in_bucket_order() -> None:
    module = _load_pilot_ops_module()
    market_frame = pd.DataFrame(
        [
            {"ticker": "AAA", "close": 10.0, "adv_shares": 1000.0, "tradable": True},
            {"ticker": "BBB", "close": 12.0, "adv_shares": 1000.0, "tradable": True},
        ]
    )

    batch_frame, _orders_frame, summary = module._generate_fill_collection_batch_plan(
        market_frame=market_frame,
        participation_buckets=[0.1, 0.2],
        orders_per_bucket=1,
        side_scope="buy-only",
        sample_id="batch_test",
        buying_power=1500.0,
    )

    assert len(batch_frame) == 1
    assert batch_frame.loc[0, "target_participation_bucket"] == "10%"
    assert float(batch_frame.loc[0, "estimated_notional"]) <= 1500.0
    assert summary["buying_power_budget"] == 1500.0
    assert summary["budget_exhausted"] is True


@pytest.mark.parametrize(
    "tradable, prices, buying_power, expected_ticker",
    [
        ({"SPY", "AAPL"}, {"SPY": 101.25, "AAPL": 201.5}, 1000.0, "SPY"),
        ({"AAPL"}, {"AAPL": 201.5}, 1000.0, "AAPL"),
    ],
)
def test_select_minimal_buy_validation_candidate_prefers_spy_then_fallback_aapl(
    monkeypatch: pytest.MonkeyPatch,
    tradable: set[str],
    prices: dict[str, float],
    buying_power: float,
    expected_ticker: str,
) -> None:
    module = _load_pilot_ops_module()

    class _FakeDataClient:
        def __init__(self, *args, **kwargs) -> None:
            _ = args
            _ = kwargs

        def get_stock_latest_quote(self, request_params):
            ticker = request_params.symbol_or_symbols
            if isinstance(ticker, list):
                ticker = ticker[0]
            ticker = str(ticker).upper()
            price = prices.get(ticker)
            if price is None:
                return {}
            quote = type("_Quote", (), {"ask_price": price})()
            return {ticker: quote}

        def get_stock_latest_trade(self, request_params):
            ticker = request_params.symbol_or_symbols
            if isinstance(ticker, list):
                ticker = ticker[0]
            ticker = str(ticker).upper()
            price = prices.get(ticker)
            if price is None:
                return {}
            trade = type("_Trade", (), {"price": price})()
            return {ticker: trade}

    monkeypatch.setattr(module.fill_collection_campaign, "_collect_tradable_alpaca_symbols", lambda symbols: set(tradable))
    monkeypatch.setattr(module, "StockHistoricalDataClient", _FakeDataClient)

    selection = module._select_minimal_buy_validation_candidate(
        account_payload={"buying_power": buying_power, "cash": buying_power},
    )

    assert selection["ticker"] == expected_ticker
    assert selection["estimated_price"] == prices[expected_ticker]
    assert selection["candidate_audit"]


def test_validate_minimal_buy_validation_precheck_requires_normal_account() -> None:
    module = _load_pilot_ops_module()
    good_account = {"status": "ACTIVE", "buying_power": 1000.0, "cash": 1000.0, "trading_blocked": False, "transfers_blocked": False}
    good_positions = pd.DataFrame(columns=["ticker", "quantity", "market_value"])
    good_open_orders = pd.DataFrame(columns=["order_id", "ticker"])

    module._validate_minimal_buy_validation_precheck(
        account_payload=good_account,
        positions=good_positions,
        open_orders=good_open_orders,
    )

    with pytest.raises(RuntimeError, match="minimal_buy_validation_precheck_failed"):
        module._validate_minimal_buy_validation_precheck(
            account_payload={"status": "ACTIVE", "buying_power": 0.0, "cash": 1000.0, "trading_blocked": False, "transfers_blocked": False},
            positions=good_positions,
            open_orders=good_open_orders,
        )


def test_build_minimal_buy_validation_orders_oms_writes_one_share_market_buy(tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    orders_path = module._build_minimal_buy_validation_orders_oms(
        output_dir=tmp_path,
        ticker="SPY",
        estimated_price=100.25,
    )
    frame = pd.read_csv(orders_path)
    assert len(frame) == 1
    assert frame.loc[0, "ticker"] == "SPY"
    assert frame.loc[0, "direction"] == "buy"
    assert int(frame.loc[0, "quantity"]) == 1
    assert frame.loc[0, "price_limit"] != frame.loc[0, "price_limit"]  # NaN after CSV round-trip
    assert bool(frame.loc[0, "extended_hours"]) is False


def test_build_minimal_buy_validation_payload_and_report_include_before_after_and_timeline(tmp_path: Path) -> None:
    module = _load_pilot_ops_module()
    run_dir = tmp_path / "alpaca_fill_collection_20260326T170000_minimal"
    run_dir.mkdir(parents=True, exist_ok=True)
    account_before = {"status": "ACTIVE", "cash": 1000.0, "buying_power": 2000.0}
    account_after = {"status": "ACTIVE", "cash": 899.75, "buying_power": 1899.75}
    positions_before = pd.DataFrame(columns=["ticker", "quantity", "market_value"])
    positions_after = pd.DataFrame([{"ticker": "SPY", "quantity": 1.0, "market_value": 100.25}])
    open_orders_before = pd.DataFrame(columns=["order_id", "ticker"])
    open_orders_after = pd.DataFrame(columns=["order_id", "ticker"])
    manifest = {
        "submitted_count": 1,
        "filled_count": 1,
        "broker_precheck_summary": {"reason_counts": {}},
    }
    orders_frame = pd.DataFrame(
        [
            {
                "sample_id": "minimal_buy_validation",
                "ticker": "SPY",
                "direction": "buy",
                "requested_qty": 1.0,
                "filled_qty": 1.0,
                "avg_fill_price": 100.25,
                "estimated_price": 100.25,
                "requested_notional": 100.25,
                "filled_notional": 100.25,
                "fill_ratio": 1.0,
                "status": "filled",
                "reject_reason": "",
                "broker_order_id": "alpaca-1",
                "submitted_at_utc": "2026-03-26T15:00:00+00:00",
                "terminal_at_utc": "2026-03-26T15:00:02+00:00",
                "latency_seconds": 2.0,
                "poll_count": 1,
                "timeout_cancelled": False,
                "cancel_requested": False,
                "cancel_acknowledged": False,
                "avg_fill_price_fallback_used": False,
                "status_history": [],
            }
        ]
    )
    events_frame = pd.DataFrame(
        [
            {
                "sample_id": "minimal_buy_validation",
                "ticker": "SPY",
                "broker_order_id": "alpaca-1",
                "event_at_utc": "2026-03-26T15:00:00+00:00",
                "status": "new",
                "filled_qty": 0.0,
                "filled_avg_price": None,
                "reject_reason": None,
                "event_type": "poll",
            },
            {
                "sample_id": "minimal_buy_validation",
                "ticker": "SPY",
                "broker_order_id": "alpaca-1",
                "event_at_utc": "2026-03-26T15:00:02+00:00",
                "status": "filled",
                "filled_qty": 1.0,
                "filled_avg_price": 100.25,
                "reject_reason": None,
                "event_type": "terminal",
            },
        ]
    )
    selection_audit = {
        "ticker": "SPY",
        "estimated_price": 100.25,
        "price_source": "latest_quote",
        "selected_reason": "preferred_spy_tradable_and_within_budget",
        "candidate_audit": [{"ticker": "SPY", "tradable": True, "estimated_price": 100.25, "price_source": "latest_quote", "feasible": True}],
    }
    validation_plan_file = tmp_path / "tomorrow_minimal_validation_plan.yaml"
    validation_plan_file.write_text("campaign_preset: minimal-buy-validation\n", encoding="utf-8")
    slippage_checklist = tmp_path / "slippage_calibration_prep_checklist.md"
    slippage_checklist.write_text("# checklist\n", encoding="utf-8")

    payload = module._build_minimal_buy_validation_payload(
        run_dir=run_dir,
        account_before=account_before,
        account_after=account_after,
        positions_before=positions_before,
        positions_after=positions_after,
        open_orders_before=open_orders_before,
        open_orders_after=open_orders_after,
        manifest=manifest,
        orders_frame=orders_frame,
        events_frame=events_frame,
        selection_audit=selection_audit,
        validation_plan_path=validation_plan_file,
        slippage_prep_checklist_path=slippage_checklist,
    )
    paths = module._write_minimal_buy_validation_artifacts(run_dir=run_dir, payload=payload)

    report_json = json.loads(paths["minimal_buy_validation_report_json"].read_text(encoding="utf-8"))
    report_md = paths["minimal_buy_validation_report_md"].read_text(encoding="utf-8")
    assert report_json["campaign_preset"] == "minimal-buy-validation"
    assert report_json["side_scope"] == "buy-only"
    assert report_json["order_type"] == "market"
    assert report_json["time_in_force"] == "day"
    assert report_json["quantity"] == 1
    assert report_json["submitted_order_count"] == 1
    assert report_json["filled_order_count"] == 1
    assert report_json["account_snapshot_before"]["status"] == "ACTIVE"
    assert report_json["account_snapshot_after"]["status"] == "ACTIVE"
    assert report_json["event_timeline"]
    assert report_json["suitable_for_slippage_calibration"] is True
    assert "minimal_buy_validation_successful" in report_json["recommendation"]
    assert "selected_ticker: SPY" in report_md
    assert "Event Timeline" in report_md
    assert "filled" in report_md


def test_classify_open_orders_marks_locking_and_suggested_cancellations() -> None:
    module = _load_pilot_ops_module()
    open_orders = [
        {
            "order_id": "buy-1",
            "ticker": "AAPL",
            "direction": "buy",
            "order_type": "limit",
            "time_in_force": "day",
            "status": "open",
            "quantity": 10,
            "filled_qty": 0,
            "submitted_at": "2026-03-26T06:00:00+00:00",
        },
        {
            "order_id": "sell-1",
            "ticker": "MSFT",
            "direction": "sell",
            "order_type": "market",
            "time_in_force": "day",
            "status": "open",
            "quantity": 5,
            "filled_qty": 0,
            "submitted_at": "2026-03-26T07:00:00+00:00",
        },
    ]

    audit = module._classify_open_orders(open_orders)

    assert audit["open_orders_count"] == 2
    assert audit["locked_buying_power_count"] == 1
    assert audit["locked_inventory_count"] == 1
    assert audit["stale_open_order_count"] == 2
    assert audit["recommended_cancellations"]
    assert audit["recommended_cancellations"][0]["reason"]


@pytest.mark.parametrize(
    "snapshot, expected_action, expected_route",
    [
        (
            {
                "captured_at_utc": "2026-03-26T12:00:00+00:00",
                "account": {"buying_power": 0.0, "cash": -97157.46, "equity": 95083.91, "account_type": "paper"},
                "positions": pd.DataFrame(
                    [
                        {"ticker": "AAPL", "quantity": 188.0, "market_value": 18800.0, "unrealized_pnl": 123.0},
                        {"ticker": "MSFT", "quantity": 121.0, "market_value": 24200.0, "unrealized_pnl": -42.0},
                    ]
                ),
            },
            "reduce positions",
            "sell_only_campaign",
        ),
        (
            {
                "captured_at_utc": "2026-03-26T12:00:00+00:00",
                "account": {"buying_power": 0.0, "cash": 0.0, "equity": 0.0, "account_type": "paper"},
                "positions": pd.DataFrame(columns=["ticker", "quantity", "market_value", "unrealized_pnl"]),
            },
            "reset account",
            "reset_account",
        ),
        (
            {
                "captured_at_utc": "2026-03-26T12:00:00+00:00",
                "account": {"buying_power": 1000.0, "cash": 1000.0, "equity": 1000.0, "account_type": "paper"},
                "positions": pd.DataFrame(columns=["ticker", "quantity", "market_value", "unrealized_pnl"]),
            },
            "buy-only campaign",
            "buy_only_campaign",
        ),
    ],
)
def test_build_broker_state_inspection_payload_scores_routes(
    snapshot: dict[str, object], expected_action: str, expected_route: str
) -> None:
    module = _load_pilot_ops_module()
    payload = module._build_broker_state_inspection_payload(snapshot, notes="inspection-test")

    assert payload["recommended_next_action"] == expected_action
    assert payload["feasible_routes"][expected_route] is True
    assert payload["broker_state_snapshot"]["positions_count"] == len(payload["positions"])
    assert "positions_summary" in payload
