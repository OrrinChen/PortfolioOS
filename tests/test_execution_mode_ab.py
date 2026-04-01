from __future__ import annotations

import importlib.util
import json
from pathlib import Path
import sys

import pandas as pd
import pytest


def _load_execution_mode_ab_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_execution_mode_ab.py"
    spec = importlib.util.spec_from_file_location("execution_mode_ab_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def _report_payload(*, planned_cost: float, evaluated_cost: float, fill_rate: float = 1.0) -> dict:
    return {
        "cost_comparison": {
            "planned_cost": planned_cost,
            "evaluated_cost": evaluated_cost,
            "planned_slippage": planned_cost,
            "evaluated_slippage": evaluated_cost,
            "planned_fee": 0.0,
            "evaluated_fee": 0.0,
        },
        "bucket_curve": {
            "bucket_count": 2,
            "buckets": [
                {"label": "open", "volume_share": 0.5, "slippage_multiplier": 1.2},
                {"label": "close", "volume_share": 0.5, "slippage_multiplier": 1.0},
            ],
        },
        "resolved_calibration": {
            "resolved_simulation_defaults": {
                "participation_limit": 0.2,
                "volume_shock_multiplier": 1.0,
            }
        },
        "portfolio_summary": {
            "total_ordered_notional": 1000.0,
            "fill_rate": fill_rate,
            "partial_fill_count": 0,
            "unfilled_order_count": 0,
        },
    }


def test_load_execution_ab_manifest_and_date_filter(tmp_path: Path) -> None:
    module = _load_execution_mode_ab_module()
    manifest_path = tmp_path / "manifest.yaml"
    request_path = tmp_path / "request.yaml"
    request_path.write_text("name: demo\nsimulation:\n  mode: impact_aware\n", encoding="utf-8")
    manifest_path.write_text(
        "\n".join(
            [
                "requests:",
                "  - sample_id: sample_us_01",
                f"    request_path: {request_path}",
                '    start_date: "2026-03-10"',
                '    end_date: "2026-03-20"',
            ]
        ),
        encoding="utf-8",
    )

    items = module.load_execution_ab_manifest(manifest_path)

    assert len(items) == 1
    item = items[0]
    assert item.sample_id == "sample_us_01"
    assert item.request_path == request_path.resolve()
    assert module._request_applies_on_date(item, "2026-03-15") is True
    assert module._request_applies_on_date(item, "2026-03-09") is False
    assert module._request_applies_on_date(item, "2026-03-21") is False


def test_build_daily_row_uses_evaluated_cost_for_winner() -> None:
    module = _load_execution_mode_ab_module()
    item = module.ExecutionABRequestItem(sample_id="sample_us_01", request_path=Path("C:/tmp/request.yaml"))
    baseline = {
        "status": "success",
        "output_dir": "C:/tmp/baseline",
        "report": _report_payload(planned_cost=10.0, evaluated_cost=12.0),
    }
    candidate = {
        "status": "success",
        "output_dir": "C:/tmp/candidate",
        "report": _report_payload(planned_cost=15.0, evaluated_cost=11.0),
    }

    row = module._build_daily_row(
        trading_day="2026-03-27",
        item=item,
        baseline=baseline,
        candidate=candidate,
        baseline_mode="participation_twap",
        candidate_mode="impact_aware",
    )

    assert row["eligible"] is True
    assert row["winner"] == "impact_aware"
    assert row["planned_cost_a"] == 10.0
    assert row["planned_cost_b"] == 15.0
    assert row["evaluated_cost_a"] == 12.0
    assert row["evaluated_cost_b"] == 11.0
    assert row["delta_evaluated_cost"] == pytest.approx(1.0)


def test_run_execution_mode_ab_writes_daily_outputs_and_summary(tmp_path: Path) -> None:
    module = _load_execution_mode_ab_module()
    request_path = tmp_path / "request.yaml"
    request_path.write_text("name: demo\nsimulation:\n  mode: impact_aware\n", encoding="utf-8")
    manifest_path = tmp_path / "manifest.yaml"
    manifest_path.write_text(
        "\n".join(
            [
                "requests:",
                "  - sample_id: sample_us_01",
                f"    request_path: {request_path}",
            ]
        ),
        encoding="utf-8",
    )

    def fake_execute_mode(*, request_path: Path, mode: str, output_dir: Path, calibration_profile_path: Path | None = None):
        _ = calibration_profile_path
        output_dir.mkdir(parents=True, exist_ok=True)
        report = _report_payload(
            planned_cost=(10.0 if mode == "participation_twap" else 14.0),
            evaluated_cost=(12.0 if mode == "participation_twap" else 11.5),
        )
        return {
            "status": "success",
            "mode": mode,
            "request_path": str(request_path),
            "output_dir": str(output_dir),
            "report": report,
        }

    def fake_trading_days(start_date: str, end_date: str, *, market: str):
        _ = start_date
        _ = end_date
        _ = market
        return (["2026-03-27"], "weekday_fallback", [])

    config = module.ExecutionABConfig(
        manifest_path=manifest_path,
        start_date="2026-03-27",
        end_date="2026-03-27",
        market="us",
        output_dir=tmp_path / "ab_output",
        baseline_mode="participation_twap",
        candidate_mode="impact_aware",
    )

    paths = module.run_execution_mode_ab(
        config,
        execute_mode=fake_execute_mode,
        trading_day_fn=fake_trading_days,
    )

    daily_csv = pd.read_csv(paths["daily_csv"])
    summary_text = paths["summary_md"].read_text(encoding="utf-8")
    manifest_payload = json.loads(paths["manifest_json"].read_text(encoding="utf-8"))

    assert len(daily_csv) == 1
    assert bool(daily_csv.loc[0, "eligible"]) is True
    assert daily_csv.loc[0, "winner"] == "impact_aware"
    assert "Execution Mode A/B Summary" in summary_text
    assert "impact_aware: 1" in summary_text
    assert manifest_payload["baseline_mode"] == "participation_twap"
    assert manifest_payload["candidate_mode"] == "impact_aware"
