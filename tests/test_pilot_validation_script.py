from __future__ import annotations

import importlib.util
from pathlib import Path
import sys
import yaml

from portfolio_os.domain.errors import ProviderDataError


def _load_pilot_validation_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "run_pilot_validation.py"
    spec = importlib.util.spec_from_file_location("pilot_validation_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_parse_args_supports_mode_and_gate_mode_compatibility() -> None:
    module = _load_pilot_validation_module()

    explicit_release = module._parse_args(["--mode", "release"])
    assert explicit_release.mode == "release"

    compat_release = module._parse_args(["--gate-mode", "strict_gate"])
    assert compat_release.mode == "release"

    compat_nightly = module._parse_args(["--gate-mode", "provisional"])
    assert compat_nightly.mode == "nightly"

    us_mode = module._parse_args(["--market", "us"])
    assert us_mode.market == "us"

    with_overlay = module._parse_args(["--config-overlay", "config/overlay.yaml"])
    assert with_overlay.config_overlay is not None
    assert with_overlay.config_overlay.name == "overlay.yaml"


def test_resolve_real_trade_date_rolls_back_for_non_trading_day(monkeypatch) -> None:
    module = _load_pilot_validation_module()

    class FakeProvider:
        def get_daily_market_snapshot(self, tickers, as_of_date):
            _ = tickers
            if as_of_date == "2026-03-21":
                raise ProviderDataError("Tushare daily returned no rows for trade_date 20260321.")
            return [{"ticker": "600519"}]

    monkeypatch.setattr(module, "get_data_provider", lambda _name: FakeProvider())
    effective_date, attempted_dates, error_text = module._resolve_real_trade_date(
        requested_date="2026-03-21",
        tickers=["600519"],
        max_lookback_days=2,
    )

    assert effective_date == "2026-03-20"
    assert attempted_dates[:2] == ["2026-03-21", "2026-03-20"]
    assert error_text is None


def test_build_approval_request_includes_auto_override_metadata() -> None:
    module = _load_pilot_validation_module()
    payload = module._build_approval_request(
        scenario_output_dir=Path("outputs/demo/scenario"),
        selected_scenario="public_conservative",
        sample_id="sample_01",
        use_override=True,
    )

    assert payload["override_auto_generated"] is True
    assert payload["override"]["enabled"] is True
    assert payload["override"]["override_reason_code"] == "workflow_continuity"


def test_run_command_resolves_portfolio_cli_via_sys_executable(monkeypatch, tmp_path: Path) -> None:
    module = _load_pilot_validation_module()
    captured: dict[str, list[str]] = {}

    class _Done:
        def __init__(self):
            self.returncode = 0
            self.stdout = "ok"
            self.stderr = ""

    def _fake_run(command, **kwargs):
        _ = kwargs
        captured["command"] = list(command)
        return _Done()

    monkeypatch.setattr(module.subprocess, "run", _fake_run)
    result = module._run_command(
        "build_market",
        [
            r"C:\missing\portfolio-os-build-market.exe",
            "--tickers-file",
            "x.txt",
            "--as-of-date",
            "2026-03-04",
            "--provider",
            "mock",
            "--output",
            "market.csv",
        ],
        tmp_path / "cmd.log",
    )

    assert result.ok is True
    assert captured["command"][0] == sys.executable
    assert captured["command"][1] == "-c"
    assert "build_market_app" in captured["command"][2]


def test_build_runtime_config_path_deep_merges_overlay(tmp_path: Path) -> None:
    module = _load_pilot_validation_module()
    base = tmp_path / "base.yaml"
    overlay = tmp_path / "overlay.yaml"
    run_root = tmp_path / "run"
    run_root.mkdir(parents=True, exist_ok=True)
    base.write_text(
        yaml.safe_dump(
            {
                "trading": {"market": "cn"},
                "risk_model": {"enabled": False, "estimator": "ledoit_wolf"},
                "objective_weights": {"tracking_error": 3.0, "risk_term": 1.0},
            },
            sort_keys=False,
            allow_unicode=False,
        ),
        encoding="utf-8",
    )
    overlay.write_text(
        yaml.safe_dump(
            {
                "risk_model": {"enabled": True},
                "objective_weights": {"tracking_error": 0.2},
            },
            sort_keys=False,
            allow_unicode=False,
        ),
        encoding="utf-8",
    )

    merged_path = module._build_runtime_config_path(
        base_config_path=base,
        config_overlay_path=overlay,
        run_root=run_root,
    )
    payload = yaml.safe_load(merged_path.read_text(encoding="utf-8"))

    assert merged_path.exists()
    assert payload["trading"]["market"] == "cn"
    assert payload["risk_model"]["enabled"] is True
    assert payload["risk_model"]["estimator"] == "ledoit_wolf"
    assert payload["objective_weights"]["tracking_error"] == 0.2
    assert payload["objective_weights"]["risk_term"] == 1.0


def test_evaluate_release_gate_fails_when_static_override_ratio_too_high() -> None:
    module = _load_pilot_validation_module()

    reports = []
    for idx in range(1, 6):
        reports.append(
            {
                "sample_id": f"sample_0{idx}",
                "market_builder_success": True,
                "reference_builder_success": True,
                "main_flow_success": True,
                "scenario_success": True,
                "approval_success": True,
                "execution_success": True,
                "cost_difference": 10.0,
                "override_used": idx <= 3,
                "scenario_score_gap": 0.02,
                "order_reasonableness_score": 4,
                "findings_explainability_score": 4,
                "execution_credibility_score": 4,
            }
        )
    reports.append(
        {
            "sample_id": "real_sample_01",
            "market_builder_success": True,
            "reference_builder_success": True,
            "main_flow_success": True,
            "scenario_success": True,
            "approval_success": True,
            "execution_success": True,
            "cost_difference": 1.0,
            "override_used": False,
            "scenario_score_gap": 0.02,
            "order_reasonableness_score": 4,
            "findings_explainability_score": 4,
            "execution_credibility_score": 4,
        }
    )

    options = module.ValidationOptions(
        mode="release",
        reviewer_input=Path("outputs/reviewer.csv"),
        include_real_sample=True,
        real_feed_as_of_date="2026-03-23",
        market="cn",
    )
    passed, reasons, metrics = module._evaluate_release_gate(
        reports=reports,
        options=options,
        reviewer_complete=True,
        real_feed_check={"provider_capability_blockers": []},
    )

    assert passed is False
    assert any("override usage" in reason for reason in reasons)
    assert metrics["full_chain_success_static"] == 5


def test_apply_us_scenario_feasibility_guards_caps_high_cash_buffer(tmp_path: Path) -> None:
    module = _load_pilot_validation_module()
    state_path = tmp_path / "portfolio_state.yaml"
    state_path.write_text(
        yaml.safe_dump(
            {
                "account_id": "us_sample",
                "as_of_date": "2026-03-23",
                "available_cash": 120000.0,
            },
            sort_keys=False,
            allow_unicode=False,
        ),
        encoding="utf-8",
    )
    scenarios = [
        {"id": "public_conservative", "overrides": {}},
        {
            "id": "public_high_cash_buffer",
            "overrides": {"min_cash_buffer": 400000.0, "min_order_notional": 15000.0},
        },
    ]

    adjusted = module._apply_us_scenario_feasibility_guards(
        scenarios,
        portfolio_state_path=state_path,
    )

    original_high_cash = scenarios[1]["overrides"]["min_cash_buffer"]
    adjusted_high_cash = adjusted[1]["overrides"]["min_cash_buffer"]
    assert original_high_cash == 400000.0
    assert adjusted_high_cash == 90000.0


def test_apply_us_scenario_feasibility_guards_no_state_keeps_original() -> None:
    module = _load_pilot_validation_module()
    scenarios = [
        {
            "id": "public_high_cash_buffer",
            "overrides": {"min_cash_buffer": 400000.0},
        }
    ]
    adjusted = module._apply_us_scenario_feasibility_guards(
        scenarios,
        portfolio_state_path=Path("missing_state.yaml"),
    )
    assert adjusted[0]["overrides"]["min_cash_buffer"] == 400000.0
