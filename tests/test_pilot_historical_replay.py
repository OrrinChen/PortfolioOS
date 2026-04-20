from __future__ import annotations

import importlib.util
from datetime import date
from pathlib import Path
import sys


def _load_replay_module():
    script_path = Path(__file__).resolve().parents[1] / "scripts" / "pilot_historical_replay.py"
    spec = importlib.util.spec_from_file_location("pilot_historical_replay_script", script_path)
    assert spec is not None
    assert spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


def test_generate_trading_days_prefers_chinese_calendar(monkeypatch) -> None:
    module = _load_replay_module()
    known_days = {"2026-02-09", "2026-02-10", "2026-02-12", "2026-02-13"}

    monkeypatch.setattr(
        module,
        "_load_chinese_calendar_checker",
        lambda: (lambda day: day.isoformat() in known_days),
    )
    monkeypatch.setattr(
        module,
        "_load_akshare_trade_dates",
        lambda: [date(2026, 2, 11), date(2026, 2, 12)],
    )

    days, source, warnings = module.generate_trading_days("2026-02-09", "2026-02-13", market="cn")

    assert source == "chinese_calendar"
    assert warnings == []
    assert days == ["2026-02-09", "2026-02-10", "2026-02-12", "2026-02-13"]


def test_determine_resume_start_index_from_current_date() -> None:
    module = _load_replay_module()
    trading_days = [f"2026-02-{day:02d}" for day in range(1, 31)]
    progress = {"current_date": trading_days[14], "completed": 15}

    start_index = module.determine_resume_start_index(trading_days, progress)

    assert start_index == 15


def test_run_historical_replay_aborts_after_max_consecutive_failures(tmp_path: Path, monkeypatch) -> None:
    module = _load_replay_module()
    trading_days = [
        "2026-02-09",
        "2026-02-10",
        "2026-02-11",
        "2026-02-12",
        "2026-02-13",
    ]
    monkeypatch.setattr(
        module,
        "generate_trading_days",
        lambda *_args, **_kwargs: (trading_days, "test_calendar", []),
    )

    calls: list[list[str]] = []

    def fake_run_command(command: list[str], *, timeout_seconds: int):
        _ = timeout_seconds
        calls.append(command)
        if "go-nogo" in command:
            return module.CommandExecution(return_code=0, stdout="", stderr="")
        return module.CommandExecution(return_code=1, stdout="", stderr="simulated nightly failure")

    config = module.ReplayConfig(
        start_date="2026-02-09",
        end_date="2026-02-13",
        phase="phase_1",
        market="cn",
        real_sample=True,
        reviewer_input=None,
        config_overlay=None,
        max_failures=3,
        output_dir=tmp_path / "pilot_tracking",
        cool_down=0.0,
        window_trading_days=20,
    )

    rc = module.run_historical_replay(
        config,
        run_command=fake_run_command,
        sleep_fn=lambda _seconds: None,
        logger=lambda _message: None,
    )

    progress_path = config.output_dir / "replay_progress.json"
    progress = module._load_progress(progress_path)
    assert progress is not None

    assert rc == 0
    assert progress["status"] == "aborted"
    assert progress["completed"] == 3
    assert progress["failed"] == 3
    assert progress["succeeded"] == 0
    assert "max_failures=3" in progress["aborted_reason"]
    assert any("go-nogo" in call for call in calls)


def test_render_replay_summary_contains_required_sections(tmp_path: Path) -> None:
    module = _load_replay_module()
    progress = {
        "start_date": "2026-02-09",
        "end_date": "2026-02-11",
        "phase": "phase_1",
        "market": "cn",
        "total_trading_days": 3,
        "completed": 3,
        "succeeded": 2,
        "failed": 1,
        "skipped": 0,
        "status": "completed",
        "started_at": "2026-03-24T19:00:00",
        "last_updated": "2026-03-24T19:10:00",
        "aborted_reason": "",
    }
    day_results = [
        {
            "date": "2026-02-09",
            "status": "success",
            "return_code": 0,
            "stderr": "",
            "duration_seconds": 40.0,
        },
        {
            "date": "2026-02-10",
            "status": "failure",
            "return_code": 1,
            "stderr": "tushare rate limit exceeded on request",
            "duration_seconds": 50.0,
        },
        {
            "date": "2026-02-11",
            "status": "success",
            "return_code": 0,
            "stderr": "",
            "duration_seconds": 45.0,
        },
    ]
    fallback_stats = {
        "available": True,
        "rows_considered": 3,
        "activated_days": 2,
        "activation_rate": 2 / 3,
        "sources": {"akshare": 1, "tencent": 1},
    }
    go_nogo_path = tmp_path / "replay_go_nogo_status.md"
    go_nogo_exec = module.CommandExecution(return_code=0, stdout="", stderr="")

    rendered = module.render_replay_summary(
        progress=progress,
        day_results=day_results,
        go_nogo_path=go_nogo_path,
        go_nogo_execution=go_nogo_exec,
        fallback_stats=fallback_stats,
    )
    summary_path = tmp_path / "replay_summary.md"
    module._save_summary(summary_path, rendered)
    text = summary_path.read_text(encoding="utf-8")

    assert "# Historical Replay Summary" in text
    assert "## Outcome" in text
    assert "succeeded: 2 (66.67%)" in text
    assert "failed: 1 (33.33%)" in text
    assert "2026-02-10: exit=1" in text
    assert "p95_seconds" in text
    assert "source_breakdown: akshare=1, tencent=1" in text
    assert f"report_path: {go_nogo_path}" in text


def test_build_nightly_command_forwards_config_overlay(tmp_path: Path) -> None:
    module = _load_replay_module()
    overlay_path = tmp_path / "overlay.yaml"
    overlay_path.write_text("risk_model:\n  enabled: true\n", encoding="utf-8")
    config = module.ReplayConfig(
        start_date="2026-02-09",
        end_date="2026-02-13",
        phase="phase_1",
        market="cn",
        real_sample=True,
        reviewer_input=None,
        config_overlay=overlay_path,
        max_failures=3,
        output_dir=tmp_path / "pilot_tracking",
        cool_down=0.0,
        window_trading_days=20,
    )

    command = module._build_nightly_command(config, "2026-02-09")

    assert "--config-overlay" in command
    idx = command.index("--config-overlay")
    assert command[idx + 1] == str(overlay_path)


def test_build_nightly_command_forwards_ab_gate_flags(tmp_path: Path) -> None:
    module = _load_replay_module()
    config = module.ReplayConfig(
        start_date="2026-02-09",
        end_date="2026-02-13",
        phase="phase_1",
        market="cn",
        real_sample=True,
        reviewer_input=None,
        config_overlay=None,
        max_failures=3,
        output_dir=tmp_path / "pilot_tracking",
        cool_down=0.0,
        window_trading_days=20,
        ab_flow=True,
        require_eligibility_gate=True,
    )

    command = module._build_nightly_command(config, "2026-02-09")

    assert "--ab-flow" in command
    assert "--require-eligibility-gate" in command


def test_build_weekly_command_forwards_ab_gate_flags(tmp_path: Path) -> None:
    module = _load_replay_module()
    reviewer_input = tmp_path / "reviewer.csv"
    reviewer_input.write_text("sample_id,score\n", encoding="utf-8")
    config = module.ReplayConfig(
        start_date="2026-02-09",
        end_date="2026-02-13",
        phase="phase_1",
        market="cn",
        real_sample=True,
        reviewer_input=reviewer_input,
        config_overlay=None,
        max_failures=3,
        output_dir=tmp_path / "pilot_tracking",
        cool_down=0.0,
        window_trading_days=20,
        ab_flow=True,
        require_eligibility_gate=True,
    )

    command = module._build_weekly_command(config, "2026-02-13", include_as_of_date=False)

    assert "--ab-flow" in command
    assert "--require-eligibility-gate" in command


def test_build_nightly_command_uses_current_python_executable(tmp_path: Path) -> None:
    module = _load_replay_module()
    config = module.ReplayConfig(
        start_date="2026-02-09",
        end_date="2026-02-13",
        phase="phase_1",
        market="us",
        real_sample=False,
        reviewer_input=None,
        config_overlay=None,
        max_failures=3,
        output_dir=tmp_path / "pilot_tracking",
        cool_down=0.0,
        window_trading_days=20,
    )

    command = module._build_nightly_command(config, "2026-02-09")

    assert command[0] == sys.executable
    assert Path(command[1]).name == "pilot_ops.py"
    assert "--as-of-date" in command
    assert command[command.index("--as-of-date") + 1] == "2026-02-09"
