from __future__ import annotations

import argparse
import csv
import json
import math
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any, Callable


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_OUTPUT_DIR = ROOT / "outputs" / "pilot_tracking"
PROGRESS_FILE = "replay_progress.json"
SUMMARY_FILE = "replay_summary.md"
GO_NOGO_FILE = "replay_go_nogo_status.md"
DASHBOARD_FILE = "pilot_dashboard.csv"

NIGHTLY_TIMEOUT_SECONDS = 600
WEEKLY_TIMEOUT_SECONDS = 600
GO_NOGO_TIMEOUT_SECONDS = 180

LOCK_RETRY_COUNT = 3
LOCK_RETRY_WAIT_SECONDS = 5.0
LOCK_ERROR_MARKERS = (
    "permissionerror",
    "winerror 32",
    "being used by another process",
)


@dataclass
class ReplayConfig:
    start_date: str
    end_date: str
    phase: str
    market: str
    real_sample: bool
    reviewer_input: Path | None
    config_overlay: Path | None
    max_failures: int
    output_dir: Path
    cool_down: float
    window_trading_days: int
    ab_flow: bool = False
    require_eligibility_gate: bool = False


@dataclass
class CommandExecution:
    return_code: int | None
    stdout: str
    stderr: str
    timed_out: bool = False
    exception_type: str | None = None


def _now_timestamp() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def _now_iso() -> str:
    return datetime.now().isoformat(timespec="seconds")


def _log(message: str) -> None:
    print(f"[{_now_timestamp()}] {message}")


def _parse_iso_date(raw: str) -> date:
    return datetime.strptime(str(raw), "%Y-%m-%d").date()


def _coerce_to_date(raw: Any) -> date | None:
    if raw is None:
        return None
    if isinstance(raw, date):
        return raw
    text = str(raw).strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
        try:
            return datetime.strptime(text, fmt).date()
        except ValueError:
            continue
    return None


def _date_range(start: date, end: date) -> list[date]:
    current = start
    values: list[date] = []
    while current <= end:
        values.append(current)
        current += timedelta(days=1)
    return values


def _load_chinese_calendar_checker() -> Callable[[date], bool] | None:
    try:
        from chinese_calendar import is_workday
    except Exception:
        return None
    return is_workday


def _load_akshare_trade_dates() -> list[date] | None:
    try:
        import akshare as ak
    except Exception:
        return None
    try:
        frame = ak.tool_trade_date_hist_sina()
    except Exception:
        return None
    if frame is None or getattr(frame, "empty", True):
        return []
    columns = list(getattr(frame, "columns", []))
    if not columns:
        return []
    chosen_column = "trade_date" if "trade_date" in columns else columns[0]
    values: list[date] = []
    for raw in frame[chosen_column].tolist():
        parsed = _coerce_to_date(raw)
        if parsed is not None:
            values.append(parsed)
    return sorted(set(values))


def generate_trading_days(
    start_date: str,
    end_date: str,
    *,
    market: str = "cn",
) -> tuple[list[str], str, list[str]]:
    start = _parse_iso_date(start_date)
    end = _parse_iso_date(end_date)
    if end < start:
        raise ValueError("--end-date must be on or after --start-date.")

    warnings: list[str] = []
    if str(market).strip().lower() != "cn":
        warnings.append(f"market={market} not explicitly supported; using weekday-only fallback calendar.")
        values = [d.isoformat() for d in _date_range(start, end) if d.weekday() < 5]
        return values, "weekday_fallback", warnings

    checker = _load_chinese_calendar_checker()
    if checker is not None:
        # Chinese stock market runs only on weekdays; keep weekday guard even when workday data exists.
        values = [d.isoformat() for d in _date_range(start, end) if d.weekday() < 5 and bool(checker(d))]
        if values:
            return values, "chinese_calendar", warnings
        warnings.append("chinese_calendar returned no trading days; trying AKShare calendar.")

    akshare_dates = _load_akshare_trade_dates()
    if akshare_dates is not None and akshare_dates:
        values = [d.isoformat() for d in akshare_dates if start <= d <= end]
        if values:
            return values, "akshare_trade_calendar", warnings
        warnings.append("AKShare trade calendar returned no rows in requested range; using weekday fallback.")

    if checker is None:
        warnings.append("chinese_calendar unavailable; using fallback sources.")
    if akshare_dates is None:
        warnings.append("AKShare trade calendar unavailable; using weekday-only fallback.")
    values = [d.isoformat() for d in _date_range(start, end) if d.weekday() < 5]
    return values, "weekday_fallback", warnings


def determine_resume_start_index(trading_days: list[str], progress: dict[str, Any] | None) -> int:
    if not progress:
        return 0
    current_date = str(progress.get("current_date", "")).strip()
    if current_date and current_date in trading_days:
        return trading_days.index(current_date) + 1
    completed = int(progress.get("completed", 0) or 0)
    return max(0, min(len(trading_days), completed))


def _normalize_output_path(path: Path) -> Path:
    if path.is_absolute():
        return path
    return (ROOT / path).resolve()


def _default_progress(config: ReplayConfig, total_days: int) -> dict[str, Any]:
    now = _now_iso()
    return {
        "start_date": config.start_date,
        "end_date": config.end_date,
        "phase": config.phase,
        "market": config.market,
        "real_sample": config.real_sample,
        "config_overlay": str(config.config_overlay) if config.config_overlay is not None else "",
        "ab_flow": bool(config.ab_flow),
        "require_eligibility_gate": bool(config.require_eligibility_gate),
        "max_failures": config.max_failures,
        "total_trading_days": total_days,
        "completed": 0,
        "succeeded": 0,
        "failed": 0,
        "skipped": 0,
        "current_date": "",
        "consecutive_failures": 0,
        "status": "running",
        "aborted_reason": "",
        "started_at": now,
        "last_updated": now,
        "daily_results": [],
    }


def _progress_path(output_dir: Path) -> Path:
    return output_dir / PROGRESS_FILE


def _load_progress(path: Path) -> dict[str, Any] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8"))


def _save_progress(path: Path, progress: dict[str, Any]) -> None:
    progress["last_updated"] = _now_iso()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(progress, indent=2, ensure_ascii=False), encoding="utf-8")


def _progress_matches(config: ReplayConfig, progress: dict[str, Any], total_days: int) -> bool:
    stored_overlay = str(progress.get("config_overlay", "") or "").strip()
    expected_overlay = str(config.config_overlay) if config.config_overlay is not None else ""
    return (
        str(progress.get("start_date")) == config.start_date
        and str(progress.get("end_date")) == config.end_date
        and str(progress.get("phase")) == config.phase
        and str(progress.get("market")) == config.market
        and bool(progress.get("real_sample")) == config.real_sample
        and stored_overlay == expected_overlay
        and bool(progress.get("ab_flow", False)) == bool(config.ab_flow)
        and bool(progress.get("require_eligibility_gate", False)) == bool(config.require_eligibility_gate)
        and int(progress.get("total_trading_days", -1)) == total_days
    )


def _truncate(text: str, limit: int = 500) -> str:
    compact = " ".join(str(text or "").split())
    return compact[:limit]


def _run_command(
    command: list[str],
    *,
    timeout_seconds: int,
    runner: Callable[..., subprocess.CompletedProcess[str]] = subprocess.run,
) -> CommandExecution:
    try:
        completed = runner(
            command,
            cwd=ROOT,
            text=True,
            capture_output=True,
            check=False,
            timeout=timeout_seconds,
        )
        return CommandExecution(
            return_code=int(completed.returncode),
            stdout=str(completed.stdout or ""),
            stderr=str(completed.stderr or ""),
        )
    except subprocess.TimeoutExpired as exc:
        stdout = "" if exc.stdout is None else str(exc.stdout)
        stderr = f"timeout after {timeout_seconds}s"
        return CommandExecution(
            return_code=-1,
            stdout=stdout,
            stderr=stderr,
            timed_out=True,
            exception_type="TimeoutExpired",
        )
    except Exception as exc:  # pragma: no cover - defensive path for unexpected environments
        return CommandExecution(
            return_code=-1,
            stdout="",
            stderr=str(exc),
            timed_out=False,
            exception_type=type(exc).__name__,
        )


def _is_dashboard_lock_error(execution: CommandExecution, dashboard_path: Path) -> bool:
    dashboard_name = dashboard_path.name.lower()
    combined = f"{execution.stdout}\n{execution.stderr}".lower()
    has_lock_marker = any(marker in combined for marker in LOCK_ERROR_MARKERS)
    mentions_dashboard = dashboard_name in combined or "pilot_dashboard.csv" in combined
    if execution.exception_type == "PermissionError":
        return True
    return has_lock_marker and mentions_dashboard


def _build_nightly_command(config: ReplayConfig, trading_day: str) -> list[str]:
    command = [
        sys.executable,
        str(ROOT / "scripts" / "pilot_ops.py"),
        "nightly",
        "--phase",
        config.phase,
        "--market",
        config.market,
        "--output-dir",
        str(config.output_dir),
        "--notes",
        f"historical_replay_{trading_day}",
        "--as-of-date",
        trading_day,
    ]
    if config.config_overlay is not None:
        command.extend(["--config-overlay", str(config.config_overlay)])
    if config.ab_flow:
        command.append("--ab-flow")
    if config.require_eligibility_gate:
        command.append("--require-eligibility-gate")
    if config.real_sample:
        command.append("--real-sample")
    return command


def _build_weekly_command(config: ReplayConfig, trading_day: str, *, include_as_of_date: bool) -> list[str]:
    if config.reviewer_input is None:
        raise ValueError("reviewer_input is required for weekly command.")
    command = [
        sys.executable,
        str(ROOT / "scripts" / "pilot_ops.py"),
        "weekly",
        "--phase",
        config.phase,
        "--market",
        config.market,
        "--reviewer-input",
        str(config.reviewer_input),
        "--output-dir",
        str(config.output_dir),
        "--notes",
        f"historical_replay_weekly_{trading_day}",
    ]
    if config.config_overlay is not None:
        command.extend(["--config-overlay", str(config.config_overlay)])
    if config.ab_flow:
        command.append("--ab-flow")
    if config.require_eligibility_gate:
        command.append("--require-eligibility-gate")
    if config.real_sample:
        command.append("--real-sample")
    if include_as_of_date:
        command.extend(["--as-of-date", trading_day])
    return command


def _build_go_nogo_command(config: ReplayConfig, output_path: Path) -> list[str]:
    return [
        sys.executable,
        str(ROOT / "scripts" / "pilot_ops.py"),
        "go-nogo",
        "--output-dir",
        str(config.output_dir),
        "--window-trading-days",
        str(config.window_trading_days),
        "--output",
        str(output_path),
    ]


def _detect_weekly_as_of_support(run_command: Callable[..., CommandExecution]) -> bool:
    probe = run_command(
        [sys.executable, str(ROOT / "scripts" / "pilot_ops.py"), "weekly", "--help"],
        timeout_seconds=60,
    )
    text = f"{probe.stdout}\n{probe.stderr}"
    return "--as-of-date" in text


def _is_unrecognized_as_of(execution: CommandExecution) -> bool:
    text = f"{execution.stdout}\n{execution.stderr}".lower()
    return "unrecognized arguments: --as-of-date" in text


def _upsert_day_result(day_results: list[dict[str, Any]], payload: dict[str, Any]) -> None:
    target_date = str(payload.get("date", ""))
    for index, existing in enumerate(day_results):
        if str(existing.get("date", "")) == target_date:
            day_results[index] = payload
            return
    day_results.append(payload)


def _compute_duration_stats(day_results: list[dict[str, Any]]) -> dict[str, float] | None:
    durations = [
        float(item.get("duration_seconds", 0.0))
        for item in day_results
        if float(item.get("duration_seconds", 0.0)) > 0.0
    ]
    if not durations:
        return None
    durations.sort()
    p95_index = max(0, math.ceil(0.95 * len(durations)) - 1)
    return {
        "min": durations[0],
        "max": durations[-1],
        "avg": sum(durations) / len(durations),
        "p95": durations[p95_index],
    }


def _is_windows_lock_os_error(exc: OSError) -> bool:
    return int(getattr(exc, "winerror", 0) or 0) == 32


def _load_dashboard_rows_with_retry(
    dashboard_path: Path,
    *,
    retries: int,
    wait_seconds: float,
    sleep_fn: Callable[[float], None],
    logger: Callable[[str], None],
) -> list[dict[str, str]]:
    if not dashboard_path.exists():
        return []
    for attempt in range(retries + 1):
        try:
            with dashboard_path.open("r", encoding="utf-8-sig", newline="") as handle:
                return list(csv.DictReader(handle))
        except PermissionError:
            if attempt >= retries:
                raise
            logger(
                f"Warning: {dashboard_path.name} appears locked (retry {attempt + 1}/{retries}, wait {wait_seconds:.0f}s)."
            )
            sleep_fn(wait_seconds)
        except OSError as exc:
            if not _is_windows_lock_os_error(exc):
                raise
            if attempt >= retries:
                raise
            logger(
                f"Warning: {dashboard_path.name} lock detected (retry {attempt + 1}/{retries}, wait {wait_seconds:.0f}s)."
            )
            sleep_fn(wait_seconds)
    return []


def aggregate_fallback_stats(
    dashboard_path: Path,
    *,
    replay_dates: set[str],
    sleep_fn: Callable[[float], None],
    logger: Callable[[str], None],
) -> dict[str, Any]:
    if not dashboard_path.exists():
        return {"available": False, "message": "pilot_dashboard.csv not found"}
    try:
        rows = _load_dashboard_rows_with_retry(
            dashboard_path,
            retries=LOCK_RETRY_COUNT,
            wait_seconds=LOCK_RETRY_WAIT_SECONDS,
            sleep_fn=sleep_fn,
            logger=logger,
        )
    except PermissionError:
        return {"available": False, "message": "pilot_dashboard.csv locked; fallback stats unavailable"}
    except OSError:
        return {"available": False, "message": "pilot_dashboard.csv could not be read; fallback stats unavailable"}
    if not rows:
        return {"available": False, "message": "pilot_dashboard.csv has no rows"}
    if "fallback_activated" not in rows[0]:
        return {"available": False, "message": "fallback_activated column not present"}

    relevant: list[dict[str, str]] = []
    for row in rows:
        row_date = str(row.get("as_of_date") or row.get("date") or "").strip()
        if row_date in replay_dates and str(row.get("notes", "")).strip().startswith("historical_replay_"):
            relevant.append(row)
    if not relevant:
        for row in rows:
            row_date = str(row.get("as_of_date") or row.get("date") or "").strip()
            if row_date in replay_dates:
                relevant.append(row)

    source_counter: dict[str, int] = {}
    activated_days = 0
    for row in relevant:
        raw = str(row.get("fallback_activated", "")).strip()
        if not raw:
            continue
        activated_days += 1
        for source in [item.strip() for item in raw.split("|") if item.strip()]:
            source_counter[source] = source_counter.get(source, 0) + 1

    return {
        "available": True,
        "rows_considered": len(relevant),
        "activated_days": activated_days,
        "activation_rate": (activated_days / len(relevant)) if relevant else 0.0,
        "sources": source_counter,
    }


def render_replay_summary(
    *,
    progress: dict[str, Any],
    day_results: list[dict[str, Any]],
    go_nogo_path: Path,
    go_nogo_execution: CommandExecution | None,
    fallback_stats: dict[str, Any],
) -> str:
    total = int(progress.get("total_trading_days", 0) or 0)
    succeeded = int(progress.get("succeeded", 0) or 0)
    failed = int(progress.get("failed", 0) or 0)
    skipped = int(progress.get("skipped", 0) or 0)
    completed = int(progress.get("completed", 0) or 0)

    success_ratio = (succeeded / total) if total else 0.0
    fail_ratio = (failed / total) if total else 0.0
    skipped_ratio = (skipped / total) if total else 0.0

    failures = [item for item in day_results if str(item.get("status")) == "failure"]
    timing = _compute_duration_stats(day_results)

    lines = [
        "# Historical Replay Summary",
        "",
        "## Replay Scope",
        f"- start_date: {progress.get('start_date', '')}",
        f"- end_date: {progress.get('end_date', '')}",
        f"- phase: {progress.get('phase', '')}",
        f"- market: {progress.get('market', '')}",
        f"- total_trading_days: {total}",
        f"- status: {progress.get('status', '')}",
        f"- started_at: {progress.get('started_at', '')}",
        f"- last_updated: {progress.get('last_updated', '')}",
        "",
        "## Outcome",
        f"- completed_days: {completed}/{total}",
        f"- succeeded: {succeeded} ({success_ratio:.2%})",
        f"- failed: {failed} ({fail_ratio:.2%})",
        f"- skipped: {skipped} ({skipped_ratio:.2%})",
    ]
    aborted_reason = str(progress.get("aborted_reason", "")).strip()
    if aborted_reason:
        lines.append(f"- aborted_reason: {aborted_reason}")

    lines.extend(["", "## Failures"])
    if not failures:
        lines.append("- none")
    else:
        for item in failures:
            lines.append(
                "- "
                + f"{item.get('date')}: exit={item.get('return_code')}, stderr={_truncate(str(item.get('stderr', '')), 500)}"
            )

    lines.extend(["", "## Timing"])
    if timing is None:
        lines.append("- N/A")
    else:
        lines.extend(
            [
                f"- min_seconds: {timing['min']:.2f}",
                f"- max_seconds: {timing['max']:.2f}",
                f"- avg_seconds: {timing['avg']:.2f}",
                f"- p95_seconds: {timing['p95']:.2f}",
            ]
        )

    lines.extend(["", "## Fallback Stats"])
    if not fallback_stats.get("available", False):
        lines.append(f"- N/A ({fallback_stats.get('message', 'unavailable')})")
    else:
        lines.append(f"- rows_considered: {int(fallback_stats.get('rows_considered', 0) or 0)}")
        lines.append(f"- activated_days: {int(fallback_stats.get('activated_days', 0) or 0)}")
        lines.append(f"- activation_rate: {float(fallback_stats.get('activation_rate', 0.0)):.2%}")
        source_counter = dict(fallback_stats.get("sources", {}))
        if not source_counter:
            lines.append("- source_breakdown: none")
        else:
            pairs = [f"{name}={count}" for name, count in sorted(source_counter.items())]
            lines.append(f"- source_breakdown: {', '.join(pairs)}")

    lines.extend(["", "## Go/No-Go"])
    lines.append(f"- report_path: {go_nogo_path}")
    if go_nogo_execution is None:
        lines.append("- command_status: not_run")
    elif (-1 if go_nogo_execution.return_code is None else int(go_nogo_execution.return_code)) == 0:
        lines.append("- command_status: success")
    else:
        lines.append(f"- command_status: failed (exit={go_nogo_execution.return_code})")
        lines.append(f"- stderr: {_truncate(go_nogo_execution.stderr, 500)}")

    lines.append("")
    return "\n".join(lines)


def _save_summary(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def run_historical_replay(
    config: ReplayConfig,
    *,
    run_command: Callable[..., CommandExecution] = _run_command,
    sleep_fn: Callable[[float], None] = time.sleep,
    logger: Callable[[str], None] = _log,
) -> int:
    config.output_dir.mkdir(parents=True, exist_ok=True)
    progress_path = _progress_path(config.output_dir)
    dashboard_path = config.output_dir / DASHBOARD_FILE
    go_nogo_path = config.output_dir / GO_NOGO_FILE
    summary_path = config.output_dir / SUMMARY_FILE

    trading_days, calendar_source, warnings = generate_trading_days(
        config.start_date,
        config.end_date,
        market=config.market,
    )
    total_days = len(trading_days)
    if total_days == 0:
        logger("No trading days found in the requested range; nothing to replay.")
        return 1

    for warning in warnings:
        logger(f"Warning: {warning}")
    logger(
        f"Replay started: {config.start_date} to {config.end_date} ({total_days} trading days)"
    )
    logger(f"Trading calendar source: {calendar_source}")
    logger(f"Trading days: {', '.join(trading_days)}")

    loaded_progress = _load_progress(progress_path)
    if loaded_progress and _progress_matches(config, loaded_progress, total_days):
        progress = loaded_progress
    else:
        if loaded_progress:
            logger("Progress file exists but does not match current parameters; starting a fresh replay state.")
        progress = _default_progress(config, total_days)

    progress["status"] = "running"
    progress.setdefault("daily_results", [])
    progress.setdefault("started_at", _now_iso())
    progress.setdefault("aborted_reason", "")
    progress["total_trading_days"] = total_days
    _save_progress(progress_path, progress)

    start_index = determine_resume_start_index(trading_days, progress)
    if start_index > 0 and start_index < total_days:
        logger(
            f"Resuming from {trading_days[start_index]}, {progress.get('completed', 0)}/{total_days} already done"
        )

    if config.reviewer_input is not None:
        weekly_as_of_supported = _detect_weekly_as_of_support(run_command)
        if not weekly_as_of_supported:
            logger("weekly command does not support --as-of-date; weekly checkpoints will run without this flag.")
    else:
        weekly_as_of_supported = False

    consecutive_failures = int(progress.get("consecutive_failures", 0) or 0)

    try:
        for index in range(start_index, total_days):
            trading_day = trading_days[index]
            day_label = f"Day {index + 1:>2}/{total_days}: {trading_day}"
            day_start = time.perf_counter()

            nightly_command = _build_nightly_command(config, trading_day)
            execution: CommandExecution | None = None
            status = "failure"
            lock_retries_used = 0
            for attempt in range(LOCK_RETRY_COUNT + 1):
                execution = run_command(nightly_command, timeout_seconds=NIGHTLY_TIMEOUT_SECONDS)
                rc = -1 if execution.return_code is None else int(execution.return_code)
                if rc == 0:
                    status = "success"
                    break
                if not _is_dashboard_lock_error(execution, dashboard_path):
                    status = "failure"
                    break
                if attempt >= LOCK_RETRY_COUNT:
                    status = "skipped"
                    break
                lock_retries_used += 1
                logger(
                    f"Warning: dashboard lock detected for {trading_day}, retry {attempt + 1}/{LOCK_RETRY_COUNT} after {LOCK_RETRY_WAIT_SECONDS:.0f}s."
                )
                sleep_fn(LOCK_RETRY_WAIT_SECONDS)

            if execution is None:
                execution = CommandExecution(
                    return_code=-1,
                    stdout="",
                    stderr="unexpected empty execution result",
                )

            elapsed = time.perf_counter() - day_start
            result_record = {
                "date": trading_day,
                "status": status,
                "return_code": (-1 if execution.return_code is None else int(execution.return_code)),
                "stdout": execution.stdout,
                "stderr": execution.stderr,
                "duration_seconds": round(elapsed, 3),
                "lock_retries_used": lock_retries_used,
                "timed_out": bool(execution.timed_out),
            }
            _upsert_day_result(progress["daily_results"], result_record)

            progress["completed"] = int(progress.get("completed", 0) or 0) + 1
            progress["current_date"] = trading_day
            if status == "success":
                progress["succeeded"] = int(progress.get("succeeded", 0) or 0) + 1
                consecutive_failures = 0
                logger(f"{day_label} ... SUCCESS ({elapsed:.0f}s)")
            elif status == "skipped":
                progress["skipped"] = int(progress.get("skipped", 0) or 0) + 1
                consecutive_failures = 0
                logger(f"{day_label} ... SKIPPED (dashboard lock after retries)")
            else:
                progress["failed"] = int(progress.get("failed", 0) or 0) + 1
                consecutive_failures += 1
                logger(
                    f"{day_label} ... FAILED (exit={execution.return_code}, {_truncate(execution.stderr, 120)})"
                )
            progress["consecutive_failures"] = consecutive_failures
            _save_progress(progress_path, progress)

            if config.reviewer_input is not None and (index + 1) % 5 == 0:
                weekly_command = _build_weekly_command(
                    config,
                    trading_day,
                    include_as_of_date=weekly_as_of_supported,
                )
                weekly_result = run_command(weekly_command, timeout_seconds=WEEKLY_TIMEOUT_SECONDS)
                if (
                    weekly_as_of_supported
                    and (-1 if weekly_result.return_code is None else int(weekly_result.return_code)) != 0
                    and _is_unrecognized_as_of(weekly_result)
                ):
                    logger("weekly command rejected --as-of-date; retrying without --as-of-date.")
                    weekly_as_of_supported = False
                    weekly_command = _build_weekly_command(
                        config,
                        trading_day,
                        include_as_of_date=False,
                    )
                    weekly_result = run_command(weekly_command, timeout_seconds=WEEKLY_TIMEOUT_SECONDS)
                weekly_rc = -1 if weekly_result.return_code is None else int(weekly_result.return_code)
                if weekly_rc == 0:
                    logger(f"Weekly check completed at {trading_day}.")
                else:
                    logger(
                        f"Warning: weekly check failed at {trading_day} (exit={weekly_result.return_code}, {_truncate(weekly_result.stderr, 160)})."
                    )

            if status == "failure" and consecutive_failures >= config.max_failures:
                progress["status"] = "aborted"
                progress["aborted_reason"] = (
                    f"Consecutive failures reached max_failures={config.max_failures} at {trading_day}"
                )
                _save_progress(progress_path, progress)
                logger(progress["aborted_reason"])
                break

            if config.cool_down > 0 and index < total_days - 1:
                sleep_fn(config.cool_down)
    except KeyboardInterrupt:
        progress["status"] = "interrupted"
        _save_progress(progress_path, progress)
        logger("Replay interrupted by user (Ctrl+C). You can rerun to resume from the next trading day.")
        return 130

    if str(progress.get("status", "")) != "aborted":
        progress["status"] = "completed"
        _save_progress(progress_path, progress)

    logger(
        f"Replay complete: {progress.get('succeeded', 0)}/{progress.get('total_trading_days', 0)} succeeded, {progress.get('failed', 0)} failed"
    )

    go_nogo_execution = run_command(
        _build_go_nogo_command(config, go_nogo_path),
        timeout_seconds=GO_NOGO_TIMEOUT_SECONDS,
    )
    go_nogo_rc = -1 if go_nogo_execution.return_code is None else int(go_nogo_execution.return_code)
    if go_nogo_rc == 0:
        logger(f"go-nogo report: {go_nogo_path}")
    else:
        logger(
            f"Warning: go-nogo generation failed (exit={go_nogo_execution.return_code}, {_truncate(go_nogo_execution.stderr, 160)})."
        )

    replay_dates = {str(item.get("date", "")).strip() for item in progress.get("daily_results", [])}
    fallback_stats = aggregate_fallback_stats(
        dashboard_path,
        replay_dates=replay_dates,
        sleep_fn=sleep_fn,
        logger=logger,
    )
    summary_content = render_replay_summary(
        progress=progress,
        day_results=list(progress.get("daily_results", [])),
        go_nogo_path=go_nogo_path,
        go_nogo_execution=go_nogo_execution,
        fallback_stats=fallback_stats,
    )
    _save_summary(summary_path, summary_content)
    logger(f"Summary report: {summary_path}")

    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Batch historical replay runner for pilot nightly operations."
    )
    parser.add_argument("--start-date", required=True, help="Replay start date (YYYY-MM-DD).")
    parser.add_argument("--end-date", required=True, help="Replay end date (YYYY-MM-DD).")
    parser.add_argument("--phase", default="phase_1", help="Pilot phase label.")
    parser.add_argument("--market", default="cn", help="Market code (default: cn).")
    parser.add_argument(
        "--real-sample",
        action="store_true",
        help="Use real feed sample and forward --as-of-date to nightly runs.",
    )
    parser.add_argument(
        "--reviewer-input",
        type=Path,
        default=None,
        help="Optional reviewer CSV path; when provided, runs weekly check every 5 trading days.",
    )
    parser.add_argument(
        "--config-overlay",
        type=Path,
        default=None,
        help="Optional YAML config overlay passed through to pilot_ops nightly/weekly.",
    )
    parser.add_argument(
        "--max-failures",
        type=int,
        default=3,
        help="Abort replay after this many consecutive nightly failures.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Output directory for tracking files.",
    )
    parser.add_argument(
        "--cool-down",
        type=float,
        default=2.0,
        help="Cooldown seconds between trading days.",
    )
    parser.add_argument(
        "--window-trading-days",
        type=int,
        default=20,
        help="Rolling window size used by final go-nogo command.",
    )
    parser.add_argument(
        "--ab-flow",
        action="store_true",
        help="Mark replay commands as A/B eligibility-aware flow and pass through to pilot_ops.",
    )
    parser.add_argument(
        "--require-eligibility-gate",
        action="store_true",
        help="Require eligibility gate in pilot_ops when running A/B flow.",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)

    try:
        _parse_iso_date(str(args.start_date))
        _parse_iso_date(str(args.end_date))
    except ValueError as exc:
        raise ValueError("Invalid date format. Use YYYY-MM-DD.") from exc

    if int(args.max_failures) < 1:
        raise ValueError("--max-failures must be >= 1.")
    if float(args.cool_down) < 0:
        raise ValueError("--cool-down must be >= 0.")
    if int(args.window_trading_days) < 1:
        raise ValueError("--window-trading-days must be >= 1.")

    reviewer_input = None
    if args.reviewer_input is not None:
        reviewer_input = _normalize_output_path(Path(args.reviewer_input))
    config_overlay = None
    if args.config_overlay is not None:
        config_overlay = _normalize_output_path(Path(args.config_overlay))
    output_dir = _normalize_output_path(Path(args.output_dir))

    config = ReplayConfig(
        start_date=str(args.start_date),
        end_date=str(args.end_date),
        phase=str(args.phase),
        market=str(args.market),
        real_sample=bool(args.real_sample),
        reviewer_input=reviewer_input,
        config_overlay=config_overlay,
        max_failures=int(args.max_failures),
        output_dir=output_dir,
        cool_down=float(args.cool_down),
        window_trading_days=int(args.window_trading_days),
        ab_flow=bool(args.ab_flow),
        require_eligibility_gate=bool(args.require_eligibility_gate),
    )
    return run_historical_replay(config)


if __name__ == "__main__":
    raise SystemExit(main())
