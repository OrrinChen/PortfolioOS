from __future__ import annotations

import json
from pathlib import Path

from portfolio_os.observability import TraceEvent, TraceWriter, summarize_trace_events


FORBIDDEN_TRACE_TERMS = {
    "api_key",
    "broker_output",
    "live_performance",
    "order",
    "password",
    "secret",
    "token",
    "trading_instruction",
}


def test_trace_writer_emits_deterministic_jsonl(tmp_path: Path) -> None:
    trace_path = tmp_path / "trace.jsonl"
    writer = TraceWriter(trace_path)

    writer.write_event(
        TraceEvent.create(
            event="bundle_loaded",
            ts="2026-05-03T00:00:00+00:00",
            payload={"bundle_id": "bundle-1", "path": "examples/bundle.yaml"},
        )
    )
    writer.write_event(
        TraceEvent.create(
            event="schema_validated",
            ts="2026-05-03T00:00:01+00:00",
            payload={"schema": "evidence_bundle", "passed": True},
        )
    )

    lines = trace_path.read_text(encoding="utf-8").splitlines()

    assert lines == [
        '{"event":"bundle_loaded","payload":{"bundle_id":"bundle-1","path":"examples/bundle.yaml"},"ts":"2026-05-03T00:00:00+00:00"}',
        '{"event":"schema_validated","payload":{"passed":true,"schema":"evidence_bundle"},"ts":"2026-05-03T00:00:01+00:00"}',
    ]


def test_trace_payload_sanitizes_sensitive_and_trading_output_terms() -> None:
    event = TraceEvent.create(
        event="bundle_loaded",
        ts="2026-05-03T00:00:00+00:00",
        payload={
            "bundle_id": "bundle-1",
            "api_key": "secret-value",
            "broker_output": {"status": "filled"},
            "orders": [{"symbol": "AAPL", "side": "buy"}],
            "safe_note": "local fixture only",
            "token": "hidden-token",
            "trading_instruction": "buy AAPL",
        },
    )

    raw = event.model_dump_json().lower()

    assert event.payload == {"bundle_id": "bundle-1", "safe_note": "local fixture only"}
    assert "secret-value" not in raw
    assert "hidden-token" not in raw
    assert "buy aapl" not in raw
    assert all(term not in raw for term in FORBIDDEN_TRACE_TERMS)


def test_trace_metrics_count_event_names() -> None:
    events = [
        TraceEvent.create(event="bundle_loaded", ts="2026-05-03T00:00:00+00:00"),
        TraceEvent.create(event="schema_validated", ts="2026-05-03T00:00:01+00:00"),
        TraceEvent.create(event="schema_validated", ts="2026-05-03T00:00:02+00:00"),
    ]

    summary = summarize_trace_events(events)

    assert summary.total_events == 3
    assert summary.counts_by_event == {
        "bundle_loaded": 1,
        "schema_validated": 2,
    }


def test_trace_writer_rejects_forbidden_trace_payload(tmp_path: Path) -> None:
    trace_path = tmp_path / "trace.jsonl"
    writer = TraceWriter(trace_path)
    writer.write("report_written", ts="2026-05-03T00:00:00+00:00", payload={"orders": []})

    raw_line = trace_path.read_text(encoding="utf-8")
    payload = json.loads(raw_line)

    assert payload == {
        "event": "report_written",
        "payload": {},
        "ts": "2026-05-03T00:00:00+00:00",
    }
    assert all(term not in raw_line.lower() for term in FORBIDDEN_TRACE_TERMS)
