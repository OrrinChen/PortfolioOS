from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_MAKE_TARGETS = {
    "test",
    "lint",
    "validate-examples",
    "audit-report",
    "demo",
    "no-network",
    "validate",
}


def test_makefile_declares_ci_validation_targets() -> None:
    makefile = REPO_ROOT / "Makefile"

    text = makefile.read_text(encoding="utf-8")

    for target in REQUIRED_MAKE_TARGETS:
        assert f"{target}:" in text


def test_audit_report_has_golden_output_regression_test() -> None:
    test_file = REPO_ROOT / "projects" / "audit_report" / "tests" / "test_demo_audit_report.py"

    text = test_file.read_text(encoding="utf-8")

    assert "test_demo_audit_report_matches_golden_output" in text
    assert "EXPECTED_DEMO_AUDIT_REPORT" in text


def test_validation_docs_include_no_network_and_trace_checks() -> None:
    validation = (REPO_ROOT / "VALIDATION.md").read_text(encoding="utf-8")

    assert "Structured trace tests" in validation
    assert "No-network guard" in validation
    assert "make validate" in validation
