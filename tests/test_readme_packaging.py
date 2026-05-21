from __future__ import annotations

from pathlib import Path


README = Path(__file__).resolve().parents[1] / "README.md"


def test_readme_packages_audit_ready_platform_story() -> None:
    text = README.read_text(encoding="utf-8")

    required_sections = (
        "## Problem",
        "## Solution",
        "## Architecture",
        "## Quickstart",
        "## Example Outputs",
        "## Safety Boundaries",
        "## Case Studies",
        "## Validation",
    )
    for section in required_sections:
        assert section in text
    assert "Q1 Alpha Triage" in text
    assert "Evidence Bundle" in text
    assert "Promotion Gate" in text
    assert "Q2 Execution-Aware Evaluation" in text
    assert "make demo" in text
    assert "make validate" in text


def test_readme_keeps_promoted_and_rejected_cases_separate() -> None:
    text = README.read_text(encoding="utf-8")

    assert "Promoted-like guidance-raise case" in text
    assert "Rejected forward-return leakage case" in text
    assert "Is this alpha real?" in text
    assert "Can this alpha survive execution?" in text


def test_readme_does_not_claim_production_trading_or_alpha_success() -> None:
    lowered = README.read_text(encoding="utf-8").lower()

    forbidden_claims = (
        "production trading system",
        "profitable alpha",
        "alpha discovery success",
        "guaranteed returns",
        "live trading bot",
    )
    for claim in forbidden_claims:
        assert claim not in lowered
