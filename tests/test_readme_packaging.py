from __future__ import annotations

from pathlib import Path


README = Path(__file__).resolve().parents[1] / "README.md"


def test_readme_packages_audit_ready_platform_story() -> None:
    text = README.read_text(encoding="utf-8")
    assert len(text.splitlines()) <= 200

    required_sections = (
        "## Headline results",
        "## Architecture",
        "## Quickstart",
        "## Reproduction",
        "## Limitations",
        "## Related components",
    )
    for section in required_sections:
        assert section in text
    assert "two independent studies, backtest-based, no live trading" in text.lower()
    assert "rank_ic_t ~ 22.7" in text
    assert "rank_ic_t ~ 3.61" in text
    assert "alpha_only_t ~ 4.03" in text
    assert "~12.3" in text
    assert "https://github.com/OrrinChen/market-replay" in text
    assert "Q1 Alpha Triage" in text
    assert "Evidence Bundle" in text
    assert "Promotion Gate" in text
    assert "Q2 Execution-Aware Evaluation" in text
    assert "make demo" in text
    assert "make validate" in text
    assert "Is this alpha real?" in text
    assert "Can this alpha survive execution?" in text


def test_platform_manual_keeps_the_former_readme_detail() -> None:
    manual = README.parent / "docs" / "platform_manual.md"
    text = manual.read_text(encoding="utf-8")

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
    assert "Promoted-like guidance-raise case" in text
    assert "Rejected forward-return leakage case" in text


def test_repo_packaging_has_license_ci_and_no_agent_scaffold() -> None:
    root = README.parent
    license_text = (root / "LICENSE").read_text(encoding="utf-8")
    assert "MIT License" in license_text
    assert "Copyright (c) 2026 Orrin Chen" in license_text
    workflow = (root / ".github" / "workflows" / "tests.yml").read_text(encoding="utf-8")
    assert "pytest" in workflow
    for name in ("AGENTS.md", "TASK_MEMORY.md", "RUNBOOK.md", "VALIDATION.md"):
        assert not (root / name).exists()
    banned = (
        "C:" + "\\" + "Users" + "\\" + "14574",
        "C:" + "/" + "Users" + "/" + "14574",
        "/" + "Users" + "/",
    )
    for path in root.rglob("*"):
        if not path.is_file() or ".git" in path.parts:
            continue
        if path.suffix.lower() not in {".md", ".py", ".yml", ".yaml", ".toml"} and path.name != "Makefile":
            continue
        text = path.read_text(encoding="utf-8")
        for needle in banned:
            assert needle not in text, f"{path} still contains {needle}"


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
