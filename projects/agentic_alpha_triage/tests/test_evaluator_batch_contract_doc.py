from __future__ import annotations

from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONTRACT_PATH = PROJECT_ROOT / "docs" / "evaluator_batch_contract.md"


def test_evaluator_batch_contract_documents_allowed_and_forbidden_outputs() -> None:
    text = CONTRACT_PATH.read_text(encoding="utf-8")

    for required_phrase in (
        "ready count",
        "rejected count",
        "mismatch count",
        "rejection reasons",
        "referenced fixture paths",
    ):
        assert required_phrase in text

    for forbidden_phrase in (
        "realized return",
        "alpha performance",
        "orders",
        "trading instructions",
        "PortfolioOS workflow output",
        "Q2 exports",
        "live FMP/SEC calls",
        "LLM agent loops",
    ):
        assert forbidden_phrase in text
