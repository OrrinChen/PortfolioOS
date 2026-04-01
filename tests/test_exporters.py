from __future__ import annotations

from portfolio_os.execution.adapters.csv_export import build_oms_frame
from portfolio_os.execution.adapters.o32_stub import build_o32_preview
from portfolio_os.optimizer.rebalancer import run_rebalance


def test_oms_export_contains_expected_fields(sample_context: dict) -> None:
    rebalance_run = run_rebalance(sample_context["universe"], sample_context["config"])
    oms_frame = build_oms_frame(
        basket=rebalance_run.basket,
        findings=rebalance_run.findings,
        config=sample_context["config"],
        basket_id="test_basket",
    )

    assert list(oms_frame.columns) == [
        "account_id",
        "ticker",
        "side",
        "quantity",
        "price_type",
        "limit_price",
        "estimated_price",
        "estimated_notional",
        "urgency",
        "strategy_tag",
        "basket_id",
        "reason",
        "blocking_checks_cleared",
    ]
    assert not oms_frame.empty
    assert (oms_frame["basket_id"] == "test_basket").all()


def test_o32_preview_structure_is_reasonable(sample_context: dict) -> None:
    rebalance_run = run_rebalance(sample_context["universe"], sample_context["config"])
    preview = build_o32_preview(
        basket=rebalance_run.basket,
        findings=rebalance_run.findings,
        config=sample_context["config"],
        basket_id="preview_basket",
    )

    assert preview
    assert {
        "fund_account",
        "stock_code",
        "entrust_bs",
        "entrust_amount",
        "price_mode",
        "entrust_price",
        "basket_no",
        "strategy_name",
        "memo",
        "risk_checks_passed",
    }.issubset(preview[0].keys())
