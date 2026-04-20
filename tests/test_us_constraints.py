from __future__ import annotations

from portfolio_os.compliance.pretrade import run_pretrade_checks
from portfolio_os.domain.enums import OrderSide
from portfolio_os.domain.models import Order, TradeInstruction
from portfolio_os.optimizer.repair import repair_instructions


def _us_config(sample_context: dict):
    config = sample_context["config"].model_copy(deep=True)
    config.trading.market = "us"
    config.trading.lot_size = 1
    config.constraints.double_ten.enabled = True
    config.constraints.double_ten.single_fund_limit = 0.10
    config.constraints.double_ten.manager_aggregate_limit = 0.10
    return config


def test_us_effective_single_name_limit_ignores_double_ten(sample_context: dict) -> None:
    config = _us_config(sample_context)
    config.constraints.single_name_max_weight = 0.18
    config.constraints.double_ten.single_fund_limit = 0.10
    assert config.effective_single_name_limit == 0.18


def test_us_repair_ignores_upper_limit_flag_and_uses_lot_size_one(sample_context: dict) -> None:
    config = _us_config(sample_context)
    config.constraints.min_order_notional = 0.0
    config.constraints.participation_limit = 1.0
    subset = sample_context["universe"].loc[
        sample_context["universe"]["ticker"].isin(["300750"])
    ].reset_index(drop=True)
    subset.loc[:, "tradable"] = True
    subset.loc[:, "upper_limit_hit"] = True
    subset.loc[:, "lower_limit_hit"] = False
    pre_trade_nav = float(config.portfolio_state.available_cash + subset["current_notional"].sum())

    repaired, findings = repair_instructions(
        [
            TradeInstruction(
                ticker="300750",
                quantity=7.6,
                estimated_price=float(subset.iloc[0]["estimated_price"]),
                current_weight=float(subset.iloc[0]["current_weight"]),
                target_weight=float(subset.iloc[0]["target_weight"]),
            )
        ],
        subset,
        config,
        pre_trade_nav=pre_trade_nav,
    )

    assert repaired
    assert repaired[0].quantity == 7.0
    assert all(item.rule_code != "trade_blocked" for item in findings)


def test_us_pretrade_skips_double_ten_manager_aggregate_findings(sample_context: dict) -> None:
    config = _us_config(sample_context)
    orders = [
        Order(
            ticker="000333",
            side=OrderSide.BUY,
            quantity=3000,
            estimated_price=60.0,
            estimated_notional=180000.0,
            estimated_fee=0.0,
            estimated_slippage=0.0,
            urgency="high",
            reason="test",
        )
    ]
    findings = run_pretrade_checks(
        sample_context["universe"],
        orders,
        config,
        pre_trade_nav=float(
            config.portfolio_state.available_cash + sample_context["universe"]["current_notional"].sum()
        ),
    )
    rule_codes = {item.rule_code for item in findings}
    assert "manager_aggregate_limit" not in rule_codes
    assert "double_ten_remediation" not in rule_codes
