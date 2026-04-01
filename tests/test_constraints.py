from __future__ import annotations

from portfolio_os.compliance.findings import summarize_findings
from portfolio_os.compliance.pretrade import run_pretrade_checks
from portfolio_os.data.portfolio import load_portfolio_state
from portfolio_os.domain.enums import OrderSide
from portfolio_os.domain.models import Order, TradeInstruction
from portfolio_os.optimizer.repair import repair_instructions
from portfolio_os.utils.config import load_app_config


def test_effective_single_name_limit_uses_stricter_value(sample_context: dict) -> None:
    assert sample_context["config"].constraints.single_name_max_weight == 0.12
    assert sample_context["config"].constraints.double_ten.single_fund_limit == 0.10
    assert sample_context["config"].effective_single_name_limit == 0.10


def test_constraint_engine_detects_single_name_and_industry_breaches(sample_context: dict) -> None:
    orders = [
        Order(
            ticker="000333",
            side=OrderSide.BUY,
            quantity=12000,
            estimated_price=60.0,
            estimated_notional=720000.0,
            estimated_fee=0.0,
            estimated_slippage=0.0,
            urgency="high",
            reason="test",
        )
    ]
    findings = run_pretrade_checks(
        sample_context["universe"],
        orders,
        sample_context["config"],
        pre_trade_nav=float(sample_context["config"].portfolio_state.available_cash + sample_context["universe"]["current_notional"].sum()),
    )
    rule_codes = {finding.rule_code for finding in findings}
    assert "single_name_limit" in rule_codes
    assert "industry_bounds" in rule_codes


def test_repair_flags_untradable_positions(sample_context: dict) -> None:
    instructions = [
        TradeInstruction(ticker="600276", quantity=-500.0, estimated_price=40.0, current_weight=0.0, target_weight=0.0),
        TradeInstruction(ticker="000858", quantity=-500.0, estimated_price=130.0, current_weight=0.0, target_weight=0.0),
        TradeInstruction(ticker="601012", quantity=1000.0, estimated_price=25.0, current_weight=0.0, target_weight=0.0),
    ]
    _, findings = repair_instructions(
        instructions,
        sample_context["universe"].loc[
            sample_context["universe"]["ticker"].isin(["600276", "000858", "601012"])
        ].reset_index(drop=True),
        sample_context["config"],
        pre_trade_nav=float(sample_context["config"].portfolio_state.available_cash + sample_context["universe"]["current_notional"].sum()),
    )
    blocked_tickers = {finding.ticker for finding in findings if finding.rule_code == "trade_blocked"}
    assert blocked_tickers == {"600276", "000858", "601012"}


def test_manager_aggregate_warning_uses_other_accounts_only(sample_context: dict) -> None:
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
        sample_context["config"],
        pre_trade_nav=float(sample_context["config"].portfolio_state.available_cash + sample_context["universe"]["current_notional"].sum()),
    )
    manager_findings = [finding for finding in findings if finding.rule_code == "manager_aggregate_limit"]
    assert manager_findings
    assert manager_findings[0].details["post_trade_quantity"] == 3500.0
    assert round(manager_findings[0].details["ratio"], 4) == 0.11


def test_finding_summary_counts_are_reusable_for_benchmarks(sample_context: dict) -> None:
    instructions = [
        TradeInstruction(ticker="600276", quantity=-500.0, estimated_price=40.0, current_weight=0.0, target_weight=0.0),
        TradeInstruction(ticker="601012", quantity=1000.0, estimated_price=25.0, current_weight=0.0, target_weight=0.0),
    ]
    _, repair_findings = repair_instructions(
        instructions,
        sample_context["universe"].loc[
            sample_context["universe"]["ticker"].isin(["600276", "601012"])
        ].reset_index(drop=True),
        sample_context["config"],
        pre_trade_nav=float(sample_context["config"].portfolio_state.available_cash + sample_context["universe"]["current_notional"].sum()),
    )
    summary = summarize_findings(repair_findings)

    assert summary["blocked_trade_count"] == 2
    assert summary["total"] >= 2


def test_manager_aggregate_warning_count_is_stable(sample_context: dict) -> None:
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
        sample_context["config"],
        pre_trade_nav=float(sample_context["config"].portfolio_state.available_cash + sample_context["universe"]["current_notional"].sum()),
    )
    summary = summarize_findings(findings)

    assert summary["rule_counts"]["manager_aggregate_limit"] == 1


def test_constraint_templates_share_structure_and_differ_in_thresholds(project_root, sample_context: dict) -> None:
    portfolio_state = load_portfolio_state(sample_context["sample_dir"] / "portfolio_state_example.yaml")
    public_config = load_app_config(
        default_path=project_root / "config" / "default.yaml",
        constraints_path=project_root / "config" / "constraints" / "public_fund.yaml",
        execution_path=project_root / "config" / "execution" / "conservative.yaml",
        portfolio_state=portfolio_state,
    )
    private_config = load_app_config(
        default_path=project_root / "config" / "default.yaml",
        constraints_path=project_root / "config" / "constraints" / "private_fund.yaml",
        execution_path=project_root / "config" / "execution" / "conservative.yaml",
        portfolio_state=portfolio_state,
    )
    quant_config = load_app_config(
        default_path=project_root / "config" / "default.yaml",
        constraints_path=project_root / "config" / "constraints" / "quant_fund.yaml",
        execution_path=project_root / "config" / "execution" / "conservative.yaml",
        portfolio_state=portfolio_state,
    )

    assert public_config.constraints.report_labels.mandate_type == "public_fund"
    assert private_config.constraints.report_labels.mandate_type == "private_fund"
    assert quant_config.constraints.report_labels.mandate_type == "quant_fund"
    assert public_config.constraints.participation_limit != private_config.constraints.participation_limit
    assert public_config.constraints.single_name_max_weight != quant_config.constraints.single_name_max_weight
    assert public_config.constraints.severity_policy.blocked_trade == "BREACH"
    assert private_config.constraints.severity_policy.blocked_trade == "WARNING"


def test_finding_fields_include_category_blocking_and_repair_status(sample_context: dict) -> None:
    instructions = [
        TradeInstruction(ticker="600276", quantity=-500.0, estimated_price=40.0, current_weight=0.0, target_weight=0.0),
    ]
    _, findings = repair_instructions(
        instructions,
        sample_context["universe"].loc[
            sample_context["universe"]["ticker"].isin(["600276"])
        ].reset_index(drop=True),
        sample_context["config"],
        pre_trade_nav=float(sample_context["config"].portfolio_state.available_cash + sample_context["universe"]["current_notional"].sum()),
    )
    finding = findings[0]

    assert finding.code == "trade_blocked"
    assert finding.category.value == "tradability"
    assert isinstance(finding.blocking, bool)
    assert finding.rule_source
    assert finding.repair_status.value == "repaired"


def test_single_name_limit_untradeable_is_non_blocking_disposition(sample_context: dict) -> None:
    subset = sample_context["universe"].loc[
        sample_context["universe"]["ticker"].isin(["000858"])
    ].reset_index(drop=True)
    subset.loc[:, "quantity"] = 1200.0
    subset.loc[:, "upper_limit_hit"] = True
    subset.loc[:, "tradable"] = True
    pre_trade_nav = float(
        sample_context["config"].portfolio_state.available_cash
        + float(subset.iloc[0]["estimated_price"]) * float(subset.iloc[0]["quantity"])
    )
    findings = run_pretrade_checks(
        subset,
        [],
        sample_context["config"],
        pre_trade_nav=pre_trade_nav,
    )
    single_name = [item for item in findings if item.rule_code == "single_name_limit"]
    assert single_name
    finding = single_name[0]
    assert finding.blocking is False
    assert finding.severity.value == "BREACH"
    assert finding.details.get("disposition") == "blocked_untradeable"
