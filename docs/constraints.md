# Constraint Support

## Template Families

PortfolioOS currently ships three constraint templates with one shared YAML schema:

- `public_fund.yaml`
- `private_fund.yaml`
- `quant_fund.yaml`

Each template includes:

- `single_name_max_weight`
- `industry_bounds`
- `max_turnover`
- `min_order_notional`
- `participation_limit`
- `cash_non_negative`
- `double_ten`
- `severity_policy`
- `report_labels`
- `blocked_trade_policy`

## Hard Blocks

These checks are treated as hard controls in the current MVP flow:

- negative cash or cash buffer breach
- negative post-trade shares
- sell quantity above current holdings
- participation limit breach
- single-name cap breach
- industry band breach
- turnover cap breach
- non-tradable, suspended, or limit-hit trade directions
- buy and sell blacklist directions

Hard blocks may appear in two different ways:

- repaired and removed before export
- still unresolved after repair, in which case the finding remains blocking

## Warning / Report Style Checks

These checks are currently more report-oriented:

- manager aggregate 10% ownership warning
- double-ten remediation reminder
- lot rounding notice
- minimum notional removal notice
- cash-repair trade reduction notice

The exact severity label can vary by template through `severity_policy`.

## Template Behavior Differences

At a high level:

- `public_fund.yaml`: stricter turnover and participation, stricter blocked-trade signaling, public-fund style strategy tags
- `private_fund.yaml`: wider exposure and turnover room, more permissive severity posture for blocked-trade reporting
- `quant_fund.yaml`: moderate risk limits, higher throughput than public-fund style, execution-oriented labeling

The field structure stays the same so the CLI and downstream reports do not need different code paths.

## MVP Placeholder Logic

The following items are intentionally simplified:

- board-lot handling is fixed at 100 shares
- manager aggregate 10% is a warning, not a hard optimization block
- 10-day remediation is a report marker only
- no exchange-specific exceptions are modeled for board-specific rules
- no commission floor is modeled to preserve convexity
- `blocked_trade_policy` currently affects reporting and export-readiness semantics, not broker connectivity

## Extension Points

- add richer regulatory rule packs in `portfolio_os.constraints.regulatory`
- support custom mandate constraints in `portfolio_os.constraints.custom`
- plug in benchmark-relative or factor-relative constraints
- attach a formal risk model and covariance-aware objectives
- add account, broker, or venue-specific execution constraints
