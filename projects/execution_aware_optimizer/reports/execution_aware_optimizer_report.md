# Execution-Aware Portfolio Optimizer Report

## 1. Research question

Can a raw alpha signal survive risk, sector, position, turnover, liquidity, and transaction-cost constraints?

## 2. Alpha input

No alpha input was loaded. The project can accept CSV/parquet files with `date`, `symbol`, and `alpha_score` columns.

## 3. Portfolio construction ladder

| layer | date | gross_return | net_return | turnover | cost | infeasibility_reason |
|---|---:|---:|---:|---:|---:|---|
| raw_top_alpha_equal_weight | Not available | Not available | Not available | Not available | Not available | PortfolioOS run disabled by config. Set portfolioos.allow_portfolioos_run=true to execute the configured backtest adapter explicitly. |
| risk_controlled | Not available | Not available | Not available | Not available | Not available | PortfolioOS run disabled by config. Set portfolioos.allow_portfolioos_run=true to execute the configured backtest adapter explicitly. |
| sector_constrained | Not available | Not available | Not available | Not available | Not available | PortfolioOS run disabled by config. Set portfolioos.allow_portfolioos_run=true to execute the configured backtest adapter explicitly. |
| position_constrained | Not available | Not available | Not available | Not available | Not available | PortfolioOS run disabled by config. Set portfolioos.allow_portfolioos_run=true to execute the configured backtest adapter explicitly. |
| turnover_constrained | Not available | Not available | Not available | Not available | Not available | PortfolioOS run disabled by config. Set portfolioos.allow_portfolioos_run=true to execute the configured backtest adapter explicitly. |
| liquidity_constrained | Not available | Not available | Not available | Not available | Not available | PortfolioOS run disabled by config. Set portfolioos.allow_portfolioos_run=true to execute the configured backtest adapter explicitly. |
| full_execution_aware_cost_adjusted | Not available | Not available | Not available | Not available | Not available | PortfolioOS run disabled by config. Set portfolioos.allow_portfolioos_run=true to execute the configured backtest adapter explicitly. |

## 4. Cost model

- Transaction-cost objective mode: `nav_fraction`
- Cost sensitivity levels: 0 bps, 5 bps, 10 bps, 25 bps, 50 bps

## 5. Constraint diagnostics

- Binding constraints: `[]`
- Rejected symbols: `[]`
- Infeasible rebalance dates: `[]`
- TODOs: `['PortfolioOS optimizer dual values / shadow prices are not exposed yet; report slack/usage metrics instead.', 'Add explicit liquidity constraint usage once PortfolioOS exports per-name participation slack.', 'Add risk exposure attribution once PortfolioOS exports rebalance-level factor exposures in a stable schema.']`

## 6. Gross vs net performance

Gross and net rows are reported only when the underlying PortfolioOS adapter returns period attribution.

| layer | observations | mean_gross_return | mean_net_return | mean_cost_drag | mean_turnover | unavailable_rows |
|---|---:|---:|---:|---:|---:|---:|
| raw_top_alpha_equal_weight | 0 | Not available | Not available | Not available | Not available | 1 |
| risk_controlled | 0 | Not available | Not available | Not available | Not available | 1 |
| sector_constrained | 0 | Not available | Not available | Not available | Not available | 1 |
| position_constrained | 0 | Not available | Not available | Not available | Not available | 1 |
| turnover_constrained | 0 | Not available | Not available | Not available | Not available | 1 |
| liquidity_constrained | 0 | Not available | Not available | Not available | Not available | 1 |
| full_execution_aware_cost_adjusted | 0 | Not available | Not available | Not available | Not available | 1 |

## 7. Alpha decay under constraints

Alpha decay is not fabricated. Missing layers remain marked with `infeasibility_reason` until PortfolioOS exposes the required adapter hooks.

Alpha decay cannot be summarized until the raw layer has net return observations.

## 8. Cost sensitivity

Cost-sensitivity rows are summarized only from supplied CSV results. Unavailable rows remain unavailable until an explicit PortfolioOS execution path produces attribution.

| cost_bps | layer | observations | mean_gross_return | mean_net_return | mean_cost_drag | mean_turnover | unavailable_rows |
|---:|---|---:|---:|---:|---:|---:|---:|
| 0 | full_execution_aware_cost_adjusted | 0 | Not available | Not available | Not available | Not available | 1 |
| 5 | full_execution_aware_cost_adjusted | 0 | Not available | Not available | Not available | Not available | 1 |
| 10 | full_execution_aware_cost_adjusted | 0 | Not available | Not available | Not available | Not available | 1 |
| 25 | full_execution_aware_cost_adjusted | 0 | Not available | Not available | Not available | Not available | 1 |
| 50 | full_execution_aware_cost_adjusted | 0 | Not available | Not available | Not available | Not available | 1 |

## 9. Infeasibility / failure cases

Rows with `infeasibility_reason` are intentional audit records, not failed backtest numbers.

## 10. Reproducibility instructions

Run the project scripts from the repository root with Poetry and explicit configs, for example:

```bash
PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_alpha_decay_ladder.py --config projects/execution_aware_optimizer/configs/alpha_decay_ladder.yaml
```
