# Execution-Aware Portfolio Optimizer

Q2 asks: **Can this alpha survive execution?**

Raw alpha can look attractive in research notebooks. This project measures how much of that raw signal survives after PortfolioOS portfolio construction layers: risk, sector, position, turnover, liquidity, and transaction-cost constraints.

This is not a generic trading bot and not a toy Markowitz notebook. It is a standalone project shell that can reuse PortfolioOS components without merging the Q2 story into the core platform.

## Relationship To Q1

Q1 is separate and asks: **Is this alpha real?**

Q2 assumes an alpha score already exists. Q1 may later export an `alpha_score.csv` file that Q2 can read, but Q2 does not import Q1 code and does not depend on Q1 agents, schemas, or data collection.

## Alpha Input Contract

The alpha adapter accepts CSV or parquet files with:

| column | required | meaning |
|---|---:|---|
| `date` | yes | Alpha observation date |
| `symbol` | yes | Tradable identifier |
| `alpha_score` | yes | Cross-sectional alpha score |
| `alpha_source` | no | Source label or model name |
| `alpha_confidence` | no | Optional numeric confidence |

The adapter validates required columns, parses dates, reports missing scores, drops missing scores, can winsorize scores by date, and can rank-normalize scores by date. Output keeps `symbol`, adds PortfolioOS-compatible `ticker`, and preserves `raw_alpha_score`.

## Portfolio Construction Ladder

The canonical ladder is:

1. `raw_top_alpha_equal_weight`
2. `risk_controlled`
3. `sector_constrained`
4. `position_constrained`
5. `turnover_constrained`
6. `liquidity_constrained`
7. `full_execution_aware_cost_adjusted`

Rows use the standard schema in `src/execution_aware_optimizer/ladder.py`. When PortfolioOS does not yet expose a stable hook for a layer, the row records `infeasibility_reason` instead of inventing performance numbers.

Current adapter status:

| layer | status | source |
|---|---|---|
| `raw_top_alpha_equal_weight` | partial | Maps existing PortfolioOS `alpha_only_top_quintile` period attribution when an explicit backtest adapter run is enabled. |
| `risk_controlled` | unavailable | Needs a stable per-layer PortfolioOS construction hook rather than only final optimizer output. |
| `sector_constrained` | unavailable | Existing PortfolioOS industry bounds exist, but this layer is not separately exposed as a run stage. |
| `position_constrained` | unavailable | Existing single-name and cash constraints exist, but this layer is not separately exposed as a run stage. |
| `turnover_constrained` | unavailable | Existing max-turnover constraint exists, but layer-specific attribution is not exposed yet. |
| `liquidity_constrained` | unavailable | Existing participation constraints exist, but per-name liquidity slack is not exposed yet. |
| `full_execution_aware_cost_adjusted` | partial | Maps existing PortfolioOS `optimizer` period attribution when an explicit backtest adapter run is enabled. |

## PortfolioOS Components Reused

The project is designed around these existing PortfolioOS concepts:

- `portfolio_os.backtest.engine.run_backtest`
- `portfolio_os.optimizer.objective` and `alpha_reward`
- `transaction_cost_objective_mode` with `nav_fraction` and `raw_currency`
- optimizer constraints for single-name, industry, turnover, participation, cash, and factor bounds
- `portfolio_os.cost` fee and slippage models
- `portfolio_os.execution` simulation and reporting concepts
- `portfolio_os.alpha.backtest_bridge` as prior art, not as a hard dependency

## Running

The default configs do not execute PortfolioOS workflows. They produce honest unavailable rows until an explicit adapter run is enabled.

```bash
PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_alpha_decay_ladder.py --config projects/execution_aware_optimizer/configs/alpha_decay_ladder.yaml
```

To allow the configured PortfolioOS backtest adapter, set:

```yaml
portfolioos:
  allow_portfolioos_run: true
```

Only do that when the configured manifest, data, and cost assumptions are intentional for the experiment.

## Executed Adapter Fixture

A focused local-only PortfolioOS-backed fixture now lives in `tests/test_portfolioos_adapter.py`. It calls `run_alpha_decay_ladder` through the library API, uses the existing local backtest manifest, and maps only the two layers PortfolioOS currently exposes through stable period attribution:

- `raw_top_alpha_equal_weight`
- `full_execution_aware_cost_adjusted`

The fixture sets `portfolioos.allow_portfolioos_run=true` only inside the test config. It also asserts that a representative intermediate layer remains explicitly unavailable and that default Q2 configs keep `allow_portfolioos_run=false`.

Default Q2 configs stay non-execution. The fixture does not enable live services, run arbitrary CLI workflow chains, write report artifacts, or fabricate intermediate layer diagnostics.

For a local-only executed report smoke, use the explicit opt-in config and write outputs outside the tracked report directory:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_alpha_decay_ladder.py --config projects/execution_aware_optimizer/configs/local_executed_fixture_report.yaml --output /tmp/portfolioos_q2_local_executed_fixture/alpha_decay_ladder_results.csv --report /tmp/portfolioos_q2_local_executed_fixture/local_executed_fixture_report.md
```

## Cost Sensitivity

`src/execution_aware_optimizer/cost_sensitivity.py` builds one cloned Q2 config per configured cost level. It returns planned PortfolioOS overrides such as `fees.commission_rate`, `execution.backtest_fixed_half_spread_bps`, and `objective_weights.transaction_cost_objective_mode` as data. The base config is not mutated, and no global PortfolioOS config is changed by default.

The same module can read a generated `cost_sensitivity_results.csv` into typed result rows. Executed rows can then be summarized in the markdown report by cost level and ladder layer. Default non-execution rows remain marked unavailable and are not treated as performance.

## Reports

The markdown report includes:

- PortfolioOS adapter execution status and layer coverage
- the full ladder row table
- gross vs net summary by layer
- alpha-decay summary versus `raw_top_alpha_equal_weight`
- cost-sensitivity summaries when a cost-sensitivity CSV is supplied or present in the default report directory

Summary tables use only observed row values. If a layer is unavailable or a PortfolioOS hook does not expose the required attribution, the report writes `Not available` rather than filling in synthetic performance numbers.

## Current Missing Hooks

The project records these as TODOs rather than faking diagnostics:

- optimizer dual values / shadow prices
- stable binding constraint labels per solve
- per-name liquidity slack and participation-budget usage
- risk exposure attribution at each rebalance
- transaction-cost attribution by layer beyond available period attribution

## Tests

Run the project tests from the repository root:

```bash
PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests -q
```
