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

## Execution Evaluation Matrix

Phase 23 adds an execution matrix over:

- cost bps: `1`, `5`, `10`, `25`, `50`
- participation rate: `0.001`, `0.005`, `0.01`
- liquidity bucket: `high`, `medium`, `low`
- constraint level: `raw`, `risk_aware`, `full_execution_aware`
- execution mode: `impact_aware`, `participation_twap`

Each scenario records a deterministic `source_config_hash`. The matrix delegates
portfolio construction to the existing ladder adapter, so default configs still
produce structured unavailable rows rather than fake scenario returns.
Unavailable rows also carry structured explanation metadata from
`portfolio_os.explain`, including `primary_reason`, `severity`,
`human_readable`, and `fix_hint`.

## Typed Alpha Execution Matrix

Phase 39 adds a typed matrix adapter for Promotion Gate v2 outputs. It consumes:

- `Q2InputContractV2`
- `alpha_projection_manifest.json`
- `expected_return_panel.csv`
- `alpha_projection_diagnostics.json`
- `alpha_abstain_report.json`

It adds typed dimensions for projection policy, abstain policy, and alpha family.
Default rows remain explicitly unavailable because the typed Q2 execution adapter
does not yet run PortfolioOS optimization. The matrix still reports active
rebalance count, active name count, expected-return used share, abstain count,
sign consistency, view overlap, cost assumptions, and constraint level so reports
can explain where projected alpha is consumed by coverage, abstain, cost, and
constraints without inventing returns.

Run the matrix without enabling PortfolioOS execution:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_execution_matrix.py --config projects/execution_aware_optimizer/configs/execution_matrix.yaml --output /tmp/portfolioos_q2_execution_matrix/execution_matrix.csv --summary-output /tmp/portfolioos_q2_execution_matrix/robustness_summary.json --report /tmp/portfolioos_q2_execution_matrix/execution_report.md
```

## Typed Q2 Execution Adapter v0

Phase 47 adds a local-only typed Q2 adapter around the existing
`run_alpha_decay_ladder` PortfolioOS adapter. It consumes a
`Q2InputContractV2`, projected `expected_return_panel.csv`, projection manifest,
and local backtest manifest. With `allow_portfolioos_run=false`, every row stays
structured unavailable and PortfolioOS is not called. With explicit opt-in, the
adapter maps locally available period-attribution rows into observed typed Q2
rows and preserves unsupported layers as unavailable.

The v0 adapter does not feed the projected expected-return panel into a new
optimizer path. It validates and records typed-alpha coverage diagnostics, then
observes only the local PortfolioOS fixture metrics exposed by stable existing
backtest outputs. It does not call live data, broker, or order workflows, and it
does not imply production alpha approval.

Run the opt-in local smoke target:

```bash
make typed-q2-adapter-fixture
```

This writes ignored local artifacts under `outputs/typed_q2_adapter_fixture/`:

- `typed_q2_execution_matrix.csv`
- `typed_q2_adapter_result.json`
- `typed_q2_robustness_summary.json`
- `typed_q2_adapter_manifest.json`
- `typed_q2_adapter_trace.jsonl`

## Typed Expected-Return Injection Fixture

Phase 48 adds a local-only injection fixture. It validates `Q2InputContractV2`
and `alpha_projection.v2`, applies optional scale/sign transforms to the
projected expected-return panel, and writes an `optimizer_input_snapshot.csv`
with the same `expected_return` column shape consumed by PortfolioOS
`run_rebalance`.

This proves reachability into optimizer input, not optimizer response. Phase 49
is responsible for directional optimizer acceptance. The fixture does not call
live data, brokers, order submission, or production approval paths.

Run the opt-in local smoke target:

```bash
make typed-expected-return-injection-fixture
```

This writes ignored local artifacts under
`outputs/typed_expected_return_injection_fixture/`:

- `typed_expected_return_injection_result.json`
- `optimizer_input_snapshot.csv`
- `injected_expected_return_panel.csv`
- `typed_q2_execution_matrix_injected.csv`
- `typed_q2_injection_robustness_summary.json`
- `typed_q2_injection_manifest.json`
- `typed_q2_injection_trace.jsonl`

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
