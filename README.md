# PortfolioOS

PortfolioOS is a compliance-aware portfolio rebalancing and execution MVP for A-share and US equity workflows. It turns a target portfolio into executable orders, applies pre-trade constraints, estimates simple trading costs, and leaves a full local audit trail.

This MVP is an auxiliary decision-support tool only. It does not constitute investment advice.

## Product Overview

- Audience: public funds, private funds, and quant teams that need repeatable rebalance workflows.
- Positioning: OR-first portfolio rebalance and intelligent execution engine.
- Scope: local CLI, CSV/YAML input, CSV/JSON/Markdown output, plus optional Alpaca paper trading adapter for US pilot loops.
- Market focus: A-share cash equities and US equities (market-switched behavior).

## MVP Scope

- Single-account daily or batch rebalance.
- Continuous optimization with `cvxpy`.
- Repair from continuous trade sizes to executable lot-aware orders (A-share 100-share lots, US 1-share lots).
- Pre-trade compliance findings and local audit package.
- Benchmark comparison against naive and cost-unaware baselines.
- Sample data and tests that run end-to-end.

## Risk Objective Integration Mode

When `risk_model.enabled=true`, objective integration behavior is controlled by
`risk_model.integration_mode`:

- `replace` (default): use risk objective only
  - `risk_term + tracking_error + transaction_cost` (weighted)
- `augment`: use legacy objective plus risk objective
  - `target_deviation + transaction_fee + turnover_penalty + slippage_penalty`
  - plus `risk_term + tracking_error + transaction_cost`

For risk validation and regime-coupling diagnostics, `augment` is recommended.
Default remains `replace` to preserve backward compatibility for existing flows.

## Not Supported Yet

- Live production broker routing (paper-trading adapter only for pilot validation).
- Real-time execution or intraday smart order routing.
- Multi-account netting or householding.
- Full risk models, factor models, or tracking-error optimization.
- ML cost fitting and RL execution.
- Special handling for ETFs, convertibles, margin trading, STAR board, and other market-specific exceptions.

## Installation

Supported development platforms:

- Windows 11 / PowerShell
- macOS 14+ / zsh or bash

Core research, backtest, optimizer, and CLI workflows are Python/Poetry based and are expected to run on both Windows and macOS. The main known portability caveat today is `scripts/run_pilot_validation.py`, which still contains Windows-style Poetry entrypoint lookup for pilot-validation orchestration. If you are continuing research or offline workflow work on a MacBook, the rest of the stack is the safer starting point.

Windows baseline:

```bash
py -3.11 -m pip install poetry
py -3.11 -m poetry install
```

From the project root:

```bash
cd C:\Users\14574\Quant\PortfolioOS
py -3.11 -m poetry install
```

macOS baseline:

```bash
cd /path/to/PortfolioOS
python3.11 -m pip install poetry
python3.11 -m poetry install
```

If `python3.11` is already your default interpreter, `poetry install` is also fine.

## CLI Usage

Shell conventions:

- Windows examples below use PowerShell continuation with `^` and launcher syntax like `py -3.11`.
- On macOS/Linux, replace `^` with `\` and prefer `python3.11 -m poetry run ...` or `poetry run ...`.
- Paths inside the codebase are handled with `pathlib`; doc examples remain Windows-heavy because the project was first developed there.

```bash
py -3.11 -m poetry run portfolio-os ^
  --holdings data/sample/holdings_example.csv ^
  --target data/sample/target_example.csv ^
  --market data/sample/market_example.csv ^
  --reference data/sample/reference_example.csv ^
  --portfolio-state data/sample/portfolio_state_example.yaml ^
  --constraints config/constraints/public_fund.yaml ^
  --config config/default.yaml ^
  --execution-profile config/execution/conservative.yaml ^
  --output-dir outputs/demo_run
```

Optional:

```bash
py -3.11 -m poetry run portfolio-os ... --skip-benchmarks
```

Pilot-style mapped input:

```bash
py -3.11 -m poetry run portfolio-os ^
  --holdings data/import_profile_samples/custodian_style_a/holdings.csv ^
  --target data/import_profile_samples/custodian_style_a/target.csv ^
  --market data/import_profile_samples/custodian_style_a/market.csv ^
  --reference data/import_profile_samples/custodian_style_a/reference.csv ^
  --portfolio-state data/sample/portfolio_state_example.yaml ^
  --constraints config/constraints/public_fund.yaml ^
  --config config/default.yaml ^
  --execution-profile config/execution/conservative.yaml ^
  --import-profile config/import_profiles/custodian_style_a.yaml ^
  --output-dir outputs/demo_run_mapped
```

Batch replay:

```bash
py -3.11 -m poetry run portfolio-os-replay ^
  --manifest data/replay_samples/manifest.yaml ^
  --constraints config/constraints/public_fund.yaml ^
  --config config/default.yaml ^
  --execution-profile config/execution/conservative.yaml ^
  --output-dir outputs/replay_demo
```

Scenario analysis:

```bash
py -3.11 -m poetry run portfolio-os-scenarios ^
  --manifest data/scenario_samples/manifest.yaml ^
  --output-dir outputs/scenario_demo
```

Approval / freeze:

```bash
py -3.11 -m poetry run portfolio-os-approve ^
  --request data/approval_samples/approval_request_example.yaml ^
  --output-dir outputs/approval_demo
```

Execution simulation:

```bash
py -3.11 -m poetry run portfolio-os-execute ^
  --request data/execution_samples/execution_request_example.yaml ^
  --calibration-profile config/calibration_profiles/balanced_day.yaml ^
  --output-dir outputs/execution_demo
```

Pilot validation (nightly mode, provisional allowed):

```bash
py -3.11 -m poetry run portfolio-os-validate-pilot --mode nightly
```

Pilot validation (release mode, strict gate):

```bash
py -3.11 -m poetry run portfolio-os-validate-pilot ^
  --mode release ^
  --reviewer-input outputs/pilot_validation_reviewer.csv ^
  --real-sample
```

Operational guides:

- `docs/pilot_runbook.md`
- `docs/pilot_operations_plan.md`
- `docs/standards/engineering_workflow.md` (TDD + SDD working standard)

## Engineering Standards

- Core workflow standard: `docs/standards/engineering_workflow.md`
- Layered testing policy (L1/L2/L3): `docs/standards/testing_policy.md`
- Task spec template: `docs/templates/task_spec_template.md`
- Test design template: `docs/templates/test_design_template.md`
- PR checklist template: `docs/templates/pr_checklist.md`

Engineering gate commands:

```bash
py -3 scripts/devtools/run_engineering_gate.py
py -3 scripts/devtools/run_engineering_gate.py --full-pytest
```

macOS/Linux equivalent:

```bash
python3 scripts/devtools/run_engineering_gate.py
python3 scripts/devtools/run_engineering_gate.py --full-pytest
```

Pilot operations helper:

```bash
py -3.11 scripts/pilot_ops.py init
py -3.11 scripts/pilot_ops.py nightly --phase phase_1 --real-sample --as-of-date 2026-03-24
py -3.11 scripts/pilot_ops.py weekly --phase phase_2 --reviewer-input outputs/reviewer_release.csv --real-sample
py -3.11 scripts/pilot_ops.py go-nogo --window-trading-days 20 --as-of-date 2026-03-24
```

Data preparation:

```bash
py -3.11 -m poetry run portfolio-os-build-market ^
  --tickers-file data/sample/tickers.txt ^
  --as-of-date 2026-03-23 ^
  --provider mock ^
  --output data/generated/market.csv
```

```bash
py -3.11 -m poetry run portfolio-os-build-reference ^
  --tickers-file data/sample/tickers.txt ^
  --as-of-date 2026-03-23 ^
  --provider mock ^
  --overlay data/sample/reference_overlay_example.csv ^
  --output data/generated/reference.csv
```

```bash
py -3.11 -m poetry run portfolio-os-build-target ^
  --index-code 000300.SH ^
  --as-of-date 2026-03-23 ^
  --provider mock ^
  --output data/generated/target.csv
```

Snapshot bundle:

```bash
py -3.11 -m poetry run portfolio-os-build-snapshot ^
  --tickers-file data/sample/tickers.txt ^
  --index-code 000300.SH ^
  --as-of-date 2026-03-23 ^
  --provider mock ^
  --reference-overlay data/sample/reference_overlay_example.csv ^
  --output-dir data/generated/snapshot_mock
```

Optional:

```bash
py -3.11 -m poetry run portfolio-os-build-snapshot ... --allow-partial-build
```

That keeps successful partial outputs on disk even when one child step is permission-limited.

Tushare-backed data preparation:

```bash
py -3.11 -m poetry run portfolio-os-build-market ^
  --tickers-file data/sample/tickers.txt ^
  --as-of-date 2026-03-23 ^
  --provider tushare ^
  --provider-token YOUR_TOKEN ^
  --output data/generated/market_real.csv
```

US + Alpaca data preparation:

```bash
set ALPACA_API_KEY=YOUR_KEY
set ALPACA_SECRET_KEY=YOUR_SECRET
py -3.11 -m poetry run portfolio-os-build-market ^
  --tickers-file data/samples/us/sample_us_01/tickers.txt ^
  --as-of-date 2026-03-23 ^
  --provider alpaca ^
  --output data/generated/market_us_real.csv
```

macOS/Linux environment setup:

```bash
export ALPACA_API_KEY=YOUR_KEY
export ALPACA_SECRET_KEY=YOUR_SECRET
python3.11 -m poetry run portfolio-os-build-market \
  --tickers-file data/samples/us/sample_us_01/tickers.txt \
  --as-of-date 2026-03-23 \
  --provider alpaca \
  --output data/generated/market_us_real.csv
```

US pilot validation:

```bash
py -3.11 scripts/pilot_ops.py nightly ^
  --phase phase_1 ^
  --market us ^
  --real-sample ^
  --broker alpaca ^
  --notes "us_pilot_smoke_test"
```

US sample basket pre-submission check:

```bash
py -3.11 scripts/pilot_ops.py pre-submit-check ^
  --orders-oms outputs\pilot_validation_20260326_034043\samples\sample_us_01\approval\final_orders_oms.csv ^
  --broker-state-snapshot outputs\broker_state_inspection\broker_state_inspection_<timestamp>\broker_state_report.json ^
  --output-dir outputs\pre_submission_checks ^
  --notes "sample_us_01_preopen_check"
```

Offline slippage calibration with synthetic fills:

```bash
py -3.11 scripts/run_slippage_calibration.py ^
  --synthetic ^
  --output-dir outputs\slippage_calibration_us ^
  --min-filled-orders 20 ^
  --min-participation-span 10
```

## Output Files

- `orders.csv`: executable order basket with prices, notionals, fees, slippage, urgency, and reason text.
- `orders_oms.csv`: OMS-friendly basket export with account, strategy tag, basket ID, and blocking-check release status.
- `audit.json`: structured audit payload containing hashes, parameters, findings, orders, and summary.
- `summary.md`: portfolio-level Markdown summary for investment or execution review.
- `benchmark_comparison.json`: static benchmark comparison versus naive and cost-unaware rebalance.
- `benchmark_comparison.md`: demo-friendly benchmark report with strategy table and comparison bullets.
- `run_manifest.json`: local run metadata with file paths and timestamps.

Replay outputs:

- `suite_results.json`: replay-suite JSON with per-sample strategy metrics, PortfolioOS-vs-baseline deltas, and aggregate statistics.
- `suite_summary.md`: replay-suite Markdown report with strategy overview, median improvement summaries, and best/worst samples.
- `sample_results/<sample_name>/benchmark_comparison.json`: per-sample benchmark comparison payload.
- `sample_results/<sample_name>/benchmark_comparison.md`: per-sample demo-friendly comparison note.

Scenario outputs:

- `scenario_comparison.json`: structured scenario metrics, ranking, labels, and recommendation metadata.
- `scenario_comparison.md`: client-facing scenario comparison with recommendation and alternatives.
- `decision_pack.md`: PM / trading / risk decision note for the current workflow scoring rule.
- `scenario_results/<scenario_id>/...`: optional per-scenario `orders.csv`, `orders_oms.csv`, `audit.json`, and `summary.md`.

Approval outputs:

- `approval_record.json`: structured approval request, selected scenario, status, warnings, override metadata, and source hashes.
- `approval_summary.md`: client-facing sign-off summary for PM / trader / risk / compliance.
- `freeze_manifest.json`: source-to-final artifact manifest with hashes, timestamps, and override metadata.
- `final_orders.csv`
- `final_orders_oms.csv`
- `final_audit.json`
- `final_summary.md`
- `handoff_checklist.md`

Execution simulation outputs:

- `execution_report.json`: structured execution request metadata, bucket curve, per-order results, portfolio summary, optional stress-test comparison, and source hashes.
- `execution_report.md`: PM / trader / risk-facing intraday execution note with fill rate, cost, worst tickets, one-line conclusion, and optional stress comparison.
- `execution_fills.csv`: per-order execution outcomes with fill ratio, average fill price, fee, slippage, and status.
- `execution_child_orders.csv`: bucket-level child execution detail with bucket caps, fills, residual quantity, and cost.
- `handoff_checklist.md`: file-based PM / trader / risk checklist that combines freeze and execution-preflight review points.

Data builder outputs:

- `market.csv`: standard market snapshot file consumable by `portfolio-os`
- `reference.csv`: standard reference file consumable by `portfolio-os`
- `target.csv`: standard target file consumable by `portfolio-os`
- `market_manifest.json`: market-builder provenance note with provider, date, request parameters, output hash, and approximation notes
- `reference_manifest.json`: reference-builder provenance note with provider, date, overlay path, output hash, and approximation notes
- `target_manifest.json`: target-builder note recording provider, weight sum, normalization status, output hash, and approximation notes
- `snapshot_manifest.json`: snapshot-bundle provenance note linking the full static package together

## Sample Workflow

1. Load current holdings, target weights, market data, reference data, account state, and YAML templates.
2. Validate required ticker coverage for market and industry data.
3. Estimate fees and simple slippage costs.
4. Solve a single-period convex rebalance problem.
5. Repair trades into 100-share executable orders.
6. Run pre-trade compliance checks on the repaired basket.
7. Export both an analysis basket and an OMS-friendly basket template.
8. Compare the PortfolioOS strategy with naive and cost-unaware baselines on the same snapshot.
9. Persist the basket, benchmark report, summary, audit package, and manifest locally.

## Benchmark Comparison

Each default CLI run evaluates three strategies on the same static snapshot:

- `naive_target_rebalance`: direct target chasing with only basic executable repair.
- `cost_unaware_rebalance`: optimization that prioritizes target fit and keeps only basic hard constraints, without cost terms in the objective.
- `portfolio_os_rebalance`: the default cost-aware PortfolioOS strategy.

The comparison output uses one fixed metric schema across all three strategies:

- `strategy_name`
- `pre_trade_nav`
- `cash_before`
- `cash_after`
- `target_deviation_before`
- `target_deviation_after`
- `target_deviation_improvement`
- `gross_traded_notional`
- `turnover`
- `estimated_fee_total`
- `estimated_slippage_total`
- `estimated_total_cost`
- `buy_order_count`
- `sell_order_count`
- `blocked_trade_count`
- `compliance_finding_count`

This is the core Phase 2 demo story: PortfolioOS can show not only the recommended basket, but also how much estimated cost and execution friction it avoids versus a more naive workflow.

## Constraint Templates

PortfolioOS ships with three mandate templates that share one YAML structure:

- `public_fund.yaml`
- `private_fund.yaml`
- `quant_fund.yaml`

All three include:

- single-name cap
- industry exposure bands
- turnover cap
- minimum order notional
- participation limit
- double-ten settings
- severity policy
- report labels
- blocked-trade policy

Why this matters:

- PMs can switch mandate templates without changing the input file shape.
- compliance and trading can see which rules are hard blocks and which are warnings.
- OMS-ready exports can carry the template-derived `strategy_tag`.

In practical terms:

- `public_fund.yaml` is the strictest and the most compliance-forward.
- `private_fund.yaml` is wider and more flexible.
- `quant_fund.yaml` keeps risk controls but is tuned for more turnover and execution throughput.

## Batch Replay

Phase 3 adds a lightweight static replay suite.

This is not an event-driven historical backtest. It is a batch replay of multiple static snapshots using the same single-run benchmark logic.

Input layout:

- `data/replay_samples/manifest.yaml`
- `data/replay_samples/sample_01/holdings.csv`
- `data/replay_samples/sample_01/target.csv`
- `data/replay_samples/sample_01/market.csv`
- `data/replay_samples/sample_01/reference.csv`
- `data/replay_samples/sample_01/portfolio_state.yaml`
- and the same structure for the other sample directories listed in the manifest

The replay suite runs all three strategies on every sample:

- `naive_target_rebalance`
- `cost_unaware_rebalance`
- `portfolio_os_rebalance`

It then aggregates the differences between PortfolioOS and the baselines across the suite.

Current aggregate statistics include:

- sample count
- `cost_savings`: mean, median, min, max, positive rate
- `turnover_reduction`: mean, median
- `blocked_trade_reduction`: mean, median
- `finding_count_difference`: mean, median

This is the Phase 3 evidence story: not just "it worked once," but "it showed repeatable cost and execution-friction benefits across multiple static samples."

## Scenario Analysis

Phase 6 adds a scenario engine for one shared snapshot.

This is different from replay:

- replay compares the workflow across multiple static samples
- scenarios compare multiple policy choices on the same static sample

The scenario manifest format keeps one shared `base_inputs` block and then defines multiple scenarios, each with:

- `id`
- `label`
- `constraints`
- `execution_profile`
- `overrides`

The override whitelist is intentionally narrow:

- `max_turnover`
- `single_name_max_weight`
- `min_order_notional`
- `participation_limit`
- `min_cash_buffer`

The scenario outputs are designed for decision support rather than benchmarking alone:

- compare conservative and aggressive policy variants
- compare mandate templates on the same snapshot
- surface named alternatives such as lowest cost and lowest turnover
- recommend one scenario under a transparent workflow scoring rule

## Approval And Freeze Workflow

Phase 7 adds a lightweight approval step on top of scenario analysis.

The flow is:

1. run `portfolio-os-scenarios`
2. review the recommendation and alternatives
3. submit an approval request with:
   - selected scenario
   - decision maker and role
   - rationale
   - acknowledged warning codes
   - handoff contacts
4. freeze the final execution package

Approval status is intentionally simple:

- `approved`
- `rejected`
- `incomplete_request`

The default workflow rule is:

- if blocking findings exist: `rejected`
- if warnings exist and are not acknowledged: `incomplete_request`
- otherwise: `approved`

The final execution package is just a frozen copy of the selected scenario artifacts plus metadata:

- `final_orders.csv`
- `final_orders_oms.csv`
- `final_audit.json`
- `final_summary.md`
- `freeze_manifest.json`

This keeps the approval step transparent and file-based rather than turning the project into a workflow platform.

## Execution Simulation

Phase 8 adds a lightweight local execution simulator on top of the frozen final basket.

The intended flow is:

1. run `portfolio-os-scenarios`
2. review the alternatives and choose one
3. run `portfolio-os-approve`
4. freeze the final execution package
5. run `portfolio-os-execute` on `final_orders_oms.csv`

The simulator is intentionally simple:

- it is local and file-based
- it is not a broker adapter
- it is not a high-frequency matching engine
- it is not a full market simulator

What it does provide:

- bucketed intraday execution under a configurable market-volume curve
- participation-based fill caps using `adv_shares`
- estimated fill rate, partial fills, and residual unfilled risk
- estimated fill price, fee, slippage, and total execution cost
- source artifact hashes that tie the simulation back to the frozen package

The default request format lives in `data/execution_samples/execution_request_example.yaml` and points to `outputs/approval_demo`.

Phase 9 adds an execution calibration layer on top:

- execution requests can still embed a curve directly
- `portfolio-os-execute` can also take `--calibration-profile`
- request overrides win over calibration profiles
- calibration profiles win over execution-profile defaults

Current sample templates live under `config/calibration_profiles/`.

A low-liquidity stress template is also available:

- `config/calibration_profiles/low_liquidity_stress.yaml`

## Import Profiles

Phase 9 adds a lightweight pilot integration layer for mapped CSV inputs.

This keeps the project file-based while making it easier to onboard real client exports that do not already match the native PortfolioOS schema.

Current import-profile support:

- `config/import_profiles/standard.yaml`
- `config/import_profiles/custodian_style_a.yaml`

What an import profile can do:

- map external column names into the standard internal schema
- define defaults for optional fields
- map booleans declaratively
- apply simple numeric scaling such as percentage-to-decimal conversion
- fail fast when required mapped fields are missing

When no import profile is passed, PortfolioOS keeps using the native standard schema exactly as before.

## Data Builders

Phase 10 adds a lightweight data feed preparation layer for pilot users.

The idea is simple:

- builders prepare standard `market.csv`, `reference.csv`, and `target.csv`
- the main PortfolioOS workflow then consumes those files unchanged
- the provider layer is replaceable
- the default provider is offline `mock`

This is intentionally not a data platform:

- no database
- no data lake
- no hidden ETL engine
- no live API dependency required for tests or demos

Today the layer is built around a small provider abstraction:

- `get_daily_market_snapshot(tickers, as_of_date)`
- `get_reference_snapshot(tickers, as_of_date)`
- `get_index_weights(index_code, as_of_date)`

That means future pilot integrations can add Tushare, AkShare, or JoinQuant-style providers later without changing the standard builder outputs.

Current providers:

- `mock`
- `tushare`

Tushare token priority:

1. `--provider-token`
2. environment variable `TUSHARE_TOKEN`

The token itself is never written into manifests. Only the token source is recorded.

Tushare account permissions still matter:

- `market` is designed to work with a conservative fallback when `stk_limit` is unavailable
- `reference` can fall back to `bak_basic` when `stock_basic` or `daily_basic` is constrained
- `target` still depends on `index_weight` access

## Real Feed Status

Current real-feed status is intentionally split into:

- fully available real-feed path
- permission-dependent enhancement path

Fully available real-feed pilot path today:

- client provides:
  - `holdings.csv`
  - `target.csv`
  - `portfolio_state.yaml`
- Tushare provides:
  - `market.csv`
  - `reference.csv`

This means PortfolioOS already supports a real static pilot workflow even when `index_weight` is unavailable.

Permission-dependent enhancement path:

- Tushare `index_weight` for automatic `target.csv` construction
- full one-shot `portfolio-os-build-snapshot` with provider-built target

If `index_weight` is unavailable:

- `market` and `reference` can still be built
- the manifests will say the target step is permission-limited
- the recommended next step is to continue with client-provided `target.csv`

## How To Prepare PortfolioOS Inputs From Tickers Or An Index

Use the builders in three steps:

1. Put the working ticker list into `data/sample/tickers.txt` or a simple CSV.
2. Run `portfolio-os-build-market` and `portfolio-os-build-reference`.
3. Run `portfolio-os-build-target` from an index code, then feed the generated files into `portfolio-os`.
4. If you want a one-shot package, run `portfolio-os-build-snapshot` instead and consume the bundled `market.csv`, `reference.csv`, and `target.csv`.

Recommended real pilot path when target permissions are limited:

1. build `market.csv` from Tushare
2. build `reference.csv` from Tushare plus optional local overlay
3. use client-provided `target.csv`
4. run `portfolio-os`

The reference builder also supports a local overlay CSV for desk-owned flags such as:

- `blacklist_buy`
- `blacklist_sell`
- `manager_aggregate_qty`

This keeps the provider layer responsible for base static data, while letting the local workflow add mandate-specific overlays without a database.

## From Static Snapshot Build To Rebalance Run

One complete local chain looks like this:

1. build a static snapshot bundle

```bash
py -3.11 -m poetry run portfolio-os-build-snapshot ^
  --tickers-file data/sample/tickers.txt ^
  --index-code 000300.SH ^
  --as-of-date 2026-03-23 ^
  --provider mock ^
  --reference-overlay data/sample/reference_overlay_example.csv ^
  --output-dir data/generated/snapshot_mock
```

2. run the main PortfolioOS rebalance on the locked snapshot

```bash
py -3.11 -m poetry run portfolio-os ^
  --holdings data/sample/holdings_example.csv ^
  --target data/generated/snapshot_mock/target.csv ^
  --market data/generated/snapshot_mock/market.csv ^
  --reference data/generated/snapshot_mock/reference.csv ^
  --portfolio-state data/sample/portfolio_state_example.yaml ^
  --constraints config/constraints/public_fund.yaml ^
  --config config/default.yaml ^
  --execution-profile config/execution/conservative.yaml ^
  --output-dir outputs/demo_run_built ^
  --skip-benchmarks
```

This is the current Phase 11 static-data story:

- build
- lock
- hash
- consume

without introducing a database or turning PortfolioOS into a data platform.

If the snapshot bundle cannot build `target.csv` because `index_weight` is permission-limited, the recommended continuation path is:

1. keep the generated `market.csv` and `reference.csv`
2. supply `target.csv` from the client or PM workflow
3. run `portfolio-os` on that mixed static package

## Handoff Checklist

Phase 9 also adds a file-based `handoff_checklist.md` for pilot-style workflow review.

This is not a task system. It is a lightweight Markdown checklist that helps PM, trader, risk, and compliance confirm:

- which scenario or frozen package is being handed off
- whether blocking findings are zero
- whether warnings were acknowledged
- whether blocked trades exist
- whether execution simulation has been reviewed
- whether residual partial-fill or unfilled risk has been accepted

## What Makes This Look Like A Real Workflow Tool

PortfolioOS is still an MVP, but this is what already makes it feel closer to a real buy-side workflow tool:

- Template switching: the same CLI can run public-fund, private-fund, or quant-fund style mandates through one shared schema.
- Structured findings: every finding now carries a code, category, severity, rule source, blocking flag, and repair status.
- OMS-friendly export: besides `orders.csv`, the CLI also emits `orders_oms.csv` for easier handoff into existing trading workflows.
- Audit trail: the audit package keeps the exact inputs, resolved constraint snapshot, structured findings, orders, benchmark comparisons, and export-readiness context.

## Data Quality And Trust

PortfolioOS now separates data-quality issues into two buckets:

- fail fast issues: the run stops because the input is not safe to use
- warning issues: the run continues, but the issue is recorded as a structured `data_quality` finding

Fail fast examples:

- duplicate tickers in holdings, target, market, or reference
- negative holdings quantities
- target weights above 1 or total target weight above 1
- non-positive `close`, `vwap`, or `adv_shares`
- negative `available_cash` or `min_cash_buffer`
- missing market coverage for holdings/target tickers
- missing reference coverage or missing industry for holdings/target tickers

Warning examples:

- target weight sum near zero
- extremely concentrated target weights
- abnormal benchmark-weight totals in reference data
- snapshots where nothing is tradable

Why this design:

- truly unsafe inputs should never silently flow into optimization
- softer data issues still matter, but should be visible as structured evidence rather than treated as fatal errors
- the audit package should show not only what the engine did, but also what it had to trust about the inputs

## Business Assumptions

- Board lot size is fixed at 100 shares.
- `available_cash` is current cash available for trading, not historical initial cash.
- `gross_traded_notional = sum(abs(quantity) * estimated_price)`.
- `turnover = gross_traded_notional / pre_trade_nav`.
- Optimization, repair, and exported reports use the same cost formulas:
  - `fee = commission + transfer_fee + sell_stamp_duty`
  - `slippage = price * k * |q| * sqrt(|q| / adv_shares)`
- Any ticker appearing in holdings or target must have complete market data and an industry classification.
- `manager_aggregate_qty` in `reference.csv` excludes the current account position and only represents other related products under the same manager.
- Public-fund style single-name control uses the stricter value of `single_name_max_weight` and `double_ten.single_fund_limit`.
- Benchmark comparison uses a static snapshot only. It is not a historical backtest engine.

## Risk Statement

PortfolioOS is a workflow automation and analysis aid. It does not produce investment advice, does not guarantee trade execution, and does not replace legal, compliance, portfolio management, or execution review. Users must review constraints, cost assumptions, and exported orders before any live trading action.

## Testing

Run all tests:

```bash
py -3.11 -m poetry run pytest
```

## Live Demo Workflow

For a live PM / trader / compliance demo:

1. Use the sample files or replace them with one current holdings snapshot, one target portfolio, one market snapshot, and one reference file.
2. Run the CLI once and open `orders.csv`, `orders_oms.csv`, `summary.md`, and `benchmark_comparison.md`.
3. Start with `orders.csv` for analytical review, then show `orders_oms.csv` as the operations handoff format.
4. Use the summary to explain blocked trades, hard blocks versus warnings, and repair outcomes.
5. Use `benchmark_comparison.md` to explain the business value in plain language: fewer blocked tickets, lower estimated cost, and a reproducible audit trail.
6. Use `audit.json` only as the supporting evidence package after the audience understands the business story.

Second-layer replay demo:

1. After the single-sample run, switch to `portfolio-os-replay` and run the replay suite.
2. Open `suite_summary.md` first, then use the per-sample files in `sample_results/`.
3. Focus on distributions and medians, not any one sample.
4. Position the replay suite as evidence that the process is robust across different static operating conditions.

Third-layer scenario demo:

1. Run `portfolio-os-scenarios` on one shared snapshot.
2. Open `scenario_comparison.md` to show the policy menu and scenario table.
3. Open `decision_pack.md` to explain the recommended scenario under the current workflow score.
4. Use the named alternatives to show that the tool supports trade-off discussion instead of pretending there is only one acceptable answer.

## How To Prove Product Value

Use the project in two steps:

1. Run one live snapshot and show the actual basket, blocked trades, findings, and audit record.
2. Run the replay suite and show the distribution of improvements versus naive and cost-unaware rebalance.

Suggested message:

"PortfolioOS does not only produce a cleaner basket on one example. Across a static replay suite, it shows how often the process lowers estimated cost, reduces avoidable blocked tickets, and keeps a full audit trail."

Example conclusion template:

"Across N static replay samples, PortfolioOS reduced estimated trading cost by median X and turnover by median Y versus naive rebalance."

For OMS handoff, an additional template message works well:

"The analytical basket is for PM and compliance review. The OMS-friendly basket carries account, strategy tag, basket ID, and blocking-check release status so trading can move it into the existing workflow more cleanly."

For trust and explainability, another useful line is:

"The result is not a black box. Inputs are validated, blocked or missing orders have explicit reasons, repaired trades keep repair reasons, and the replay suite shows the behavior across multiple samples rather than one cherry-picked case."

## How To Compare Alternative Rebalance Policies

Use the scenario engine when the conversation changes from "is the workflow useful?" to "which version of the workflow should we use today?"

The scenario recommendation is intentionally transparent:

- lower score is better
- the score is built from target deviation, estimated total cost, turnover, blocked trades, and blocking findings
- the weights are fixed in code and documented in the output

The scenario pack also labels:

- `lowest_cost_scenario`
- `lowest_turnover_scenario`
- `fewest_blocked_trades_scenario`
- `best_target_fit_scenario`

That means the tool does not hide alternatives. It gives one recommended scenario under the current workflow scoring rule and keeps the trade-offs visible for PM, trading, and risk discussion.

## Scenario -> Decision -> Freeze

The project now supports a simple chain from analysis to handoff:

1. `portfolio-os` for one operational basket
2. `portfolio-os-replay` for multi-sample evidence
3. `portfolio-os-scenarios` for same-snapshot policy comparison
4. `portfolio-os-approve` for explicit selection, warning acknowledgement, freeze, and handoff
5. `portfolio-os-execute` for local intraday execution preflight on the frozen basket

This is still a local-file MVP, but it now covers the core progression that a real workflow tool needs:

- compare alternatives
- choose a final path
- record who chose it and why
- freeze the execution package that will be handed off
- simulate how that frozen basket may get filled before any real trading connectivity exists

## How To Onboard A Pilot Client With Local Files

Use the pilot flow in four simple steps:

1. Take the client's existing holdings, target, market, and reference exports as local CSV files.
2. Create or adapt one declarative import profile in `config/import_profiles/`.
3. Run `portfolio-os` with `--import-profile` and confirm the mapped files land in the standard PortfolioOS domain model.
4. Tune execution-preflight assumptions with one calibration profile in `config/calibration_profiles/`, then review `handoff_checklist.md` before any downstream OMS or trader handoff.
5. If the client does not already have PortfolioOS-ready market or reference files, use the new builder CLIs and a provider adapter to prepare them locally first.

This keeps pilot onboarding transparent:

- no database migration
- no custom UI
- no hidden ETL layer
- no broker integration required

The product remains a local-file workflow tool, but it is now easier to align with a design partner's real file exports and execution-preference assumptions.

## MVP Simplifications

- Prices are treated as fixed snapshots using `vwap` when present, otherwise `close`.
- No minimum commission floor is modeled to keep the optimizer convex and stable.
- Manager aggregate 10% uses optional reference fields and produces warnings, not hard optimization blocks.
- The 10-day remediation concept is captured as an audit/report marker only.
- The benchmark suite is static and snapshot-based. It does not model market drift or execution fill uncertainty.
- The replay suite is a batch of static snapshots, not a historical simulator or event-driven backtest.
- Data-quality warnings are heuristic and intentionally lightweight; they are not a full data-governance system.
- Scenario recommendations come from a transparent workflow scoring rule, not from a forecast model or investment recommendation engine.
- Execution simulation uses a static `adv_shares` snapshot plus a simple intraday bucket curve; it does not model queue position, price drift, or order-book microstructure.
- Import profiles are declarative CSV mappings only. They are not a general ETL framework and do not support arbitrary scripts or spreadsheet-specific parsing magic.
- Calibration profiles are static parameter templates. They are not ML execution models and do not fit themselves to realized fills.
- Data builders are local preparation utilities. They are not a data governance platform and do not replace formal market-data operations.
- The sample issuer share counts are scaled for demo readability and not meant to represent actual market capitalization.
