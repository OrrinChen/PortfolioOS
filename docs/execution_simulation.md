# Execution Simulation

PortfolioOS Phase 8 adds a lightweight local execution simulator for frozen order baskets.

The simulator is designed for preflight and demo use:

- it reads a frozen or scenario-level OMS basket
- it reconstructs the needed market and cost inputs from the source audit package
- it simulates intraday fills under a simple bucketed volume curve
- it outputs per-order and portfolio-level execution estimates

It is not a broker adapter, not a live execution engine, and not a full market simulator.

## Request Format

Example request:

```yaml
name: demo_execution_request
description: Simulate intraday execution for a frozen final basket
artifact_dir: outputs/approval_demo
input_orders: final_orders_oms.csv
audit: final_audit.json
market: data/sample/market_example.csv
portfolio_state: data/sample/portfolio_state_example.yaml
execution_profile: config/execution/conservative.yaml
calibration_profile: config/calibration_profiles/balanced_day.yaml
market_curve:
  buckets:
    - label: open
      volume_share: 0.25
      slippage_multiplier: 1.2
    - label: mid_morning
      volume_share: 0.2
      slippage_multiplier: 1.0
    - label: midday
      volume_share: 0.1
      slippage_multiplier: 0.9
    - label: afternoon
      volume_share: 0.2
      slippage_multiplier: 1.0
    - label: close
      volume_share: 0.25
      slippage_multiplier: 1.15
simulation:
  mode: participation_twap
  bucket_count: 5
  allow_partial_fill: true
  force_completion: false
  max_bucket_participation_override: null
```

Rules:

- `artifact_dir` is required.
- `input_orders` is required and is usually resolved relative to `artifact_dir`.
- `audit` is optional; if omitted, the simulator auto-detects `final_audit.json` or `audit.json` under `artifact_dir`.
- `market` is optional; if omitted, the simulator resolves market path from the audit payload.
- `market_curve` is optional when a calibration profile is available.
- `simulation.mode` supports:
  - `participation_twap`: sequential bucket-filling under the resolved participation cap
  - `impact_aware`: cost-aware bucket allocation that prefers cheaper liquidity buckets when capacity is available
- `simulation.bucket_count` must match the number of resolved curve buckets when provided.
- `market_curve.buckets[*].volume_share` must sum to `1.0` whenever an inline curve is supplied.
- `force_completion` controls whether the final bucket may ignore the normal bucket cap and fill the residual.

## Source Recovery

The simulator expects the artifact directory to contain:

- `final_orders_oms.csv` or another OMS basket named by the request
- `final_audit.json` or `audit.json`

From the audit payload it recovers:

- the original `market.csv` path
- fee parameters
- slippage parameter `k`
- trading lot size
- the source participation limit when no override is provided

If present, `freeze_manifest.json` is also hashed and carried into the execution report so the output is visibly linked to the approval/freeze step.

If the source audit payload includes an `import_profile`, the execution simulator also reloads that profile before reading the source `market.csv`. This keeps the execution-preflight step compatible with mapped pilot inputs.

## Calibration Profiles

Execution simulation can now be configured in two ways:

1. inline in the request file
2. through a calibration profile

Calibration profiles live under `config/calibration_profiles/`.

Current templates:

- `liquid_midday.yaml`
- `balanced_day.yaml`
- `tight_liquidity.yaml`
- `low_liquidity_stress.yaml`

Priority:

1. request overrides
2. selected calibration profile
3. execution-profile default calibration

Selection precedence for the profile itself:

1. `--calibration-profile`
2. request `calibration_profile`
3. execution profile `default_calibration_profile`
4. built-in fallback `balanced_day`

The resolved selection and overridden fields are written into `execution_report.json` under `resolved_calibration`.

## Bucket Curve Structure

Each bucket contains:

- `label`: business-facing bucket name
- `volume_share`: share of day volume assigned to the bucket
- `slippage_multiplier`: relative bump applied to the base slippage estimate for that bucket

Example interpretation:

- `open` and `close` often carry larger volume but may have slightly higher execution friction
- `midday` often carries lighter volume and may have lower slippage pressure

This is intentionally heuristic and demo-friendly.

## Fill Rules

For `participation_twap`:

1. daily available volume is `adv_shares`
2. bucket available volume is `adv_shares * volume_share`
3. bucket fill cap is `bucket_available_volume * participation_limit`
4. the cap is floored to the current lot size
5. actual bucket fill is:
   - remaining quantity, if the remaining order fits inside the cap
   - the cap, if partial fills are allowed and the remaining order is larger
   - zero, if partial fills are disabled and the basket cannot complete under normal caps
   - the remaining quantity in the final bucket when `force_completion=true`

For `impact_aware`:

1. the same bucket capacities are computed from `adv_shares`, `volume_share`, and `participation_limit`
2. PortfolioOS assigns more quantity to buckets with lower expected impact
3. expected impact is driven by:
   - bucket volume
   - slippage multiplier
   - the configured power-law slippage function
4. if total bucket capacity is insufficient:
   - residual quantity remains when `force_completion=false`
   - the final bucket absorbs the residual when `force_completion=true`

Participation limit priority:

1. `simulation.max_bucket_participation_override`
2. order-level participation column if present
3. optional `participation_limit` field in the execution profile YAML if present
4. source audit constraint participation limit

## Price And Cost Treatment

Base execution price:

- `estimated_price` from the OMS basket

Slippage:

- PortfolioOS keeps the frozen slippage definition:
  - `slippage = price * k * |q| * sqrt(|q| / adv_shares)`
- execution simulation applies the same base formula per bucket
- bucket slippage is then multiplied by the bucket `slippage_multiplier`

Mode difference:

- `participation_twap` uses the same slippage formula after a sequential bucket fill plan
- `impact_aware` uses the same slippage formula inside a cost-aware bucket allocator

Fill price:

- buy:
  - `fill_price = base_price * (1 + slippage_bump)`
- sell:
  - `fill_price = base_price * (1 - slippage_bump)`
- `slippage_bump = bucket_slippage / (base_price * filled_quantity)`

Fees:

- PortfolioOS keeps the frozen fee definition:
  - `fee = commission + transfer_fee + sell_stamp_duty`

Per-order totals:

- `estimated_total_cost = estimated_fee + estimated_slippage`

Portfolio-level fill rate:

- `fill_rate = (total_ordered_notional - total_unfilled_notional) / total_ordered_notional`
- this keeps fill rate on a reference-notional basis even when buy-side slippage makes actual filled notional slightly larger than the original estimate

## Output Files

The execution output directory writes:

- `execution_report.json`
- `execution_report.md`
- `execution_fills.csv`
- `execution_child_orders.csv`
- `handoff_checklist.md`
- `run_manifest.json`

`execution_report.json` includes:

- request metadata
- bucket curve definition
- per-order execution results
- portfolio-level summary
- optional stress-test comparison summary (`stress_test`)
- source artifact hashes

`execution_report.md` highlights:

- fill rate
- filled / partial / unfilled order counts
- filled and unfilled notional
- total cost
- selected calibration profile and request overrides
- worst 3 execution outcomes
- optional baseline-vs-stress comparison
- one-line conclusion

`handoff_checklist.md` highlights:

- selected scenario or frozen package source
- whether blocking findings are zero
- whether warnings were acknowledged
- whether execution simulation has been reviewed
- whether partial-fill or unfilled risk remains
- trader / reviewer / compliance contacts when available

## Current MVP Simplifications

The execution simulator is intentionally simplified:

- two modes only: `participation_twap` and `impact_aware`
- static one-day `adv_shares` liquidity input
- fixed bucket curve from the request file
- no queue modeling
- no venue routing
- no spread forecasting
- no price drift or dynamic intraday alpha
- no broker acknowledgements, cancels, or rejects

That is deliberate. The goal is to answer:

"If we send this frozen basket today under a simple participation/TWAP style schedule, what is likely to fill, what may remain, and what residual execution risk should PM / trading / risk discuss before live handoff?"
