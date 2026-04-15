# Paper Calibration Live Run 2026-04-15

## Scope

First neutral Alpaca paper calibration run for the PortfolioOS paper calibration sprint.

- Strategy: `neutral_buy_and_hold`
- Instrument: `SPY`
- Order size: `1` share
- Purpose: validate live paper execution telemetry and simulator-calibration artifact chain

## Artifact Roots

- First raw run:
  - `C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_live_2026-04-15`
- Corrected rerun after workflow fixes:
  - `C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_live_2026-04-15_v2`

Use the `_v2` run as the canonical read.

## Canonical Read (`_v2`)

From:

- [alpaca_fill_manifest.json](/C:/Users/14574/Quant/PortfolioOS/outputs/paper_calibration_live_2026-04-15_v2/alpaca_fill_manifest.json)
- [reconciliation_report.json](/C:/Users/14574/Quant/PortfolioOS/outputs/paper_calibration_live_2026-04-15_v2/reconciliation_report.json)
- [paper_calibration_report.md](/C:/Users/14574/Quant/PortfolioOS/outputs/paper_calibration_live_2026-04-15_v2/paper_calibration_report.md)

Observed:

- order count: `1`
- status mix: `filled = 1`
- fill rate: `100%`
- partial fills: `0`
- rejections: `0`
- timeout cancels: `0`
- requested notional: `696.95`
- filled notional: `696.95`
- average fill price: `696.95`
- reconciliation: `matched_count = 12`, `mismatched_count = 0`

## What This Validated

- The `paper-calibration` CLI can submit a real neutral Alpaca paper order.
- The workflow now writes standard fill telemetry artifacts:
  - broker account before/after
  - broker positions before/after
  - fill manifest
  - fill orders/events
  - reconciliation report
  - execution result
- The workflow fix to carry forward existing broker positions into expected post-trade reconciliation worked.
- Requested notional is no longer silently zero for neutral paper runs.

## Remaining Limitations

- `reference_price` and `estimated_price` are currently inferred from the available position/fill state, not from a dedicated pre-trade market snapshot.
- Report-level deviation language is still coarse:
  - good enough for platform validation
  - not yet a full simulator-vs-paper slippage attribution pack
- The run is too small to say anything about partial-fill behavior or stress execution.

## Next Useful Step

Run a second neutral paper tranche with slightly broader coverage, still without alpha semantics:

- either `SPY` repeated over multiple days
- or a tiny deterministic 2-ticker neutral basket

The goal is to observe whether:

- fill rate stays near `100%`
- latency remains stable
- partial fills/rejections remain rare
- simulator assumptions remain directionally reasonable under repeated live runs
