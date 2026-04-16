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
- Reference-snapshot rerun after dedicated pre-trade capture:
  - `C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_live_2026-04-15_v3`

Use the `_v3` run as the canonical read.

## Canonical Read (`_v3`)

From:

- [pretrade_reference_snapshot.csv](/C:/Users/14574/Quant/PortfolioOS/outputs/paper_calibration_live_2026-04-15_v3/pretrade_reference_snapshot.csv)
- [alpaca_fill_manifest.json](/C:/Users/14574/Quant/PortfolioOS/outputs/paper_calibration_live_2026-04-15_v3/alpaca_fill_manifest.json)
- [reconciliation_report.json](/C:/Users/14574/Quant/PortfolioOS/outputs/paper_calibration_live_2026-04-15_v3/reconciliation_report.json)
- [paper_calibration_report.md](/C:/Users/14574/Quant/PortfolioOS/outputs/paper_calibration_live_2026-04-15_v3/paper_calibration_report.md)

Observed:

- order count: `1`
- status mix: `filled = 1`
- fill rate: `100%`
- partial fills: `0`
- rejections: `0`
- timeout cancels: `0`
- dedicated reference snapshot: `captured_ticker_count = 1`, `fallback_reference_count = 0`
- latest trade price: `697.36`
- quoted mid price: `697.33`
- quoted spread: `0.86 bps`
- requested notional: `697.33`
- filled notional: `697.36`
- average fill price: `697.36`
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
- The workflow now captures a dedicated pre-trade market snapshot before order submission.
- Requested notional now uses the dedicated reference price instead of inferring everything from post-trade position state.

## Remaining Limitations

- Report-level deviation language is still coarse:
  - good enough for platform validation
  - not yet a full simulator-vs-paper slippage attribution pack
- The run is too small to say anything about partial-fill behavior or stress execution.
- The current calibration read still uses a single-name trivial basket and only one observation of quoted spread/slippage.

## Next Useful Step

Run a second neutral paper tranche with slightly broader coverage, still without alpha semantics:

- either `SPY` repeated over multiple days
- or a tiny deterministic 2-ticker neutral basket

The goal is to observe whether:

- dedicated reference snapshots remain complete
- fill rate stays near `100%`
- latency remains stable
- partial fills/rejections remain rare
- reference-to-fill drift remains small and stable under repeated live runs
- simulator assumptions remain directionally reasonable under repeated live runs

## Session1 Repeated Tranche

Same-day repeated neutral tranche now exists at:

- `C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_tranche_2026-04-15_session1`

Aggregated read:

- run count: `3`
- observation count: `3`
- unique tickers: `['SPY']`
- reference source mix: `{'mid_price': 3}`
- fallback reference count: `0`
- median drift: `0.1430 bps`
- drift IQR: `0.5004 bps`
- time-of-day bucket coverage: `14:30-16:00` only

Interpretation discipline for this read:

- this tranche validates repeat-mode artifact capture and aggregation on real multi-run data
- this tranche does not support any drift-regime conclusion
- this tranche should be treated as a process verification milestone, not a simulator-calibration conclusion

Immediate next step:

- run `session2` with phase-diversified coverage, prioritizing open and midday buckets instead of adding more late-session-only observations

## Session2 Exact Run Plan (`2026-04-16`)

Use a new root:

- `C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_tranche_2026-04-16_session2`

Run four separate one-shot commands instead of one repeated loop so that phase coverage can be controlled manually.

### `run_001` (`09:45` ET)

```powershell
python -m portfolio_os.api.cli paper-calibration --ticker SPY --quantity 1 --submit-paper --output-dir C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_tranche_2026-04-16_session2\run_001
```

### `run_002` (`10:30` ET)

```powershell
python -m portfolio_os.api.cli paper-calibration --ticker SPY --quantity 1 --submit-paper --output-dir C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_tranche_2026-04-16_session2\run_002
```

### `run_003` (`12:30` ET)

```powershell
python -m portfolio_os.api.cli paper-calibration --ticker SPY --quantity 1 --submit-paper --output-dir C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_tranche_2026-04-16_session2\run_003
```

### `run_004` (`14:00` ET)

```powershell
python -m portfolio_os.api.cli paper-calibration --ticker SPY --quantity 1 --submit-paper --output-dir C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_tranche_2026-04-16_session2\run_004
```

After `run_004`, aggregate the whole session:

```powershell
python -m portfolio_os.api.cli paper-calibration-aggregate --input-root C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_tranche_2026-04-16_session2 --output-dir C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_tranche_2026-04-16_session2\aggregate_session
```

Expected use of the aggregate:

- confirm that open and midday buckets are now represented
- confirm that reference capture and fill artifacts still stay on the happy path
- re-check the aggregator behavior once cumulative observations reach roughly `N = 7`
- if clean, continue toward cumulative `N = 10-12` before the next review stop

Implementation support for that next step now exists:

- repeated runner:
  - `python -m portfolio_os.api.cli paper-calibration --ticker SPY --quantity 1 --submit-paper --output-dir C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_live_batch --repeat 8 --interval-seconds 1800`
- offline aggregation:
  - `python -m portfolio_os.api.cli paper-calibration-aggregate --input-root C:\Users\14574\Quant\PortfolioOS\outputs --output-dir C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_drift_aggregate`
