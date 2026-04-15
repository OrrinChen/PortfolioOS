# Paper Calibration Runbook

## Purpose

This runbook describes how to use the PortfolioOS paper calibration sprint as a platform-validation tool.

The calibration line is not an alpha test. It is a controlled execution and simulator-validation loop built around a deliberately neutral strategy.

## Modes

### 1. Dry-Run Contract Check

Use this first to validate the target/export/report contract without sending broker orders.

```powershell
python -m portfolio_os.api.cli paper-calibration --ticker SPY --output-dir C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_demo
```

Expected outputs:

- `target.csv`
- `paper_calibration_manifest.json`
- `paper_calibration_payload.json`
- `paper_calibration_report.md`

Use this mode whenever:

- the sprint is newly set up
- code changed in target generation or reporting
- you want to verify artifact shapes before a real paper run

### 2. Neutral Alpaca Paper Run

Use this only after the dry-run path is verified and Alpaca paper credentials are configured.

```powershell
python -m portfolio_os.api.cli paper-calibration --ticker SPY --quantity 1 --submit-paper --output-dir C:\Users\14574\Quant\PortfolioOS\outputs\paper_calibration_live
```

Required environment variables:

- `ALPACA_API_KEY`
- `ALPACA_SECRET_KEY`

Expected outputs:

- all dry-run artifacts
- `pretrade_reference_snapshot.csv`
- `alpaca_fill_manifest.json`
- `alpaca_fill_orders.csv`
- `alpaca_fill_events.csv`
- `broker_account_before.json`
- `broker_account_after.json`
- `broker_positions_before.csv`
- `broker_positions_after.csv`
- `reconciliation_report.json`
- `execution_result.json`
- `execution_result.csv`
- `alpaca_fill_summary.md`

## What To Check

After a paper run, inspect these first:

1. `paper_calibration_report.md`
2. `pretrade_reference_snapshot.csv`
3. `alpaca_fill_manifest.json`
4. `reconciliation_report.json`
5. `alpaca_fill_orders.csv`
6. `alpaca_fill_events.csv`

The first review questions are:

- Did the run capture a dedicated pre-trade reference snapshot?
- Did orders submit successfully?
- Were fills complete, partial, or rejected?
- Did the broker lifecycle look normal?
- Did realized behavior look meaningfully different from expected assumptions?

## Interpretation Rules

Use the artifacts to answer execution questions, not alpha questions.

Good uses:

- estimating fill rate realism
- measuring reference-to-fill drift from a dedicated pre-trade snapshot
- checking timeout/partial-fill frequency
- validating order lifecycle assumptions
- comparing simulator assumptions with real paper outcomes

Bad uses:

- inferring whether a strategy has alpha
- treating a good paper execution day as alpha confirmation
- reopening unrelated US event-alpha research because paper fills looked clean

## Failure Attribution

If a paper run looks bad, classify the problem before changing code:

### A. Contract failure

Examples:

- missing artifacts
- malformed files
- CLI path breaks

Likely fix area:

- target generator
- workflow orchestration
- artifact writing

### B. Broker/runtime failure

Examples:

- credential issues
- rejected orders
- missing telemetry
- lifecycle polling problems

Likely fix area:

- Alpaca adapter
- paper runner assumptions
- environment setup

### C. Calibration mismatch

Examples:

- fill rate much lower than expected
- partial fills much more frequent than simulator assumes
- reconciliation repeatedly diverges

Likely fix area:

- simulator defaults
- cost/TCA assumptions
- execution constraint assumptions

## Notes

- Start with `SPY`
- Keep quantity small for initial runs
- Do not attach alpha logic to this line until the calibration path is trusted
- Treat this runbook as a platform-validation tool, not a research shortcut
