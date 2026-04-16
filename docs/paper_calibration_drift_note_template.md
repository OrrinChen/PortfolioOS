# Paper Calibration Drift Note Template

## Purpose

Use this note to summarize repeated neutral paper-calibration tranches without over-interpreting early samples.

This template is intentionally pre-registered:

- before `N >= 30`, the note should make process statements only
- before `N >= 30`, the note should not claim a stable drift regime
- before `N >= 30`, the note should not infer simulator bias terms from the paper venue

## Inputs

Primary inputs:

- `drift_summary.md`
- `drift_observations.csv`
- per-run `pretrade_reference_snapshot.csv`
- per-run `alpaca_fill_manifest.json`
- per-run `reconciliation_report.json`

## Sample Inventory

- Observation count:
- Run count:
- Session roots included:
- Unique tickers:
- Reference source mix:
- Fallback reference count:
- Time-of-day buckets represented:

Session inventory table:

| Session | Date | Runs | Primary bucket coverage | Notes |
|---|---|---:|---|---|
| session1 |  |  |  |  |
| session2 |  |  |  |  |

## Process Verification

Questions to answer:

- Did dedicated pre-trade reference capture remain on the happy path?
- Did any run fall back to a secondary reference source?
- Did any run show partial fills, rejects, or reconciliation mismatches?
- Did repeat-mode artifact writing remain complete for every run?

Record here:

- reference capture verdict:
- fill-path verdict:
- reconciliation verdict:
- artifact completeness verdict:

## Descriptive Outputs

These are descriptive summaries only. Do not attach a drift-regime conclusion here when `N < 30`.

### Drift Distribution

- median drift (bps):
- mean drift (bps):
- IQR drift (bps):
- p05 / p95 drift (bps):

### Relative To Spread

- median spread (bps):
- median half-spread (bps):
- median drift / half-spread:
- IQR drift / half-spread:

### Latency Relationship

- latency sample count:
- slope (bps / second):
- correlation:
- any obvious outliers:

### Time-Of-Day Conditioning

| Bucket | Count | Median Drift (bps) | Median Spread (bps) | Read |
|---|---:|---:|---:|---|
| 09:30-10:30 |  |  |  |  |
| 10:30-12:00 |  |  |  |  |
| 12:00-14:30 |  |  |  |  |
| 14:30-16:00 |  |  |  |  |

## Interpretation Rules

Apply these rules in order.

### Rule 1: Early-Sample Discipline

If `N < 30`:

- report only that the tranche process is working or failing
- report whether phase coverage is still narrow or already diversified
- do not claim a stable noise floor
- do not claim staleness bias
- do not claim venue-specific drift behavior

### Rule 2: Noise-Floor Candidate

If `N >= 30`, median drift remains sub-bp, and latency relation stays weak:

- working read: the paper venue may be contributing only a small execution noise floor
- next action: treat this as an upper-bound calibration read, not a structural simulator bias term

### Rule 3: Staleness-Bias Candidate

If `N >= 30` and drift rises with capture-to-submit latency:

- working read: pre-trade reference staleness may be a meaningful contributor
- next action: test whether the effect persists across time-of-day buckets and ticker regimes

### Rule 4: Venue-Behavior Candidate

If `N >= 30` and drift frequently exceeds half-spread or concentrates in specific buckets:

- working read: the paper venue may be introducing fill logic that does not map cleanly to reference quotes
- next action: compare against a second ticker regime before adding any simulator adjustment

### Rule 5: Coverage First

If phase coverage is one-sided:

- do not escalate the interpretation
- collect the missing contrasting bucket before concluding anything

## Current Tranche Status

Use this section for the live working read.

- Current status:
- What is known:
- What is not yet knowable:
- Immediate next tranche:

## Session2 Plan

The first follow-up tranche should prioritize phase coverage over raw count.

Recommended goals:

- add at least one `09:30-10:30` observation
- add at least one `12:00-14:30` observation
- avoid stacking every new run into the same late-session bucket as `session1`
- prefer a manually staggered tranche or unequal spacing, not a perfectly regular cadence

Suggested shape:

| Run | Approximate local time | Goal |
|---|---|---|
| run_001 | 09:45 | open-phase reference/fill behavior |
| run_002 | 10:30 | post-open settling behavior |
| run_003 | 12:30 | midday baseline |
| run_004 | 14:00 | bridge bucket before late session |

Stopping rule:

- inspect again around cumulative `N = 10-12`
- if aggregation still looks operationally clean, continue toward `N = 30`
- once `N >= 30`, freeze the first real drift-regime read before extending scope
