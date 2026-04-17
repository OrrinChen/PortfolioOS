# US Residual Momentum Calibration Setup Note (2026-04-16)

## Purpose

This note records the first executable calibration-family slice after the v2 discovery charter was approved.

It is not a family closeout.
It is a setup read whose purpose is to answer a narrower question:

> Is the calibration-family harness live enough to start testing whether the discovery machine can distinguish live expressions from controls?

## What Was Implemented

The following were added:

- `src/portfolio_os/alpha/discovery_calibration.py`
- `scripts/run_us_residual_momentum_calibration.py`
- `tests/test_alpha_discovery_calibration.py`

The first slice includes:

- a frozen calibration registry with `3` live expressions and `3` controls
- control builders for:
  - shuffled placebo
  - pre-window placebo
  - baseline mimic
- monthly next-period evaluation on the existing expanded-US returns panel
- artifact writing:
  - `registry.csv`
  - `per_date_metrics.csv`
  - `summary.csv`
  - `summary.json`
  - `note.md`

## Verification

Targeted verification passed:

```text
python -m pytest tests/test_alpha_discovery_calibration.py tests/test_alpha_qualification.py -q
9 passed, 3 warnings
```

The live runner also completed and wrote artifacts under:

- `outputs/us_residual_momentum_calibration/2026-04-16/`

## First Read

The harness is live, but the read is **not** yet a calibration success.

Current summary:

- best live expression = `RM3_VOL_MANAGED`
  - `mean_rank_ic ~ 0.0329`
  - `rank_ic_t ~ 0.7371`
- strongest control = `CTRL1_SHUFFLED_PLACEBO`
  - `mean_rank_ic ~ 0.0668`
  - `rank_ic_t ~ 1.7694`

Other key reads:

- `RM2_SECTOR_RESIDUAL` has the best live mean spread, but still weak significance
- `CTRL2_PRE_WINDOW_PLACEBO` is also not weak enough
- none of the live expressions yet dominate the controls clearly

## Interpretation

This is exactly the kind of read the calibration family is supposed to surface.

The correct conclusion is **not**:

- residual momentum is dead
- or the charter should jump to the primary family anyway

The correct conclusion is:

- the calibration harness is operational,
- but the discovery machine is not yet well-calibrated enough to trust a family winner decision,
- because at least one placebo-style control is still too strong relative to the live expressions.

## Immediate Consequence

The primary family remains blocked.

`A-share state-transition microstructure` should **not** open yet.

The next justified work stays inside calibration and should focus on:

1. tightening control interpretation,
2. improving the calibration evaluator so placebo controls are more clearly separated from mechanism-bearing expressions,
3. and only then asking whether the calibration family can produce a trustworthy closeout.

## Status Label

Current status:

`calibration harness live; calibration family not yet validated`
