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

The second slice extends the harness with a repeated shuffled-null benchmark:

- `100` shuffled-placebo seeds
- persisted null-distribution artifact:
  - `shuffle_null_distribution.csv`
- summary-level null percentiles for each live expression:
  - `shuffle_null_mean_rank_ic_percentile`
  - `shuffle_null_rank_ic_t_percentile`

## Verification

Targeted verification passed:

```text
python -m pytest tests/test_alpha_discovery_calibration.py tests/test_alpha_qualification.py -q
11 passed, 3 warnings
```

The live runner also completed and wrote artifacts under:

- `outputs/us_residual_momentum_calibration/2026-04-16/`

## Updated Read After Null Benchmark Extension

The harness is live, but the read is still **not** a calibration success.

Current summary:

- best live expression = `RM3_VOL_MANAGED`
  - `mean_rank_ic ~ 0.0329`
  - `rank_ic_t ~ 0.7371`
  - shuffled-null `mean_rank_ic` percentile `~ 70%`
  - shuffled-null `rank_ic_t` percentile `~ 72%`
- strongest single shuffled draw = `CTRL1_SHUFFLED_PLACEBO`
  - `mean_rank_ic ~ 0.0668`
  - `rank_ic_t ~ 1.7694`
  - but this now reads as a high null draw rather than a standalone falsification result:
    - shuffled-null `mean_rank_ic` percentile `~ 92%`
    - shuffled-null `rank_ic_t` percentile `~ 95%`

Other key reads:

- `RM2_SECTOR_RESIDUAL` and `RM1_MKT_RESIDUAL` also fail to separate cleanly from the shuffled null
- `CTRL2_PRE_WINDOW_PLACEBO` and `CTRL3_BASELINE_MIMIC` remain economically nontrivial controls
- no live expression yet clears the stronger claim the calibration family needs:
  - that a mechanism-bearing expression is obviously distinct from the null/control envelope, not just better than one arbitrary placebo seed

## Interpretation

This is exactly the kind of update the calibration family is supposed to force before any primary-family mining opens.

The first-pass concern was:

- one shuffled placebo looked too strong relative to the live expressions

The refined read is more precise:

- a single shuffled placebo draw is too noisy to interpret alone on this short sample
- after moving to a `100`-seed shuffled null, the core problem is not "one placebo got lucky"
- the core problem is that the live expressions still do not sit far enough into the right tail of the null distribution to validate the discovery machine

So the correct conclusion is:

- the calibration harness is now materially better calibrated than the first slice
- but the calibration family is still **not validated**
- because the best live expression is only around the `70-72` percentile of the shuffled null, not at a level that supports a trustworthy family closeout

## Immediate Consequence

The primary family remains blocked.

`A-share state-transition microstructure` should **not** open yet.

The next justified work stays inside calibration and should focus on:

1. extending or otherwise strengthening the calibration sample so the null becomes more informative,
2. adding deeper adversarial controls / mechanism-breaking checks rather than relying on one placebo draw,
3. and only then asking whether the calibration family can produce a trustworthy closeout.

## Status Label

Current status:

`calibration harness live with shuffled-null benchmark; calibration family not yet validated`
