# US Left-Tail B2 Cohort Design

**Date:** 2026-04-16  
**Workspace:** `C:\Users\14574\Quant\PortfolioOS`  
**Status:** Approved-for-implementation

## Goal

Rewrite Layer B2 so it no longer assumes a discrete low-frequency crash mode.

The new objective is to test whether the deployable US `operational 21d top_bottom_spread` exhibits:

- a broad left-tail vulnerability with a consistent cross-sectional pattern
- a two-regime left tail where sharper bad months and milder bad months differ in kind
- or no stable bad-month pattern beyond ordinary noise

## Why B2 Needed Reframing

Layer B1 already changed the problem statement.

The current evidence no longer supports "three canonical crash months define one identified mode." Instead:

- the left tail is broader than three months
- standard style factors explain very little of the deployable 21d object
- the practical research object is the raw deployable spread series, not a residualized synthetic series

That means the old `N=3 crash-month characterization` frame is no longer valid. B2 must now study **bad-month cohorts**, not just a handful of extreme dates.

## Research Object

The ranking basis is frozen as the raw deployable object:

- `operational 21d top_bottom_spread`

This ranking is **not** residualized before cohort construction because:

- B4 already showed standard-factor `R^2` is too small for residualization to materially reorder months
- the deployable platform object is the raw 21d spread, not a residual-only construct

## Cohort Definition

### Primary Cohort

Use the **worst quintile** of monthly `operational 21d top_bottom_spread`.

For the current sample length, this implies:

- total months: approximately `234`
- worst-quintile cohort size: `47`

### Inner / Outer Split

Split the worst quintile into:

- `outer half`: the worse `23` months
- `inner half`: the remaining `24` months

This split is frozen so the study can answer:

- whether sharper left-tail months and milder bad months are the same mechanism at different intensity
- or whether the left tail contains distinct sub-regimes

## Null Benchmark

Each bad-month comparison must be anchored against a non-bad benchmark.

### Bootstrap Design

For each comparison that involves `outer half` or `inner half`, build a non-bad benchmark by bootstrap sampling from months outside the worst quintile.

Frozen bootstrap details:

- iterations: `5000`
- sampling: without replacement inside each iteration
- cohort size: matched to the target bad-month cohort (`23` or `24`)
- random seed: fixed and written into output metadata

### Required Comparisons

For each cross-sectional dimension, report:

- `outer vs inner`
- `outer vs non-bad bootstrap`
- `inner vs non-bad bootstrap`

This null benchmark is required because the study must distinguish among three worlds:

1. `outer` and `inner` are similar to each other and both differ from normal months
2. only `outer` differs from normal months, while `inner` looks ordinary
3. neither `outer` nor `inner` differs meaningfully from normal months

## Temporal Confound Check

Before interpreting cross-sectional similarity, B2 must report whether `outer` and `inner` are temporally separated.

The minimum required outputs are:

- yearly histogram or yearly count table
- median year
- IQR of year

If `outer` and `inner` are materially separated in calendar time, the note must explicitly say that time composition is a confound and lower the confidence of any same-type claim.

## Cross-Sectional Dimensions

### 1. Size Bucket

Compute a stock-level historical market-cap proxy at each month end:

- `market_cap_proxy = month_end_adjusted_close * historical_shares`

`historical_shares` comes from `yfinance.Ticker.get_shares_full()`.

Size buckets are stock-level quintiles formed cross-sectionally within each month.

#### Coverage Sanity Check

Before relying on size buckets, B2 must report historical-shares coverage, especially for pre-2010 months and focus left-tail dates. If coverage is materially incomplete:

- the report must quantify the gap
- the affected rows must be marked
- any fallback must be explicitly labeled as fallback

### 2. Sector / Industry

Use current static security labels as a first-pass proxy.

Required caveat language:

- sector and industry labels are taken as of analysis date
- those labels are applied to all historical months
- this ignores historical reclassification and business transitions

This dimension is acceptable for hypothesis-level B2 work but must not be over-interpreted as point-in-time sector history.

### 3. Pre-Crash Return Bucket

Stock-level trailing `63` trading-day return, bucketed into cross-sectional quintiles at the bad month start.

### 4. Pre-Crash Volatility Bucket

Stock-level trailing `63` trading-day realized volatility, bucketed into cross-sectional quintiles at the bad month start.

### 5. Active-Leg Concentration

Report concentration for **both** legs:

- top-leg `HHI`
- bottom-leg `HHI`
- top-leg `effective N`
- bottom-leg `effective N`

HHI must be computed on per-stock contribution shares within the relevant leg.

For a leg loss decomposition:

- compute each stock's contribution to that leg's month return
- convert to share of total absolute leg loss
- compute `HHI = sum(share_i^2)`
- compute `effective N = 1 / HHI`

#### Small-Loss Guard

If a leg's total absolute loss is below `0.5%`, concentration metrics for that leg are set to `NaN` and excluded from cohort aggregation.

This guard avoids unstable concentration statistics in months where the leg is not meaningfully under stress.

### 6. Long / Short Attribution

Represent each month with the fixed vector:

- `top_leg_return`
- `bottom_leg_return`
- `spread = top_leg_return - bottom_leg_return`

Gap calculations use one frozen distance metric:

- `L1 distance`

## Similarity Metrics

B2 does **not** pre-register hard acceptance thresholds. It pre-registers a reporting protocol.

For each dimension:

- report absolute similarity or distance
- report the same statistic relative to the non-bad bootstrap distribution
- report the bootstrap percentile of the observed bad-month comparison

Suggested metric mapping:

- sector / industry: rank correlation of share distributions
- size / vol / return buckets: `L1 distance`
- concentration metrics: difference in cohort-level summary distributions
- long / short attribution vector: `L1 distance`

## Interpretation Rules

B2 must use descriptive, non-overclaiming language.

Allowed phrasing:

- `consistent with`
- `suggests`
- `does not suggest`
- `pattern heterogeneity`
- `same-type pattern is not supported`

Disallowed phrasing:

- `identified mechanism`
- `characterized mode`
- `established trigger`

Even if outer and inner are similar, the ceiling remains:

- `consistent with a shared left-tail pattern; does not rule out multiple overlapping mechanisms`

## B3 Gate

Macro conditioning is opened only if:

- worst-quintile structure is coherent
- and `outer vs inner` also looks same-type under the null-benchmarked comparisons

If `outer` and `inner` are not same-type, B3 does **not** proceed in the original form.

A possible fallback is:

- separate macro conditioning for `outer` and `inner`

But that fallback is explicitly deferred until B2 results exist.

## Outputs

B2 should produce a compact, reviewable artifact set:

- one cohort membership table
- one temporal-distribution summary
- one bootstrap metadata file
- one cross-sectional comparison table per dimension
- one markdown note summarizing:
  - whether bad months differ from normal months
  - whether outer and inner are same-type or different-type
  - where the evidence is limited by sample size or static labels

## Non-Goals

This slice does **not**:

- reopen B3 macro conditioning yet
- design a protection rule
- move to CRSP
- revisit A-share framing
- claim a fully identified crash mechanism

## Success Criteria

B2 is successful if it can answer, in a review-safe way:

1. whether bad months exhibit cross-sectional structure distinct from normal months
2. whether sharper and milder bad months are same-type or different-type
3. whether any observed pattern survives a simple non-bad bootstrap benchmark

If the answer is "no stable pattern," that still counts as success because it prevents the project from inventing a false mechanism.
