# Calibration Machine Failure Taxonomy (2026-04-16)

## Purpose

This document records **known calibration-machine failure modes / boundary behaviors** discovered during Alpha Discovery v2 calibration.

It is not a family scorecard.
It does not decide whether a family has alpha.
It exists to answer a narrower question:

> When the discovery machine emits an ambiguous or suspicious read, what exact pattern did we observe, how should it be classified, and what repair order follows from that classification?

## Scope

This taxonomy sits above any single family.
Entries here are allowed to originate from one calibration family, but they are recorded only as **machine-layer patterns**.

Two layers matter:

1. **Expression-generation layer**
   - examples:
     - residualization,
     - scaling,
     - baseline stripping,
     - transformation interactions

2. **Signal-validation layer**
   - examples:
     - placebo envelope,
     - null percentiles,
     - mechanism-breaking controls,
     - winner-selection reads

## Use Rule

When a future read resembles an entry below:

1. classify the pattern against this taxonomy first,
2. avoid promoting the read into a family-alpha claim,
3. repair the machine in the order specified by the entry,
4. only reopen family-level interpretation after the machine-layer read is no longer taxonomy-consistent.

## Entry States

- `observed`
  - seen once with a complete diagnostic signature
  - recorded, but not yet promoted to a generalized machine law
- `replicated`
  - observed in more than one family or more than one calibration context
- `retired`
  - machine changes have removed the behavior or made the entry obsolete

---

## FM-001: Null-Consistent Residualization Strengthening

### Status

- `observed`

### Observed In

- calibration family:
  - `US residual momentum / residual reversal`
- expression:
  - `RM3_VOL_MANAGED`
- canonical artifact root:
  - `outputs/us_residual_momentum_calibration/2026-04-16/`
- first committed read:
  - `301ce75`

### Layer Classification

- primary layer:
  - `signal-validation`
- interacting layer:
  - `expression-generation`

This is recorded as a **cross-layer boundary behavior**:

- residualization appears to strengthen the read,
- but validation shows the strengthening is still plausibly null-consistent.

### Diagnostic Signature

The entry is considered matched when all or most of the following hold:

1. **Live residualized read appears stronger than raw**
   - example pattern:
     - residualized `rank_ic_t` > raw `rank_ic_t`

2. **The stronger residualized read still sits inside a realistic placebo/null envelope**
   - observed read here:
     - live residualized `rank_ic_t = 1.0258`
     - placebo-null percentile `= 81%`
     - `19 / 100` placebo seeds meet or exceed the live residualized `rank_ic_t`

3. **The strengthening is not broad across baseline exposure**
   - observed read here:
     - `low` baseline-exposure tercile:
       - `rank_ic_t = -1.9107`
       - mean spread `= -3.6069%`
     - `mid` baseline-exposure tercile:
       - `rank_ic_t = 1.0498`
       - mean spread `= 6.4612%`
     - `high` baseline-exposure tercile:
       - `rank_ic_t = 1.4632`
       - mean spread `= 7.7576%`

4. **The read can be mis-seen as incremental-alpha evidence if the placebo envelope is ignored**
   - this is the key interpretive hazard

### Correct Interpretation

When FM-001 is matched, the correct working interpretation is:

- the machine has produced a residualized strengthening read that is **still null-consistent**
- the read should be treated as a **machine-boundary / calibration issue**
- the read does **not** justify a stronger family prior
- the read does **not** justify a family-level winner claim

### Incorrect Interpretation

FM-001 explicitly prohibits the following conclusions:

- "the family has incremental alpha after baseline stripping"
- "residualization has revealed the true winner"
- "the raw family failed but the residualized family survives"
- "this is enough evidence to move directly into mechanism rescue or family reopen"

### Why This Matters

FM-001 is dangerous because it creates a plausible false-positive story:

- the expression looks stronger after residualization,
- the strengthening is directionally aligned with the desired narrative,
- and without the placebo envelope it can be mistaken for genuine separation.

The entry exists to block exactly that mistake.

### Required Repair Order

When FM-001 is active, the next slices must proceed in this order:

1. **Adversarial-layer hardening first**
   - goal:
     - improve the reliability and interpretability of null-envelope reads
   - examples:
     - stricter placebo calibration,
     - stronger negative controls,
     - more explicit null percentiles / envelopes for boundary cases

2. **Residualization-layer review second**
   - goal:
     - inspect whether residualization itself is creating tercile-asymmetric amplification
   - examples:
     - transformation audit,
     - leverage/outlier sensitivity,
     - alternative residualization protocols

3. **Only after 1 and 2**
   - reconsider whether any family-level interpretation is justified

### Immediate Program Consequence

Under FM-001:

- calibration remains open
- primary-family mining remains blocked
- family-alpha conclusions remain out of scope

### Exit Condition

FM-001 can be downgraded or retired only if a later slice shows one of the following:

1. the residualized strengthening moves clearly outside the calibrated null envelope, **and**
2. the strengthening is no longer concentrated only in favorable baseline-exposure buckets,

or

3. machine changes remove the strengthening artifact entirely and the pattern no longer recurs.

Until then, FM-001 stays active as a known calibration-machine boundary behavior.
