# US Alpha Core Restart Charter (2026-04-16)

## Purpose

This charter reopens the US alpha line as a tightly scoped factor qualification sprint under the platform roadmap, not as a reopen of the old Branch A debates.

The objective is to find one US mid-cap alpha winner that:

- can be consumed by the current platform without label or optimizer redesign
- shows clear out-of-sample signal quality under the current evaluator
- adds incremental value relative to the frozen Branch A baselines

This is an 8-week time-box with a hard Week 4 stop/go gate.

## Strategic Boundary

### In Scope

- US only
- `rank_500_1500` style mid-cap universe
- factor definition redesign
- factor qualification
- factor combination only after a single-factor winner exists

### Out of Scope

- no A-share mainline work
- no Branch A reopen
- no hybrid label redesign
- no optimizer / multi-period redesign
- no more than 8 total candidates
- no parameter sweeps during qualification

## What "Current Platform Can Consume" Means

Every candidate factor must pass all of the following before it enters Week 2-3 qualification:

1. `Cadence / PIT`
   - the factor can be sampled onto the current monthly / 21d decision grid
   - PIT lag is explicit and reproducible

2. `Coverage`
   - median monthly effective coverage on `rank_500_1500` is at least `70%`

3. `Cost retention`
   - under the current impact-aware cost model, gross-to-net retention is at least `50%`

4. `Capacity hint`
   - removing the least liquid bottom `20%` of names must not collapse the factor completely

Turnover is tracked as a diagnostic, but it is not an admission gate for this sprint.

## Frozen Baselines

Old definitions are frozen and remain comparison baselines only:

- momentum
- value triplet
- ROE
- asset growth
- revision_1m
- 13F family

They are not reopened in original form.

However, new definitions built on old families are allowed to compete, for example:

- residual momentum
- quality-adjusted value
- residual reversal

Week 4 winner decisions must be made relative to the frozen baseline scorecard produced in Week 1.

## Qualification Gates

### Single-Factor Admission Gate

Each candidate must satisfy all of the following:

- OOS rank IC `> 0`
- rank-IC t-stat `>= 2.0`
- alpha-only spread t-stat `>= 2.0`
- median monthly coverage `>= 70%`
- gross-to-net retention `>= 50%`

### Subperiod Stability Gate

Split the OOS period into 3 equal-length subperiods.

A candidate passes only if:

- at least 2 of the 3 subperiods have positive rank IC
- the weakest subperiod has rank IC `>= 0`

### Winner Increment Gate

Relative to the strongest frozen baseline under the same evaluator:

- comparable rank IC or IR improvement must be at least `15%`

### Orthogonality Gate

Orthogonality is measured as the time-series correlation of top-bottom quintile spreads.

A candidate can only advance if:

- correlation with the strongest frozen baseline is `< 0.70`

If correlation is `>= 0.70`, the candidate is treated as insufficiently incremental unless a later finalist process proves otherwise. That proof is not part of Week 4.

### Finalist Robustness Gate (Week 5-8 Only)

Only Week 4 finalists receive:

- bootstrap / robustness work
- deeper cost and capacity checks
- simple package qualification

Bootstrap / purged confidence intervals are explicitly out of scope for the Week 4 first-pass gate.

## Candidate Families

This sprint runs exactly 3 family directions and at most 8 total candidates.

### Family A: Residual Momentum / Residual Reversal

Priority: highest.

Reason:

- fastest path to a proof-of-direction result
- directly tests whether a better momentum-family expression exists under the current evaluator
- also helps explain why the old momentum line failed

Candidates:

- `A1`: market-residual `84/21` momentum
- `A2`: sector-residual `84/21` momentum
- `A3`: vol-managed residual momentum

### Family B: Low-Frequency Microstructure / Liquidity

Reason:

- strongest link to existing market microstructure know-how
- plausible fit for mid-cap cross-sectional inefficiency

Candidates:

- `B1`: Amihud illiquidity level
- `B2`: illiquidity change / shock
- `B3`: abnormal-turnover-conditioned short-term reversal

### Family C: Idiosyncratic Risk / Lottery

Reason:

- economically distinct from the frozen baselines
- plausible mid-cap fit without reopening old event-driven label debates

Candidates:

- `C1`: idiosyncratic volatility
- `C2`: MAX effect / lottery proxy

## Deferred Families

These are explicitly not part of the 4-week qualification sprint:

- analyst disagreement / forecast-error family
- short-interest / borrow fee

Reason:

- higher data-path and cleaning risk under the current time box

## Time Box

### Week 1: Freeze

Required outputs:

- frozen baseline scorecard
- candidate definition sheet for all 8 candidates
- PIT / cadence assumptions frozen
- qualification artifact template frozen
- Week 4 stop/go rules frozen

### Week 2: Family A First

Run residual momentum / reversal first.

Only candidates that pass the admission gate remain alive.

No parameter sweep is allowed.

### Week 3: Families B and C

Run the remaining families under the same evaluator and artifact structure.

No new family may be added during Week 3.

### Week 4: Winner Decision

Only one question is allowed:

`Is there a winner?`

Winner means:

- passes single-factor admission gate
- passes subperiod stability
- beats the strongest frozen baseline by at least `15%`
- passes spread-correlation orthogonality

If no candidate satisfies all four, the sprint closes.

### Weeks 5-8: Finalist Only

Only if Week 4 has a winner:

- run robustness / bootstrap work
- run deeper cost and capacity checks
- run simple package qualification
- produce a closeout note with a clear keep / stop judgment

If Week 4 has no winner, Weeks 5-8 are not used to keep searching.

## Sidecar Policy Tweak

`min_evaluation_dates: 20 -> 19` is not the mainline.

It is allowed only as a sidecar engineering policy tweak because:

- it can recover one known clean `insufficient_history` month
- it does not change the broader alpha-search problem
- it does not have enough upside to become the main objective of this sprint

This sidecar must not consume the Week 1-4 mainline budget.

## Hard Stop Conditions

Stop the sprint at Week 4 if any of the following holds:

- no candidate passes the winner gate
- the best candidate fails orthogonality
- the best candidate only works because of one subperiod
- cost retention falls below `50%`

## Expected Output

This sprint should end in exactly one of two states:

1. `Winner found`
   - one factor qualifies as the next US alpha core candidate

2. `No winner`
   - the factor-layer restart closes honestly
   - no fallback expansion, no quiet reopening of deferred families, and no post-hoc broadening of the gate

The whole point of this charter is to force an honest directional answer within 8 weeks rather than drifting into open-ended factor exploration.
