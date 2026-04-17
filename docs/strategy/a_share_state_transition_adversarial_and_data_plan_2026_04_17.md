# A-Share State-Transition Microstructure Adversarial And Data Plan (2026-04-17)

## Role In Discovery v2

This document is `D1 Slice B` for the primary mining family:

- `A-share state-transition microstructure`

It completes the remaining D1 work left open by `D1 Slice A`.

Its purpose is to freeze:

1. adversarial falsification rules
2. ideal-data honesty rules
3. regime-gate rules

This is still a design-stage document.
It does **not** start D2 expression mining and does **not** run data.

## Inputs

- `docs/strategy/a_share_state_transition_phase0_memo_2026_04_17.md`
- `docs/strategy/a_share_state_transition_mechanism_charter_2026_04_17.md`
- `docs/strategy/a_share_state_transition_expression_ledger_initial_2026_04_17.md`
- archived A-share notes on:
  - limit-up failure
  - T+1 asymmetry
  - event-family feasibility / negatives

## Part I. Adversarial Plan

## Why Adversarial Design Is First-Order Here

This family has a structural selection-bias problem:

- event names are not random
- constrained-state names are already unusual before the event
- raw event alpha can easily be mistaken for:
  - stock-selection alpha,
  - retail-attention tails,
  - or generic reversal in speculative names

Therefore the adversarial layer is not a nice-to-have robustness check.
It is part of the definition of what this family is allowed to claim.

## A. Mandatory Negative Controls

### NC-1. Matched Non-Event Control

#### Purpose

Distinguish state-transition alpha from generic cross-sectional alpha on unusual stocks.

#### Pre-registered design

For each event stock on each event date, construct a same-day matched non-event control from the tradable universe excluding names that triggered the same event family on that date.

Minimum matching fields:

- log float market cap
- recent liquidity / turnover proxy
- recent realized volatility
- recent return state
- broad industry bucket

Preferred operational matching shape:

- coarse bucket first:
  - industry
  - size tercile
  - liquidity tercile
- then nearest-neighbor inside the bucket on:
  - recent volatility
  - recent return state

#### Interpretation rule

If the event expression and the matched non-event control show the same directional effect, the read is not sufficient to support a state-transition claim.

### NC-2. Pre-Event Placebo

#### Purpose

Test whether the measured effect already exists before the transition occurs.

#### Pre-registered design

Take the same event names and apply the same ranking logic in a pre-event window before the transition.

Default placebo window family:

- one pre-event observation window aligned to the later expression horizon
- examples:
  - for short event horizons, use the last comparable pre-event window before the transition day
  - for next-day release expressions, use the prior day or prior short window on the same names

#### Interpretation rule

If the same signal shape is already present in the pre-event placebo, the measured result should be interpreted as stock selection or pre-existing trend, not as transition-caused alpha.

## B. Mechanism-Breaking Tests

Each mechanism must face at least one test whose expected direction is fixed in advance.

### M1. Sealed Upper-Limit Continuation

#### Break test

Compare sealed-limit continuation expressions against opened-board names from the same upper-limit event set.

Expected result if `M1` is real:

- continuation is materially stronger in sealed or near-sealed states
- if the effect is equally strong in unstable/opened boards, `M1` is weakened

### M2. Open-Board Failure / Failed Continuation Reversal

#### Break test

Remove failure intensity and keep only event presence.

Expected result if `M2` is real:

- failure-intensity expressions should dominate simple event-presence expressions
- if simple upper-limit-event presence explains the same result, `M2` is weakened

### M3. Lower-Limit Release / Forced-Seller Exhaustion

#### Break test

Compare released downside-constraint states against equally large drawdown names without a constrained-state transition.

Expected result if `M3` is real:

- release expressions outperform plain downside-reversal expressions
- if generic downside reversal explains the result, `M3` is weakened

### M4. Turnover-Shock Transition

#### Break test

Strip out the event-state label and test turnover alone.

Expected result if `M4` is real:

- turnover only becomes predictive when paired with the correct state-transition label
- if raw turnover carries the same effect by itself, the mechanism collapses into a turnover family

### M5. T+1 Next-Day Release Asymmetry

#### Break test

Compare event-conditioned next-day release expressions against the archived generic overnight/intraday decomposition.

Expected result if `M5` is real:

- the event-conditioned version shows a different and stronger shape than the generic H5-style decomposition
- if the generic decomposition already explains the effect, `M5` is weakened

## C. Event-Conditioned Null Principle

`P-001: Exposure-Conditioned Adversarial Nulls` is binding here.

For this family, the null must preserve not only broad exposure structure but also event-structure partitions when those partitions are part of the expression definition.

Minimum conditioning dimensions:

- event type bucket
- size bucket
- liquidity bucket

Optional later dimensions if sample supports them:

- volatility bucket
- regime bucket

Interpretation rule:

- unconditional placebo is not sufficient for event-conditioned expressions
- a winner claim may only be made against a null that respects the same event-structure partitions that the live expression uses

## Part II. Ideal-Data Gap

## A. Ideal Dataset

The ideal dataset for this family would contain:

1. full daily OHLCV and state-label history
2. complete historical limit-state event panels
3. intraday path information for:
   - first limit touch
   - open-board count and timing
   - re-seal timing
   - close-state persistence
4. queue / seal-book strength or similar depth proxies
5. robust market-cap, float, liquidity, and industry controls
6. long enough history to span multiple institutional regimes

## B. Current Realistic Dataset

Current realistic dataset is narrower:

### Cleanly supportable now

- daily OHLCV
- daily returns
- daily turnover / liquidity summaries
- float-cap / size style controls
- industry buckets
- next-day and short-horizon daily forward returns
- some daily constrained-state labels, where a historical source is available at honest cost

### Potentially supportable with caveat

- daily limit-event fields such as open-count or event-type labels, if sourced through:
  - cached bulk pulls
  - or a lower-cost historical source than the current live rate-limited endpoint
- minute-level summary statistics that do not require full path reconstruction

### Not honestly supportable in this cycle

- full intraday open-board / re-seal path reconstruction
- full queue-strength / seal-book depth history
- intraday sequence-dependent expressions whose sign depends on exact within-day timing

## C. D2 Admission Consequence

The ideal-data gap creates three expression classes:

### Class 1. Admissible to D2 now

- daily-state expressions
- next-day / short-horizon release expressions
- state-conditioned turnover interactions using daily summaries

### Class 2. Admissible only if a historical event panel is cached or otherwise unlocked

- limit-up failure intensity expressions that require reliable event-level daily fields
- lower-limit release expressions that require consistent historical event tagging

### Class 3. Excluded from this cycle

- queue-strength formulas
- exact re-seal timing formulas
- intraday path-shape expressions that need full tick or minute-sequence reconstruction

Rule:

If an expression belongs to Class 3, it must be removed from the D2 ledger rather than kept as a vague future possibility inside the active family.

## Part III. Regime Gate

## Why Regime Is A Gate, Not A Decoration

For this family, institutional regimes can change the meaning of the same state-transition label.

Examples:

- registration-system reform can alter price-limit dynamics
- market participation mix can change what limit states mean
- a mechanism that works only under one rule set is not a family winner

Therefore regime is part of the family gate itself.

## Pre-Registered Regime Partition

The default regime map for this family is:

### R1. Pre-registration-reform regime

- from sample start
- through `2020-08-23`

### R2. Registration-transition regime

- `2020-08-24` through `2023-04-09`

### R3. Full-registration regime

- `2023-04-10` onward

Optional secondary diagnostic split:

- `2018` stress-period analysis may be used descriptively, but does not replace the three-regime institutional map above

## Regime Gate Rules

### Family winner regime gate

For a family to be classified as a `family winner` later:

1. the best expression must achieve `rank_ic_t >= 2.0` in at least two institutional regimes
2. the sign of mean spread must be correct in those same two regimes
3. the expression-ledger ranking across those two regimes must have Spearman rank correlation `>= 0.50`

### Downgrade rules

If only one regime clears the gate:

- classify as `regime-contingent local pattern`

If two regimes clear but expression-rank correlation is below `0.50`:

- treat as unstable regime dependence, not as a portable family winner

If no regime pair clears the gate:

- no family winner

## Part IV. Exit Read

`D1 Slice B` is complete if judged on its intended standard:

- the family has mandatory negative controls
- each mechanism has a pre-registered mechanism-breaking test
- the data gap is honest enough to exclude inadmissible expressions
- the regime gate is numerically defined rather than described vaguely

That standard is met.

## D1 Closeout

With `D1 Slice A` and `D1 Slice B` both complete:

- D1 is now complete for `A-share state-transition microstructure`
- the next live stage is `D2: Mechanism-Linked Expression Mining`

Important transition rule:

- D2 should begin only after this D1 packet is read as the governing boundary
- D2 may not reopen excluded alternative families by drift
- D2 may not admit expressions that violate the data-honesty classifications above
