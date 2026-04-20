# Alpha Discovery Charter v2 (2026-04-16)

## Purpose

This charter replaces the prior `US alpha core restart` tournament logic with a discovery-first research program.

The new objective is not to screen a frozen field for something the current platform can immediately consume.
The objective is to mine one information family deeply enough to determine:

1. whether the family contains real predictive information,
2. what the best expression of that information is,
3. why that expression works,
4. when it fails,
5. and only then whether it deserves to enter a downstream qualification lane.

The research unit is now the **mechanism-bearing family**, not the frozen single factor.

## Strategic Reset

### What This Charter Replaces

The previous sprint optimized for:

- narrow candidate freeze,
- fast qualification,
- immediate platform-consumability,
- hard Week 4 stop/go on a small checked-in sample.

That workflow was useful for promotion discipline, but it was not a strong alpha-discovery workflow.
It was closer to platform qualification than buy-side mining.

### New Order Of Operations

This charter adopts the following order:

1. information source
2. mechanism decomposition
3. expression mining
4. adversarial falsification
5. family winner closeout
6. qualification handoff

Qualification is now a downstream consumer of discovery, not its admission filter.

## Program Structure

This program runs two families in strict sequence, not in parallel:

1. **Calibration family**
   - `US residual momentum / residual reversal`
   - purpose: validate the discovery loop itself
   - not expected to be the final winner family

2. **Primary mining family**
   - `A-share state-transition microstructure`
   - purpose: search for a true research winner in a domain with objective structural edge

The calibration family must close before the primary family opens.

## Why The Primary Family Changes

The primary family is no longer selected by:

- existing code convenience,
- continuity with the previous failed sprint,
- or ease of immediate platform ingestion.

The primary family is selected by:

- high expected information density,
- lower crowding,
- structural market asymmetry,
- and objective edge from market design rather than self-asserted informational superiority.

Under that criterion, the chosen primary family is:

## Primary Family

### A-share State-Transition Microstructure

This family studies predictive information created by **exchange and market-structure-enforced state transitions**, not by generic price-based anomalies.

The core research domain is the transition between constrained and unconstrained trading states, especially when the transition itself is informative.

Initial mechanism clusters include:

1. **Limit-state persistence vs exhaustion**
   - examples:
     - post-limit-up continuation
     - post-limit-up reversal
     - limit-down release behavior

2. **Open-board / re-seal path dependence**
   - examples:
     - open-board after first limit touch
     - re-seal success vs failure
     - next-day response after intraday state break

3. **Turnover-shock state transitions**
   - examples:
     - extreme turnover after constrained-price events
     - exhaustion vs informed continuation after abnormal participation

This is still one family.
The sub-mechanisms above are not parallel families; they are internal mechanism hypotheses inside the same family.

## Non-Goals

This charter explicitly does not do the following:

- no direct reopening of the frozen `US alpha core restart` winner race
- no broad candidate tournament across unrelated families
- no optimizer redesign as part of discovery
- no paper-account validation during discovery
- no claim that a family winner is automatically platform-promotable
- no intraday path modeling that current data cannot honestly support

## Discovery Unit: Mechanism, Not Variant

The minimum valid discovery unit is:

- one family,
- decomposed into explicit mechanism hypotheses,
- each hypothesis linked to observable signatures,
- each candidate expression linked back to one mechanism hypothesis.

This means expression mining is not a cartesian product of transformations.
A new expression may only enter the ledger if it answers:

1. which mechanism it is testing,
2. what signature it should strengthen or weaken,
3. what failure pattern would falsify that mechanism.

## Discovery Phases

## Phase 0: Family Selection Memo

Purpose:

- justify why this family is worth mining before code and data work expand.

Required output:

- one-page selection memo covering:
  - expected information density
  - crowding level
  - structural edge
  - known data liabilities
  - why this family is superior to at least two alternative family directions

Exit rule:

- if the family cannot beat obvious alternatives on edge and information density, do not open it.

## Phase D1: Mechanism Charter

This is the heaviest planning phase.
For the primary family, D1 is expected to take roughly `1.5-2 weeks`.

### D1 Required Outputs

1. **Mechanism map**
   - list the internal mechanisms of the family
   - state the economic story of each mechanism

2. **Mechanism-signature table**
   - for each mechanism, define:
     - expected return pattern,
     - expected horizon,
     - expected cross-sectional concentration,
     - expected failure regime,
     - expected sign under stress

3. **Expression ledger template**
   - every future expression must map to:
     - one primary mechanism,
     - one optional secondary mechanism,
     - one explicit falsification path

4. **Ideal-data gap statement**
   - define the ideal dataset for this family
   - define what current data can precisely identify
   - define what current data can only approximate
   - define what current data cannot recover
   - state how each missing data component could bias inference

5. **Event-universe completeness statement**
   - define the full event universe the family would ideally require
   - separate:
     - transition events currently observable,
     - control events currently observable,
     - control events missing because current data do not preserve the full path

6. **Regime map**
   - predefine institutional / market-design regimes, not just generic subperiods
   - at minimum, the map must explicitly handle:
     - pre-major-rule-change regime
     - post-major-rule-change regime
     - any registration-system or trading-rule discontinuity relevant to the chosen event family

7. **Adversarial falsification pre-registration**
   - all mandatory negative controls and mechanism-breaking tests must be written before D2 begins

### D1 Mandatory Negative Controls

The primary family must pre-register at least these two:

1. **Matched-control negative control**
   - for each event stock, build a same-day matched control from non-event stocks using a predeclared matching scheme such as:
     - size,
     - liquidity,
     - beta,
     - recent return state,
     - and volatility
   - purpose:
     - distinguish state-transition alpha from ordinary stock-selection alpha

2. **Pre-event placebo control**
   - apply the same signal logic to the same stocks before the transition event window
   - purpose:
     - detect whether the measured effect predates the transition and is therefore not caused by the state change

Optional additional controls may be added later, but these two are mandatory.

### D1 Regime Handling Rule

For this family, regime is a first-order design input, not a late robustness decoration.

Therefore D1 must:

- pre-register the regime partition,
- explain why the partition is mechanism-relevant,
- and specify what would count as:
  - family-level persistence across regimes,
  - regime-contingent local pattern,
  - or regime break.

## Phase D2: Mechanism-Linked Expression Mining

Purpose:

- generate and evaluate expressions inside one family without collapsing back into tournament selection.

### D2 Rules

- only expressions linked to a D1 mechanism may enter the ledger
- no expression may be admitted solely because it "looks different"
- every expression must name:
  - tested mechanism,
  - expected signature,
  - expected failure mode

### D2 Output

- family expression ledger
- mechanism-expression mapping table
- raw discovery scorecard
- top cluster of surviving expressions, not yet a winner

The goal of D2 is not to find a final winner.
The goal is to identify which expressions deserve adversarial attack.

## Phase D3: Adversarial Falsification

Only the top expression cluster survives into D3.

### D3 Mandatory Tests

1. **Negative controls**
   - matched-control
   - pre-event placebo

2. **Mechanism-breaking tests**
   - at least three pre-registered tests whose expected sign is fixed in advance
   - example pattern:
     - if a mechanism depends on constrained continuation, relaxing the state transition should weaken the effect

3. **Intra-family orthogonality**
   - top expressions must be tested against each other
   - if multiple surviving expressions are nearly redundant, the family has fewer genuine discovery objects than it appears

4. **Residualization against frozen baselines**
   - not to re-run the old sprint,
   - but to verify that the family does not collapse once obvious baseline overlap is removed

5. **Cost retention**
   - after-cost performance is required at this stage

## Phase D4: Family Winner Closeout

The winner is now a **family-level** object, not simply the single best raw factor line.

### Family Winner Quantitative Gate

A family winner must satisfy all of the following:

1. **Incremental predictive strength**
   - after residualizing against frozen baselines, the best expression must achieve either:
     - `rank_ic_t >= 2.0`
     - or `after-cost Sharpe-equivalent >= 0.75`

2. **Stable expression dominance**
   - in bootstrap ranking across the family ledger:
     - `top-1 frequency >= 60%`
     - and top-1 vs top-2 performance gap must remain meaningful:
       - `Sharpe-equivalent gap > 0.30`

3. **Economic survivability**
   - cost retention must remain `>= 50%`

4. **Distinctness**
   - spread correlation versus the strongest frozen baseline must be `< 0.70`
   - and the surviving winner must not merely be a near-duplicate of another top expression

5. **Mechanism validation**
   - at least `2/3` pre-registered mechanism-breaking tests must land in the predicted direction

6. **Regime portability**
   - the family must independently show viable performance in at least `2` pre-registered institutional regimes
   - if the family only works in one regime, the result is downgraded to:
     - `regime-contingent local pattern`
   - it may not be declared a family winner

### Interpretation Levels

At D4, the closeout note must explicitly classify the result as one of:

1. `family winner`
2. `regime-contingent local pattern`
3. `mechanism suggestive but unproven`
4. `no winner`

This prevents narrative inflation.

## Phase D5: Qualification Handoff

Only a family winner enters D5.

D5 asks a new question:

> Can the winner be translated into a platform-consumable object without destroying its informational edge?

This is where the later workflow may examine:

- platform-compatible cadence,
- packaging,
- qualification artifacts,
- cost/capacity translation,
- and promotion-readiness.

If translation destroys the edge, that is a handoff failure, not a discovery failure.

## Calibration Family

### US Residual Momentum / Residual Reversal

The calibration family exists to validate the discovery machine itself.

It is useful because:

- the data path is familiar,
- the literature baseline is strong,
- and a rough answer is already known.

It should answer:

- do the negative controls behave sensibly?
- do the bootstrap and orthogonality tools behave sensibly?
- does the discovery machine distinguish a merely optimized expression from a real mechanism-bearing expression?

It is not the family expected to produce the final winner in this charter.

## Data Honesty Rule

This charter treats data limitations as first-class objects.

Any result must explicitly state whether it is based on:

- daily state labels,
- daily summaries with partial intraday approximation,
- or fully reconstructed intraday transition paths.

The program must never silently slide from:

- "we can observe the state label"

to:

- "we can therefore infer the path mechanism."

If the path is not observed, the charter must say so.

## What Counts As Progress

Progress is not:

- adding more expressions,
- extending the ledger indefinitely,
- or keeping a weak family alive because the story is interesting.

Progress is:

- reducing uncertainty about whether a mechanism exists,
- reducing uncertainty about which expression best captures it,
- and reducing uncertainty about whether the signal survives adversarial attack.

## Stop Rules

Stop the family if any of the following occurs:

1. matched-control and pre-event placebo both show the same effect as the event expression
2. top expressions only differ cosmetically and fail intra-family distinctness
3. the family cannot survive residualization against obvious frozen baselines
4. the result is regime-local and cannot cross the minimum two-regime bar
5. the ideal-data gap is so severe that the tested mechanism cannot honestly be inferred from current data

## Immediate Next Action

Open this charter in two serial steps:

1. run the calibration family and validate the discovery machine
2. only after that, open `A-share state-transition microstructure` as the primary mining family

No primary-family mining begins before the calibration closeout exists.
