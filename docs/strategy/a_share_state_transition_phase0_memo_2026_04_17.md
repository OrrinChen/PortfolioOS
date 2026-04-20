# A-Share State-Transition Microstructure Phase 0 Family-Selection Memo (2026-04-17)

## Decision

The primary mining family for Alpha Discovery v2 remains:

- `A-share state-transition microstructure`

Phase 0 now upgrades that choice from a charter-level statement into an executable D1 entry point.

The purpose of this memo is not to design the family in full.
It is to answer a narrower question:

> Is this family still the best next mining target after calibration closeout, and what must be frozen before D1 begins?

The answer is `yes`.

## Why This Family Still Wins

This family remains superior because it combines four properties that the obvious alternatives do not jointly offer:

1. **mechanism granularity**
   - the candidate alpha source is tied to exchange-enforced state changes rather than to a broad anomaly bucket

2. **structural edge**
   - the source of inefficiency is rooted in market design and trading constraints that do not map cleanly onto standard US large-cap factor families

3. **adversarial designability**
   - matched controls, pre-event placebos, and regime splits can be specified in a way that is mechanism-relevant rather than decorative

4. **data honesty with daily observability**
   - even without perfect intraday path reconstruction, there is a meaningful daily-state layer that can be studied honestly

This makes the family a better first primary mining target than broader A-share anomaly families and better than cross-market ideas whose first problem is still data synchronization.

## Alternatives Considered But Rejected

These alternatives are not rejected forever.
They are rejected for **this** primary-family slot.

### 1. A-Share Generic Anti-Momentum / Reversal

Why it loses:

- too broad as a mining family
- too easy to slide back into price-based expression tournaments
- too entangled with the already-audited `anti_mom_21_5` line
- weaker mechanism specificity than state-transition microstructure

Why this matters:

- if Phase 0 starts from a family this broad, D1 risks collapsing into a disguised parameter sweep rather than a mechanism charter

Disposition:

- deferred
- may later serve as a downstream comparison or secondary mining lane

### 2. A-Share Participant-Structure / Flow Dislocation

Representative directions:

- northbound flow dislocation
- Dragon-Tiger attention concentration
- financing / crowding overhang

Why it loses:

- still attractive economically, but participant attribution is heterogeneous
- negative controls are less clean than in state-transition events
- several branches depend on external event feeds or account-specific data quality in a way that would make the first discovery cycle spend too much time on data-path dispute

Why this matters:

- the first primary mining family should stress-test mechanism design, not spend its earliest budget proving that the event feed is trustworthy

Disposition:

- deferred as a later family cluster
- not selected as the first primary family

### 3. Cross-Market Connect / Dual-List / ADR State Effects

Why it loses:

- structural edge is real
- but the first-order burden is market/calendar alignment, venue synchronization, and event-universe completeness across markets
- this would make D1 spend too much of its energy on data plumbing rather than on mechanism decomposition

Why this matters:

- the current stage needs a family where the hardest problem is mechanism design, not cross-market stitching

Disposition:

- explicitly deferred
- can reopen if this primary family ends in `no winner` or if the data gap narrows materially

## Family Boundary

This memo intentionally keeps the family broad enough to support multiple internal mechanisms, but narrow enough to stop scope drift.

Included family territory:

- limit-state persistence versus exhaustion
- open-board / re-seal path dependence
- turnover-shock transitions after constrained-price events
- T+1-linked next-day transition behavior when it is tied to a visible trading-state change

Excluded from this family:

- generic short-horizon reversal without a state-transition anchor
- pure participant-flow families
- pure information-structure families
- intraday path-dependent expressions that require unobserved order-book or tick-level sequence data

Important distinction:

- `Limit-Up Failure Attention Reversal` or `T+1 Overnight / Intraday Asymmetry` are **not** alternative families here
- they are candidate **mechanisms or branches inside** the chosen family, subject to D1 filtering

## D1 Preflight Freeze

Phase 0 does not answer the D1 questions.
It only freezes what D1 must answer before D2 can begin.

### 1. Event Universe Boundary

D1 must specify:

- what counts as an observable state-transition event
- what counts as a non-event control
- what counts as a same-family control event that never transitions

Phase 0 boundary:

- the primary observable layer is daily state labels plus subsequent daily/weekly outcomes
- D1 may use minute-level summary statistics if they are already available cheaply and honestly
- D1 may **not** assume full intraday path observability unless that path is explicitly shown to exist in current data

This is the first protection against accidentally designing a family that current data cannot honestly test.

### 2. Selection-Bias Negative Controls

D1 must pre-register controls that answer the core threat:

> Are we measuring state-transition alpha, or just rediscovering stock-selection alpha on a highly non-random event subset?

Minimum controls already frozen by Phase 0:

- matched non-event control
- pre-event placebo control

D1 still needs to decide:

- matching variables
- matching tolerance / bucket method
- placebo window length
- what result would count as a failure of causal interpretation

### 3. Regime Breakpoints

For this family, regime is not a late robustness decoration.
It is part of the mechanism definition.

Phase 0 freezes the expectation that D1 must anchor regime splits to institutional change, not arbitrary subperiods.

Required candidates:

- `2020` registration-system / price-limit reform as the primary breakpoint
- `2023` full registration reform as the second institutional breakpoint
- `2018` may be retained as a secondary market-structure stress split if it improves interpretability

D1 must decide:

- the exact regime partition
- why the partition is mechanism-relevant
- what counts as:
  - family persistence across regimes
  - regime-local pattern
  - regime break

### 4. Ideal-Data Gap

Phase 0 freezes one honesty rule:

> Expressions may not enter the D2 ledger if they depend on path information that current data do not actually preserve.

Current likely reality:

- daily state labels should be observable
- some minute-level summary proxies may be recoverable
- full intraday open-board / re-seal path, queue strength, and seal-book dynamics are unlikely to be fully reconstructible from current free/cheap inputs

D1 must therefore produce three lists:

1. expressions current data can test cleanly
2. expressions current data can only approximate with caveat
3. expressions that must be excluded from this cycle

This is a hard scope-control device, not a documentation nicety.

### 5. Alternatives Rejected But Preserved

This family should not expand every time a nearby idea looks interesting.

Phase 0 therefore freezes a backlog container:

- generic A-share reversal family
- participant-structure / flow-dislocation family
- cross-market state-effects family

D1 may reference these families only to sharpen its own boundary.
It may not absorb them into the state-transition family by convenience.

## What Phase 0 Authorizes

Phase 0 authorizes exactly one next step:

- open `D1 Slice A: Mechanism Charter`

It does **not** authorize:

- expression mining
- data harvesting beyond what D1 needs to define observability
- fast event backtests
- reopening any deferred alternative as a parallel branch

## Exit Read

This memo passes Phase 0 if judged on its intended standard:

- the selected family still beats obvious alternatives on mechanism specificity and adversarial designability
- the D1 preflight items are fully enumerated
- the next step is now narrower, not broader

That standard is met.

The correct next move is:

- pause before implementation-scale expansion
- then open `D1 Slice A: Mechanism Charter`
