# A-Share State-Transition Microstructure D2 Pilot Scope (2026-04-17)

## Role In Discovery v2

This note is the `D2` entry memo for the primary mining family:

- `A-share state-transition microstructure`

It does not reopen `D1`.
It translates the completed `Phase 0 + D1 Slice A + D1 Slice B` packet into one narrow pilot scope that can be implemented without sliding back into tournament selection.

Its job is to answer one practical question:

> What is the first live mining slice that is narrow enough to stay mechanism-linked, honest about data, and executable with the current platform?

## Decision

The first `D2` live slice is:

- **upper-limit daily-state pilot**

The pilot opens only the following mechanism cluster:

- `M1 Sealed Upper-Limit Continuation`
- `M2 Open-Board Failure / Failed Continuation Reversal`
- `M5 T+1 Next-Day Release Asymmetry`

The pilot does **not** open:

- `M3 Lower-Limit Release / Forced-Seller Exhaustion`
- `M4 Turnover-Shock Transition`
- any `Class 2` expression that depends on unlocked event-panel fields
- any `Class 3` expression that depends on full intraday path reconstruction

## Why This Slice Goes First

This pilot is chosen because it has the best combination of:

1. **mechanism separability**
   - `M1`, `M2`, and `M5` are distinct enough to produce informative failure

2. **data honesty**
   - the first expressions can be defined from daily bars plus daily price-limit information
   - they do not require open-count timing, re-seal timing, or queue-depth reconstruction

3. **adversarial tractability**
   - matched non-event controls and pre-event placebo remain meaningful for these expressions

4. **high-information failure**
   - if this pilot fails, it will say something specific about upper-limit state transitions
   - it will not fail because too many mechanisms were mixed together

## Admitted D2 Pilot Expressions

Only the following first-wave expression classes are admitted.

### P1. Sealed Upper-Limit Close Indicator

- maps to: `M1`
- economic object:
  - name hits the upper limit and finishes the day effectively sealed at or extremely near the upper limit
- expected sign:
  - positive
- horizon:
  - `t+1` through short horizon
- minimum data requirement:
  - daily `high`, `close`, `upper_limit_price`

### P2. Opened-After-Limit Weak-Close Proxy

- maps to: `M2`
- economic object:
  - name touches the upper limit intraday but fails to close at the limit
- expected sign:
  - negative
- horizon:
  - `t+1` through short horizon
- minimum data requirement:
  - daily `high`, `close`, `upper_limit_price`

### P3. Next-Day Release Asymmetry After Sealed Upper-Limit

- maps to: `M5` with `M1` as the state anchor
- economic object:
  - next-day overnight / close behavior conditional on `P1`
- expected sign:
  - continuation-leaning relative to non-event controls
- horizon:
  - next day to `t+3`
- minimum data requirement:
  - `P1` state tag plus next-day daily bar

### P4. Next-Day Release Asymmetry After Failed Upper-Limit

- maps to: `M5` with `M2` as the state anchor
- economic object:
  - next-day overnight / close behavior conditional on `P2`
- expected sign:
  - reversal-leaning relative to `P1`
- horizon:
  - next day to `t+3`
- minimum data requirement:
  - `P2` state tag plus next-day daily bar

## Deferred Expressions

These expressions remain inside the family but are deferred out of the first pilot.

### Deferred from `M1`

- `E2 sealed-limit persistence strength`
- reason:
  - would become much cleaner with minute-summary or path-like persistence information
  - not needed to learn whether the basic sealed-state effect exists

### Deferred from `M2`

- `E3 open-board failure intensity`
- reason:
  - requires reliable event-level failure-count or instability fields
  - that is closer to `Class 2` than to the cleanest `Class 1` slice

### Deferred from `M3`

- all lower-limit release expressions
- reason:
  - downside release tagging is still more dependent on a consistent historical event panel

### Deferred from `M4`

- all turnover-conditioned transition expressions
- reason:
  - too easy to collapse into a raw turnover family before the state-transition layer is validated

## Immediate Data-Contract Consequence

The current platform `market.csv` contract is too thin for this pilot.

Current `market.csv` fields are built around:

- `close`
- `vwap`
- `adv_shares`
- `tradable`
- `upper_limit_hit`
- `lower_limit_hit`

That is sufficient for static trading checks.
It is **not** sufficient for `D2` pilot expressions that need:

- whether `high` reached the upper limit
- whether `close` finished at the upper limit
- how far `close` ended below the upper limit
- next-day open/close decomposition

Therefore the first implementation object for `D2` is **not** a family-wide miner.
It is a dedicated:

- `state-transition daily panel`

## Required State-Transition Daily Panel Contract

The first implementation slice must produce one long-form daily panel with at least:

- `date`
- `ticker`
- `open`
- `high`
- `low`
- `close`
- `volume`
- `amount` or equivalent dollar-value proxy
- `upper_limit_price`
- `lower_limit_price`
- `upper_limit_hit`
- `lower_limit_hit`
- `tradable`

Optional but acceptable in the same panel:

- `industry`
- `float_market_cap`
- `size_bucket`
- `liquidity_bucket`

Derived state tags should be built **after** this panel exists, not embedded in the raw provider layer.

## First Implementation Order

`D2` should begin in the following order:

1. build the `state-transition daily panel`
2. add daily-state taggers for:
   - sealed upper-limit state
   - intraday upper-limit touch with failed close-at-limit state
3. add pilot expression builders for `P1` through `P4`
4. run the pilot with:
   - matched non-event control
   - pre-event placebo
   - event-conditioned nulls under `P-001`

This means the first coding unit is a **data-and-tagging slice**, not a broad family miner.

## What This Pilot Is Allowed To Claim

If this pilot works, it may support only the following narrow claims:

- there is or is not signal in upper-limit daily-state transitions
- the `M1/M2/M5` cluster does or does not survive first adversarial attack
- the sealed-state and failed-state branches do or do not separate economically

It may **not** claim:

- a family winner
- anything about lower-limit release
- anything about turnover-shock interaction
- anything about full intraday path dependence

## Exit Read

This memo is complete if:

- the first `D2` live slice is narrower than the family
- the admitted expressions are explicit
- deferred expressions are explicit
- the first implementation object is now clear

That standard is met.

The correct next move is:

- freeze the `state-transition daily panel` contract
- then implement the first `M1/M2/M5` daily-state pilot against that contract
