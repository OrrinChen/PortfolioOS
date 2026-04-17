# A-Share State-Transition Microstructure Mechanism Charter (2026-04-17)

## Role In Discovery v2

This document is `D1 Slice A` for the primary mining family:

- `A-share state-transition microstructure`

Its job is narrower than a full D1 packet.
It does **not** finalize event-universe controls, ideal-data exclusions, or regime-gate numerics.
Those belong to `D1 Slice B`.

This slice answers a different question:

> What are the candidate mechanisms inside this family, what observable signatures would distinguish them, and what kinds of expressions would actually test those mechanisms rather than drifting into nearby but different families?

## Scope Rule

The family remains defined as:

- exchange- or settlement-driven state transitions,
- with economically meaningful movement between constrained and unconstrained trading states,
- evaluated primarily on daily state labels and post-event daily returns.

This slice therefore excludes:

- generic short-horizon reversal without a state-transition anchor
- pure participant-flow families
- pure information-structure families
- any expression whose defining signal requires unobserved full intraday path reconstruction

## Mechanism Map

This charter uses five candidate mechanisms.
The goal is not to prove they all exist.
The goal is to make them separable enough that later expressions and falsification tests can tell them apart.

### M1. Sealed Upper-Limit Continuation

#### Economic story

An upper-limit state that remains sealed into the close may represent unresolved buy-side demand rather than simple overreaction. The exchange constraint prevents immediate price discovery. If the state survives without repeated reopening, some of that demand may carry into the next session rather than exhaust immediately.

#### Observable signature

- event type: upper-limit touch that stays sealed or nearly sealed
- expected sign: positive
- expected horizon: very short to short (`t+1` to `t+5`, potentially fading by `t+10`)
- expected concentration:
  - stronger in names with visible state persistence
  - weaker when the board repeatedly opens intraday
- expected failure regime:
  - speculative blowoff periods where sealing is caused by retail chase rather than informed demand

#### What would count as evidence for this mechanism

- stronger post-event continuation among sealed-limit names than among opened-board names
- persistence survives matched controls based on size / turnover / recent return state
- effect weakens when the state is not truly persistent

#### What is **not** this mechanism

- generic momentum in names that happened to hit a limit-up
- pure attention stories with no distinction between sealed and unstable boards
- overnight gap continuation unrelated to a visible constrained-price state

### M2. Open-Board Failure / Failed Continuation Reversal

#### Economic story

A limit-up event that repeatedly opens instead of staying sealed can reveal unstable continuation demand. The key object is not the limit-up itself, but the failure to maintain the constrained state. That failure can expose speculative exhaustion and trapped late participation, which should predict weaker subsequent returns.

#### Observable signature

- event type: upper-limit touch followed by repeated opening / failed re-seal
- expected sign: negative
- expected horizon: short (`t+1` to `t+10`)
- expected concentration:
  - stronger when board instability is more visible
  - stronger when event-day failure intensity is high
- expected failure regime:
  - information-driven repricings where open-board behavior is just liquidity churn, not exhaustion

#### What would count as evidence for this mechanism

- within the limit-up event set, more unstable boards underperform more stable boards
- failure-intensity measures dominate simple event presence
- pre-event placebo on the same names does not show the same shape

#### What is **not** this mechanism

- raw `anti_mom_21_5`
- broad attention or Dragon-Tiger event effects without a constrained-price transition
- generic high-turnover reversal if it ignores state persistence versus failure

### M3. Lower-Limit Release / Forced-Seller Exhaustion

#### Economic story

A lower-limit or near-lower-limit state can compress forced or one-sided selling into a constrained trading state. Once that state breaks or releases, the market may rebound if the forced-seller inventory has largely been cleared. This is not generic distress reversal; it is a transition from constrained selling to released price discovery.

#### Observable signature

- event type: lower-limit state or severe constrained downside state followed by release
- expected sign: positive after release
- expected horizon: very short to short (`t+1` to `t+5`, maybe `t+10`)
- expected concentration:
  - strongest when release follows visible downside constraint
  - weaker in names where release is part of a broader negative information cascade
- expected failure regime:
  - fundamental deterioration regimes where the constraint reflects real information, not forced liquidity

#### What would count as evidence for this mechanism

- released lower-limit names outperform matched non-event controls
- release-specific expressions do better than plain downside-reversal expressions
- the effect depends on the transition from constrained to released state, not just on a large negative return

#### What is **not** this mechanism

- generic mean reversion after large drawdowns
- ST / distress lottery reversal
- lower-volatility value-type rebound with no constrained-state event

### M4. Turnover-Shock Transition After Constrained-Price Events

#### Economic story

Turnover intensity conditional on a constrained-price event may distinguish informed continuation from exhaustion or handoff. The key is not turnover by itself, but turnover as a state-transition modifier: identical price-limit events may have different post-event paths depending on whether volume reflects orderly absorption or unstable distribution.

#### Observable signature

- event type: constrained-price event with abnormal turnover intensity
- expected sign:
  - not fixed globally
  - depends on whether turnover is attached to persistence or failure
- expected horizon: short (`t+1` to `t+10`)
- expected concentration:
  - strongest when turnover is measured relative to the same event class
- expected failure regime:
  - when turnover alone explains the result and the constrained-price state adds no incremental information

#### What would count as evidence for this mechanism

- interaction-style expressions outperform raw turnover expressions
- turnover-shock reads split sealed-state events from failure-state events in economically sensible directions
- effect disappears when the transition label is removed

#### What is **not** this mechanism

- abnormal turnover as a standalone anomaly
- retail attention overreaction with no state-transition conditioning
- any expression where turnover dominates and the state label is decorative

### M5. T+1 Next-Day Release Asymmetry Conditional On State Transition

#### Economic story

The T+1 settlement rule does not automatically create alpha.
But when a visible constrained trading state occurs, next-day release behavior may be asymmetric because traders who entered on the event day face a forced overnight hold. The mechanism here is not generic overnight-versus-intraday decomposition; it is the interaction between a prior constrained state and next-session release behavior.

#### Observable signature

- event type: visible state-transition day followed by next-day release
- expected sign:
  - depends on state type
  - continuation after credible sealed strength
  - reversal after unstable speculative failure
- expected horizon: immediate next-day to short (`t+1` to `t+3`, maybe `t+5`)
- expected concentration:
  - should be stronger after explicit state events than in the full cross-section
- expected failure regime:
  - if the same pattern exists in the non-event cross-section, then this is not a T+1 state-transition mechanism

#### What would count as evidence for this mechanism

- next-day asymmetry becomes visible only inside event-conditioned subsets
- event-conditioned reads are different from the archived generic H5 decomposition
- the transition anchor, not the raw overnight move, carries the explanatory power

#### What is **not** this mechanism

- the archived generic `T+1 Overnight / Intraday Asymmetry` branch
- ordinary overnight gap reversal without an event anchor
- simple microstructure carry with no settlement-specific story

## Mechanism-Signature Summary

| Mechanism | Core event | Expected sign | Expected horizon | Concentration clue | Primary failure mode |
|---|---|---:|---|---|---|
| `M1 Sealed Upper-Limit Continuation` | sealed upper-limit | `+` | `t+1` to `t+5` | persistence / no reopen | speculative blowoff mistaken for persistence |
| `M2 Open-Board Failure Reversal` | unstable upper-limit / failed re-seal | `-` | `t+1` to `t+10` | failure intensity | attention-only overreaction with no state distinction |
| `M3 Lower-Limit Release Exhaustion` | lower-limit release | `+` | `t+1` to `t+5` / `t+10` | release after forced downside constraint | true bad news, not forced liquidity |
| `M4 Turnover-Shock Transition` | constrained-price event plus abnormal turnover | conditional | `t+1` to `t+10` | turnover only matters with state label | turnover anomaly pretending to be state-transition alpha |
| `M5 T+1 Release Asymmetry` | next-day behavior after event state | conditional | `t+1` to `t+3` / `t+5` | event-conditioned asymmetry only | generic overnight-gap effect |

## Mechanism Boundary Rules

The charter imposes three boundary rules on later D2 work:

1. **One expression, one primary mechanism**
   - every expression must map to exactly one primary mechanism
   - "tests multiple mechanisms" is not allowed as a primary label

2. **Mechanism-specific signatures must differ**
   - if two mechanisms cannot be separated by observable signature, D1 has failed and the map must be rewritten

3. **Counterexamples are mandatory**
   - every mechanism must carry examples of expressions that look nearby but do **not** validate the mechanism

## Known Tensions To Carry Into D1 Slice B

This slice deliberately stops before resolving the following:

1. **Selection bias**
   - event names are structurally non-random
   - D1 Slice B must define matched non-event controls and pre-event placebos

2. **Event-universe completeness**
   - some control events may be missing because full intraday path is not observed
   - D1 Slice B must state which missing controls are acceptable and which are fatal

3. **Regime dependence**
   - registration-system changes may alter the meaning of the same state label across periods
   - D1 Slice B must freeze the regime map before D2 starts

4. **Data-layer honesty**
   - some mechanism stories are richer than what current daily/minute-summary data can actually identify
   - D1 Slice B must separate cleanly testable expressions from approximation-only expressions

## Exit Read

`D1 Slice A` is complete if read on its intended standard:

- the family has a mechanism map rather than a vague theme
- each mechanism has an observable signature
- each mechanism has a "not this" boundary
- the next slice can now design falsification and data honesty around specific mechanisms rather than around a loose family label

That standard is met.

The correct next move is:

- pause before expanding scope
- then open `D1 Slice B: Adversarial Plan + Ideal-Data Gap + Regime Gate`
