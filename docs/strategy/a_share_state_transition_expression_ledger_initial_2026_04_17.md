# A-Share State-Transition Microstructure Expression Ledger Initial Mapping (2026-04-17)

## Purpose

This is the initial mechanism-to-expression ledger for `D1 Slice A`.

It is **not** the final D2 expression ledger.
It only records which expression classes are currently legitimate candidates to test each mechanism, and which nearby expression classes must be excluded because they do not test the claimed mechanism.

## Mapping Rule

Every row below must satisfy:

- one primary mechanism
- optional secondary mechanism only if clearly subordinate
- one expected signature
- one falsification path

Rows here remain provisional until `D1 Slice B` completes:

- event-universe controls
- ideal-data exclusions
- regime gate

## Initial Mapping

| Expr ID | Expression class | Primary mechanism | Secondary mechanism | Expected signature | Data tier | Why this belongs | What would falsify it |
|---|---|---|---|---|---|---|---|
| `E1` | sealed-limit event indicator | `M1` | none | sealed upper-limit names continue more than matched controls | daily state label | tests whether persistence itself carries information | no continuation vs matched controls |
| `E2` | sealed-limit persistence strength | `M1` | `M4` | stronger persistence score -> stronger short-horizon continuation | daily state + optional minute summary | tests intensity inside the sealed-state mechanism | effect vanishes once persistence is measured more finely |
| `E3` | open-board failure intensity | `M2` | none | more failed openings / instability -> worse forward return | daily state or minute summary | direct test of failed-continuation exhaustion | failure intensity adds nothing beyond event presence |
| `E4` | failed re-seal close weakness | `M2` | `M4` | weak close after failed re-seal predicts underperformance | daily state + daily bar summary | tests whether end-of-day failure matters, not just opening count | same effect appears in non-event names with weak closes |
| `E5` | lower-limit release rebound indicator | `M3` | none | released lower-limit events rebound versus controls | daily state label | simplest release-versus-no-release test | no rebound after constrained downside release |
| `E6` | downside release exhaustion intensity | `M3` | `M4` | stronger forced-seller exhaustion proxy -> stronger rebound | daily state + turnover summary | tests whether the release transition has gradation | effect reduces to generic drawdown reversal |
| `E7` | event-day turnover shock conditional on sealed state | `M4` | `M1` | turnover only helps continuation when the state is genuinely persistent | daily state + turnover | turnover is treated as state modifier, not raw anomaly | same turnover effect appears without the sealed-state label |
| `E8` | event-day turnover shock conditional on failure state | `M4` | `M2` | turnover intensifies exhaustion after unstable boards | daily state + turnover | tests state-conditioned turnover handoff / exhaustion | turnover dominates and state label becomes decorative |
| `E9` | next-day release asymmetry after sealed upper-limit | `M5` | `M1` | next-day release path differs from generic overnight carry | daily state + next-day daily bar | event-conditioned T+1 asymmetry, not generic overnight gap | same asymmetry exists in non-event cross-section |
| `E10` | next-day release asymmetry after failed upper-limit | `M5` | `M2` | next-day release path is reversal-heavy after failed continuation | daily state + next-day daily bar | ties T+1 release to unstable speculative states | effect is no different from raw failure reversal without next-day decomposition |
| `E11` | next-day release asymmetry after lower-limit release | `M5` | `M3` | next-day recovery path differs after forced-seller release | daily state + next-day daily bar | settlement constraint interacts with downside release | rebound is fully explained by generic downside reversal |

## Nearby But Excluded Expression Classes

These are intentionally **not** admitted into the mechanism ledger at this stage.

| Excluded class | Why excluded now |
|---|---|
| generic `anti_mom_21_5` variants | tests broad reversal, not state-transition mechanisms |
| raw abnormal turnover factor | participant/attention anomaly unless explicitly conditioned on event state |
| Dragon-Tiger attention proxies | participant-structure family, not state-transition family |
| northbound / financing / ownership flow shocks | participant-flow family |
| raw overnight-gap or intraday-return decomposition | archived H5-style decomposition without event anchor |
| intraday queue-strength / seal-book path formulas | currently depend on path data that D1 Slice B may exclude |

## D2 Admission Rule Preview

No expression class above automatically enters D2.

Before D2 admission, each row must survive three later filters:

1. `D1 Slice B` says the needed data tier is honestly available
2. its mechanism-specific falsification path is pre-registered
3. it still maps cleanly to one primary mechanism after the regime gate is frozen

## Exit Read

This initial ledger is sufficient for `D1 Slice A` if:

- every admitted expression class maps to one primary mechanism
- nearby but invalid expression classes are explicitly named
- the next slice can now decide what survives on data honesty and falsification grounds

That standard is met.
