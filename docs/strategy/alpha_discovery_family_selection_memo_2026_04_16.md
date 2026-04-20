# Alpha Discovery Family Selection Memo (2026-04-16)

## Decision

Calibration family:

- `US residual momentum / residual reversal`

Primary mining family:

- `A-share state-transition microstructure`

## Why This Is Not The Old US Restart

The closed `US alpha core restart` sprint asked a qualification question:

> can a frozen single-factor candidate clear the current platform gates quickly?

That question was useful for promotion discipline, but it was not a strong alpha-discovery question.
It implicitly privileged:

- current evaluator compatibility,
- current platform shape,
- and narrow checked-in sample availability.

The new program asks a different question:

> which information family contains mechanism-bearing predictive information, and what is the best expression of that information before qualification logic is allowed to compress it?

This is why the new program is serial:

- calibration family first,
- primary mining family second,
- qualification only after a family winner exists.

The calibration family exists to validate the discovery machine.
The primary family exists to search for the winner.

## Candidate Family Comparison

| Family | Expected information density | Crowding | Structural edge | Data liabilities | Decision |
|---|---:|---:|---:|---:|---|
| US residual momentum / residual reversal | medium | high | low | low | calibration only |
| A-share state-transition microstructure | high | low-to-medium | high | medium | primary |
| A-share generic anti-momentum / reversal | medium | medium | medium | low | deferred |
| Cross-market connect / dual-list / ADR state effects | medium-to-high | medium | high | high | deferred |

## Why The Primary Family Wins

`A-share state-transition microstructure` is selected as the primary mining family because its edge is objective and market-design based rather than narrative based.

The expected alpha source is not "Chinese stocks are different" in a vague sense.
It is the presence of state transitions that are:

- exchange-constrained,
- mechanically observable,
- and economically meaningful.

Examples include:

- limit-state persistence versus exhaustion,
- open-board versus re-seal dynamics,
- turnover shocks after constrained-price events,
- and next-day continuation or reversal after a forced state break.

These transitions exist because of market structure.
They are not simply another expression of generic momentum or value.

This family also has a better path to adversarial falsification than a generic anomaly family because:

- event definition is concrete,
- placebo windows are natural,
- matched controls are definable,
- and regime changes can be anchored to known institutional rule shifts.

## Why The Calibration Family Stays

`US residual momentum / residual reversal` stays in the program only as the calibration family.

It stays because:

- the data path is already familiar,
- the literature baseline is legible,
- the code path already exists in this repo,
- and a rough answer is already known, which is useful for checking whether the discovery machine behaves sensibly.

It does **not** stay because it is the best place to look for a winner.

The calibration family is intentionally not the same thing as the primary mining family.
Its purpose is to answer questions like:

- do the negative controls suppress obvious false positives?
- do bootstrap ranking and intra-family orthogonality behave sensibly?
- does the discovery loop correctly distinguish expression optimization from mechanism evidence?

## Why The Deferred Families Lose For Now

### A-share Generic Anti-Momentum / Reversal

This direction is deferred because it is too broad for the primary family slot.
It may later become a valuable downstream family, but it is less mechanism-specific than state-transition microstructure and therefore a weaker first mining target.

### Cross-Market Connect / Dual-List / ADR State Effects

This direction remains attractive, but it loses the first slot because the data and event definition burden is higher.
The risk here is not that the family is weak.
The risk is that the program spends its earliest discovery cycle resolving data-path ambiguity rather than mining mechanisms.

## Anti-Streetlight Rule

The primary family is not selected by:

- existing code convenience,
- continuity with the failed US restart sprint,
- or ease of immediate platform ingestion.

The primary family is selected by:

- expected information density,
- lower crowding,
- objective structural edge,
- and a credible path to mechanism-level adversarial tests.

This memo therefore rejects both of the following weak justifications:

- "we already have code for it"
- "it sounds like something we should be good at"

## Reopen Conditions For Deferred Families

Deferred families may reopen only if at least one of the following becomes true:

1. the primary family closes with `no winner`,
2. the calibration family reveals that the discovery machine is not well-calibrated for event-structured families,
3. the data gap for a deferred family materially narrows,
4. or a deferred family gains a clearer structural edge than the current primary family.

## Immediate Consequence

The next live step under the charter is:

1. complete the calibration-family D1 packet,
2. validate the discovery machine there,
3. and only then open the primary family for mining.

This memo explicitly does **not** authorize direct primary-family mining today.
