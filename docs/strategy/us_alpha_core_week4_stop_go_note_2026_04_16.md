# US Alpha Core Restart Week 4 Stop / Go Note (2026-04-16)

## Purpose

This note closes the Week 4 gate for the US alpha core restart on the currently checked-in PortfolioOS sample.

It answers exactly one charter question:

`Is there a winner?`

## Scope

- evaluator: current platform-native monthly / `21d` qualification stack
- sample: checked-in expanded-US platform sample (`50` names), not a full `rank_500_1500` rerun
- candidate field: all eight frozen candidates
  - Family A: `A1`, `A2`, `A3`
  - Family B: `B1`, `B2`, `B3`
  - Family C: `C1`, `C2`
- frozen anchor baseline:
  - `alt_momentum_4_1`
  - structural tag = `anchor_eligible`

This note does **not** claim a final target-universe judgment for the full US mid-cap research program.
It closes the chartered sprint on the current platform-native evidence base.

## Week 4 Gate Recap

Per the frozen charter, a winner must simultaneously:

1. pass the single-factor admission gate
2. pass the 3-slice subperiod stability gate
3. beat the strongest `anchor_eligible` frozen baseline by at least `15%`
4. pass spread-correlation orthogonality

If no candidate satisfies all four, the sprint closes and Weeks 5-8 do not continue.

## Scorecard Read

The complete narrow-sample scorecard is recorded in:

- `outputs/us_alpha_core_restart/week2_interim_synthesis/2026-04-16/week2_scorecard.csv`
- `outputs/us_alpha_core_restart/week2_interim_synthesis/2026-04-16/week2_interim_synthesis.md`

The decisive read is:

- `A2` is the most balanced residual-momentum candidate
  - positive economics
  - orthogonal enough
  - still below both admission-stat thresholds and below the `50%` retention line
- `B2` is the least-bad liquidity candidate
  - best alpha-only t-stat in the full field
  - still below the admission thresholds and below the `50%` retention line
- `C1` has the strongest raw rank-IC
  - but coverage is too thin (`coverage_median ~ 0.40`)
  - and it still fails both admission and subperiod gates

Negative reads also matter:

- `A1` fails orthogonality against the frozen baseline
- `B1` is economically negative
- `C2` is directionally wrong on the current sample

## Stop / Go Decision

### Decision

`STOP`

### Reason

No candidate clears the Week 4 winner definition on the current platform-native sample.

More specifically:

- no candidate passes the single-factor admission gate
- no candidate passes the subperiod stability gate
- therefore no candidate can satisfy the full winner gate

This means the chartered sprint does **not** produce a winner on the current checked-in evidence base.

## Interpretation Boundary

This is a **narrow-sample no-go**, not a universal claim that the underlying factor families are permanently dead.

The disciplined interpretation is:

- the restart sprint has failed to find a platform-consumable single-factor winner on the present checked-in sample
- the correct action under the frozen charter is to stop the sprint rather than keep searching inside Weeks 5-8
- any later continuation would require an explicit new decision, for example:
  - a full `rank_500_1500` rerun under the same contract
  - or a fresh charter with a different candidate field / data layer

What this note explicitly rejects:

- no silent candidate expansion
- no post-hoc parameter sweep
- no use of Weeks 5-8 without a Week 4 winner

## Project-Level Consequence

On the current evidence base, the US alpha core restart remains a useful methods asset but does not graduate into a promotable platform alpha line.

The best current read of the frozen field is:

- `A2` = best balanced residual-momentum candidate
- `B2` = least-bad liquidity candidate
- `C1` = strongest raw rank-IC but too sparse

That ranking is informative, but it is not enough to override the hard stop rule.

## Final Closeout Statement

The US alpha core restart sprint is closed at Week 4 on the current platform-native sample with `no winner`.

Further work on this line should happen only through an explicitly reopened branch decision, not by continuing the frozen sprint by inertia.
