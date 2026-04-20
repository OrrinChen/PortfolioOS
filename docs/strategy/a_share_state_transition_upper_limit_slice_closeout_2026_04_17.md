# A-Share State-Transition Upper-Limit Slice Closeout (2026-04-17)

## Role

This note closes the first live `D2 -> D3` read for the primary family:

- `A-share state-transition microstructure`

It covers only the first admitted slice from
[a_share_state_transition_d2_pilot_scope_2026_04_17.md](C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze\docs\strategy\a_share_state_transition_d2_pilot_scope_2026_04_17.md):

- upper-limit daily-state pilot
- mechanisms `M1 / M2 / M5`
- first-wave expressions `P1 / P2 / P3 / P4`

It does **not** claim a family winner and does **not** close the full family on behalf of `M3` or `M4`.

## Data And Regime Scope

Live pilot read used:

- `data/generated/state_transition_daily_panel_growth60_2024_real.csv`
- `60` A-share growth names
- `553` trading dates
- `2024-01-02` through `2026-04-17`

Observed event counts in this panel:

- upper-limit touched: `51`
- sealed upper-limit: `42`
- failed upper-limit: `9`
- lower-limit touched: `7`

Institutional regime coverage:

- this panel sits entirely inside `R3 Full-registration regime`
- therefore the `D4` two-regime portability gate is **not testable** in this cycle

That means the strongest possible classification for this slice is below `family winner`.

## Final Read: First-Wave Upper-Limit Expressions

| expression_id | obs | observed_mean | excess_vs_control | excess_vs_placebo | null_percentile |
|---|---:|---:|---:|---:|---:|
| `P1_SEALED_UPPER_LIMIT` | `109` | `+0.9857%` | `+2.9016%` | `-19.0181%` | `1.00` |
| `P2_FAILED_UPPER_LIMIT` | `31` | `+0.3270%` | `+1.1822%` | `-12.4534%` | `0.00` |
| `P3_NEXT_DAY_AFTER_SEALED` | `109` | `-1.0570%` | `+0.8907%` | `-4.2665%` | `0.00` |
| `P4_NEXT_DAY_AFTER_FAILED` | `31` | `+2.1166%` | `+2.7333%` | `-2.0556%` | `1.00` |

Key implications:

1. `P-001` no longer degenerates.
   - exposure-conditioned nulls are now functioning as intended on real data
   - this slice is no longer blocked by machine failure

2. `P1` and `P4` are the only expressions with clearly positive null separation.
   - both sit at the top of the conditioned-null distribution in this read
   - both also beat matched non-event controls

3. The full `M1 / M2 / M5` cluster fails the placebo sanity check.
   - all four expressions are worse than their own pre-event placebo windows
   - this is the strongest evidence that the event day is not the clean origin of the effect

4. `M2` is weakened immediately.
   - the simplest failed-upper-limit expression (`P2`) has the wrong sign versus its expected mechanism direction

5. `M5` does not survive as a portable branch at first-wave level.
   - `P3` is outright negative
   - `P4` is positive, but still loses to the same-name pre-event placebo

## M4 Exploratory Read

An exploratory `M4` turnover-shock read was run on the same panel using high-turnover conditional subsets inside the upper-limit event universe.

Thresholds:

- sealed-state high-turnover threshold: `3.9664x` recent liquidity
- failed-state high-turnover threshold: `4.7870x` recent liquidity

Read:

| expression_id | obs | observed_mean | excess_vs_control | excess_vs_placebo | null_percentile |
|---|---:|---:|---:|---:|---:|
| sealed high-turnover subset | `37` | `-2.1953%` | `+1.1702%` | `-22.1996%` | `0.00` |
| failed high-turnover subset | `11` | `+2.5446%` | `+0.2579%` | `-10.0163%` | `1.00` |

Interpretation:

- the turnover-conditioned sealed branch does **not** rescue `M1`
- the turnover-conditioned failed branch goes in the wrong direction for a failure/exhaustion story
- this exploratory read does not justify opening a dedicated `M4` winner lane on current evidence

## D3 / D4 Interpretation

Per the charter interpretation ladder, this slice is best classified as:

- `mechanism suggestive but unproven`

Why not `family winner`:

- only one institutional regime is observed
- placebo contamination is unresolved
- no stable mechanism story survives across the whole upper-limit cluster

Why not full `no winner` for the family yet:

- `P1` and `P4` do show local positive separation against conditioned nulls and matched controls
- but that evidence is not clean enough to upgrade beyond a suggestive read

## Closeout Consequence

The upper-limit first-wave slice is now closed on its intended standard:

- the machine works on real data
- the first admitted `M1 / M2 / M5` expressions have been honestly falsified
- the slice does not justify promotion into a family-winner claim

Practical next-step implication:

1. do **not** keep re-sweeping `P1 / P2 / P3 / P4`
2. do **not** open a dedicated `M4` lane from this read
3. treat this slice as a bounded, informative non-winner
4. if the family continues, the next honest object is:
   - either a downside-state (`M3`) slice with a deliberately event-richer universe
   - or a broader data-unlocking move aimed at pre-`R3` regime coverage
