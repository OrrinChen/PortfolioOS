# US Alpha Core Week 1 Baseline Registry (2026-04-16)

## Purpose

This registry freezes the comparison set for the US alpha core restart before Week 2 qualification begins.

It separates three categories that must not be mixed:

1. `Platform-native comparable baselines`
   - factors already evaluated inside the current PortfolioOS factor/evaluator stack
   - these are the only baselines that can serve as direct Week 4 comparator objects without a fresh rerun

2. `External methodological benchmarks`
   - strong research results from the external US WRDS workspace
   - these remain important alpha references, but they are **not** directly comparable to current-platform monthly factor runs because they use different event timing, labels, and universes

3. `Frozen family references without current-platform scorecards`
   - old family names that remain frozen as historical context
   - they are not reopened in original form and they are not admissible as numerical Week 4 comparators unless rerun later under the current qualification contract

## Registry Rule

Until a fresh target-universe rerun exists under the Week 1 qualification contract, the strongest direct comparator inside PortfolioOS remains:

- `alt_momentum_4_1`

This is a **provisional platform-native comparator**, not a claim that the 50-name expanded-US Phase 1 sample is already the final `rank_500_1500` target universe.

## A. Platform-Native Comparable Baselines

These are frozen numeric comparators because they already exist in checked-in PortfolioOS artifacts.

| Baseline | Family | Sample / Runtime | Mean Rank IC | Mean Top-Bottom Spread | Positive Rank IC Ratio | Mean Monthly Turnover | Notes |
| --- | --- | --- | ---: | ---: | ---: | ---: | --- |
| `alt_momentum_4_1` | momentum | PortfolioOS Phase 1 expanded-US frozen sample (`50` names) | `0.1063` | `1.2255%` | `77.5%` | `0.35` | Accepted Phase 1 seed alpha; strongest currently checked-in platform-native comparator |
| `equal_weight_momentum_6_1` | momentum | PortfolioOS Phase 1 expanded-US frozen sample (`50` names) | `0.0551` | `0.8707%` | not logged in closeout note | `0.25` | Legacy baseline from Phase 1 acceptance loop |

### Caveats On The Platform-Native Scorecard

- These numbers come from [`docs/phase_1_alpha_closeout_note.md`](/C:/Users/14574/Quant/PortfolioOS/.worktrees/codex-us-alpha-week1-freeze/docs/phase_1_alpha_closeout_note.md), not from a fresh Week 1 rerun on `rank_500_1500`.
- The accepted recipe carries an explicit stability warning:
  - development mean rank IC was negative while holdout mean rank IC was positive
  - the Phase 1 closeout treats it as a provisional seed, not a fully validated production alpha
- The scorecard is still useful because Week 1 needs one frozen, platform-native comparator object before new candidates start competing.

## B. External Methodological Benchmarks (Not Direct Week 4 Comparators)

These objects are strong research assets and should guide intuition, but they are not direct scorecard comparators for the current restart because they live outside the current PortfolioOS monthly factor stack.

| Benchmark | Workspace | Signal Family | Key Read | Why It Is Not A Direct Comparator |
| --- | --- | --- | --- | --- |
| announcement-timed `SUE` | external WRDS workspace | event-driven earnings surprise | best clean window `[+2,+2]` with `rank_ic_t ~ 22.7`; best alpha-only t at `[+2,+3]` with `~12.3` | event-driven label, announcement timing, WRDS-specific PIT workflow, different evaluation frame |
| event-aware `revision_1m` | external WRDS workspace | analyst revision | pure `to-next-announcement` closeout at `rank_ic_t ~ 3.61`, `alpha_only_t ~ 4.03` | event-aware label and full-panel event workflow, not current fixed monthly factor grid |
| same-event `SUE + revision` package | external WRDS workspace | event package | underperformed pure `SUE` on the package gate | package result is methodologically important, but it is not a current-platform monthly factor baseline |

### Interpretation

- `SUE` and finalized event-aware `revision` remain the strongest **US methods assets** in the broader research estate.
- They should inform future platform redesign decisions.
- They should **not** be used to claim that a Week 2-4 factor candidate beat the current platform baseline unless they are rerun under the same evaluator, cost model, universe, and cadence.

## C. Frozen Family References Without Current-Platform Week 1 Scorecards

These remain frozen as historical family names, but no current checked-in Week 1 numeric scorecard exists for direct comparison inside PortfolioOS:

- value triplet
- ROE / profitability variants
- asset growth
- 13F family
- original Branch A monthly `revision_1m` feature-only mainline attempts

### Rule For These Families

- They may be referenced qualitatively in research notes.
- They may reappear only as **new definitions** if they enter the candidate sheet as fresh Week 2-3 competitors.
- Their original forms remain frozen and are not silently reopened.

## Week 1 Comparator Hierarchy

Use this hierarchy for the restart sprint:

1. `Direct Week 4 comparator`
   - `alt_momentum_4_1`

2. `Secondary platform-native historical reference`
   - `equal_weight_momentum_6_1`

3. `External diagnostic anchors`
   - announcement-timed `SUE`
   - event-aware `revision`
   - failed same-event `SUE + revision` package

4. `Frozen family names without direct current-platform scorecards`
   - value / ROE / asset growth / 13F

## What This Registry Freezes

- Week 2-3 candidate evaluation may not invent a new baseline midstream.
- Week 4 winner claims must compare first against `alt_momentum_4_1` unless a new target-universe baseline rerun is explicitly produced under the same contract.
- External WRDS results may motivate design choices, but they do not substitute for current-platform qualification evidence.
