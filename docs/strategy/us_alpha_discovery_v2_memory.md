# US Alpha Discovery v2 Memory

Short branch-local operating note for the active discovery-v2 lane.

## Status

- Worktree: `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze`
- Branch: `codex/us-alpha-week1-freeze`
- Last sync: `2026-04-17`
- Active lane: calibration-family closeout complete
- Old `US alpha core restart` sprint remains closed with no winner

## Canonical References

- Charter:
  - `docs/strategy/alpha_discovery_charter_v2_2026_04_16.md`
- Family selection memo:
  - `docs/strategy/alpha_discovery_family_selection_memo_2026_04_16.md`
- Calibration D1:
  - `docs/strategy/us_residual_momentum_calibration_d1_2026_04_16.md`
- Kickoff plan:
  - `docs/superpowers/plans/2026-04-16-alpha-discovery-v2-kickoff.md`
- Calibration setup note:
  - `docs/strategy/us_residual_momentum_calibration_setup_note_2026_04_16.md`
- Calibration failure taxonomy:
  - `docs/strategy/calibration_machine_failure_taxonomy_2026_04_16.md`
- Residualization confirmation closeout:
  - `docs/strategy/us_residual_momentum_residualization_confirmation_note_2026_04_17.md`
- A-share primary family Phase 0 memo:
  - `docs/strategy/a_share_state_transition_phase0_memo_2026_04_17.md`
- A-share primary family D1 Slice A:
  - `docs/strategy/a_share_state_transition_mechanism_charter_2026_04_17.md`
  - `docs/strategy/a_share_state_transition_expression_ledger_initial_2026_04_17.md`

## Program Structure

- Calibration family:
  - `US residual momentum / residual reversal`
- Primary mining family:
  - `A-share state-transition microstructure`
- Hard serial rule:
  - no primary-family mining before the calibration-family closeout exists

## Current Calibration Read

- Harness state:
  - `3` live expressions
  - `3` controls
  - persisted `100`-seed shuffled null distribution
  - expression bootstrap ranking artifact
  - expression spread-correlation artifact
  - baseline-residualization artifact versus frozen baseline mimic
  - exposure-conditioned residualized summary artifact
- Best current live expression:
  - `RM3_VOL_MANAGED`
  - `mean_rank_ic ~ 0.0329`
  - `rank_ic_t ~ 0.7371`
  - shuffled-null percentiles only `~70-72%`
  - bootstrap top-1 frequency only `~53.8%`
  - baseline-residualized `rank_ic_t ~ 1.03`
- Key interpretation:
  - a single shuffled placebo draw is too noisy to judge alone on this short sample
  - after moving to a null distribution, the stronger read is still negative:
    - no live expression yet separates cleanly from the null/control envelope
  - orthogonality is not the main issue:
    - live-expression spread correlations stay modest (`max abs ~0.40`)
  - baseline residualization now exposes a boundary behavior rather than a positive read:
    - `RM3` and `RM1` strengthen after baseline removal
    - `RM2` largely collapses
    - `RM3` live residualized `rank_ic_t ~ 1.03` only reaches about the `81st` percentile of the residualization placebo null
    - `19 / 100` placebo seeds match or exceed the live residualized `rank_ic_t`
    - after exposure-conditioned adversarial hardening, `RM3` drops to about the `15th` percentile of the hardened null
    - all live residualized expressions now sit low under the hardened null:
      - `RM1 ~ 7%`
      - `RM2 ~ 8%`
      - `RM3 ~ 15%`
    - residualized `RM3` strength is concentrated in the `mid/high` baseline-exposure terciles while the `low` tercile is negative
    - practical read:
      - the strengthening is not just null-consistent
      - under a stricter, exposure-aware null it is plainly non-exceptional
      - and is being treated as a residualization-calibration edge case, not as family evidence
  - machine principle now recorded:
    - `P-001: Exposure-Conditioned Adversarial Nulls`
    - whenever expression generation changes exposure structure, the adversarial null must preserve that same structure
  - the updated failure mode is:
    - no clear null separation,
    - no stable internal winner dominance,
    - and a residualization step that still shows null-consistent strengthening under `RM3_VOL_MANAGED`
  - this boundary behavior is now formalized as:
    - `FM-001: Null-Consistent Residualization Strengthening`
  - adversarial hardening status:
    - complete for `FM-001`
  - residualization review status:
    - complete as a light confirmation review
    - no broad residualization redesign justified
    - `FM-001` now classified as `partially explained`
  - calibration machine is therefore live and sufficiently validated for calibration exit

## Calibration Exit Condition

Calibration exits when all three conditions hold:

1. adversarial layer can produce credible conditional-null reads for exposure-biased expressions,
2. residualization layer has been reviewed and classified:
   - either the main issue was null-layer miscalibration,
   - or a residualization algorithm issue has been explicitly located,
3. machine governance assets exist:
   - at least one recorded failure-mode entry,
   - and at least one reusable machine principle.

Exit consequence:

- open the primary family immediately after those conditions are met,
- keep the calibration family's alpha conclusion permanently out of scope.
- Current state:
  - all calibration exit conditions are now satisfied
  - next lane is `A-share state-transition microstructure`

## Active Constraints

- Do not reopen the calibration family as a winner search.
- Do not reopen Branch A / fixed candidate tournament logic.
- Do not escalate into optimizer redesign from this lane.

## Prior Shift Before Primary Family

- Calibration prior:
  - suspicious reads are presumed machine-boundary behavior until proven otherwise
- Primary-family prior:
  1. first suspect family-specific event/data structure
  2. then suspect a mismatch between the family and an existing machine principle
  3. only then conclude that the family has no signal

## Practical Next Work

1. Treat calibration-family closeout as complete; do not reopen `US residual momentum / residual reversal` as a winner search.
2. Preserve the shuffled-null benchmark and `P-001` as default machine principles for future families.
3. Use `FM-001` as the first machine-taxonomy entry; classify similar residualization reads against it before making family claims.
4. Phase 0 family-selection work for `A-share state-transition microstructure` is now recorded through:
   - `docs/strategy/a_share_state_transition_phase0_memo_2026_04_17.md`
5. `D1 Slice A` is now complete through:
   - `docs/strategy/a_share_state_transition_mechanism_charter_2026_04_17.md`
   - `docs/strategy/a_share_state_transition_expression_ledger_initial_2026_04_17.md`
6. Next live step is `D1 Slice B: Adversarial Plan + Ideal-Data Gap + Regime Gate`, after a deliberate buffer and not direct D2 expression mining.
7. Keep calibration-family alpha conclusions permanently out of scope unless a future program explicitly redefines scope.
