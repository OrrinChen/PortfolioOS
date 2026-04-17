# US Alpha Discovery v2 Memory

Short branch-local operating note for the active discovery-v2 lane.

## Status

- Worktree: `C:\Users\14574\Quant\PortfolioOS\.worktrees\codex-us-alpha-week1-freeze`
- Branch: `codex/us-alpha-week1-freeze`
- Last sync: `2026-04-16`
- Active lane: calibration-family validation only
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
    - next justified slice is residualization-layer review
    - current triage says that review should start as a light confirmation review, not a broad repair program
  - calibration machine is therefore live but not yet validated

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
- keep the calibration family’s alpha conclusion permanently out of scope.

## Active Constraints

- Do not open the primary family yet.
- Do not treat the calibration family as a winner search.
- Do not reopen Branch A / fixed candidate tournament logic.
- Do not escalate into optimizer redesign from this lane.

## Next Gate

- Calibration-family closeout only if one of these becomes true:
  - live expressions clearly outrun the null/control envelope, or
  - the calibration family honestly shows that the current discovery machine is not yet trustworthy

## Practical Next Work

1. Keep strengthening calibration, not primary mining.
2. Preserve the shuffled-null benchmark as the default reference; do not reason from one placebo draw.
3. Use `FM-001` as the first machine-taxonomy entry; classify similar future reads against it before making family claims.
4. Adversarial-layer hardening is now complete for the current `RM3` edge case.
5. Next slice order is now:
   - residualization-layer review (light confirmation scope unless new evidence widens it)
   - only then any renewed family-level interpretation
6. Keep calibration-family alpha conclusions out of scope unless a later slice pushes the residualized read outside the hardened null envelope.
