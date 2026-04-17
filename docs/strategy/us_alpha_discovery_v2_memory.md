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
- A-share primary family D1 Slice B:
  - `docs/strategy/a_share_state_transition_adversarial_and_data_plan_2026_04_17.md`
- A-share primary family D2 pilot scope:
  - `docs/strategy/a_share_state_transition_d2_pilot_scope_2026_04_17.md`

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
6. `D1 Slice B` is now complete through:
   - `docs/strategy/a_share_state_transition_adversarial_and_data_plan_2026_04_17.md`
7. `D2` is now narrowed through:
   - `docs/strategy/a_share_state_transition_d2_pilot_scope_2026_04_17.md`
8. The first live `D2` slice is:
   - upper-limit daily-state pilot
   - mechanisms `M1/M2/M5`
   - admitted first-wave expressions `P1/P2/P3/P4`
9. The next implementation object is not a full family miner; it is:
   - a dedicated `state-transition daily panel` contract plus daily-state taggers
10. D2 implementation has now started with the first coding slice:
    - `state-transition daily panel`
    - `sealed / failed upper-limit` daily-state taggers
    - active code path:
      - `src/portfolio_os/alpha/state_transition_panel.py`
11. The second D2 coding slice is now in place inside the same panel module:
   - `next_intraday_return` decomposition field added
   - first-wave upper-limit pilot expression builder added:
     - `build_upper_limit_pilot_expression_frame(...)`
   - emitted expression contract now covers:
     - `P1_SEALED_UPPER_LIMIT`
     - `P2_FAILED_UPPER_LIMIT`
     - `P3_NEXT_DAY_AFTER_SEALED`
     - `P4_NEXT_DAY_AFTER_FAILED`
   - current practical read:
     - D2 now has a deterministic event-to-expression expansion layer for the `M1/M2/M5` pilot
     - matched controls, placebo checks, and event-conditioned nulls remain deferred to later D2 slices
12. The third D2 coding slice is now in place:
   - minimal `NC-1` same-day matched non-event control selector added:
     - `build_upper_limit_matched_non_event_control_frame(...)`
   - current control-selection contract:
     - coarse bucket filter on `industry`, `size_tercile`, `liquidity_tercile`
     - nearest-neighbor tie-break on:
       - `recent_realized_volatility`
       - `recent_return_state`
   - explicit boundary:
     - this slice assumes matching covariates are already present on the input frame
     - covariate derivation and no-replacement policies remain deferred
13. The fourth D2 coding slice is now in place:
   - `NC-1` matching-covariate enrichment helper added:
     - `build_state_transition_matching_covariates(...)`
   - current enrichment contract:
     - merges static reference fields:
       - `industry`
       - `issuer_total_shares`
     - derives:
       - `float_market_cap`
       - `recent_liquidity_amount`
       - `size_tercile`
       - `liquidity_tercile`
       - `recent_realized_volatility`
       - `recent_return_state`
     - lookback is configurable, default `20`
   - selector hardening:
     - matched-control selection now skips rows with incomplete matching covariates
   - explicit boundary:
     - no-replacement matching policy still remains deferred
     - placebo / event-conditioned nulls remain deferred
14. The fifth D2 coding slice is now in place:
   - matched event-vs-control comparison helper added:
     - `build_upper_limit_matched_control_comparison_frame(...)`
   - current comparison contract:
     - joins first-wave pilot expressions against the `NC-1` matched-control map
     - emits paired rows with:
       - `event_forward_return`
       - `control_forward_return`
       - `excess_forward_return`
     - horizon alignment is expression-specific:
       - `P1/P2` compare on `next_close_return`
       - `P3/P4` compare on `next_intraday_return`
   - practical meaning:
     - D2 now has a deterministic bridge from
       - event-state tagging
       - to expression emission
       - to matched-control pairing
       - to excess-return comparison
   - verification status:
     - targeted state-transition panel tests passed
     - broader regression pack passed:
       - `33 passed, 3 warnings`
   - explicit boundary:
     - no-replacement matching policy still remains deferred
     - placebo / event-conditioned nulls remain deferred
15. The sixth D2 coding slice is now in place:
   - pre-event placebo comparison helper added:
     - `build_upper_limit_pre_event_placebo_comparison_frame(...)`
   - current placebo contract:
     - keeps the same event names as the live `P1/P2/P3/P4` rows
     - maps each event date to the immediately prior trading-day placebo window on the same ticker
     - expression-specific horizon alignment is:
       - `P1/P2` -> prior one-day close return
       - `P3/P4` -> prior one-day intraday return
     - emits:
       - `event_forward_return`
       - `placebo_forward_return`
       - `placebo_excess_return`
   - practical meaning:
     - D2 now has both mandatory adversarial legs from `D1 Slice B` in live coding form:
       - `NC-1` matched non-event control comparison
       - `NC-2` pre-event placebo comparison
   - verification status:
     - targeted state-transition panel tests passed
     - broader regression pack passed:
       - `34 passed, 3 warnings`
   - explicit boundary:
     - no-replacement matching policy still remains deferred
     - event-conditioned null generation under `P-001` remains deferred
16. The seventh D2 coding slice is now in place:
   - `P-001` event-conditioned null pool helper added:
     - `build_upper_limit_event_conditioned_null_pool(...)`
   - current null-pool contract:
     - attaches required `P-001` minimum conditioning dimensions to live `P1/P2/P3/P4` rows:
       - `event_type_bucket`
       - `horizon_bucket`
       - `size_tercile`
       - `liquidity_tercile`
     - materializes a deterministic resampling key:
       - `conditioning_bucket_key`
     - keeps the object at the pool layer only:
       - no seed loop
       - no shuffled null draw
       - no percentile / summary logic
   - practical meaning:
     - D2 now has the full set of preregistered pilot ingredients needed before null sampling:
       - live event expressions
       - matched non-event comparisons
       - pre-event placebo comparisons
       - event-conditioned null strata
   - verification status:
     - targeted state-transition panel tests passed
     - broader regression pack passed:
       - `36 passed, 3 warnings`
   - explicit boundary:
     - seed-based event-conditioned null generation remains the next slice
     - no-replacement matching policy still remains deferred
17. The eighth D2 coding slice is now in place:
   - seed-based `P-001` event-conditioned null draw helper added:
     - `build_upper_limit_event_conditioned_null_draw(...)`
   - current null-draw contract:
     - consumes the live event-conditioned null pool
     - reshuffles `forward_return` only within:
       - `date`
       - `conditioning_bucket_key`
     - emits:
       - `null_forward_return`
       - `null_seed`
   - practical meaning:
     - D2 now has the first executable `P-001` null mechanism, not just null strata
     - the upper-limit pilot can now produce:
       - live event rows
       - matched-control rows
       - pre-event placebo rows
       - one seeded event-conditioned null draw
   - verification status:
     - targeted state-transition panel tests passed
     - broader regression pack passed:
       - `38 passed, 3 warnings`
   - explicit boundary:
     - per-expression null summaries / percentiles remain deferred
     - no-replacement matching policy still remains deferred
18. Keep calibration-family alpha conclusions permanently out of scope unless a future program explicitly redefines scope.
