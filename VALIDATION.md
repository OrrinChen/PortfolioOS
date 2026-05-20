# VALIDATION.md

Use this file to choose the smallest validation set that proves the touched work.

## Always Run

```bash
git status --short
git diff --check
```

Research track boundary registry:

```bash
make research-track-boundaries
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run pytest tests/test_research_track_boundaries.py -q
```

PortfolioOS portfolio quant walk-forward smoke and freeze guards:

```bash
make portfolio-quant-walk-forward
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_walk_forward.py tests/test_portfolio_quant_walk_forward.py tests/test_research_freeze_before_portfolio_quant.py -q
```

## CI-Style Local Validation

Run the hardened local validation target when touching workflow, report,
schema, provenance, observability, or validation infrastructure:

```bash
make validate
```

No-network guard:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run python scripts/devtools/no_network_guard.py
```

One-command demo smoke:

```bash
make demo
```

Typed alpha demo-v2 smoke:

```bash
make demo-v2
```

Typed alpha release-candidate hardening tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_typed_alpha_release_candidate.py -q
```

Demo-v2 golden snapshot tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_demo_v2_golden_snapshot.py -q
```

Typed alpha closeout report smoke and tests:

```bash
make typed-alpha-closeout
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_typed_alpha_closeout_report.py -q
```

Alpha Registry v2 smoke and tests:

```bash
make alpha-registry-v2
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_alpha_registry_v2.py -q
```

PortfolioOS v1 research-audit release hygiene tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_portfolioos_v1_research_audit_release.py -q
```

PortfolioOS v1 maintenance freeze tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_portfolioos_v1_maintenance_freeze.py -q
```

Factor Discovery Sandbox FD-1 teaching baseline smoke and tests:

```bash
make factor-discovery-teaching-baseline
make factor-discovery-design-layer
make factor-discovery-design-d1
make factor-discovery-design-d2
make factor-discovery-insider-d2-observability
make factor-discovery-insider-d2-real-observability
make factor-discovery-insider-d2-sell-contrast
make factor-discovery-insider-plan-flag-audit
make factor-discovery-insider-plan-flag-repair
make factor-discovery-8k-d2-observability
make factor-discovery-8k-d2-real-observability
WRDS_USERNAME=<wrds_username> make factor-discovery-8k-wrds-market-rescue
make factor-discovery-insider-d3-signal-builder
make factor-discovery-insider-q1-evidence
make factor-discovery-insider-q1-label-rescue
make track-a-forensic-workflow-fixture
make factor-discovery-fd-wide-design-audit
make factor-discovery-factor-specs
make factor-discovery-rolling-oos
make factor-discovery-marginal-value-gate
make factor-discovery-allocator
make factor-discovery-survival
make factor-discovery-real-data-validation
make factor-discovery-real-data-validation-daily
make factor-discovery-real-factor-replay
make factor-discovery-real-rolling-oos
make factor-discovery-real-placebo-robustness
make factor-discovery-real-failure-diagnosis
make factor-discovery-formula-mechanism-audit
make factor-discovery-weighting-reliability
make factor-discovery-momentum-low-vol-candidate
make factor-discovery-revision-confirmed-alpha
make factor-discovery-small-cap-wrds-pull
make factor-discovery-small-cap-quality-pull
make factor-discovery-small-cap-data-admission
make factor-discovery-small-cap-quality-residual-momentum
make factor-discovery-small-cap-dominance-diagnosis
make factor-discovery-small-cap-s4-2
make factor-discovery-small-cap-s4-3
make factor-discovery-small-emotion-d2
make factor-discovery-small-emotion-full-replay
make factor-discovery-small-emotion-direction-remap
make factor-discovery-small-emotion-exploratory-sweep
make factor-discovery-small-emotion-top-pocket-replay
make factor-discovery-small-emotion-d3-charter
make factor-discovery-small-emotion-sharpening-sweep
make factor-discovery-small-emotion-sharpened-top-pocket-replay
make factor-discovery-small-emotion-sharpened-d3-charter
make factor-discovery-small-emotion-leaf-search
make factor-discovery-small-emotion-full-market-overfit-lab
make factor-discovery-small-emotion-full-market-feature-cache
make factor-discovery-small-emotion-full-market-cached-replay
make factor-discovery-small-emotion-full-market-cost-clean-cached-replay
make factor-discovery-small-emotion-full-market-cost-stale-clean-cached-replay
make factor-discovery-small-emotion-freeze-validation
make factor-discovery-small-emotion-measurement-spec
make factor-discovery-small-emotion-q1-oos
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run python scripts/run_factor_discovery_small_emotion_q1_oos.py --measurement-spec outputs/factor_discovery/small_emotion/freeze_02_cost_stale_clean_profile_rank2_validation_20260518/measurement_spec.yaml --output-dir outputs/factor_discovery/small_emotion/q1_profile_rank2_broad_post_1_22_oos_20260518 --max-rows 0 --minimum-event-count 200 --minimum-event-month-count 24 --minimum-oos-event-count 50 --max-falsifier-events 5000 --exclude-stale-price-events
make factor-discovery-small-emotion-promotion-gate
make factor-discovery-small-emotion-q2-intake
make factor-discovery-small-emotion-q2-survival
make factor-discovery-small-emotion-q2-optimizer-dry-run
make factor-discovery-small-emotion-q2-complete
make factor-discovery-small-emotion-q2-portfolio-replay
make factor-discovery-small-emotion-q2-factor-exposure-audit
make factor-discovery-small-emotion-q2-robustness-audit
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_insider_disclosure_d2.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_insider_disclosure_d2_real.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_insider_disclosure_d2_sell_contrast.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_insider_disclosure_plan_flag_audit.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_insider_disclosure_plan_flag_repair.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_eightk_subtype_d2.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_eightk_subtype_d2_real.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_eightk_wrds_market_rescue.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_insider_disclosure_d3_signal_builder.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_insider_disclosure_q1_evidence.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_insider_disclosure_q1_label_rescue.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_d2.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src:projects/multifactor_alpha_validation/factor_discovery_sandbox/tests poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_full_replay.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_direction_remap.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src:projects/multifactor_alpha_validation/factor_discovery_sandbox/tests poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_exploratory_sweep.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src:projects/multifactor_alpha_validation/factor_discovery_sandbox/tests poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_top_pocket_replay.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src:projects/multifactor_alpha_validation/factor_discovery_sandbox/tests poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_sharpening_sweep.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src:projects/multifactor_alpha_validation/factor_discovery_sandbox/tests poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_sharpened_top_pocket_replay.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src:projects/multifactor_alpha_validation/factor_discovery_sandbox/tests poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_leaf_search.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src:projects/multifactor_alpha_validation/factor_discovery_sandbox/tests poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_full_market_overfit_lab.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src:projects/multifactor_alpha_validation/factor_discovery_sandbox/tests poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_freeze_validation.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_d3_charter.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src:projects/multifactor_alpha_validation/factor_discovery_sandbox/tests poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_measurement_spec.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src:projects/multifactor_alpha_validation/factor_discovery_sandbox/tests poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_q1_oos.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_small_emotion_q2_candidate_intake.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_small_emotion_q2_factor_exposure_audit.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_small_emotion_q2_robustness_audit.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_track_a_forensic_workflow.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/factor_discovery_sandbox/src poetry run pytest projects/multifactor_alpha_validation/factor_discovery_sandbox/tests -q
```

Formal multifactor research-mode preflight smoke and tests:

```bash
make multifactor-research-mode-preflight
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_week1_contracts.py projects/multifactor_alpha_validation/tests/test_research_mode_preflight.py -q
```

WRDS multifactor ingest config smoke and tests:

```bash
make multifactor-wrds-config-check
make multifactor-external-source-check
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_research_mode_preflight.py projects/multifactor_alpha_validation/tests/test_wrds_ingest.py projects/multifactor_alpha_validation/tests/test_external_dataset_source_adapter.py -q
```

WRDS monthly PIT real dataset dry run smoke and tests, after the local WRDS
monthly bundle exists under `data/cache/wrds_multifactor/nasdaq100/`:

```bash
make multifactor-real-dataset-dry-run
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_real_dataset_dry_run.py -q
```

Multifactor research dataset onboarding smoke and tests:

```bash
make multifactor-research-universe
make multifactor-research-panels
make multifactor-research-delistings
make multifactor-first-research-dry-run
make multifactor-rolling-oos-validation
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_research_dataset_onboarding.py projects/multifactor_alpha_validation/tests/test_research_dry_run.py projects/multifactor_alpha_validation/tests/test_rolling_oos_validation.py -q
```

Standalone Multi-Factor Alpha Validation Engine smoke and tests:

```bash
make factor-validate
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests -q
```

Multifactor strict residual evidence closeout smoke and tests:

```bash
make multifactor-strict-residual-closeout
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_strict_residual_closeout.py -q
```

Multifactor fixed failure diagnosis report smoke and tests:

```bash
make multifactor-failure-diagnosis-report
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_failure_diagnosis_report.py -q
```

Multifactor portfolio component gate smoke and tests:

```bash
make multifactor-portfolio-component-gate
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_portfolio_component_gate.py -q
```

Multifactor candidate filter audit and component resurrection smoke and tests:

```bash
make multifactor-candidate-filter-audit
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_candidate_filter_audit.py -q
```

Multifactor portfolio-level diagnostic ensemble validation smoke and tests:

```bash
make multifactor-portfolio-validation
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_portfolio_validation.py -q
```

Multifactor portfolio assembly audit smoke and tests:

```bash
make multifactor-portfolio-assembly-audit
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_portfolio_assembly_audit.py -q
```

Multifactor component OOS availability expansion smoke and tests:

```bash
make multifactor-component-oos-observations
make multifactor-component-oos-availability
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_component_oos_observation_expansion.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_component_oos_availability.py -q
```

Multifactor post-portfolio contribution / ablation smoke and tests:

```bash
make multifactor-portfolio-contribution
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/multifactor_alpha_validation/src poetry run pytest projects/multifactor_alpha_validation/tests/test_portfolio_contribution.py -q
```

Typed alpha dashboard readability tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_typed_alpha_dashboard_readability.py -q
```

Typed AlphaView contract tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_alpha_view_contract.py -q
```

Event-aware alpha evaluation contract tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_event_alpha_evaluation_contract.py -q
```

Alpha Projection Bridge v2 tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_alpha_projection_bridge_v2.py -q
```

Paper overlay readiness tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_paper_overlay_readiness.py -q
```

Typed SUE alpha pilot tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/typed_alpha_pilot/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run pytest projects/typed_alpha_pilot/tests -q
```

## Q1 Project

Run when touching `projects/agentic_alpha_triage`:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run pytest projects/agentic_alpha_triage/tests -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/validate_examples.py
```

The Q1 example validation script also validates committed evaluator-plan manifests.

CLI dry-run smoke, when touching the Q1 evaluator planner or wrapper:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator.py --fixture projects/agentic_alpha_triage/examples/evaluator_fixtures/valid/guidance_raise_drift.yaml --event-registry-dir projects/agentic_alpha_triage/examples/event_registry/valid
```

Rejected-plan audit JSON smoke, when touching Q1 evaluator rejection handling:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator.py --fixture projects/agentic_alpha_triage/examples/evaluator_fixtures/invalid/guidance_raise_forward_return_leakage.yaml --event-registry-dir projects/agentic_alpha_triage/examples/event_registry/valid --emit-rejected-json --indent 0
```

Batch manifest dry-run smoke, when touching Q1 evaluator manifest wrappers:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator_manifest.py --manifest projects/agentic_alpha_triage/examples/evaluator_plan_manifest.yaml --indent 0
```

Batch manifest summary smoke, when touching Q1 evaluator manifest summary output:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run python projects/agentic_alpha_triage/scripts/plan_evaluator_manifest.py --manifest projects/agentic_alpha_triage/examples/evaluator_plan_manifest.yaml --summary --indent 0
```

Q1 batch contract note is guarded by:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/agentic_alpha_triage/src poetry run pytest projects/agentic_alpha_triage/tests/test_evaluator_batch_contract_doc.py -q
```

## Q2 Project

Run when touching `projects/execution_aware_optimizer`:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests -q
```

Typed Q2 execution matrix contract test:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_typed_execution_matrix.py -q
```

Typed Q2 local adapter contract test:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_typed_portfolioos_adapter.py -q
```

Typed expected-return injection fixture test:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_typed_expected_return_injection.py -q
```

Smoke scripts, default non-execution mode:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_alpha_decay_ladder.py --config projects/execution_aware_optimizer/configs/alpha_decay_ladder.yaml --output projects/execution_aware_optimizer/reports/alpha_decay_ladder_results.csv --report projects/execution_aware_optimizer/reports/execution_aware_optimizer_report.md
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_cost_sensitivity.py --config projects/execution_aware_optimizer/configs/cost_sensitivity.yaml --output projects/execution_aware_optimizer/reports/cost_sensitivity_results.csv
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_constraint_diagnostics.py --config projects/execution_aware_optimizer/configs/alpha_decay_ladder.yaml --output projects/execution_aware_optimizer/reports/constraint_diagnostics.json
```

Execution matrix smoke, default non-execution mode:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_execution_matrix.py --config projects/execution_aware_optimizer/configs/execution_matrix.yaml --output /tmp/portfolioos_q2_execution_matrix/execution_matrix.csv --summary-output /tmp/portfolioos_q2_execution_matrix/robustness_summary.json --report /tmp/portfolioos_q2_execution_matrix/execution_report.md
```

Explicit local executed fixture report smoke, only when touching that opt-in fixture/report path:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run python projects/execution_aware_optimizer/scripts/run_alpha_decay_ladder.py --config projects/execution_aware_optimizer/configs/local_executed_fixture_report.yaml --output /tmp/portfolioos_q2_local_executed_fixture/alpha_decay_ladder_results.csv --report /tmp/portfolioos_q2_local_executed_fixture/local_executed_fixture_report.md
```

Typed Q2 local adapter smoke, only when touching Phase 47 adapter paths:

```bash
make typed-q2-adapter-fixture
```

Typed expected-return injection smoke, only when touching Phase 48 injection paths:

```bash
make typed-expected-return-injection-fixture
```

Typed optimizer response acceptance smoke, only when touching Phase 49 optimizer response paths:

```bash
make typed-optimizer-response-acceptance
```

Small-emotion Q2 execution-survival closeout smoke, only when touching this
candidate Q2 path:

```bash
make factor-discovery-small-emotion-q2-intake
make factor-discovery-small-emotion-q2-survival
make factor-discovery-small-emotion-q2-optimizer-dry-run
make factor-discovery-small-emotion-q2-complete
make factor-discovery-small-emotion-q2-portfolio-replay
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_small_emotion_q2_candidate_intake.py projects/execution_aware_optimizer/tests/test_small_emotion_q2_execution_survival.py projects/execution_aware_optimizer/tests/test_small_emotion_q2_optimizer_dry_run.py projects/execution_aware_optimizer/tests/test_small_emotion_q2_complete.py projects/execution_aware_optimizer/tests/test_small_emotion_q2_portfolio_replay.py -q
```

SUE typed Q2 survival smoke, only when touching Phase 50 SUE survival paths:

```bash
make sue-typed-q2-survival
```

SUE execution-survival attribution smoke, only when touching Phase 51 attribution paths:

```bash
make sue-survival-attribution
```

Expanded SUE typed-Q2 candidate smoke and tests, only when touching Phase 56A paths:

```bash
make sue-expanded-typed-q2-survival
PYTHONDONTWRITEBYTECODE=1 poetry run python scripts/build_sue_expanded_q2_attribution.py
make alpha-registry-v2
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_sue_expanded_typed_q2_survival.py -q
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_alpha_registry_v2.py -q
```

SUE optimizer input bridge smoke and tests, only when touching explicit Reopen-O1 bridge paths:

```bash
make sue-optimizer-input-bridge-fixture
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run pytest tests/test_typed_alpha_optimizer_input_bridge.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_sue_optimizer_input_bridge_fixture.py -q
```

WRDS PIT-labeled historical SUE event panel builder smoke/full-audit and tests, only when touching explicit Reopen-H1A paths:

```bash
make sue-historical-event-panel-smoke
PGUSER=<wrds_username> make sue-historical-linkage-rescue
PGUSER=<wrds_username> make sue-historical-crsp-price-extract
make sue-historical-event-panel-full-audit
PGUSER=<wrds_username> make sue-historical-event-panel-expanded
make sue-historical-event-evidence-grid
make sue-coverage-linkage-price-diagnostics
make sue-score-definition-diagnostics
make sue-score-definition-gate
make sue-placebo-failure-attribution
make sue-regime-filter-placebo-check
make sue-event-timing-anchor-audit
make sue-announcement-timestamp-policy-audit
make sue-timestamp-source-extract
make sue-timestamp-enrichment
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run pytest tests/test_sue_historical_event_panel.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run pytest tests/test_sue_historical_event_evidence_grid.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run pytest tests/test_sue_coverage_linkage_price_diagnostics.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run pytest tests/test_sue_linkage_rescue.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run pytest tests/test_sue_score_definition_diagnostics.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run pytest tests/test_sue_score_definition_gate.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run pytest tests/test_sue_placebo_failure_attribution.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run pytest tests/test_sue_regime_filter_placebo_check.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run pytest tests/test_sue_event_timing_anchor_audit.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run pytest tests/test_sue_announcement_timestamp_policy.py -q
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src poetry run pytest tests/test_sue_timestamp_enrichment.py -q
```

Revision marginal-value gate tests:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/execution_aware_optimizer/src poetry run pytest projects/execution_aware_optimizer/tests/test_revision_marginal_value_gate.py -q
```

Revision marginal-value gate smoke, only when touching Phase 52 gate paths:

```bash
make revision-marginal-value-gate
```

## Evidence Bundle Project

Run when touching `projects/evidence_bundle`:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=projects/evidence_bundle/src poetry run pytest projects/evidence_bundle/tests -q
```

## Promotion Gate Project

Run when touching `projects/promotion_gate` or the Q1-to-Q2 handoff contract:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/promotion_gate/src:projects/evidence_bundle/src poetry run pytest projects/promotion_gate/tests -q
```

## Audit Report Project

Run when touching `projects/audit_report` or unified report generation:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run pytest projects/audit_report/tests -q
```

Demo audit report smoke:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run python projects/audit_report/scripts/build_demo_audit_report.py --manifest projects/audit_report/examples/demo_audit_manifest.yaml --output reports/demo_audit_report.md --provenance-output /tmp/portfolioos_demo_run_manifest.json
```

Demo audit report smoke with structured trace:

```bash
PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src poetry run python projects/audit_report/scripts/build_demo_audit_report.py --manifest projects/audit_report/examples/demo_audit_manifest.yaml --output reports/demo_audit_report.md --provenance-output /tmp/portfolioos_demo_run_manifest.json --trace-jsonl /tmp/portfolioos_demo_trace.jsonl
```

## PortfolioOS Core

Decision explainability tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_decision_explainability.py -q
```

Provenance manifest tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_provenance_manifest.py -q
```

Structured trace tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_observability_trace.py -q
```

Local batch orchestrator tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_local_batch_orchestrator.py -q
```

Content-addressed cache tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_content_addressed_cache.py -q
```

Read-only artifact service tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_read_only_service.py -q
```

Static dashboard tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_static_dashboard.py -q
```

One-command demo tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_one_command_demo.py -q
```

README packaging tests:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_readme_packaging.py -q
```

Run a focused subset when touching optimizer, cost, execution, backtest, replay, scenario, or alpha bridge code:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest tests/test_objective_integration_mode.py tests/test_solver_objective_decomposition.py tests/test_optimizer.py tests/test_cost.py tests/test_execution_simulation.py tests/test_backtest.py tests/test_alpha_backtest_bridge.py tests/test_event_targets.py tests/test_replay.py tests/test_scenarios.py -q
```

Run the full suite before a broad platform change or release-style handoff:

```bash
PYTHONDONTWRITEBYTECODE=1 poetry run pytest -q
```

## Docs-Only Changes

For docs-only changes, run:

```bash
git diff --check
```

If the docs update changes workflow instructions, also run the relevant project tests for the affected project area.

## External Services

Do not run live Alpaca, FMP, WRDS, Tushare, or other paid/external-service workflows unless the user explicitly asks and required credentials are present in the environment.
