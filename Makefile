PYTHON ?= poetry run python
PYTEST ?= poetry run pytest
PYTHONPATH_Q1 := projects/agentic_alpha_triage/src
PYTHONPATH_AUDIT := src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src
PYTHONPATH_PROMOTION := src:projects/promotion_gate/src:projects/evidence_bundle/src
PYTHONPATH_Q2 := src:projects/execution_aware_optimizer/src
PYTHONPATH_TYPED_PILOT := src:projects/typed_alpha_pilot/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src
PYTHONPATH_FACTOR_DISCOVERY := projects/multifactor_alpha_validation/factor_discovery_sandbox/src
PYTHONPATH_MULTIFACTOR := projects/multifactor_alpha_validation/src

.PHONY: test lint validate-examples audit-report demo demo-v2 portfolio-quant-walk-forward typed-alpha-closeout alpha-registry-v2 research-track-boundaries track-a-forensic-workflow-fixture factor-discovery-teaching-baseline factor-discovery-design-layer factor-discovery-design-d1 factor-discovery-design-d2 factor-discovery-insider-d2-observability factor-discovery-insider-d2-real-observability factor-discovery-insider-d2-sell-contrast factor-discovery-insider-plan-flag-audit factor-discovery-insider-plan-flag-repair factor-discovery-8k-d2-observability factor-discovery-8k-d2-real-observability factor-discovery-8k-wrds-market-rescue factor-discovery-insider-d3-signal-builder factor-discovery-insider-q1-evidence factor-discovery-insider-q1-label-rescue factor-discovery-fd-wide-design-audit factor-discovery-factor-specs factor-discovery-rolling-oos factor-discovery-marginal-value-gate factor-discovery-allocator factor-discovery-survival factor-discovery-real-data-validation factor-discovery-real-data-validation-daily factor-discovery-real-factor-replay factor-discovery-real-rolling-oos factor-discovery-real-placebo-robustness factor-discovery-real-failure-diagnosis factor-discovery-formula-mechanism-audit factor-discovery-weighting-reliability factor-discovery-momentum-low-vol-candidate factor-discovery-revision-confirmed-alpha factor-discovery-small-cap-wrds-pull factor-discovery-small-cap-quality-pull factor-discovery-small-cap-data-admission factor-discovery-small-cap-quality-residual-momentum factor-discovery-small-cap-dominance-diagnosis factor-discovery-small-cap-s4-2 factor-discovery-small-cap-s4-3 factor-discovery-small-emotion-d2 factor-discovery-small-emotion-full-replay factor-discovery-small-emotion-direction-remap factor-discovery-small-emotion-exploratory-sweep factor-discovery-small-emotion-top-pocket-replay factor-discovery-small-emotion-d3-charter factor-discovery-small-emotion-sharpening-sweep factor-discovery-small-emotion-sharpened-top-pocket-replay factor-discovery-small-emotion-sharpened-d3-charter factor-discovery-small-emotion-leaf-search factor-discovery-small-emotion-full-market-overfit-lab factor-discovery-small-emotion-full-market-feature-cache factor-discovery-small-emotion-full-market-cached-replay factor-discovery-small-emotion-full-market-cost-clean-cached-replay factor-discovery-small-emotion-full-market-cost-stale-clean-cached-replay factor-discovery-small-emotion-freeze-validation factor-discovery-small-emotion-measurement-spec factor-discovery-small-emotion-q1-oos factor-discovery-small-emotion-promotion-gate factor-discovery-small-emotion-q2-intake factor-discovery-small-emotion-q2-survival factor-discovery-small-emotion-q2-optimizer-dry-run factor-discovery-small-emotion-q2-complete factor-discovery-small-emotion-q2-portfolio-replay factor-discovery-small-emotion-q2-factor-exposure-audit factor-discovery-small-emotion-q2-robustness-audit multifactor-research-mode-preflight multifactor-wrds-config-check multifactor-external-source-check multifactor-research-universe multifactor-research-panels multifactor-research-delistings multifactor-first-research-dry-run multifactor-rolling-oos-validation multifactor-real-dataset-dry-run multifactor-strict-residual-closeout multifactor-failure-diagnosis-report multifactor-portfolio-component-gate multifactor-candidate-filter-audit multifactor-portfolio-validation multifactor-portfolio-assembly-audit multifactor-component-oos-availability multifactor-component-oos-observations multifactor-portfolio-contribution factor-spec-validate factor-signals factor-q1 factor-redundancy factor-shrinkage factor-allocator factor-survival factor-registry factor-report factor-dashboard factor-release-manifest factor-validate typed-q2-adapter-fixture typed-expected-return-injection-fixture typed-optimizer-response-acceptance sue-typed-q2-survival sue-survival-attribution sue-expanded-typed-q2-survival sue-optimizer-input-bridge-fixture sue-historical-event-panel-smoke sue-historical-event-panel-full-audit sue-historical-linkage-rescue sue-historical-crsp-price-extract sue-historical-event-panel-expanded sue-historical-event-evidence-grid sue-coverage-linkage-price-diagnostics sue-score-definition-diagnostics sue-score-definition-gate sue-placebo-failure-attribution sue-regime-filter-placebo-check sue-event-timing-anchor-audit sue-announcement-timestamp-policy-audit sue-timestamp-source-extract sue-timestamp-enrichment revision-marginal-value-gate no-network validate

test:
	PYTHONDONTWRITEBYTECODE=1 $(PYTEST) -q

lint:
	git diff --check

validate-examples:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q1) $(PYTHON) projects/agentic_alpha_triage/scripts/validate_examples.py

audit-report:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_AUDIT) $(PYTHON) projects/audit_report/scripts/build_demo_audit_report.py --manifest projects/audit_report/examples/demo_audit_manifest.yaml --output reports/demo_audit_report.md --provenance-output /tmp/portfolioos_demo_run_manifest.json --trace-jsonl /tmp/portfolioos_demo_trace.jsonl

demo:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_AUDIT) $(PYTHON) scripts/run_portfolioos_demo.py --output-dir outputs/demo

demo-v2:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_TYPED_PILOT) $(PYTHON) scripts/run_portfolioos_demo_v2.py --output-dir outputs/demo_v2

portfolio-quant-walk-forward:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/run_portfolio_quant_walk_forward.py --output-dir outputs/portfolio_quant_walk_forward

typed-alpha-closeout:
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/build_typed_alpha_closeout_report.py --output reports/typed_alpha_closeout_report.md

alpha-registry-v2:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/build_alpha_registry_v2.py

research-track-boundaries:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTEST) tests/test_research_track_boundaries.py -q

track-a-forensic-workflow-fixture:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_track_a_forensic_workflow.py

factor-discovery-teaching-baseline:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_teaching_baseline.py

factor-discovery-design-layer:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_design_layer.py

factor-discovery-design-d1:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_design_d1.py

factor-discovery-design-d2:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_design_d2.py

factor-discovery-insider-d2-observability:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_insider_disclosure_d2_observability.py

factor-discovery-insider-d2-real-observability:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_insider_disclosure_d2_real_observability.py

factor-discovery-insider-d2-sell-contrast:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_insider_disclosure_d2_sell_contrast.py

factor-discovery-insider-plan-flag-audit:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_insider_disclosure_plan_flag_audit.py

factor-discovery-insider-plan-flag-repair:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_insider_disclosure_plan_flag_repair.py

factor-discovery-8k-d2-observability:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_8k_subtype_d2.py

factor-discovery-8k-d2-real-observability:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_8k_subtype_d2_real.py

factor-discovery-8k-wrds-market-rescue:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/rescue_factor_discovery_8k_wrds_market_coverage.py

factor-discovery-insider-d3-signal-builder:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_insider_disclosure_d3_signal_builder.py

factor-discovery-insider-q1-evidence:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_insider_disclosure_q1_evidence.py

factor-discovery-insider-q1-label-rescue:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_insider_disclosure_q1_label_rescue.py

factor-discovery-fd-wide-design-audit:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_fd_wide_design_audit.py

factor-discovery-factor-specs:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_factor_specs.py

factor-discovery-rolling-oos:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_rolling_oos.py

factor-discovery-marginal-value-gate:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_marginal_value_gate.py

factor-discovery-allocator:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_allocator.py

factor-discovery-survival:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_survival.py

factor-discovery-real-data-validation:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_real_data_validation.py

factor-discovery-real-data-validation-daily:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_real_data_validation.py --manifest data/cache/wrds_multifactor/nasdaq100_daily_full10/standardized/research_mode_dataset_manifest.yaml --output-dir outputs/factor_discovery/real_data_daily

factor-discovery-real-factor-replay:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_real_factor_replay.py

factor-discovery-real-rolling-oos:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_real_rolling_oos.py

factor-discovery-real-placebo-robustness:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_real_placebo_robustness.py

factor-discovery-real-failure-diagnosis:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_real_failure_diagnosis.py

factor-discovery-formula-mechanism-audit:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_formula_mechanism_audit.py

factor-discovery-weighting-reliability:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_weighting_reliability.py

factor-discovery-momentum-low-vol-candidate:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_momentum_low_vol_candidate.py

factor-discovery-revision-confirmed-alpha:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_revision_confirmed_earnings_underreaction.py

factor-discovery-small-cap-wrds-pull:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_cap_wrds_pull.py

factor-discovery-small-cap-quality-pull:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_cap_quality_pull.py

factor-discovery-small-cap-data-admission:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_cap_data_admission.py

factor-discovery-small-cap-quality-residual-momentum:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_cap_quality_residual_momentum.py

factor-discovery-small-cap-dominance-diagnosis:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_cap_dominance_diagnosis.py

factor-discovery-small-cap-s4-2:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_cap_s4_2.py

factor-discovery-small-cap-s4-3:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_cap_s4_3.py

factor-discovery-small-emotion-d2:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_d2.py

factor-discovery-small-emotion-full-replay:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_full_replay.py

factor-discovery-small-emotion-direction-remap:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_direction_remap.py

factor-discovery-small-emotion-exploratory-sweep:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_exploratory_sweep.py

factor-discovery-small-emotion-top-pocket-replay:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_top_pocket_replay.py

factor-discovery-small-emotion-d3-charter:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_d3_charter.py

factor-discovery-small-emotion-sharpening-sweep:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_sharpening_sweep.py

factor-discovery-small-emotion-sharpened-top-pocket-replay:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_sharpened_top_pocket_replay.py

factor-discovery-small-emotion-sharpened-d3-charter:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_d3_charter.py --freeze-review outputs/factor_discovery/small_emotion/e0_sharpened_top_pocket_replay/sharpened_candidate_freeze_review.json --top-pocket-summary outputs/factor_discovery/small_emotion/e0_sharpened_top_pocket_replay/sharpened_top_pocket_replay_summary.json --chunk-metrics outputs/factor_discovery/small_emotion/e0_sharpened_top_pocket_replay/sharpened_top_pocket_chunk_metrics.csv --output-dir outputs/factor_discovery/small_emotion/d3_sharpened_up_shock_reversal_charter

factor-discovery-small-emotion-leaf-search:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_leaf_search.py

factor-discovery-small-emotion-full-market-overfit-lab:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_full_market_overfit_lab.py

factor-discovery-small-emotion-full-market-feature-cache:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_full_market_overfit_lab.py --max-rows 0 --feature-cache-dir data/cache/factor_discovery/small_emotion/e1_full_market_overfit_lab_full --cache-only --force-rebuild-cache --output-dir outputs/factor_discovery/small_emotion/e1_full_market_feature_cache_full

factor-discovery-small-emotion-full-market-cached-replay:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_full_market_overfit_lab.py --max-rows 0 --feature-cache-dir data/cache/factor_discovery/small_emotion/e1_full_market_overfit_lab_full --output-dir outputs/factor_discovery/small_emotion/e1_full_market_overfit_lab_full_cached

factor-discovery-small-emotion-full-market-cost-clean-cached-replay:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_full_market_overfit_lab.py --max-rows 0 --feature-cache-dir data/cache/factor_discovery/small_emotion/e1_full_market_overfit_lab_full --output-dir outputs/factor_discovery/small_emotion/e1_full_market_cost_clean_cached_replay --shock-thresholds 0.05,0.08,0.10,0.15,0.20 --volume-spike-thresholds 1.0,1.5,2.0,3.0,5.0 --adv-min-dollars 250000 --max-depth 5 --beam-width 64 --top-n 100 --exclude-predicates spread_wide,price_under_5,weak_liquidity,liquidity_low

factor-discovery-small-emotion-full-market-cost-stale-clean-cached-replay:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_full_market_overfit_lab.py --max-rows 0 --feature-cache-dir data/cache/factor_discovery/small_emotion/e1_full_market_overfit_lab_full --output-dir outputs/factor_discovery/small_emotion/e1_full_market_cost_stale_clean_cached_replay --shock-thresholds 0.05,0.08,0.10,0.15,0.20 --volume-spike-thresholds 1.0,1.5,2.0,3.0,5.0 --adv-min-dollars 250000 --max-depth 5 --beam-width 64 --top-n 100 --exclude-predicates spread_wide,price_under_5,weak_liquidity,liquidity_low --exclude-stale-price-events

factor-discovery-small-emotion-freeze-validation:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_freeze_validation.py

factor-discovery-small-emotion-measurement-spec:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_measurement_spec.py

factor-discovery-small-emotion-q1-oos:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_q1_oos.py

factor-discovery-small-emotion-promotion-gate:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_small_emotion_promotion_gate.py --q1-max-rows 0

factor-discovery-small-emotion-q2-intake:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_factor_discovery_small_emotion_q2_intake.py --candidate 'rank1_micro_post_1_22|outputs/factor_discovery/small_emotion/freeze_02_cost_stale_clean_profile_score_top_validation_20260518/measurement_spec.yaml|outputs/factor_discovery/small_emotion/q1_profile_rank1_micro_post_1_22_oos_20260519|outputs/factor_discovery/small_emotion/pg_profile_rank1_micro_post_1_22_20260519|5837c53cf1a5142d321ce4d83584ef77f04340947b1d5477dfb53883463be0f7' --candidate 'rank2_broad_post_1_22|outputs/factor_discovery/small_emotion/freeze_02_cost_stale_clean_profile_rank2_validation_20260518/measurement_spec.yaml|outputs/factor_discovery/small_emotion/q1_profile_rank2_broad_post_1_22_oos_20260518|outputs/factor_discovery/small_emotion/pg_profile_rank2_broad_post_1_22_20260519|21cbf8277ed0778a8b0aa1ef473d65bcfa14c202830f4d7d391923dcdd0fd9b9' --candidate 'rank3_broad_post_1_10|outputs/factor_discovery/small_emotion/freeze_02_cost_stale_clean_profile_rank3_validation_20260518/measurement_spec.yaml|outputs/factor_discovery/small_emotion/q1_profile_rank3_broad_post_1_10_oos_20260519|outputs/factor_discovery/small_emotion/pg_profile_rank3_broad_post_1_10_20260519|212944f01ef09e23a07d5807aae0ede1b42fb24d647838e1792cd9889da0d1ba' --output-dir outputs/factor_discovery/small_emotion/q2_candidate_intake_20260519

factor-discovery-small-emotion-q2-survival:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_factor_discovery_small_emotion_q2_survival.py --q2-intake-dir outputs/factor_discovery/small_emotion/q2_candidate_intake_20260519 --output-dir outputs/factor_discovery/small_emotion/q2_execution_survival_20260519

factor-discovery-small-emotion-q2-optimizer-dry-run:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_factor_discovery_small_emotion_q2_optimizer_dry_run.py --q2-survival-dir outputs/factor_discovery/small_emotion/q2_execution_survival_20260519 --output-dir outputs/factor_discovery/small_emotion/q2_optimizer_dry_run_20260520

factor-discovery-small-emotion-q2-complete:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_factor_discovery_small_emotion_q2_complete.py --q2-intake-dir outputs/factor_discovery/small_emotion/q2_candidate_intake_20260519 --q2-survival-dir outputs/factor_discovery/small_emotion/q2_execution_survival_20260519 --optimizer-dry-run-dir outputs/factor_discovery/small_emotion/q2_optimizer_dry_run_20260520 --output-dir outputs/factor_discovery/small_emotion/q2_complete_20260520

factor-discovery-small-emotion-q2-portfolio-replay:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_factor_discovery_small_emotion_q2_portfolio_replay.py --q2-complete-dir outputs/factor_discovery/small_emotion/q2_complete_20260520 --q2-intake-dir outputs/factor_discovery/small_emotion/q2_candidate_intake_20260519 --output-dir outputs/factor_discovery/small_emotion/q2_portfolio_replay_20260520

factor-discovery-small-emotion-q2-factor-exposure-audit:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_factor_discovery_small_emotion_q2_factor_exposure_audit.py --q2-complete-dir outputs/factor_discovery/small_emotion/q2_complete_20260520 --output-dir outputs/factor_discovery/small_emotion/q2_factor_exposure_audit_20260520

factor-discovery-small-emotion-q2-robustness-audit:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_factor_discovery_small_emotion_q2_robustness_audit.py --q2-complete-dir outputs/factor_discovery/small_emotion/q2_complete_20260520 --output-dir outputs/factor_discovery/small_emotion/q2_robustness_audit_20260520

multifactor-research-mode-preflight:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) scripts/run_multifactor_research_mode_preflight.py

multifactor-wrds-config-check:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_wrds_multifactor_ingest.py --config projects/multifactor_alpha_validation/configs/wrds_nasdaq100_research_mode.yaml --check-config-only

multifactor-external-source-check:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_external_dataset_source_adapter.py --require-ready

multifactor-research-universe:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_research_dataset_universe.py

multifactor-research-panels:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_research_dataset_panels.py

multifactor-research-delistings:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_research_dataset_delistings.py

multifactor-first-research-dry-run:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_first_research_dry_run.py

multifactor-rolling-oos-validation:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_rolling_oos_factor_validation.py

multifactor-real-dataset-dry-run:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_real_dataset_dry_run.py

multifactor-strict-residual-closeout:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_strict_residual_closeout.py

multifactor-failure-diagnosis-report:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_failure_diagnosis_report.py

multifactor-portfolio-component-gate:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_portfolio_component_gate.py

multifactor-candidate-filter-audit:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_candidate_filter_audit.py

multifactor-portfolio-validation:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_portfolio_validation.py

multifactor-portfolio-assembly-audit:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_portfolio_assembly_audit.py

multifactor-component-oos-availability:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_component_oos_availability.py

multifactor-component-oos-observations:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_component_oos_observation_expansion.py

multifactor-portfolio-contribution:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_portfolio_contribution.py

factor-spec-validate:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/validate_factor_specs.py

factor-signals:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_factor_signal_builders.py

factor-q1:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_factor_q1_evidence.py

factor-redundancy:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_factor_redundancy_gate.py

factor-shrinkage:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_factor_shrinkage.py

factor-allocator:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_factor_allocator.py

factor-survival:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTHON) projects/multifactor_alpha_validation/scripts/run_factor_survival.py

factor-registry:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR):projects/multifactor_alpha_validation/scripts $(PYTHON) projects/multifactor_alpha_validation/scripts/build_factor_registry.py

factor-report:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR):projects/multifactor_alpha_validation/scripts $(PYTHON) projects/multifactor_alpha_validation/scripts/build_factor_research_report.py

factor-dashboard:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR):projects/multifactor_alpha_validation/scripts $(PYTHON) projects/multifactor_alpha_validation/scripts/render_factor_dashboard.py

factor-release-manifest:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR):projects/multifactor_alpha_validation/scripts $(PYTHON) projects/multifactor_alpha_validation/scripts/build_factor_release_manifest.py

factor-validate: factor-spec-validate factor-signals factor-q1 factor-redundancy factor-shrinkage factor-allocator factor-survival factor-registry factor-report factor-dashboard factor-release-manifest
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_MULTIFACTOR) $(PYTEST) projects/multifactor_alpha_validation/tests -q

typed-q2-adapter-fixture:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_typed_q2_adapter_fixture.py --allow-portfolioos-run

typed-expected-return-injection-fixture:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_typed_expected_return_injection_fixture.py --allow-portfolioos-run

typed-optimizer-response-acceptance:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_typed_optimizer_response_acceptance.py --allow-portfolioos-run

sue-typed-q2-survival:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_sue_typed_q2_survival.py --allow-portfolioos-run

sue-survival-attribution: sue-typed-q2-survival
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/build_sue_typed_q2_survival_attribution.py

sue-expanded-typed-q2-survival:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_sue_expanded_typed_q2_survival.py --allow-portfolioos-run

sue-optimizer-input-bridge-fixture:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_sue_optimizer_input_bridge_fixture.py --allow-typed-alpha-optimizer-injection

sue-historical-event-panel-smoke:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/build_wrds_sue_event_panel.py --mode smoke

sue-historical-event-panel-full-audit:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/build_wrds_sue_event_panel.py --config configs/wrds_sue_event_panel_full.yaml

sue-historical-linkage-rescue:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/rescue_wrds_sue_crsp_links.py --config configs/wrds_sue_linkage_rescue.yaml

sue-historical-crsp-price-extract:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/extract_wrds_sue_crsp_prices.py --config configs/wrds_sue_crsp_price_extract.yaml

sue-historical-event-panel-expanded:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/build_wrds_sue_event_panel_expanded.py --config configs/wrds_sue_event_panel_expanded.yaml

sue-historical-event-evidence-grid:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/run_sue_historical_event_evidence_grid.py --config configs/sue_historical_event_evidence_grid.yaml

sue-coverage-linkage-price-diagnostics:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/run_sue_coverage_linkage_price_diagnostics.py --config configs/sue_coverage_linkage_price_diagnostics.yaml

sue-score-definition-diagnostics:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/run_sue_score_definition_diagnostics.py --config configs/sue_score_definition_diagnostics.yaml

sue-score-definition-gate:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/run_sue_score_definition_gate.py --config configs/sue_score_definition_gate.yaml

sue-placebo-failure-attribution:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/run_sue_placebo_failure_attribution.py --config configs/sue_placebo_failure_attribution.yaml

sue-regime-filter-placebo-check:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/run_sue_regime_filter_placebo_check.py --config configs/sue_regime_filter_placebo_check.yaml

sue-event-timing-anchor-audit:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/run_sue_event_timing_anchor_audit.py --config configs/sue_event_timing_anchor_audit.yaml

sue-announcement-timestamp-policy-audit:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/run_sue_announcement_timestamp_policy_audit.py --config configs/sue_announcement_timestamp_policy.yaml

sue-timestamp-source-extract:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/extract_wrds_sue_timestamp_sources.py --config configs/wrds_sue_timestamp_sources.yaml

sue-timestamp-enrichment:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/run_sue_timestamp_enrichment.py --config configs/sue_timestamp_enrichment.yaml

revision-marginal-value-gate:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_revision_marginal_value_gate.py

no-network:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/devtools/no_network_guard.py

validate: lint no-network validate-examples audit-report
	PYTHONDONTWRITEBYTECODE=1 $(PYTEST) tests/test_ci_regression_hardening.py tests/test_no_network_guard.py tests/test_schema_backward_compatibility.py tests/test_forbidden_output_guards.py tests/test_observability_trace.py tests/test_provenance_manifest.py tests/test_decision_explainability.py tests/test_local_batch_orchestrator.py tests/test_content_addressed_cache.py tests/test_read_only_service.py tests/test_static_dashboard.py tests/test_one_command_demo.py tests/test_demo_v2.py tests/test_typed_alpha_release_candidate.py tests/test_demo_v2_golden_snapshot.py tests/test_typed_alpha_closeout_report.py tests/test_typed_alpha_dashboard_readability.py tests/test_alpha_registry_v2.py tests/test_portfolioos_v1_research_audit_release.py tests/test_portfolioos_v1_maintenance_freeze.py tests/test_readme_packaging.py tests/test_research_track_boundaries.py tests/test_alpha_view_contract.py tests/test_event_alpha_evaluation_contract.py tests/test_alpha_projection_bridge_v2.py tests/test_paper_overlay_readiness.py tests/test_typed_alpha_optimizer_input_bridge.py tests/test_sue_historical_event_panel.py tests/test_sue_historical_event_evidence_grid.py tests/test_sue_coverage_linkage_price_diagnostics.py tests/test_sue_linkage_rescue.py tests/test_sue_score_definition_diagnostics.py tests/test_sue_score_definition_gate.py tests/test_sue_placebo_failure_attribution.py tests/test_sue_regime_filter_placebo_check.py tests/test_sue_event_timing_anchor_audit.py tests/test_sue_announcement_timestamp_policy.py tests/test_sue_timestamp_enrichment.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTEST) projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_track_a_forensic_workflow.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTEST) projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_d2.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY):projects/multifactor_alpha_validation/factor_discovery_sandbox/tests $(PYTEST) projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_full_replay.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTEST) projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_direction_remap.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY):projects/multifactor_alpha_validation/factor_discovery_sandbox/tests $(PYTEST) projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_exploratory_sweep.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY):projects/multifactor_alpha_validation/factor_discovery_sandbox/tests $(PYTEST) projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_top_pocket_replay.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY):projects/multifactor_alpha_validation/factor_discovery_sandbox/tests $(PYTEST) projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_sharpening_sweep.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY):projects/multifactor_alpha_validation/factor_discovery_sandbox/tests $(PYTEST) projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_sharpened_top_pocket_replay.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY):projects/multifactor_alpha_validation/factor_discovery_sandbox/tests $(PYTEST) projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_leaf_search.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTEST) projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_d3_charter.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY):projects/multifactor_alpha_validation/factor_discovery_sandbox/tests $(PYTEST) projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_measurement_spec.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY):projects/multifactor_alpha_validation/factor_discovery_sandbox/tests $(PYTEST) projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_q1_oos.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY):projects/multifactor_alpha_validation/factor_discovery_sandbox/tests $(PYTEST) projects/multifactor_alpha_validation/factor_discovery_sandbox/tests/test_small_emotion_promotion_gate.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_PROMOTION) $(PYTEST) projects/promotion_gate/tests -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTEST) projects/execution_aware_optimizer/tests/test_typed_execution_matrix.py projects/execution_aware_optimizer/tests/test_typed_portfolioos_adapter.py projects/execution_aware_optimizer/tests/test_typed_expected_return_injection.py projects/execution_aware_optimizer/tests/test_typed_optimizer_response.py projects/execution_aware_optimizer/tests/test_sue_typed_q2_survival.py projects/execution_aware_optimizer/tests/test_sue_execution_survival_attribution.py projects/execution_aware_optimizer/tests/test_sue_expanded_typed_q2_survival.py projects/execution_aware_optimizer/tests/test_sue_optimizer_input_bridge_fixture.py projects/execution_aware_optimizer/tests/test_revision_marginal_value_gate.py projects/execution_aware_optimizer/tests/test_small_emotion_q2_candidate_intake.py projects/execution_aware_optimizer/tests/test_small_emotion_q2_execution_survival.py projects/execution_aware_optimizer/tests/test_small_emotion_q2_optimizer_dry_run.py projects/execution_aware_optimizer/tests/test_small_emotion_q2_complete.py projects/execution_aware_optimizer/tests/test_small_emotion_q2_portfolio_replay.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_TYPED_PILOT) $(PYTEST) projects/typed_alpha_pilot/tests -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_AUDIT) $(PYTEST) projects/audit_report/tests -q
