PYTHON ?= poetry run python
PYTEST ?= poetry run pytest
PYTHONPATH_Q1 := projects/agentic_alpha_triage/src
PYTHONPATH_AUDIT := src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src
PYTHONPATH_PROMOTION := src:projects/promotion_gate/src:projects/evidence_bundle/src
PYTHONPATH_Q2 := src:projects/execution_aware_optimizer/src
PYTHONPATH_TYPED_PILOT := src:projects/typed_alpha_pilot/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src
PYTHONPATH_FACTOR_DISCOVERY := projects/multifactor_alpha_validation/factor_discovery_sandbox/src
PYTHONPATH_MULTIFACTOR := projects/multifactor_alpha_validation/src

.PHONY: test lint validate-examples audit-report demo demo-v2 typed-alpha-closeout alpha-registry-v2 factor-discovery-teaching-baseline factor-discovery-factor-specs factor-discovery-rolling-oos factor-discovery-marginal-value-gate factor-discovery-allocator factor-discovery-survival multifactor-research-mode-preflight multifactor-wrds-config-check multifactor-external-source-check multifactor-research-universe multifactor-research-panels multifactor-research-delistings multifactor-first-research-dry-run multifactor-rolling-oos-validation factor-spec-validate factor-signals factor-q1 factor-redundancy factor-shrinkage factor-allocator factor-survival factor-registry factor-report factor-dashboard factor-release-manifest factor-validate typed-q2-adapter-fixture typed-expected-return-injection-fixture typed-optimizer-response-acceptance sue-typed-q2-survival sue-survival-attribution sue-expanded-typed-q2-survival sue-optimizer-input-bridge-fixture sue-historical-event-panel-smoke revision-marginal-value-gate no-network validate

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

typed-alpha-closeout:
	PYTHONDONTWRITEBYTECODE=1 $(PYTHON) scripts/build_typed_alpha_closeout_report.py --output reports/typed_alpha_closeout_report.md

alpha-registry-v2:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/build_alpha_registry_v2.py

factor-discovery-teaching-baseline:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_FACTOR_DISCOVERY) $(PYTHON) scripts/run_factor_discovery_teaching_baseline.py

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

revision-marginal-value-gate:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_revision_marginal_value_gate.py

no-network:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/devtools/no_network_guard.py

validate: lint no-network validate-examples audit-report
	PYTHONDONTWRITEBYTECODE=1 $(PYTEST) tests/test_ci_regression_hardening.py tests/test_no_network_guard.py tests/test_schema_backward_compatibility.py tests/test_forbidden_output_guards.py tests/test_observability_trace.py tests/test_provenance_manifest.py tests/test_decision_explainability.py tests/test_local_batch_orchestrator.py tests/test_content_addressed_cache.py tests/test_read_only_service.py tests/test_static_dashboard.py tests/test_one_command_demo.py tests/test_demo_v2.py tests/test_typed_alpha_release_candidate.py tests/test_demo_v2_golden_snapshot.py tests/test_typed_alpha_closeout_report.py tests/test_typed_alpha_dashboard_readability.py tests/test_alpha_registry_v2.py tests/test_portfolioos_v1_research_audit_release.py tests/test_portfolioos_v1_maintenance_freeze.py tests/test_readme_packaging.py tests/test_alpha_view_contract.py tests/test_event_alpha_evaluation_contract.py tests/test_alpha_projection_bridge_v2.py tests/test_paper_overlay_readiness.py tests/test_typed_alpha_optimizer_input_bridge.py tests/test_sue_historical_event_panel.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_PROMOTION) $(PYTEST) projects/promotion_gate/tests -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTEST) projects/execution_aware_optimizer/tests/test_typed_execution_matrix.py projects/execution_aware_optimizer/tests/test_typed_portfolioos_adapter.py projects/execution_aware_optimizer/tests/test_typed_expected_return_injection.py projects/execution_aware_optimizer/tests/test_typed_optimizer_response.py projects/execution_aware_optimizer/tests/test_sue_typed_q2_survival.py projects/execution_aware_optimizer/tests/test_sue_execution_survival_attribution.py projects/execution_aware_optimizer/tests/test_sue_expanded_typed_q2_survival.py projects/execution_aware_optimizer/tests/test_sue_optimizer_input_bridge_fixture.py projects/execution_aware_optimizer/tests/test_revision_marginal_value_gate.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_TYPED_PILOT) $(PYTEST) projects/typed_alpha_pilot/tests -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_AUDIT) $(PYTEST) projects/audit_report/tests -q
