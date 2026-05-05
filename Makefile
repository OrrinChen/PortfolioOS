PYTHON ?= poetry run python
PYTEST ?= poetry run pytest
PYTHONPATH_Q1 := projects/agentic_alpha_triage/src
PYTHONPATH_AUDIT := src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src
PYTHONPATH_PROMOTION := src:projects/promotion_gate/src:projects/evidence_bundle/src
PYTHONPATH_Q2 := src:projects/execution_aware_optimizer/src
PYTHONPATH_TYPED_PILOT := src:projects/typed_alpha_pilot/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src

.PHONY: test lint validate-examples audit-report demo demo-v2 typed-alpha-closeout typed-q2-adapter-fixture typed-expected-return-injection-fixture typed-optimizer-response-acceptance sue-typed-q2-survival sue-survival-attribution revision-marginal-value-gate no-network validate

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

revision-marginal-value-gate:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTHON) scripts/run_revision_marginal_value_gate.py

no-network:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/devtools/no_network_guard.py

validate: lint no-network validate-examples audit-report
	PYTHONDONTWRITEBYTECODE=1 $(PYTEST) tests/test_ci_regression_hardening.py tests/test_no_network_guard.py tests/test_schema_backward_compatibility.py tests/test_forbidden_output_guards.py tests/test_observability_trace.py tests/test_provenance_manifest.py tests/test_decision_explainability.py tests/test_local_batch_orchestrator.py tests/test_content_addressed_cache.py tests/test_read_only_service.py tests/test_static_dashboard.py tests/test_one_command_demo.py tests/test_demo_v2.py tests/test_typed_alpha_release_candidate.py tests/test_demo_v2_golden_snapshot.py tests/test_typed_alpha_closeout_report.py tests/test_typed_alpha_dashboard_readability.py tests/test_readme_packaging.py tests/test_alpha_view_contract.py tests/test_event_alpha_evaluation_contract.py tests/test_alpha_projection_bridge_v2.py tests/test_paper_overlay_readiness.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_PROMOTION) $(PYTEST) projects/promotion_gate/tests -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q2) $(PYTEST) projects/execution_aware_optimizer/tests/test_typed_execution_matrix.py projects/execution_aware_optimizer/tests/test_typed_portfolioos_adapter.py projects/execution_aware_optimizer/tests/test_typed_expected_return_injection.py projects/execution_aware_optimizer/tests/test_typed_optimizer_response.py projects/execution_aware_optimizer/tests/test_sue_typed_q2_survival.py projects/execution_aware_optimizer/tests/test_sue_execution_survival_attribution.py projects/execution_aware_optimizer/tests/test_revision_marginal_value_gate.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_TYPED_PILOT) $(PYTEST) projects/typed_alpha_pilot/tests -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_AUDIT) $(PYTEST) projects/audit_report/tests -q
