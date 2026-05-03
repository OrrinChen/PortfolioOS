PYTHON ?= poetry run python
PYTEST ?= poetry run pytest
PYTHONPATH_Q1 := projects/agentic_alpha_triage/src
PYTHONPATH_AUDIT := src:projects/audit_report/src:projects/agentic_alpha_triage/src:projects/evidence_bundle/src:projects/promotion_gate/src:projects/execution_aware_optimizer/src

.PHONY: test lint validate-examples audit-report demo no-network validate

test:
	PYTHONDONTWRITEBYTECODE=1 $(PYTEST) -q

lint:
	git diff --check

validate-examples:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_Q1) $(PYTHON) projects/agentic_alpha_triage/scripts/validate_examples.py

audit-report:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_AUDIT) $(PYTHON) projects/audit_report/scripts/build_demo_audit_report.py --manifest projects/audit_report/examples/demo_audit_manifest.yaml --output reports/demo_audit_report.md --provenance-output /tmp/portfolioos_demo_run_manifest.json --trace-jsonl /tmp/portfolioos_demo_trace.jsonl

demo: audit-report

no-network:
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=src $(PYTHON) scripts/devtools/no_network_guard.py

validate: lint no-network validate-examples audit-report
	PYTHONDONTWRITEBYTECODE=1 $(PYTEST) tests/test_ci_regression_hardening.py tests/test_no_network_guard.py tests/test_schema_backward_compatibility.py tests/test_forbidden_output_guards.py tests/test_observability_trace.py tests/test_provenance_manifest.py tests/test_decision_explainability.py tests/test_local_batch_orchestrator.py -q
	PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=$(PYTHONPATH_AUDIT) $(PYTEST) projects/audit_report/tests -q
