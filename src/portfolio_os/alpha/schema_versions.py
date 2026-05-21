"""Typed alpha contract schema versions.

This module is the single local lockfile for Phase 35-43 typed-alpha artifact
versions. It is intentionally static and does not trigger workflows.
"""

from __future__ import annotations


ALPHA_VIEW_SCHEMA_VERSION = "alpha_view.v1"
EVENT_EVIDENCE_SCHEMA_VERSION = "event_evidence_bundle.v1"
PROJECTION_MANIFEST_SCHEMA_VERSION = "alpha_projection.v2"
PROMOTION_DECISION_V2_SCHEMA_VERSION = "promotion_decision.v2"
Q2_INPUT_CONTRACT_V2_SCHEMA_VERSION = "q2_input_contract.v2"
Q2_TYPED_MATRIX_SCHEMA_VERSION = "q2_typed_matrix.v1"
PAPER_OVERLAY_READINESS_SCHEMA_VERSION = "paper_overlay_readiness.v1"
TYPED_ALPHA_RELEASE_MANIFEST_SCHEMA_VERSION = "typed_alpha_release_manifest.v1"

TYPED_ALPHA_SCHEMA_VERSIONS = {
    "alpha_view": ALPHA_VIEW_SCHEMA_VERSION,
    "event_evidence": EVENT_EVIDENCE_SCHEMA_VERSION,
    "projection_manifest": PROJECTION_MANIFEST_SCHEMA_VERSION,
    "promotion_decision_v2": PROMOTION_DECISION_V2_SCHEMA_VERSION,
    "q2_input_contract_v2": Q2_INPUT_CONTRACT_V2_SCHEMA_VERSION,
    "q2_typed_matrix": Q2_TYPED_MATRIX_SCHEMA_VERSION,
    "paper_overlay_readiness": PAPER_OVERLAY_READINESS_SCHEMA_VERSION,
    "typed_alpha_release_manifest": TYPED_ALPHA_RELEASE_MANIFEST_SCHEMA_VERSION,
}
