from __future__ import annotations

from pathlib import Path

from portfolio_os.service import ReadOnlyArtifactService


def test_read_only_service_exposes_artifact_endpoints(tmp_path: Path) -> None:
    _write_json(tmp_path / "runs" / "run-1.json", {"run_id": "run-1", "status": "complete"})
    _write_json(tmp_path / "bundles" / "bundle-1.json", {"bundle_id": "bundle-1"})
    _write_json(tmp_path / "decisions" / "bundle-1.json", {"bundle_id": "bundle-1", "decision": "reject"})
    _write_text(tmp_path / "reports" / "run-1.md", "# Run 1\n")
    service = ReadOnlyArtifactService(tmp_path)

    assert service.handle("GET", "/health").body == {"status": "ok", "mode": "read_only"}
    assert service.handle("GET", "/runs").body == {"runs": ["run-1"]}
    assert service.handle("GET", "/runs/run-1").body == {"run_id": "run-1", "status": "complete"}
    assert service.handle("GET", "/bundles/bundle-1").body == {"bundle_id": "bundle-1"}
    assert service.handle("GET", "/decisions/bundle-1").body == {
        "bundle_id": "bundle-1",
        "decision": "reject",
    }
    assert service.handle("GET", "/reports/run-1").body == "# Run 1\n"


def test_read_only_service_rejects_write_and_trading_routes(tmp_path: Path) -> None:
    service = ReadOnlyArtifactService(tmp_path)

    for forbidden_path in ("/trade", "/order", "/broker", "/orders"):
        response = service.handle("POST", forbidden_path)
        assert response.status_code == 405
        assert response.body == {"error": "read_only_service_allows_get_only"}

    assert service.handle("GET", "/trade").status_code == 404
    assert not any(tmp_path.rglob("*"))


def test_read_only_service_reports_missing_artifacts_without_side_effects(tmp_path: Path) -> None:
    service = ReadOnlyArtifactService(tmp_path)

    response = service.handle("GET", "/runs/missing-run")

    assert response.status_code == 404
    assert response.body == {"error": "artifact_not_found", "artifact": "runs/missing-run.json"}
    assert not any(tmp_path.rglob("*"))


def test_read_only_service_route_contract_contains_no_forbidden_write_routes(tmp_path: Path) -> None:
    service = ReadOnlyArtifactService(tmp_path)

    routes = service.route_contract()

    assert "GET /health" in routes
    assert "GET /runs/{run_id}" in routes
    assert "GET /reports/{run_id}" in routes
    assert all("POST" not in route for route in routes)
    assert all("trade" not in route.lower() for route in routes)
    assert all("broker" not in route.lower() for route in routes)


def _write_json(path: Path, text: dict[str, object]) -> None:
    import json

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(text, sort_keys=True) + "\n", encoding="utf-8")


def _write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")
