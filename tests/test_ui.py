"""Smoke tests for the server-rendered UI pages and their new payloads."""

from __future__ import annotations

import time
from pathlib import Path

from fastapi.testclient import TestClient

from piply.api.app import create_app
from piply.core.service import PipelineService

DEPLOYMENT_CONFIG = "\n".join(
    [
        'version: "1"',
        "title: UI Test",
        "workspace: workspace",
        "pipeline_templates:",
        "  tenant_ingest:",
        "    description: Shared ingest template deployed per tenant.",
        "    env:",
        "      STAGE: production",
        "    triggers_on_success:",
        "      - tenant_report",
        "    tasks:",
        "      ingest:",
        "        type: cli",
        "        priority: high",
        "        timeout: 5m",
        "        command: python -c \"print('ingest {tenant}')\"",
        "pipeline_deployments:",
        "  acme_ingest:",
        "    template: tenant_ingest",
        "    tenant: acme",
        "  globex_ingest:",
        "    template: tenant_ingest",
        "    tenant: globex",
        "pipelines:",
        "  tenant_report:",
        "    tasks:",
        "      report:",
        "        type: cli",
        "        command: python -c \"print('report')\"",
    ]
)


def _project(tmp_path: Path, config_text: str = DEPLOYMENT_CONFIG) -> Path:
    """Write a workspace and config for the UI tests."""
    (tmp_path / "workspace").mkdir(exist_ok=True)
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(config_text, encoding="utf-8")
    return config_path


def test_every_ui_page_renders(tmp_path: Path) -> None:
    """Each navigable page returns HTML without a template error."""
    config_path = _project(tmp_path)
    service = PipelineService(config_path=config_path)
    run = service.trigger_pipeline("acme_ingest", wait=True)

    with TestClient(create_app(str(config_path))) as client:
        pages = [
            "/",
            "/pipelines",
            "/pipelines/acme_ingest",
            "/runs",
            f"/runs/{run.run_id}",
            "/execution-matrix",
            "/logs",
            "/diagnostics",
            "/settings",
        ]
        for page in pages:
            response = client.get(page)
            assert response.status_code == 200, f"{page} returned {response.status_code}"
            assert "text/html" in response.headers["content-type"], page


def test_pipelines_page_groups_deployments_of_one_template(tmp_path: Path) -> None:
    """Deployments of a shared template are grouped rather than listed alphabetically."""
    config_path = _project(tmp_path)
    PipelineService(config_path=config_path)

    with TestClient(create_app(str(config_path))) as client:
        body = client.get("/pipelines").text

    assert "Template: tenant_ingest" in body
    assert '"group_id": "template:tenant_ingest"' in body
    # Sorting and filtering controls the requirement asked for.
    assert 'data-sort="next"' in body
    assert 'data-filter="running"' in body


def test_run_detail_exposes_downstream_pipeline_status(tmp_path: Path) -> None:
    """A run shows the downstream pipeline it triggered, with a link to that run."""
    config_path = _project(tmp_path)
    service = PipelineService(config_path=config_path)
    parent = service.trigger_pipeline("acme_ingest", wait=True)

    for _ in range(60):
        if service.list_runs(pipeline_id="tenant_report"):
            break
        time.sleep(0.2)

    with TestClient(create_app(str(config_path))) as client:
        payload = client.get(f"/api/runs/{parent.run_id}").json()
        page = client.get(f"/runs/{parent.run_id}").text

    downstream = payload["downstream"]
    assert len(downstream) == 1
    assert downstream[0]["pipeline_id"] == "tenant_report"
    assert downstream[0]["run_id"] is not None
    assert "Downstream pipelines" in page

    with TestClient(create_app(str(config_path))) as client:
        child = client.get(f"/api/runs/{downstream[0]['run_id']}").json()
    assert child["upstream"]["run_id"] == parent.run_id


def test_deployment_inherits_template_settings_and_variables(tmp_path: Path) -> None:
    """A deployment picks up the template's env, tasks, policies, and its own tenant."""
    config_path = _project(tmp_path)
    service = PipelineService(config_path=config_path)

    acme = service.get_pipeline("acme_ingest")
    globex = service.get_pipeline("globex_ingest")

    assert acme.template_id == "tenant_ingest"
    assert acme.deployment_id == "acme_ingest"
    assert acme.variables["tenant"] == "acme"
    assert globex.variables["tenant"] == "globex"
    assert acme.tasks["ingest"].env["STAGE"] == "production"
    assert acme.tasks["ingest"].priority == 1
    assert acme.tasks["ingest"].timeout_seconds == 300
    assert acme.triggers_on_success == ("tenant_report",)

    # The tenant variable is interpolated into each deployment's command.
    assert "ingest acme" in acme.tasks["ingest"].command_preview
    assert "ingest globex" in globex.tasks["ingest"].command_preview


def test_preview_endpoint_backs_the_execution_preview_ui(tmp_path: Path) -> None:
    """The preview drawer's endpoint returns stages, variables, and commands."""
    config_path = _project(tmp_path)
    PipelineService(config_path=config_path)

    with TestClient(create_app(str(config_path))) as client:
        response = client.post("/api/pipelines/acme_ingest/preview", json={})

    payload = response.json()
    assert response.status_code == 200
    assert payload["deployment_id"] == "acme_ingest"
    assert payload["variables"]["tenant"] == "acme"
    assert payload["stages"] == [["ingest"]]
    assert payload["tasks"][0]["priority"] == 1
    assert "ingest acme" in payload["tasks"][0]["command"]
