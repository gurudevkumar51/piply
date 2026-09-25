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


def test_pipelines_page_groups_deployments_of_one_template(tmp_path: Path, monkeypatch) -> None:
    """Deployments of a shared template are grouped rather than listed alphabetically."""
    config_path = _project(tmp_path)
    # A configured database, as any install has after first-run setup; without
    # one an empty install is redirected to the setup page instead.
    monkeypatch.setenv("PIPLY_DATABASE", str(tmp_path / "piply.db"))
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


RUN_HISTORY_CONFIG = "\n".join(
    [
        'version: "1"',
        "title: Run History Test",
        "workspace: workspace",
        "pipelines:",
        "  ok_flow:",
        "    tasks:",
        "      main: {type: cli, command: python -c \"print('ok')\"}",
        "  failing_flow:",
        "    tasks:",
        '      main: {type: cli, command: python -c "import sys; sys.exit(3)"}',
        "  never_run:",
        "    tasks:",
        "      main: {type: cli, command: echo hi}",
    ]
)


def test_recent_runs_are_capped_and_newest_first(tmp_path: Path) -> None:
    """Each pipeline carries its last N runs, newest first, in one query."""
    config_path = _project(tmp_path, RUN_HISTORY_CONFIG)
    service = PipelineService(config_path=config_path)

    created = [service.trigger_pipeline("ok_flow", wait=True).run_id for _ in range(7)]
    service.trigger_pipeline("failing_flow", wait=True)

    summaries = {item.pipeline_id: item for item in service.list_pipelines()}

    # Capped at the configured history count, newest first.
    history = summaries["ok_flow"].recent_runs
    assert len(history) == service.settings.pipeline_run_history_count == 5
    assert [run.run_id for run in history] == list(reversed(created))[:5]

    # The newest of those is also the summary's last_run, from the same query.
    assert summaries["ok_flow"].last_run is not None
    assert summaries["ok_flow"].last_run.run_id == history[0].run_id

    # Status travels with each dot, so colour comes from real data.
    assert {run.status for run in history} == {"success"}
    assert summaries["failing_flow"].recent_runs[0].status == "failed"

    # A pipeline that has never run gets an empty history rather than an error.
    assert summaries["never_run"].recent_runs == ()
    assert summaries["never_run"].last_run is None


def test_run_history_does_not_add_queries_per_pipeline(tmp_path: Path) -> None:
    """The listing query count stays constant as pipelines are added.

    The dots come from the same windowed query that supplies the latest run, so
    showing five runs each must not reintroduce an N+1.
    """
    from contextlib import contextmanager

    import piply.core.store as store_mod

    def count_queries(pipeline_count: int) -> int:
        body = ['version: "1"', "title: Scaling Test", "workspace: workspace", "pipelines:"]
        for index in range(pipeline_count):
            body += [f"  flow_{index}:", "    tasks:", "      main: {type: cli, command: echo hi}"]
        project = tmp_path / f"p{pipeline_count}"
        project.mkdir()
        (project / "workspace").mkdir()
        config_path = project / "piply.yaml"
        config_path.write_text("\n".join(body), encoding="utf-8")

        service = PipelineService(config_path=config_path)
        for index in range(pipeline_count):
            service.trigger_pipeline(f"flow_{index}", wait=True)

        statements: list[str] = []
        original = store_mod.RunStore._connect

        @contextmanager
        def counting(self):
            with original(self) as connection:
                real = connection.execute

                def wrapped(sql, parameters=()):
                    statements.append(sql)
                    return real(sql, parameters)

                connection.execute = wrapped  # type: ignore[method-assign]
                yield connection

        store_mod.RunStore._connect = counting
        try:
            summaries = service.list_pipelines()
        finally:
            store_mod.RunStore._connect = original

        assert len(summaries) == pipeline_count
        assert all(item.recent_runs for item in summaries)
        return len(statements)

    small = count_queries(2)
    large = count_queries(10)

    # Five times the pipelines, the same number of queries.
    assert small == large, f"{small} queries for 2 pipelines, {large} for 10"


def test_pipelines_page_renders_clickable_run_dots(tmp_path: Path) -> None:
    """Every dot is a link to its own run page."""
    config_path = _project(tmp_path, RUN_HISTORY_CONFIG)
    service = PipelineService(config_path=config_path)
    passing = [service.trigger_pipeline("ok_flow", wait=True).run_id for _ in range(2)]
    failing = service.trigger_pipeline("failing_flow", wait=True).run_id

    with TestClient(create_app(str(config_path))) as client:
        body = client.get("/pipelines").text
        payload = client.get("/api/pipelines").json()

    # The template ships the run history and the renderer that draws it.
    assert "renderRunHistory" in body
    assert 'class="run-dot' in body
    assert "/runs/${escapeHtml(run.run_id)}" in body
    for run_id in [*passing, failing]:
        assert run_id in body, f"{run_id} missing from the page payload"

    # The API exposes the same history, with the status each dot is coloured by.
    by_id = {item["pipeline_id"]: item for item in payload}
    assert [item["id"] for item in by_id["ok_flow"]["recent_runs"]] == list(reversed(passing))
    assert by_id["failing_flow"]["recent_runs"][0]["status"] == "failed"
    assert by_id["never_run"]["recent_runs"] == []


def test_pipeline_groups_are_collapsible(tmp_path: Path, monkeypatch) -> None:
    """Template groups can be collapsed, and the choice survives a re-render.

    With one template deployed per tenant a group holds a row per tenant, so the
    page becomes a wall of near-identical entries. The page ships the pieces that
    make collapsing work: a per-group toggle, bulk controls, persisted state, and
    a summary that stays readable once the rows are hidden.
    """
    config_path = _project(tmp_path)
    monkeypatch.setenv("PIPLY_DATABASE", str(tmp_path / "piply.db"))
    PipelineService(config_path=config_path)

    with TestClient(create_app(str(config_path))) as client:
        body = client.get("/pipelines").text

    # Each group renders a real button, so it is keyboard reachable.
    assert "togglePipelineGroup(" in body
    assert 'class="collapse-toggle"' in body
    assert 'data-group-id="' in body
    assert "collapsible-body" in body

    # Bulk controls, and the storage key that makes the choice persist.
    assert "setAllPipelineGroups(true)" in body
    assert "setAllPipelineGroups(false)" in body
    assert "piply.collapsedPipelineGroups" in body

    # A collapsed group still reports what is inside it.
    assert "groupSummary(" in body
    assert "group-stat" in body

    # Searching must force groups open, or a match hides behind a collapsed header.
    assert "!needle && collapsedPipelineGroups.has(groupId)" in body


def test_dag_labels_are_measured_and_shortened(tmp_path: Path) -> None:
    """Long entity task names must not spill outside their node box.

    SVG text neither wraps nor clips, so `payer_claim_status_dashboard / Extract`
    used to paint straight across the node border and over the edges. The fix has
    three parts and all three matter, so all three are pinned here.
    """
    config_path = _project(tmp_path)
    PipelineService(config_path=config_path)

    with TestClient(create_app(str(config_path))) as client:
        script = client.get("/static/dag.js").text

    # 1. Labels are measured and shortened to the node's width.
    assert "function shortenLabel" in script
    assert "getComputedTextLength" in script

    # 2. Measuring before the web font loads produces labels that fit the
    #    fallback metrics and overflow once IBM Plex Mono arrives.
    assert "document.fonts.ready.then(fitAll)" in script

    # 3. Identifiers differ at both ends, so the middle is dropped and the full
    #    value stays reachable on hover.
    assert '"middle"' in script
    assert 'createElementNS("http://www.w3.org/2000/svg", "title")' in script

    # An entity task's id restates its title, so it is dropped rather than
    # shown twice in shortened form.
    assert "titleRestatesId" in script
