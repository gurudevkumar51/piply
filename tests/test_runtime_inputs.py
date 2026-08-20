"""Interactive runtime inputs for manual runs.

A pipeline that normally receives its variables from an upstream trigger has
nothing to fill them in when it is started by hand. These tests cover detecting
what is missing, validating what the user supplies, and confirming the values
actually reach the executed command rather than being recorded and ignored.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from piply.api.app import create_app
from piply.core.loader import load_project
from piply.core.preview import unresolved_placeholders
from piply.core.service import PipelineService

CONFIG = "\n".join(
    [
        'version: "1"',
        "title: Runtime Inputs",
        "workspace: .",
        "pipelines:",
        "  Bronze_to_Silver:",
        "    tasks:",
        "      dbt:",
        "        type: cli",
        "        command: echo target={practice} batch={batch_id}",
        "      report:",
        "        type: cli",
        "        command: echo date={report_date}",
        "        depends_on: [dbt]",
        "  ready_flow:",
        "    variables:",
        "      who: world",
        "    tasks:",
        "      greet:",
        "        type: cli",
        "        command: echo hello {who}",
        "pipeline_templates:",
        "  etl:",
        "    tasks:",
        "      extract:",
        "        type: cli",
        "        command: echo extract {practice}",
        "pipeline_deployments:",
        "  BENNETT_ETL:",
        "    template: etl",
        "    variables:",
        "      practice: BENNETT",
        "    triggers_on_success: [Bronze_to_Silver]",
    ]
)


def _project(tmp_path: Path) -> Path:
    """Write the shared config and return its path."""
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(CONFIG, encoding="utf-8")
    return config_path


@pytest.fixture()
def service(tmp_path: Path) -> PipelineService:
    """Return a service backed by the shared config."""
    return PipelineService(config_path=_project(tmp_path), database_path=tmp_path / "piply.db")


# --- Detection ---------------------------------------------------------------


def test_placeholders_are_found_across_every_field(tmp_path: Path) -> None:
    """A placeholder is worth prompting for wherever it appears, not just in `command`."""
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Fields",
                "workspace: .",
                "pipelines:",
                "  wide:",
                "    tasks:",
                "      call:",
                "        type: api",
                "        url: https://api.example.com/{tenant}/jobs",
                "        method: POST",
                '        body: \'{"batch": "{batch_id}"}\'',
                "        headers:",
                '          X-Client: "{practice}"',
                "        env:",
                '          REGION: "{region}"',
            ]
        ),
        encoding="utf-8",
    )
    found = unresolved_placeholders(load_project(config_path).pipelines["wide"])
    assert set(found) == {"tenant", "batch_id", "practice", "region"}
    assert found["tenant"] == ("call",)


def test_entity_placeholders_are_not_prompted_for(tmp_path: Path) -> None:
    """An entity value is filled in when the task runs, so nobody should be asked."""
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Entities",
                "workspace: .",
                "pipelines:",
                "  fanout:",
                "    entities:",
                "      report: [payment, refund]",
                "    tasks:",
                "      extract:",
                "        type: cli",
                "        command: echo {report} for {practice}",
            ]
        ),
        encoding="utf-8",
    )
    found = unresolved_placeholders(load_project(config_path).pipelines["fanout"])
    # `report` is per-entity and already resolved; `practice` genuinely has no value.
    assert set(found) == {"practice"}
    assert set(found["practice"]) == {"payment.extract", "refund.extract"}


def test_runtime_inputs_reports_what_a_manual_run_needs(service: PipelineService) -> None:
    """The report names the values, the tasks using them, and the usual source."""
    details = service.runtime_inputs("Bronze_to_Silver")

    assert details["ready"] is False
    assert [item["name"] for item in details["required"]] == ["batch_id", "practice", "report_date"]
    assert details["required"][0]["tasks"] == ["dbt"]
    assert details["required"][2]["tasks"] == ["report"]
    # Knowing which pipeline normally supplies these is what tells the user this
    # is a normal manual run of a downstream pipeline, not a broken config.
    assert details["triggered_by"] == ["BENNETT_ETL"]


def test_a_fully_resolved_pipeline_needs_nothing(service: PipelineService) -> None:
    """Pipelines that resolve on their own must not prompt."""
    assert service.runtime_inputs("ready_flow")["ready"] is True
    assert service.runtime_inputs("ready_flow")["required"] == []
    # A deployment supplies its own variables, so it is ready too.
    assert service.runtime_inputs("BENNETT_ETL")["ready"] is True


def test_supplying_values_clears_them_one_at_a_time(service: PipelineService) -> None:
    """Re-checking with answers lets a caller confirm nothing is left."""
    partial = service.runtime_inputs("Bronze_to_Silver", provided={"practice": "BENNETT"})
    assert partial["ready"] is False
    assert [item["name"] for item in partial["required"]] == ["batch_id", "report_date"]

    complete = service.runtime_inputs(
        "Bronze_to_Silver",
        provided={"practice": "BENNETT", "batch_id": "B-1", "report_date": "2026-08-20"},
    )
    assert complete["ready"] is True
    assert complete["required"] == []


def test_task_scope_asks_only_for_what_that_task_uses(service: PipelineService) -> None:
    """Running one task should not demand values only a different task needs."""
    details = service.runtime_inputs("Bronze_to_Silver", task_id="dbt")
    assert [item["name"] for item in details["required"]] == ["batch_id", "practice"]
    assert "report_date" not in {item["name"] for item in details["required"]}


# --- Validation --------------------------------------------------------------


def test_supplied_values_are_validated() -> None:
    """Blank values and unusable names are rejected with a reason."""
    validate = PipelineService.validate_runtime_inputs

    assert validate({"practice": " BENNETT "}) == {"practice": "BENNETT"}
    assert validate(None) == {}

    # An empty value would substitute an empty string, producing a different
    # broken command rather than an obvious one.
    with pytest.raises(ValueError, match="needs a value"):
        validate({"practice": "   "})
    with pytest.raises(ValueError, match="needs a value"):
        validate({"practice": None})
    # A name that cannot be a placeholder could never match one.
    with pytest.raises(ValueError, match="not a usable variable name"):
        validate({"not a name": "x"})
    with pytest.raises(ValueError, match="not a usable variable name"):
        validate({"9lives": "x"})


# --- End to end --------------------------------------------------------------


def test_supplied_values_reach_the_executed_command(tmp_path: Path) -> None:
    """The whole point: the run executes with the values, not with `{practice}`."""
    with TestClient(create_app(str(_project(tmp_path)))) as client:
        service = client.app.state.service

        listing = client.get("/api/pipelines/Bronze_to_Silver/runtime-inputs").json()
        assert listing["ready"] is False

        response = client.post(
            "/api/pipelines/Bronze_to_Silver/run",
            json={"variables": {"practice": "BENNETT", "batch_id": "B-42", "report_date": "2026-08-20"}},
        )
        assert response.status_code == 200
        run_id = response.json()["id"]

        for _ in range(80):
            record = service.store.get_run(run_id)
            if record and record.status in {"success", "failed"}:
                break
            time.sleep(0.2)

        record, task_runs, _ = service.get_run(run_id)
        assert record.status == "success"
        commands = {task.task_id: task.command_preview for task in task_runs}
        assert commands["dbt"] == "echo target=BENNETT batch=B-42"
        assert commands["report"] == "echo date=2026-08-20"

        # Captured in the run config, so a retry or backfill replays the same
        # values instead of prompting again.
        snapshot = service.store.get_run_config(run_id)
        assert snapshot["inherited_variables"] == {
            "practice": "BENNETT",
            "batch_id": "B-42",
            "report_date": "2026-08-20",
        }


def test_a_retry_reuses_the_supplied_values(tmp_path: Path) -> None:
    """Answering the prompt once must be enough, even after a retry."""
    with TestClient(create_app(str(_project(tmp_path)))) as client:
        service = client.app.state.service
        first = client.post(
            "/api/pipelines/Bronze_to_Silver/run",
            json={"variables": {"practice": "PALOS", "batch_id": "B-9", "report_date": "2026-09-01"}},
        ).json()["id"]

        for _ in range(80):
            record = service.store.get_run(first)
            if record and record.status in {"success", "failed"}:
                break
            time.sleep(0.2)

        retried = client.post(f"/api/runs/{first}/retry", json={"mode": "startover"}).json()["id"]
        for _ in range(80):
            record = service.store.get_run(retried)
            if record and record.status in {"success", "failed"}:
                break
            time.sleep(0.2)

        _, task_runs, _ = service.get_run(retried)
        commands = {task.task_id: task.command_preview for task in task_runs}
        assert commands["dbt"] == "echo target=PALOS batch=B-9"


def test_invalid_values_are_refused_before_a_run_is_created(tmp_path: Path) -> None:
    """A bad value must produce an error, not a run that fails later."""
    with TestClient(create_app(str(_project(tmp_path)))) as client:
        service = client.app.state.service

        blank = client.post("/api/pipelines/Bronze_to_Silver/run", json={"variables": {"practice": "  "}})
        assert blank.status_code == 400
        assert "needs a value" in blank.json()["detail"]

        bad_name = client.post("/api/pipelines/Bronze_to_Silver/run", json={"variables": {"not a name": "x"}})
        assert bad_name.status_code == 400

        assert service.list_runs(pipeline_id="Bronze_to_Silver") == []


def test_running_without_values_still_works(tmp_path: Path) -> None:
    """Backward compatibility: the endpoint does not start refusing runs.

    Existing API and CLI callers post no variables and expect a run. The prompt
    is a UI affordance layered on top, not a new precondition.
    """
    with TestClient(create_app(str(_project(tmp_path)))) as client:
        response = client.post("/api/pipelines/Bronze_to_Silver/run", json={})
        assert response.status_code == 200
        assert client.post("/api/pipelines/ready_flow/run", json={}).status_code == 200


def test_runtime_inputs_requires_run_permission(tmp_path: Path) -> None:
    """The report describes what a run would do, so it follows the run grant."""
    with TestClient(create_app(str(_project(tmp_path)))) as client:
        service = client.app.state.service
        service.create_user("root", "root-password", role="admin")
        service.create_user("viewer", "viewer-password", permissions={"Bronze_to_Silver": "view"})
        service.create_user("runner", "runner-password", permissions={"Bronze_to_Silver": "view,run"})

        url = "/api/pipelines/Bronze_to_Silver/runtime-inputs"
        assert client.get(url).status_code == 401
        assert client.get(url, auth=("viewer", "viewer-password")).status_code == 403
        assert client.get(url, auth=("runner", "runner-password")).status_code == 200
        assert client.get(url, auth=("root", "root-password")).status_code == 200
