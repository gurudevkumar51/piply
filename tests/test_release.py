"""Release guards for locally bundled UI code and packaged installations."""

from __future__ import annotations

import fnmatch
import json
import re
import shutil
import sqlite3
import subprocess
import sys
from pathlib import Path

import pytest
from fastapi.testclient import TestClient
from typer.testing import CliRunner

from piply.api.app import create_app
from piply.cli.main import app as cli
from piply.core.store import RunStore
from piply.engine.task_runner import TaskRunner, _capture_bindings, _OutputCapture, _StreamingLogSink

try:
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - Python 3.10
    import tomli as tomllib

ROOT = Path(__file__).resolve().parents[1]


def test_review_ci_cannot_trigger_staging_publication() -> None:
    import yaml

    workflows = ROOT / ".gitea" / "workflows"
    validation = yaml.load((workflows / "validate-pr.yml").read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    release = yaml.load((workflows / "release.yml").read_text(encoding="utf-8"), Loader=yaml.BaseLoader)
    branch = "codex/release-v0.3.4-validation"
    assert any(fnmatch.fnmatch(branch, pattern) for pattern in validation["on"]["push"]["branches"])
    assert not any(fnmatch.fnmatch(branch, pattern) for pattern in release["on"]["push"]["branches"])
    assert release["on"]["push"]["branches"] == ["staging"]
    assert set(release["jobs"]["build-and-publish"]["needs"]) == {"compatibility", "postgres"}
    for workflow in (validation, release):
        postgres = workflow["jobs"]["postgres"]
        assert postgres["container"] == "ghcr.io/catthehacker/ubuntu:act-22.04"
        assert postgres["steps"][-1]["env"]["PIPLY_TEST_POSTGRES_RESET"] == "1"
        matrix = workflow["jobs"]["validate" if workflow is validation else "compatibility"]
        check = next(step for step in matrix["steps"] if step.get("name") == "Verify matrix environment")
        assert check["env"] == {"EXPECTED_RUNNER": "${{ matrix.os }}", "EXPECTED_PYTHON": "${{ matrix.python }}"}
        assert "platform.system()==expected" in check["run"]


def test_log_callback_does_not_recapture_its_own_output(tmp_path, capsys):
    store = RunStore(tmp_path / "runs.db")
    runner = TaskRunner(store=store, run_id="capture", on_log=lambda line: print(line, file=sys.stderr))
    sink = _StreamingLogSink(lambda line: runner.emit(line, task_id="step"))
    with _OutputCapture(sink, sink):
        print("first")
        print("second", file=sys.stderr)
    sink.close()
    assert capsys.readouterr().err.splitlines() == ["[step] first", "[step] second"]
    assert sorted(line.message for line in store.list_logs("capture")) == ["first", "second"]


def test_log_callback_restores_capture_after_an_exception(tmp_path):
    def broken_callback(line):
        raise RuntimeError("callback failed")

    runner = TaskRunner(store=RunStore(tmp_path / "runs.db"), run_id="capture", on_log=broken_callback)
    sink = _StreamingLogSink(lambda line: None)
    with _OutputCapture(sink, sink):
        with pytest.raises(RuntimeError, match="callback failed"):
            runner.emit("message")
        assert _capture_bindings.stdout is sink
        assert _capture_bindings.stderr is sink


def test_starter_workspace_executes_through_cli_with_live_callable_logs(tmp_path, monkeypatch):
    monkeypatch.setenv("PIPLY_DATABASE", str(tmp_path / "runtime.db"))
    workspace = tmp_path / "workspace"
    runner = CliRunner()
    assert runner.invoke(cli, ["init", str(workspace)]).exit_code == 0
    result = runner.invoke(cli, ["run", "extract_flow", "--config", str(workspace / "piply.yaml"), "--wait"])
    assert result.exit_code == 0, result.output
    assert "Finished with status: success" in result.output
    assert result.output.count("[extract] Extracting 120 records") == 1


def test_package_discovery_excludes_demo_workspaces():
    metadata = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    patterns = metadata["tool"]["setuptools"]["packages"]["find"]["include"]
    assert any(fnmatch.fnmatch("piply.api", pattern) for pattern in patterns)
    assert not any(fnmatch.fnmatch("piply-demo.pipelines", pattern) for pattern in patterns)


@pytest.mark.parametrize("ready", [True, False])
def test_error_reporter_escapes_messages_and_sources(ready):
    node = shutil.which("node")
    if node is None:
        pytest.skip("Node.js is needed to execute the browser error reporter")
    source = (ROOT / "piply/ui/templates/base.html").read_text(encoding="utf-8")
    handler = re.search(r"<script>\s*(.*?)</script>", source, re.S).group(1)
    script = r"""
const vm = require('node:vm');
let onError, onReady, rendered = [];
const body = {firstChild: null, insertBefore: box => rendered.push(box.innerHTML)};
const document = {body: READY ? body : null, createElement: () => ({setAttribute: () => {}}),
  addEventListener: (name, fn) => { onReady = fn; }};
const window = {addEventListener: (name, fn) => { onError = fn; }};
vm.runInNewContext(HANDLER, {window, document});
onError({target: window, message: '<img src=x onerror="alert(1)">',
  filename: 'http://local/<img src=x>.js', lineno: '</span><script>bad</script>'});
if (!READY) { document.body = body; onReady(); }
onError({target: window, message: 'second error'});
console.log(JSON.stringify(rendered));
""".replace("HANDLER", json.dumps(handler)).replace("READY", json.dumps(ready))
    result = subprocess.run([node, "-e", script], capture_output=True, text=True, check=True, timeout=20)
    rendered = json.loads(result.stdout)
    assert len(rendered) == 1
    assert "<img" not in rendered[0] and "<script>" not in rendered[0]
    assert "&lt;img" in rendered[0] and "&lt;/span&gt;" in rendered[0]


def test_graph_libraries_are_local_licensed_and_served_with_correct_types(tmp_path):
    config = tmp_path / "piply.yaml"
    config.write_text(
        "version: '1'\nworkspace: .\npipelines:\n  flow:\n    tasks:\n      step: {type: cli, command: echo hi}\n",
        encoding="utf-8",
    )
    with TestClient(create_app(str(config))) as client:
        for template in ("pipeline_detail.html", "run_detail.html"):
            source = (ROOT / "piply/ui/templates" / template).read_text(encoding="utf-8")
            assert 'src="https://' not in source
        for folder, file in (("dagre-1.1.2", "dagre.min.js"), ("graphlib-2.1.13", "graphlib.min.js")):
            response = client.get(f"/static/vendor/{folder}/dist/{file}")
            assert response.status_code == 200
            assert "javascript" in response.headers["content-type"]
            assert response.headers["x-content-type-options"] == "nosniff"
            assert "cdn.jsdelivr.net" not in response.headers["content-security-policy"]
            assert "MIT" in (ROOT / "piply/ui/static/vendor" / folder / "LICENSE").read_text()


def test_sqlite_backup_closes_its_destination(tmp_path, monkeypatch):
    source = RunStore(tmp_path / "source.db")
    target = tmp_path / "backup.db"
    real_connect = sqlite3.connect
    opened = []

    def connect(path, *args, **kwargs):
        connection = real_connect(path, *args, **kwargs)
        if Path(path) == target:
            opened.append(connection)
        return connection

    monkeypatch.setattr(sqlite3, "connect", connect)
    source.backup_to(target)
    assert len(opened) == 1
    with pytest.raises(sqlite3.ProgrammingError, match="closed"):
        opened[0].execute("SELECT 1")
    assert RunStore(target).row_counts() == source.row_counts()


def test_sqlite_restore_closes_its_destination(tmp_path, monkeypatch):
    source = tmp_path / "backup.db"
    RunStore(source).set_meta("proof", "restored")
    target = tmp_path / "restored.db"
    monkeypatch.setenv("PIPLY_DATABASE", str(target))
    config = tmp_path / "piply.yaml"
    config.write_text("version: '1'\npipelines: {}\n", encoding="utf-8")
    real_connect = sqlite3.connect
    opened = []

    def connect(path, *args, **kwargs):
        connection = real_connect(path, *args, **kwargs)
        if Path(path) == target:
            opened.append(connection)
        return connection

    monkeypatch.setattr(sqlite3, "connect", connect)
    result = CliRunner().invoke(cli, ["restore", str(source), "--config", str(config), "--yes"])
    assert result.exit_code == 0, result.output
    assert opened
    for connection in opened:
        with pytest.raises(sqlite3.ProgrammingError, match="closed"):
            connection.execute("SELECT 1")
    assert RunStore(target).get_meta("proof") == "restored"


@pytest.mark.parametrize("value", [None, "0", "true"])
def test_postgres_schema_reset_requires_explicit_opt_in(monkeypatch, value) -> None:
    from tests.test_postgres_store import _require_schema_reset_permission

    if value is None:
        monkeypatch.delenv("PIPLY_TEST_POSTGRES_RESET", raising=False)
    else:
        monkeypatch.setenv("PIPLY_TEST_POSTGRES_RESET", value)
    with pytest.raises(RuntimeError, match="PIPLY_TEST_POSTGRES_RESET=1"):
        _require_schema_reset_permission()
    monkeypatch.setenv("PIPLY_TEST_POSTGRES_RESET", "1")
    _require_schema_reset_permission()
