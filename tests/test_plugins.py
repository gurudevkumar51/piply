from __future__ import annotations

import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

from piply.core.service import PipelineService


class TokenHandler(BaseHTTPRequestHandler):
    seen_auth = None
    seen_body = None

    def do_POST(self) -> None:  # noqa: N802
        length = int(self.headers.get("Content-Length", "0"))
        TokenHandler.seen_auth = self.headers.get("Authorization")
        TokenHandler.seen_body = self.rfile.read(length).decode("utf-8")
        self.send_response(201)
        self.end_headers()
        self.wfile.write(b'{"ok": true}')

    def log_message(self, format: str, *args) -> None:  # noqa: A003
        return


def test_api_operator_sends_bearer_token(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()

    server = HTTPServer(("127.0.0.1", 0), TokenHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: API Operator Test",
                "workspace: workspace",
                "pipelines:",
                "  call_api:",
                "    tasks:",
                "      webhook:",
                "        type: api",
                f"        url: http://127.0.0.1:{server.server_port}/hook",
                "        method: POST",
                "        token: demo-token",
                "        body: '{\"hello\": \"world\"}'",
                "        expected_status: 201",
            ]
        ),
        encoding="utf-8",
    )

    try:
        service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
        run = service.trigger_pipeline("call_api", wait=True)
        stored_run, _, logs = service.get_run(run.run_id)
    finally:
        server.shutdown()
        server.server_close()

    assert stored_run.status == "success"
    assert TokenHandler.seen_auth == "Bearer demo-token"
    assert TokenHandler.seen_body == '{"hello": "world"}'
    assert any("Response 201" in line.message for line in logs)


def test_ssh_operator_executes_configured_binary(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    fake_ssh = workspace / "fake_ssh.cmd"
    fake_ssh.write_text(
        "@echo off\r\necho fake ssh %*\r\nexit /b 0\r\n",
        encoding="utf-8",
    )

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: SSH Operator Test",
                "workspace: workspace",
                "pipelines:",
                "  ssh_probe:",
                "    tasks:",
                "      remote_check:",
                "        type: ssh",
                "        host: localhost",
                "        user: demo",
                "        command: echo remote-ok",
                f"        ssh_binary: '{fake_ssh.as_posix()}'",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    run = service.trigger_pipeline("ssh_probe", wait=True)
    stored_run, _, logs = service.get_run(run.run_id)

    assert stored_run.status == "success"
    assert any("fake ssh" in line.message.lower() for line in logs)


def test_python_call_operator_runs_module_function(tmp_path: Path) -> None:
    workspace = tmp_path / "workspace"
    workspace.mkdir()
    (workspace / "report_ops.py").write_text(
        "\n".join(
            [
                "from __future__ import annotations",
                "",
                "def build_report(report_name: str, batch_size: int = 10) -> dict[str, object]:",
                "    print(f'building {report_name} with batch {batch_size}')",
                "    return {'report_name': report_name, 'batch_size': batch_size}",
            ]
        ),
        encoding="utf-8",
    )

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Python Call Operator Test",
                "workspace: workspace",
                "pipelines:",
                "  report_flow:",
                "    tasks:",
                "      build_report:",
                "        type: python_call",
                "        path: report_ops.py",
                "        function: build_report",
                "        kwargs:",
                "          report_name: nightly",
                "          batch_size: 24",
            ]
        ),
        encoding="utf-8",
    )

    service = PipelineService(config_path=config_path, database_path=tmp_path / "runs.db")
    run = service.trigger_pipeline("report_flow", wait=True)
    stored_run, _, logs = service.get_run(run.run_id)

    assert stored_run.status == "success"
    assert any("building nightly with batch 24" in line.message for line in logs)
    assert any("Return value" in line.message for line in logs)
