"""Exercise an installed wheel outside the source checkout. No external services."""

from __future__ import annotations

import importlib.metadata
import json
import os
import subprocess
import sys
import tempfile
from datetime import datetime, timezone
from pathlib import Path


def main() -> None:
    from fastapi.testclient import TestClient

    import piply.api.app as piply_app
    from piply.api.app import create_app
    from piply.core.scheduling import CronSchedule
    from piply.core.service import PipelineService

    installed = Path(piply_app.__file__).resolve()
    checkout = Path(__file__).resolve().parents[1]
    if installed.is_relative_to(checkout / "piply"):
        raise RuntimeError("The smoke test imported the checkout, not the installed wheel. Run with python -I.")
    version = importlib.metadata.version("mr-piply")
    assert version == "0.3.4", version
    for name in ("UTC", "Asia/Kolkata", "America/New_York"):
        schedule = CronSchedule("0 * * * *", timezone_name=name)
        assert schedule.next_after(datetime(2026, 10, 6, tzinfo=timezone.utc)) is not None

    with tempfile.TemporaryDirectory(prefix="piply-wheel-smoke-") as folder:
        root = Path(folder)
        for key in list(os.environ):
            if key.startswith("PIPLY_"):
                del os.environ[key]
        os.environ.update(PIPLY_DATABASE=str(root / "runtime.db"), PIPLY_SCHEDULER_ENABLED="false")
        environment = Path(sys.executable).parent
        os.environ["PATH"] = os.pathsep.join(
            [str(environment), str(environment / "Scripts"), str(environment / "Library" / "bin"), os.environ["PATH"]]
        )
        command = [sys.executable, "-I", "-m", "piply.cli.main"]

        def cli(*args: str) -> str:
            result = subprocess.run(command + list(args), cwd=root, capture_output=True, text=True, timeout=120)
            if result.returncode:
                raise RuntimeError(f"CLI failed: {args}\n{result.stdout}\n{result.stderr}")
            return result.stdout

        cli("init", "workspace")
        config = root / "workspace" / "piply.yaml"
        cli("validate", "--config", str(config))
        cli("plan", "extract_flow", "--config", str(config))
        cli("run", "extract_flow", "--config", str(config), "--wait")
        admission = subprocess.run(
            [sys.executable, "-I", str(Path(__file__).with_name("check_run_admission.py"))],
            cwd=root,
            capture_output=True,
            text=True,
            timeout=60,
        )
        assert admission.returncode == 0, admission.stdout + admission.stderr
        assert json.loads(admission.stdout)["passed"]
        service = PipelineService(config_path=config)
        runs = service.store.list_runs(limit=20)
        assert runs and any(run.status == "success" for run in runs)
        assert service.store.list_audit_events(actor="cli", action="run.create")
        run = next(item for item in runs if item.pipeline_id == "extract_flow")

        with TestClient(create_app(str(config))) as client:
            for path in (
                "/",
                "/pipelines",
                "/pipelines/extract_flow",
                "/runs",
                f"/runs/{run.run_id}",
                "/execution-matrix",
                "/logs",
                "/settings",
                "/diagnostics",
                "/audit",
                "/health",
                "/api/pipelines",
                "/api/runs",
                "/api/audit",
                "/metrics",
            ):
                response = client.get(path)
                assert response.status_code == 200, (path, response.status_code, response.text[:200])
            for path in (
                "/static/app.js",
                "/static/dag.js",
                "/static/vendor/dagre-1.1.2/dist/dagre.min.js",
                "/static/vendor/graphlib-2.1.13/dist/graphlib.min.js",
            ):
                response = client.get(path)
                assert response.status_code == 200 and "javascript" in response.headers["content-type"], path
                assert response.headers["x-content-type-options"] == "nosniff"
            assert client.get("/static/../version.py").status_code == 404
            client.app.state.service.create_user("smoke-admin", "wheel-smoke-only", role="admin")
            assert client.get("/api/audit").status_code == 401
            auth = ("smoke-admin", "wheel-smoke-only")
            assert client.get("/api/audit", auth=auth).status_code == 200
            login = client.post("/login", data={"username": auth[0], "password": auth[1]}, follow_redirects=False)
            assert login.status_code == 303
            assert client.get("/audit").status_code == 200
            client.app.state.service.update_user(auth[0], password="wheel-smoke-changed")
            assert client.get("/api/audit").status_code == 401
        print(
            json.dumps(
                {
                    "version": version,
                    "imported_from": str(installed),
                    "result": "passed",
                    "checks": [
                        "init",
                        "validate",
                        "plan",
                        "execution",
                        "run admission",
                        "cron timezones",
                        "UI/API",
                        "local graph assets",
                        "MIME/security headers",
                        "audit",
                        "auth",
                        "password session invalidation",
                    ],
                },
                indent=2,
            )
        )


if __name__ == "__main__":
    main()
