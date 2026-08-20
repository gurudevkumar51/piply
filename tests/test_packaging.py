"""Packaging guards.

The failure these prevent is the worst kind: everything passes on a developer
machine that happens to have an extra package installed, and the server refuses
to start on a clean install.

That is exactly what happened with ``python-multipart``. FastAPI's ``Form(...)``
pulls it in implicitly, so no Piply source file imports it and a static scan
sees nothing. It was present locally, so the whole suite passed while
``pip install mr-piply`` produced an app that could not boot.
"""

from __future__ import annotations

import ast
import subprocess
import sys
from pathlib import Path

import pytest

try:  # `tomllib` is 3.11+; the package itself supports 3.10.
    import tomllib
except ModuleNotFoundError:  # pragma: no cover - only on Python 3.10
    tomllib = None  # type: ignore[assignment]

pytestmark = pytest.mark.skipif(tomllib is None, reason="tomllib requires Python 3.11+")

ROOT = Path(__file__).resolve().parent.parent

#: Import name -> distribution name, where they differ.
_DISTRIBUTION_NAMES = {
    "yaml": "pyyaml",
    "dotenv": "python-dotenv",
    "multipart": "python-multipart",
    "jinja2": "jinja2",
}


def _declared_distributions() -> set[str]:
    """Return every distribution pyproject declares, required or optional."""
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    project = data["project"]
    declared = list(project.get("dependencies", []))
    for extra in project.get("optional-dependencies", {}).values():
        declared.extend(extra)

    names = set()
    for spec in declared:
        # "psycopg[binary]>=3.1,<4" -> "psycopg"
        name = spec.split("[")[0]
        for separator in (">=", "<=", "==", "!=", "~=", ">", "<", ";", " "):
            name = name.split(separator)[0]
        names.add(name.strip().lower().replace("_", "-"))
    return names


def _third_party_imports() -> set[str]:
    """Return the top-level non-stdlib modules the package imports."""
    found: set[str] = set()
    for path in (ROOT / "piply").rglob("*.py"):
        tree = ast.parse(path.read_text(encoding="utf-8"))
        for node in ast.walk(tree):
            if isinstance(node, ast.Import):
                found.update(alias.name.split(".")[0] for alias in node.names)
            elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
                found.add(node.module.split(".")[0])
    return {
        name for name in found if name not in sys.stdlib_module_names and name != "piply" and not name.startswith("_")
    }


def test_every_imported_package_is_declared() -> None:
    """Anything the package imports must be installable from its metadata."""
    declared = _declared_distributions()
    missing = sorted(
        name for name in _third_party_imports() if _DISTRIBUTION_NAMES.get(name, name).lower() not in declared
    )
    assert not missing, f"imported but not declared in pyproject.toml: {missing}"


def test_runtime_dependency_count_is_deliberate() -> None:
    """The dependency budget is a product decision, so changing it is deliberate.

    Piply's pitch is that it needs no broker, no scheduler service, and very
    little else. If this fails, either remove the dependency or update the
    number here and in the docs that quote it.
    """
    data = tomllib.loads((ROOT / "pyproject.toml").read_text(encoding="utf-8"))
    assert len(data["project"]["dependencies"]) == 8


@pytest.mark.parametrize("blocked", ["multipart"])
def test_the_app_starts_without_optional_transitive_packages(tmp_path: Path, blocked: str) -> None:
    """The server must boot and sign users in on a clean install.

    Run in a subprocess with the module hidden, because a developer machine
    usually has it installed as a transitive extra of something else. In-process
    import blocking would also leak into later tests.
    """
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Clean Install",
                "workspace: .",
                "pipelines:",
                "  demo:",
                "    tasks:",
                "      main: {type: cli, command: echo hi}",
            ]
        ),
        encoding="utf-8",
    )

    script = f"""
import builtins, sys, os
_real = builtins.__import__
def _blocked(name, *a, **k):
    if name == {blocked!r} or name.startswith({blocked!r} + "."):
        raise ModuleNotFoundError("No module named " + {blocked!r})
    return _real(name, *a, **k)
builtins.__import__ = _blocked
os.environ["PIPLY_DATABASE"] = {str(tmp_path / "piply.db")!r}

from fastapi.testclient import TestClient
from piply.api.app import create_app

app = create_app({str(config_path)!r})
with TestClient(app) as client:
    app.state.service.create_user("root", "root-password", role="admin")
    response = client.post(
        "/login",
        data={{"username": "root", "password": "root-password", "next": "/runs"}},
        follow_redirects=False,
    )
    assert response.status_code == 303, response.status_code
    assert response.headers["location"] == "/runs"
    assert response.cookies.get("piply_session")
    assert client.get("/api/pipelines", auth=("root", "root-password")).status_code == 200
print("OK")
"""
    result = subprocess.run(
        [sys.executable, "-c", script],
        capture_output=True,
        text=True,
        cwd=ROOT,
        timeout=180,
    )
    assert result.returncode == 0, f"app failed without {blocked}:\n{result.stdout}\n{result.stderr}"
    assert "OK" in result.stdout


def test_login_form_parsing_handles_awkward_credentials(tmp_path: Path) -> None:
    """Hand-rolled form parsing has to match what a browser actually sends."""
    from fastapi.testclient import TestClient

    from piply.api.app import create_app

    config_path = tmp_path / "piply.yaml"
    config_path.write_text(
        "\n".join(
            [
                'version: "1"',
                "title: Form Parsing",
                "workspace: .",
                "pipelines:",
                "  demo:",
                "    tasks:",
                "      main: {type: cli, command: echo hi}",
            ]
        ),
        encoding="utf-8",
    )

    with TestClient(create_app(str(config_path))) as client:
        service = client.app.state.service
        # Space, plus, ampersand, equals, percent-escape, and non-ASCII all have
        # to survive url-decoding intact.
        password = "p@ss w+rd&x=1%2Fé中"
        service.create_user("root", password, role="admin")

        ok = client.post(
            "/login",
            data={"username": "root", "password": password, "next": "/runs"},
            follow_redirects=False,
        )
        assert ok.status_code == 303
        assert ok.headers["location"] == "/runs"
        assert ok.cookies.get("piply_session")

        # A blank password must read as a bad credential, not a malformed form.
        blank = client.post("/login", data={"username": "root", "password": ""}, follow_redirects=False)
        assert blank.status_code == 303
        assert "Invalid+username+or+password" in blank.headers["location"]

        # Wrong content type and oversized bodies are refused, not buffered.
        assert client.post("/login", json={"username": "root"}, follow_redirects=False).status_code == 415
        oversized = client.post(
            "/login",
            content=b"username=x&password=" + b"a" * 9000,
            headers={"content-type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
        assert oversized.status_code == 413
        invalid_utf8 = client.post(
            "/login",
            content=b"username=\xff\xfe&password=x",
            headers={"content-type": "application/x-www-form-urlencoded"},
            follow_redirects=False,
        )
        assert invalid_utf8.status_code == 400
