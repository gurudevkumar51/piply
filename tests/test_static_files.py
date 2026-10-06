"""Bundled UI assets must not depend on host MIME-type registrations."""

from __future__ import annotations

import mimetypes
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from piply.api.app import create_app

ASSETS = [
    ("app.js", "text/javascript", "function escapeHtml("),
    ("dag.js", "text/javascript", "window.PiplyDag"),
    ("styles.css", "text/css", ".app-shell"),
]


@pytest.fixture
def static_client(tmp_path: Path, monkeypatch):
    monkeypatch.setenv("PIPLY_SCHEDULER_ENABLED", "false")
    config = tmp_path / "piply.yaml"
    config.write_text(
        'version: "1"\nworkspace: .\npipelines:\n  demo:\n    tasks:\n      main: {type: cli, command: echo hi}\n',
        encoding="utf-8",
    )
    with TestClient(create_app(str(config))) as client:
        yield client


@pytest.mark.parametrize("filename,media_type,marker", ASSETS)
@pytest.mark.parametrize("host_type", ["text/plain", "application/octet-stream", None])
def test_bundled_asset_types_ignore_host_mime_mapping(
    static_client, monkeypatch, filename: str, media_type: str, marker: str, host_type: str | None
) -> None:
    mimetypes.init()
    extension = Path(filename).suffix
    if host_type is None:
        monkeypatch.delitem(mimetypes.types_map, extension, raising=False)
    else:
        monkeypatch.setitem(mimetypes.types_map, extension, host_type)
    assert mimetypes.guess_type(filename)[0] == host_type

    response = static_client.get(f"/static/{filename}?v=regression")

    assert response.status_code == 200
    assert response.headers["content-type"] == f"{media_type}; charset=utf-8"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert marker in response.text
    assert int(response.headers["content-length"]) == len(response.content)
    # Serving the UI must not change MIME guesses for user artifacts.
    assert mimetypes.guess_type(filename)[0] == host_type


@pytest.mark.parametrize("filename,media_type,marker", ASSETS)
def test_static_head_and_conditional_requests_are_preserved(
    static_client, monkeypatch, filename: str, media_type: str, marker: str
) -> None:
    mimetypes.init()
    monkeypatch.setitem(mimetypes.types_map, Path(filename).suffix, "text/plain")
    path = f"/static/{filename}"
    response = static_client.get(path)
    head = static_client.head(path)

    assert head.status_code == 200
    assert head.content == b""
    assert head.headers["content-type"] == f"{media_type}; charset=utf-8"
    assert head.headers["content-length"] == response.headers["content-length"]
    assert head.headers["etag"] == response.headers["etag"]
    assert head.headers["x-content-type-options"] == "nosniff"

    for header, value in (
        ("If-None-Match", response.headers["etag"]),
        ("If-Modified-Since", response.headers["last-modified"]),
    ):
        cached = static_client.get(path, headers={header: value})
        assert cached.status_code == 304
        assert cached.content == b""
        assert cached.headers["etag"] == response.headers["etag"]
        assert cached.headers["x-content-type-options"] == "nosniff"


@pytest.mark.parametrize(
    "path",
    [
        "/static/missing.js",
        "/static/missing.css",
        "/static/%2e%2e/templates/base.html",
        "/static/..%2f..%2f..%2fREADME.md",
    ],
)
def test_static_errors_and_traversal_are_not_served_as_executable_assets(static_client, path: str) -> None:
    response = static_client.get(path)

    assert response.status_code == 404
    assert response.headers["content-type"] == "application/json"
    assert response.headers["x-content-type-options"] == "nosniff"
    assert response.json() == {"detail": "Not Found"}


def test_static_files_still_reject_post_requests(static_client) -> None:
    response = static_client.post("/static/app.js")

    assert response.status_code == 405
    assert response.headers["content-type"] == "application/json"
    assert response.headers["x-content-type-options"] == "nosniff"
