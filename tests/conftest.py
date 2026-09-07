"""Shared test configuration.

Almost every test exercises a *configured* Piply install — one that has already
chosen where its data lives. Only `test_setup.py` cares about the first-run
experience, and it opts out explicitly.
"""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def configured_database(tmp_path, monkeypatch):
    """Point each test at its own database, as a real install has after setup.

    Without this, every test app would look like a brand-new install and be
    redirected to `/setup` before reaching the page under test. Each test gets a
    separate file so nothing leaks between them.

    Tests that want the first-run experience remove the variable themselves with
    `monkeypatch.delenv("PIPLY_DATABASE", raising=False)`, which takes effect
    because this fixture runs first.
    """
    monkeypatch.setenv("PIPLY_DATABASE", str(tmp_path / "piply-test.db"))
