"""Splitting `piply.yaml` across several files with `include:`.

A production config reached 974 lines, so adding a tenant meant editing one
enormous file and two people touching unrelated tenants conflicted in git for no
reason. The split is purely additive: a config with no `include:` behaves
exactly as it always has.
"""

from __future__ import annotations

import time
from pathlib import Path

import pytest

from piply.core.loader import ConfigError, load_project
from piply.core.service import PipelineService

ROOT = "\n".join(
    [
        'version: "1"',
        "title: Split",
        "workspace: .",
        "include:",
        "  - piply_pipe.yaml",
        "  - piply_alert.yaml",
    ]
)

PIPE = "\n".join(
    [
        "pipelines:",
        "  claim_pipeline:",
        "    tasks:",
        "      t: {type: cli, command: echo claim}",
    ]
)

ALERT = "\n".join(
    [
        "notifications:",
        "  teams:",
        "    production_alerts:",
        "      type: channel",
        "      webhook: https://example.invalid/hook",
    ]
)


def _write(tmp_path: Path, **files: str) -> Path:
    for name, body in files.items():
        (tmp_path / name.replace("__", ".")).write_text(body, encoding="utf-8")
    return tmp_path / "piply.yaml"


def test_a_config_with_no_include_is_unchanged(tmp_path: Path) -> None:
    """The feature must not alter how a single-file project loads."""
    config = _write(
        tmp_path,
        piply__yaml="\n".join(
            [
                'version: "1"',
                "title: Single",
                "workspace: .",
                "pipelines:",
                "  solo:",
                "    tasks:",
                "      t: {type: cli, command: echo hi}",
            ]
        ),
    )

    project = load_project(config)

    assert sorted(project.pipelines) == ["solo"]
    assert project.config_sources == (config.resolve(),)


def test_pipelines_and_notifications_can_live_in_separate_files(tmp_path: Path) -> None:
    """The layout the split exists for: deployments central, the rest split out."""
    config = _write(tmp_path, piply__yaml=ROOT, piply_pipe__yaml=PIPE, piply_alert__yaml=ALERT)

    project = load_project(config)

    assert sorted(project.pipelines) == ["claim_pipeline"]
    assert sorted(project.notifications.destinations) == ["production_alerts"]
    assert [path.name for path in project.config_sources] == [
        "piply.yaml",
        "piply_pipe.yaml",
        "piply_alert.yaml",
    ]


def test_include_accepts_a_glob(tmp_path: Path) -> None:
    """One line should pick up every file in a folder."""
    (tmp_path / "config").mkdir()
    (tmp_path / "config" / "a.yaml").write_text(
        "pipelines:\n  a:\n    tasks:\n      t: {type: cli, command: echo a}\n", encoding="utf-8"
    )
    (tmp_path / "config" / "b.yaml").write_text(
        "pipelines:\n  b:\n    tasks:\n      t: {type: cli, command: echo b}\n", encoding="utf-8"
    )
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Glob\nworkspace: .\ninclude:\n  - config/*.yaml\n',
    )

    project = load_project(config)

    assert sorted(project.pipelines) == ["a", "b"]


def test_a_pipeline_defined_twice_is_an_error_naming_both_files(tmp_path: Path) -> None:
    """Never last-wins: silently preferring one file is the bug being avoided."""
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Dup\nworkspace: .\ninclude:\n  - one.yaml\n  - two.yaml\n',
        one__yaml="pipelines:\n  same:\n    tasks:\n      t: {type: cli, command: echo one}\n",
        two__yaml="pipelines:\n  same:\n    tasks:\n      t: {type: cli, command: echo two}\n",
    )

    with pytest.raises(ConfigError) as error:
        load_project(config)

    message = str(error.value)
    assert "pipelines.same" in message
    # Both files are named, or the error sends you hunting.
    assert "one.yaml" in message and "two.yaml" in message


def test_an_include_pattern_that_matches_nothing_is_an_error(tmp_path: Path) -> None:
    """A silent no-match would look like the pipelines simply vanished."""
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Missing\nworkspace: .\ninclude:\n  - nope/*.yaml\n',
    )

    with pytest.raises(ConfigError, match="matched no files"):
        load_project(config)


def test_included_files_may_not_include_further_files(tmp_path: Path) -> None:
    """One level keeps the merge order obvious and cycles impossible."""
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Nested\nworkspace: .\ninclude:\n  - child.yaml\n',
        child__yaml="include:\n  - grandchild.yaml\n",
        grandchild__yaml="pipelines: {}\n",
    )

    with pytest.raises(ConfigError, match="only the root config file may do"):
        load_project(config)


def test_editing_an_included_file_is_picked_up(tmp_path: Path) -> None:
    """Reload watches every source, or an edit appears to do nothing."""
    config = _write(tmp_path, piply__yaml=ROOT, piply_pipe__yaml=PIPE, piply_alert__yaml=ALERT)
    service = PipelineService(config_path=config, database_path=tmp_path / "runs.db")
    assert sorted(service.project.pipelines) == ["claim_pipeline"]

    time.sleep(0.01)
    (tmp_path / "piply_pipe.yaml").write_text(
        PIPE + "\n  second_pipeline:\n    tasks:\n      t: {type: cli, command: echo two}\n",
        encoding="utf-8",
    )

    assert sorted(service.project.pipelines) == ["claim_pipeline", "second_pipeline"]


def test_a_pipeline_may_be_split_across_files_by_block(tmp_path: Path) -> None:
    """Tasks in one file, sensors in another — the `piply_sensor.yaml` layout."""
    (tmp_path / "inbox").mkdir()
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Split\nworkspace: .\ninclude: [piply_pipe.yaml, piply_sensor.yaml]\n',
        piply_pipe__yaml="pipelines:\n  ingest:\n    tasks:\n      load: {type: cli, command: echo hi}\n",
        piply_sensor__yaml=(
            "pipelines:\n  ingest:\n    sensors:\n      inbox:\n"
            "        type: file_sensor\n        path: inbox\n        pattern: '*.csv'\n"
        ),
    )

    project = load_project(config)

    pipeline = project.pipelines["ingest"]
    assert sorted(pipeline.tasks) == ["load"]
    assert sorted(pipeline.sensors) == ["inbox"]


def test_the_same_block_in_two_files_is_still_an_error(tmp_path: Path) -> None:
    """Splitting a pipeline's blocks is fine; splitting one block is not."""
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Split\nworkspace: .\ninclude: [a.yaml, b.yaml]\n',
        a__yaml="pipelines:\n  ingest:\n    tasks:\n      one: {type: cli, command: echo 1}\n",
        b__yaml="pipelines:\n  ingest:\n    tasks:\n      two: {type: cli, command: echo 2}\n",
    )

    with pytest.raises(ConfigError, match=r"pipelines\.ingest\.tasks"):
        load_project(config)
