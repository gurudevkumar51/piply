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


def _tenant_template(tmp_path: Path, folder: str, template_id: str) -> None:
    """A `<tenant>/piply_template.yaml`, the layout that made the names collide."""
    (tmp_path / folder).mkdir(exist_ok=True)
    (tmp_path / folder / "piply_template.yaml").write_text(
        f"pipeline_templates:\n  {template_id}:\n    tasks:\n      extract: {{type: cli, command: echo {folder}}}\n",
        encoding="utf-8",
    )


def test_included_files_may_share_a_name_in_different_folders(tmp_path: Path) -> None:
    """Nothing keys on the filename: one `piply_template.yaml` per tenant folder is fine.

    Organising templates per tenant gives every file the same name, which looked
    like it was being rejected because the duplicate error printed bare
    filenames.
    """
    _tenant_template(tmp_path, "ecw", "ECW_Extraction")
    _tenant_template(tmp_path, "athena", "ATHENA_Extraction")
    config = _write(
        tmp_path,
        piply__yaml="\n".join(
            [
                'version: "1"',
                "title: Tenants",
                "workspace: .",
                "include:",
                '  - "*/piply_template.yaml"',
                "pipeline_deployments:",
                "  ecw_extract:",
                "    template: ECW_Extraction",
                "  athena_extract:",
                "    template: ATHENA_Extraction",
            ]
        ),
    )

    project = load_project(config)

    assert sorted(project.pipelines) == ["athena_extract", "ecw_extract"]
    assert [path.name for path in project.config_sources[1:]] == [
        "piply_template.yaml",
        "piply_template.yaml",
    ]


def test_a_duplicate_across_same_named_files_names_the_folders(tmp_path: Path) -> None:
    """The error has to say *which* `piply_template.yaml`, or it is unactionable."""
    _tenant_template(tmp_path, "ecw", "ECW_Extraction")
    _tenant_template(tmp_path, "athena", "ECW_Extraction")
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Dup\nworkspace: .\ninclude:\n  - "*/piply_template.yaml"\n',
    )

    with pytest.raises(ConfigError) as error:
        load_project(config)

    message = str(error.value)
    assert "pipeline_templates.ECW_Extraction" in message
    # The folder is the only thing telling the two files apart.
    assert "athena/piply_template.yaml" in message, message
    assert "ecw/piply_template.yaml" in message, message


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


PIPE_DEFAULTS = "\n".join(
    [
        "pipeline_defaults:",
        "  tags: [claims, prod]",
        "  notifications:",
        "    on_failure: [claims_oncall]",
        "pipelines:",
        "  claim_extract:",
        "    tasks:",
        "      t: {type: cli, command: echo one}",
        "  claim_load:",
        "    tasks:",
        "      t: {type: cli, command: echo two}",
    ]
)

ALERT_DESTINATIONS = "\n".join(
    [
        "notifications:",
        "  teams:",
        "    claims_oncall:",
        "      type: channel",
        "      webhook: https://example.invalid/claims",
        "    company_wide:",
        "      type: channel",
        "      webhook: https://example.invalid/all",
        "  defaults:",
        "    on_failure: [company_wide]",
        "    on_success: [company_wide]",
    ]
)


def _split_project(tmp_path: Path, claims: str = PIPE_DEFAULTS) -> Path:
    return _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Split\nworkspace: .\ninclude: [piply_claims.yaml, piply_alert.yaml]\n',
        piply_claims__yaml=claims,
        piply_alert__yaml=ALERT_DESTINATIONS,
    )


def test_a_file_can_tag_every_pipeline_it_declares(tmp_path: Path) -> None:
    """The reason the config was split: one file per team, one label for all of it."""
    project = load_project(_split_project(tmp_path))

    assert project.pipelines["claim_extract"].tags == ("claims", "prod")
    assert project.pipelines["claim_load"].tags == ("claims", "prod")


def test_a_pipeline_adds_to_its_file_tags_rather_than_replacing_them(tmp_path: Path) -> None:
    """A tag is a label; the file's is still true whatever the pipeline calls itself."""
    project = load_project(
        _split_project(
            tmp_path,
            PIPE_DEFAULTS.replace(
                "      t: {type: cli, command: echo one}",
                "      t: {type: cli, command: echo one}\n    tags: [nightly]",
            ),
        )
    )

    assert project.pipelines["claim_extract"].tags == ("claims", "prod", "nightly")
    # Its neighbour in the same file is untouched.
    assert project.pipelines["claim_load"].tags == ("claims", "prod")


def test_file_notifications_override_the_project_default(tmp_path: Path) -> None:
    """Per-file routing is the point: the claims file pages the claims on-call."""
    project = load_project(_split_project(tmp_path))

    pipeline = project.pipelines["claim_extract"]
    assert pipeline.alert_on_failure == ("claims_oncall",)
    # Stated only on_failure, so the project's on_success still applies.
    assert pipeline.alert_on_success == ("company_wide",)


def test_a_pipeline_still_wins_over_its_file(tmp_path: Path) -> None:
    """Precedence is pipeline, then file, then project."""
    claims = PIPE_DEFAULTS.replace(
        "  claim_load:\n    tasks:",
        "  claim_load:\n    notifications:\n      on_failure: [company_wide]\n    tasks:",
    )
    project = load_project(_split_project(tmp_path, claims))

    assert project.pipelines["claim_extract"].alert_on_failure == ("claims_oncall",)
    assert project.pipelines["claim_load"].alert_on_failure == ("company_wide",)


def test_file_defaults_apply_to_deployments_declared_in_that_file(tmp_path: Path) -> None:
    """One file per tenant is the layout with the most repetition to remove."""
    config = _write(
        tmp_path,
        piply__yaml="\n".join(
            [
                'version: "1"',
                "title: Tenants",
                "workspace: .",
                "include: [piply_ecw.yaml]",
                "pipeline_templates:",
                "  extraction:",
                "    tasks:",
                "      t: {type: cli, command: echo go}",
            ]
        ),
        piply_ecw__yaml="\n".join(
            [
                "pipeline_defaults:",
                "  tags: [ecw]",
                "pipeline_deployments:",
                "  ecw_extract: {template: extraction}",
                "  ecw_reconcile: {template: extraction}",
            ]
        ),
    )

    project = load_project(config)

    assert project.pipelines["ecw_extract"].tags == ("ecw",)
    assert project.pipelines["ecw_reconcile"].tags == ("ecw",)


def test_pipeline_defaults_with_nothing_to_apply_to_is_an_error(tmp_path: Path) -> None:
    """Silently doing nothing would look like the tags simply failed to appear."""
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Split\nworkspace: .\ninclude: [piply_alert.yaml]\npipelines:\n  p:\n    tasks:\n      t: {type: cli, command: echo hi}\n',
        piply_alert__yaml="pipeline_defaults:\n  tags: [orphan]\n" + ALERT_DESTINATIONS,
    )

    with pytest.raises(ConfigError, match="declares no pipelines or deployments"):
        load_project(config)


def test_an_unknown_key_in_pipeline_defaults_is_rejected(tmp_path: Path) -> None:
    """A typo must not be silently ignored, and the file has to be named."""
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Split\nworkspace: .\ninclude: [piply_claims.yaml]\n',
        piply_claims__yaml="pipeline_defaults:\n  tag: [oops]\npipelines:\n  p:\n    tasks:\n      t: {type: cli, command: echo hi}\n",
    )

    with pytest.raises(ConfigError) as error:
        load_project(config)

    message = str(error.value)
    assert "'tag'" in message
    assert "piply_claims.yaml" in message


def test_two_files_may_each_carry_their_own_defaults(tmp_path: Path) -> None:
    """`pipeline_defaults` is file-scoped, so it must not collide across files."""
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Split\nworkspace: .\ninclude: [a.yaml, b.yaml]\n',
        a__yaml="pipeline_defaults:\n  tags: [alpha]\npipelines:\n  one:\n    tasks:\n      t: {type: cli, command: echo a}\n",
        b__yaml="pipeline_defaults:\n  tags: [beta]\npipelines:\n  two:\n    tasks:\n      t: {type: cli, command: echo b}\n",
    )

    project = load_project(config)

    assert project.pipelines["one"].tags == ("alpha",)
    assert project.pipelines["two"].tags == ("beta",)


def test_two_files_may_each_define_the_same_variable_name(tmp_path: Path) -> None:
    """The reason file-scoped variables exist.

    A top-level `variables:` block merges across every file, so two teams both
    wanting a `batch_size` collide and one of them has to rename it. Inside
    `pipeline_defaults` the name is private to the file.
    """
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Scoped\nworkspace: .\ninclude: [claims.yaml, reports.yaml]\n',
        claims__yaml="\n".join(
            [
                "pipeline_defaults:",
                "  variables:",
                "    batch_size: 500",
                "    source: ECW",
                "pipelines:",
                "  claim_extract:",
                "    tasks:",
                "      t: {type: cli, command: 'echo {source} {batch_size}'}",
            ]
        ),
        reports__yaml="\n".join(
            [
                "pipeline_defaults:",
                "  variables:",
                "    batch_size: 50",
                "    source: ATHENA",
                "pipelines:",
                "  report_build:",
                "    tasks:",
                "      t: {type: cli, command: 'echo {source} {batch_size}'}",
            ]
        ),
    )

    project = load_project(config)

    assert "echo ECW 500" in project.pipelines["claim_extract"].tasks["t"].command_preview
    assert "echo ATHENA 50" in project.pipelines["report_build"].tasks["t"].command_preview


def test_the_same_name_at_the_top_level_of_two_files_is_still_a_collision(tmp_path: Path) -> None:
    """Top-level `variables:` stays global, so the old error is unchanged."""
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Clash\nworkspace: .\ninclude: [a.yaml, b.yaml]\n',
        a__yaml="variables:\n  batch_size: 500\npipelines:\n  one:\n    tasks:\n      t: {type: cli, command: echo a}\n",
        b__yaml="variables:\n  batch_size: 50\npipelines:\n  two:\n    tasks:\n      t: {type: cli, command: echo b}\n",
    )

    with pytest.raises(ConfigError, match=r"variables\.batch_size"):
        load_project(config)


def test_variable_precedence_is_pipeline_then_file_then_project(tmp_path: Path) -> None:
    """Narrowest wins, and each layer can build on the one above it."""
    config = _write(
        tmp_path,
        piply__yaml="\n".join(
            [
                'version: "1"',
                "title: Layers",
                "workspace: .",
                "include: [team.yaml]",
                "variables:",
                "  region: global-region",
                "  owner: platform",
                "  stage: prod",
            ]
        ),
        team__yaml="\n".join(
            [
                "pipeline_defaults:",
                "  variables:",
                "    owner: claims-team",
                "    label: '{stage}-{owner}'",  # sees the project's `stage`
                "pipelines:",
                "  inherits_file:",
                "    tasks:",
                "      t: {type: cli, command: 'echo {region} {owner} {label}'}",
                "  overrides_it:",
                "    variables:",
                "      owner: one-pipeline",
                "    tasks:",
                "      t: {type: cli, command: 'echo {region} {owner} {label}'}",
            ]
        ),
    )

    project = load_project(config)

    # Project value passes through; file value beats project; file value may
    # interpolate a project one.
    assert "echo global-region claims-team prod-claims-team" in (
        project.pipelines["inherits_file"].tasks["t"].command_preview
    )
    # The pipeline beats the file, but `label` was already resolved from the file.
    assert "echo global-region one-pipeline prod-claims-team" in (
        project.pipelines["overrides_it"].tasks["t"].command_preview
    )


def test_file_variables_do_not_leak_into_another_file(tmp_path: Path) -> None:
    """A name defined in one file must be unknown in the next."""
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Leak\nworkspace: .\ninclude: [has.yaml, hasnt.yaml]\n',
        has__yaml=(
            "pipeline_defaults:\n  variables:\n    secret_label: only-here\n"
            "pipelines:\n  knows:\n    tasks:\n      t: {type: cli, command: 'echo {secret_label}'}\n"
        ),
        hasnt__yaml="pipelines:\n  unaware:\n    tasks:\n      t: {type: cli, command: 'echo {secret_label}'}\n",
    )

    project = load_project(config)

    assert "echo only-here" in project.pipelines["knows"].tasks["t"].command_preview
    # Unresolved placeholders are left verbatim rather than silently emptied.
    assert "{secret_label}" in project.pipelines["unaware"].tasks["t"].command_preview


def test_file_variables_reach_deployments_in_that_file(tmp_path: Path) -> None:
    """One file per tenant is the layout with the most repetition to remove."""
    config = _write(
        tmp_path,
        piply__yaml="\n".join(
            [
                'version: "1"',
                "title: Tenants",
                "workspace: .",
                "include: [ecw.yaml]",
                "pipeline_templates:",
                "  extraction:",
                "    tasks:",
                "      t: {type: cli, command: 'echo {source_system}'}",
            ]
        ),
        ecw__yaml="\n".join(
            [
                "pipeline_defaults:",
                "  variables:",
                "    source_system: ECW",
                "pipeline_deployments:",
                "  ecw_extract: {template: extraction}",
                "  ecw_reconcile: {template: extraction}",
            ]
        ),
    )

    project = load_project(config)

    for deployment in ("ecw_extract", "ecw_reconcile"):
        assert "echo ECW" in project.pipelines[deployment].tasks["t"].command_preview, deployment


def test_a_malformed_file_variables_block_names_its_file(tmp_path: Path) -> None:
    """The error has to say which file, with one `pipeline_defaults` per file."""
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Bad\nworkspace: .\ninclude: [claims.yaml]\n',
        claims__yaml=(
            "pipeline_defaults:\n  variables:\n    - not\n    - a mapping\n"
            "pipelines:\n  p:\n    tasks:\n      t: {type: cli, command: echo hi}\n"
        ),
    )

    with pytest.raises(ConfigError) as error:
        load_project(config)

    assert "claims.yaml" in str(error.value)
    assert "variables" in str(error.value)


def test_a_variable_collision_points_at_the_file_scoped_form(tmp_path: Path) -> None:
    """ "Defined twice" alone leaves you to work out which of two fixes applies."""
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Clash\nworkspace: .\ninclude: [a.yaml, b.yaml]\n',
        a__yaml="variables:\n  batch_size: 500\npipelines:\n  one:\n    tasks:\n      t: {type: cli, command: echo a}\n",
        b__yaml="variables:\n  batch_size: 50\npipelines:\n  two:\n    tasks:\n      t: {type: cli, command: echo b}\n",
    )

    with pytest.raises(ConfigError) as error:
        load_project(config)

    assert "pipeline_defaults.variables" in str(error.value)


def test_a_destination_collision_points_at_distinct_names(tmp_path: Path) -> None:
    """Destinations stay project-wide on purpose, so the fix is the opposite one.

    A destination is a real channel; one name meaning two different channels is
    a trap for whoever reads the settings page. The message says so rather than
    suggesting a private form that deliberately does not exist.
    """
    config = _write(
        tmp_path,
        piply__yaml='version: "1"\ntitle: Clash\nworkspace: .\ninclude: [a.yaml, b.yaml]\n',
        a__yaml=(
            "notifications:\n  teams:\n    oncall: {type: channel, webhook: 'https://example.invalid/a'}\n"
            "pipelines:\n  one:\n    tasks:\n      t: {type: cli, command: echo a}\n"
        ),
        b__yaml=(
            "notifications:\n  teams:\n    oncall: {type: channel, webhook: 'https://example.invalid/b'}\n"
            "pipelines:\n  two:\n    tasks:\n      t: {type: cli, command: echo b}\n"
        ),
    )

    with pytest.raises(ConfigError) as error:
        load_project(config)

    message = str(error.value)
    assert "distinct names" in message
    assert "pipeline_defaults.notifications.on_failure" in message
    # And it must not suggest a private destination form, which does not exist.
    assert "pipeline_defaults.notifications.teams" not in message
