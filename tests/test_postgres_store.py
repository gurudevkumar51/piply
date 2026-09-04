"""Behaviour parity tests for the optional PostgreSQL metadata store.

These run against a real PostgreSQL server. Set ``PIPLY_TEST_POSTGRES_DSN`` to
enable them; without it the whole module is skipped, so the default developer
setup still needs nothing but SQLite.

    docker run -d --name piply-pg -e POSTGRES_PASSWORD=piply \\
        -e POSTGRES_USER=piply -e POSTGRES_DB=piply -p 55432:5432 postgres:16-alpine
    PIPLY_TEST_POSTGRES_DSN=postgresql://piply:piply@127.0.0.1:55432/piply pytest tests/test_postgres_store.py
"""

from __future__ import annotations

import os
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from piply.core.dialects import (
    PostgresDialect,
    SqliteDialect,
    build_dialect,
    is_postgres_dsn,
    normalize_postgres_dsn,
    translate_placeholders,
)
from piply.core.service import PipelineService
from piply.core.store import RunStore

DSN = os.environ.get("PIPLY_TEST_POSTGRES_DSN")

pytestmark = pytest.mark.skipif(not DSN, reason="PIPLY_TEST_POSTGRES_DSN is not set")


@pytest.fixture()
def dsn() -> str:
    """Return a DSN pointing at an empty schema."""
    import psycopg

    with psycopg.connect(DSN, autocommit=True) as connection:
        connection.execute("DROP SCHEMA public CASCADE")
        connection.execute("CREATE SCHEMA public")
    return DSN


def _project(tmp_path: Path, body: str) -> Path:
    """Write a config and workspace, returning the config path."""
    (tmp_path / "workspace").mkdir(exist_ok=True)
    config_path = tmp_path / "piply.yaml"
    config_path.write_text(body, encoding="utf-8")
    return config_path


SIMPLE = "\n".join(
    [
        'version: "1"',
        "title: Postgres Store Test",
        "workspace: workspace",
        "pipelines:",
        "  flow:",
        "    tasks:",
        "      first:",
        "        type: cli",
        "        priority: high",
        "        command: python -c \"print('first done')\"",
        "      second:",
        "        type: cli",
        "        depends_on: [first]",
        "        command: python -c \"print('second done')\"",
    ]
)


def test_schema_initialises_and_is_idempotent(dsn: str) -> None:
    """Creating the store twice against one database is safe."""
    first = RunStore(dsn)
    second = RunStore(dsn)

    assert first.is_sqlite is False
    assert second.dialect.name == "postgres"
    # The DSN password never appears in the description.
    assert "piply:***@" in first.describe_location()
    assert first.database_size_bytes() == 0


def test_full_run_lifecycle_on_postgres(tmp_path: Path, dsn: str) -> None:
    """A run executes and persists exactly as it does on SQLite."""
    service = PipelineService(config_path=_project(tmp_path, SIMPLE), database_path=dsn)
    run = service.trigger_pipeline("flow", wait=True)
    record, task_runs, logs = service.get_run(run.run_id)

    assert record.status == "success"
    assert record.task_count == 2
    assert record.successful_tasks == 2
    assert [task.task_id for task in task_runs] == ["first", "second"]
    assert task_runs[0].priority == 1
    assert any("first done" in line.message for line in logs)
    assert any("second done" in line.message for line in logs)
    # Task-scoped log attribution survives the round trip.
    assert {line.task_id for line in logs if line.task_id} == {"first", "second"}


def test_aggregates_and_metrics_on_postgres(tmp_path: Path, dsn: str) -> None:
    """Counting, grouping, and the epoch-difference metrics work on Postgres."""
    service = PipelineService(config_path=_project(tmp_path, SIMPLE), database_path=dsn)
    service.trigger_pipeline("flow", wait=True)
    service.trigger_pipeline("flow", wait=True)

    counts = service.store.status_counts()
    assert counts["runs"]["success"] == 2
    assert counts["tasks"]["success"] == 4
    assert counts["triggers"]["manual"] == 2

    # duration_metrics uses EXTRACT(EPOCH ...) rather than julianday().
    durations = service.store.duration_metrics()
    assert durations["completed_runs"] == 2
    assert durations["total_seconds"] > 0
    assert durations["average_seconds"] > 0

    summaries = service.list_pipelines()
    assert summaries[0].last_run is not None
    assert summaries[0].active_runs == 0

    latest = service.store.latest_runs_by_pipeline()
    assert set(latest) == {"flow"}


def test_trigger_queue_dedupe_on_postgres(tmp_path: Path, dsn: str) -> None:
    """INSERT ... ON CONFLICT DO NOTHING enforces the queue dedupe key."""
    config = "\n".join(
        [
            'version: "1"',
            "title: Queue Test",
            "workspace: workspace",
            "pipelines:",
            "  flow:",
            "    tasks:",
            "      main: {type: cli, command: echo hi}",
        ]
    )
    service = PipelineService(config_path=_project(tmp_path, config), database_path=dsn)

    assert service.enqueue_pipeline_trigger("flow", trigger="manual", dedupe_key="same") is True
    assert service.enqueue_pipeline_trigger("flow", trigger="manual", dedupe_key="same") is False
    assert service.store.count_queue() == 1

    metrics = service.store.queue_metrics()
    assert metrics["queued"] == 1


def test_log_search_with_wildcards_on_postgres(tmp_path: Path, dsn: str) -> None:
    """A LIKE '%...%' search survives placeholder translation.

    psycopg treats `%` as a placeholder marker, so an unescaped LIKE pattern
    would raise instead of matching.
    """
    service = PipelineService(config_path=_project(tmp_path, SIMPLE), database_path=dsn)
    run = service.trigger_pipeline("flow", wait=True)

    matches = service.search_logs(query="second done")
    assert matches
    assert all("second done" in line.message for line in matches)

    scoped = service.search_logs(query="done", pipeline_id="flow", task_id="first")
    assert scoped
    assert {line.task_id for line in scoped} == {"first"}

    tail = service.tail_logs(run_id=run.run_id, limit=500)
    assert tail
    assert tail[0]["pipeline_id"] == "flow"


def test_retention_prune_on_postgres(tmp_path: Path, dsn: str) -> None:
    """Prune deletes history and the per-pipeline cap uses LIMIT ALL OFFSET."""
    service = PipelineService(config_path=_project(tmp_path, SIMPLE), database_path=dsn)
    for _ in range(4):
        service.trigger_pipeline("flow", wait=True)

    planned = service.prune(dry_run=True, max_runs_per_pipeline=1, run_retention_days=0, log_retention_days=0)
    assert planned["runs_deleted"] == 3

    summary = service.prune(max_runs_per_pipeline=1, run_retention_days=0, log_retention_days=0)
    assert summary["runs_deleted"] == 3
    assert len(service.list_runs(pipeline_id="flow")) == 1


def test_recovery_and_reconciliation_on_postgres(tmp_path: Path, dsn: str) -> None:
    """Startup recovery and heartbeat reconciliation behave the same."""
    service = PipelineService(config_path=_project(tmp_path, SIMPLE), database_path=dsn)
    pipeline = service.get_pipeline("flow")
    run = service.store.create_run(pipeline, trigger="manual")
    service.store.mark_running(run.run_id)
    service.store.mark_task_running(run.run_id, "first")

    import psycopg

    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute("UPDATE runs SET owner_pid = %s WHERE id = %s", (999_999, run.run_id))

    recovered = PipelineService(config_path=_project(tmp_path, SIMPLE), database_path=dsn)
    record, task_runs, _ = recovered.get_run(run.run_id)
    assert record.status == "interrupted"
    assert task_runs[0].status == "interrupted"

    # Heartbeat-based reconciliation on a second run.
    other = service.store.create_run(pipeline, trigger="manual")
    service.store.mark_running(other.run_id)
    stale = (datetime.now(timezone.utc) - timedelta(hours=3)).isoformat()
    with psycopg.connect(dsn, autocommit=True) as connection:
        connection.execute("UPDATE runs SET heartbeat_at = %s WHERE id = %s", (stale, other.run_id))

    assert other.run_id in service.store.reconcile_stale_runs(3600)
    assert service.store.get_run(other.run_id).status == "interrupted"


def test_run_config_and_backfill_on_postgres(tmp_path: Path, dsn: str) -> None:
    """JSON run-configuration snapshots round-trip through Postgres."""
    service = PipelineService(config_path=_project(tmp_path, SIMPLE), database_path=dsn)
    original = service.trigger_pipeline(
        "flow",
        wait=True,
        tenant_id="tenant-a",
        inherited_variables={"season": "q3"},
    )

    snapshot = service.store.get_run_config(original.run_id)
    assert snapshot["tenant_id"] == "tenant-a"
    assert snapshot["inherited_variables"] == {"season": "q3"}

    replay = service.backfill_run(original.run_id, wait=True)
    assert service.store.get_run(replay.run_id).status == "success"
    assert service.store.get_run_config(replay.run_id)["inherited_variables"] == {"season": "q3"}


def test_downstream_chain_on_postgres(tmp_path: Path, dsn: str) -> None:
    """Pipeline triggering, child lookup, and lineage work on Postgres."""
    config = "\n".join(
        [
            'version: "1"',
            "title: Chain Test",
            "workspace: workspace",
            "pipelines:",
            "  upstream:",
            "    triggers_on_success: [downstream]",
            "    tasks:",
            "      emit: {type: cli, command: echo emitted}",
            "  downstream:",
            "    tasks:",
            "      consume: {type: cli, command: echo consumed}",
        ]
    )
    service = PipelineService(config_path=_project(tmp_path, config), database_path=dsn)
    parent = service.trigger_pipeline("upstream", wait=True)

    for _ in range(60):
        if service.list_runs(pipeline_id="downstream"):
            break
        time.sleep(0.2)

    children = service.store.list_child_runs(parent.run_id)
    assert len(children) == 1
    assert children[0].pipeline_id == "downstream"

    links = service.downstream_run_links(service.store.get_run(parent.run_id))
    assert [item["pipeline_id"] for item in links] == ["downstream"]
    assert links[0]["run_id"] == children[0].run_id


def test_artifacts_sensor_health_and_meta_on_postgres(tmp_path: Path, dsn: str) -> None:
    """The upsert-heavy tables round-trip on Postgres."""
    service = PipelineService(config_path=_project(tmp_path, SIMPLE), database_path=dsn)
    run = service.trigger_pipeline("flow", wait=True)

    service.store.record_task_artifacts(
        run.run_id,
        "first",
        [
            {
                "name": "out.csv",
                "path": "/tmp/out.csv",
                "size_bytes": 12,
                "content_type": "text/csv",
                "modified_at": None,
            }
        ],
    )
    # Recording the same path twice updates rather than duplicating.
    service.store.record_task_artifacts(
        run.run_id,
        "first",
        [
            {
                "name": "out.csv",
                "path": "/tmp/out.csv",
                "size_bytes": 34,
                "content_type": "text/csv",
                "modified_at": None,
            }
        ],
    )
    artifacts = service.store.list_task_artifacts(run.run_id)
    assert len(artifacts) == 1
    assert artifacts[0]["size_bytes"] == 34

    service.store.record_sensor_health(
        "flow:watch",
        pipeline_id="flow",
        sensor_id="watch",
        sensor_type="file_sensor",
        succeeded=False,
        produced_event=False,
        error="boom",
    )
    service.store.record_sensor_health(
        "flow:watch",
        pipeline_id="flow",
        sensor_id="watch",
        sensor_type="file_sensor",
        succeeded=False,
        produced_event=False,
        error="boom again",
    )
    health = {item["sensor_id"]: item for item in service.store.list_sensor_health()}
    assert health["watch"]["status"] == "failing"
    assert health["watch"]["consecutive_failures"] == 2
    assert health["watch"]["poll_count"] == 2

    service.store.set_meta_many({"a": "1", "b": "2"})
    assert service.store.get_meta("a") == "1"
    service.store.set_meta("a", "3")
    assert service.store.get_meta("a") == "3"


def test_accounts_and_permissions_on_postgres(tmp_path: Path, dsn: str) -> None:
    """Accounts, grants, and the SMTP settings round-trip on Postgres.

    ``users`` and ``user_permissions`` use a composite ``ON CONFLICT`` target
    and an integer-backed boolean, both of which behave differently enough
    between the two backends to be worth proving against a real server.
    """
    service = PipelineService(config_path=_project(tmp_path, SIMPLE), database_path=dsn)

    admin = service.create_user("root", "root-secret", role="admin")
    assert admin.role == "admin"
    assert admin.can("run", "flow") is True

    alice = service.create_user("alice", "alice-secret", permissions={"flow": "view,run"})
    assert alice.can("view", "flow") is True
    assert alice.can("edit", "flow") is False

    # Re-granting the same pipeline updates in place rather than duplicating.
    alice = service.grant_permission("alice", "flow", "view")
    assert alice.can("run", "flow") is False
    assert [user.username for user in service.list_users()] == ["alice", "root"]

    assert service.authenticate("alice", "alice-secret") is not None
    assert service.authenticate("alice", "wrong") is None
    assert service.authenticate("nobody", "alice-secret") is None

    # is_active is stored as an integer; deactivating must actually block login.
    service.update_user("alice", is_active=False)
    assert service.authenticate("alice", "alice-secret") is None

    # The last-admin guard reads back the integer flag correctly.
    with pytest.raises(Exception) as excinfo:
        service.delete_user("root")
    assert "only active admin" in str(excinfo.value)

    # Grants are cleared in the same transaction as the account rather than by
    # a foreign key, so recreating the username must not inherit old access.
    service.delete_user("alice")
    assert [user.username for user in service.list_users()] == ["root"]
    recreated = service.create_user("alice", "fresh-secret")
    assert recreated.can("view", "flow") is False

    saved = service.save_smtp_settings({"host": "smtp.example.com", "port": 2525, "password": "s3cret"})
    assert saved["configured"] is True
    assert saved["password_set"] is True
    assert "s3cret" not in str(saved)
    # A blank password on a later edit keeps the stored one.
    saved = service.save_smtp_settings({"host": "smtp2.example.com", "password": ""})
    assert saved["host"] == "smtp2.example.com"
    assert saved["password_set"] is True


def test_run_history_filters_and_sorting_on_postgres(tmp_path: Path, dsn: str) -> None:
    """The window-function history, filters, sorts, and lineage work on Postgres."""
    config = "\n".join(
        [
            'version: "1"',
            "title: History Test",
            "workspace: workspace",
            "pipelines:",
            "  upstream:",
            "    triggers_on_success: [downstream]",
            "    tasks:",
            "      emit: {type: cli, command: echo emitted}",
            "  downstream:",
            "    tasks:",
            "      consume: {type: cli, command: echo consumed}",
        ]
    )
    service = PipelineService(config_path=_project(tmp_path, config), database_path=dsn)
    for _ in range(3):
        service.trigger_pipeline("upstream", wait=True)

    for _ in range(60):
        if len(service.list_runs(pipeline_id="downstream")) >= 3:
            break
        time.sleep(0.2)

    # ROW_NUMBER() OVER (PARTITION BY ...) drives the pipeline-row status dots.
    history = service.store.recent_runs_by_pipeline(limit=2)
    assert set(history) == {"upstream", "downstream"}
    assert len(history["upstream"]) == 2
    assert all(record.pipeline_id == "upstream" for record in history["upstream"])

    # "trigger" is a keyword and has to stay quoted in both the filter and the sort.
    assert len(service.list_runs(trigger="manual")) == 3
    assert len(service.list_runs(trigger="pipeline")) == 3
    assert len(service.list_runs(trigger="manual,pipeline")) == 6
    assert len(service.list_runs(status="success")) == 6
    assert service.list_runs(status="failed") == []
    assert len(service.list_runs(pipeline_id="downstream", trigger="pipeline")) == 3

    for sort in [*RunStore.RUN_SORTS, "duration_desc", "duration_asc", "bogus_key"]:
        assert len(service.list_runs(sort=sort)) == 6, sort

    ascending = service.list_runs(sort="started_asc")
    descending = service.list_runs(sort="started_desc")
    assert [run.run_id for run in ascending] == [run.run_id for run in reversed(descending)]

    # Lineage walks parent_run_id one generation at a time.
    downstream_runs = service.list_runs(pipeline_id="downstream")
    lineage = service.lineage_for_runs(downstream_runs)
    assert len(lineage) == 3
    for run in downstream_runs:
        chain = lineage[run.run_id]
        assert [step["pipeline_id"] for step in chain] == ["upstream"]


def test_migrating_sqlite_to_postgres_preserves_history(tmp_path: Path, dsn: str) -> None:
    """`piply migrate-db` moves a SQLite runtime onto PostgreSQL intact."""
    config = "\n".join(
        [
            'version: "1"',
            "title: Migration Test",
            "workspace: workspace",
            "pipelines:",
            "  upstream:",
            "    triggers_on_success: [downstream]",
            "    tasks:",
            "      emit: {type: cli, command: echo emitted}",
            "  downstream:",
            "    tasks:",
            "      consume: {type: cli, command: echo consumed}",
        ]
    )
    config_path = _project(tmp_path, config)
    sqlite_path = tmp_path / "piply.db"

    source_service = PipelineService(config_path=config_path, database_path=sqlite_path)
    source_service.create_user("root", "root-password", role="admin")
    source_service.create_user("alice", "alice-password", permissions={"upstream": "view,run"})
    parent = source_service.trigger_pipeline("upstream", wait=True)
    for _ in range(60):
        if source_service.list_runs(pipeline_id="downstream"):
            break
        time.sleep(0.2)
    source_service.store.set_meta("smtp_host", "smtp.example.com")
    before = source_service.store.row_counts()

    copied = RunStore(sqlite_path).copy_into(RunStore(dsn))
    assert copied == before

    migrated = PipelineService(config_path=config_path, database_path=dsn)
    assert migrated.store.row_counts() == before

    # Ids are preserved, so retry chains and downstream links still resolve.
    child = next(item for item in migrated.list_runs(limit=50) if item.pipeline_id == "downstream")
    assert child.parent_run_id == parent.run_id
    assert [step["pipeline_id"] for step in migrated.lineage_for_runs([child])[child.run_id]] == ["upstream"]

    # Accounts survive with their grants, and the stored hash still verifies.
    assert [user.username for user in migrated.list_users()] == ["alice", "root"]
    assert migrated.get_user("alice").permissions == {"upstream": frozenset({"view", "run"})}
    assert migrated.authenticate("alice", "alice-password") is not None
    assert migrated.store.get_meta("smtp_host") == "smtp.example.com"
    assert migrated.store.list_logs(parent.run_id, limit=100)

    # The identity sequences must be advanced past the copied ids, or the first
    # new insert collides on the primary key.
    fresh = migrated.trigger_pipeline("upstream", wait=True)
    assert fresh.status == "success"
    assert migrated.store.list_logs(fresh.run_id, limit=100)


def test_migration_refuses_a_non_empty_target(tmp_path: Path, dsn: str) -> None:
    """Merging two histories has no safe answer, so it is refused rather than guessed."""
    config_path = _project(tmp_path, SIMPLE)
    sqlite_path = tmp_path / "piply.db"
    source = PipelineService(config_path=config_path, database_path=sqlite_path)
    source.trigger_pipeline("flow", wait=True)

    target = PipelineService(config_path=config_path, database_path=dsn)
    target.trigger_pipeline("flow", wait=True)

    with pytest.raises(ValueError) as excinfo:
        RunStore(sqlite_path).copy_into(RunStore(dsn))
    assert "already contains data" in str(excinfo.value)

    # An empty source is a no-op rather than an error.
    empty = tmp_path / "empty.db"
    RunStore(empty)
    assert sum(RunStore(empty).row_counts().values()) == 0


def test_backup_command_refuses_a_server_store(tmp_path: Path, dsn: str) -> None:
    """`piply backup` explains that a server store needs its own tooling."""
    store = RunStore(dsn)
    with pytest.raises(RuntimeError) as excinfo:
        store.backup_to(tmp_path / "snapshot.db")
    assert "pg_dump" in str(excinfo.value)


# --- Dialect unit tests, no server required ------------------------------------


@pytest.mark.parametrize("marker", [True])
def test_placeholder_translation(marker: bool) -> None:
    """`?` becomes `%s`, but only outside string literals, and `%` is escaped."""
    del marker
    assert translate_placeholders("SELECT * FROM t WHERE a = ?") == "SELECT * FROM t WHERE a = %s"
    # A literal question mark inside quotes is left alone.
    assert translate_placeholders("SELECT '?' , ?") == "SELECT '?' , %s"
    # LIKE wildcards must survive; psycopg would otherwise read them as markers.
    assert translate_placeholders("WHERE m LIKE '%x%' AND a = ?") == "WHERE m LIKE '%%x%%' AND a = %s"


def test_dsn_detection_and_normalisation() -> None:
    """Postgres URLs are recognised; anything else stays a SQLite path."""
    assert is_postgres_dsn("postgresql://u:p@h/db") is True
    assert is_postgres_dsn("postgres://u:p@h/db") is True
    assert is_postgres_dsn("postgresql+psycopg://u:p@h/db") is True
    assert is_postgres_dsn("/var/lib/piply/piply.db") is False
    assert is_postgres_dsn("mysql://u:p@h/db") is False

    # SQLAlchemy-style driver suffixes are dropped for psycopg.
    assert normalize_postgres_dsn("postgresql+psycopg2://u:p@h/db") == "postgresql://u:p@h/db"
    assert normalize_postgres_dsn("postgres://u:p@h/db") == "postgresql://u:p@h/db"


def test_build_dialect_selects_the_backend(tmp_path: Path) -> None:
    """A path yields SQLite; a Postgres URL yields Postgres."""
    assert isinstance(build_dialect(tmp_path / "runs.db"), SqliteDialect)
    assert isinstance(build_dialect("postgresql://u:p@h/db"), PostgresDialect)
    # The description never leaks the password.
    assert "p@h" not in build_dialect("postgresql://u:p@h/db").describe()
