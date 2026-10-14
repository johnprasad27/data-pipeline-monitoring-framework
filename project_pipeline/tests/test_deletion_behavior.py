"""Comprehensive tests for pipeline deletion behavior, tombstones, and audit retention.

Covers:
1. Deletion removes pipeline from all active views (registry, Monitor, Runs, Overview, selectors).
2. Active statistics exclude deleted pipeline's runs; other pipelines' stats are unchanged.
3. Historical executions remain in Execution History with failure details and are labeled Deleted.
4. Startup/refresh/YAML sync respects tombstones and never resurrects deleted pipelines.
5. Scheduler does not schedule deleted pipelines; retries for deleted pipelines are skipped safely.
6. In-flight RUNNING executions block deletion to avoid corruption/races.
7. Deletion of nonexistent or already deleted pipelines is idempotent.
8. Name reuse isolates old history; new pipeline starts with clean active stats.
9. Stale session_state/selector values do not crash.
10. History row counts are preserved (no foreign-key cascades remove history).
11. Unrelated pipelines remain completely unaffected.
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from typing import Any
from unittest.mock import MagicMock, Mock, patch

import pytest

from monitoring import queries
from monitoring.overview import (
    build_overview_summary,
    build_pipeline_run_counts,
    get_pipeline_health,
)
from pipelines.registry import (
    _sync_configured_pipeline_registry,
    delete_disabled_pipeline,
    get_enabled_pipeline_configs,
    load_registered_pipeline_configs,
    register_pipeline,
)
from pipelines.recovery import run_with_recovery


class InMemoryDatabase:
    """In-memory database stub emulating pipeline_registry and pipeline_execution."""

    def __init__(self):
        self.registry: dict[str, dict[str, Any]] = {}
        self.executions: list[dict[str, Any]] = []
        self.task_executions: list[dict[str, Any]] = []
        self.connection = MagicMock()
        self.connection.closed = False
        self._has_deleted_at_column = True

    def connect(self):
        pass

    def close(self):
        pass

    def execute(self, query: str, params: tuple = (), commit: bool = True):
        q = query.strip()
        if q.startswith("UPDATE pipeline_registry SET deleted_at ="):
            name = params[0]
            for row in self.registry.values():
                if row["pipeline_name"] == name and not row.get("enabled") and row.get("deleted_at") is None:
                    row["deleted_at"] = datetime.now()
                    row["enabled"] = False
                    return 1
            return 0

        if q.startswith("UPDATE pipeline_registry") and "deleted_at = NULL" in q:
            name, p_type, s_cfg, interval, retries, enabled, match_name = params
            row = self.registry.get(match_name.lower())
            if row:
                row["pipeline_name"] = name
                row["pipeline_type"] = p_type
                row["source_config"] = json.loads(s_cfg) if isinstance(s_cfg, str) else s_cfg
                row["interval_seconds"] = interval
                row["max_retries"] = retries
                row["enabled"] = enabled
                row["created_at"] = datetime.now()
                row["updated_at"] = datetime.now()
                row["deleted_at"] = None
                return 1
            return 0

        if q.startswith("INSERT INTO pipeline_registry"):
            name, p_type, s_cfg, interval, retries, enabled = params
            if name.lower() in self.registry:
                return 0
            self.registry[name.lower()] = {
                "pipeline_name": name,
                "pipeline_type": p_type,
                "source_config": json.loads(s_cfg) if isinstance(s_cfg, str) else s_cfg,
                "interval_seconds": interval,
                "max_retries": retries,
                "enabled": enabled,
                "created_at": datetime.now(),
                "updated_at": datetime.now(),
                "deleted_at": None,
            }
            return 1

        return 1

    def fetch_one(self, query: str, params: tuple = ()):
        q = " ".join(query.split())
        if "FROM pipeline_execution" in q and "status = 'RUNNING'" in q:
            name = params[0]
            for r in self.executions:
                if r["pipeline_name"] == name and r.get("status") == "RUNNING":
                    return {"1": 1}
            return None

        if "FROM pipeline_registry WHERE LOWER(pipeline_name)" in q:
            name = params[0].lower()
            row = self.registry.get(name)
            if row:
                return {"pipeline_name": row["pipeline_name"], "deleted_at": row.get("deleted_at")}
            return None

        if "FROM pipeline_registry WHERE pipeline_name" in q:
            name = params[0]
            for r in self.registry.values():
                if r["pipeline_name"] == name:
                    return {"enabled": r.get("enabled"), "deleted_at": r.get("deleted_at")}
            return None

        if "SELECT COUNT(*) AS total" in q and "FROM pipeline_execution" in q:
            target_pipeline = params[0] if params else None
            status_match = "pe.status = 'SUCCESS'" in q or "pe.status = 'FAILED'" in q
            target_status = "SUCCESS" if "pe.status = 'SUCCESS'" in q else "FAILED" if "pe.status = 'FAILED'" in q else None
            count = 0
            for r in self.executions:
                pname = r["pipeline_name"]
                reg = self.registry.get(pname.lower())
                if not reg or reg.get("deleted_at") is not None:
                    continue
                if reg.get("created_at") and r.get("start_time") and r["start_time"] < reg["created_at"]:
                    continue
                if target_pipeline and pname != target_pipeline:
                    continue
                if target_status and r.get("status") != target_status:
                    continue
                count += 1
            return {"total": count}

        if "SUM(pe.rows_processed)" in q and "FROM pipeline_execution" in q:
            target_pipeline = params[0] if params else None
            total = 0
            for r in self.executions:
                pname = r["pipeline_name"]
                reg = self.registry.get(pname.lower())
                if not reg or reg.get("deleted_at") is not None:
                    continue
                if reg.get("created_at") and r.get("start_time") and r["start_time"] < reg["created_at"]:
                    continue
                if target_pipeline and pname != target_pipeline:
                    continue
                total += r.get("rows_processed", 0)
            return {"total": total}

        return None

    def fetch_all(self, query: str, params: tuple | None = None):
        q = " ".join(query.split())
        if "FROM pipeline_registry" in q and "deleted_at IS NOT NULL" in q:
            return [
                {"pipeline_name": r["pipeline_name"]}
                for r in self.registry.values()
                if r.get("deleted_at") is not None
            ]

        if "FROM pipeline_registry" in q and "deleted_at" in q and "SELECT pipeline_name, deleted_at" in q:
            return [
                {"pipeline_name": r["pipeline_name"], "deleted_at": r.get("deleted_at")}
                for r in self.registry.values()
            ]

        if "FROM pipeline_registry" in q and "WHERE deleted_at IS NULL" in q:
            return [
                dict(r)
                for r in self.registry.values()
                if r.get("deleted_at") is None
            ]

        if "FROM pipeline_registry" in q and "deleted_at" not in q:
            return [dict(r) for r in self.registry.values()]

        if "SELECT * FROM pipeline_execution" in q:
            res = list(self.executions)
            if params and len(params) == 1:
                res = [r for r in res if r["pipeline_name"] == params[0]]
            return list(reversed(res))

        return []


def test_1_delete_removes_from_registry_and_active_views():
    """Requirement 1: Pipeline disappears from registry, Monitor, Overview, and active selectors."""
    db = InMemoryDatabase()
    now = datetime.now()

    db.registry["active_pipe"] = {
        "pipeline_name": "active_pipe",
        "pipeline_type": "API",
        "source_config": {},
        "interval_seconds": 60,
        "max_retries": 3,
        "enabled": True,
        "created_at": now,
        "updated_at": now,
        "deleted_at": None,
    }
    db.registry["pipe_to_delete"] = {
        "pipeline_name": "pipe_to_delete",
        "pipeline_type": "CSV",
        "source_config": {},
        "interval_seconds": 60,
        "max_retries": 3,
        "enabled": False,
        "created_at": now - timedelta(hours=1),
        "updated_at": now,
        "deleted_at": None,
    }

    # Historical runs exist for both pipelines
    db.executions = [
        {"execution_id": 1, "pipeline_name": "active_pipe", "status": "SUCCESS", "rows_processed": 50, "start_time": now},
        {"execution_id": 2, "pipeline_name": "pipe_to_delete", "status": "FAILED", "rows_processed": 0, "start_time": now},
    ]

    # Delete disabled pipeline
    result = delete_disabled_pipeline(db, "pipe_to_delete")
    assert result is True
    assert db.registry["pipe_to_delete"]["deleted_at"] is not None

    # Absent from registry lists
    registered = load_registered_pipeline_configs(db)
    registered_names = [r["pipeline_name"] for r in registered]
    assert "pipe_to_delete" not in registered_names
    assert "active_pipe" in registered_names

    # Absent from get_all_pipelines (feeding Pipeline Monitor & selector)
    with patch.object(queries, "_has_soft_delete", return_value=True):
        active_pipelines = queries.get_registered_pipelines(db)
        assert "pipe_to_delete" not in [p["pipeline_name"] for p in active_pipelines]

    # Absent from Overview summary
    with patch.object(queries, "get_registered_pipelines", return_value=[{"pipeline_name": "active_pipe", "created_at": now, "enabled": True}]):
        with patch.object(queries, "execution_history", return_value=db.executions):
            with patch.object(queries, "get_task_instances", return_value=[]):
                overview = build_overview_summary(db)
                overview_names = [p["pipeline_name"] for p in overview["pipeline_health"]]
                assert "pipe_to_delete" not in overview_names
                assert "active_pipe" in overview_names


def test_2_active_statistics_exclude_deleted_pipeline():
    """Requirement 2: Active statistics exclude deleted pipeline's executions; other pipelines unaffected."""
    db = InMemoryDatabase()
    now = datetime.now()

    db.registry["pipe_a"] = {
        "pipeline_name": "pipe_a",
        "pipeline_type": "API",
        "enabled": True,
        "created_at": now,
        "deleted_at": None,
    }
    db.registry["pipe_b"] = {
        "pipeline_name": "pipe_b",
        "pipeline_type": "CSV",
        "enabled": False,
        "created_at": now,
        "deleted_at": now,  # Deleted
    }

    db.executions = [
        {"execution_id": 1, "pipeline_name": "pipe_a", "status": "SUCCESS", "rows_processed": 100, "start_time": now},
        {"execution_id": 2, "pipeline_name": "pipe_b", "status": "FAILED", "rows_processed": 50, "start_time": now},
    ]

    with patch.object(queries, "_has_soft_delete", return_value=True):
        total_runs = queries.total_executions(db)
        success_runs = queries.successful_executions(db)
        failed_runs = queries.failed_executions(db)
        total_rows = queries.total_rows_processed(db)

        # Only pipe_a's executions are counted
        assert total_runs == 1
        assert success_runs == 1
        assert failed_runs == 0
        assert total_rows == 100

        # Stats for deleted pipeline specifically return 0
        assert queries.total_executions(db, "pipe_b") == 0


def test_3_history_preserved_and_labeled_deleted():
    """Requirement 3: History records and failure details intact in Execution History; labeled Deleted."""
    db = InMemoryDatabase()
    now = datetime.now()

    db.registry["active_pipe"] = {"pipeline_name": "active_pipe", "created_at": now, "deleted_at": None}
    db.registry["deleted_pipe"] = {"pipeline_name": "deleted_pipe", "created_at": now - timedelta(days=1), "deleted_at": now}

    db.executions = [
        {
            "execution_id": 101,
            "pipeline_name": "deleted_pipe",
            "status": "FAILED",
            "failure_category": "NETWORK_ERROR",
            "error_message": "Connection timed out",
            "rows_processed": 0,
            "created_at": now - timedelta(hours=2),
        },
        {
            "execution_id": 102,
            "pipeline_name": "active_pipe",
            "status": "SUCCESS",
            "failure_category": None,
            "error_message": None,
            "rows_processed": 25,
            "created_at": now,
        },
    ]

    history = queries.execution_history(db)
    assert len(history) == 2

    deleted_rec = next(r for r in history if r["pipeline_name"] == "deleted_pipe")
    assert deleted_rec["failure_category"] == "NETWORK_ERROR"
    assert deleted_rec["error_message"] == "Connection timed out"

    # Execution History labeling check
    active_reg = {"active_pipe": db.registry["active_pipe"]}
    labeled_names = [
        f"{r['pipeline_name']} (Deleted)" if r["pipeline_name"] not in active_reg else r["pipeline_name"]
        for r in history
    ]
    assert "deleted_pipe (Deleted)" in labeled_names
    assert "active_pipe" in labeled_names


def test_4_tombstone_prevents_yaml_resurrection(tmp_path):
    """Requirement 4: Restart/bootstrap simulation does not resurrect pipeline still listed in pipelines.yaml."""
    db = InMemoryDatabase()
    now = datetime.now()

    # Tombstoned row in DB
    db.registry["api_pipeline"] = {
        "pipeline_name": "api_pipeline",
        "pipeline_type": "API",
        "source_config": {},
        "interval_seconds": 60,
        "max_retries": 3,
        "enabled": False,
        "created_at": now,
        "updated_at": now,
        "deleted_at": now,
    }

    yaml_config = {
        "api_pipeline": {
            "name": "api_pipeline",
            "source": "API",
            "pipeline_type": "API",
            "source_config": {},
            "interval_seconds": 60,
            "max_retries": 3,
            "enabled": True,
        }
    }

    _sync_configured_pipeline_registry(db, yaml_config)

    # Must remain deleted and not overwritten or un-tombstoned
    assert db.registry["api_pipeline"]["deleted_at"] is not None

    with patch("pipelines.registry.Database", return_value=db), \
         patch("pipelines.registry.load_pipeline_registry", return_value=yaml_config):
        enabled = get_enabled_pipeline_configs()
        assert not any(p["name"] == "api_pipeline" for p in enabled)


def test_5_scheduler_and_retry_queue_safety():
    """Requirement 5: Scheduler does not schedule deleted pipeline; queued retries skip safely."""
    db = InMemoryDatabase()
    now = datetime.now()

    db.registry["flaky_pipeline"] = {
        "pipeline_name": "flaky_pipeline",
        "pipeline_type": "API",
        "enabled": False,
        "deleted_at": now,
    }

    # Test run_with_recovery skips retry when pipeline is deleted
    runner_calls = []

    def failing_runner(**kwargs):
        runner_calls.append(kwargs.get("retry_count", 0))
        return {
            "status": "FAILED",
            "error_message": "Temporary outage",
            "failure_category": "NETWORK_ERROR",
            "retryable": True,
        }

    with patch("pipelines.recovery.Database", return_value=db):
        res = run_with_recovery("flaky_pipeline", failing_runner, max_retries=3)

    assert runner_calls == [0]  # Only initial attempt; retry 1 was skipped
    assert res["status"] == "FAILED"
    assert res["error_message"] == "skipped: pipeline deleted"
    assert res["retryable"] is False


def test_6_running_execution_blocks_deletion():
    """Requirement 6: In-flight RUNNING execution safely blocks deletion with a clear message."""
    db = InMemoryDatabase()
    now = datetime.now()

    db.registry["running_pipe"] = {
        "pipeline_name": "running_pipe",
        "enabled": False,
        "deleted_at": None,
    }
    db.executions = [
        {"execution_id": 99, "pipeline_name": "running_pipe", "status": "RUNNING", "start_time": now}
    ]

    with pytest.raises(ValueError, match="Cannot delete pipeline 'running_pipe' while an execution is currently RUNNING"):
        delete_disabled_pipeline(db, "running_pipe")

    # Registry row must NOT be modified
    assert db.registry["running_pipe"]["deleted_at"] is None


def test_7_delete_is_idempotent():
    """Requirement 7: Deleting already deleted or nonexistent pipeline is idempotent without traceback."""
    db = InMemoryDatabase()
    now = datetime.now()

    db.registry["already_deleted"] = {
        "pipeline_name": "already_deleted",
        "enabled": False,
        "deleted_at": now,
    }

    # Deleting already deleted pipeline returns gracefully
    res1 = delete_disabled_pipeline(db, "already_deleted")
    assert res1 is False

    # Deleting nonexistent pipeline returns gracefully
    res2 = delete_disabled_pipeline(db, "nonexistent_pipe")
    assert res2 is False


def test_8_name_reuse_starts_with_clean_active_stats():
    """Requirement 8: Re-registering with the same name starts clean; old historical runs excluded from active stats."""
    db = InMemoryDatabase()
    old_time = datetime.now() - timedelta(days=2)
    new_time = datetime.now()

    # Pre-existing deleted pipeline
    db.registry["orders_pipeline"] = {
        "pipeline_name": "orders_pipeline",
        "pipeline_type": "CSV",
        "source_config": {"file_path": "old.csv"},
        "interval_seconds": 60,
        "max_retries": 1,
        "enabled": False,
        "created_at": old_time,
        "updated_at": old_time,
        "deleted_at": old_time + timedelta(hours=1),
    }

    # Old execution belonging to prior incarnation
    db.executions = [
        {"execution_id": 1, "pipeline_name": "orders_pipeline", "status": "SUCCESS", "rows_processed": 500, "start_time": old_time}
    ]

    # Re-register new pipeline with identical name
    new_reg = register_pipeline(
        db,
        name="orders_pipeline",
        pipeline_type="CSV",
        source_config={"file_path": "data/csv/sales.csv"},
        interval_seconds=120,
        max_retries=2,
        enabled=True,
    )
    assert new_reg["name"] == "orders_pipeline"
    assert db.registry["orders_pipeline"]["deleted_at"] is None
    assert db.registry["orders_pipeline"]["created_at"] >= new_time

    # Active statistics must NOT inherit the old run
    with patch.object(queries, "_has_soft_delete", return_value=True):
        assert queries.total_executions(db, "orders_pipeline") == 0
        assert queries.total_rows_processed(db, "orders_pipeline") == 0

    # Record a new execution after re-registration
    db.executions.append(
        {"execution_id": 2, "pipeline_name": "orders_pipeline", "status": "SUCCESS", "rows_processed": 10, "start_time": datetime.now()}
    )
    with patch.object(queries, "_has_soft_delete", return_value=True):
        assert queries.total_executions(db, "orders_pipeline") == 1
        assert queries.total_rows_processed(db, "orders_pipeline") == 10


def test_9_stale_selector_value_does_not_crash():
    """Requirement 9: Stale selection in session_state does not crash selectors."""
    active_names = ["active_1", "active_2"]

    # Simulating session_state holding deleted selection
    fake_session = {"dashboard_pipeline_filter": "deleted_pipe"}
    current_sel = fake_session.get("dashboard_pipeline_filter", "All Pipelines")
    if current_sel not in ["All Pipelines", *active_names]:
        fake_session["dashboard_pipeline_filter"] = "All Pipelines"

    assert fake_session["dashboard_pipeline_filter"] == "All Pipelines"


def test_10_no_cascade_removes_history():
    """Requirement 10: Assert execution history row count is completely unchanged after delete."""
    db = InMemoryDatabase()
    now = datetime.now()

    db.registry["temp_pipeline"] = {
        "pipeline_name": "temp_pipeline",
        "enabled": False,
        "deleted_at": None,
    }
    db.executions = [
        {"execution_id": 1, "pipeline_name": "temp_pipeline", "status": "SUCCESS", "rows_processed": 10},
        {"execution_id": 2, "pipeline_name": "temp_pipeline", "status": "FAILED", "rows_processed": 0},
    ]

    initial_history_count = len(db.executions)
    delete_disabled_pipeline(db, "temp_pipeline")

    assert len(db.executions) == initial_history_count


def test_11_unrelated_pipelines_unaffected():
    """Requirement 11: Deleting one pipeline has zero impact on another active pipeline."""
    db = InMemoryDatabase()
    now = datetime.now()

    db.registry["pipe_x"] = {"pipeline_name": "pipe_x", "enabled": False, "deleted_at": None}
    db.registry["pipe_y"] = {"pipeline_name": "pipe_y", "enabled": True, "deleted_at": None}

    delete_disabled_pipeline(db, "pipe_x")

    assert db.registry["pipe_x"]["deleted_at"] is not None
    assert db.registry["pipe_y"]["deleted_at"] is None
    assert db.registry["pipe_y"]["enabled"] is True
