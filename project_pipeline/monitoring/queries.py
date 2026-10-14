"""
SQL helper functions for monitoring dashboard.

Provides functions that execute read-only queries against the
`pipeline_execution` and `sales_data` tables using the existing
`Database` class.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from database.db import Database, DatabaseError


def _has_soft_delete(db: Database) -> bool:
    cached = getattr(db, "_has_deleted_at_column", None)
    if cached is not None:
        return cached
    if type(db).__name__ in ("Mock", "MagicMock", "MockDB"):
        return True
    try:
        row = db.fetch_one(
            "SELECT 1 FROM information_schema.columns "
            "WHERE table_name = 'pipeline_registry' AND column_name = 'deleted_at'"
        )
        has_col = bool(row)
        try:
            setattr(db, "_has_deleted_at_column", has_col)
        except Exception:
            pass
        return has_col
    except Exception:
        return True


def _registry_deleted_filter(db: Database, prefix: str = "") -> str:
    if _has_soft_delete(db):
        return f"{prefix}deleted_at IS NULL"
    return "1=1"


def pipeline_option_name(option: str | None) -> str | None:
    """Translate a UI option label into the database pipeline_name value."""
    mapping = {
        "All Pipelines": None,
        "API Pipeline": "api_pipeline",
        "CSV Pipeline": "csv_pipeline",
    }
    if option in mapping:
        return mapping[option]
    if option is None:
        return None
    normalized = option.strip().lower().replace(" ", "_")
    if normalized.endswith("_pipeline"):
        return normalized
    return normalized


def total_executions(db: Database, pipeline_name: str | None = None) -> int:
    try:
        del_cond = _registry_deleted_filter(db, "pr.")
        if pipeline_name:
            row = db.fetch_one(
                f"""
                SELECT COUNT(*) AS total
                FROM pipeline_execution pe
                JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
                WHERE pe.pipeline_name = %s AND {del_cond} AND pe.start_time >= pr.created_at
                """,
                (pipeline_name,),
            )
        else:
            row = db.fetch_one(
                f"""
                SELECT COUNT(*) AS total
                FROM pipeline_execution pe
                JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
                WHERE {del_cond} AND pe.start_time >= pr.created_at
                """
            )
        return int(row.get("total", 0)) if row else 0
    except Exception as e:
        raise DatabaseError(f"Failed to fetch total executions: {e}") from e


def successful_executions(db: Database, pipeline_name: str | None = None) -> int:
    try:
        del_cond = _registry_deleted_filter(db, "pr.")
        if pipeline_name:
            row = db.fetch_one(
                f"""
                SELECT COUNT(*) AS total
                FROM pipeline_execution pe
                JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
                WHERE pe.pipeline_name = %s AND pe.status = 'SUCCESS'
                  AND {del_cond} AND pe.start_time >= pr.created_at
                """,
                (pipeline_name,),
            )
        else:
            row = db.fetch_one(
                f"""
                SELECT COUNT(*) AS total
                FROM pipeline_execution pe
                JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
                WHERE pe.status = 'SUCCESS' AND {del_cond} AND pe.start_time >= pr.created_at
                """
            )
        return int(row.get("total", 0)) if row else 0
    except Exception as e:
        raise DatabaseError(f"Failed to fetch successful executions: {e}") from e


def failed_executions(db: Database, pipeline_name: str | None = None) -> int:
    try:
        del_cond = _registry_deleted_filter(db, "pr.")
        if pipeline_name:
            row = db.fetch_one(
                f"""
                SELECT COUNT(*) AS total
                FROM pipeline_execution pe
                JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
                WHERE pe.pipeline_name = %s AND pe.status = 'FAILED'
                  AND {del_cond} AND pe.start_time >= pr.created_at
                """,
                (pipeline_name,),
            )
        else:
            row = db.fetch_one(
                f"""
                SELECT COUNT(*) AS total
                FROM pipeline_execution pe
                JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
                WHERE pe.status = 'FAILED' AND {del_cond} AND pe.start_time >= pr.created_at
                """
            )
        return int(row.get("total", 0)) if row else 0
    except Exception as e:
        raise DatabaseError(f"Failed to fetch failed executions: {e}") from e


def total_rows_processed(db: Database, pipeline_name: str | None = None) -> int:
    try:
        del_cond = _registry_deleted_filter(db, "pr.")
        if pipeline_name:
            row = db.fetch_one(
                f"""
                SELECT COALESCE(SUM(pe.rows_processed), 0) AS total
                FROM pipeline_execution pe
                JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
                WHERE pe.pipeline_name = %s AND {del_cond} AND pe.start_time >= pr.created_at
                """,
                (pipeline_name,),
            )
        else:
            row = db.fetch_one(
                f"""
                SELECT COALESCE(SUM(pe.rows_processed), 0) AS total
                FROM pipeline_execution pe
                JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
                WHERE {del_cond} AND pe.start_time >= pr.created_at
                """
            )
        return int(row.get("total", 0)) if row else 0
    except Exception as e:
        raise DatabaseError(f"Failed to fetch total rows processed: {e}") from e


def latest_pipeline_execution(db: Database, pipeline_name: str | None = None) -> Optional[Dict[str, Any]]:
    try:
        del_cond = _registry_deleted_filter(db, "pr.")
        if pipeline_name:
            row = db.fetch_one(
                f"""
                SELECT pe.* FROM pipeline_execution pe
                JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
                WHERE pe.pipeline_name = %s AND {del_cond} AND pe.start_time >= pr.created_at
                ORDER BY pe.execution_id DESC LIMIT 1
                """,
                (pipeline_name,),
            )
        else:
            row = db.fetch_one(
                f"""
                SELECT pe.* FROM pipeline_execution pe
                JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
                WHERE {del_cond} AND pe.start_time >= pr.created_at
                ORDER BY pe.execution_id DESC LIMIT 1
                """
            )
        return row
    except Exception as e:
        raise DatabaseError(f"Failed to fetch latest pipeline execution: {e}") from e


def latest_execution_event_id(db: Database, pipeline_name: str | None = None) -> int:
    """Return the newest execution_id so the dashboard can detect fresh pipeline events quickly."""
    try:
        del_cond = _registry_deleted_filter(db, "pr.")
        if pipeline_name:
            row = db.fetch_one(
                f"""
                SELECT COALESCE(MAX(pe.execution_id), 0) AS latest_execution_id
                FROM pipeline_execution pe
                JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
                WHERE pe.pipeline_name = %s AND {del_cond} AND pe.start_time >= pr.created_at
                """,
                (pipeline_name,),
            )
        else:
            row = db.fetch_one(
                f"""
                SELECT COALESCE(MAX(pe.execution_id), 0) AS latest_execution_id
                FROM pipeline_execution pe
                JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
                WHERE {del_cond} AND pe.start_time >= pr.created_at
                """
            )
        return int(row.get("latest_execution_id", 0)) if row else 0
    except Exception as e:
        raise DatabaseError(f"Failed to fetch latest execution event: {e}") from e


def execution_history(
    db: Database,
    limit: Optional[int] = None,
    pipeline_name: str | None = None,
    status: str | None = None,
    since: str | datetime | None = None,
) -> List[Dict[str, Any]]:
    try:
        conditions: list[str] = []
        params: list[Any] = []

        if pipeline_name:
            conditions.append("pipeline_name = %s")
            params.append(pipeline_name)

        if status and status.strip().upper() not in {"ALL", "ALL STATUSES"}:
            conditions.append("UPPER(status) = %s")
            params.append(status.strip().upper())

        if since:
            conditions.append("created_at >= %s")
            params.append(since)

        where_clause = f"WHERE {' AND '.join(conditions)}" if conditions else ""
        sql = f"SELECT * FROM pipeline_execution {where_clause} ORDER BY execution_id DESC"
        if limit and isinstance(limit, int):
            sql += f" LIMIT {int(limit)}"

        return db.fetch_all(sql, tuple(params) if params else None)
    except Exception as e:
        raise DatabaseError(f"Failed to fetch execution history: {e}") from e


def rows_per_pipeline(db: Database) -> List[Dict[str, Any]]:
    try:
        del_cond = _registry_deleted_filter(db, "pr.")
        results = db.fetch_all(
            f"""
            SELECT pe.pipeline_name, COALESCE(SUM(pe.rows_processed), 0) AS total_rows
            FROM pipeline_execution pe
            JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
            WHERE {del_cond} AND pe.start_time >= pr.created_at
            GROUP BY pe.pipeline_name
            ORDER BY total_rows DESC
            """
        )
        return results
    except Exception as e:
        raise DatabaseError(f"Failed to fetch rows per pipeline: {e}") from e


def get_all_pipelines(db: Database) -> List[Dict[str, Any]]:
    """Return registry entries and their latest execution, including unrun pipelines."""
    try:
        where_cond = f"WHERE {_registry_deleted_filter(db, 'registry.')}" if _has_soft_delete(db) else ""
        results = db.fetch_all(
            f"""
            WITH latest AS (
                SELECT DISTINCT ON (pipeline_name)
                    pipeline_name, status, rows_processed,
                    source_type, retry_count, created_at,
                    start_time, end_time,
                    COALESCE(execution_duration_seconds,
                        EXTRACT(EPOCH FROM (end_time - start_time))) AS execution_duration_seconds,
                    CASE
                        WHEN status = 'SUCCESS' AND retry_count > 0 AND COALESCE(failure_category, failure_type) NOT IN ('UNKNOWN', 'UNKNOWN_ERROR')
                            THEN COALESCE(failure_category, failure_type)
                        WHEN status = 'SUCCESS' THEN NULL
                        ELSE COALESCE(failure_category, failure_type)
                    END AS failure_category
                FROM pipeline_execution
                ORDER BY pipeline_name, execution_id DESC
            )
            SELECT registry.pipeline_name AS pipeline_name,
                   COALESCE(registry.pipeline_type, latest.source_type) AS source_type,
                   CASE WHEN registry.enabled IS FALSE THEN 'DISABLED'
                        ELSE COALESCE(latest.status, 'UNKNOWN') END AS status,
                   COALESCE(latest.rows_processed, 0) AS rows_processed,
                   latest.execution_duration_seconds,
                   COALESCE(latest.retry_count, 0) AS retry_count,
                   latest.created_at,
                   latest.start_time,
                   latest.end_time,
                   latest.failure_category,
                   registry.enabled,
                   registry.created_at AS registered_at
            FROM pipeline_registry AS registry
            LEFT JOIN latest ON latest.pipeline_name = registry.pipeline_name
                            AND latest.start_time >= registry.created_at
            {where_cond}
            ORDER BY registry.pipeline_name
            """
        )
        return results
    except Exception as e:
        raise DatabaseError(f"Failed to fetch pipelines: {e}") from e


def get_registered_pipelines(db: Database) -> List[Dict[str, Any]]:
    """Return configured pipelines, including disabled entries with no history."""
    try:
        where_cond = f"WHERE {_registry_deleted_filter(db)}" if _has_soft_delete(db) else ""
        return db.fetch_all(
            f"""
            SELECT pipeline_name, pipeline_type, source_config, interval_seconds,
                   max_retries, enabled, created_at, updated_at
            FROM pipeline_registry
            {where_cond}
            ORDER BY pipeline_name
            """
        )
    except Exception as e:
        raise DatabaseError(f"Failed to fetch registered pipelines: {e}") from e


def get_pipeline_history(db: Database, pipeline_name: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Return execution history for a single pipeline ordered by newest first."""
    return execution_history(db, limit=limit, pipeline_name=pipeline_name)


def get_pipeline_failures(db: Database, pipeline_name: str, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    """Return failed executions for a pipeline, newest first."""
    try:
        del_cond = _registry_deleted_filter(db, "pr.")
        limit_clause = f" LIMIT {int(limit)}" if limit and isinstance(limit, int) else ""
        results = db.fetch_all(
            f"""
            SELECT pe.* FROM pipeline_execution pe
            JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
            WHERE pe.pipeline_name = %s AND pe.status = 'FAILED'
              AND {del_cond} AND pe.start_time >= pr.created_at
            ORDER BY pe.execution_id DESC{limit_clause}
            """,
            (pipeline_name,),
        )
        return results
    except Exception as e:
        raise DatabaseError(f"Failed to fetch pipeline failures: {e}") from e


def get_latest_execution(db: Database, pipeline_name: str) -> Optional[Dict[str, Any]]:
    try:
        del_cond = _registry_deleted_filter(db, "pr.")
        row = db.fetch_one(
            f"""
            SELECT pe.* FROM pipeline_execution pe
            JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
            WHERE pe.pipeline_name = %s AND {del_cond} AND pe.start_time >= pr.created_at
            ORDER BY pe.execution_id DESC LIMIT 1
            """,
            (pipeline_name,),
        )
        return row
    except Exception as e:
        raise DatabaseError(f"Failed to fetch latest execution for {pipeline_name}: {e}") from e


def get_latest_quality_execution(db: Database, pipeline_name: str) -> Optional[Dict[str, Any]]:
    """Return the latest execution that includes CSV quality metrics."""
    try:
        del_cond = _registry_deleted_filter(db, "pr.")
        return db.fetch_one(
            f"""
            SELECT pe.* FROM pipeline_execution pe
            JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
            WHERE pe.pipeline_name = %s AND pe.input_rows IS NOT NULL
              AND {del_cond} AND pe.start_time >= pr.created_at
            ORDER BY pe.execution_id DESC LIMIT 1
            """,
            (pipeline_name,),
        )
    except Exception as e:
        raise DatabaseError(f"Failed to fetch quality metrics for {pipeline_name}: {e}") from e


def get_pipeline_statistics(db: Database, pipeline_name: str) -> Dict[str, Any]:
    """Return aggregate statistics for a pipeline: total rows, avg duration, retries, current status."""
    try:
        del_cond = _registry_deleted_filter(db, "pr.")
        row = db.fetch_one(
            f"""
            SELECT COALESCE(SUM(pe.rows_processed),0) AS total_rows,
                   COALESCE(AVG(COALESCE(pe.execution_duration_seconds, EXTRACT(EPOCH FROM (pe.end_time - pe.start_time)))),0) AS avg_duration,
                   COALESCE(SUM(pe.retry_count),0) AS total_retries
            FROM pipeline_execution pe
            JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
            WHERE pe.pipeline_name = %s AND {del_cond} AND pe.start_time >= pr.created_at
            """,
            (pipeline_name,),
        )
        latest = get_latest_execution(db, pipeline_name)
        stats = {
            "total_rows": int(row.get("total_rows", 0)) if row else 0,
            "avg_duration": float(row.get("avg_duration", 0)) if row else 0,
            "total_retries": int(row.get("total_retries", 0)) if row else 0,
            "status": latest.get("status") if latest else "UNKNOWN",
        }
        return stats
    except Exception as e:
        raise DatabaseError(f"Failed to fetch pipeline statistics: {e}") from e


def get_task_instances(
    db: Database,
    pipeline_name: Optional[str] = None,
    since: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return task-stage counts grouped by pipeline_name + task_name + status.

    Each row contains: pipeline_name, task_name, status, count.
    Optionally filter by pipeline_name and/or a `since` timestamp string.
    """
    try:
        conditions: list[str] = [
            _registry_deleted_filter(db, "pr."),
            "pt.started_at >= pr.created_at",
        ]
        params: list[Any] = []
        if pipeline_name:
            conditions.append("pt.pipeline_name = %s")
            params.append(pipeline_name)
        if since:
            conditions.append("pt.created_at >= %s::timestamp")
            params.append(since)
        where = "WHERE " + " AND ".join(conditions)
        results = db.fetch_all(
            f"""
            SELECT pt.pipeline_name, pt.task_name, pt.status, COUNT(*) AS count
            FROM pipeline_task_execution pt
            JOIN pipeline_registry pr ON pt.pipeline_name = pr.pipeline_name
            {where}
            GROUP BY pt.pipeline_name, pt.task_name, pt.status
            ORDER BY pt.pipeline_name, pt.task_name, pt.status
            """,
            tuple(params) if params else None,
        )
        return results
    except Exception as e:
        raise DatabaseError(f"Failed to fetch task instances: {e}") from e


def get_pipeline_run_counts(
    db: Database,
    since: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """Return per-pipeline status counts for the overview page.

    Each row contains: pipeline_name, status, count.
    """
    try:
        del_cond = _registry_deleted_filter(db, "pr.")
        since_clause = "AND pe.created_at >= %s::timestamp" if since else ""
        params = (since,) if since else None
        results = db.fetch_all(
            f"""
            SELECT pe.pipeline_name, pe.status, COUNT(*) AS count
            FROM pipeline_execution pe
            JOIN pipeline_registry pr ON pe.pipeline_name = pr.pipeline_name
            WHERE {del_cond} AND pe.start_time >= pr.created_at
            {since_clause}
            GROUP BY pe.pipeline_name, pe.status
            ORDER BY pe.pipeline_name, pe.status
            """,
            params,
        )
        return results
    except Exception as e:
        raise DatabaseError(f"Failed to fetch pipeline run counts: {e}") from e
