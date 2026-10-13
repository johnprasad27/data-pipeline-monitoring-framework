"""
SQL helper functions for monitoring dashboard.

Provides functions that execute read-only queries against the
`pipeline_execution` and `sales_data` tables using the existing
`Database` class.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from database.db import Database, DatabaseError


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
        if pipeline_name:
            row = db.fetch_one(
                "SELECT COUNT(*) AS total FROM pipeline_execution WHERE pipeline_name = %s",
                (pipeline_name,),
            )
        else:
            row = db.fetch_one("SELECT COUNT(*) AS total FROM pipeline_execution")
        return int(row.get("total", 0))
    except Exception as e:
        raise DatabaseError(f"Failed to fetch total executions: {e}") from e


def successful_executions(db: Database, pipeline_name: str | None = None) -> int:
    try:
        if pipeline_name:
            row = db.fetch_one(
                "SELECT COUNT(*) AS total FROM pipeline_execution WHERE pipeline_name = %s AND status = 'SUCCESS'",
                (pipeline_name,),
            )
        else:
            row = db.fetch_one("SELECT COUNT(*) AS total FROM pipeline_execution WHERE status = 'SUCCESS'")
        return int(row.get("total", 0))
    except Exception as e:
        raise DatabaseError(f"Failed to fetch successful executions: {e}") from e


def failed_executions(db: Database, pipeline_name: str | None = None) -> int:
    try:
        if pipeline_name:
            row = db.fetch_one(
                "SELECT COUNT(*) AS total FROM pipeline_execution WHERE pipeline_name = %s AND status = 'FAILED'",
                (pipeline_name,),
            )
        else:
            row = db.fetch_one("SELECT COUNT(*) AS total FROM pipeline_execution WHERE status = 'FAILED'")
        return int(row.get("total", 0))
    except Exception as e:
        raise DatabaseError(f"Failed to fetch failed executions: {e}") from e


def total_rows_processed(db: Database, pipeline_name: str | None = None) -> int:
    try:
        if pipeline_name:
            row = db.fetch_one(
                "SELECT COALESCE(SUM(rows_processed), 0) AS total FROM pipeline_execution WHERE pipeline_name = %s",
                (pipeline_name,),
            )
        else:
            row = db.fetch_one("SELECT COALESCE(SUM(rows_processed), 0) AS total FROM pipeline_execution")
        return int(row.get("total", 0))
    except Exception as e:
        raise DatabaseError(f"Failed to fetch total rows processed: {e}") from e


def latest_pipeline_execution(db: Database, pipeline_name: str | None = None) -> Optional[Dict[str, Any]]:
    try:
        if pipeline_name:
            row = db.fetch_one(
                "SELECT * FROM pipeline_execution WHERE pipeline_name = %s ORDER BY execution_id DESC LIMIT 1",
                (pipeline_name,),
            )
        else:
            row = db.fetch_one("SELECT * FROM pipeline_execution ORDER BY execution_id DESC LIMIT 1")
        return row
    except Exception as e:
        raise DatabaseError(f"Failed to fetch latest pipeline execution: {e}") from e


def latest_execution_event_id(db: Database, pipeline_name: str | None = None) -> int:
    """Return the newest execution_id so the dashboard can detect fresh pipeline events quickly."""
    try:
        if pipeline_name:
            row = db.fetch_one(
                "SELECT COALESCE(MAX(execution_id), 0) AS latest_execution_id FROM pipeline_execution WHERE pipeline_name = %s",
                (pipeline_name,),
            )
        else:
            row = db.fetch_one("SELECT COALESCE(MAX(execution_id), 0) AS latest_execution_id FROM pipeline_execution")
        return int(row.get("latest_execution_id", 0)) if row else 0
    except Exception as e:
        raise DatabaseError(f"Failed to fetch latest execution event: {e}") from e


def execution_history(db: Database, limit: Optional[int] = None, pipeline_name: str | None = None) -> List[Dict[str, Any]]:
    try:
        if pipeline_name:
            if limit and isinstance(limit, int):
                results = db.fetch_all(
                    "SELECT * FROM pipeline_execution WHERE pipeline_name = %s ORDER BY execution_id DESC LIMIT %s",
                    (pipeline_name, limit),
                )
            else:
                results = db.fetch_all(
                    "SELECT * FROM pipeline_execution WHERE pipeline_name = %s ORDER BY execution_id DESC",
                    (pipeline_name,),
                )
        else:
            if limit and isinstance(limit, int):
                results = db.fetch_all("SELECT * FROM pipeline_execution ORDER BY execution_id DESC LIMIT %s", (limit,))
            else:
                results = db.fetch_all("SELECT * FROM pipeline_execution ORDER BY execution_id DESC")
        return results
    except Exception as e:
        raise DatabaseError(f"Failed to fetch execution history: {e}") from e


def rows_per_pipeline(db: Database) -> List[Dict[str, Any]]:
    try:
        results = db.fetch_all(
            "SELECT pipeline_name, COALESCE(SUM(rows_processed),0) AS total_rows "
            "FROM pipeline_execution GROUP BY pipeline_name ORDER BY total_rows DESC"
        )
        return results
    except Exception as e:
        raise DatabaseError(f"Failed to fetch rows per pipeline: {e}") from e


def get_all_pipelines(db: Database) -> List[Dict[str, Any]]:
    """Return registry entries and their latest execution, including unrun pipelines."""
    try:
        results = db.fetch_all(
            """
            WITH latest AS (
                SELECT DISTINCT ON (pipeline_name)
                    pipeline_name, status, rows_processed,
                    COALESCE(execution_duration_seconds,
                        EXTRACT(EPOCH FROM (end_time - start_time))) AS execution_duration_seconds,
                    retry_count, created_at, source_type, start_time, end_time,
                    COALESCE(failure_category, failure_type) AS failure_category
                FROM pipeline_execution
                ORDER BY pipeline_name, execution_id DESC
            )
            SELECT COALESCE(registry.pipeline_name, latest.pipeline_name) AS pipeline_name,
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
            FULL OUTER JOIN latest USING (pipeline_name)
            ORDER BY COALESCE(registry.pipeline_name, latest.pipeline_name)
            """
        )
        return results
    except Exception as e:
        raise DatabaseError(f"Failed to fetch pipelines: {e}") from e


def get_registered_pipelines(db: Database) -> List[Dict[str, Any]]:
    """Return configured pipelines, including disabled entries with no history."""
    try:
        return db.fetch_all(
            """
            SELECT pipeline_name, pipeline_type, source_config, interval_seconds,
                   max_retries, enabled, created_at, updated_at
            FROM pipeline_registry
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
        if limit and isinstance(limit, int):
            results = db.fetch_all(
                "SELECT * FROM pipeline_execution WHERE pipeline_name = %s AND status = 'FAILED' ORDER BY execution_id DESC LIMIT %s",
                (pipeline_name, limit),
            )
        else:
            results = db.fetch_all(
                "SELECT * FROM pipeline_execution WHERE pipeline_name = %s AND status = 'FAILED' ORDER BY execution_id DESC",
                (pipeline_name,),
            )
        return results
    except Exception as e:
        raise DatabaseError(f"Failed to fetch pipeline failures: {e}") from e


def get_latest_execution(db: Database, pipeline_name: str) -> Optional[Dict[str, Any]]:
    try:
        row = db.fetch_one(
            "SELECT * FROM pipeline_execution WHERE pipeline_name = %s ORDER BY execution_id DESC LIMIT 1",
            (pipeline_name,),
        )
        return row
    except Exception as e:
        raise DatabaseError(f"Failed to fetch latest execution for {pipeline_name}: {e}") from e


def get_latest_quality_execution(db: Database, pipeline_name: str) -> Optional[Dict[str, Any]]:
    """Return the latest execution that includes CSV quality metrics."""
    try:
        return db.fetch_one(
            "SELECT * FROM pipeline_execution WHERE pipeline_name = %s "
            "AND input_rows IS NOT NULL ORDER BY execution_id DESC LIMIT 1",
            (pipeline_name,),
        )
    except Exception as e:
        raise DatabaseError(f"Failed to fetch quality metrics for {pipeline_name}: {e}") from e


def get_pipeline_statistics(db: Database, pipeline_name: str) -> Dict[str, Any]:
    """Return aggregate statistics for a pipeline: total rows, avg duration, retries, current status."""
    try:
        row = db.fetch_one(
            "SELECT COALESCE(SUM(rows_processed),0) AS total_rows, COALESCE(AVG(COALESCE(execution_duration_seconds, EXTRACT(EPOCH FROM (end_time - start_time)))),0) AS avg_duration, COALESCE(SUM(retry_count),0) AS total_retries FROM pipeline_execution WHERE pipeline_name = %s",
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
