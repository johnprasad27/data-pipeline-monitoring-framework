"""
SQL helper functions for monitoring dashboard.

Provides functions that execute read-only queries against the
`pipeline_execution` and `sales_data` tables using the existing
`Database` class.
"""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from database.db import Database, DatabaseError


def total_executions(db: Database) -> int:
    try:
        row = db.fetch_one("SELECT COUNT(*) AS total FROM pipeline_execution")
        return int(row.get("total", 0))
    except Exception as e:
        raise DatabaseError(f"Failed to fetch total executions: {e}") from e


def successful_executions(db: Database) -> int:
    try:
        row = db.fetch_one("SELECT COUNT(*) AS total FROM pipeline_execution WHERE status = 'SUCCESS'")
        return int(row.get("total", 0))
    except Exception as e:
        raise DatabaseError(f"Failed to fetch successful executions: {e}") from e


def failed_executions(db: Database) -> int:
    try:
        row = db.fetch_one("SELECT COUNT(*) AS total FROM pipeline_execution WHERE status = 'FAILED'")
        return int(row.get("total", 0))
    except Exception as e:
        raise DatabaseError(f"Failed to fetch failed executions: {e}") from e


def total_rows_processed(db: Database) -> int:
    try:
        row = db.fetch_one("SELECT COALESCE(SUM(rows_processed), 0) AS total FROM pipeline_execution")
        return int(row.get("total", 0))
    except Exception as e:
        raise DatabaseError(f"Failed to fetch total rows processed: {e}") from e


def latest_pipeline_execution(db: Database) -> Optional[Dict[str, Any]]:
    try:
        row = db.fetch_one("SELECT * FROM pipeline_execution ORDER BY execution_id DESC LIMIT 1")
        return row
    except Exception as e:
        raise DatabaseError(f"Failed to fetch latest pipeline execution: {e}") from e


def execution_history(db: Database, limit: Optional[int] = None) -> List[Dict[str, Any]]:
    try:
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
    """Return latest execution row per pipeline (one row per pipeline)."""
    try:
        results = db.fetch_all(
            """
            SELECT DISTINCT ON (pipeline_name)
                pipeline_name,
                status,
                rows_processed,
                COALESCE(execution_duration_seconds, CAST(EXTRACT(EPOCH FROM (end_time - start_time)) AS INTEGER)) AS execution_duration_seconds,
                retry_count,
                created_at,
                source_type,
                start_time,
                end_time
            FROM pipeline_execution
            ORDER BY pipeline_name, execution_id DESC
            """
        )
        return results
    except Exception as e:
        raise DatabaseError(f"Failed to fetch pipelines: {e}") from e


def get_pipeline_history(db: Database, pipeline_name: str) -> List[Dict[str, Any]]:
    """Return execution history for a pipeline ordered by most recent."""
    try:
        results = db.fetch_all(
            "SELECT * FROM pipeline_execution WHERE pipeline_name = %s ORDER BY execution_id DESC",
            (pipeline_name,),
        )
        return results
    except Exception as e:
        raise DatabaseError(f"Failed to fetch pipeline history: {e}") from e


def get_latest_execution(db: Database, pipeline_name: str) -> Optional[Dict[str, Any]]:
    try:
        row = db.fetch_one(
            "SELECT * FROM pipeline_execution WHERE pipeline_name = %s ORDER BY execution_id DESC LIMIT 1",
            (pipeline_name,),
        )
        return row
    except Exception as e:
        raise DatabaseError(f"Failed to fetch latest execution for {pipeline_name}: {e}") from e


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
