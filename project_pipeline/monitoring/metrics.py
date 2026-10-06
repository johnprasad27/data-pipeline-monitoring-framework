"""
Metrics aggregation helpers for the monitoring dashboard.

These functions call SQL helper functions from `queries.py` and
return simple Python types suitable for rendering in the dashboard.
"""

from __future__ import annotations

import logging
from typing import Dict, List

from database.db import Database, DatabaseError
from monitoring import queries

logger = logging.getLogger(__name__)


def gather_kpis(db: Database, pipeline_name: str | None = None) -> Dict[str, int]:
    """Return a dict of KPI values for the dashboard."""
    try:
        total = queries.total_executions(db, pipeline_name)
        success = queries.successful_executions(db, pipeline_name)
        failed = queries.failed_executions(db, pipeline_name)
        rows = queries.total_rows_processed(db, pipeline_name)
        return {
            "total_executions": total,
            "successful_executions": success,
            "failed_executions": failed,
            "total_rows_processed": rows,
        }
    except DatabaseError as e:
        logger.error("Error gathering KPIs: %s", e)
        raise


def get_latest_execution(db: Database, pipeline_name: str | None = None) -> Dict[str, any] | None:
    try:
        if pipeline_name:
            return queries.get_latest_execution(db, pipeline_name)
        return queries.latest_pipeline_execution(db)
    except DatabaseError as e:
        logger.error("Error fetching latest execution: %s", e)
        raise


def get_execution_history(db: Database, limit: int | None = 100, pipeline_name: str | None = None) -> List[Dict[str, any]]:
    try:
        return queries.execution_history(db, limit=limit, pipeline_name=pipeline_name)
    except DatabaseError as e:
        logger.error("Error fetching execution history: %s", e)
        raise


def get_pipeline_failures(db: Database, pipeline_name: str, limit: int | None = 20) -> List[Dict[str, any]]:
    try:
        return queries.get_pipeline_failures(db, pipeline_name, limit)
    except DatabaseError as e:
        logger.error("Error fetching pipeline failures: %s", e)
        raise


def get_rows_per_pipeline(db: Database) -> List[Dict[str, any]]:
    try:
        return queries.rows_per_pipeline(db)
    except DatabaseError as e:
        logger.error("Error fetching rows per pipeline: %s", e)
        raise
