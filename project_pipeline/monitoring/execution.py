"""Execution-history wrapper for configured pipeline handlers."""

from __future__ import annotations

import logging
from collections.abc import Callable
from datetime import datetime
from typing import Any

from database.db import Database
from pipelines.failure_handling import classify_failure

logger = logging.getLogger(__name__)


def execution_history_managed(
    runner: Callable[..., Any],
) -> Callable[..., Any]:
    """Mark a legacy handler that already writes its own execution history."""
    setattr(runner, "records_execution_history", True)
    return runner


def monitor_pipeline_runner(
    pipeline_runner: Callable[..., dict[str, Any]],
    source_type: str,
) -> Callable[..., dict[str, Any]]:
    """Persist one execution row around each attempt of a configured handler."""

    def run(
        *,
        pipeline_name: str,
        source_config: dict[str, Any],
        retry_count: int = 0,
    ) -> dict[str, Any]:
        start_time = datetime.now()
        db = Database()
        try:
            db.connect()
            execution_id = db.start_pipeline_execution(
                pipeline_name, source_type, start_time, retry_count
            )
            try:
                result = pipeline_runner(
                    pipeline_name=pipeline_name,
                    source_config=source_config,
                    retry_count=retry_count,
                )
                if not isinstance(result, dict):
                    raise TypeError("Pipeline handler must return a result mapping.")
            except Exception as exc:
                logger.exception("Configured pipeline handler failed: %s", pipeline_name)
                failure_category, retryable = classify_failure(exc)
                result = {
                    "status": "FAILED",
                    "rows_processed": 0,
                    "error_message": str(exc) or exc.__class__.__name__,
                    "failure_category": failure_category,
                    "retryable": retryable,
                }

            status = str(result.get("status") or "FAILED").upper()
            error_message = result.get("error_message")
            if status not in {"SUCCESS", "FAILED"}:
                status = "FAILED"
                error_message = error_message or (
                    "Pipeline handler returned an unsupported execution status."
                )
            if status == "FAILED" and not error_message:
                error_message = "Pipeline handler returned FAILED without an error message."

            try:
                rows_processed = int(result.get("rows_processed") or 0)
            except (TypeError, ValueError):
                rows_processed = 0
                status = "FAILED"
                error_message = "Pipeline handler returned an invalid rows_processed value."
            if rows_processed < 0:
                rows_processed = 0
                status = "FAILED"
                error_message = "Pipeline handler returned a negative rows_processed value."

            failure_category: str | None = None
            retryable: bool | None = None
            if status == "FAILED":
                failure_category, retryable = classify_failure(
                    error_message or result.get("failure_category"),
                    default_retryable=bool(result.get("retryable", False)),
                )

            end_time = datetime.now()
            db.update_pipeline_execution(
                execution_id,
                status,
                rows_processed,
                start_time,
                end_time=end_time,
                error_message=error_message,
                retry_count=retry_count,
                execution_log=result.get("execution_log")
                or (
                    "pipeline completed successfully"
                    if status == "SUCCESS"
                    else f"pipeline failed: {error_message}"
                ),
                pipeline_name=pipeline_name,
                failure_type=failure_category,
                failure_category=failure_category,
                retryable=retryable,
            )
            return {
                **result,
                "pipeline_name": pipeline_name,
                "status": status,
                "rows_processed": rows_processed,
                "error_message": error_message,
                "execution_id": execution_id,
                "retry_count": retry_count,
                "failure_type": failure_category,
                "failure_category": failure_category,
                "retryable": retryable,
            }
        finally:
            db.close()

    return run
