"""Retry and recovery orchestration for scheduled pipeline executions."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import config
from database.db import Database

logger = logging.getLogger(__name__)


def run_with_recovery(
    pipeline_name: str,
    pipeline_runner: Callable[..., dict[str, Any]],
    *runner_args: Any,
) -> dict[str, Any]:
    """Run a pipeline and retry failures without stopping the scheduler."""
    last_result: dict[str, Any] = {
        "status": "FAILED",
        "rows_processed": 0,
        "error_message": "Pipeline did not run",
    }

    for retry_count in range(config.MAX_RETRIES + 1):
        if retry_count:
            logger.warning(
                "retry started: %s attempt %d/%d",
                pipeline_name,
                retry_count,
                config.MAX_RETRIES,
            )

        try:
            last_result = pipeline_runner(*runner_args, retry_count=retry_count)
        except Exception as exc:
            logger.exception("Unhandled pipeline runner error: %s", exc)
            last_result = {
                "status": "FAILED",
                "rows_processed": 0,
                "error_message": str(exc),
            }

        if last_result.get("status") == "SUCCESS":
            if retry_count:
                logger.info("retry completed: %s succeeded on attempt %d", pipeline_name, retry_count)
            return last_result

        execution_id = last_result.get("execution_id")
        if retry_count < config.MAX_RETRIES:
            _mark_retrying(execution_id, last_result)
            continue

        logger.error(
            "pipeline failed permanently: %s after %d retries: %s",
            pipeline_name,
            retry_count,
            last_result.get("error_message"),
        )

    return last_result


def _mark_retrying(execution_id: Any, result: dict[str, Any]) -> None:
    """Make the recovery transition visible before the next attempt starts."""
    if execution_id is None:
        return

    db = Database()
    try:
        db.connect()
        db.execute(
            "UPDATE pipeline_execution SET status = 'RETRYING', execution_log = "
            "CONCAT_WS(E'\\n', execution_log, %s) WHERE execution_id = %s",
            (
                f"retry started after failure: {result.get('error_message')}",
                execution_id,
            ),
        )
        db.notify_pipeline_status(
            pipeline_name=result.get("pipeline_name", "unknown_pipeline"),
            status="RETRYING",
            execution_id=execution_id,
            retry_count=result.get("retry_count", 0),
        )
    except Exception:
        logger.exception("database error while marking execution RETRYING")
    finally:
        db.close()