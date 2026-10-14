"""Retry and recovery orchestration for scheduled pipeline executions."""

from __future__ import annotations

import logging
from collections.abc import Callable
from typing import Any

import config
from database.db import Database
from pipelines.failure_handling import classify_failure

logger = logging.getLogger(__name__)


def _is_pipeline_deleted(pipeline_name: str) -> bool:
    """Check if pipeline is explicitly marked deleted in the registry."""
    try:
        db = Database()
        db.connect()
        try:
            row = db.fetch_one(
                "SELECT deleted_at FROM pipeline_registry WHERE pipeline_name = %s",
                (pipeline_name,),
            )
            if row and row.get("deleted_at") is not None:
                return True
            return False
        finally:
            db.close()
    except Exception:
        return False


def run_with_recovery(
    pipeline_name: str,
    pipeline_runner: Callable[..., dict[str, Any]],
    *runner_args: Any,
    max_retries: int | None = None,
    runner_kwargs: dict[str, Any] | None = None,
) -> dict[str, Any]:
    """Run a pipeline and retry failures without stopping the scheduler."""
    retry_limit = config.MAX_RETRIES if max_retries is None else max(0, int(max_retries))
    runner_kwargs = runner_kwargs or {}
    last_result: dict[str, Any] = {
        "status": "FAILED",
        "rows_processed": 0,
        "error_message": "Pipeline did not run",
    }
    earlier_failure_category: str | None = None

    for retry_count in range(retry_limit + 1):
        if retry_count:
            if _is_pipeline_deleted(pipeline_name):
                logger.info("retry skipped: pipeline %s was deleted", pipeline_name)
                last_result["status"] = "FAILED"
                last_result["error_message"] = "skipped: pipeline deleted"
                last_result["retryable"] = False
                return last_result
            logger.warning(
                "retry started: %s attempt %d/%d",
                pipeline_name,
                retry_count,
                retry_limit,
            )

        try:
            last_result = pipeline_runner(
                *runner_args, **runner_kwargs, retry_count=retry_count
            )
        except Exception as exc:
            logger.exception("Unhandled pipeline runner error: %s", exc)
            failure_type, retryable = classify_failure(exc)
            error_message = str(exc) or exc.__class__.__name__
            last_result = {
                "status": "FAILED",
                "rows_processed": 0,
                "error_message": error_message,
                "pipeline_name": pipeline_name,
                "retry_count": retry_count,
                "failure_type": failure_type,
                "failure_category": failure_type,
                "retryable": retryable,
            }

        if last_result.get("status") == "SUCCESS":
            if retry_count:
                logger.info("retry completed: %s succeeded on attempt %d", pipeline_name, retry_count)
                if earlier_failure_category:
                    recovered_label = f"Recovered ({earlier_failure_category})"
                    last_result["recovered_failure_category"] = earlier_failure_category
                    last_result["failure_category"] = recovered_label
                    last_result["failure_type"] = recovered_label
                    execution_id = last_result.get("execution_id")
                    if execution_id is not None:
                        _record_recovered_category(execution_id, recovered_label)
            return last_result

        failure_type, retryable = classify_failure(
            last_result.get("error_message") or last_result.get("failure_category") or last_result.get("failure_type"),
            default_retryable=bool(last_result.get("retryable", False)),
        )
        last_result["failure_type"] = failure_type
        last_result["failure_category"] = failure_type
        last_result["retryable"] = retryable
        earlier_failure_category = failure_type

        execution_id = last_result.get("execution_id")
        if last_result["retryable"] and retry_count < retry_limit:
            if _is_pipeline_deleted(pipeline_name):
                logger.info("recovery skipped: pipeline %s was deleted", pipeline_name)
                last_result["error_message"] = "skipped: pipeline deleted"
                last_result["retryable"] = False
                break
            _mark_retrying(execution_id, last_result)
            continue

        err_text = str(last_result.get("error_message") or "").lower()
        if any(tok in err_text for tok in ("data incomplete", "incomplete data", "incomplete extraction", "missing records")):
            last_result["failure_category"] = "DATA_INCOMPLETE"
            last_result["failure_type"] = "FAILED — DATA INCOMPLETE"
            last_result["status_display"] = "FAILED — DATA INCOMPLETE"

        logger.error(
            "pipeline failed permanently: %s after %d retries: %s",
            pipeline_name,
            retry_count,
            last_result.get("error_message"),
        )
        break

    return last_result


def _mark_retrying(execution_id: Any, result: dict[str, Any]) -> None:
    """Make the recovery transition visible before the next attempt starts."""
    if execution_id is None:
        return

    db = Database()
    try:
        db.connect()
        db.execute(
            "UPDATE pipeline_execution SET status = 'RETRYING', failure_type = COALESCE(%s, failure_type), failure_category = COALESCE(%s, failure_category), retryable = %s, execution_log = CONCAT_WS(E'\\n', execution_log, %s) WHERE execution_id = %s",
            (
                result.get("failure_type"),
                result.get("failure_category"),
                bool(result.get("retryable", True)),
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


def _record_recovered_category(execution_id: Any, recovered_label: str) -> None:
    """Store recovered failure label on a successfully retried execution row."""
    if execution_id is None:
        return
    db = Database()
    try:
        db.connect()
        db.execute(
            "UPDATE pipeline_execution SET failure_category = %s, failure_type = %s WHERE execution_id = %s",
            (recovered_label, recovered_label, execution_id),
        )
    except Exception:
        logger.exception("database error while recording recovered category")
    finally:
        db.close()