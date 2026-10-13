"""
API ETL pipeline that mirrors the CSV pipeline architecture.

Flow:
    - Connect to DB
    - Fetch data via APIClient
    - Validate -> Transform -> Load using pipelines.etl helpers
    - Log execution to pipeline_execution

The module returns a result dict similar to csv_pipeline.run_csv_pipeline().
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

import config
from api.api_client import APIClient, APIError
from database.db import Database, DatabaseError
from pipelines.etl import (
    ETLError,
    evaluate_data_quality,
    load_sales_data,
    read_api_data,
    transform_sales_data,
    validate_sales_data,
)
from monitoring.freshness import calculate_pipeline_health, get_freshness_status
from monitoring.execution import execution_history_managed
from pipelines.failure_handling import classify_failure

logger = logging.getLogger(__name__)


def log_pipeline_execution(
    db: Database,
    pipeline_name: str,
    source_type: str,
    status: str,
    rows_processed: int,
    start_time: datetime,
    end_time: datetime,
    error_message: str | None = None,
    failure_category: str | None = None,
    retryable: bool | None = None,
) -> None:
    """
    Reuse logging function similar to csv_pipeline.log_pipeline_execution.
    Duplicate here to avoid circular imports while keeping behavior identical.
    """
    if status == "FAILED":
        classified_category, classified_retryable = classify_failure(error_message)
        failure_category = failure_category or classified_category
        if retryable is None:
            retryable = classified_retryable
    duration_seconds = int((end_time - start_time).total_seconds())
    query = """
        INSERT INTO pipeline_execution 
        (pipeline_name, source_type, status, rows_processed, start_time, 
         end_time, execution_duration_seconds, retry_count, error_message,
         failure_type, failure_category, retryable)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """
    params = (
        pipeline_name,
        source_type,
        status,
        rows_processed,
        start_time,
        end_time,
        duration_seconds,
        0,
        error_message,
        failure_category,
        failure_category,
        retryable,
    )
    db.execute(query, params, commit=True)


@execution_history_managed
def run_api_pipeline(
    retry_count: int = 0,
    *,
    pipeline_name: str = "api_pipeline",
    source_config: dict | None = None,
) -> dict:
    """
    Execute the API ETL pipeline.

    Returns:
        dict: Execution result (status, rows_processed, error_message, start_time, end_time)
    """
    source_type = "API"
    start_time = datetime.now()
    end_time = None
    rows_processed = 0
    error_message = None
    status = "FAILED"
    failure_type = "UNKNOWN"
    retryable = False
    data_quality_score = 100.0
    freshness_status = "STALE"
    health_score = 0.0

    db = Database()
    execution_id = None

    try:
        db.connect()
        execution_id = db.start_pipeline_execution(
            pipeline_name, source_type, start_time, retry_count
        )
        logger.info("API pipeline started")

        api_url = (source_config or {}).get("url") or config.API_BASE_URL
        client = APIClient(base_url=api_url)

        def update_progress(rows_loaded: int) -> None:
            db.update_pipeline_execution(
                execution_id, "RUNNING", rows_loaded, start_time
            )

        # Extract
        df = read_api_data(client)

        quality_report = evaluate_data_quality(df, check_duplicates=False)
        data_quality_score = quality_report["score"]

        # Validate
        df, validation_errors = validate_sales_data(df, check_duplicates=False)
        if validation_errors:
            raise ETLError(f"Validation failed: {'; '.join(validation_errors)}")

        # Transform
        df = transform_sales_data(df)

        # Load
        rows_processed = load_sales_data(df, db, update_progress)

        status = "SUCCESS"
        logger.info("API pipeline completed successfully")

    except (APIError, ETLError, DatabaseError) as e:
        error_message = str(e)
        failure_type, retryable = classify_failure(e)
        logger.error("Pipeline error: %s | failure_type=%s retryable=%s", error_message, failure_type, retryable)

    except Exception as e:
        error_message = f"Unexpected error: {e}"
        failure_type, retryable = classify_failure(e)
        logger.exception(error_message)

    finally:
        end_time = datetime.now()
        freshness_status = get_freshness_status(end_time if status == "SUCCESS" else (end_time - timedelta(seconds=1)))
        health_score = calculate_pipeline_health(status, freshness_status, data_quality_score, retry_count)
        if execution_id is not None:
            try:
                db.update_pipeline_execution(
                    execution_id,
                    status,
                    rows_processed,
                    start_time,
                    end_time,
                    error_message,
                    retry_count,
                    "pipeline completed successfully"
                    if status == "SUCCESS"
                    else f"pipeline failed: {error_message}",
                    pipeline_name=pipeline_name,
                    failure_type=failure_type if status == "FAILED" else None,
                    failure_category=failure_type if status == "FAILED" else None,
                    retryable=retryable if status == "FAILED" else None,
                    data_quality_score=data_quality_score,
                    freshness_status=freshness_status,
                    health_score=health_score,
                )
            except Exception as e:
                logger.error("Failed to log pipeline execution: %s", e)

        db.close()

    return {
        "pipeline_name": pipeline_name,
        "status": status,
        "rows_processed": rows_processed,
        "error_message": error_message,
        "start_time": start_time,
        "end_time": end_time,
        "execution_id": execution_id,
        "retry_count": retry_count,
        "failure_type": failure_type,
        "failure_category": failure_type,
        "retryable": retryable,
        "data_quality_score": data_quality_score,
        "freshness_status": freshness_status,
        "health_score": health_score,
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    result = run_api_pipeline()
    print("API Pipeline Result:\n", result)
