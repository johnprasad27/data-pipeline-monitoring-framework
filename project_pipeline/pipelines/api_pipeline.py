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
from datetime import datetime
from pathlib import Path

import config
from api.api_client import APIClient, APIError
from database.db import Database, DatabaseError
from pipelines.etl import (
    ETLError,
    load_sales_data,
    read_api_data,
    transform_sales_data,
    validate_sales_data,
)

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
) -> None:
    """
    Reuse logging function similar to csv_pipeline.log_pipeline_execution.
    Duplicate here to avoid circular imports while keeping behavior identical.
    """
    duration_seconds = int((end_time - start_time).total_seconds())
    query = """
        INSERT INTO pipeline_execution 
        (pipeline_name, source_type, status, rows_processed, start_time, 
         end_time, execution_duration_seconds, retry_count, error_message)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
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
    )
    db.execute(query, params, commit=True)


def run_api_pipeline(retry_count: int = 0) -> dict:
    """
    Execute the API ETL pipeline.

    Returns:
        dict: Execution result (status, rows_processed, error_message, start_time, end_time)
    """
    pipeline_name = "api_pipeline"
    source_type = "API"
    start_time = datetime.now()
    end_time = None
    rows_processed = 0
    error_message = None
    status = "FAILED"

    db = Database()
    client = APIClient()
    execution_id = None

    try:
        db.connect()
        execution_id = db.start_pipeline_execution(
            pipeline_name, source_type, start_time, retry_count
        )
        logger.info("API pipeline started")

        def update_progress(rows_loaded: int) -> None:
            db.update_pipeline_execution(
                execution_id, "RUNNING", rows_loaded, start_time
            )

        # Extract
        df = read_api_data(client)

        # Validate
        df, validation_errors = validate_sales_data(df)
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
        logger.error("Pipeline error: %s", error_message)

    except Exception as e:
        error_message = f"Unexpected error: {e}"
        logger.exception(error_message)

    finally:
        end_time = datetime.now()
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
    }


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    result = run_api_pipeline()
    print("API Pipeline Result:\n", result)
