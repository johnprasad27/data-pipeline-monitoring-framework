"""
CSV ETL Pipeline for the Data Pipeline Monitoring and Recovery Framework.

Orchestrates the complete Extract-Transform-Load process for CSV files:
    1. Read CSV data
    2. Validate data quality
    3. Transform data
    4. Load into PostgreSQL
    5. Log execution to pipeline_execution table
"""

from __future__ import annotations

import logging
from datetime import datetime, timedelta
from pathlib import Path

import config
from database.db import Database, DatabaseError
from pipelines.etl import (
    ETLError,
    load_sales_data,
    read_csv,
    transform_sales_data,
)
from pipelines.csv_quality import finalize_csv_quality, validate_csv_data
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
    Log pipeline execution metadata to pipeline_execution table.
    
    Args:
        db: Database object.
        pipeline_name: Name of the pipeline (e.g., 'csv_pipeline').
        source_type: Source type ('CSV' or 'API').
        status: Execution status ('SUCCESS', 'FAILED', 'RUNNING').
        rows_processed: Number of rows processed.
        start_time: Execution start timestamp.
        end_time: Execution end timestamp.
        error_message: Error description (if applicable).
    
    Raises:
        DatabaseError: If logging fails.
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
        0,  # retry_count default
        error_message,
        failure_category,
        failure_category,
        retryable,
    )
    
    try:
        db.execute(query, params, commit=True)
        logger.info(f"Pipeline execution logged: {pipeline_name} | Status: {status}")
    except DatabaseError as e:
        logger.error(f"Failed to log pipeline execution: {e}")
        raise


@execution_history_managed
def run_csv_pipeline(
    csv_file_path: str | Path | None = None,
    retry_count: int = 0,
    *,
    pipeline_name: str = "csv_pipeline",
    source_config: dict | None = None,
) -> dict:
    """
    Execute the complete CSV ETL pipeline.
    
    Process:
        1. Connect to database
        2. Read CSV file
        3. Validate data quality
        4. Transform data
        5. Load into database
        6. Log execution
    
    Args:
        csv_file_path: Path to the CSV file to process.
    
    Returns:
        dict: Pipeline execution result with keys:
            - status: 'SUCCESS' or 'FAILED'
            - rows_processed: Number of rows loaded
            - error_message: Error details (if failed)
            - start_time: Execution start time
            - end_time: Execution end time
    
    Raises:
        None (errors are caught and returned in result dict).
    """
    source_type = "CSV"
    start_time = datetime.now()
    end_time = None
    rows_processed = 0
    error_message = None
    status = "FAILED"  # Default to FAILED; set to SUCCESS if all steps pass
    failure_type = "UNKNOWN"
    retryable = False
    data_quality_score = 100.0
    freshness_status = "STALE"
    health_score = 0.0
    quality_report = {
        "input_rows": 0,
        "valid_rows": 0,
        "rejected_rows": 0,
        "loaded_rows": 0,
        "missing_column_count": 0,
        "null_count": 0,
        "duplicate_count": 0,
        "datatype_error_count": 0,
        "business_rule_error_count": 0,
        "data_quality_status": "FAIL",
        "reasons": [],
    }
    
    db = Database()
    execution_id = None
    
    try:
        # Connect to database
        db.connect()
        execution_id = db.start_pipeline_execution(
            pipeline_name, source_type, start_time, retry_count
        )
        logger.info("CSV pipeline started")

        # Extract: Read CSV
        configured_path = (source_config or {}).get("file_path")
        csv_path = Path(configured_path or csv_file_path or config.CSV_PIPELINE_PATH)
        if not csv_path.is_absolute():
            csv_path = config.BASE_DIR / csv_path
        csv_path = csv_path.resolve()
        data_dir = (config.BASE_DIR / "data").resolve()
        if not csv_path.is_relative_to(data_dir):
            raise ETLError("Invalid CSV configuration: source must be inside the project's data directory")
        source_frame = read_csv(csv_path)
        df, quality_report = validate_csv_data(
            source_frame,
            (source_config or {}).get("quality_config"),
        )
        data_quality_score = (
            round(quality_report["valid_rows"] * 100 / quality_report["input_rows"], 2)
            if quality_report["input_rows"]
            else 100.0
        )
        if quality_report["data_quality_status"] == "FAIL":
            raise ETLError("; ".join(quality_report["reasons"]) or "CSV schema validation failed")
        
        # Transform: Apply transformations
        df = transform_sales_data(df)
        
        # Load in one transaction so loaded-row counts match committed rows.
        rows_processed = load_sales_data(df, db)
        if quality_report["reasons"] and not error_message:
            logger.warning("CSV rows rejected: %s", "; ".join(quality_report["reasons"]))
        status = "SUCCESS"
        logger.info("CSV pipeline completed: %s rows loaded", rows_processed)
    
    except ETLError as e:
        error_message = str(e)
        failure_type, retryable = classify_failure(e)
        logger.error(f"ETL Error: {error_message} | failure_type={failure_type} retryable={retryable}")
    
    except DatabaseError as e:
        error_message = str(e)
        failure_type, retryable = classify_failure(e)
        logger.error(f"Database Error: {error_message} | failure_type={failure_type} retryable={retryable}")
    
    except Exception as e:
        error_message = f"Unexpected error: {str(e)}"
        failure_type, retryable = classify_failure(e)
        logger.exception(error_message)
    
    finally:
        quality_report, reconciliation_error = finalize_csv_quality(
            quality_report, rows_processed
        )
        if reconciliation_error:
            status = "FAILED"
            error_message = "; ".join(
                part for part in (error_message, reconciliation_error) if part
            )
            failure_type, retryable = classify_failure(error_message)

        # Finalize the row created when the pipeline started.
        end_time = datetime.now()
        freshness_status = get_freshness_status(end_time if status == "SUCCESS" else (end_time - timedelta(seconds=1)))
        health_score = calculate_pipeline_health(status, freshness_status, data_quality_score, retry_count)
        
        if execution_id is not None:
            try:
                quality_summary = (
                    "CSV data quality: "
                    f"input={quality_report['input_rows']}, "
                    f"valid={quality_report['valid_rows']}, "
                    f"rejected={quality_report['rejected_rows']}, "
                    f"loaded={quality_report['loaded_rows']}, "
                    f"missing_columns={quality_report['missing_column_count']}, "
                    f"nulls={quality_report['null_count']}, "
                    f"duplicates={quality_report['duplicate_count']}, "
                    f"datatype_errors={quality_report['datatype_error_count']}, "
                    f"business_rule_errors={quality_report['business_rule_error_count']}, "
                    f"status={quality_report['data_quality_status']}"
                )
                if quality_report["reasons"]:
                    quality_summary += "; reasons=" + "; ".join(quality_report["reasons"])
                if error_message:
                    quality_summary += f"; failure={error_message}"

                db.update_pipeline_execution(
                    execution_id,
                    status,
                    rows_processed,
                    start_time,
                    end_time,
                    error_message,
                    retry_count,
                    quality_summary,
                    pipeline_name=pipeline_name,
                    failure_type=failure_type if status == "FAILED" else None,
                    failure_category=failure_type if status == "FAILED" else None,
                    retryable=retryable if status == "FAILED" else None,
                    data_quality_score=data_quality_score,
                    freshness_status=freshness_status,
                    health_score=health_score,
                    input_rows=quality_report["input_rows"],
                    valid_rows=quality_report["valid_rows"],
                    rejected_rows=quality_report["rejected_rows"],
                    loaded_rows=quality_report["loaded_rows"],
                    missing_column_count=quality_report["missing_column_count"],
                    null_count=quality_report["null_count"],
                    duplicate_count=quality_report["duplicate_count"],
                    datatype_error_count=quality_report["datatype_error_count"],
                    business_rule_error_count=quality_report["business_rule_error_count"],
                    data_quality_status=quality_report["data_quality_status"],
                )
            except DatabaseError as e:
                logger.error(f"Failed to log pipeline execution: {e}")
        
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
        **quality_report,
    }


if __name__ == "__main__":
    # Configure logging for standalone execution
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    )
    
    # Run pipeline with sample CSV
    csv_path = Path("data/csv/sales.csv")
    result = run_csv_pipeline(csv_path)
    
    # Print results
    print("\n" + "=" * 60)
    print("CSV Pipeline Execution Result")
    print("=" * 60)
    print(f"Status: {result['status']}")
    print(f"Rows Processed: {result['rows_processed']}")
    if result['error_message']:
        print(f"Error: {result['error_message']}")
    print(f"Duration: {(result['end_time'] - result['start_time']).total_seconds():.2f} seconds")
    print("=" * 60)
