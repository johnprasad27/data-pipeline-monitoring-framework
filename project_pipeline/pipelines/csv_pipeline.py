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
from datetime import datetime
from pathlib import Path

import config
from database.db import Database, DatabaseError
from pipelines.etl import (
    ETLError,
    load_sales_data,
    read_csv,
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
        0,  # retry_count default
        error_message,
    )
    
    try:
        db.execute(query, params, commit=True)
        logger.info(f"Pipeline execution logged: {pipeline_name} | Status: {status}")
    except DatabaseError as e:
        logger.error(f"Failed to log pipeline execution: {e}")
        raise


def run_csv_pipeline(csv_file_path: str | Path) -> dict:
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
    pipeline_name = "csv_pipeline"
    source_type = "CSV"
    start_time = datetime.now()
    end_time = None
    rows_processed = 0
    error_message = None
    status = "FAILED"  # Default to FAILED; set to SUCCESS if all steps pass
    
    db = Database()
    
    try:
        # Connect to database
        db.connect()
        logger.info("CSV pipeline started")
        
        # Extract: Read CSV
        df = read_csv(csv_file_path)
        
        # Validate: Check data quality
        df, validation_errors = validate_sales_data(df)
        if validation_errors:
            raise ETLError(f"Validation failed: {'; '.join(validation_errors)}")
        
        # Transform: Apply transformations
        df = transform_sales_data(df)
        
        # Load: Insert into database
        rows_processed = load_sales_data(df, db)
        
        # Success
        status = "SUCCESS"
        logger.info(f"CSV pipeline completed successfully: {rows_processed} rows loaded")
    
    except ETLError as e:
        error_message = str(e)
        logger.error(f"ETL Error: {error_message}")
    
    except DatabaseError as e:
        error_message = str(e)
        logger.error(f"Database Error: {error_message}")
    
    except Exception as e:
        error_message = f"Unexpected error: {str(e)}"
        logger.exception(error_message)
    
    finally:
        # Log execution and close connection
        end_time = datetime.now()
        
        try:
            log_pipeline_execution(
                db,
                pipeline_name,
                source_type,
                status,
                rows_processed,
                start_time,
                end_time,
                error_message,
            )
        except DatabaseError as e:
            logger.error(f"Failed to log pipeline execution: {e}")
        
        db.close()
    
    return {
        "status": status,
        "rows_processed": rows_processed,
        "error_message": error_message,
        "start_time": start_time,
        "end_time": end_time,
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
