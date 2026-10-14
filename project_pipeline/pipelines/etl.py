"""
ETL (Extract, Transform, Load) module for data pipeline operations.

Provides reusable functions for:
    - Reading CSV files
    - Validating data quality
    - Transforming data into target format
    - Loading data into the database
"""

from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Callable

import pandas as pd

logger = logging.getLogger(__name__)


class ETLError(Exception):
    """Custom exception for ETL operation failures."""
    pass


def read_csv(file_path: str | Path) -> pd.DataFrame:
    """
    Read CSV file into a pandas DataFrame.
    
    Args:
        file_path: Path to the CSV file.
    
    Returns:
        pd.DataFrame: Loaded CSV data.
    
    Raises:
        ETLError: If file does not exist or cannot be read.
    """
    file_path = Path(file_path)
    
    if not file_path.exists():
        raise ETLError(f"CSV file not found: {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        logger.info(f"CSV loaded successfully: {file_path} ({len(df)} rows)")
        return df
    
    except Exception as e:
        raise ETLError(f"Failed to read CSV: {e}") from e


def read_api_data(api_client) -> pd.DataFrame:
    """
    Read data from an API client and return as a pandas DataFrame.

    Args:
        api_client: Instance of APIClient (from api.api_client).

    Returns:
        pd.DataFrame: DataFrame constructed from API JSON payload.

    Raises:
        ETLError: If fetching or conversion fails.
    """
    try:
        payload = api_client.fetch_data()
        df = api_client.get_dataframe(payload)
        logger.info("API data loaded into DataFrame (%d rows)", len(df))
        return df
    except Exception as e:
        raise ETLError(f"Failed to read API data: {e}") from e


def evaluate_data_quality(
    df: pd.DataFrame | list[dict],
    required_columns: list[str] | None = None,
    check_duplicates: bool = True,
) -> dict:
    """Return a compact quality summary, including a simple quality score."""
    frame = df.copy() if isinstance(df, pd.DataFrame) else pd.DataFrame(df)
    required = required_columns or [
        "customer_name",
        "product_name",
        "quantity",
        "amount",
        "sale_date",
    ]
    missing_columns = [col for col in required if col not in frame.columns]
    total_rows = len(frame)

    missing_values = 0
    if required and not missing_columns:
        missing_values = int(frame[required].isna().sum().sum())

    duplicate_rows = 0
    if check_duplicates and total_rows > 0:
        duplicate_rows = int(frame.duplicated(subset=required if not missing_columns else None).sum())

    invalid_rows = 0
    issues: list[str] = []
    if "quantity" in frame.columns:
        invalid_rows += int((frame["quantity"] <= 0).sum())
        if (frame["quantity"] <= 0).sum() > 0:
            issues.append(f"Found {(frame['quantity'] <= 0).sum()} rows with quantity <= 0")
    if "amount" in frame.columns:
        invalid_rows += int((frame["amount"] <= 0).sum())
        if (frame["amount"] <= 0).sum() > 0:
            issues.append(f"Found {(frame['amount'] <= 0).sum()} rows with amount <= 0")
    if "sale_date" in frame.columns:
        invalid_dates = pd.to_datetime(frame["sale_date"], errors="coerce").isna()
        invalid_rows += int(invalid_dates.sum())
        if invalid_dates.sum() > 0:
            issues.append(f"Found {int(invalid_dates.sum())} rows with invalid sale dates")

    if missing_columns:
        issues.append(f"Missing required columns: {missing_columns}")
    if duplicate_rows > 0:
        issues.append(f"Found {duplicate_rows} duplicate rows")
    if missing_values > 0:
        issues.append(f"Found {missing_values} missing values")

    if total_rows == 0:
        score = 100.0
    else:
        penalties = missing_values + duplicate_rows + invalid_rows
        score = max(0.0, round(100 * (1 - (penalties / total_rows)), 2))

    return {
        "total_rows": total_rows,
        "missing_values": missing_values,
        "duplicate_rows": duplicate_rows,
        "invalid_rows": invalid_rows,
        "missing_columns": missing_columns,
        "issues": issues,
        "score": score,
        "status": "PASS" if not issues else "FAIL",
    }


def validate_sales_data(
    df: pd.DataFrame,
    check_duplicates: bool = True,
) -> tuple[pd.DataFrame, list[str]]:
    """
    Validate sales_data DataFrame for data quality issues.

    Checks:
        - Required columns exist
        - No null values in required columns
        - Quantity > 0
        - Amount > 0
        - Valid sale_date format
        - Duplicate rows are reported for monitoring
    """
    errors: list[str] = []
    required_columns = ["customer_name", "product_name", "quantity", "amount", "sale_date"]
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ETLError(f"Missing required columns: {missing_columns}")

    quality = evaluate_data_quality(
        df,
        required_columns=required_columns,
        check_duplicates=check_duplicates,
    )
    if quality["missing_values"] > 0:
        errors.append(f"Found {quality['missing_values']} missing values")
    if check_duplicates and quality["duplicate_rows"] > 0:
        errors.append(f"Found {quality['duplicate_rows']} duplicate rows")
    if quality["invalid_rows"] > 0:
        for issue in quality["issues"]:
            if "Found" in issue and "rows" in issue:
                errors.append(issue)

    if errors:
        logger.warning("Validation errors found: %s", "; ".join(errors))
    else:
        logger.info("Data validation passed")

    return df, errors


def transform_sales_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Transform sales_data into target format and data types.
    
    Operations:
        - Trim whitespace from string columns
        - Standardize customer and product names (strip, title case)
        - Convert data types (quantity to int, amount to float, sale_date to date)
    
    Args:
        df: DataFrame to transform.
    
    Returns:
        pd.DataFrame: Transformed DataFrame.
    
    Raises:
        ETLError: If transformation fails.
    """
    try:
        # Create a copy to avoid modifying original
        df = df.copy()
        
        # Trim whitespace from string columns
        df['customer_name'] = df['customer_name'].str.strip()
        df['product_name'] = df['product_name'].str.strip()
        
        # Standardize names (title case for readability)
        df['customer_name'] = df['customer_name'].str.title()
        df['product_name'] = df['product_name'].str.title()
        
        # Convert data types
        df['quantity'] = df['quantity'].astype(int)
        df['amount'] = df['amount'].astype(float)
        df['sale_date'] = pd.to_datetime(df['sale_date']).dt.date
        
        logger.info(f"Data transformation completed: {len(df)} rows transformed")
        return df
    
    except Exception as e:
        raise ETLError(f"Transformation failed: {e}") from e


def load_sales_data(
    df: pd.DataFrame,
    db,
    progress_callback: Callable[[int], None] | None = None,
) -> int:
    """
    Load transformed data into the sales_data table.
    
    Args:
        df: Transformed DataFrame to load.
        db: Database object (instance of database.db.Database).
    
    Returns:
        int: Number of rows successfully loaded.
    
    Raises:
        ETLError: If load operation fails.
    """
    if df.empty:
        logger.warning("No data to load (empty DataFrame)")
        return 0
    
    try:
        rows_loaded = 0
        
        for _, row in df.iterrows():
            query = """
                INSERT INTO sales_data 
                (customer_name, product_name, quantity, amount, sale_date)
                VALUES (%s, %s, %s, %s, %s)
            """
            params = (
                row['customer_name'],
                row['product_name'],
                int(row['quantity']),
                float(row['amount']),
                row['sale_date']
            )
            
            db.execute(query, params, commit=False)
            rows_loaded += 1

            if progress_callback is not None:
                progress_callback(rows_loaded)
        
        # Commit all rows at once
        db.connection.commit()
        logger.info(f"Successfully loaded {rows_loaded} rows into sales_data")
        return rows_loaded
    
    except Exception as e:
        db.connection.rollback()
        raise ETLError(f"Load failed: {e}") from e


def load_generic_data(
    records: list[dict[str, Any]],
    db: Any,
    execution_id: int | None = None,
    pipeline_name: str = "generic_csv",
    progress_callback: Callable[[int], None] | None = None,
) -> int:
    """
    Load generic CSV row payloads into pipeline_generic_data table.
    Uses parameterized SQL without interpolating any user-provided column names.
    """
    if not records:
        logger.info("No generic data to load")
        return 0

    import json
    try:
        rows_loaded = 0
        for idx, row in enumerate(records, start=1):
            query = """
                INSERT INTO pipeline_generic_data
                (execution_id, pipeline_name, row_number, data)
                VALUES (%s, %s, %s, %s::jsonb)
            """
            params = (
                execution_id,
                pipeline_name,
                idx,
                json.dumps(row, default=str),
            )
            db.execute(query, params, commit=False)
            rows_loaded += 1
            if progress_callback is not None:
                progress_callback(rows_loaded)

        if getattr(db, "connection", None):
            db.connection.commit()
        logger.info("Successfully loaded %d rows into pipeline_generic_data", rows_loaded)
        return rows_loaded
    except Exception as e:
        if getattr(db, "connection", None):
            try:
                db.connection.rollback()
            except Exception:
                pass
        raise ETLError(f"Generic load failed: {e}") from e


def load_rejected_rows(
    rejected_records: list[dict[str, Any]],
    db: Any,
    execution_id: int | None = None,
    pipeline_name: str = "generic_csv",
) -> int:
    """
    Persist rejected row audit records into pipeline_rejected_rows table.
    """
    if not rejected_records:
        return 0

    import json
    try:
        count = 0
        for item in rejected_records:
            query = """
                INSERT INTO pipeline_rejected_rows
                (execution_id, pipeline_name, row_number, raw_data, rejection_reasons)
                VALUES (%s, %s, %s, %s::jsonb, %s::jsonb)
            """
            params = (
                execution_id,
                pipeline_name,
                item.get("row_number", count + 1),
                json.dumps(item.get("raw_data", {}), default=str),
                json.dumps(item.get("rejection_reasons", []), default=str),
            )
            db.execute(query, params, commit=False)
            count += 1
        if getattr(db, "connection", None):
            db.connection.commit()
        logger.info("Persisted %d rejected rows into pipeline_rejected_rows", count)
        return count
    except Exception as e:
        if getattr(db, "connection", None):
            try:
                db.connection.rollback()
            except Exception:
                pass
        logger.warning("Failed to persist rejected rows audit: %s", e)
        return 0
