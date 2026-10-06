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


def validate_sales_data(df: pd.DataFrame) -> tuple[pd.DataFrame, list[str]]:
    """
    Validate sales_data DataFrame for data quality issues.
    
    Checks:
        - No null values in required columns
        - Quantity > 0
        - Amount > 0
        - Valid sale_date format
    
    Args:
        df: DataFrame to validate.
    
    Returns:
        tuple: (validated_df, list of error messages)
               Empty error list means validation passed.
    
    Raises:
        ETLError: If validation cannot be performed.
    """
    errors = []
    
    # Check required columns exist
    required_columns = ['customer_name', 'product_name', 'quantity', 'amount', 'sale_date']
    missing_columns = [col for col in required_columns if col not in df.columns]
    if missing_columns:
        raise ETLError(f"Missing required columns: {missing_columns}")
    
    # Check for null values
    null_counts = df[required_columns].isnull().sum()
    for col, count in null_counts.items():
        if count > 0:
            errors.append(f"Found {count} null values in column '{col}'")
    
    # Validate quantity > 0
    invalid_qty = (df['quantity'] <= 0).sum()
    if invalid_qty > 0:
        errors.append(f"Found {invalid_qty} rows with quantity <= 0")
    
    # Validate amount > 0
    invalid_amount = (df['amount'] <= 0).sum()
    if invalid_amount > 0:
        errors.append(f"Found {invalid_amount} rows with amount <= 0")
    
    # Validate sale_date is datetime
    try:
        pd.to_datetime(df['sale_date'])
    except Exception as e:
        errors.append(f"Invalid date format in 'sale_date': {e}")
    
    if errors:
        logger.warning(f"Validation errors found: {'; '.join(errors)}")
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
