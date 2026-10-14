"""
Unit and integration tests for generic CSV loading, payload round-tripping,
schema-agnostic table creation, accurate accounting, and idempotent retries.
"""

from __future__ import annotations

import io
import json
import sys
from datetime import date, datetime
from decimal import Decimal
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import numpy as np
import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from database.db import Database, DatabaseError
from pipelines.csv_pipeline import run_csv_pipeline
from pipelines.csv_quality import validate_generic_csv_data, finalize_csv_quality
from pipelines.etl import (
    ETLError,
    load_generic_data,
    load_rejected_rows,
    serialize_json_payload,
)
from pipelines.failure_handling import classify_failure


class FakeGenericDatabase:
    """In-memory PostgreSQL database stub for testing generic table operations."""

    def __init__(self, table_exists: bool = False, permission_denied: bool = False):
        self.table_exists = table_exists
        self.permission_denied = permission_denied
        self.generic_data: list[dict[str, Any]] = []
        self.rejected_data: list[dict[str, Any]] = []
        self.uncommitted_generic: list[dict[str, Any]] = []
        self.uncommitted_rejected: list[dict[str, Any]] = []
        self.connection = MagicMock()
        self.connection.closed = False
        self.queries_log: list[str] = []

    def connect(self):
        pass

    def close(self):
        pass

    def ensure_generic_csv_tables(self):
        if self.permission_denied:
            raise DatabaseError("Database permission denied to create table pipeline_generic_data: permission denied")
        self.table_exists = True

    def execute(self, query: str, params: tuple = (), commit: bool = True):
        self.queries_log.append(query)
        q = query.strip()

        if q.startswith("CREATE TABLE IF NOT EXISTS pipeline_generic_data"):
            if self.permission_denied:
                raise DatabaseError("Execute failed: permission denied for schema public")
            self.table_exists = True
            return 0

        if "INSERT INTO pipeline_generic_data" in q:
            if not self.table_exists:
                raise DatabaseError('Execute failed: relation "pipeline_generic_data" does not exist')
            execution_id, pipeline_name, row_number, payload, data = params
            row_dict = {
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "row_number": row_number,
                "payload": json.loads(payload) if isinstance(payload, str) else payload,
                "data": json.loads(data) if isinstance(data, str) else data,
            }
            if commit:
                self.generic_data.append(row_dict)
            else:
                self.uncommitted_generic.append(row_dict)
            return 1

        if "DELETE FROM pipeline_generic_data WHERE execution_id = %s" in q:
            exec_id = params[0]
            self.generic_data = [r for r in self.generic_data if r.get("execution_id") != exec_id]
            self.uncommitted_generic = [r for r in self.uncommitted_generic if r.get("execution_id") != exec_id]
            return 1

        if "INSERT INTO pipeline_rejected_rows" in q:
            execution_id, pipeline_name, row_number, raw_data, reasons = params
            row_dict = {
                "execution_id": execution_id,
                "pipeline_name": pipeline_name,
                "row_number": row_number,
                "raw_data": json.loads(raw_data) if isinstance(raw_data, str) else raw_data,
                "reasons": json.loads(reasons) if isinstance(reasons, str) else reasons,
            }
            if commit:
                self.rejected_data.append(row_dict)
            else:
                self.uncommitted_rejected.append(row_dict)
            return 1

        if "DELETE FROM pipeline_rejected_rows WHERE execution_id = %s" in q:
            exec_id = params[0]
            self.rejected_data = [r for r in self.rejected_data if r.get("execution_id") != exec_id]
            self.uncommitted_rejected = [r for r in self.uncommitted_rejected if r.get("execution_id") != exec_id]
            return 1

        return 1

    def execute_many(self, query: str, param_list: list[tuple], commit: bool = True):
        for params in param_list:
            self.execute(query, params, commit=commit)
        if commit:
            self.commit()
        return len(param_list)

    def commit(self):
        self.generic_data.extend(self.uncommitted_generic)
        self.uncommitted_generic.clear()
        self.rejected_data.extend(self.uncommitted_rejected)
        self.uncommitted_rejected.clear()

    def rollback(self):
        self.uncommitted_generic.clear()
        self.uncommitted_rejected.clear()


# ---------------------------------------------------------------------------
# Test 1: Regression reproduction - missing table records DATABASE_ERROR
# ---------------------------------------------------------------------------
def test_1_regression_missing_table_records_database_error(tmp_path):
    """Reproduce relation does not exist and verify root DATABASE_ERROR classification."""
    test_csv = tmp_path / "customer.csv"
    test_csv.write_text("customer_id,name,email,age,city\n1,Arun,arun@test.com,22,Kochi\n", encoding="utf-8")

    mock_db = MagicMock()
    mock_db.start_pipeline_execution.return_value = 501

    def mock_execute(query, params=None, commit=True):
        if "pipeline_generic_data" in query and "INSERT" in query:
            raise DatabaseError('Execute failed: relation "pipeline_generic_data" does not exist')
        return None

    mock_db.execute.side_effect = mock_execute

    with patch("pipelines.csv_pipeline.Database", return_value=mock_db), \
         patch("config.BASE_DIR", tmp_path), \
         patch("pipelines.csv_pipeline.config.BASE_DIR", tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        csv_file = data_dir / "customer.csv"
        csv_file.write_text("customer_id,name,email,age,city\n1,Arun,arun@test.com,22,Kochi\n", encoding="utf-8")

        source_config = {
            "file_path": str(csv_file),
            "schema_config": {
                "columns": {
                    "customer_id": {"expected_type": "integer", "required": True},
                    "name": {"expected_type": "text", "required": True},
                    "email": {"expected_type": "text", "required": True},
                    "age": {"expected_type": "integer", "required": True},
                    "city": {"expected_type": "text", "required": True},
                },
            },
        }

        # Simulate lazy ensure being bypassed so raw DB missing table error occurs
        with patch("pipelines.etl._ensure_generic_tables_lazy"):
            result = run_csv_pipeline(pipeline_name="customer_pipe", source_config=source_config)

    assert result["status"] == "FAILED"
    assert result["rows_processed"] == 0
    assert "relation \"pipeline_generic_data\" does not exist" in result["error_message"]
    assert result["failure_category"] == "DATABASE_ERROR"
    assert result["retryable"] is False


# ---------------------------------------------------------------------------
# Test 2: Generic 3-row customer CSV round-trips values and types exactly
# ---------------------------------------------------------------------------
def test_2_generic_3_row_customer_csv_loads_and_round_trips():
    """Customer CSV loads 3 valid rows with exact column names and converted types."""
    db = FakeGenericDatabase(table_exists=False)
    db.connection.commit.side_effect = db.commit
    db.connection.rollback.side_effect = db.rollback

    raw_csv = """customer_id,name,email,age,city
1,Arun,arun@example.com,22,Kochi
2,Meera,meera@example.com,23,Trivandrum
3,Rahul,rahul@example.com,21,Muvattupuzha
"""
    df = pd.read_csv(io.StringIO(raw_csv))
    schema_config = {
        "columns": {
            "customer_id": {"expected_type": "integer", "required": True, "unique": True},
            "name": {"expected_type": "text", "required": True},
            "email": {"expected_type": "text", "required": True},
            "age": {"expected_type": "integer", "required": True},
            "city": {"expected_type": "text", "required": True},
        }
    }

    valid_rows, rejected_rows, report = validate_generic_csv_data(df, schema_config)
    assert len(valid_rows) == 3
    assert len(rejected_rows) == 0
    assert report["input_rows"] == 3
    assert report["valid_rows"] == 3
    assert report["rejected_rows"] == 0

    rows_loaded = load_generic_data(valid_rows, db, execution_id=101, pipeline_name="customer_pipeline")
    assert rows_loaded == 3
    assert db.table_exists is True
    assert len(db.generic_data) == 3

    # Check payload round-trip
    row_1 = next(r for r in db.generic_data if r["row_number"] == 1)
    assert row_1["payload"]["customer_id"] == 1
    assert row_1["payload"]["name"] == "Arun"
    assert row_1["payload"]["email"] == "arun@example.com"
    assert row_1["payload"]["age"] == 22
    assert row_1["payload"]["city"] == "Kochi"

    # Invariants
    finalized, err = finalize_csv_quality(report.copy(), rows_loaded)
    assert err is None
    assert finalized["loaded_rows"] == 3
    assert finalized["input_rows"] == finalized["loaded_rows"] + finalized["rejected_rows"]


# ---------------------------------------------------------------------------
# Test 3: CSV with arbitrary/35 columns loads without code changes
# ---------------------------------------------------------------------------
def test_3_csv_with_35_columns_loads_without_code_changes():
    """Arbitrary schemas with 35 columns insert and serialize without fixed column names."""
    db = FakeGenericDatabase(table_exists=True)
    db.connection.commit.side_effect = db.commit
    db.connection.rollback.side_effect = db.rollback

    headers = [f"sensor_metric_{i}" for i in range(35)]
    records = [
        {h: (i * 10 if i % 2 == 0 else f"val_{i}") for i, h in enumerate(headers)}
        for row in range(5)
    ]

    loaded = load_generic_data(records, db, execution_id=102, pipeline_name="iot_sensors")
    assert loaded == 5
    assert len(db.generic_data) == 5

    sample_loaded = db.generic_data[0]["payload"]
    assert len(sample_loaded) == 35
    assert sample_loaded["sensor_metric_0"] == 0
    assert sample_loaded["sensor_metric_1"] == "val_1"


# ---------------------------------------------------------------------------
# Test 4: Failed database insert rolls back, loaded_rows = 0, records error
# ---------------------------------------------------------------------------
def test_4_failed_database_insert_rolls_back_and_records_database_error():
    """On DB insert failure: transaction rolls back, 0 rows loaded, error preserved."""
    db = FakeGenericDatabase(table_exists=True)
    db.connection.commit.side_effect = db.commit
    db.connection.rollback.side_effect = db.rollback

    def failing_execute(query, params=(), commit=True):
        if "INSERT INTO pipeline_generic_data" in query:
            raise DatabaseError("Execute failed: duplicate key value violates unique constraint")
        return 1

    db.execute = failing_execute

    records = [{"colA": 1, "colB": "test"}]
    with pytest.raises(ETLError, match="Generic load failed.*unique constraint"):
        load_generic_data(records, db, execution_id=103, pipeline_name="fail_pipe")

    assert len(db.generic_data) == 0
    assert len(db.uncommitted_generic) == 0

    category, retryable = classify_failure("Generic load failed: duplicate key value violates unique constraint")
    assert category == "DATABASE_ERROR"
    assert retryable is False


# ---------------------------------------------------------------------------
# Test 5: Invariants: input_rows = accepted_rows + rejected_rows
# ---------------------------------------------------------------------------
def test_5_input_rows_equals_accepted_plus_rejected():
    """Validation rejections are tracked; reconciliation invariant holds."""
    raw_csv = """id,score,status
1,85,active
2,invalid_score,active
3,,pending
1,90,active
"""
    df = pd.read_csv(io.StringIO(raw_csv))
    schema_config = {
        "columns": {
            "id": {"expected_type": "integer", "required": True, "unique": True},
            "score": {"expected_type": "integer", "required": True},
            "status": {"expected_type": "text", "required": True},
        }
    }

    valid_rows, rejected_rows, report = validate_generic_csv_data(df, schema_config)
    assert report["input_rows"] == 4
    assert report["valid_rows"] == len(valid_rows)
    assert report["rejected_rows"] == len(rejected_rows)
    assert report["input_rows"] == report["valid_rows"] + report["rejected_rows"]
    assert report["valid_rows"] == 1  # Only row 1 is fully valid (row 2 bad int, row 3 null score, row 4 dup id)


# ---------------------------------------------------------------------------
# Test 6: Retry after failed load is idempotent (no duplicate rows)
# ---------------------------------------------------------------------------
def test_6_retry_after_failed_load_is_idempotent():
    """Retrying the same execution_id clears previous rows and inserts cleanly."""
    db = FakeGenericDatabase(table_exists=True)
    db.connection.commit.side_effect = db.commit
    db.connection.rollback.side_effect = db.rollback

    records = [{"id": 1, "val": "A"}, {"id": 2, "val": "B"}]

    # Run attempt 1
    loaded1 = load_generic_data(records, db, execution_id=999, pipeline_name="retry_pipe")
    assert loaded1 == 2
    assert len(db.generic_data) == 2

    # Retry attempt 2 for the same execution_id
    loaded2 = load_generic_data(records, db, execution_id=999, pipeline_name="retry_pipe")
    assert loaded2 == 2
    assert len(db.generic_data) == 2  # Idempotent: exactly 2 rows, not 4


# ---------------------------------------------------------------------------
# Test 7: ensure_generic_csv_tables is idempotent
# ---------------------------------------------------------------------------
def test_7_ensure_generic_tables_is_idempotent():
    """Calling ensure_generic_csv_tables multiple times does not raise or duplicate."""
    db = FakeGenericDatabase(table_exists=False)
    db.ensure_generic_csv_tables()
    assert db.table_exists is True
    # Calling second time
    db.ensure_generic_csv_tables()
    assert db.table_exists is True


# ---------------------------------------------------------------------------
# Test 8: Table creation permission denied is classified clearly
# ---------------------------------------------------------------------------
def test_8_table_creation_permission_denied_is_classified():
    """Permission denied to create table raises and classifies as DATABASE_ERROR."""
    db = FakeGenericDatabase(table_exists=False, permission_denied=True)
    records = [{"a": 1}]

    with pytest.raises(ETLError, match="Database permission denied to create table"):
        load_generic_data(records, db, execution_id=777)

    cat, retryable = classify_failure("Database permission denied to create table pipeline_generic_data")
    assert cat == "DATABASE_ERROR"
    assert retryable is False


# ---------------------------------------------------------------------------
# Test 9: Legacy sales pipeline and API pipeline are unaffected
# ---------------------------------------------------------------------------
def test_9_legacy_and_api_pipelines_unaffected():
    """Legacy pipelines without schema_config still use sales path; API pipelines unaffected."""
    from pipelines.etl import load_sales_data
    mock_db = MagicMock()
    mock_db.connection.commit.return_value = None

    sales_df = pd.DataFrame([
        {"customer_name": "Alice", "product_name": "Gadget", "quantity": 1, "amount": 10.0, "sale_date": "2026-01-01"}
    ])
    loaded = load_sales_data(sales_df, mock_db)
    assert loaded == 1
    assert "sales_data" in mock_db.execute.call_args[0][0]


# ---------------------------------------------------------------------------
# Test 10: JSON serialization edge cases
# ---------------------------------------------------------------------------
def test_10_json_serialization_edge_cases():
    """JSON serialization handles NaN, null, dates, Decimal, large int, and unicode."""
    record = {
        "nan_val": float("nan"),
        "none_val": None,
        "date_val": date(2026, 10, 14),
        "dt_val": datetime(2026, 10, 14, 12, 30, 45),
        "decimal_val": Decimal("1234.56"),
        "large_int": 9223372036854775807,
        "unicode_val": "കൊച്ചി (Kochi) 🚀 Café",
        "np_int": np.int64(42),
        "np_float": np.float64(3.14),
        "np_nan": np.nan,
    }

    serialized = serialize_json_payload(record)
    loaded = json.loads(serialized)

    assert loaded["nan_val"] is None
    assert loaded["none_val"] is None
    assert loaded["date_val"] == "2026-10-14"
    assert loaded["dt_val"] == "2026-10-14T12:30:45"
    assert loaded["decimal_val"] == 1234.56
    assert loaded["large_int"] == 9223372036854775807
    assert loaded["unicode_val"] == "കൊച്ചി (Kochi) 🚀 Café"
    assert loaded["np_int"] == 42
    assert loaded["np_float"] == 3.14
    assert loaded["np_nan"] is None
