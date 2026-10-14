"""
Regression reproduction test for customer_pipeline failure.
Reproduces the exact failure WITHOUT running against the live database or modifying records.
"""

from __future__ import annotations

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from unittest.mock import MagicMock, patch
import pandas as pd
import pytest

from database.db import DatabaseError
from pipelines.csv_pipeline import run_csv_pipeline


SAMPLE_CUSTOMER_CSV = """customer_id,name,email,age,city
1,Arun,arun@example.com,22,Kochi
2,Meera,meera@example.com,23,Trivandrum
3,Rahul,rahul@example.com,21,Muvattupuzha
"""


def test_reproduce_customer_pipeline_generic_load_failure(tmp_path):
    # Setup test file inside project's data directory or mocked path
    test_csv = tmp_path / "customer_test.csv"
    test_csv.write_text(SAMPLE_CUSTOMER_CSV, encoding="utf-8")

    source_config = {
        "file_path": str(test_csv),
        "schema_config": {
            "columns": {
                "customer_id": {"expected_type": "integer", "required": True, "unique": True},
                "name": {"expected_type": "text", "required": True},
                "email": {"expected_type": "text", "required": True},
                "age": {"expected_type": "integer", "required": True},
                "city": {"expected_type": "text", "required": True},
            },
            "composite_unique_keys": [],
        },
    }

    # Mock Database to simulate the exact real DB state where pipeline_generic_data table does not exist
    mock_db = MagicMock()
    mock_db.start_pipeline_execution.return_value = 9999

    def mock_execute(query, params=None, commit=True):
        if "pipeline_generic_data" in query:
            raise DatabaseError('Execute failed: relation "pipeline_generic_data" does not exist\nLINE 2:                 INSERT INTO pipeline_generic_data')
        return None

    mock_db.execute.side_effect = mock_execute

    with patch("pipelines.csv_pipeline.Database", return_value=mock_db), \
         patch("config.BASE_DIR", tmp_path), \
         patch("pipelines.csv_pipeline.config.BASE_DIR", tmp_path):
        # Create data dir in tmp_path to satisfy relative_to checks
        data_dir = tmp_path / "data"
        data_dir.mkdir(parents=True, exist_ok=True)
        csv_file = data_dir / "customers.csv"
        csv_file.write_text(SAMPLE_CUSTOMER_CSV, encoding="utf-8")
        source_config["file_path"] = str(csv_file)

        result = run_csv_pipeline(
            pipeline_name="customer_pipeline_2",
            source_config=source_config,
        )

    # Verify that it reproduces the exact symptoms and failure message
    assert result["status"] == "FAILED"
    assert result["rows_processed"] == 0
    assert "relation \"pipeline_generic_data\" does not exist" in result["error_message"]
    assert "CSV load count mismatch: 3 valid rows, 0 loaded rows" in result["error_message"]
    assert result["failure_category"] == "DATABASE_ERROR"
