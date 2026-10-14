import sys
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from pipelines.completeness import (
    CompletenessReport,
    evaluate_data_completeness,
    inspect_api_response_completeness,
    simulate_recovery_workflow,
)
from pipelines.failure_handling import classify_failure
from pipelines.recovery import run_with_recovery


def test_incomplete_row_counts_detection():
    """Verify detection of incomplete extraction when received rows < expected rows."""
    data = [
        {"customer_id": 1, "name": "Arun"},
        {"customer_id": 2, "name": "Meera"},
        {"customer_id": 3, "name": "Rahul"},
        {"customer_id": 4, "name": "Ananya"},
    ]
    report = evaluate_data_completeness(data, expected_rows=6, unique_keys="customer_id")

    assert report.expected_rows == 6
    assert report.received_rows == 4
    assert report.discrepancy == 2
    assert report.is_complete is False
    assert report.status == "FAILED — DATA INCOMPLETE"
    assert any(i.category == "COMPLETENESS" and "2 missing" in i.problem for i in report.issues)


def test_duplicates_checked_separately_not_substitute_for_missing_records():
    """Verify duplicate checking does NOT mask or substitute for missing records."""
    # 5 rows received for expected 6. Among them, customer_id 103 is duplicated.
    data = [
        {"customer_id": 101, "name": "Arun"},
        {"customer_id": 102, "name": "Meera"},
        {"customer_id": 103, "name": "Rahul"},
        {"customer_id": 104, "name": "Ananya"},
        {"customer_id": 103, "name": "Rahul Duplicate"},
    ]
    report = evaluate_data_completeness(data, expected_rows=6, unique_keys="customer_id")

    # Missing records must remain strictly 1
    assert report.discrepancy == 1
    # Duplicate records must be detected as 1
    assert report.duplicate_count == 1
    # Valid rows must be 4 (5 received - 1 duplicate rejected)
    assert report.valid_rows == 4
    assert report.rejected_rows == 1
    # Invariant: received == valid + rejected
    assert report.received_rows == report.valid_rows + report.rejected_rows

    # Both issues must be reported distinctly
    categories = [i.category for i in report.issues]
    assert "COMPLETENESS" in categories
    assert "DUPLICATE" in categories


def test_composite_unique_key_duplicates():
    """Verify composite unique key duplicate detection."""
    data = [
        {"store_id": 1, "product_id": 10, "amount": 100},
        {"store_id": 1, "product_id": 20, "amount": 200},
        {"store_id": 2, "product_id": 10, "amount": 150},
        {"store_id": 1, "product_id": 10, "amount": 100},  # Duplicate on (store_id, product_id)
    ]
    report = evaluate_data_completeness(data, unique_keys=["store_id", "product_id"])

    assert report.duplicate_count == 1
    assert report.valid_rows == 3
    assert report.rejected_rows == 1


def test_api_pagination_and_completeness_distinguishes_http_200():
    """Distinguish successful HTTP 200 response from complete data extraction."""
    # Payload has 50 items total, but page 1 only returns 10 carts
    payload = {
        "carts": [{"id": i} for i in range(1, 11)],
        "total": 50,
        "skip": 0,
        "limit": 10,
    }
    extracted_count = len(payload["carts"])

    info = inspect_api_response_completeness(payload, extracted_rows_count=extracted_count)
    assert info["has_pagination"] is True
    assert info["is_complete"] is False
    assert info["total_available"] == 50
    assert info["discrepancy"] == 40
    assert "HTTP 200 OK received, but extraction is incomplete" in info["notes"]

    # When all 50 items are extracted
    info_full = inspect_api_response_completeness(payload, extracted_rows_count=50)
    assert info_full["is_complete"] is True
    assert info_full["discrepancy"] == 0


def test_retry_exhaustion_scenario_reports_failed_data_incomplete():
    """Simulation: 6 expected, 4 received; retries exhausted -> FAILED — DATA INCOMPLETE."""
    report = simulate_recovery_workflow(
        scenario="incomplete_exhausted",
        expected_rows=6,
        initial_received_rows=4,
        max_retries=2,
    )

    assert report.expected_rows == 6
    assert report.received_rows == 4
    assert report.discrepancy == 2
    assert report.status == "FAILED — DATA INCOMPLETE"
    assert len(report.attempts) == 3  # Attempt 0, Retry 1, Retry 2
    assert report.attempts[-1].status == "FAILED — DATA INCOMPLETE"
    assert report.loaded_rows == 0  # Incomplete data must not commit partial records


def test_successful_retry_recovery_reports_recovered():
    """Simulation: 6 expected, 4 received on attempt 0 -> retry 1 succeeds with 6 -> RECOVERED."""
    report = simulate_recovery_workflow(
        scenario="incomplete_recovered",
        expected_rows=6,
        initial_received_rows=4,
        max_retries=2,
    )

    assert report.expected_rows == 6
    assert report.received_rows == 6
    assert report.discrepancy == 0
    assert report.status == "RECOVERED"
    assert len(report.attempts) == 2  # Attempt 0 (failed), Retry 1 (succeeded)
    assert report.attempts[0].status == "RETRYING"
    assert report.attempts[1].status == "SUCCESS"
    assert report.loaded_rows == 6


def test_permanent_schema_errors_not_retried():
    """Verify permanent schema errors (e.g. missing column) are non-retryable."""
    cat, retryable = classify_failure("Missing required columns: customer_name, sale_date")
    assert cat == "DATA_QUALITY_ERROR"
    assert retryable is False

    cat_comp, retryable_comp = classify_failure("Incomplete extraction: expected 6 rows, but received only 4 rows")
    assert cat_comp == "DATA_QUALITY_ERROR"
    assert retryable_comp is True


def test_workflow_stages_generation():
    """Verify visual workflow pipeline stages are generated with correct structure."""
    report = simulate_recovery_workflow(scenario="incomplete_exhausted")
    assert len(report.workflow_stages) == 6
    stage_names = [s["stage"] for s in report.workflow_stages]
    assert stage_names == [
        "1. Extract",
        "2. Validate",
        "3. Detect Issue",
        "4. Retry Engine",
        "5. Verify & Reconcile",
        "6. Final Status",
    ]


def test_recovery_orchestrator_marks_failed_data_incomplete(monkeypatch):
    """Verify recovery runner updates failure_type to FAILED — DATA INCOMPLETE when retries exhaust."""
    mock_runner = MagicMock()
    mock_runner.return_value = {
        "status": "FAILED",
        "rows_processed": 0,
        "error_message": "Incomplete extraction: expected 6 rows, but received only 4 rows (2 missing)",
        "retry_count": 0,
    }

    result = run_with_recovery(
        "test_pipeline",
        mock_runner,
        max_retries=1,
    )

    assert result["status"] == "FAILED"
    assert result["failure_type"] == "FAILED — DATA INCOMPLETE"
    assert result["failure_category"] == "DATA_INCOMPLETE"


def test_simulation_scenario_incomplete_with_duplicates():
    """Verify simulation of 5 rows received for 6 expected with 1 duplicate."""
    report = simulate_recovery_workflow(
        scenario="incomplete_with_duplicates",
        expected_rows=6,
        initial_received_rows=5,
        max_retries=2,
    )

    assert report.expected_rows == 6
    assert report.received_rows == 5
    assert report.discrepancy == 1
    assert report.duplicate_count == 1
    assert report.valid_rows == 4
    assert report.rejected_rows == 1
    assert report.status == "FAILED — DATA INCOMPLETE"
    assert report.loaded_rows == 0


def test_csv_pipeline_expected_rows_completeness_failure():
    """Verify run_csv_pipeline validates expected_rows and reports completeness error."""
    from pipelines.csv_pipeline import run_csv_pipeline

    # Mock database to prevent any live DB writes
    mock_db = MagicMock()
    mock_db.start_pipeline_execution.return_value = 8888
    mock_db.update_pipeline_execution.return_value = None

    import database.db
    orig_db = database.db.Database
    database.db.Database = MagicMock(return_value=mock_db)
    try:
        # sales.csv has ~10 rows; set expected_rows=99999 to guarantee completeness failure
        result = run_csv_pipeline(
            source_config={"expected_rows": 99999, "file_path": "data/csv/sales.csv"},
        )
        assert result["status"] == "FAILED"
        assert "Incomplete extraction" in result["error_message"]
        assert result["expected_rows"] == 99999
        assert result["failure_type"] == "DATA_QUALITY_ERROR"
        assert result["retryable"] is True
    finally:
        database.db.Database = orig_db

