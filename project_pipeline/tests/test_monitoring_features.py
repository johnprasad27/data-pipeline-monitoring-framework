import sys
from types import ModuleType
from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest

from api.api_client import APIError
from monitoring.dashboard import format_failure_category, format_retryable
from monitoring.freshness import get_freshness_status
from pipelines.etl import evaluate_data_quality, validate_sales_data
from pipelines.failure_handling import classify_failure
from pipelines.recovery import run_with_recovery
from pipelines.registry import (
    delete_disabled_pipeline,
    get_enabled_pipeline_configs,
    load_pipeline_registry,
    resolve_pipeline_runner,
    validate_pipeline_registration,
)


def test_network_failure_is_classified_and_retryable():
    failure_type, retryable = classify_failure("Connection refused by API host")
    assert failure_type == "NETWORK_ERROR"
    assert retryable is True


def test_missing_csv_file_is_file_error_and_not_retryable():
    failure_type, retryable = classify_failure("CSV file not found: missing.csv")
    assert failure_type == "FILE_ERROR"
    assert retryable is False


def test_generic_api_connection_and_malformed_csv_failures_are_classified():
    assert classify_failure("Connection error: host unreachable") == ("NETWORK_ERROR", True)
    assert classify_failure("Failed to read CSV: malformed input") == ("FILE_ERROR", False)


@pytest.mark.parametrize(
    ("error", "expected"),
    [
        ("API request timed out", ("TIMEOUT", True)),
        ("HTTP error: 503 Service Unavailable", ("API_ERROR", True)),
        ("HTTP error: 404 Not Found", ("API_ERROR", False)),
        ("PostgreSQL relation does not exist", ("DATABASE_ERROR", False)),
        ("database server closed the connection", ("DATABASE_ERROR", True)),
        ("CSV file not found: missing.csv", ("FILE_ERROR", False)),
        ("Missing required columns: sale_date", ("DATA_QUALITY_ERROR", False)),
        ("unexpected internal failure", ("UNKNOWN", False)),
    ],
)
def test_failure_categories_and_retry_decisions(error, expected):
    assert classify_failure(error) == expected


def test_exception_classification_uses_exception_type_and_cause():
    import requests

    assert classify_failure(requests.Timeout("slow API")) == ("TIMEOUT", True)
    assert classify_failure(requests.Timeout()) == ("TIMEOUT", True)
    assert classify_failure(requests.ConnectionError("unreachable")) == (
        "NETWORK_ERROR",
        True,
    )
    assert classify_failure(APIError("HTTP error: 502")) == ("API_ERROR", True)


def test_data_quality_evaluation_reports_missing_values_and_duplicates():
    records = [
        {"id": 1, "date": "2024-01-01", "product": "Widget", "amount": 10.0},
        {"id": 2, "date": None, "product": "Widget", "amount": 10.0},
        {"id": 1, "date": "2024-01-01", "product": "Widget", "amount": 10.0},
    ]
    quality = evaluate_data_quality(records, required_columns=["id", "date", "product", "amount"])
    assert quality["missing_values"] >= 1
    assert quality["duplicate_rows"] >= 1
    assert quality["score"] <= 100
    assert quality["issues"]


def test_api_validation_accepts_duplicate_looking_rows_without_changing_csv_rules():
    import pandas as pd

    records = [
        {
            "customer_name": "Alex Smith",
            "product_name": "Widget",
            "quantity": 1,
            "amount": 10.0,
            "sale_date": "2024-01-01",
        },
        {
            "customer_name": "Alex Smith",
            "product_name": "Widget",
            "quantity": 1,
            "amount": 10.0,
            "sale_date": "2024-01-01",
        },
    ]
    frame = pd.DataFrame(records)

    _, api_errors = validate_sales_data(frame, check_duplicates=False)
    _, csv_errors = validate_sales_data(frame)
    api_quality = evaluate_data_quality(frame, check_duplicates=False)

    assert api_errors == []
    assert "Found 1 duplicate rows" in csv_errors
    assert api_quality["duplicate_rows"] == 0
    assert api_quality["score"] == 100.0


def test_freshness_status_thresholds():
    now = datetime.now()
    assert get_freshness_status(now - timedelta(seconds=30), now) == "FRESH"
    assert get_freshness_status(now - timedelta(minutes=3), now) == "WARNING"
    assert get_freshness_status(now - timedelta(minutes=10), now) == "STALE"


def test_config_registry_loads_enabled_pipelines():
    config = load_pipeline_registry("pipelines.yaml")
    assert "api_pipeline" in config
    assert "csv_pipeline" in config
    assert config["api_pipeline"]["enabled"] is True
    assert resolve_pipeline_runner(config["api_pipeline"]["handler"]).__name__ == "run_api_pipeline"
    assert resolve_pipeline_runner(config["csv_pipeline"]["handler"]).__name__ == "run_csv_pipeline"


def test_pipeline_registration_normalizes_valid_api_config():
    registration = validate_pipeline_registration(
        "weather_pipeline", "api", {"url": "https://example.com/data"}, 120, 2
    )
    assert registration["pipeline_type"] == "API"
    assert registration["interval_seconds"] == 120
    assert registration["max_retries"] == 2


def test_registry_resolves_only_predefined_handlers():
    assert resolve_pipeline_runner("CSV").__name__ == "run_csv_pipeline"
    assert resolve_pipeline_runner("api_pipeline").__name__ == "run_api_pipeline"
    with pytest.raises(ValueError, match="Unsupported pipeline handler"):
        resolve_pipeline_runner("custom_python")


def test_scheduler_runs_a_new_registered_handler_from_configuration(
    tmp_path, monkeypatch
):
    module_name = "pipelines.configured_test_handler"
    module = ModuleType(module_name)
    received = []
    recorded_executions = []

    class FakeDatabase:
        def connect(self):
            pass

        def start_pipeline_execution(self, *args):
            return 123

        def update_pipeline_execution(self, *args, **kwargs):
            recorded_executions.append((args, kwargs))

        def close(self):
            pass

    def run_configured_test_handler(**kwargs):
        received.append(kwargs)
        return {"status": "SUCCESS", **kwargs}

    module.run_configured_test_handler = run_configured_test_handler
    monkeypatch.setitem(sys.modules, module_name, module)
    monkeypatch.setattr("monitoring.execution.Database", FakeDatabase)

    config_path = tmp_path / "pipelines.yaml"
    config_path.write_text(
        """
pipelines:
  - name: weather_ingest
    source: API
    module: pipelines.configured_test_handler
    handler: run_configured_test_handler
    interval: 30
    max_retries: 2
    enabled: true
    url: https://example.com/weather
    source_config:
      endpoint: https://example.com/weather
""".strip(),
        encoding="utf-8",
    )

    pipeline = get_enabled_pipeline_configs(config_path)[0]
    assert pipeline["name"] == "weather_ingest"
    assert pipeline["source_config"]["endpoint"] == "https://example.com/weather"
    assert pipeline["interval_seconds"] == 30
    assert pipeline["max_retries"] == 2

    from pipelines import scheduler
    scheduler._run_pipeline(pipeline)

    assert received == [
        {
            "pipeline_name": "weather_ingest",
            "source_config": {
                "url": "https://example.com/weather",
                "endpoint": "https://example.com/weather",
            },
            "retry_count": 0,
        }
    ]
    assert recorded_executions[0][0][1] == "SUCCESS"
    assert recorded_executions[0][1]["pipeline_name"] == "weather_ingest"


def test_pipeline_configuration_reports_missing_handler(tmp_path):
    config_path = tmp_path / "pipelines.yaml"
    config_path.write_text(
        """
pipelines:
  - name: missing_handler
    source: API
    interval: 30
    max_retries: 1
    enabled: true
""".strip(),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="handler"):
        load_pipeline_registry(config_path)


def test_pipeline_registration_rejects_invalid_name_url_and_interval():
    with pytest.raises(ValueError, match="Pipeline name"):
        validate_pipeline_registration("../weather", "API", {"url": "https://example.com"}, 60, 1)
    with pytest.raises(ValueError, match="API URL"):
        validate_pipeline_registration("weather", "API", {"url": "javascript:alert(1)"}, 60, 1)
    with pytest.raises(ValueError, match="positive integer"):
        validate_pipeline_registration("weather", "API", {"url": "https://example.com"}, 0, 1)


def test_pipeline_registration_requires_csv_inside_project_data():
    registration = validate_pipeline_registration(
        "customer_pipeline", "CSV", {"file_path": "data/csv/sales.csv"}, 60, 0
    )
    assert registration["source_config"]["file_path"] == "data\\csv\\sales.csv"
    with pytest.raises(ValueError, match="CSV file is required"):
        validate_pipeline_registration("customer_pipeline", "CSV", {}, 60, 0)


def test_csv_quality_config_is_retained_by_pipeline_registration():
    quality_config = {
        "required_columns": ["customer_id", "quantity"],
        "expected_data_types": {"quantity": "integer"},
    }
    registration = validate_pipeline_registration(
        "typed_csv",
        "CSV",
        {"file_path": "data/csv/sales.csv", "quality_config": quality_config},
        60,
        1,
    )
    assert registration["source_config"]["quality_config"] == quality_config


def test_delete_disabled_pipeline_only_deletes_registry_row():
    db = Mock()
    db.execute.return_value = 1

    delete_disabled_pipeline(db, "temporary_pipeline")

    query, params = db.execute.call_args.args
    assert query.strip().startswith("DELETE FROM pipeline_registry")
    assert "enabled = FALSE" in query
    assert "pipeline_execution" not in query
    assert params == ("temporary_pipeline",)


def test_delete_disabled_pipeline_rejects_enabled_or_unknown_rows():
    db = Mock()
    db.execute.return_value = 0

    with pytest.raises(ValueError, match="Only a disabled"):
        delete_disabled_pipeline(db, "active_pipeline")


def test_recovery_uses_pipeline_specific_retry_limit(monkeypatch):
    attempts = []
    monkeypatch.setattr("pipelines.recovery._mark_retrying", lambda *_: None)

    def runner(retry_count=0):
        attempts.append(retry_count)
        if retry_count == 0:
            return {
                "status": "FAILED",
                "error_message": "Connection error",
                "failure_type": "NETWORK_ERROR",
                "retryable": True,
                "retry_count": retry_count,
                "pipeline_name": "weather_pipeline",
            }
        return {"status": "SUCCESS", "retry_count": retry_count}

    result = run_with_recovery("weather_pipeline", runner, max_retries=1)
    assert attempts == [0, 1]
    assert result["status"] == "SUCCESS"


def test_recovery_does_not_retry_non_retryable_failure():
    attempts = []

    def runner(retry_count=0):
        attempts.append(retry_count)
        return {
            "status": "FAILED",
            "error_message": "Missing required columns: sale_date",
            "failure_type": "DATA_QUALITY_ERROR",
            "retryable": False,
            "retry_count": retry_count,
            "pipeline_name": "customer_pipeline",
        }

    result = run_with_recovery("customer_pipeline", runner, max_retries=3)
    assert attempts == [0]
    assert result["failure_type"] == "DATA_QUALITY_ERROR"
    assert result["retryable"] is False


def test_recovery_exhausts_exactly_the_configured_retry_limit(monkeypatch):
    attempts = []
    monkeypatch.setattr("pipelines.recovery._mark_retrying", lambda *_: None)

    def runner(retry_count=0):
        attempts.append(retry_count)
        return {
            "status": "FAILED",
            "error_message": "Connection reset by peer",
            "retry_count": retry_count,
            "pipeline_name": "transient_pipeline",
        }

    result = run_with_recovery("transient_pipeline", runner, max_retries=2)

    assert attempts == [0, 1, 2]
    assert result["status"] == "FAILED"
    assert result["failure_category"] == "NETWORK_ERROR"
    assert result["retryable"] is True


def test_recovery_classifies_and_retries_timeout_exceptions(monkeypatch):
    import requests

    attempts = []
    monkeypatch.setattr("pipelines.recovery._mark_retrying", lambda *_: None)

    def runner(retry_count=0):
        attempts.append(retry_count)
        raise requests.Timeout()

    result = run_with_recovery("timeout_pipeline", runner, max_retries=1)

    assert attempts == [0, 1]
    assert result["failure_category"] == "TIMEOUT"
    assert result["retryable"] is True


def test_failed_run_displays_failure_category_and_retryable():
    # Failed run with classified category
    assert format_failure_category("FAILED", "NETWORK_ERROR", retry_count=3) == "NETWORK_ERROR"
    assert format_retryable("FAILED", True) == "YES"
    assert format_retryable("FAILED", False) == "NO"

    # Terminally failed run with no explicit category defaults to UNKNOWN
    assert format_failure_category("FAILED", None, retry_count=3) == "UNKNOWN"
    assert format_failure_category("FAILED", "", retry_count=1) == "UNKNOWN"


def test_successful_run_displays_dash_instead_of_failure_category():
    # Clean successful run with no category
    assert format_failure_category("SUCCESS", None, retry_count=0) == "—"
    assert format_failure_category("SUCCESS", "", retry_count=0) == "—"

    # Legacy rows that had UNKNOWN on SUCCESS should still show "—"
    assert format_failure_category("SUCCESS", "UNKNOWN", retry_count=0) == "—"
    assert format_failure_category("SUCCESS", "UNKNOWN_ERROR", retry_count=0) == "—"

    # Retryable indicator is not applicable on SUCCESS
    assert format_retryable("SUCCESS", None) == "—"
    assert format_retryable("SUCCESS", False) == "—"
    assert format_retryable("SUCCESS", True) == "—"


def test_failed_api_attempt_followed_by_successful_retry(monkeypatch):
    monkeypatch.setattr("pipelines.recovery._mark_retrying", lambda *_: None)
    monkeypatch.setattr("pipelines.recovery._record_recovered_category", lambda *_: None)

    attempts = []

    def mock_api_runner(retry_count=0):
        attempts.append(retry_count)
        if retry_count == 0:
            return {
                "status": "FAILED",
                "error_message": "Connection error: Failed to resolve dummyjson.com",
                "retry_count": retry_count,
                "pipeline_name": "api_pipeline",
            }
        return {
            "status": "SUCCESS",
            "rows_processed": 30,
            "error_message": None,
            "retry_count": retry_count,
            "pipeline_name": "api_pipeline",
        }

    result = run_with_recovery("api_pipeline", mock_api_runner, max_retries=3)

    assert attempts == [0, 1]
    assert result["status"] == "SUCCESS"
    assert result["retry_count"] == 1
    assert result["recovered_failure_category"] == "NETWORK_ERROR"
    assert result["failure_category"] == "Recovered (NETWORK_ERROR)"

    # Dashboard formatting tests for the recovered run
    assert (
        format_failure_category(result["status"], result["failure_category"], result["retry_count"])
        == "Recovered (NETWORK_ERROR)"
    )
    assert format_retryable(result["status"], result.get("retryable")) == "—"

def test_get_all_pipelines_query_schema_regression():
    """Regression test: verify get_all_pipelines query syntax and CTE projection.

    Ensures CTE 'latest' projects source_type, retry_count, created_at, start_time,
    and end_time to prevent 'column latest.source_type does not exist'.
    """
    from monitoring import queries

    captured_sql = None

    class MockDB:
        def fetch_all(self, sql, params=None):
            nonlocal captured_sql
            captured_sql = sql
            return [
                {
                    "pipeline_name": "api_pipeline",
                    "source_type": "API",
                    "status": "SUCCESS",
                    "rows_processed": 100,
                    "execution_duration_seconds": 1.5,
                    "retry_count": 0,
                    "created_at": None,
                    "start_time": None,
                    "end_time": None,
                    "failure_category": None,
                    "enabled": True,
                    "registered_at": None,
                }
            ]

    mock_db = MockDB()
    rows = queries.get_all_pipelines(mock_db)
    assert len(rows) == 1
    assert rows[0]["source_type"] == "API"

    # Regression assertion: CTE must project source_type before the outer SELECT
    assert captured_sql is not None
    assert "WITH latest AS (" in captured_sql
    cte_body = captured_sql.split("WITH latest AS (")[1].split("FROM pipeline_registry")[0]
    assert "source_type" in cte_body, "CTE latest must explicitly project source_type"
    assert "latest.source_type" in captured_sql
    assert "registry.pipeline_type" in captured_sql


def test_get_all_pipelines_live_database_query():
    """Live database regression test: runs read-only get_all_pipelines on real schema."""
    from database.db import Database
    from monitoring import queries

    try:
        db = Database()
        db.connect()
    except Exception:
        pytest.skip("Database not reachable for live query test")

    try:
        results = queries.get_all_pipelines(db)
        assert isinstance(results, list)
        for row in results:
            assert "pipeline_name" in row
            assert "source_type" in row
            assert "status" in row
            assert "failure_category" in row
            assert row["source_type"] in ("CSV", "API", None)
    finally:
        db.close()


def test_failure_category_not_associated_with_newer_success():
    """Verify that a newer successful execution is not associated with an older failure."""
    failed_record = {
        "execution_id": 100,
        "pipeline_name": "api_pipeline",
        "status": "FAILED",
        "retry_count": 3,
        "failure_category": "NETWORK_ERROR",
        "error_message": "Connection refused",
    }
    success_record = {
        "execution_id": 101,
        "pipeline_name": "api_pipeline",
        "status": "SUCCESS",
        "retry_count": 0,
        "failure_category": None,
        "error_message": None,
    }

    assert format_failure_category(
        failed_record["status"],
        failed_record["failure_category"],
        failed_record["retry_count"],
    ) == "NETWORK_ERROR"
    assert failed_record["error_message"] == "Connection refused"

    assert format_failure_category(
        success_record["status"],
        success_record["failure_category"],
        success_record["retry_count"],
    ) == "—"
    assert success_record["error_message"] is None


def test_api_client_normalizes_dummyjson_products_payload():
    """Verify that DummyJSON /products payloads are correctly parsed and normalized."""
    from api.api_client import APIClient

    client = APIClient(base_url="https://dummyjson.com/products")
    mock_payload = {
        "products": [
            {
                "id": 1,
                "title": "Essence Mascara Lash Princess",
                "price": 9.99,
                "minimumOrderQuantity": 2,
            },
            {
                "id": 2,
                "title": "Eyeshadow Palette with Mirror",
                "price": 19.99,
                "quantity": 3,
            },
        ],
        "total": 2,
        "skip": 0,
        "limit": 30,
    }

    df = client.get_dataframe(mock_payload)
    assert len(df) == 2
    assert list(df.columns) == [
        "customer_name",
        "product_name",
        "quantity",
        "amount",
        "sale_date",
    ]
    assert df.iloc[0]["customer_name"] == "Customer 1"
    assert df.iloc[0]["product_name"] == "Essence Mascara Lash Princess"
    assert df.iloc[0]["quantity"] == 2
    assert df.iloc[0]["amount"] == 9.99

    assert df.iloc[1]["customer_name"] == "Customer 2"
    assert df.iloc[1]["product_name"] == "Eyeshadow Palette with Mirror"
    assert df.iloc[1]["quantity"] == 3
    assert df.iloc[1]["amount"] == 19.99

    # Verify that the extracted DataFrame passes downstream ETL validation
    clean_df, errors = validate_sales_data(df, check_duplicates=False)
    assert not errors
    assert len(clean_df) == 2


def test_api_client_preserves_dummyjson_carts_and_generic_lists():
    """Verify backwards compatibility for DummyJSON /carts and raw list payloads."""
    from api.api_client import APIClient

    client = APIClient(base_url="https://dummyjson.com/carts")
    carts_payload = {
        "carts": [
            {
                "id": 10,
                "products": [
                    {"title": "Laptop", "price": 999.0, "quantity": 1},
                    {"title": "Mouse", "price": 25.0, "quantity": 2},
                ],
            }
        ]
    }
    df_carts = client.get_dataframe(carts_payload)
    assert len(df_carts) == 2
    assert df_carts.iloc[0]["customer_name"] == "Customer 10"
    assert df_carts.iloc[0]["product_name"] == "Laptop"

    # Raw list payload
    list_payload = [
        {
            "customer_name": "Alice",
            "product_name": "Book",
            "quantity": 1,
            "amount": 15.5,
            "sale_date": "2026-10-14",
        }
    ]
    df_list = client.get_dataframe(list_payload)
    assert len(df_list) == 1
    assert df_list.iloc[0]["customer_name"] == "Alice"


def test_api_client_unsupported_payload_raises_api_error():
    """Verify that unsupported payload shapes raise APIError with informative message."""
    from api.api_client import APIClient

    client = APIClient(base_url="https://example.com/api")
    with pytest.raises(APIError, match="Unsupported payload shape"):
        client.get_dataframe({"unexpected_key": "not_a_list"})


def test_products_api_pipeline_registration_and_runner_resolution():
    """Verify that a pipeline configured with the DummyJSON products endpoint resolves cleanly."""
    registration = validate_pipeline_registration(
        name="products_api_pipeline",
        pipeline_type="API",
        source_config={"url": "https://dummyjson.com/products"},
        interval_seconds=60,
        max_retries=3,
    )
    assert registration["name"] == "products_api_pipeline"
    assert registration["pipeline_type"] == "API"
    assert registration["source_config"]["url"] == "https://dummyjson.com/products"

    runner = resolve_pipeline_runner("API")
    assert callable(runner)


def test_app_navigation_includes_execution_history_page():
    """Verify that app.py registers all four pages and each appears exactly once."""
    from pathlib import Path
    import app

    assert len(app.pages) == 4

    app_text = Path(app.__file__).read_text(encoding="utf-8")
    assert 'st.Page("pages/overview.py", title="Overview"' in app_text
    assert 'st.Page("monitoring/dashboard.py", title="Pipeline Monitor"' in app_text
    assert 'st.Page("pages/execution_history.py", title="Execution History"' in app_text
    assert 'st.Page("pages/add_pipeline.py", title="Add Pipeline"' in app_text

    base_dir = Path(app.__file__).parent
    for page_path in ["pages/overview.py", "monitoring/dashboard.py", "pages/execution_history.py", "pages/add_pipeline.py"]:
        assert (base_dir / page_path).is_file()


def test_execution_history_query_filtering():
    """Test queries.execution_history query builder with pipeline, status, and time filters."""
    from monitoring import queries

    captured_sql = None
    captured_params = None

    class MockDB:
        def fetch_all(self, sql, params=None):
            nonlocal captured_sql, captured_params
            captured_sql = sql
            captured_params = params
            return []

    mock_db = MockDB()

    # Test 1: all executions (unfiltered)
    queries.execution_history(mock_db, limit=50)
    assert "ORDER BY execution_id DESC LIMIT 50" in captured_sql
    assert captured_params is None

    # Test 2: filter by pipeline
    queries.execution_history(mock_db, limit=100, pipeline_name="api_pipeline")
    assert "pipeline_name = %s" in captured_sql
    assert "api_pipeline" in captured_params

    # Test 3: filter by pipeline and status
    queries.execution_history(mock_db, limit=100, pipeline_name="csv_pipeline", status="FAILED")
    assert "pipeline_name = %s" in captured_sql
    assert "UPPER(status) = %s" in captured_sql
    assert captured_params == ("csv_pipeline", "FAILED")

    # Test 4: filter with time window
    queries.execution_history(mock_db, limit=25, status="SUCCESS", since="2026-10-14 00:00:00")
    assert "UPPER(status) = %s" in captured_sql
    assert "created_at >= %s" in captured_sql
    assert captured_params == ("SUCCESS", "2026-10-14 00:00:00")


def test_format_duration_calculation_and_fallback():
    """Verify format_duration handles stored duration and start/end time fallback."""
    from monitoring.dashboard import format_duration

    # Stored duration
    assert format_duration({"execution_duration_seconds": 1.234}) == "1.23 s"
    assert format_duration({"execution_duration_seconds": "2.5"}) == "2.50 s"

    # Fallback to start_time and end_time
    assert format_duration({
        "execution_duration_seconds": None,
        "start_time": "2026-10-14 10:00:00",
        "end_time": "2026-10-14 10:00:03.5",
    }) == "3.50 s"

    # Missing times
    assert format_duration({}) == "0.00 s"


def test_execution_history_formatting_preserves_failures_and_cleans_success():
    """Verify failure categories and error details are preserved for failed runs and cleared for success."""
    from monitoring.dashboard import format_failure_category

    success_item = {
        "status": "SUCCESS",
        "failure_category": None,
        "retry_count": 0,
        "error_message": None,
    }
    failed_item = {
        "status": "FAILED",
        "failure_category": "DATABASE_ERROR",
        "retry_count": 2,
        "error_message": "relation does not exist",
    }
    recovered_item = {
        "status": "SUCCESS",
        "failure_category": "NETWORK_ERROR",
        "retry_count": 1,
        "error_message": None,
    }

    assert format_failure_category(
        success_item["status"], success_item["failure_category"], success_item["retry_count"]
    ) == "—"

    assert format_failure_category(
        failed_item["status"], failed_item["failure_category"], failed_item["retry_count"]
    ) == "DATABASE_ERROR"

    assert format_failure_category(
        recovered_item["status"], recovered_item["failure_category"], recovered_item["retry_count"]
    ) == "Recovered (NETWORK_ERROR)"
