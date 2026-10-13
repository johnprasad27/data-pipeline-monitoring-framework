import sys
from types import ModuleType
from datetime import datetime, timedelta
from unittest.mock import Mock

import pytest

from api.api_client import APIError
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
