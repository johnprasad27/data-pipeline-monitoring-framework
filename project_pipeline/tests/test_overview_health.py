from monitoring.overview import get_latest_pipeline_status, parse_time_window


def test_get_latest_pipeline_status_uses_latest_execution_not_old_failure():
    rows = [
        {
            "pipeline_name": "api_pipeline",
            "status": "FAILED",
            "execution_id": 10,
            "created_at": "2024-01-01T11:00:00",
        },
        {
            "pipeline_name": "api_pipeline",
            "status": "SUCCESS",
            "execution_id": 11,
            "created_at": "2024-01-01T13:00:00",
        },
    ]

    assert get_latest_pipeline_status(rows, "api_pipeline") == "SUCCESS"


def test_parse_time_window_matches_selected_periods():
    assert parse_time_window("Last 1 Hour") == 3600
    assert parse_time_window("Last 6 Hours") == 21600
    assert parse_time_window("Last 24 Hours") == 86400
    assert parse_time_window("Last 7 Days") == 604800
