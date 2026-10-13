from monitoring.overview import (
    build_pipeline_run_counts,
    build_task_instance_summary,
    get_latest_pipeline_status,
    parse_time_window,
)


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


def test_pipeline_run_counts_group_registered_names_and_percentages():
    rows = [
        {"pipeline_name": "weather_ingest", "status": "SUCCESS"},
        {"pipeline_name": "weather_ingest", "status": "FAILED"},
        {"pipeline_name": "weather_ingest", "status": "RUNNING"},
        {"pipeline_name": "weather_ingest", "status": "RETRYING"},
        {"pipeline_name": "orders", "status": "QUEUED"},
    ]

    counts = build_pipeline_run_counts(rows, ["weather_ingest", "orders", "empty"])

    weather = next(item for item in counts if item["pipeline_name"] == "weather_ingest")
    assert weather["total"] == 4
    assert weather["success"] == 1
    assert weather["failed"] == 1
    assert weather["running"] == 2
    assert weather["running_pct"] == 50.0

    empty = next(item for item in counts if item["pipeline_name"] == "empty")
    assert empty["total"] == 0
    assert empty["success_pct"] == 0.0


def test_task_instance_summary_preserves_pipeline_and_stage_groups():
    summary = build_task_instance_summary(
        [
            {"pipeline_name": "api_pipeline", "task_name": "Extract", "status": "SUCCESS", "count": 2},
            {"pipeline_name": "api_pipeline", "task_name": "Validate", "status": "FAILED", "count": 1},
            {"pipeline_name": "csv_pipeline", "task_name": "Load", "status": "SKIPPED", "count": 3},
        ]
    )

    assert summary["total"] == 6
    assert summary["by_status"] == {"SUCCESS": 2, "FAILED": 1, "SKIPPED": 3}
    assert summary["by_pipeline"]["api_pipeline"]["Validate"]["FAILED"] == 1
    assert summary["by_pipeline"]["csv_pipeline"]["Load"]["SKIPPED"] == 3
