import json

from database.db import Database


def test_status_notification_payload_includes_execution_metadata():
    payload = Database.build_status_notification(
        pipeline_name="api_pipeline",
        status="SUCCESS",
        execution_id=42,
        retry_count=2,
    )

    data = json.loads(payload)
    assert data["pipeline_name"] == "api_pipeline"
    assert data["status"] == "SUCCESS"
    assert data["execution_id"] == 42
    assert data["retry_count"] == 2
