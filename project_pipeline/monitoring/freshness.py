"""Helpers for freshness and health monitoring."""

from __future__ import annotations

from datetime import datetime
from typing import Any

import config


def parse_datetime(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        return value
    if isinstance(value, str):
        cleaned = value.replace("Z", "+00:00")
        try:
            return datetime.fromisoformat(cleaned)
        except ValueError:
            try:
                return datetime.strptime(value, "%Y-%m-%d %H:%M:%S")
            except ValueError:
                return None
    return None


def get_freshness_status(
    last_success_time: Any,
    now: datetime | None = None,
    fresh_seconds: int | None = None,
    warning_seconds: int | None = None,
) -> str:
    """Return simple freshness state for a pipeline based on its last successful execution."""
    if last_success_time is None:
        return "STALE"

    ts = parse_datetime(last_success_time)
    if ts is None:
        return "STALE"

    fresh_seconds = config.DATA_FRESHNESS_FRESH_SECONDS if fresh_seconds is None else fresh_seconds
    warning_seconds = config.DATA_FRESHNESS_WARNING_SECONDS if warning_seconds is None else warning_seconds
    reference = now or datetime.now()
    age_seconds = max(0.0, (reference - ts).total_seconds())
    if age_seconds < fresh_seconds:
        return "FRESH"
    if age_seconds <= warning_seconds:
        return "WARNING"
    return "STALE"


def calculate_pipeline_health(status: str, freshness_status: str, data_quality_score: float | int = 100.0, retry_count: int = 0) -> float:
    """Simple weighted health score from status, freshness, data quality and retry count."""
    status_weight = {
        "SUCCESS": 55,
        "FAILED": 10,
        "RUNNING": 35,
        "RETRYING": 25,
        "UNKNOWN": 0,
    }.get((status or "UNKNOWN").upper(), 0)

    freshness_weight = {
        "FRESH": 25,
        "WARNING": 15,
        "STALE": 5,
    }.get((freshness_status or "STALE").upper(), 5)

    quality_weight = max(0.0, float(data_quality_score) * 0.2)
    retry_penalty = min(20, max(0, int(retry_count)) * 5)
    score = status_weight + freshness_weight + quality_weight - retry_penalty
    return round(max(0.0, min(100.0, score)), 2)
