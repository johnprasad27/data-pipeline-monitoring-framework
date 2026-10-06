"""Continuous scheduler for the API and CSV pipelines."""

from __future__ import annotations

import logging
import threading
import time
from pathlib import Path

import config
from database.db import Database
from pipelines.api_pipeline import run_api_pipeline
from pipelines.csv_pipeline import run_csv_pipeline
from pipelines.recovery import run_with_recovery

logger = logging.getLogger(__name__)


def prepare_database() -> None:
    """Apply additive schema changes before scheduled work begins."""
    db = Database()
    try:
        db.connect()
        db.ensure_monitoring_schema()
    finally:
        db.close()


def run_once() -> None:
    """Run both configured pipelines once, isolating failures per pipeline."""
    run_with_recovery("api_pipeline", run_api_pipeline)
    run_with_recovery(
        "csv_pipeline",
        run_csv_pipeline,
        Path(config.CSV_PIPELINE_PATH),
    )


def run_forever(stop_event: threading.Event | None = None) -> None:
    """Run immediately, then repeat at the configured interval."""
    prepare_database()
    stop_event = stop_event or threading.Event()
    logger.info(
        "continuous execution started; interval=%s seconds",
        config.PIPELINE_INTERVAL_SECONDS,
    )

    while not stop_event.is_set():
        started_at = time.monotonic()
        try:
            run_once()
        except Exception:
            logger.exception("scheduler cycle failed; continuing")

        elapsed = time.monotonic() - started_at
        wait_seconds = max(0, config.PIPELINE_INTERVAL_SECONDS - elapsed)
        stop_event.wait(wait_seconds)
