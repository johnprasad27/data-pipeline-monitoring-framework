"""Continuous scheduler for configured pipeline executions."""

from __future__ import annotations

import logging
import threading
import time

import config
from database.db import Database
from monitoring.execution import monitor_pipeline_runner
from pipelines.registry import (
    get_enabled_pipeline_configs,
    resolve_pipeline_runner,
    seed_pipeline_registry,
)
from pipelines.recovery import run_with_recovery

logger = logging.getLogger(__name__)


def prepare_database() -> None:
    """Apply additive schema changes before scheduled work begins."""
    db = Database()
    try:
        db.connect()
        db.ensure_monitoring_schema()
        seed_pipeline_registry(db)
    finally:
        db.close()


def _run_pipeline(pipeline: dict) -> None:
    pipeline_name = pipeline["name"]
    runner = resolve_pipeline_runner(
        pipeline["module"],
        pipeline.get("handler_function"),
    )
    if not getattr(runner, "records_execution_history", False):
        runner = monitor_pipeline_runner(runner, pipeline["pipeline_type"])
    run_with_recovery(
        pipeline_name,
        runner,
        max_retries=int(pipeline["max_retries"]),
        runner_kwargs={
            "pipeline_name": pipeline_name,
            "source_config": pipeline["source_config"],
        },
    )


def run_once() -> None:
    """Run each enabled pipeline once, isolating failures per pipeline."""
    for pipeline in get_enabled_pipeline_configs():
        try:
            _run_pipeline(pipeline)
        except Exception:
            logger.exception("pipeline execution failed: %s", pipeline.get("name", "unknown"))


def run_forever(stop_event: threading.Event | None = None) -> None:
    """Run immediately, then repeat at the configured interval."""
    prepare_database()
    stop_event = stop_event or threading.Event()
    logger.info("continuous execution started; pipeline intervals come from the registry")
    next_run_at: dict[str, float] = {}

    while not stop_event.is_set():
        try:
            pipelines = get_enabled_pipeline_configs()
            active_names = {pipeline["name"] for pipeline in pipelines}
            next_run_at = {
                name: timestamp
                for name, timestamp in next_run_at.items()
                if name in active_names
            }
            now = time.monotonic()
            for pipeline in pipelines:
                name = pipeline["name"]
                if next_run_at.get(name, 0) <= now:
                    try:
                        _run_pipeline(pipeline)
                    except Exception:
                        logger.exception("pipeline execution failed: %s", name)
                    next_run_at[name] = now + max(1, int(pipeline["interval_seconds"]))
        except Exception:
            logger.exception("scheduler registry refresh failed; continuing")

        stop_event.wait(2)
