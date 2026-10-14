"""Helpers for the Airflow-inspired overview page."""

from __future__ import annotations

import html
from datetime import datetime, timedelta
from typing import Any, Iterable

import streamlit as st

from monitoring import queries

TIME_WINDOW_SECONDS = {
    "Last 1 Hour": 60 * 60,
    "Last 6 Hours": 6 * 60 * 60,
    "Last 24 Hours": 24 * 60 * 60,
    "Last 7 Days": 7 * 24 * 60 * 60,
}


def parse_time_window(label: str) -> int:
    return TIME_WINDOW_SECONDS.get(label, TIME_WINDOW_SECONDS["Last 24 Hours"])


def _normalize_status(value: Any) -> str:
    status = value or "UNKNOWN"
    return str(status).upper()


def _row_timestamp(row: dict[str, Any]) -> datetime | None:
    for key in ("created_at", "start_time", "end_time"):
        value = row.get(key)
        if value:
            try:
                return datetime.fromisoformat(str(value).replace("Z", "+00:00"))
            except ValueError:
                try:
                    return datetime.strptime(str(value), "%Y-%m-%d %H:%M:%S")
                except ValueError:
                    return None
    return None


def filter_rows_by_window(rows: Iterable[dict[str, Any]], label: str) -> list[dict[str, Any]]:
    seconds = parse_time_window(label)
    cutoff = datetime.now() - timedelta(seconds=seconds)
    filtered: list[dict[str, Any]] = []
    for row in rows:
        timestamp = _row_timestamp(row)
        if timestamp and timestamp >= cutoff:
            filtered.append(row)
    return filtered


def get_latest_pipeline_status(rows: Iterable[dict[str, Any]], pipeline_name: str) -> str:
    matches = [
        row for row in rows if str(row.get("pipeline_name") or "").lower() == str(pipeline_name).lower()
    ]
    if not matches:
        return "UNKNOWN"

    latest = sorted(
        matches,
        key=lambda row: (
            int(row.get("execution_id", 0)) if row.get("execution_id") is not None else 0,
            _row_timestamp(row).timestamp() if _row_timestamp(row) else 0,
        ),
        reverse=True,
    )[0]
    return _normalize_status(latest.get("status"))


def get_status_label(status: str) -> str:
    status = _normalize_status(status)
    return {
        "SUCCESS": "Healthy",
        "FAILED": "Failed",
        "RUNNING": "Running",
        "RETRYING": "Retrying",
        "QUEUED": "Queued",
        "DISABLED": "Disabled",
        "UNKNOWN": "Unknown",
    }.get(status, "Unknown")


def get_pipeline_health(database, pipeline_names: list[str]) -> list[dict[str, Any]]:
    rows = queries.execution_history(database, limit=5000)
    health: list[dict[str, Any]] = []
    for name in pipeline_names:
        status = get_latest_pipeline_status(rows, name)
        health.append({
            "pipeline_name": name,
            "status": status,
            "label": get_status_label(status),
        })
    return health


def summarize_execution_counts(rows: Iterable[dict[str, Any]]) -> dict[str, int]:
    summary = {"SUCCESS": 0, "FAILED": 0, "RETRYING": 0, "RUNNING": 0, "QUEUED": 0, "TOTAL": 0}
    for row in rows:
        status = _normalize_status(row.get("status"))
        if status in summary:
            summary[status] += 1
        summary["TOTAL"] += 1
    return summary


# ------------------------------------------------------------------ #
#  Per-pipeline run counts (used by the Pipeline Runs section)        #
# ------------------------------------------------------------------ #

def build_pipeline_run_counts(
    rows: Iterable[dict[str, Any]],
    pipeline_names: list[str],
) -> list[dict[str, Any]]:
    """Build per-pipeline run counts with status breakdown and percentages.

    Returns a list of dicts, each containing:
      pipeline_name, total, queued, success, failed, running, retrying,
      queued_pct, success_pct, failed_pct, running_pct, retrying_pct
    """
    counts: dict[str, dict[str, int]] = {}
    for name in pipeline_names:
        counts[name] = {"SUCCESS": 0, "FAILED": 0, "RUNNING": 0, "RETRYING": 0, "QUEUED": 0, "TOTAL": 0}

    for row in rows:
        pname = str(row.get("pipeline_name") or "")
        status = _normalize_status(row.get("status"))
        if pname not in counts:
            counts[pname] = {"SUCCESS": 0, "FAILED": 0, "RUNNING": 0, "RETRYING": 0, "QUEUED": 0, "TOTAL": 0}
        if status in counts[pname]:
            counts[pname][status] += 1
        counts[pname]["TOTAL"] += 1

    result: list[dict[str, Any]] = []
    for name in sorted(counts):
        c = counts[name]
        total = c["TOTAL"]
        result.append({
            "pipeline_name": name,
            "total": total,
            "queued": c["QUEUED"],
            "success": c["SUCCESS"],
            "failed": c["FAILED"],
            "running": c["RUNNING"] + c["RETRYING"],
            "retrying": c["RETRYING"],
            "queued_pct": round(c["QUEUED"] / total * 100, 2) if total else 0.0,
            "success_pct": round(c["SUCCESS"] / total * 100, 2) if total else 0.0,
            "failed_pct": round(c["FAILED"] / total * 100, 2) if total else 0.0,
            "running_pct": round((c["RUNNING"] + c["RETRYING"]) / total * 100, 2) if total else 0.0,
            "retrying_pct": round(c["RETRYING"] / total * 100, 2) if total else 0.0,
        })
    return result


def render_bar(label: str, count: int, percentage: float, color: str, symbol: str) -> str:
    """Render an HTML progress bar for a status category with symbol, label, count, and percentage."""
    width = min(max(float(percentage), 0.0), 100.0)
    return (
        "<div style='margin-bottom:0.45rem;'>"
        "<div style='display:flex;justify-content:space-between;align-items:center;"
        "color:#F8FAFC;margin-bottom:0.18rem;font-size:0.84rem;'>"
        f"<span>{symbol} {html.escape(label)}</span><span>{count} &nbsp; {percentage:.1f}%</span></div>"
        "<div style='height:8px;background:#1E293B;border-radius:999px;overflow:hidden;'>"
        f"<div style='height:100%;width:{width}%;background:{color};border-radius:999px;'></div>"
        "</div></div>"
    )


def render_pipeline_runs(pipeline_run_counts: list[dict[str, Any]]) -> None:
    """Render individual pipeline cards with status counts and progress bars."""
    st.markdown("<div style='font-size:1.08rem; font-weight:700; margin-bottom: 0.65rem;'>Pipeline Runs</div>", unsafe_allow_html=True)
    if not pipeline_run_counts:
        st.markdown(
            "<div class='card' style='text-align:center; padding: 2rem 1rem; color: #94A3B8; margin-top: 0.5rem;'>"
            "<div style='font-size: 1.8rem; margin-bottom: 0.4rem;'>ℹ️</div>"
            "<div style='font-size: 1rem; font-weight: 600; color: #F8FAFC;'>No registered pipelines found</div>"
            "<div style='font-size: 0.85rem; margin-top: 0.2rem;'>Register a pipeline using the Add Pipeline page to start monitoring runs.</div>"
            "</div>",
            unsafe_allow_html=True,
        )
        return

    for pipeline in pipeline_run_counts:
        total = pipeline.get("total", 0)
        pname = pipeline.get("pipeline_name", "Unknown")
        queued = pipeline.get("queued", 0)
        queued_pct = pipeline.get("queued_pct", 0.0)
        running = pipeline.get("running", 0)
        running_pct = pipeline.get("running_pct", 0.0)
        success = pipeline.get("success", 0)
        success_pct = pipeline.get("success_pct", 0.0)
        failed = pipeline.get("failed", 0)
        failed_pct = pipeline.get("failed_pct", 0.0)

        card_html = (
            f"<div style='border:1px solid #1F2937;background:#0F172A;border-radius:10px;"
            "padding:0.75rem 0.85rem;margin-bottom:0.65rem;'>"
            f"<div style='font-weight:700;color:#F8FAFC;margin-bottom:0.55rem;'>{html.escape(pname)}"
            f"<span style='float:right;color:#94A3B8;font-size:0.8rem;'>{total} total</span></div>"
            + render_bar("Queued", queued, queued_pct, "#64748B", "·")
            + render_bar("Running", running, running_pct, "#06B6D4", "↻")
            + render_bar("Success", success, success_pct, "#22C55E", "✓")
            + render_bar("Failed", failed, failed_pct, "#EF4444", "✕")
            + "</div>"
        )
        st.markdown(card_html, unsafe_allow_html=True)


# ------------------------------------------------------------------ #
#  Task instance counts (used by the Task Instances section)          #
# ------------------------------------------------------------------ #

def build_task_instance_summary(
    task_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    """Aggregate task-instance query rows into a displayable summary.

    Returns:
        {
            "total": int,
            "by_status": {"SUCCESS": int, "FAILED": int, "SKIPPED": int},
            "by_pipeline": {
                "<pipeline_name>": {
                    "<task_name>": {"SUCCESS": int, "FAILED": int, "SKIPPED": int, "total": int},
                    ...
                }, ...
            }
        }
    """
    total = 0
    by_status: dict[str, int] = {"SUCCESS": 0, "FAILED": 0, "SKIPPED": 0}
    by_pipeline: dict[str, dict[str, dict[str, int]]] = {}

    for row in task_rows:
        pname = str(row.get("pipeline_name", ""))
        tname = str(row.get("task_name", ""))
        status = str(row.get("status", "")).upper()
        count = int(row.get("count", 0))

        total += count
        if status in by_status:
            by_status[status] += count

        by_pipeline.setdefault(pname, {})
        by_pipeline[pname].setdefault(tname, {"SUCCESS": 0, "FAILED": 0, "SKIPPED": 0, "total": 0})
        if status in by_pipeline[pname][tname]:
            by_pipeline[pname][tname][status] += count
        by_pipeline[pname][tname]["total"] += count

    return {
        "total": total,
        "by_status": by_status,
        "by_pipeline": by_pipeline,
    }


# ------------------------------------------------------------------ #
#  Main entry point used by the overview page                         #
# ------------------------------------------------------------------ #

def build_overview_summary(database, selected_window: str = "Last 24 Hours") -> dict[str, Any]:
    registered = queries.get_registered_pipelines(database)
    registration_by_name = {
        row["pipeline_name"]: row for row in registered
    }
    pipeline_names = sorted(registration_by_name.keys())

    all_rows = queries.execution_history(database, limit=5000)
    # Filter rows to only active, non-deleted pipelines and respect created_at for reused names
    active_rows: list[dict[str, Any]] = []
    for row in all_rows:
        pname = str(row.get("pipeline_name") or "")
        if pname in registration_by_name:
            reg_created = registration_by_name[pname].get("created_at")
            row_ts = _row_timestamp(row)
            if reg_created and row_ts:
                try:
                    reg_dt = datetime.fromisoformat(str(reg_created).replace("Z", "+00:00"))
                except Exception:
                    try:
                        reg_dt = datetime.strptime(str(reg_created), "%Y-%m-%d %H:%M:%S")
                    except Exception:
                        reg_dt = None
                if reg_dt and row_ts < reg_dt:
                    continue
            active_rows.append(row)

    recent_rows = filter_rows_by_window(active_rows, selected_window)
    counts = summarize_execution_counts(recent_rows)
    pipeline_health = get_pipeline_health(database, pipeline_names)
    for pipeline in pipeline_health:
        registration = registration_by_name.get(pipeline["pipeline_name"])
        pipeline["enabled"] = registration.get("enabled", True) if registration else True
        if not pipeline["enabled"]:
            pipeline["status"] = "DISABLED"
            pipeline["label"] = get_status_label("DISABLED")
    failed_pipelines = sum(1 for row in pipeline_health if row["status"] == "FAILED")
    running_pipelines = sum(1 for row in pipeline_health if row["status"] == "RUNNING")
    successful_pipelines = sum(1 for row in pipeline_health if row["status"] == "SUCCESS")

    total = counts["TOTAL"]
    success_pct = round((counts["SUCCESS"] / total) * 100, 1) if total else 0.0
    failed_pct = round((counts["FAILED"] / total) * 100, 1) if total else 0.0
    retrying_pct = round((counts["RETRYING"] / total) * 100, 1) if total else 0.0
    queued_pct = round((counts["QUEUED"] / total) * 100, 1) if total else 0.0

    # Per-pipeline run counts
    pipeline_run_counts = build_pipeline_run_counts(recent_rows, pipeline_names)

    # Task instance summary (time-window filtering via SQL cutoff)
    seconds = parse_time_window(selected_window)
    since_dt = datetime.now() - timedelta(seconds=seconds)
    since_str = since_dt.strftime("%Y-%m-%d %H:%M:%S")
    task_rows = queries.get_task_instances(database, since=since_str)
    task_summary = build_task_instance_summary(task_rows)

    return {
        "window": selected_window,
        "total_executions": total,
        "successful": counts["SUCCESS"],
        "failed": counts["FAILED"],
        "retrying": counts["RETRYING"],
        "running": counts["RUNNING"],
        "queued": counts["QUEUED"],
        "pipeline_health": pipeline_health,
        "failed_pipelines": failed_pipelines,
        "running_pipelines": running_pipelines,
        "successful_pipelines": successful_pipelines,
        "distribution": {
            "success_pct": success_pct,
            "failed_pct": failed_pct,
            "retrying_pct": retrying_pct,
            "queued_pct": queued_pct,
        },
        "pipeline_run_counts": pipeline_run_counts,
        "task_summary": task_summary,
        "recent_activity": recent_rows[:8],
        "since_str": since_str,
    }
