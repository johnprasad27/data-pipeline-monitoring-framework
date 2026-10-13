"""Airflow-inspired overview page for the pipeline monitoring framework."""

from __future__ import annotations

import html
from datetime import datetime

import pandas as pd
import streamlit as st

import config
from database.db import Database
from monitoring.overview import build_overview_summary, parse_time_window


st.set_page_config(page_title="Overview", layout="wide")


def _badge_color(status: str) -> str:
    mapping = {
        "SUCCESS": "#22C55E",
        "FAILED": "#EF4444",
        "RUNNING": "#3B82F6",
        "RETRYING": "#F59E0B",
        "UNKNOWN": "#94A3B8",
    }
    return mapping.get(str(status).upper(), "#94A3B8")


def _status_tag(status: str) -> str:
    label = str(status).upper()
    color = _badge_color(label)
    return f"<span style='display:inline-block;padding:4px 10px;border-radius:9999px;font-size:11px;font-weight:700;background:{color}22;color:{color};border:1px solid {color}55'>{label}</span>"


def _display_timestamp(row: dict) -> str:
    for key in ("created_at", "start_time", "end_time"):
        value = row.get(key)
        if value:
            return str(value)
    return "-"


with st.sidebar:
    st.markdown("<div style='font-size: 1.1rem; font-weight: 800; color: #F8FAFC; margin-bottom: 0.5rem;'>📊 Data Pipeline Monitor</div>", unsafe_allow_html=True)
    st.markdown("<hr style='border-top: 1px solid #334155; margin: 0.5rem 0 0.75rem 0;' />", unsafe_allow_html=True)
    st.page_link("pages/overview.py", label="🏠 Overview", icon="🏠")
    st.page_link("monitoring/dashboard.py", label="📊 Pipeline Monitor", icon="📊")
    st.page_link("pages/add_pipeline.py", label="Add Pipeline", icon=":material/add_circle:")
    st.markdown("<hr style='border-top: 1px solid #334155; margin: 0.75rem 0;' />", unsafe_allow_html=True)
    st.markdown("<div><strong>Status</strong>: <span style='color:#22C55E'>●</span> Database Connected</div>", unsafe_allow_html=True)
    st.caption("© Data Pipeline Monitoring")


@st.fragment(run_every=config.DASHBOARD_REFRESH_SECONDS)
def overview_page() -> None:
    st.markdown("<h1 style='margin-bottom: 0.1rem;'>Data Pipeline Monitor</h1>", unsafe_allow_html=True)
    st.markdown("<div style='color: #94A3B8; margin-bottom: 1.2rem;'>Overview</div>", unsafe_allow_html=True)
    st.markdown("<div style='color: #CBD5E1; margin-bottom: 1.25rem;'>System health and pipeline execution overview</div>", unsafe_allow_html=True)

    window_option = st.selectbox(
        "History Period",
        ["Last 1 Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days"],
        index=2,
    )

    db = Database()
    try:
        db.connect()
        summary = build_overview_summary(db, window_option)
    except Exception as exc:
        st.error(f"Unable to load overview data: {exc}")
        return
    finally:
        db.close()

    status_cols = st.columns(3)
    status_cols[0].markdown(
        "<div class='card' style='padding: 0.75rem 1rem; border-radius: 12px; background: #111827; border: 1px solid #1F2937;'><div style='display:flex; align-items:center; gap:0.6rem;'><span style='font-size:1.2rem;'>🔴</span><div><div style='font-size:0.75rem; color:#94A3B8;'>Failed Pipelines</div><div style='font-size:1.3rem; font-weight:700; color:#F8FAFC;'>" + str(summary["failed_pipelines"]) + "</div></div></div></div>",
        unsafe_allow_html=True,
    )
    status_cols[1].markdown(
        "<div class='card' style='padding: 0.75rem 1rem; border-radius: 12px; background: #111827; border: 1px solid #1F2937;'><div style='display:flex; align-items:center; gap:0.6rem;'><span style='font-size:1.2rem;'>🔵</span><div><div style='font-size:0.75rem; color:#94A3B8;'>Running Pipelines</div><div style='font-size:1.3rem; font-weight:700; color:#F8FAFC;'>" + str(summary["running_pipelines"]) + "</div></div></div></div>",
        unsafe_allow_html=True,
    )
    status_cols[2].markdown(
        "<div class='card' style='padding: 0.75rem 1rem; border-radius: 12px; background: #111827; border: 1px solid #1F2937;'><div style='display:flex; align-items:center; gap:0.6rem;'><span style='font-size:1.2rem;'>🟢</span><div><div style='font-size:0.75rem; color:#94A3B8;'>Successful Pipelines</div><div style='font-size:1.3rem; font-weight:700; color:#F8FAFC;'>" + str(summary["successful_pipelines"]) + "</div></div></div></div>",
        unsafe_allow_html=True,
    )

    left, right = st.columns([2, 1])

    with left:
        st.markdown("<div style='font-size:1.08rem; font-weight:700; margin-top: 1.5rem; margin-bottom: 0.65rem;'>Pipeline Health</div>", unsafe_allow_html=True)
        for item in summary["pipeline_health"]:
            status = item["status"]
            color = _badge_color(status)
            st.markdown(
                f"<div style='display:flex; justify-content:space-between; align-items:center; border:1px solid #1F2937; background:#0F172A; border-radius:10px; padding:0.55rem 0.8rem; margin-bottom:0.5rem;'><div style='font-weight:600; color:#F8FAFC;'>{item['pipeline_name']}</div><span style='display:inline-flex; align-items:center; gap:0.45rem; padding:4px 10px; border-radius:9999px; border:1px solid {color}55; background:{color}22; color:{color}; font-size:11px; font-weight:700;'>{'🟢' if status == 'SUCCESS' else '🔴' if status == 'FAILED' else '🔵' if status == 'RUNNING' else '🟡' if status == 'RETRYING' else '⚪'} {item['label']}</span></div>",
                unsafe_allow_html=True,
            )

        st.markdown("<div style='font-size:1.08rem; font-weight:700; margin-top: 1.2rem; margin-bottom: 0.65rem;'>System Component Health</div>", unsafe_allow_html=True)
        components = [
            ("Database", "Connected" if db.connection and not db.connection.closed else "Unavailable"),
            ("Scheduler", "Running" if summary["running"] > 0 or summary["total_executions"] > 0 else "Idle"),
            ("API Pipeline", next((item["label"] for item in summary["pipeline_health"] if item["pipeline_name"] == "api_pipeline"), "Unknown")),
            ("CSV Pipeline", next((item["label"] for item in summary["pipeline_health"] if item["pipeline_name"] == "csv_pipeline"), "Unknown")),
            ("Recovery System", "Active" if summary["retrying"] > 0 or summary["failed"] > 0 else "Idle"),
            ("Monitoring", "Active" if db.connection and not db.connection.closed else "Unavailable"),
        ]
        for name, state in components:
            state_color = "#22C55E" if state in {"Connected", "Running", "Healthy", "Available", "Active"} else "#F59E0B" if state in {"Retrying", "Idle"} else "#EF4444"
            st.markdown(
                f"<div style='display:flex; justify-content:space-between; align-items:center; border:1px solid #1F2937; background:#0F172A; border-radius:10px; padding:0.55rem 0.75rem; margin-bottom:0.45rem;'><div style='color:#F8FAFC; font-weight:600;'>{name}</div><span style='display:inline-flex; align-items:center; gap:0.45rem; padding:4px 10px; border-radius:9999px; background:{state_color}22; color:{state_color}; border:1px solid {state_color}55; font-size:11px; font-weight:700;'>● {state}</span></div>",
                unsafe_allow_html=True,
            )

        st.markdown("<div style='font-size:1.08rem; font-weight:700; margin-top: 1.2rem; margin-bottom: 0.65rem;'>Execution Summary</div>", unsafe_allow_html=True)
        stat_rows = [
            ("Total Executions", summary["total_executions"]),
            ("Successful", summary["successful"]),
            ("Failed", summary["failed"]),
            ("Retrying", summary["retrying"]),
            ("Running", summary["running"]),
        ]
        for label, value in stat_rows:
            st.markdown(
                f"<div style='display:flex; justify-content:space-between; align-items:center; border:1px solid #1F2937; background:#0F172A; border-radius:10px; padding:0.55rem 0.75rem; margin-bottom:0.45rem;'><div style='color:#CBD5E1;'>{label}</div><div style='font-weight:700; color:#F8FAFC;'>{value}</div></div>",
                unsafe_allow_html=True,
            )

        st.markdown("<div style='font-size:1.08rem; font-weight:700; margin-top: 1.2rem; margin-bottom: 0.65rem;'>Pipeline Runs</div>", unsafe_allow_html=True)
        distribution = [
            ("Success", summary["successful"], summary["distribution"]["success_pct"]),
            ("Failed", summary["failed"], summary["distribution"]["failed_pct"]),
            ("Retrying", summary["retrying"], summary["distribution"]["retrying_pct"]),
        ]
        for label, count, pct in distribution:
            if label == "Success":
                symbol = "✓"
                color = "#22C55E"
            elif label == "Failed":
                symbol = "✕"
                color = "#EF4444"
            else:
                symbol = "↻"
                color = "#F59E0B"
            st.markdown(
                f"<div style='margin-bottom:0.5rem;'><div style='display:flex; justify-content:space-between; align-items:center; color:#F8FAFC; margin-bottom:0.2rem; font-size:0.92rem;'><span>{symbol} {label}</span><span>{count} &nbsp; {pct:.1f}%</span></div><div style='height:9px; background:#1E293B; border-radius:999px; overflow:hidden;'><div style='height:100%; width:{min(max(pct, 0.0), 100.0)}%; background:{color}; border-radius:999px;'></div></div></div>",
                unsafe_allow_html=True,
            )

    with right:
        st.markdown("<div style='font-size:1.08rem; font-weight:700; margin-top: 1.5rem; margin-bottom: 0.65rem;'>Recent Activity</div>", unsafe_allow_html=True)
        for row in summary["recent_activity"]:
            status = str(row.get("status") or "UNKNOWN").upper()
            text = row.get("error_message") or f"{row.get('rows_processed') or 0} rows processed"
            failure_category = row.get("failure_category") or row.get("failure_type")
            category_label = (
                f"<div style='color:#F59E0B; margin-top:0.3rem; font-size:0.8rem;'>Failure category: {html.escape(str(failure_category))}</div>"
                if failure_category
                else ""
            )
            st.markdown(
                f"<div style='background:#0F172A; border:1px solid #1F2937; border-radius:10px; padding:0.7rem 0.8rem; margin-bottom:0.5rem;'><div style='color:#94A3B8; font-size:0.76rem;'>{_display_timestamp(row)}</div><div style='font-weight:700; margin-top:0.2rem; color:#F8FAFC;'>{row.get('pipeline_name') or 'Unknown'}</div><div style='margin-top:0.2rem;'>{_status_tag(status)}</div>{category_label}<div style='color:#CBD5E1; margin-top:0.4rem; font-size:0.85rem;'>{html.escape(str(text))}</div></div>",
                unsafe_allow_html=True,
            )


overview_page()
