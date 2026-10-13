"""Execution History page for the Data Pipeline Monitoring and Recovery Framework."""

from __future__ import annotations

import html
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd
import streamlit as st

import config
from database.db import Database
from monitoring import metrics, queries
from monitoring.dashboard import format_duration, format_failure_category


def load_styles() -> None:
    css_path = Path(__file__).resolve().parents[1] / "monitoring" / "styles.css"
    if css_path.is_file():
        st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)


load_styles()

with st.sidebar:
    st.markdown(
        "<div class='sidebar-brand'>📊 <strong>Data Pipeline Monitor</strong></div>",
        unsafe_allow_html=True,
    )
    st.divider()
    st.markdown(
        "<div><strong>Status</strong>: <span style='color:#22C55E'>●</span> Database Connected</div>",
        unsafe_allow_html=True,
    )
    st.divider()
    st.caption("© Data Pipeline Monitoring")


@st.fragment(run_every=config.DASHBOARD_REFRESH_SECONDS)
def execution_history_page() -> None:
    st.markdown("<h1 style='margin-bottom: 0.1rem;'>Execution History</h1>", unsafe_allow_html=True)
    st.markdown(
        "<div style='color: #94A3B8; margin-bottom: 1.25rem;'>"
        "Audit log of pipeline executions with filtering by pipeline, status, and time range."
        "</div>",
        unsafe_allow_html=True,
    )

    db = Database()
    try:
        db.connect()
    except Exception as exc:
        st.error(f"Unable to connect to database: {exc}")
        return

    try:
        # Discover all available pipeline names for filter
        registered_pipelines = queries.get_registered_pipelines(db)
        registered_names = {p["pipeline_name"] for p in registered_pipelines}
        historical_executions = queries.execution_history(db, limit=200)
        history_names = {r["pipeline_name"] for r in historical_executions if r.get("pipeline_name")}
        all_pipeline_names = sorted(registered_names | history_names)

        # Filter controls
        f1, f2, f3, f4 = st.columns([1.5, 1.2, 1.3, 1])
        with f1:
            pipeline_filter = st.selectbox(
                "Pipeline",
                ["All Pipelines", *all_pipeline_names],
                index=0,
            )
        with f2:
            status_filter = st.selectbox(
                "Status",
                ["All Statuses", "SUCCESS", "FAILED", "RUNNING", "RETRYING", "QUEUED"],
                index=0,
            )
        with f3:
            time_filter = st.selectbox(
                "Time Window",
                ["All Time", "Last 1 Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days", "Last 30 Days"],
                index=0,
            )
        with f4:
            limit_filter = st.selectbox(
                "Max Rows",
                [50, 100, 250, 500],
                index=1,
            )

        # Calculate time cutoff if selected
        now = datetime.now()
        since_dt: datetime | None = None
        if time_filter == "Last 1 Hour":
            since_dt = now - timedelta(hours=1)
        elif time_filter == "Last 6 Hours":
            since_dt = now - timedelta(hours=6)
        elif time_filter == "Last 24 Hours":
            since_dt = now - timedelta(hours=24)
        elif time_filter == "Last 7 Days":
            since_dt = now - timedelta(days=7)
        elif time_filter == "Last 30 Days":
            since_dt = now - timedelta(days=30)

        selected_pipeline = None if pipeline_filter == "All Pipelines" else pipeline_filter
        selected_status = None if status_filter == "All Statuses" else status_filter
        since_str = since_dt.strftime("%Y-%m-%d %H:%M:%S") if since_dt else None

        records = metrics.get_execution_history(
            db,
            limit=limit_filter,
            pipeline_name=selected_pipeline,
            status=selected_status,
            since=since_str,
        )

        # Summary KPIs
        total_runs = len(records)
        success_runs = sum(1 for r in records if (r.get("status") or "").upper() == "SUCCESS")
        failed_runs = sum(1 for r in records if (r.get("status") or "").upper() == "FAILED")
        total_rows = sum((r.get("rows_processed") or 0) for r in records)

        kpi_cols = st.columns(4)
        kpi_cols[0].markdown(
            f"<div class='card kpi-card' style='width:100%'><div class='kpi-inline'>"
            f"<div class='icon'>📋</div><div class='meta'>"
            f"<div class='title'>Filtered Runs</div><div class='value'>{total_runs}</div>"
            f"</div></div></div>",
            unsafe_allow_html=True,
        )
        kpi_cols[1].markdown(
            f"<div class='card kpi-card' style='width:100%'><div class='kpi-inline'>"
            f"<div class='icon'>✅</div><div class='meta'>"
            f"<div class='title'>Successful</div><div class='value' style='color:#22C55E'>{success_runs}</div>"
            f"</div></div></div>",
            unsafe_allow_html=True,
        )
        kpi_cols[2].markdown(
            f"<div class='card kpi-card' style='width:100%'><div class='kpi-inline'>"
            f"<div class='icon'>❌</div><div class='meta'>"
            f"<div class='title'>Failed</div><div class='value' style='color:#EF4444'>{failed_runs}</div>"
            f"</div></div></div>",
            unsafe_allow_html=True,
        )
        kpi_cols[3].markdown(
            f"<div class='card kpi-card' style='width:100%'><div class='kpi-inline'>"
            f"<div class='icon'>📊</div><div class='meta'>"
            f"<div class='title'>Rows Processed</div><div class='value'>{total_rows:,}</div>"
            f"</div></div></div>",
            unsafe_allow_html=True,
        )

        st.markdown("<div class='spacer'></div>", unsafe_allow_html=True)

        if not records:
            st.markdown(
                "<div class='card' style='text-align:center; padding: 2.5rem 1rem; color: #94A3B8; margin-top: 1rem;'>"
                "<div style='font-size: 2rem; margin-bottom: 0.5rem;'>🔍</div>"
                "<div style='font-size: 1.1rem; font-weight: 600; color: #F8FAFC;'>No executions match the selected filters</div>"
                "<div style='font-size: 0.85rem; margin-top: 0.25rem;'>Try selecting a different pipeline, status, or date range.</div>"
                "</div>",
                unsafe_allow_html=True,
            )
            return

        table_rows: list[dict[str, Any]] = []
        for r in records:
            status_val = (r.get("status") or "").upper()
            retries = r.get("retry_count") if r.get("retry_count") is not None else 0
            failure_cat = format_failure_category(
                status_val,
                r.get("failure_category") or r.get("failure_type"),
                retries,
            )

            # Preserve error details only for failed/retrying executions
            if status_val == "SUCCESS":
                error_details = "—"
            elif status_val == "FAILED":
                error_details = r.get("error_message") or "Execution failed"
            elif status_val == "RETRYING":
                error_details = r.get("error_message") or "Retrying..."
            else:
                error_details = "—"

            table_rows.append({
                "Pipeline": r.get("pipeline_name") or "Unknown",
                "Status": status_val,
                "Rows": r.get("rows_processed") if r.get("rows_processed") is not None else 0,
                "Duration": format_duration(r),
                "Failure Category": failure_cat,
                "Error Details": error_details,
                "Execution Time": str(r.get("created_at") or r.get("start_time") or ""),
            })

        st.subheader("Executions")
        st.dataframe(pd.DataFrame(table_rows), hide_index=True, width="stretch")

        failed_in_view = [r for r in records if (r.get("status") or "").upper() == "FAILED"]
        if failed_in_view:
            with st.expander(f"⚠️ View Error Logs ({len(failed_in_view)} failed runs in view)", expanded=False):
                for f in failed_in_view[:20]:
                    exec_id = f.get("execution_id")
                    exec_label = f"Execution #{exec_id} &nbsp; " if exec_id else ""
                    cat = f.get("failure_category") or f.get("failure_type") or "UNKNOWN"
                    error_msg = f.get("error_message") or "No error message recorded"
                    st.markdown(
                        f"<div class='card' style='margin-bottom:0.6rem;'>"
                        f"<div style='display:flex; justify-content:space-between; align-items:center;'>"
                        f"<div><strong>{f.get('pipeline_name')}</strong> <span class='status-pill status-failed' style='margin-left:6px; font-size:10px;'>FAILED</span></div>"
                        f"<div class='muted small'>{exec_label}{f.get('created_at')}</div>"
                        f"</div>"
                        f"<div style='margin-top:0.4rem; font-size:0.85rem;'><strong>Failure Category:</strong> {cat}</div>"
                        f"<div style='color:var(--danger); margin-top:0.3rem; font-family:monospace; font-size:0.82rem; background:rgba(239,68,68,0.08); padding:6px 10px; border-radius:6px; border:1px solid rgba(239,68,68,0.2); word-break:break-all;'>{html.escape(str(error_msg))}</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )

    finally:
        db.close()


execution_history_page()
