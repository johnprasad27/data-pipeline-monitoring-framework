"""Pipeline Runs page for the Data Pipeline Monitoring and Recovery Framework."""

from __future__ import annotations

from pathlib import Path

import streamlit as st

import config
from database.db import Database
from monitoring import queries
from monitoring.overview import (
    build_pipeline_run_counts,
    filter_rows_by_window,
    render_pipeline_runs,
)


def load_styles() -> None:
    css_path = Path(__file__).resolve().parents[1] / "monitoring" / "styles.css"
    if css_path.is_file():
        st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)


load_styles()

with st.sidebar:
    st.markdown(
        "<div style='font-size: 1.1rem; font-weight: 800; color: #F8FAFC; margin-bottom: 0.5rem;'>📊 Data Pipeline Monitor</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<hr style='border-top: 1px solid #334155; margin: 0.5rem 0 0.75rem 0;' />", unsafe_allow_html=True)
    st.markdown(
        "<div><strong>Status</strong>: <span style='color:#22C55E'>●</span> Database Connected</div>",
        unsafe_allow_html=True,
    )
    st.markdown("<hr style='border-top: 1px solid #334155; margin: 0.75rem 0;' />", unsafe_allow_html=True)
    st.caption("© Data Pipeline Monitoring")


@st.fragment(run_every=config.DASHBOARD_REFRESH_SECONDS)
def pipeline_runs_page() -> None:
    st.markdown("<h1 style='margin-bottom: 0.1rem;'>Pipeline Runs</h1>", unsafe_allow_html=True)
    st.markdown(
        "<div style='color: #94A3B8; margin-bottom: 1.25rem;'>"
        "Execution counts, status breakdown, and progress distribution across all pipelines."
        "</div>",
        unsafe_allow_html=True,
    )

    window_option = st.selectbox(
        "History Period",
        ["Last 1 Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days"],
        index=2,
        key="pipeline_runs_window_option",
    )

    db = Database()
    try:
        db.connect()
        registered = queries.get_registered_pipelines(db)
        all_rows = queries.execution_history(db, limit=5000)
    except Exception as exc:
        st.error(f"Unable to load pipeline runs data: {exc}")
        return
    finally:
        db.close()

    # Discover currently active registered pipelines
    registration_by_name = {r["pipeline_name"]: r for r in registered if r.get("pipeline_name")}
    pipeline_names = sorted(registration_by_name.keys())

    # Filter executions so only non-deleted pipelines and active incarnations appear
    active_rows: list[dict[str, Any]] = []
    for row in all_rows:
        pname = str(row.get("pipeline_name") or "")
        if pname in registration_by_name:
            reg_created = registration_by_name[pname].get("created_at")
            row_ts = row.get("created_at") or row.get("start_time")
            if reg_created and row_ts:
                try:
                    import pandas as pd
                    reg_dt = pd.to_datetime(reg_created)
                    r_dt = pd.to_datetime(row_ts)
                    if r_dt < reg_dt:
                        continue
                except Exception:
                    pass
            active_rows.append(row)

    # Filter executions by selected time window
    recent_rows = filter_rows_by_window(active_rows, window_option)
    pipeline_run_counts = build_pipeline_run_counts(recent_rows, pipeline_names)

    # Summary KPI row for the selected time window
    total_runs = len(recent_rows)
    success_runs = sum(1 for r in recent_rows if (r.get("status") or "").upper() == "SUCCESS")
    failed_runs = sum(1 for r in recent_rows if (r.get("status") or "").upper() == "FAILED")
    running_runs = sum(1 for r in recent_rows if (r.get("status") or "").upper() in {"RUNNING", "RETRYING", "QUEUED"})

    kpi_cols = st.columns(4)
    kpi_cols[0].markdown(
        "<div class='card' style='padding: 0.75rem 1rem; border-radius: 12px; background: #111827; border: 1px solid #1F2937;'>"
        "<div style='display:flex; align-items:center; gap:0.6rem;'><span style='font-size:1.2rem;'>📋</span>"
        "<div><div style='font-size:0.75rem; color:#94A3B8;'>Total Runs</div>"
        f"<div style='font-size:1.3rem; font-weight:700; color:#F8FAFC;'>{total_runs}</div></div></div></div>",
        unsafe_allow_html=True,
    )
    kpi_cols[1].markdown(
        "<div class='card' style='padding: 0.75rem 1rem; border-radius: 12px; background: #111827; border: 1px solid #1F2937;'>"
        "<div style='display:flex; align-items:center; gap:0.6rem;'><span style='font-size:1.2rem;'>🟢</span>"
        "<div><div style='font-size:0.75rem; color:#94A3B8;'>Successful</div>"
        f"<div style='font-size:1.3rem; font-weight:700; color:#22C55E;'>{success_runs}</div></div></div></div>",
        unsafe_allow_html=True,
    )
    kpi_cols[2].markdown(
        "<div class='card' style='padding: 0.75rem 1rem; border-radius: 12px; background: #111827; border: 1px solid #1F2937;'>"
        "<div style='display:flex; align-items:center; gap:0.6rem;'><span style='font-size:1.2rem;'>🔴</span>"
        "<div><div style='font-size:0.75rem; color:#94A3B8;'>Failed</div>"
        f"<div style='font-size:1.3rem; font-weight:700; color:#EF4444;'>{failed_runs}</div></div></div></div>",
        unsafe_allow_html=True,
    )
    kpi_cols[3].markdown(
        "<div class='card' style='padding: 0.75rem 1rem; border-radius: 12px; background: #111827; border: 1px solid #1F2937;'>"
        "<div style='display:flex; align-items:center; gap:0.6rem;'><span style='font-size:1.2rem;'>🔵</span>"
        "<div><div style='font-size:0.75rem; color:#94A3B8;'>In Flight</div>"
        f"<div style='font-size:1.3rem; font-weight:700; color:#06B6D4;'>{running_runs}</div></div></div></div>",
        unsafe_allow_html=True,
    )

    st.markdown("<div style='margin-top: 1.5rem;'></div>", unsafe_allow_html=True)

    # Render individual pipeline runs cards
    render_pipeline_runs(pipeline_run_counts)


pipeline_runs_page()
