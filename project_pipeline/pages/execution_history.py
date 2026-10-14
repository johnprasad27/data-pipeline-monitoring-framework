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
from monitoring.dashboard import format_duration


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

    # Initialize session state for interactive status filter
    if "history_status_filter" not in st.session_state:
        st.session_state["history_status_filter"] = "All Statuses"

    def _on_status_card_click(target_status: str) -> None:
        """Update the status filter in session state before widget re-instantiation."""
        st.session_state["history_status_filter"] = target_status

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
            current_pipeline_filter = st.session_state.get("history_pipeline_filter", "All Pipelines")
            if current_pipeline_filter not in ["All Pipelines", *all_pipeline_names]:
                st.session_state["history_pipeline_filter"] = "All Pipelines"
            pipeline_filter = st.selectbox(
                "Pipeline",
                ["All Pipelines", *all_pipeline_names],
                key="history_pipeline_filter",
            )
        with f2:
            status_filter = st.selectbox(
                "Status",
                ["All Statuses", "SUCCESS", "FAILED", "RUNNING", "RETRYING", "QUEUED"],
                key="history_status_filter",
            )
        with f3:
            time_filter = st.selectbox(
                "Time Window",
                ["All Time", "Last 1 Hour", "Last 6 Hours", "Last 24 Hours", "Last 7 Days", "Last 30 Days"],
                key="history_time_filter",
            )
        with f4:
            limit_filter = st.selectbox(
                "Max Rows",
                [50, 100, 250, 500],
                key="history_limit_filter",
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

        # Summary KPIs calculated from the active filtered dataset
        total_runs = len(records)
        success_runs = sum(1 for r in records if (r.get("status") or "").upper() == "SUCCESS")
        failed_runs = sum(1 for r in records if (r.get("status") or "").upper() == "FAILED")
        total_rows = sum((r.get("rows_processed") or 0) for r in records)

        # Visual highlighting states
        current_status = st.session_state.get("history_status_filter", "All Statuses")
        is_all_active = current_status in (None, "All Statuses")
        is_success_active = current_status == "SUCCESS"
        is_failed_active = current_status == "FAILED"

        # Embedded styles scoped strictly to summary card buttons and info card
        active_border_css = ""
        if is_all_active:
            active_border_css = """
            .st-key-btn_kpi_all button {
                border: 1.5px solid #3B82F6 !important;
                box-shadow: 0 0 16px rgba(59, 130, 246, 0.45) !important;
            }
            """
        elif is_success_active:
            active_border_css = """
            .st-key-btn_kpi_success button {
                border: 1.5px solid #22C55E !important;
                box-shadow: 0 0 16px rgba(34, 197, 94, 0.45) !important;
            }
            """
        elif is_failed_active:
            active_border_css = """
            .st-key-btn_kpi_failed button {
                border: 1.5px solid #EF4444 !important;
                box-shadow: 0 0 16px rgba(239, 68, 68, 0.45) !important;
            }
            """

        card_styles = f"""
        <style>
        /* Responsive flex row container for summary cards */
        div[data-testid="stHorizontalBlock"]:has(.st-key-btn_kpi_all) {{
            display: flex !important;
            flex-wrap: wrap !important;
            gap: 12px !important;
            align-items: stretch !important;
        }}
        div[data-testid="stHorizontalBlock"]:has(.st-key-btn_kpi_all) > div[data-testid="stColumn"] {{
            min-width: 180px !important;
            flex: 1 1 180px !important;
        }}

        /* Clickable summary card buttons */
        .st-key-btn_kpi_all button,
        .st-key-btn_kpi_success button,
        .st-key-btn_kpi_failed button {{
            width: 100% !important;
            min-height: 84px !important;
            height: 84px !important;
            padding: 12px 16px !important;
            border-radius: 12px !important;
            background: linear-gradient(180deg, #0F172A, #0B1625) !important;
            border: 1px solid rgba(255, 255, 255, 0.08) !important;
            box-shadow: 0 4px 12px rgba(2, 6, 23, 0.4) !important;
            display: flex !important;
            flex-direction: row !important;
            align-items: center !important;
            justify-content: flex-start !important;
            text-align: left !important;
            gap: 12px !important;
            cursor: pointer !important;
            transition: all 0.2s cubic-bezier(0.4, 0, 0.2, 1) !important;
            box-sizing: border-box !important;
        }}

        .st-key-btn_kpi_all button:hover,
        .st-key-btn_kpi_success button:hover,
        .st-key-btn_kpi_failed button:hover {{
            border-color: rgba(255, 255, 255, 0.25) !important;
            transform: translateY(-2px);
            box-shadow: 0 8px 24px rgba(2, 6, 23, 0.8) !important;
        }}

        .st-key-btn_kpi_all button:active,
        .st-key-btn_kpi_success button:active,
        .st-key-btn_kpi_failed button:active {{
            transform: translateY(0);
        }}

        .st-key-btn_kpi_all button:focus,
        .st-key-btn_kpi_success button:focus,
        .st-key-btn_kpi_failed button:focus {{
            outline: none !important;
        }}

        /* Button icon styling */
        .st-key-btn_kpi_all button [data-testid="stIconMaterial"],
        .st-key-btn_kpi_all button span:first-child,
        .st-key-btn_kpi_success button [data-testid="stIconMaterial"],
        .st-key-btn_kpi_success button span:first-child,
        .st-key-btn_kpi_failed button [data-testid="stIconMaterial"],
        .st-key-btn_kpi_failed button span:first-child {{
            font-size: 20px !important;
            background: rgba(255, 255, 255, 0.05);
            padding: 8px 10px;
            border-radius: 8px;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            line-height: 1 !important;
            margin: 0 !important;
            flex-shrink: 0 !important;
        }}

        /* Button text layout */
        .st-key-btn_kpi_all div[data-testid="stMarkdownContainer"],
        .st-key-btn_kpi_success div[data-testid="stMarkdownContainer"],
        .st-key-btn_kpi_failed div[data-testid="stMarkdownContainer"] {{
            display: flex !important;
            flex-direction: column !important;
            align-items: flex-start !important;
            justify-content: center !important;
            text-align: left !important;
            width: 100% !important;
            overflow: hidden !important;
        }}

        .st-key-btn_kpi_all div[data-testid="stMarkdownContainer"] p,
        .st-key-btn_kpi_success div[data-testid="stMarkdownContainer"] p,
        .st-key-btn_kpi_failed div[data-testid="stMarkdownContainer"] p {{
            font-size: 12px !important;
            font-weight: 500 !important;
            color: #94A3B8 !important;
            white-space: pre-line !important;
            word-break: normal !important;
            overflow-wrap: normal !important;
            text-align: left !important;
            line-height: 1.35 !important;
            margin: 0 !important;
            padding: 0 !important;
        }}

        .st-key-btn_kpi_all div[data-testid="stMarkdownContainer"] strong,
        .st-key-btn_kpi_success div[data-testid="stMarkdownContainer"] strong,
        .st-key-btn_kpi_failed div[data-testid="stMarkdownContainer"] strong {{
            display: block !important;
            font-size: 22px !important;
            font-weight: 800 !important;
            color: #F8FAFC !important;
            margin-top: 2px !important;
            line-height: 1.2 !important;
            white-space: normal !important;
            word-break: normal !important;
            overflow-wrap: normal !important;
        }}

        .st-key-btn_kpi_success div[data-testid="stMarkdownContainer"] strong {{
            color: #22C55E !important;
        }}

        .st-key-btn_kpi_failed div[data-testid="stMarkdownContainer"] strong {{
            color: #EF4444 !important;
        }}

        /* Non-clickable Rows Processed card */
        .kpi-info-card {{
            width: 100% !important;
            min-height: 84px !important;
            height: 84px !important;
            padding: 12px 16px !important;
            border-radius: 12px !important;
            background: linear-gradient(180deg, #0F172A, #0B1625) !important;
            border: 1px solid rgba(255, 255, 255, 0.08) !important;
            box-shadow: 0 4px 12px rgba(2, 6, 23, 0.4) !important;
            box-sizing: border-box !important;
            display: flex !important;
            align-items: center !important;
        }}
        .kpi-card-inner {{
            display: flex !important;
            flex-direction: row !important;
            align-items: center !important;
            justify-content: flex-start !important;
            gap: 12px !important;
            width: 100% !important;
        }}
        .kpi-icon-box {{
            font-size: 20px !important;
            background: rgba(255, 255, 255, 0.05);
            padding: 8px 10px;
            border-radius: 8px;
            display: flex !important;
            align-items: center !important;
            justify-content: center !important;
            line-height: 1 !important;
            flex-shrink: 0 !important;
        }}
        .kpi-text-box {{
            display: flex !important;
            flex-direction: column !important;
            align-items: flex-start !important;
            justify-content: center !important;
            text-align: left !important;
            overflow: hidden !important;
        }}
        .kpi-card-title {{
            font-size: 12px !important;
            font-weight: 500 !important;
            color: #94A3B8 !important;
            margin: 0 !important;
            line-height: 1.35 !important;
            white-space: normal !important;
            word-break: normal !important;
            overflow-wrap: normal !important;
        }}
        .kpi-card-value {{
            font-size: 22px !important;
            font-weight: 800 !important;
            color: #F8FAFC !important;
            margin-top: 2px !important;
            line-height: 1.2 !important;
            white-space: normal !important;
            word-break: normal !important;
            overflow-wrap: normal !important;
        }}

        {active_border_css}
        </style>
        """
        st.markdown(card_styles, unsafe_allow_html=True)

        kpi_cols = st.columns(4)

        # Card 1: Filtered Runs (clickable -> clears status filter)
        with kpi_cols[0]:
            st.button(
                f"Filtered Runs\n**{total_runs}**",
                key="btn_kpi_all",
                icon="📋",
                use_container_width=True,
                on_click=_on_status_card_click,
                args=("All Statuses",),
                help="Click to show all execution statuses",
            )

        # Card 2: Successful (clickable -> sets status to SUCCESS)
        with kpi_cols[1]:
            st.button(
                f"Successful\n**{success_runs}**",
                key="btn_kpi_success",
                icon="✅",
                use_container_width=True,
                on_click=_on_status_card_click,
                args=("SUCCESS",),
                help="Click to filter by SUCCESS runs",
            )

        # Card 3: Failed (clickable -> sets status to FAILED)
        with kpi_cols[2]:
            st.button(
                f"Failed\n**{failed_runs}**",
                key="btn_kpi_failed",
                icon="❌",
                use_container_width=True,
                on_click=_on_status_card_click,
                args=("FAILED",),
                help="Click to filter by FAILED runs",
            )

        # Card 4: Rows Processed (informational only)
        with kpi_cols[3]:
            st.markdown(
                f"""
                <div class='kpi-info-card'>
                    <div class='kpi-card-inner'>
                        <div class='kpi-icon-box'>📊</div>
                        <div class='kpi-text-box'>
                            <div class='kpi-card-title'>Rows Processed</div>
                            <div class='kpi-card-value'>{total_rows:,}</div>
                        </div>
                    </div>
                </div>
                """,
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

        active_registry_map = {p["pipeline_name"]: p for p in registered_pipelines}
        table_rows: list[dict[str, Any]] = []
        for r in records:
            status_val = (r.get("status") or "").upper()
            raw_pname = r.get("pipeline_name") or "Unknown"

            # Check if this execution belongs to a deleted pipeline or pre-creation incarnation
            is_deleted = False
            if raw_pname not in active_registry_map:
                is_deleted = True
            else:
                reg_created = active_registry_map[raw_pname].get("created_at")
                r_ts = r.get("created_at") or r.get("start_time")
                if reg_created and r_ts:
                    try:
                        if pd.to_datetime(r_ts) < pd.to_datetime(reg_created):
                            is_deleted = True
                    except Exception:
                        pass

            pipeline_display = f"{raw_pname} (Deleted)" if is_deleted else raw_pname

            # Failure category and error details must come from THAT execution's own record.
            # For successful runs (or any run with no failure info), show "-" for failure category and error details.
            if status_val == "SUCCESS":
                failure_cat = "-"
                error_details = "-"
            elif status_val == "FAILED":
                failure_cat = str(r.get("failure_category") or r.get("failure_type") or "UNKNOWN")
                error_details = str(r.get("error_message") or "-")
            elif status_val == "RETRYING":
                failure_cat = str(r.get("failure_category") or r.get("failure_type") or "UNKNOWN")
                error_details = str(r.get("error_message") or "Retrying...")
            else:
                failure_cat = "-"
                error_details = "-"

            table_rows.append({
                "Pipeline": pipeline_display,
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
                    raw_f_name = f.get("pipeline_name") or "Unknown"
                    is_deleted_run = raw_f_name not in active_registry_map
                    if not is_deleted_run:
                        reg_created = active_registry_map[raw_f_name].get("created_at")
                        f_ts = f.get("created_at") or f.get("start_time")
                        if reg_created and f_ts:
                            try:
                                if pd.to_datetime(f_ts) < pd.to_datetime(reg_created):
                                    is_deleted_run = True
                            except Exception:
                                pass
                    del_badge = " <span class='status-pill' style='margin-left:6px; font-size:10px; background:rgba(148,163,184,0.15); color:#94A3B8; border:1px solid rgba(148,163,184,0.3);'>DELETED</span>" if is_deleted_run else ""
                    st.markdown(
                        f"<div class='card' style='margin-bottom:0.6rem;'>"
                        f"<div style='display:flex; justify-content:space-between; align-items:center;'>"
                        f"<div><strong>{raw_f_name}</strong>{del_badge} <span class='status-pill status-failed' style='margin-left:6px; font-size:10px;'>FAILED</span></div>"
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
