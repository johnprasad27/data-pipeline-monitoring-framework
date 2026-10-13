"""Simplified single-page monitoring dashboard.

This module is intentionally minimal: a single Streamlit entrypoint
that visualizes KPIs, pipelines, charts, latest executions and logs.
It keeps monitoring backend code (queries.py, metrics.py, database/) untouched.
"""
from __future__ import annotations

from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))
    
# pyrefly: ignore [missing-import]
import streamlit as st
from datetime import datetime
import pandas as pd
# pyrefly: ignore [missing-import]
import plotly.express as px

import config
from database.db import Database
from monitoring import queries, metrics


def _load_css() -> None:
    try:
        css_path = __file__.replace("dashboard.py", "styles.css")
        with open(css_path, "r", encoding="utf-8") as fh:
            st.markdown(f"<style>{fh.read()}</style>", unsafe_allow_html=True)
    except Exception:
        # Silently continue if CSS unavailable
        pass


def _format_duration(row: dict) -> str:
    # Prefer stored execution_duration_seconds if present and non-null
    dur = row.get("execution_duration_seconds")
    if dur is not None:
        try:
            val = float(dur)
            return f"{val:.2f} s"
        except Exception:
            pass

    # Fallback: compute from end_time - start_time when available
    start = row.get("start_time")
    end = row.get("end_time")
    if start and end:
        try:
            s = pd.to_datetime(start)
            e = pd.to_datetime(end)
            delta = (e - s).total_seconds()
            return f"{delta:.2f} s"
        except Exception:
            pass
    return "0.00 s"


format_duration = _format_duration


def format_failure_category(
    status: str | None,
    failure_category: str | None,
    retry_count: int = 0,
) -> str:
    """Format failure category for display:
    - Terminally failed: show category (e.g. NETWORK_ERROR) or UNKNOWN
    - Successful with no failure: show '—'
    - Successful with recovered failure (retry_count > 0): show 'Recovered (<category>)'
    - Retrying: show category
    - Other/Running: show '—'
    """
    s = (status or "").upper()
    cat = str(failure_category or "").strip()
    if cat in {"-", "—", "None", "NULL"}:
        cat = ""

    if s == "SUCCESS":
        if cat.startswith("Recovered (") and cat.endswith(")"):
            return cat
        if cat and cat not in {"UNKNOWN", "UNKNOWN_ERROR"} and retry_count > 0:
            return f"Recovered ({cat})"
        return "—"

    if s in {"FAILED", "RETRYING"}:
        if cat and cat not in {"-", "—"}:
            return cat
        return "UNKNOWN" if s == "FAILED" else "—"

    if cat and cat not in {"UNKNOWN", "UNKNOWN_ERROR", "-", "—"}:
        return cat
    return "—"


def format_retryable(status: str | None, retryable: bool | None) -> str:
    """Format retryable status: only applicable for failed/retrying states."""
    s = (status or "").upper()
    if s == "SUCCESS":
        return "—"
    if retryable is None:
        return "—"
    return "YES" if retryable else "NO"


def _status_pill(status: str) -> str:
    if not status:
        status = "UNKNOWN"
    s = status.upper()
    if s == "SUCCESS":
        cls = "status-pill status-success"
    elif s == "FAILED":
        cls = "status-pill status-failed"
    elif s == "RUNNING":
        cls = "status-pill status-running"
    else:
        cls = "status-pill"
    return f"<span class=\"{cls}\">{s}</span>"


@st.fragment(run_every=config.DASHBOARD_REFRESH_SECONDS)
def main() -> None:
    st.set_page_config(page_title="Data Pipeline Monitor", layout="wide")
    _load_css()

    # Keep the existing sidebar design and add the pipeline selector without creating pages.
    with st.sidebar:
        st.markdown("<div class='sidebar-brand'>📊 <strong>Data Pipeline Monitor</strong></div>", unsafe_allow_html=True)
        st.divider()
        st.markdown("<div><strong>Status</strong>: <span style='color:#22C55E'>●</span> Database Connected</div>", unsafe_allow_html=True)
        st.divider()
        st.caption("© Data Pipeline Monitoring")

    db = Database()
    try:
        db.connect()
    except Exception as e:
        st.error(f"Unable to connect to database: {e}")
        return

    try:
        pipeline_rows = queries.get_all_pipelines(db)
        pipeline_names = sorted({row["pipeline_name"] for row in pipeline_rows})
        pipeline_filter = st.selectbox("Pipeline", ["All Pipelines", *pipeline_names])
        selected_pipeline = queries.pipeline_option_name(pipeline_filter)

        event_key = f"{selected_pipeline or 'all'}:{queries.latest_execution_event_id(db, selected_pipeline)}"
        previous_event_key = st.session_state.get("last_dashboard_event_key")
        event_changed = previous_event_key is None or previous_event_key != event_key
        if event_changed:
            st.session_state["last_dashboard_event_key"] = event_key

        if event_changed or "dashboard_snapshot" not in st.session_state or st.session_state.get("dashboard_filter") != selected_pipeline:
            snapshot = {
                "kpis": metrics.gather_kpis(db, selected_pipeline),
                "rows_by_pipeline": metrics.get_rows_per_pipeline(db),
                "history": metrics.get_execution_history(db, limit=500, pipeline_name=selected_pipeline),
                "all_pipelines": pipeline_rows,
            }
            st.session_state["dashboard_snapshot"] = snapshot
            st.session_state["dashboard_filter"] = selected_pipeline
        else:
            snapshot = st.session_state["dashboard_snapshot"]

        kpis = snapshot["kpis"]
        rows_by_pipeline = snapshot["rows_by_pipeline"]
        history = snapshot["history"]
        pipelines = snapshot["all_pipelines"]

        # Header
        col1, col2 = st.columns([3, 1])
        with col1:
            st.markdown("<h1 class='dashboard-title'>Data Pipeline Monitoring</h1>", unsafe_allow_html=True)
            st.markdown("<div class='dashboard-subtitle'>Simple monitoring view</div>", unsafe_allow_html=True)
        with col2:
            st.markdown(f"<div class='muted'>{datetime.now().isoformat(sep=' ', timespec='seconds')}</div>")

        # KPI cards
        avg = 0.0
        if history:
            durations = []
            for row in history:
                value = row.get("execution_duration_seconds")
                if value is None and row.get("start_time") and row.get("end_time"):
                    try:
                        value = (pd.to_datetime(row.get("end_time")) - pd.to_datetime(row.get("start_time"))).total_seconds()
                    except Exception:
                        value = None
                if value is not None:
                    try:
                        durations.append(float(value))
                    except Exception:
                        pass
            if durations:
                avg = sum(durations) / len(durations)

        icons = ["📦", "✅", "❌", "📊", "⏱️"]
        titles = ["Total Pipelines", "Successful Runs", "Failed Runs", "Rows Processed", "Avg Exec Duration"]
        if selected_pipeline is None:
            values = [
                len(pipelines or []),
                kpis.get("successful_executions", 0),
                kpis.get("failed_executions", 0),
                kpis.get("total_rows_processed", 0),
                f"{avg:.2f} s",
            ]
        else:
            values = [
                1,
                kpis.get("successful_executions", 0),
                kpis.get("failed_executions", 0),
                kpis.get("total_rows_processed", 0),
                f"{avg:.2f} s",
            ]

        kcols = st.columns(5)
        for i, col in enumerate(kcols):
            col.markdown(
                "<div class='card kpi-card' style='width:100%'><div class='kpi-inline'><div class='icon'>" + icons[i] + "</div><div class='meta'><div class='title'>" + titles[i] + "</div><div class='value'>" + str(values[i]) + "</div></div></div></div>",
                unsafe_allow_html=True,
            )

        st.markdown("<div class='spacer'></div>", unsafe_allow_html=True)

        if selected_pipeline is None:
            st.subheader("Pipeline Status")
            table_rows_html = ""
            if pipelines:
                for pipeline in pipelines:
                    name = pipeline.get("pipeline_name") or ""
                    source = pipeline.get("source_type") or ""
                    status = (pipeline.get("status") or "").upper()
                    rows = pipeline.get("rows_processed") if pipeline.get("rows_processed") is not None else 0
                    duration = _format_duration(pipeline) or "0.00 s"
                    retries = pipeline.get("retry_count") if pipeline.get("retry_count") is not None else 0
                    last = str(pipeline.get("created_at") or "")
                    failure_category = format_failure_category(status, pipeline.get("failure_category"), retries)

                    if status == "SUCCESS":
                        badge_html = "<span class='status-badge status-success'>SUCCESS</span>"
                    elif status == "FAILED":
                        badge_html = "<span class='status-badge status-failed'>FAILED</span>"
                    elif status == "RUNNING":
                        badge_html = "<span class='status-badge status-running'>RUNNING</span>"
                    else:
                        badge_html = f"<span class='status-badge status-default'>{status}</span>"

                    table_rows_html += f"""<tr>
                        <td>{name}</td>
                        <td>{source}</td>
                        <td>{badge_html}</td>
                        <td>{rows}</td>
                        <td>{duration}</td>
                        <td>{retries}</td>
                        <td>{failure_category}</td>
                        <td>{last}</td>
                        <td class='action-cell'>
                            <button class='btn btn-view'>View</button>
                            <button class='btn btn-logs'>Logs</button>
                            <button class='btn btn-retry'>Retry</button>
                        </td>
                    </tr>"""
            else:
                table_rows_html = """<tr>
                    <td colspan='9' style='text-align: center; color: #94A3B8; padding: 20px;'>No pipelines available</td>
                </tr>"""

            pipeline_table_html = f"""
<style>
.pipeline-table-container {{
    width: 100%;
    overflow-x: auto;
    border-radius: 12px;
    border: 1px solid #1E293B;
    background-color: #0F172A;
    box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
    margin-bottom: 24px;
}}
.pipeline-table {{
    width: 100%;
    border-collapse: collapse;
    color: #F8FAFC;
    font-family: inherit;
    font-size: 14px;
    text-align: left;
}}
.pipeline-table th {{
    background-color: #1E293B;
    color: #94A3B8;
    font-weight: 600;
    padding: 12px 16px;
    border-bottom: 1px solid #334155;
    white-space: nowrap;
    text-transform: uppercase;
    font-size: 12px;
    letter-spacing: 0.05em;
}}
.pipeline-table td {{
    padding: 12px 16px;
    border-bottom: 1px solid #1E293B;
    vertical-align: middle;
    white-space: nowrap;
}}
.pipeline-table tbody tr:nth-child(even) {{
    background-color: #0F172A;
}}
.pipeline-table tbody tr:nth-child(odd) {{
    background-color: #162032;
}}
.pipeline-table tbody tr:hover {{
    background-color: #1E293B;
}}
.pipeline-table tbody tr:last-child td {{
    border-bottom: none;
}}
.status-badge {{
    display: inline-block;
    padding: 4px 10px;
    border-radius: 9999px;
    font-size: 11px;
    font-weight: 700;
    letter-spacing: 0.05em;
    text-transform: uppercase;
    text-align: center;
    white-space: nowrap;
}}
.status-success {{
    background-color: rgba(34, 197, 94, 0.15);
    color: #22C55E;
    border: 1px solid rgba(34, 197, 94, 0.3);
}}
.status-failed {{
    background-color: rgba(239, 68, 68, 0.15);
    color: #EF4444;
    border: 1px solid rgba(239, 68, 68, 0.3);
}}
.status-running {{
    background-color: rgba(249, 115, 22, 0.15);
    color: #F97316;
    border: 1px solid rgba(249, 115, 22, 0.3);
}}
.status-default {{
    background-color: rgba(148, 163, 184, 0.15);
    color: #94A3B8;
    border: 1px solid rgba(148, 163, 184, 0.3);
}}
.action-cell {{
    white-space: nowrap;
}}
.btn {{
    display: inline-block;
    padding: 5px 12px;
    font-size: 12px;
    font-weight: 500;
    border-radius: 6px;
    cursor: pointer;
    border: 1px solid transparent;
    margin-right: 4px;
    white-space: nowrap;
    transition: background-color 0.15s ease-in-out;
}}
.btn-view {{
    background-color: #334155;
    color: #F8FAFC;
    border-color: #475569;
}}
.btn-view:hover {{
    background-color: #475569;
}}
.btn-logs {{
    background-color: #334155;
    color: #F8FAFC;
    border-color: #475569;
}}
.btn-logs:hover {{
    background-color: #475569;
}}
.btn-retry {{
    background-color: #2563EB;
    color: #FFFFFF;
    border-color: #3B82F6;
}}
.btn-retry:hover {{
    background-color: #1D4ED8;
}}
</style>

<div class="pipeline-table-container">
<table class="pipeline-table">
<thead>
<tr>
    <th>Pipeline Name</th>
    <th>Source</th>
    <th>Status</th>
    <th>Rows</th>
    <th>Duration</th>
    <th>Retries</th>
    <th>Failure Category</th>
    <th>Last Executed</th>
    <th>Actions</th>
</tr>
</thead>

<tbody>
{table_rows_html}
</tbody>

</table>
</div>
"""
            st.markdown(pipeline_table_html, unsafe_allow_html=True)
        else:
            latest = queries.get_latest_execution(db, selected_pipeline)
            if latest:
                title_label = selected_pipeline.replace("_", " ").title()
                status = (latest.get("status") or "UNKNOWN").upper()
                source = latest.get("source_type") or ""
                rows = latest.get("rows_processed") if latest.get("rows_processed") is not None else 0
                duration = _format_duration(latest) or "0.00 s"
                retries = latest.get("retry_count") if latest.get("retry_count") is not None else 0
                last_executed = str(latest.get("created_at") or latest.get("start_time") or "")

                st.subheader(title_label)
                info_cols = st.columns(5)
                info_cols[0].markdown(f"<div class='card'><strong>Current Status</strong><br>{_status_pill(status)}</div>", unsafe_allow_html=True)
                info_cols[1].markdown(f"<div class='card'><strong>Source</strong><br>{source}</div>", unsafe_allow_html=True)
                info_cols[2].markdown(f"<div class='card'><strong>Rows Processed</strong><br>{rows}</div>", unsafe_allow_html=True)
                info_cols[3].markdown(f"<div class='card'><strong>Execution Duration</strong><br>{duration}</div>", unsafe_allow_html=True)
                info_cols[4].markdown(f"<div class='card'><strong>Retry Count</strong><br>{retries}</div>", unsafe_allow_html=True)

                health_value = latest.get("health_score")
                quality_value = latest.get("data_quality_score")
                freshness_value = latest.get("freshness_status") or "STALE"
                failure_category = format_failure_category(
                    status,
                    latest.get("failure_category") or latest.get("failure_type"),
                    retries,
                )
                retryable_text = format_retryable(status, latest.get("retryable"))
                detail_cols = st.columns(5)
                detail_cols[0].markdown(f"<div class='card'><strong>Health</strong><br>{health_value if health_value is not None else 'N/A'} / 100</div>", unsafe_allow_html=True)
                detail_cols[1].markdown(f"<div class='card'><strong>Freshness</strong><br>{freshness_value}</div>", unsafe_allow_html=True)
                detail_cols[2].markdown(f"<div class='card'><strong>Data Quality</strong><br>{quality_value if quality_value is not None else 'N/A'}%</div>", unsafe_allow_html=True)
                detail_cols[3].markdown(f"<div class='card'><strong>Failure Category</strong><br>{failure_category}</div>", unsafe_allow_html=True)
                detail_cols[4].markdown(f"<div class='card'><strong>Retryable</strong><br>{retryable_text}</div>", unsafe_allow_html=True)
                st.markdown(f"<div class='card' style='margin-top: 1rem;'><strong>Last Executed</strong><br>{last_executed}</div>", unsafe_allow_html=True)

                quality_execution = latest
                if source.upper() == "CSV" and latest.get("input_rows") is None:
                    quality_execution = queries.get_latest_quality_execution(
                        db, selected_pipeline
                    )
                if source.upper() == "CSV" and quality_execution:
                    st.subheader("Data Quality")
                    if quality_execution.get("execution_id") != latest.get("execution_id"):
                        st.caption(
                            "Metrics from the most recent execution with recorded quality data."
                        )
                    quality_rows = [
                        {
                            "Metric": label,
                            "Value": str(value) if value is not None else "N/A",
                        }
                        for label, value in (
                            ("Input rows", quality_execution.get("input_rows")),
                            ("Valid rows", quality_execution.get("valid_rows")),
                            ("Rejected rows", quality_execution.get("rejected_rows")),
                            ("Loaded rows", quality_execution.get("loaded_rows")),
                            ("Missing columns", quality_execution.get("missing_column_count")),
                            ("Null values", quality_execution.get("null_count")),
                            ("Duplicates", quality_execution.get("duplicate_count")),
                            ("Datatype errors", quality_execution.get("datatype_error_count")),
                            ("Business rule errors", quality_execution.get("business_rule_error_count")),
                            ("Quality status", quality_execution.get("data_quality_status")),
                        )
                    ]
                    st.dataframe(pd.DataFrame(quality_rows), hide_index=True, width="stretch")
            else:
                st.info(f"No execution data for {selected_pipeline} yet.")

        st.subheader("Execution Charts")
        try:
            df_rows = pd.DataFrame(history)
            left_col, right_col = st.columns([1, 1])
            with left_col:
                if not df_rows.empty:
                    status_counts = df_rows['status'].fillna('UNKNOWN').value_counts().reset_index()
                    status_counts.columns = ['status', 'count']
                    title_text = 'Success vs Failure' if selected_pipeline is None else f'{selected_pipeline.replace("_", " ").title()} Success vs Failure'
                    fig = px.pie(status_counts, names='status', values='count', hole=0.5, title=title_text)
                    st.plotly_chart(fig, width='stretch')
                else:
                    st.info('No execution data for chart')

            with right_col:
                if selected_pipeline is None:
                    dur_rows = []
                    for p in pipelines or []:
                        name = p.get('pipeline_name')
                        dur = p.get('execution_duration_seconds')
                        if dur is None and p.get('start_time') and p.get('end_time'):
                            try:
                                dur = (pd.to_datetime(p.get('end_time')) - pd.to_datetime(p.get('start_time'))).total_seconds()
                            except Exception:
                                dur = None
                        if dur is not None:
                            dur_rows.append({'pipeline_name': name, 'duration': float(dur)})
                    if dur_rows:
                        dur_df = pd.DataFrame(dur_rows)
                        fig2 = px.bar(dur_df, x='pipeline_name', y='duration', title='Execution Duration by Pipeline', labels={'duration':'Duration (s)', 'pipeline_name':'Pipeline'})
                        st.plotly_chart(fig2, width='stretch')
                else:
                    dur_rows = []
                    for row in history:
                        d = row.get('execution_duration_seconds')
                        if d is None and row.get('start_time') and row.get('end_time'):
                            try:
                                d = (pd.to_datetime(row.get('end_time')) - pd.to_datetime(row.get('start_time'))).total_seconds()
                            except Exception:
                                d = None
                        if d is not None:
                            dur_rows.append({'execution_time': row.get('created_at') or row.get('start_time'), 'duration': float(d)})
                    if dur_rows:
                        dur_df = pd.DataFrame(dur_rows)
                        title_text = f'{selected_pipeline.replace("_", " ").title()} Execution Duration'
                        fig2 = px.bar(dur_df, x='execution_time', y='duration', title=title_text, labels={'duration':'Duration (s)', 'execution_time':'Execution Time'})
                        st.plotly_chart(fig2, width='stretch')
                if 'fig2' not in locals():
                    st.info('No duration data available')
        except Exception:
            st.info('Charts unavailable')

        st.markdown("<div class='spacer'></div>", unsafe_allow_html=True)

        if selected_pipeline is None:
            st.subheader('Failed Executions')
            failed_runs = [r for r in (metrics.get_execution_history(db, limit=200) or []) if (r.get('status') or '').upper() == 'FAILED']
            if failed_runs:
                for r in failed_runs[:10]:
                    exec_id = r.get('execution_id')
                    exec_label = f"Execution #{exec_id} &nbsp; " if exec_id else ""
                    st.markdown(
                        f"<div class='card'>"
                        f"<div style='display:flex; justify-content:space-between; align-items:center;'>"
                        f"<div><strong>{r.get('pipeline_name')}</strong> <span class='status-pill status-failed' style='margin-left:6px; font-size:10px;'>FAILED</span></div>"
                        f"<div class='muted small'>{exec_label}{r.get('created_at')}</div>"
                        f"</div>"
                        f"<div style='margin-top:0.4rem;'><strong>Failure Category:</strong> {r.get('failure_category') or r.get('failure_type') or 'UNKNOWN'}</div>"
                        f"<div style='color:var(--danger); margin-top:0.2rem;'>{r.get('error_message') or ''}</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown("<div class='card'><div style='text-align:center'>🎉 No failed pipelines.<br/>All pipelines executed successfully.</div></div>", unsafe_allow_html=True)
        else:
            st.subheader('Failed Executions')
            failed_runs = metrics.get_pipeline_failures(db, selected_pipeline, limit=20)
            if failed_runs:
                for r in failed_runs[:10]:
                    exec_id = r.get('execution_id')
                    exec_label = f"Execution #{exec_id} &nbsp; " if exec_id else ""
                    st.markdown(
                        f"<div class='card'>"
                        f"<div style='display:flex; justify-content:space-between; align-items:center;'>"
                        f"<div><strong>{r.get('pipeline_name')}</strong> <span class='status-pill status-failed' style='margin-left:6px; font-size:10px;'>FAILED</span></div>"
                        f"<div class='muted small'>{exec_label}{r.get('created_at')}</div>"
                        f"</div>"
                        f"<div style='margin-top:0.4rem;'><strong>Failure Category:</strong> {r.get('failure_category') or r.get('failure_type') or 'UNKNOWN'}</div>"
                        f"<div style='color:var(--danger); margin-top:0.2rem;'>{r.get('error_message') or ''}</div>"
                        f"</div>",
                        unsafe_allow_html=True,
                    )
            else:
                st.markdown("<div class='card'><div style='text-align:center'>🎉 No failed executions for this pipeline.</div></div>", unsafe_allow_html=True)

    except Exception as e:
        st.error(f"Dashboard error: {e}")
    finally:
        db.close()

if __name__ == '__main__':
    main()
