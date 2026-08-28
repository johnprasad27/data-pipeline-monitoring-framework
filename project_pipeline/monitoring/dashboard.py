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


def main() -> None:
    st.set_page_config(page_title="Data Pipeline Monitor", layout="wide")
    _load_css()

    # Simple sidebar (no navigation)
    with st.sidebar:
        st.markdown("<div class='sidebar-brand'>📊 <strong>Data Pipeline Monitor</strong></div>", unsafe_allow_html=True)
        st.divider()
        st.markdown("<div><strong>Status</strong>: <span style='color:#22C55E'>●</span> Database Connected</div>", unsafe_allow_html=True)
        st.divider()
        st.caption("© Data Pipeline Monitoring")

    # Auto-refresh every 5 seconds
    if "auto_refresh" not in st.session_state:
        st.session_state["auto_refresh"] = True

    # Header
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("<h1 class='dashboard-title'>Data Pipeline Monitoring</h1>", unsafe_allow_html=True)
        st.markdown("<div class='dashboard-subtitle'>Simple monitoring view</div>", unsafe_allow_html=True)
    with col2:
        st.markdown(f"<div class='muted'>{datetime.now().isoformat(sep=' ', timespec='seconds')}</div>")

    # Connect DB
    db = Database()
    try:
        db.connect()
    except Exception as e:
        st.error(f"Unable to connect to database: {e}")
        return

    try:
        # KPIs
        kpis = metrics.gather_kpis(db)
        rows_by_pipeline = metrics.get_rows_per_pipeline(db)

        # compute avg duration
        try:
            history = metrics.get_execution_history(db, limit=500)
            durations = []
            for r in history:
                d = None
                if r.get("execution_duration_seconds") is not None:
                    try:
                        d = float(r.get("execution_duration_seconds"))
                    except Exception:
                        d = None
                if d is None and r.get("start_time") and r.get("end_time"):
                    try:
                        s = pd.to_datetime(r.get("start_time"))
                        e = pd.to_datetime(r.get("end_time"))
                        d = (e - s).total_seconds()
                    except Exception:
                        d = None
                if d is not None:
                    durations.append(d)
            avg = sum(durations) / len(durations) if durations else 0
        except Exception:
            avg = 0

        icons = ["📦", "✅", "❌", "📊", "⏱️"]
        titles = ["Total Pipelines", "Successful Runs", "Failed Runs", "Rows Processed", "Avg Exec Duration"]
        values = [
            len(rows_by_pipeline or []),
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

        # Pipeline status table
        st.subheader("Pipeline Status")
        pipelines = queries.get_all_pipelines(db)

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
                    <td>{last}</td>
                    <td class='action-cell'>
                        <button class='btn btn-view'>View</button>
                        <button class='btn btn-logs'>Logs</button>
                        <button class='btn btn-retry'>Retry</button>
                    </td>
                </tr>"""
        else:
            table_rows_html = """<tr>
                <td colspan='8' style='text-align: center; color: #94A3B8; padding: 20px;'>No pipelines available</td>
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

        st.markdown(
            pipeline_table_html,
            unsafe_allow_html=True
        )



        # Charts: left donut (success vs failure), right bar (execution duration by pipeline)
        st.subheader("Execution Charts")
        try:
            df_rows = pd.DataFrame(history) if 'history' in locals() else pd.DataFrame(metrics.get_execution_history(db, limit=1000))

            left_col, right_col = st.columns([1, 1])
            with left_col:
                if not df_rows.empty:
                    status_counts = df_rows['status'].fillna('UNKNOWN').value_counts().reset_index()
                    status_counts.columns = ['status', 'count']
                    fig = px.pie(status_counts, names='status', values='count', hole=0.5, title='Success vs Failure')
                    st.plotly_chart(fig, use_container_width=True)
                else:
                    st.info('No execution data for chart')

            with right_col:
                # Build exec duration per pipeline from `pipelines` latest rows
                pdp = []
                for p in pipelines or []:
                    name = p.get('pipeline_name')
                    dur = p.get('execution_duration_seconds')
                    if dur is None and p.get('start_time') and p.get('end_time'):
                        try:
                            s = pd.to_datetime(p.get('start_time'))
                            e = pd.to_datetime(p.get('end_time'))
                            dur = (e - s).total_seconds()
                        except Exception:
                            dur = None
                    try:
                        dur_val = float(dur) if dur is not None else 0.0
                    except Exception:
                        dur_val = 0.0
                    pdp.append({'pipeline_name': name, 'duration': dur_val})

                dur_df = pd.DataFrame(pdp)
                if not dur_df.empty:
                    fig2 = px.bar(dur_df, x='pipeline_name', y='duration', title='Execution Duration by Pipeline', labels={'duration':'Duration (s)', 'pipeline_name':'Pipeline'})
                    st.plotly_chart(fig2, use_container_width=True)
                else:
                    st.info('No duration data available')
        except Exception:
            st.info("Charts unavailable")

        st.markdown("<div class='spacer'></div>", unsafe_allow_html=True)

        # Recent Executions and Failed Executions side-by-side
        left, right = st.columns([2, 1])
        with left:
            st.subheader("Recent Executions")
            recent = metrics.get_execution_history(db, limit=10)
            if recent:
                rows = []
                for r in recent:
                    rows.append({
                        'Pipeline': r.get('pipeline_name'),
                        'Status': r.get('status'),
                        'Rows': r.get('rows_processed') or 0,
                        'Duration': _format_duration(r),
                        'Execution Time': r.get('created_at') or '',
                    })
                df_recent = pd.DataFrame(rows)
                st.table(df_recent)
            else:
                st.info('No executions yet.')

        with right:
            st.subheader('Failed Executions')
            failed_runs = [r for r in (metrics.get_execution_history(db, limit=200) or []) if (r.get('status') or '').upper() == 'FAILED']
            if failed_runs:
                for r in failed_runs[:10]:
                    st.markdown(f"<div class='card'><div><strong>{r.get('pipeline_name')}</strong> <div class='muted small'>{r.get('created_at')}</div></div><div style='color:var(--danger)'>{r.get('error_message') or ''}</div></div>", unsafe_allow_html=True)
            else:
                st.markdown("<div class='card'><div style='text-align:center'>🎉 No failed pipelines.<br/>All pipelines executed successfully.</div></div>", unsafe_allow_html=True)

    except Exception as e:
        st.error(f"Dashboard error: {e}")
    finally:
        db.close()

    # Auto-refresh loop
    if st.session_state.get('auto_refresh'):
        try:
            st.experimental_sleep(5 * 1000)
            st.experimental_rerun()
        except Exception:
            pass


if __name__ == '__main__':
    main()
