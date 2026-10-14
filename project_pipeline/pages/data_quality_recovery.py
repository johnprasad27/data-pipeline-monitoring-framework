"""Data Quality & Recovery Page.

Presentation-ready visual demonstration and monitoring of:
  - Detecting incomplete CSV/API data against expected row counts
  - Checking duplicates independently (without treating duplicates as missing records)
  - Distinguishing successful HTTP 200 from complete data extraction
  - Orchestrating retry attempts with an attempt-by-attempt audit trail
  - Preventing duplicate inserts through idempotent loading
  - Final status reporting: PASSED, RECOVERED, or FAILED — DATA INCOMPLETE
  - Presentation / Demo mode using clearly labelled sample data
  - Live Pipeline Audit mode inspecting actual database executions
"""

from __future__ import annotations

import html
from datetime import datetime
from pathlib import Path
from typing import Any, Optional

import pandas as pd
import streamlit as st

import config
from database.db import Database
from monitoring import queries
from monitoring.dashboard import format_duration
from pipelines.completeness import (
    CompletenessReport,
    evaluate_data_completeness,
    inspect_api_response_completeness,
    simulate_recovery_workflow,
)


def load_styles() -> None:
    css_path = Path(__file__).resolve().parents[1] / "monitoring" / "styles.css"
    if css_path.is_file():
        st.markdown(f"<style>{css_path.read_text(encoding='utf-8')}</style>", unsafe_allow_html=True)

    # Dedicated high-aesthetic styling for quality workflow and status pills
    st.markdown(
        """
        <style>
        .demo-banner {
            background: linear-gradient(90deg, rgba(37, 99, 235, 0.15), rgba(16, 185, 129, 0.15));
            border: 1px solid rgba(59, 130, 246, 0.3);
            border-radius: 10px;
            padding: 12px 18px;
            margin-bottom: 20px;
            display: flex;
            align-items: center;
            justify-content: space-between;
        }
        .demo-tag {
            background: #2563EB;
            color: #FFFFFF;
            font-size: 11px;
            font-weight: 700;
            padding: 4px 10px;
            border-radius: 6px;
            letter-spacing: 0.5px;
            text-transform: uppercase;
        }
        .status-badge-lg {
            display: inline-block;
            font-size: 15px;
            font-weight: 800;
            padding: 8px 18px;
            border-radius: 999px;
            letter-spacing: 0.4px;
            box-shadow: 0 4px 14px rgba(0,0,0,0.3);
        }
        .status-passed {
            background: linear-gradient(135deg, #10B981, #059669);
            color: #FFFFFF;
            border: 1px solid #34D399;
        }
        .status-recovered {
            background: linear-gradient(135deg, #06B6D4, #0891B2);
            color: #FFFFFF;
            border: 1px solid #67E8F9;
        }
        .status-incomplete {
            background: linear-gradient(135deg, #EF4444, #B91C1C);
            color: #FFFFFF;
            border: 1px solid #F87171;
            animation: pulse-border 2s infinite;
        }
        .status-failed {
            background: linear-gradient(135deg, #DC2626, #991B1B);
            color: #FFFFFF;
            border: 1px solid #FCA5A5;
        }
        .workflow-container {
            display: grid;
            grid-template-columns: repeat(6, 1fr);
            gap: 12px;
            margin: 20px 0;
        }
        .workflow-card {
            background: #0F172A;
            border: 1px solid rgba(255, 255, 255, 0.08);
            border-radius: 10px;
            padding: 14px 12px;
            position: relative;
            text-align: center;
            transition: all 0.2s ease;
        }
        .workflow-card:hover {
            border-color: rgba(59, 130, 246, 0.5);
            transform: translateY(-2px);
        }
        .workflow-step {
            font-size: 11px;
            font-weight: 700;
            color: #94A3B8;
            text-transform: uppercase;
            letter-spacing: 0.5px;
            margin-bottom: 4px;
        }
        .workflow-icon {
            font-size: 24px;
            margin: 6px 0;
        }
        .workflow-title {
            font-size: 13px;
            font-weight: 700;
            color: #F8FAFC;
            margin-bottom: 6px;
        }
        .workflow-state {
            font-size: 11px;
            font-weight: 700;
            padding: 3px 8px;
            border-radius: 999px;
            display: inline-block;
        }
        .wf-success { background: rgba(16, 185, 129, 0.2); color: #34D399; }
        .wf-warning { background: rgba(245, 158, 11, 0.2); color: #FBBF24; }
        .wf-failed { background: rgba(239, 68, 68, 0.2); color: #F87171; }
        .wf-info { background: rgba(59, 130, 246, 0.2); color: #60A5FA; }
        .metric-box {
            background: #0F172A;
            border: 1px solid rgba(255,255,255,0.06);
            border-radius: 10px;
            padding: 14px;
            text-align: center;
        }
        .metric-title {
            color: #94A3B8;
            font-size: 12px;
            font-weight: 600;
            margin-bottom: 4px;
        }
        .metric-val {
            font-size: 26px;
            font-weight: 800;
            color: #F8FAFC;
        }
        .metric-sub {
            font-size: 11px;
            margin-top: 4px;
        }
        @media (max-width: 900px) {
            .workflow-container {
                grid-template-columns: repeat(2, 1fr);
            }
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


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


def render_workflow_pipeline(stages: list[dict[str, Any]]) -> None:
    """Render the 6-stage visual pipeline workflow: Extract -> Validate -> Detect -> Retry -> Verify -> Final."""
    html_cards = []
    for s in stages:
        state = s.get("status", "COMPLETED")
        if state in ("COMPLETED", "VERIFIED", "PASSED"):
            state_class = "wf-success"
        elif state in ("WARNING", "RECOVERED"):
            state_class = "wf-info"
        elif state in ("FAILED", "EXHAUSTED", "FAILED — DATA INCOMPLETE"):
            state_class = "wf-failed"
        else:
            state_class = "wf-warning"

        html_cards.append(
            f"""
            <div class="workflow-card">
                <div class="workflow-step">{html.escape(s.get("stage", ""))}</div>
                <div class="workflow-icon">{s.get("icon", "⚙️")}</div>
                <div class="workflow-title">{html.escape(s.get("title", ""))}</div>
                <div class="workflow-state {state_class}">{html.escape(state)}</div>
                <div style="font-size: 10px; color: #94A3B8; margin-top: 6px; line-height: 1.3;">
                    {html.escape(s.get("details", ""))}
                </div>
            </div>
            """
        )

    st.markdown(
        f'<div class="workflow-container">{"".join(html_cards)}</div>',
        unsafe_allow_html=True,
    )


def render_metric_cards(report: CompletenessReport) -> None:
    """Render the top-level row count and discrepancy metrics."""
    c1, c2, c3, c4, c5, c6, c7 = st.columns(7)

    with c1:
        exp_display = str(report.expected_rows) if report.expected_rows is not None else "N/A"
        st.markdown(
            f"""
            <div class="metric-box">
                <div class="metric-title">Expected Rows</div>
                <div class="metric-val" style="color: #60A5FA;">{exp_display}</div>
                <div class="metric-sub" style="color: #94A3B8;">Target volume</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c2:
        st.markdown(
            f"""
            <div class="metric-box">
                <div class="metric-title">Received Rows</div>
                <div class="metric-val">{report.received_rows}</div>
                <div class="metric-sub" style="color: #94A3B8;">Source extracted</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c3:
        disc_color = "#F87171" if report.discrepancy > 0 else "#34D399"
        disc_text = f"-{report.discrepancy} Missing" if report.discrepancy > 0 else "0 (Complete)"
        st.markdown(
            f"""
            <div class="metric-box">
                <div class="metric-title">Discrepancy</div>
                <div class="metric-val" style="color: {disc_color};">{report.discrepancy}</div>
                <div class="metric-sub" style="color: {disc_color}; font-weight: 600;">{disc_text}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c4:
        dup_color = "#FBBF24" if report.duplicate_count > 0 else "#34D399"
        dup_sub = f"{report.duplicate_count} Isolated" if report.duplicate_count > 0 else "0 Duplicates"
        st.markdown(
            f"""
            <div class="metric-box">
                <div class="metric-title">Duplicates</div>
                <div class="metric-val" style="color: {dup_color};">{report.duplicate_count}</div>
                <div class="metric-sub" style="color: {dup_color}; font-weight: 600;">{dup_sub}</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c5:
        st.markdown(
            f"""
            <div class="metric-box">
                <div class="metric-title">Valid Rows</div>
                <div class="metric-val" style="color: #34D399;">{report.valid_rows}</div>
                <div class="metric-sub" style="color: #94A3B8;">Passed rules</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c6:
        rej_color = "#F87171" if report.rejected_rows > 0 else "#94A3B8"
        st.markdown(
            f"""
            <div class="metric-box">
                <div class="metric-title">Rejected Rows</div>
                <div class="metric-val" style="color: {rej_color};">{report.rejected_rows}</div>
                <div class="metric-sub" style="color: {rej_color};">Quarantined</div>
            </div>
            """,
            unsafe_allow_html=True,
        )

    with c7:
        load_color = "#34D399" if report.loaded_rows > 0 else "#94A3B8"
        st.markdown(
            f"""
            <div class="metric-box">
                <div class="metric-title">Loaded Rows</div>
                <div class="metric-val" style="color: {load_color};">{report.loaded_rows}</div>
                <div class="metric-sub" style="color: #94A3B8;">Committed to DB</div>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_status_banner(status: str, quality_score: float) -> None:
    """Render the primary outcome status badge."""
    if status == "PASSED":
        badge_cls = "status-passed"
        status_icon = "✅"
        msg = "All expected records were extracted, validated, and loaded idempotently. Data quality score: 100%."
    elif status == "RECOVERED":
        badge_cls = "status-recovered"
        status_icon = "🔄"
        msg = "Initial completeness discrepancy recovered via retry extraction. All records validated and committed."
    elif "DATA INCOMPLETE" in status:
        badge_cls = "status-incomplete"
        status_icon = "❌"
        msg = "Permanent completeness failure: source returned fewer records than expected. Retries exhausted; load halted."
    else:
        badge_cls = "status-failed"
        status_icon = "⚠️"
        msg = "Validation failed due to schema or constraint rejections. Data quarantined."

    st.markdown(
        f"""
        <div style="background: #0F172A; border: 1px solid rgba(255,255,255,0.08); border-radius: 12px; padding: 18px 24px; margin: 16px 0; display: flex; align-items: center; justify-content: space-between; flex-wrap: wrap; gap: 16px;">
            <div>
                <div style="font-size: 12px; color: #94A3B8; text-transform: uppercase; font-weight: 700; letter-spacing: 0.5px; margin-bottom: 6px;">Pipeline Final Status</div>
                <div class="status-badge-lg {badge_cls}">{status_icon} {html.escape(status)}</div>
                <div style="font-size: 13px; color: #CBD5E1; margin-top: 8px;">{html.escape(msg)}</div>
            </div>
            <div style="text-align: right;">
                <div style="font-size: 12px; color: #94A3B8; font-weight: 600;">Data Quality Score</div>
                <div style="font-size: 32px; font-weight: 800; color: {'#34D399' if quality_score >= 80 else ('#FBBF24' if quality_score >= 50 else '#F87171')};">
                    {quality_score:.1f}%
                </div>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_attempts_table(attempts: list[Any]) -> None:
    """Render retry attempts breakdown."""
    st.markdown("### 🔄 Extraction Retry Attempts & Outcomes")
    st.markdown(
        "<div style='color: #94A3B8; font-size: 13px; margin-bottom: 12px;'>"
        "Chronological audit of each extraction attempt, row counts, and recovery transitions."
        "</div>",
        unsafe_allow_html=True,
    )

    if not attempts:
        st.info("No retry attempts were executed for this run (initial attempt satisfied all criteria).")
        return

    records = []
    for a in attempts:
        row = a.to_dict() if hasattr(a, "to_dict") else dict(a)
        records.append({
            "Attempt #": f"Attempt {row.get('attempt_number', 0)}" + (" (Initial)" if row.get("attempt_number") == 0 else ""),
            "Timestamp": row.get("timestamp", "—"),
            "Received": row.get("received_rows", 0),
            "Expected": row.get("expected_rows", "—"),
            "Discrepancy": f"-{row.get('discrepancy', 0)}" if row.get("discrepancy", 0) > 0 else "0",
            "Duplicates": row.get("duplicate_count", 0),
            "Valid": row.get("valid_rows", 0),
            "Loaded": row.get("loaded_rows", 0),
            "Status": row.get("status", "—"),
            "Action Taken / Error": row.get("action_taken") or row.get("error_message") or "—",
        })

    df = pd.DataFrame(records)
    st.dataframe(df, use_container_width=True, hide_index=True)


def render_issues_table(issues: list[Any]) -> None:
    """Render the validation issues table with problems, affected items, and corrective actions."""
    st.markdown("### 📋 Validation Issues & Corrective Actions")
    st.markdown(
        "<div style='color: #94A3B8; font-size: 13px; margin-bottom: 12px;'>"
        "Identified defects, affected records or columns, and prescribed corrective actions."
        "</div>",
        unsafe_allow_html=True,
    )

    if not issues:
        st.markdown(
            """
            <div style="background: rgba(16, 185, 129, 0.1); border: 1px solid rgba(16, 185, 129, 0.3); border-radius: 8px; padding: 14px 18px; color: #34D399; font-size: 14px;">
                ✅ <strong>Zero Issues Detected</strong>: Expected volume, data types, required columns, and unique key constraints fully verified.
            </div>
            """,
            unsafe_allow_html=True,
        )
        return

    records = []
    for issue in issues:
        i = issue.to_dict() if hasattr(issue, "to_dict") else dict(issue)
        records.append({
            "Category": i.get("category", "GENERAL"),
            "Severity": i.get("severity", "ERROR"),
            "Problem Description": i.get("problem", ""),
            "Affected Column / Rows": i.get("affected", ""),
            "Recommended Corrective Action": i.get("corrective_action", ""),
        })

    df = pd.DataFrame(records)
    st.dataframe(
        df,
        use_container_width=True,
        hide_index=True,
        column_config={
            "Severity": st.column_config.TextColumn(width="small"),
            "Category": st.column_config.TextColumn(width="small"),
            "Problem Description": st.column_config.TextColumn(width="medium"),
            "Affected Column / Rows": st.column_config.TextColumn(width="small"),
            "Recommended Corrective Action": st.column_config.TextColumn(width="large"),
        },
    )


# =====================================================================
# Main Page View
# =====================================================================
def main() -> None:
    st.markdown("<h1 style='margin-bottom: 0.1rem;'>Data Quality & Recovery</h1>", unsafe_allow_html=True)
    st.markdown(
        "<div style='color: #94A3B8; margin-bottom: 1.25rem; font-size: 15px;'>"
        "Detect incomplete CSV & API extracts, evaluate unique key duplicates separately, "
        "manage automated extraction recovery, and audit resolution status."
        "</div>",
        unsafe_allow_html=True,
    )

    # Top Navigation Mode Selection
    mode = st.radio(
        "Audit Mode",
        ["🧪 Presentation / Demo Mode", "📊 Live Pipeline Audit"],
        horizontal=True,
        label_visibility="collapsed",
    )

    if "Presentation / Demo" in mode:
        st.markdown(
            """
            <div class="demo-banner">
                <div style="display: flex; align-items: center; gap: 12px;">
                    <span class="demo-tag">Presentation Demo</span>
                    <span style="color: #E2E8F0; font-size: 13px;">
                        Interactive demonstration of detecting incomplete datasets, separating duplicate checks, and orchestrating retries.
                    </span>
                </div>
                <div style="color: #94A3B8; font-size: 12px;">
                    🛡️ Database Safety: In-memory simulation. Zero modifications to live records.
                </div>
            </div>
            """,
            unsafe_allow_html=True,
        )

        st.markdown("#### Select Demonstration Scenario")
        scenario_choice = st.selectbox(
            "Scenario",
            [
                "🔴 Required Demo: Incomplete CSV/API Data (6 Expected, 4 Received -> Retries Exhaust -> FAILED — DATA INCOMPLETE)",
                "🟢 Transient Incomplete Data with Successful Recovery (6 Expected, 4 Received -> Retry 1 catches up -> RECOVERED)",
                "🟡 Incomplete Data with Distinct Duplicates (6 Expected, 5 Received with 1 Duplicate -> Independent checks)",
                "🔵 Complete Ingestion Baseline (6 Expected, 6 Received -> PASSED)",
                "⚙️ Custom Interactive Parameters",
            ],
            index=0,
            key="dqr_scenario_selector",
        )

        # Parameter mapping
        if "Required Demo" in scenario_choice:
            scenario_key = "incomplete_exhausted"
            exp_val = 6
            rec_val = 4
            max_r = 2
            st.caption(
                "**Scenario details**: 6 rows are expected from source, but only 4 rows are received. "
                "Discrepancy (2 missing) is flagged, duplicates are checked separately (0 duplicates). "
                "Extraction retries are executed up to the limit (2 retries). Because the source continues returning only 4 rows, "
                "the recovery stops and marks status **`FAILED — DATA INCOMPLETE`**."
            )
        elif "Successful Recovery" in scenario_choice:
            scenario_key = "incomplete_recovered"
            exp_val = 6
            rec_val = 4
            max_r = 2
            st.caption(
                "**Scenario details**: Initial extraction receives only 4 of 6 rows (e.g. source stream buffering or pagination delay). "
                "Completeness error is detected and retry 1 is dispatched. On retry 1, all 6 rows are successfully extracted, validated, "
                "and committed via idempotent load. Status marks **`RECOVERED`**."
            )
        elif "Distinct Duplicates" in scenario_choice:
            scenario_key = "incomplete_with_duplicates"
            exp_val = 6
            rec_val = 5
            max_r = 2
            st.caption(
                "**Scenario details**: 6 rows expected, 5 received (1 missing). Among the 5 received, 1 row has a duplicate `customer_id` (103). "
                "Demonstrates that duplicate checking is **strictly separate** and never treated as a substitute for missing records. "
                "Valid rows: 4, Rejected (duplicates): 1, Discrepancy: 1 missing."
            )
        elif "Complete Ingestion" in scenario_choice:
            scenario_key = "perfect_ingestion"
            exp_val = 6
            rec_val = 6
            max_r = 2
            st.caption(
                "**Scenario details**: Ideal ingestion baseline. All 6 expected rows arrive, 0 duplicates detected, schema and data types 100% valid. "
                "Status: **`PASSED`**."
            )
        else:
            # Custom Parameters
            scenario_key = "incomplete_exhausted"
            p1, p2, p3, p4 = st.columns(4)
            with p1:
                exp_val = st.number_input("Expected Row Count", min_value=1, max_value=100, value=6)
            with p2:
                rec_val = st.number_input("Initial Received Rows", min_value=1, max_value=100, value=4)
            with p3:
                max_r = st.number_input("Max Retries", min_value=1, max_value=5, value=2)
            with p4:
                sim_outcome = st.selectbox("Retry Outcome", ["Source Still Incomplete", "Source Recovers on Retry 1"])
            scenario_key = "incomplete_recovered" if "Recovers" in sim_outcome else "incomplete_exhausted"

        # Execute simulation
        report = simulate_recovery_workflow(
            scenario=scenario_key,
            expected_rows=exp_val,
            initial_received_rows=rec_val,
            max_retries=max_r,
            unique_key="customer_id",
        )

        st.divider()

        # 1. Metric Cards
        render_metric_cards(report)

        # 2. Status Banner
        render_status_banner(report.status, report.quality_score)

        # 3. Visual Workflow Pipeline
        st.markdown("### 🔀 Visual Execution & Recovery Workflow")
        st.markdown(
            "<div style='color: #94A3B8; font-size: 13px; margin-bottom: 8px;'>"
            "End-to-end execution path from source extraction through automated retry backoff to idempotent load verification."
            "</div>",
            unsafe_allow_html=True,
        )
        render_workflow_pipeline(report.workflow_stages)

        st.divider()

        # 4. Retry Attempts Table
        render_attempts_table(report.attempts)

        st.divider()

        # 5. Validation Issues Table
        render_issues_table(report.issues)

        # 6. Sample Data Inspector
        with st.expander("🔍 Inspect Extracted & Validated Sample Records", expanded=False):
            st.markdown(
                "<div style='color: #94A3B8; font-size: 13px; margin-bottom: 8px;'>"
                "Inspected batch records showing schema headers, types, and duplicate key flags."
                "</div>",
                unsafe_allow_html=True,
            )
            sample_records = [
                {"customer_id": 101, "name": "Arun", "email": "arun@example.com", "age": 24, "city": "Kochi", "status": "VALID"},
                {"customer_id": 102, "name": "Meera", "email": "meera@example.com", "age": 28, "city": "Trivandrum", "status": "VALID"},
                {"customer_id": 103, "name": "Rahul", "email": "rahul@example.com", "age": 22, "city": "Muvattupuzha", "status": "VALID"},
                {"customer_id": 104, "name": "Ananya", "email": "ananya@example.com", "age": 31, "city": "Calicut", "status": "VALID"},
            ]
            if report.received_rows > 4:
                sample_records.append({"customer_id": 105, "name": "Deepak", "email": "deepak@example.com", "age": 29, "city": "Thrissur", "status": "VALID"})
            if report.received_rows > 5:
                sample_records.append({"customer_id": 106, "name": "Pooja", "email": "pooja@example.com", "age": 26, "city": "Kottayam", "status": "VALID"})
            if scenario_key == "incomplete_with_duplicates":
                sample_records[-1] = {"customer_id": 103, "name": "Rahul", "email": "rahul@example.com", "age": 22, "city": "Muvattupuzha", "status": "DUPLICATE_KEY_REJECTED"}

            st.dataframe(pd.DataFrame(sample_records), use_container_width=True, hide_index=True)

    else:
        # Live Pipeline Audit Mode
        st.markdown(
            "<div style='color: #94A3B8; font-size: 13px; margin-bottom: 16px;'>"
            "Audit real pipeline execution history, inspect configured expected row counts, "
            "and examine quarantined rejection records directly from PostgreSQL."
            "</div>",
            unsafe_allow_html=True,
        )

        db = Database()
        try:
            db.connect()
            registered = queries.get_registered_pipelines(db)
            executions = queries.execution_history(db, limit=100)
        except Exception as exc:
            st.error(f"Database connection error: {exc}")
            return
        finally:
            db.close()

        if not executions:
            st.warning("No live pipeline execution records found in database.")
            return

        pipeline_names = sorted({r.get("pipeline_name") for r in executions if r.get("pipeline_name")})
        c1, c2 = st.columns([1.5, 1.5])
        with c1:
            selected_pipeline = st.selectbox("Select Pipeline", ["All Pipelines", *pipeline_names])
        with c2:
            filtered_execs = [
                e for e in executions
                if selected_pipeline == "All Pipelines" or e.get("pipeline_name") == selected_pipeline
            ]
            exec_options = [
                f"Exec #{e.get('execution_id')} - {e.get('pipeline_name')} ({e.get('status')}) - {e.get('start_time')}"
                for e in filtered_execs
            ]
            selected_exec_label = st.selectbox("Select Execution", exec_options) if exec_options else None

        if selected_exec_label:
            exec_id = int(selected_exec_label.split(" - ")[0].replace("Exec #", ""))
            curr_exec = next((e for e in filtered_execs if e.get("execution_id") == exec_id), None)
            if curr_exec:
                # Build CompletenessReport from real database row
                exp_rows = curr_exec.get("expected_rows")
                inp_rows = curr_exec.get("input_rows") or curr_exec.get("rows_processed", 0)
                val_rows = curr_exec.get("valid_rows") or curr_exec.get("rows_processed", 0)
                rej_rows = curr_exec.get("rejected_rows", 0)
                load_rows = curr_exec.get("loaded_rows") or curr_exec.get("rows_processed", 0)
                dup_rows = curr_exec.get("duplicate_count", 0)
                discrepancy = max(0, exp_rows - inp_rows) if exp_rows else 0
                q_score = float(curr_exec.get("data_quality_score") or 100.0)

                raw_status = (curr_exec.get("status") or "FAILED").upper()
                fail_cat = curr_exec.get("failure_category") or ""
                if "DATA_INCOMPLETE" in fail_cat:
                    disp_status = "FAILED — DATA INCOMPLETE"
                elif raw_status == "SUCCESS" and curr_exec.get("retry_count", 0) > 0:
                    disp_status = "RECOVERED"
                elif raw_status == "SUCCESS":
                    disp_status = "PASSED"
                elif discrepancy > 0:
                    disp_status = "FAILED — DATA INCOMPLETE"
                else:
                    disp_status = raw_status

                live_report = CompletenessReport(
                    expected_rows=exp_rows,
                    received_rows=inp_rows,
                    discrepancy=discrepancy,
                    is_complete=(discrepancy == 0),
                    duplicate_count=dup_rows,
                    valid_rows=val_rows,
                    rejected_rows=rej_rows,
                    loaded_rows=load_rows,
                    status=disp_status,
                    quality_score=q_score,
                )

                render_metric_cards(live_report)
                render_status_banner(disp_status, q_score)

                # Show real stage breakdown
                db_sub = Database()
                try:
                    db_sub.connect()
                    task_stages = db_sub.fetch_all(
                        "SELECT task_name, status, started_at, ended_at, error_message FROM pipeline_task_execution WHERE execution_id = %s ORDER BY task_id ASC",
                        (exec_id,),
                    )
                    rejected_data = db_sub.fetch_all(
                        "SELECT row_number, raw_data, rejection_reasons FROM pipeline_rejected_rows WHERE execution_id = %s ORDER BY row_number ASC LIMIT 50",
                        (exec_id,),
                    )
                except Exception:
                    task_stages = []
                    rejected_data = []
                finally:
                    db_sub.close()

                if task_stages:
                    st.markdown("### 🔀 Stage Task Breakdown (Recorded in PostgreSQL)")
                    st.dataframe(pd.DataFrame(task_stages), use_container_width=True, hide_index=True)

                if rejected_data:
                    st.markdown("### ⚠️ Quarantined Row Rejections")
                    st.dataframe(pd.DataFrame(rejected_data), use_container_width=True, hide_index=True)


if __name__ == "__main__":
    main()
