"""Register predefined CSV and API pipelines from the Streamlit application."""

from __future__ import annotations

import hashlib
import re
import uuid
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

import config
from database.db import Database
from pipelines.csv_quality import (
    SUPPORTED_DATA_TYPES,
    generate_schema_rows,
    inspect_csv_source,
    validate_schema_configuration,
)
from pipelines.registry import (
    delete_disabled_pipeline,
    initialize_pipeline_registry,
    register_pipeline,
    seed_pipeline_registry,
    set_pipeline_enabled,
)


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

st.title("Add Pipeline")
st.caption("Register a CSV or API pipeline for the existing scheduler.")
success_message = st.session_state.pop("pipeline_registration_success", None)
if success_message:
    st.success(success_message)
delete_success_message = st.session_state.pop("pipeline_delete_success", None)
if delete_success_message:
    st.success(delete_success_message)

try:
    registered_pipelines = initialize_pipeline_registry()
except Exception as exc:
    st.error(f"Unable to load the pipeline registry: {exc}")
    st.stop()

pending_delete = st.session_state.get("pipeline_delete_confirmation")
if pending_delete:
    pipeline_to_delete = next(
        (row for row in registered_pipelines if row["pipeline_name"] == pending_delete),
        None,
    )
    if pipeline_to_delete and not pipeline_to_delete["enabled"]:
        with st.container(border=True):
            st.warning(
                f"Delete registry entry '{pending_delete}'? Its execution history will be retained."
            )
            confirm_col, cancel_col = st.columns(2)
            if confirm_col.button(
                "Confirm delete",
                type="primary",
                key=f"confirm_delete_{pending_delete}",
            ):
                db = Database()
                try:
                    db.connect()
                    delete_disabled_pipeline(db, pending_delete)
                    st.cache_data.clear()
                    st.session_state.pop("pipeline_delete_confirmation", None)
                    st.session_state.pop("dashboard_snapshot", None)
                    st.session_state.pop("last_dashboard_event_key", None)
                    if st.session_state.get("dashboard_pipeline_filter") == pending_delete:
                        st.session_state["dashboard_pipeline_filter"] = "All Pipelines"
                    if st.session_state.get("dashboard_filter") == pending_delete:
                        st.session_state.pop("dashboard_filter", None)
                    if st.session_state.get("history_pipeline_filter") == pending_delete:
                        st.session_state["history_pipeline_filter"] = "All Pipelines"

                    # Check if pipeline is defined in static configuration file
                    yaml_note = ""
                    try:
                        from pipelines.registry import load_pipeline_registry
                        import config
                        yaml_configs = load_pipeline_registry(config.PIPELINES_CONFIG_PATH)
                        if pending_delete in yaml_configs:
                            yaml_note = f" (Note: '{pending_delete}' remains listed in pipelines.yaml, but its database tombstone prevents it from being scheduled or resurrected.)"
                    except Exception:
                        pass

                    st.session_state["pipeline_delete_success"] = (
                        f"Pipeline '{pending_delete}' was deleted. Execution history was retained.{yaml_note}"
                    )
                    st.rerun()
                except Exception as exc:
                    st.error(f"Unable to delete pipeline: {exc}")
                finally:
                    db.close()
            if cancel_col.button("Cancel", key=f"cancel_delete_{pending_delete}"):
                st.session_state.pop("pipeline_delete_confirmation", None)
                st.rerun()
    else:
        st.session_state.pop("pipeline_delete_confirmation", None)

pipeline_type = st.segmented_control(
    "Pipeline type",
    options=["CSV", "API"],
    default="CSV",
    key="new_pipeline_type",
)

uploaded_csv = None
api_url = ""
csv_inspection: dict | None = None
csv_schema_config: dict | None = None
csv_schema_error: str | None = None

with st.container(border=True):
    st.subheader("Pipeline configuration")
    if pipeline_type == "CSV":
        uploaded_csv = st.file_uploader("CSV file", type=["csv"], key="uploaded_csv_uploader")
        if uploaded_csv is not None:
            csv_inspection = inspect_csv_source(uploaded_csv.getvalue())
            if not csv_inspection["valid"]:
                csv_schema_error = csv_inspection.get("error") or "Invalid CSV file."
                st.error(f"❌ {csv_schema_error}")
            else:
                file_hash = csv_inspection["content_hash"][:12]
                st.session_state["last_csv_inspection"] = csv_inspection

                # Quality / file stats
                st.markdown(
                    f"**Encoding:** `{csv_inspection['encoding']}` &nbsp;|&nbsp; "
                    f"**Total Rows:** `{csv_inspection['total_rows']:,}` &nbsp;|&nbsp; "
                    f"**Columns:** `{len(csv_inspection['headers'])}` &nbsp;|&nbsp; "
                    f"**Duplicate Rows:** `{csv_inspection['duplicate_count']:,}`"
                )
                for w in csv_inspection.get("warnings", []):
                    st.warning(f"⚠️ {w}")

                # Preview expander
                preview_count = min(10, len(csv_inspection["preview_rows"]))
                with st.expander(f"Data Preview (First {preview_count} rows)", expanded=False):
                    st.dataframe(csv_inspection["preview_df"], hide_index=True, width="stretch")

                # Schema table
                st.markdown("**Detected Column Schema & Type Configuration**")
                st.caption(
                    "Review inferred data types and required constraints. You can override expected types directly in the table."
                )

                # Build schema rows using helper
                initial_schema_records = [
                    {
                        "Column": col,
                        "Inferred Type": csv_inspection["inferred_types"].get(col, "text"),
                        "Expected Type": csv_inspection["inferred_types"].get(col, "text"),
                        "Required": True,
                        "Missing Values": csv_inspection["missing_counts"].get(col, 0),
                    }
                    for col in csv_inspection["headers"]
                ]
                schema_df = pd.DataFrame(initial_schema_records)

                # Deriving widget key from file_hash guarantees fresh state for different files
                editor_key = f"schema_editor_{file_hash}"
                edited_schema_df = st.data_editor(
                    schema_df,
                    column_config={
                        "Column": st.column_config.TextColumn("Column", disabled=True),
                        "Inferred Type": st.column_config.TextColumn("Inferred Type", disabled=True),
                        "Expected Type": st.column_config.SelectboxColumn(
                            "Expected Type",
                            options=list(SUPPORTED_DATA_TYPES),
                            required=True,
                        ),
                        "Required": st.column_config.CheckboxColumn("Required / Non-Null", default=True),
                        "Missing Values": st.column_config.NumberColumn("Missing Values", disabled=True),
                    },
                    hide_index=True,
                    key=editor_key,
                    width="stretch",
                )

                # Unique key selection (single or composite)
                unique_keys_key = f"unique_keys_{file_hash}"
                selected_unique_keys = st.multiselect(
                    "Unique Key / Identifier Columns (Single or Composite)",
                    options=csv_inspection["headers"],
                    default=[],
                    key=unique_keys_key,
                    help="Select column(s) that identify uniqueness. If multiple columns are chosen, they form a composite unique key.",
                )

                # Advanced optional rules
                with st.expander("Advanced Column Rules (Optional formats, regex, ranges, allowed values)", expanded=False):
                    st.caption(
                        "Configure optional validation constraints per column. No rules are auto-applied."
                    )
                    rule_col = st.selectbox(
                        "Select column to configure rules",
                        options=["-- Select a column --", *csv_inspection["headers"]],
                        key=f"adv_col_picker_{file_hash}",
                    )
                    adv_rules_store = st.session_state.setdefault(f"adv_rules_{file_hash}", {})

                    if rule_col != "-- Select a column --":
                        curr_rule = adv_rules_store.get(rule_col, {})
                        r_col1, r_col2 = st.columns(2)
                        with r_col1:
                            r_regex = st.text_input(
                                f"Regex pattern for {rule_col}",
                                value=curr_rule.get("regex_pattern") or "",
                                placeholder="e.g. ^[a-z0-9_]+$",
                                key=f"regex_{file_hash}_{rule_col}",
                            )
                            r_date_fmt = st.text_input(
                                f"Date format for {rule_col}",
                                value=curr_rule.get("date_format") or "",
                                placeholder="e.g. %Y-%m-%d",
                                key=f"date_fmt_{file_hash}_{rule_col}",
                            )
                            r_severity = st.radio(
                                f"Rule severity for {rule_col}",
                                options=["ERROR", "WARNING"],
                                index=0 if curr_rule.get("severity", "ERROR") == "ERROR" else 1,
                                horizontal=True,
                                key=f"severity_{file_hash}_{rule_col}",
                                help="ERROR rejects invalid rows; WARNING logs the issue while accepting the row.",
                            )
                        with r_col2:
                            r_min = st.text_input(
                                f"Numeric minimum for {rule_col}",
                                value=str(curr_rule.get("min_value")) if curr_rule.get("min_value") is not None else "",
                                placeholder="e.g. 0",
                                key=f"min_{file_hash}_{rule_col}",
                            )
                            r_max = st.text_input(
                                f"Numeric maximum for {rule_col}",
                                value=str(curr_rule.get("max_value")) if curr_rule.get("max_value") is not None else "",
                                placeholder="e.g. 100",
                                key=f"max_{file_hash}_{rule_col}",
                            )
                            r_allowed = st.text_input(
                                f"Allowed values (comma-separated) for {rule_col}",
                                value=", ".join(curr_rule.get("allowed_values", [])) if curr_rule.get("allowed_values") else "",
                                placeholder="e.g. active, pending, closed",
                                key=f"allowed_{file_hash}_{rule_col}",
                            )

                        # Update in session state store
                        adv_rules_store[rule_col] = {
                            "regex_pattern": r_regex.strip() if r_regex.strip() else None,
                            "date_format": r_date_fmt.strip() if r_date_fmt.strip() else None,
                            "min_value": float(r_min.strip()) if r_min.strip() else None,
                            "max_value": float(r_max.strip()) if r_max.strip() else None,
                            "allowed_values": [item.strip() for item in r_allowed.split(",") if item.strip()] if r_allowed.strip() else None,
                            "severity": r_severity,
                        }

                # Construct final schema_config
                columns_map = {}
                adv_rules = st.session_state.get(f"adv_rules_{file_hash}", {})
                for _, r in edited_schema_df.iterrows():
                    c_name = str(r["Column"])
                    c_adv = adv_rules.get(c_name, {})
                    columns_map[c_name] = {
                        "expected_type": str(r["Expected Type"]).lower(),
                        "required": bool(r["Required"]),
                        "unique": c_name in selected_unique_keys,
                        "min_value": c_adv.get("min_value"),
                        "max_value": c_adv.get("max_value"),
                        "date_format": c_adv.get("date_format"),
                        "regex_pattern": c_adv.get("regex_pattern"),
                        "allowed_values": c_adv.get("allowed_values"),
                        "severity": c_adv.get("severity", "ERROR"),
                    }

                csv_schema_config = {
                    "columns": columns_map,
                    "composite_unique_keys": [selected_unique_keys] if len(selected_unique_keys) > 1 else [],
                }

    with st.form("register_pipeline_form"):
        pipeline_name = st.text_input("Pipeline name", placeholder="customer_pipeline")
        if pipeline_type == "API":
            api_url = st.text_input(
                "API URL",
                placeholder="https://example.com/api/data",
                help="Use an HTTP or HTTPS endpoint that returns the expected sales data format.",
            )

        settings_left, settings_right = st.columns(2)
        with settings_left:
            interval_seconds = st.number_input(
                "Execution interval (seconds)", min_value=1, value=60, step=1
            )
        with settings_right:
            max_retries = st.number_input("Maximum retries", min_value=0, value=3, step=1)
        enabled = st.checkbox("Enabled", value=True)
        submitted = st.form_submit_button(
            "Register pipeline", type="primary", icon=":material/add_circle:"
        )

if submitted:
    name = pipeline_name.strip()
    if any(row["pipeline_name"].lower() == name.lower() for row in registered_pipelines):
        st.error("Pipeline name already exists.")
    elif csv_schema_error:
        st.error(csv_schema_error)
    elif pipeline_type == "CSV" and uploaded_csv is None:
        st.error("CSV file is required.")
    elif pipeline_type == "CSV" and uploaded_csv is not None and not uploaded_csv.getvalue():
        st.error("CSV file is empty.")
    elif pipeline_type == "CSV" and uploaded_csv is not None and not uploaded_csv.name.lower().endswith(".csv"):
        st.error("CSV file is required.")
    elif pipeline_type == "API" and not api_url.strip():
        st.error("API URL is invalid.")
    else:
        # Validate schema configuration if CSV
        if pipeline_type == "CSV" and csv_schema_config:
            is_valid_cfg, cfg_err = validate_schema_configuration(
                csv_schema_config,
                available_columns=csv_inspection["headers"] if csv_inspection else None,
            )
            if not is_valid_cfg:
                st.error(f"Invalid schema configuration: {cfg_err}")
                st.stop()

        stored_file: Path | None = None
        source_config: dict[str, Any]
        if pipeline_type == "CSV":
            storage_dir = config.BASE_DIR / "data" / "storage"
            storage_dir.mkdir(parents=True, exist_ok=True)
            safe_name = re.sub(r"[^A-Za-z0-9_-]", "_", name) or "csv_pipeline"
            content_hash = csv_inspection["content_hash"] if csv_inspection else hashlib.sha256(uploaded_csv.getvalue()).hexdigest()
            file_name = f"{safe_name}_{content_hash[:12]}_{uuid.uuid4().hex[:8]}.csv"
            stored_file = storage_dir / file_name
            stored_file.write_bytes(uploaded_csv.getvalue())
            source_config = {
                "file_path": stored_file.relative_to(config.BASE_DIR).as_posix(),
                "original_filename": uploaded_csv.name,
                "content_hash": content_hash,
                "file_size_bytes": csv_inspection["file_size_bytes"] if csv_inspection else len(uploaded_csv.getvalue()),
                "schema_config": csv_schema_config,
            }
        else:
            source_config = {"url": api_url.strip()}

        db = Database()
        try:
            db.connect()
            db.ensure_monitoring_schema()
            seed_pipeline_registry(db)
            registration = register_pipeline(
                db,
                name,
                pipeline_type,
                source_config,
                int(interval_seconds),
                int(max_retries),
                enabled,
            )
            st.session_state["pipeline_registration_success"] = (
                f"Pipeline registered successfully: {registration['name']} "
                f"({registration['pipeline_type']}, {registration['interval_seconds']} seconds, "
                f"{registration['max_retries']} retries, "
                f"{'enabled' if registration['enabled'] else 'disabled'})."
            )
            st.rerun()
        except ValueError as exc:
            if stored_file is not None:
                stored_file.unlink(missing_ok=True)
            st.error(str(exc))
        except Exception as exc:
            if stored_file is not None:
                stored_file.unlink(missing_ok=True)
            st.error(f"Unable to register pipeline: {exc}")
        finally:
            db.close()

st.subheader("Registered pipelines")
if registered_pipelines:
    table = pd.DataFrame(
        [
            {
                "Pipeline": row["pipeline_name"],
                "Type": row["pipeline_type"],
                "Interval": f"{row['interval_seconds']}s",
                "Retries": row["max_retries"],
                "Enabled": "Yes" if row["enabled"] else "No",
                "Created": row["created_at"],
            }
            for row in registered_pipelines
        ]
    )
    st.dataframe(table, hide_index=True, width="stretch")

    for row in registered_pipelines:
        name = row["pipeline_name"]
        left, toggle_column, delete_column = st.columns(
            [4, 1, 1], vertical_alignment="center"
        )
        left.markdown(f"**{name}**  ·  {row['pipeline_type']}  ·  {'Enabled' if row['enabled'] else 'Disabled'}")
        button_label = "Disable" if row["enabled"] else "Enable"
        if toggle_column.button(button_label, key=f"toggle_pipeline_{name}"):
            db = Database()
            try:
                db.connect()
                set_pipeline_enabled(db, name, not row["enabled"])
                st.rerun()
            except Exception as exc:
                st.error(f"Unable to update {name}: {exc}")
            finally:
                db.close()
        if not row["enabled"] and delete_column.button(
            "Delete", key=f"delete_pipeline_{name}"
        ):
            st.session_state["pipeline_delete_confirmation"] = name
            st.rerun()
else:
    st.info("No pipelines are registered yet.")