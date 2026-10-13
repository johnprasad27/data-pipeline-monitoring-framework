"""Register predefined CSV and API pipelines from the Streamlit application."""

from __future__ import annotations

import uuid
from io import BytesIO
from pathlib import Path

import pandas as pd
import streamlit as st

import config
from database.db import Database
from pipelines.csv_quality import TARGET_FIELDS, default_csv_quality_config
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
                    st.session_state.pop("pipeline_delete_confirmation", None)
                    st.session_state["pipeline_delete_success"] = (
                        f"Pipeline '{pending_delete}' was deleted. Execution history was retained."
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
csv_quality_config = None
csv_schema_error = None
with st.container(border=True):
    st.subheader("Pipeline configuration")
    csv_headers: list[str] = []
    default_quality_config: dict = {}
    if pipeline_type == "CSV":
        uploaded_csv = st.file_uploader("CSV file", type=["csv"])
        if uploaded_csv is not None:
            try:
                csv_headers = list(
                    pd.read_csv(BytesIO(uploaded_csv.getvalue()), nrows=0).columns
                )
                if not csv_headers:
                    csv_schema_error = "CSV file must contain a header row."
            except Exception as exc:
                csv_schema_error = f"Unable to read CSV headers: {exc}"

        if csv_headers:
            default_quality_config = default_csv_quality_config(csv_headers)
            schema_key = str(abs(hash(tuple(csv_headers))))
            st.markdown("**CSV schema and quality rules**")
            required_columns = st.multiselect(
                "Required source columns",
                csv_headers,
                default=csv_headers,
                key=f"csv_required_columns_{schema_key}",
            )
            required_fields = st.multiselect(
                "Required / non-null fields",
                csv_headers,
                default=csv_headers,
                key=f"csv_required_fields_{schema_key}",
            )
            duplicate_key_columns = st.multiselect(
                "Duplicate key columns (empty checks exact duplicate rows)",
                csv_headers,
                default=[],
                key=f"csv_duplicate_keys_{schema_key}",
            )

            expected_data_types = {}
            for column in csv_headers:
                type_options = ["text", "integer", "number", "date"]
                inferred_type = default_quality_config["expected_data_types"][column]
                type_columns = st.columns([2, 1])
                type_columns[0].write(column)
                expected_data_types[column] = type_columns[1].selectbox(
                    f"Expected type for {column}",
                    type_options,
                    index=type_options.index(inferred_type),
                    key=f"csv_type_{schema_key}_{column}",
                    label_visibility="collapsed",
                )

            field_mapping = {}
            mapping_options = ["Select source column", *csv_headers]
            for target in TARGET_FIELDS:
                aliases = {
                    "customer_name": ("customer_name", "customer_id"),
                    "product_name": ("product_name", "product"),
                    "quantity": ("quantity",),
                    "amount": ("amount", "total_amount"),
                    "sale_date": ("sale_date", "transaction_date"),
                }[target]
                inferred_source = default_quality_config["field_mapping"][target]
                default_index = (
                    mapping_options.index(inferred_source)
                    if inferred_source in mapping_options
                    else 0
                )
                selected_source = st.selectbox(
                    f"{target} source column",
                    mapping_options,
                    index=default_index,
                    key=f"csv_mapping_{schema_key}_{target}",
                )
                field_mapping[target] = (
                    "" if selected_source == "Select source column" else selected_source
                )

            price_options = ["None", *csv_headers]
            inferred_price = default_quality_config["price_column"]
            price_index = price_options.index(inferred_price) if inferred_price in price_options else 0
            price_column = st.selectbox(
                "Unit price column (optional)",
                price_options,
                index=price_index,
                key=f"csv_price_{schema_key}",
            )
            csv_quality_config = {
                "source_columns": csv_headers,
                "required_columns": required_columns,
                "expected_data_types": expected_data_types,
                "required_fields": required_fields,
                "duplicate_key_columns": duplicate_key_columns,
                "field_mapping": field_mapping,
                "price_column": "" if price_column == "None" else price_column,
                "total_amount_column": field_mapping["amount"],
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
    elif pipeline_type == "CSV" and csv_quality_config and any(
        not source_column
        for source_column in csv_quality_config["field_mapping"].values()
    ):
        st.error("Select a source column for each required sales field.")
    elif pipeline_type == "CSV" and uploaded_csv is not None and not uploaded_csv.getvalue():
        st.error("CSV file is empty.")
    elif pipeline_type == "API" and not api_url.strip():
        st.error("API URL is invalid.")
    elif pipeline_type == "CSV" and uploaded_csv is not None and not uploaded_csv.name.lower().endswith(".csv"):
        st.error("CSV file is required.")
    else:
        stored_file: Path | None = None
        source_config: dict[str, str]
        if pipeline_type == "CSV":
            storage_dir = config.BASE_DIR / "data" / "storage"
            storage_dir.mkdir(parents=True, exist_ok=True)
            stored_file = storage_dir / f"{uuid.uuid4().hex}.csv"
            stored_file.write_bytes(uploaded_csv.getvalue())
            source_config = {
                "file_path": stored_file.relative_to(config.BASE_DIR).as_posix(),
                "quality_config": csv_quality_config,
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