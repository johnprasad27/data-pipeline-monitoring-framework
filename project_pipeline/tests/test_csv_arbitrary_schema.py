"""
Comprehensive unit tests for arbitrary CSV schema refactoring.
Tests all 12 required scenarios without accessing or modifying live database data.
"""

from __future__ import annotations

import io
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from pipelines.csv_quality import (
    SUPPORTED_DATA_TYPES,
    detect_csv_encoding,
    finalize_csv_quality,
    generate_schema_rows,
    infer_column_type,
    inspect_csv_source,
    validate_generic_csv_data,
    validate_schema_configuration,
)
from pipelines.failure_handling import classify_failure
from pipelines.registry import validate_pipeline_registration


SAMPLE_ARUN_CSV = """customer_id,name,email,age,city
1,Arun,arun@example.com,22,Kochi
2,Meera,meera@example.com,23,Trivandrum
3,Rahul,rahul@example.com,21,Muvattupuzha
"""


# ---------------------------------------------------------------------------
# Test 1: Sample CSV (customer_id,name,email,age,city) works end to end
# ---------------------------------------------------------------------------
def test_sample_customer_csv_inspection_validation_and_reconciliation():
    raw_bytes = SAMPLE_ARUN_CSV.encode("utf-8")
    inspection = inspect_csv_source(raw_bytes)

    assert inspection["valid"] is True
    assert inspection["headers"] == ["customer_id", "name", "email", "age", "city"]
    assert inspection["total_rows"] == 3
    assert inspection["duplicate_count"] == 0
    assert inspection["inferred_types"]["customer_id"] == "integer"
    assert inspection["inferred_types"]["name"] == "text"
    assert inspection["inferred_types"]["email"] == "text"
    assert inspection["inferred_types"]["age"] == "integer"
    assert inspection["inferred_types"]["city"] == "text"

    schema_config = {
        "columns": {
            "customer_id": {"expected_type": "integer", "required": True, "unique": True},
            "name": {"expected_type": "text", "required": True},
            "email": {"expected_type": "text", "required": True},
            "age": {"expected_type": "integer", "required": True, "min_value": 0, "max_value": 120},
            "city": {"expected_type": "text", "required": True},
        }
    }

    df = pd.read_csv(io.StringIO(SAMPLE_ARUN_CSV))
    valid_rows, rejected_rows, report = validate_generic_csv_data(df, schema_config)

    assert len(valid_rows) == 3
    assert len(rejected_rows) == 0
    assert report["input_rows"] == 3
    assert report["valid_rows"] == 3
    assert report["rejected_rows"] == 0
    assert report["data_quality_status"] == "PASS"
    assert report["input_rows"] == report["valid_rows"] + report["rejected_rows"]

    finalized, error = finalize_csv_quality(report.copy(), loaded_rows=3)
    assert error is None
    assert finalized["loaded_rows"] == 3
    assert finalized["input_rows"] == finalized["loaded_rows"] + finalized["rejected_rows"]


# ---------------------------------------------------------------------------
# Test 2: CSVs with different column counts (3, 5, 35 columns)
# ---------------------------------------------------------------------------
def test_different_column_counts_and_names():
    # 3 columns
    csv_3 = "id,status,score\n1,active,95.5\n2,pending,80.0\n"
    insp_3 = inspect_csv_source(csv_3.encode("utf-8"))
    assert insp_3["valid"] is True
    assert insp_3["headers"] == ["id", "status", "score"]
    assert insp_3["inferred_types"]["id"] == "integer"
    assert insp_3["inferred_types"]["score"] == "decimal"

    # 35 columns
    headers_35 = [f"metric_col_{i}" for i in range(35)]
    row_vals = [str(i * 10) for i in range(35)]
    csv_35 = ",".join(headers_35) + "\n" + ",".join(row_vals) + "\n"
    insp_35 = inspect_csv_source(csv_35.encode("utf-8"))
    assert insp_35["valid"] is True
    assert len(insp_35["headers"]) == 35
    assert insp_35["headers"] == headers_35
    for h in headers_35:
        assert insp_35["inferred_types"][h] == "integer"


# ---------------------------------------------------------------------------
# Test 3: Missing required column -> classified failure
# ---------------------------------------------------------------------------
def test_missing_required_column_causes_classified_failure():
    # Schema requires 'email', but CSV does not have it
    csv_missing_email = "customer_id,name,age,city\n1,Arun,22,Kochi\n"
    df = pd.read_csv(io.StringIO(csv_missing_email))

    schema_config = {
        "columns": {
            "customer_id": {"expected_type": "integer", "required": True},
            "name": {"expected_type": "text", "required": True},
            "email": {"expected_type": "text", "required": True},
            "age": {"expected_type": "integer", "required": True},
            "city": {"expected_type": "text", "required": False},
        }
    }

    valid_rows, rejected_rows, report = validate_generic_csv_data(df, schema_config)

    assert len(valid_rows) == 0
    assert len(rejected_rows) == 1
    assert report["missing_column_count"] == 1
    assert report["data_quality_status"] == "FAIL"
    assert "email" in report["reasons"][0]

    failure_category, retryable = classify_failure("; ".join(report["reasons"]))
    assert failure_category == "DATA_QUALITY_ERROR"
    assert retryable is False


# ---------------------------------------------------------------------------
# Test 4: Invalid data types (e.g. age = "abc") -> rejected rows with reasons
# ---------------------------------------------------------------------------
def test_invalid_data_types_records_rejection_reasons_and_row_numbers():
    csv_bad_age = """customer_id,name,email,age,city
1,Arun,arun@example.com,22,Kochi
2,Meera,meera@example.com,abc,Trivandrum
3,Rahul,rahul@example.com,21,Muvattupuzha
"""
    df = pd.read_csv(io.StringIO(csv_bad_age))
    schema_config = {
        "columns": {
            "customer_id": {"expected_type": "integer", "required": True},
            "name": {"expected_type": "text", "required": True},
            "email": {"expected_type": "text", "required": True},
            "age": {"expected_type": "integer", "required": True},
            "city": {"expected_type": "text", "required": False},
        }
    }

    valid_rows, rejected_rows, report = validate_generic_csv_data(df, schema_config)

    assert len(valid_rows) == 2
    assert len(rejected_rows) == 1
    bad_record = rejected_rows[0]
    assert bad_record["row_number"] == 2
    assert bad_record["raw_data"]["name"] == "Meera"
    assert bad_record["raw_data"]["age"] == "abc"
    assert bad_record["rejection_reasons"][0]["rule"] == "INVALID_DATA_TYPE"
    assert bad_record["rejection_reasons"][0]["column"] == "age"
    assert "cannot be parsed as integer" in bad_record["rejection_reasons"][0]["message"]
    assert report["datatype_error_count"] == 1
    assert report["input_rows"] == report["valid_rows"] + report["rejected_rows"]


# ---------------------------------------------------------------------------
# Test 5: Duplicate single and composite unique keys
# ---------------------------------------------------------------------------
def test_duplicate_single_and_composite_unique_keys():
    # Single unique key duplicate on customer_id
    csv_single_dup = """customer_id,name,email,age,city
1,Arun,arun@example.com,22,Kochi
1,Arun_Duplicate,arun2@example.com,25,Kochi
"""
    df_single = pd.read_csv(io.StringIO(csv_single_dup))
    schema_single = {
        "columns": {
            "customer_id": {"expected_type": "integer", "required": True, "unique": True},
            "name": {"expected_type": "text", "required": True},
            "email": {"expected_type": "text", "required": True},
            "age": {"expected_type": "integer", "required": True},
            "city": {"expected_type": "text", "required": True},
        }
    }
    val, rej, rep = validate_generic_csv_data(df_single, schema_single)
    assert len(val) == 1
    assert len(rej) == 1
    assert rej[0]["row_number"] == 2
    assert rej[0]["rejection_reasons"][0]["rule"] == "DUPLICATE_KEY"
    assert rep["duplicate_count"] == 1

    # Composite unique key: (customer_id, city)
    csv_composite = """customer_id,name,email,age,city
1,Arun,arun@example.com,22,Kochi
1,Arun,arun@example.com,22,Trivandrum
1,Arun,arun@example.com,22,Kochi
"""
    df_composite = pd.read_csv(io.StringIO(csv_composite))
    schema_composite = {
        "columns": {
            "customer_id": {"expected_type": "integer", "required": True},
            "name": {"expected_type": "text", "required": True},
            "email": {"expected_type": "text", "required": True},
            "age": {"expected_type": "integer", "required": True},
            "city": {"expected_type": "text", "required": True},
        },
        "composite_unique_keys": [["customer_id", "city"]],
    }
    val_c, rej_c, rep_c = validate_generic_csv_data(df_composite, schema_composite)
    # Row 1 and Row 2 have different cities (Kochi vs Trivandrum), so both are valid.
    # Row 3 is duplicate of Row 1 (1, Kochi), so Row 3 is rejected.
    assert len(val_c) == 2
    assert len(rej_c) == 1
    assert rej_c[0]["row_number"] == 3


# ---------------------------------------------------------------------------
# Test 6: Optional column containing nulls -> accepted
# ---------------------------------------------------------------------------
def test_optional_column_with_nulls_is_accepted():
    csv_null_city = """customer_id,name,email,age,city
1,Arun,arun@example.com,22,Kochi
2,Meera,meera@example.com,23,
3,Rahul,rahul@example.com,21,
"""
    df = pd.read_csv(io.StringIO(csv_null_city), keep_default_na=False)
    schema_config = {
        "columns": {
            "customer_id": {"expected_type": "integer", "required": True},
            "name": {"expected_type": "text", "required": True},
            "email": {"expected_type": "text", "required": True},
            "age": {"expected_type": "integer", "required": True},
            "city": {"expected_type": "text", "required": False},
        }
    }
    valid_rows, rejected_rows, report = validate_generic_csv_data(df, schema_config)
    assert len(valid_rows) == 3
    assert len(rejected_rows) == 0
    assert valid_rows[1]["city"] is None
    assert valid_rows[2]["city"] is None
    assert report["null_count"] == 0
    assert report["data_quality_status"] == "PASS"


# ---------------------------------------------------------------------------
# Test 7: Schema drift between runs (added, removed, retyped)
# ---------------------------------------------------------------------------
def test_schema_drift_between_executions():
    base_schema = {
        "columns": {
            "customer_id": {"expected_type": "integer", "required": True},
            "name": {"expected_type": "text", "required": True},
            "age": {"expected_type": "integer", "required": True},
        }
    }

    # Column added: extra column 'loyalty_points' -> accepted with WARNING
    csv_added = "customer_id,name,age,loyalty_points\n1,Arun,22,500\n"
    df_added = pd.read_csv(io.StringIO(csv_added))
    val_a, rej_a, rep_a = validate_generic_csv_data(df_added, base_schema)
    assert len(val_a) == 1
    assert len(rej_a) == 0
    assert len(rep_a["warnings"]) == 1
    assert "loyalty_points" in rep_a["warnings"][0]

    # Required column removed: 'age' missing -> classified failure
    csv_removed = "customer_id,name\n1,Arun\n"
    df_removed = pd.read_csv(io.StringIO(csv_removed))
    val_r, rej_r, rep_r = validate_generic_csv_data(df_removed, base_schema)
    assert len(val_r) == 0
    assert len(rej_r) == 1
    assert rep_r["data_quality_status"] == "FAIL"

    # Retyped column: 'age' is now float '22.5' or string 'old' -> rejected rows with reasons
    csv_retyped = "customer_id,name,age\n1,Arun,old\n"
    df_retyped = pd.read_csv(io.StringIO(csv_retyped))
    val_t, rej_t, rep_t = validate_generic_csv_data(df_retyped, base_schema)
    assert len(val_t) == 0
    assert len(rej_t) == 1
    assert rej_t[0]["rejection_reasons"][0]["rule"] == "INVALID_DATA_TYPE"


# ---------------------------------------------------------------------------
# Test 8: Row reconciliation invariant holds across all scenarios
# ---------------------------------------------------------------------------
def test_row_reconciliation_invariant_in_all_cases():
    test_cases = [
        # (valid, rejected, input)
        pd.DataFrame({"id": [1, 2, 3], "val": [10, 20, 30]}),
        pd.DataFrame({"id": [1, 2, 3], "val": [10, "bad", 30]}),
        pd.DataFrame({"id": [1, 1, 2], "val": [10, 20, 30]}),
        pd.DataFrame({"id": [1, None, 3], "val": [10, 20, 30]}),
    ]
    cfg = {
        "columns": {
            "id": {"expected_type": "integer", "required": True, "unique": True},
            "val": {"expected_type": "integer", "required": True},
        }
    }
    for df in test_cases:
        v, r, rep = validate_generic_csv_data(df, cfg)
        assert rep["input_rows"] == rep["valid_rows"] + rep["rejected_rows"]
        # Finalizer check
        finalized, err = finalize_csv_quality(rep.copy(), loaded_rows=len(v))
        assert finalized["input_rows"] == finalized["loaded_rows"] + finalized["rejected_rows"]


# ---------------------------------------------------------------------------
# Test 9: Saved config and uploaded file persist and are readable
# ---------------------------------------------------------------------------
def test_saved_config_and_uploaded_file_persist(tmp_path):
    storage_dir = tmp_path / "storage"
    storage_dir.mkdir(parents=True)

    csv_content = SAMPLE_ARUN_CSV.encode("utf-8")
    inspection = inspect_csv_source(csv_content)

    file_name = f"customer_pipeline_{inspection['content_hash'][:12]}_abc123.csv"
    stored_path = storage_dir / file_name
    stored_path.write_bytes(csv_content)

    # Simulate deleting the upload object
    del csv_content

    # Verify persisted file is readable and content hash matches
    read_bytes = stored_path.read_bytes()
    assert stored_path.is_file()
    insp_persisted = inspect_csv_source(read_bytes)
    assert insp_persisted["valid"] is True
    assert insp_persisted["content_hash"] == inspection["content_hash"]
    assert insp_persisted["headers"] == ["customer_id", "name", "email", "age", "city"]


# ---------------------------------------------------------------------------
# Test 10: Legacy pipeline without stored schema still works
# ---------------------------------------------------------------------------
def test_legacy_pipeline_without_stored_schema_fallback():
    # Legacy sales DataFrame
    sales_df = pd.DataFrame(
        [
            ["Acme Corp", "Widget", 5, 25.0, "2026-10-01"],
            ["Global Tech", "Gadget", 10, 50.0, "2026-10-02"],
        ],
        columns=["customer_name", "product_name", "quantity", "amount", "sale_date"],
    )

    from pipelines.csv_quality import validate_csv_data, default_csv_quality_config
    legacy_cfg = default_csv_quality_config(list(sales_df.columns))

    # Calling legacy validator
    mapped, report = validate_csv_data(sales_df, legacy_cfg)
    assert list(mapped.columns) == ["customer_name", "product_name", "quantity", "amount", "sale_date"]
    assert report["valid_rows"] == 2
    assert report["rejected_rows"] == 0
    assert report["data_quality_status"] == "PASS"


# ---------------------------------------------------------------------------
# Test 11: Edge inputs: empty file, duplicate columns, bad encoding
# ---------------------------------------------------------------------------
def test_edge_inputs_inspection():
    # Empty file
    assert inspect_csv_source(b"")["valid"] is False
    assert "empty" in inspect_csv_source(b"")["error"].lower()

    # Whitespace only
    assert inspect_csv_source(b"   \n  ")["valid"] is False
    assert "empty" in inspect_csv_source(b"   \n  ")["error"].lower()

    # Header only
    insp_hdr = inspect_csv_source(b"col1,col2,col3\n")
    assert insp_hdr["valid"] is True
    assert insp_hdr["total_rows"] == 0
    assert len(insp_hdr["warnings"]) >= 1

    # Duplicate column names
    insp_dup = inspect_csv_source(b"col1,col2,col1\n1,2,3\n")
    assert insp_dup["valid"] is False
    assert "duplicate" in insp_dup["error"].lower()

    # Blank column names
    insp_blank = inspect_csv_source(b"col1,,col3\n1,2,3\n")
    assert insp_blank["valid"] is False
    assert "blank" in insp_blank["error"].lower()

    # UTF-8 with BOM
    bom_csv = b"\xef\xbb\xbfcustomer_id,name\n1,Arun\n"
    insp_bom = inspect_csv_source(bom_csv)
    assert insp_bom["valid"] is True
    assert insp_bom["encoding"] == "utf-8-sig"
    assert insp_bom["headers"] == ["customer_id", "name"]

    # Latin1 fallback encoding
    latin1_csv = "customer_id,name\n1,Ren\xe9\n".encode("latin1")
    insp_latin = inspect_csv_source(latin1_csv)
    assert insp_latin["valid"] is True
    assert insp_latin["encoding"] == "latin1"
    assert len(insp_latin["warnings"]) >= 1
    assert "latin1" in insp_latin["warnings"][0].lower()


# ---------------------------------------------------------------------------
# Test 12: Form-generation logic (schema-to-rows helper)
# ---------------------------------------------------------------------------
def test_form_generation_schema_to_rows_helper():
    headers = ["customer_id", "name", "email", "age", "city"]
    inferred_types = {
        "customer_id": "integer",
        "name": "text",
        "email": "text",
        "age": "integer",
        "city": "text",
    }

    schema_rows = generate_schema_rows(headers, inferred_types)
    assert len(schema_rows) == 5
    assert [r["Column"] for r in schema_rows] == headers
    assert [r["Expected Type"] for r in schema_rows] == ["integer", "text", "text", "integer", "text"]
    assert all(r["Required"] is True for r in schema_rows)

    # Config validation
    good_cfg = {
        "columns": {
            col: {"expected_type": inferred_types[col], "required": True}
            for col in headers
        }
    }
    is_valid, err = validate_schema_configuration(good_cfg, available_columns=headers)
    assert is_valid is True
    assert err is None

    # Config validation rejects invalid type
    bad_type_cfg = {
        "columns": {
            "customer_id": {"expected_type": "unknown_type", "required": True}
        }
    }
    is_valid_bad, err_bad = validate_schema_configuration(bad_type_cfg, available_columns=headers)
    assert is_valid_bad is False
    assert "Unsupported expected type" in err_bad

    # Config validation rejects min > max
    bad_bounds_cfg = {
        "columns": {
            "age": {"expected_type": "integer", "min_value": 100, "max_value": 10}
        }
    }
    is_valid_bounds, err_bounds = validate_schema_configuration(bad_bounds_cfg, available_columns=headers)
    assert is_valid_bounds is False
    assert "cannot be greater than maximum" in err_bounds
