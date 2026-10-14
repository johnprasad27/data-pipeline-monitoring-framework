"""Configurable quality checks for CSV sources loaded into the sales table."""

from __future__ import annotations

import csv
import hashlib
import io
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

SUPPORTED_DATA_TYPES = ("text", "integer", "decimal", "boolean", "date", "datetime")
TARGET_FIELDS = ("customer_name", "product_name", "quantity", "amount", "sale_date")
FIELD_ALIASES = {
    "customer_name": ("customer_name", "customer_id"),
    "product_name": ("product_name", "product"),
    "quantity": ("quantity",),
    "amount": ("amount", "total_amount"),
    "sale_date": ("sale_date", "transaction_date"),
}


def detect_csv_encoding(raw_bytes: bytes) -> tuple[str, str]:
    """
    Attempts decoding raw bytes using utf-8-sig, utf-8, then latin1 fallback.
    Returns (decoded_text, encoding_name).
    """
    if raw_bytes.startswith(b"\xef\xbb\xbf"):
        try:
            return raw_bytes.decode("utf-8-sig"), "utf-8-sig"
        except Exception:
            pass
    try:
        return raw_bytes.decode("utf-8"), "utf-8"
    except UnicodeDecodeError:
        try:
            return raw_bytes.decode("latin1"), "latin1"
        except Exception as e:
            raise ValueError(f"Unsupported file encoding: {e}") from e


def infer_column_type(series: pd.Series) -> str:
    """Infer the likely data type of a column from non-empty sample values."""
    s = series.dropna()
    if s.empty:
        return "text"
    s_str = s.astype(str).str.strip()
    non_empty = s_str[s_str != ""]
    if non_empty.empty:
        return "text"

    # Check boolean
    lowered = non_empty.str.lower()
    unique_vals = set(lowered.unique())
    if unique_vals.issubset({"true", "false", "t", "f", "yes", "no"}) and any(
        v in {"true", "false", "yes", "no"} for v in unique_vals
    ):
        return "boolean"

    # Check numeric
    num = pd.to_numeric(non_empty, errors="coerce")
    if num.notna().all():
        has_dot = non_empty.str.contains(r"\.").any()
        if not has_dot and (num % 1 == 0).all():
            return "integer"
        return "decimal"

    # Check date / datetime
    try:
        if non_empty.str.contains(r"[-/:\s]").any():
            dt = pd.to_datetime(non_empty, format="mixed", errors="coerce")
            if dt.notna().all():
                has_time = ((dt.dt.hour != 0) | (dt.dt.minute != 0) | (dt.dt.second != 0)).any()
                return "datetime" if has_time else "date"
    except Exception:
        pass

    return "text"


def inspect_csv_source(
    source: bytes | bytearray | str | Path | Any,
    max_preview_rows: int = 10,
    max_file_size_bytes: int = 50 * 1024 * 1024,
) -> dict[str, Any]:
    """
    Inspect a CSV file or byte buffer:
      - Enforces size limit (< 50MB)
      - Detects encoding (utf-8, utf-8-sig, fallback latin1)
      - Detects column names and infers data types
      - Detects empty file, header-only, duplicate/blank headers, malformed rows
      - Returns preview rows, missing value counts, duplicate count, and content hash
    """
    if isinstance(source, (str, Path)):
        path = Path(source)
        if not path.is_file():
            return {"valid": False, "error": f"CSV file not found: {path}", "file_size_bytes": 0}
        raw_bytes = path.read_bytes()
    elif hasattr(source, "getvalue"):
        raw_bytes = source.getvalue()
    elif hasattr(source, "read"):
        raw_bytes = source.read()
    elif isinstance(source, (bytes, bytearray)):
        raw_bytes = bytes(source)
    else:
        return {"valid": False, "error": f"Unsupported source type: {type(source)}", "file_size_bytes": 0}

    file_size_bytes = len(raw_bytes)
    if not raw_bytes or not raw_bytes.strip():
        return {"valid": False, "error": "CSV file is empty.", "file_size_bytes": 0}

    if file_size_bytes > max_file_size_bytes:
        limit_mb = max_file_size_bytes // (1024 * 1024)
        return {
            "valid": False,
            "error": f"CSV file exceeds the maximum size limit of {limit_mb} MB.",
            "file_size_bytes": file_size_bytes,
        }

    try:
        decoded_text, encoding = detect_csv_encoding(raw_bytes)
    except Exception as exc:
        return {"valid": False, "error": str(exc), "file_size_bytes": file_size_bytes}

    content_hash = hashlib.sha256(raw_bytes).hexdigest()

    # Parse headers with csv.reader to detect blank or duplicate column names
    reader = csv.reader(io.StringIO(decoded_text))
    try:
        raw_header = next(reader, None)
    except Exception as exc:
        return {
            "valid": False,
            "error": f"Malformed CSV file: {exc}",
            "file_size_bytes": file_size_bytes,
            "encoding": encoding,
            "content_hash": content_hash,
        }

    if not raw_header:
        return {
            "valid": False,
            "error": "CSV file must contain a header row.",
            "file_size_bytes": file_size_bytes,
            "encoding": encoding,
            "content_hash": content_hash,
        }

    # Check for blank or unnamed headers
    if any(not str(c).strip() for c in raw_header):
        return {
            "valid": False,
            "error": "CSV file contains blank or unnamed column headers.",
            "file_size_bytes": file_size_bytes,
            "encoding": encoding,
            "content_hash": content_hash,
        }

    # Check for duplicate headers
    seen_headers = set()
    duplicate_headers = []
    for c in raw_header:
        c_clean = str(c).strip()
        if c_clean in seen_headers and c_clean not in duplicate_headers:
            duplicate_headers.append(c_clean)
        seen_headers.add(c_clean)
    if duplicate_headers:
        return {
            "valid": False,
            "error": f"CSV file contains duplicate column headers: {', '.join(duplicate_headers)}",
            "file_size_bytes": file_size_bytes,
            "encoding": encoding,
            "content_hash": content_hash,
        }

    headers = [str(c).strip() for c in raw_header]
    warnings: list[str] = []
    if encoding == "latin1":
        warnings.append("File decoded using fallback encoding: latin1 (ISO-8859-1).")

    # Read DataFrame via pandas
    try:
        df = pd.read_csv(io.StringIO(decoded_text), encoding=encoding)
    except Exception as exc:
        return {
            "valid": False,
            "error": f"Malformed CSV data: {exc}",
            "file_size_bytes": file_size_bytes,
            "encoding": encoding,
            "content_hash": content_hash,
        }

    total_rows = len(df)
    if total_rows == 0:
        warnings.append("CSV file contains a header row but no data rows.")

    inferred_types = {col: infer_column_type(df[col]) for col in headers if col in df.columns}
    missing_counts = {col: int(df[col].isna().sum()) for col in headers if col in df.columns}
    duplicate_count = int(df.duplicated().sum()) if total_rows > 0 else 0

    preview_df = df.head(max_preview_rows)
    preview_rows = preview_df.to_dict(orient="records")

    return {
        "valid": True,
        "error": None,
        "warnings": warnings,
        "encoding": encoding,
        "headers": headers,
        "inferred_types": inferred_types,
        "total_rows": total_rows,
        "missing_counts": missing_counts,
        "duplicate_count": duplicate_count,
        "preview_rows": preview_rows,
        "preview_df": preview_df,
        "file_size_bytes": file_size_bytes,
        "content_hash": content_hash,
    }


def generate_schema_rows(
    headers: list[str],
    inferred_types: dict[str, str],
    required_default: bool = True,
) -> list[dict[str, Any]]:
    """Helper function to build editable schema editor rows for each detected column."""
    return [
        {
            "Column": col,
            "Inferred Type": inferred_types.get(col, "text"),
            "Expected Type": inferred_types.get(col, "text"),
            "Required": required_default,
        }
        for col in headers
    ]


def default_csv_quality_config(columns: list[str]) -> dict[str, Any]:
    """Build a practical schema and sales-field mapping from CSV headers."""
    lowered = {column.lower(): column for column in columns}
    mapping = {
        target: next(
            (lowered[alias] for alias in aliases if alias in lowered),
            "",
        )
        for target, aliases in FIELD_ALIASES.items()
    }
    expected_types = {}
    for column in columns:
        normalized = column.strip().lower()
        if normalized in {"quantity"}:
            expected_types[column] = "integer"
        elif normalized.endswith("date") or normalized.endswith("_date"):
            expected_types[column] = "date"
        elif normalized in {"price", "amount", "total_amount"}:
            expected_types[column] = "number"
        else:
            expected_types[column] = "text"

    return {
        "source_columns": list(columns),
        "required_columns": list(columns),
        "expected_data_types": expected_types,
        "required_fields": list(columns),
        "duplicate_key_columns": [],
        "field_mapping": mapping,
        "price_column": lowered.get("price", ""),
        "total_amount_column": mapping["amount"],
    }


def validate_csv_data(
    frame: pd.DataFrame,
    quality_config: dict[str, Any] | None = None,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    """Validate source rows, map passing rows to the existing sales table shape."""
    source = frame.copy().reset_index(drop=True)
    quality_config = quality_config or default_csv_quality_config(list(source.columns))
    defaults = default_csv_quality_config(list(source.columns))
    required_columns = list(quality_config.get("required_columns", defaults["required_columns"]))
    expected_types = dict(quality_config.get("expected_data_types", defaults["expected_data_types"]))
    duplicate_keys = list(quality_config.get("duplicate_key_columns", []))
    field_mapping = dict(quality_config.get("field_mapping", defaults["field_mapping"]))
    required_fields = list(
        dict.fromkeys(
            [
                *quality_config.get("required_fields", defaults["required_fields"]),
                *(field_mapping.get(target, "") for target in TARGET_FIELDS),
            ]
        )
    )
    required_fields = [column for column in required_fields if column]

    missing_columns = set(required_columns) - set(source.columns)
    missing_columns.update(set(required_fields) - set(source.columns))
    missing_columns.update(set(expected_types) - set(source.columns))
    missing_columns.update(set(duplicate_keys) - set(source.columns))
    missing_mappings = [
        target for target in TARGET_FIELDS
        if not field_mapping.get(target) or field_mapping[target] not in source.columns
    ]
    missing_column_count = len(missing_columns)
    missing_schema = bool(missing_column_count or missing_mappings)
    input_rows = len(source)
    empty_target = pd.DataFrame(columns=TARGET_FIELDS)

    report: dict[str, Any] = {
        "input_rows": input_rows,
        "valid_rows": 0,
        "rejected_rows": input_rows,
        "loaded_rows": 0,
        "missing_column_count": missing_column_count,
        "null_count": 0,
        "duplicate_count": 0,
        "datatype_error_count": 0,
        "business_rule_error_count": 0,
        "data_quality_status": "FAIL" if missing_schema else "PASS",
        "reasons": [],
    }
    if missing_schema:
        if missing_columns:
            report["reasons"].append(
                "Missing required source columns: " + ", ".join(sorted(missing_columns))
            )
        if missing_mappings:
            report["reasons"].append(
                "Missing source mappings for: " + ", ".join(missing_mappings)
            )
        return empty_target, report

    invalid_rows = pd.Series(False, index=source.index)
    required_nulls = source[required_fields].isna()
    report["null_count"] = int(required_nulls.to_numpy().sum())
    null_rows = required_nulls.any(axis=1) if required_fields else invalid_rows.copy()
    invalid_rows |= null_rows

    datatype_rows = pd.Series(False, index=source.index)
    parsed_columns: dict[str, pd.Series] = {}
    type_errors: list[str] = []
    for column, expected_type in expected_types.items():
        values = source[column]
        normalized_type = str(expected_type).strip().lower()
        if normalized_type in {"text", "string", "str"}:
            parsed_columns[column] = values.astype("string")
            continue
        if normalized_type in {"integer", "int"}:
            numeric = pd.to_numeric(values, errors="coerce")
            bad = values.notna() & (
                numeric.isna()
                | ~np.isfinite(numeric.astype(float))
                | numeric.mod(1).ne(0)
            )
            parsed_columns[column] = numeric
        elif normalized_type in {"number", "numeric", "float", "decimal"}:
            numeric = pd.to_numeric(values, errors="coerce")
            bad = values.notna() & (
                numeric.isna() | ~np.isfinite(numeric.astype(float))
            )
            parsed_columns[column] = numeric
        elif normalized_type in {"date", "datetime"}:
            dates = pd.to_datetime(values, errors="coerce")
            bad = values.notna() & dates.isna()
            parsed_columns[column] = dates
        else:
            raise ValueError(f"Unsupported expected data type '{expected_type}' for {column}.")
        if bad.any():
            datatype_rows |= bad
            type_errors.append(column)

    report["datatype_error_count"] = int(datatype_rows.sum())
    invalid_rows |= datatype_rows
    if type_errors:
        report["reasons"].append(
            "Invalid expected data types in: " + ", ".join(type_errors)
        )

    duplicate_subset = duplicate_keys or None
    duplicate_rows = source.duplicated(subset=duplicate_subset, keep="first")
    report["duplicate_count"] = int(duplicate_rows.sum())
    invalid_rows |= duplicate_rows
    if duplicate_rows.any():
        report["reasons"].append(
            f"Found {report['duplicate_count']} duplicate rows"
        )

    business_rows = pd.Series(False, index=source.index)
    quantity_column = field_mapping["quantity"]
    amount_column = field_mapping["amount"]
    quantity = pd.to_numeric(source[quantity_column], errors="coerce")
    amount = pd.to_numeric(source[amount_column], errors="coerce")
    business_rows |= quantity.notna() & quantity.le(0)
    business_rows |= amount.notna() & amount.lt(0)

    price_column = quality_config.get("price_column", defaults["price_column"])
    total_amount_column = quality_config.get(
        "total_amount_column", defaults["total_amount_column"]
    )
    if price_column and price_column in source.columns:
        price = pd.to_numeric(source[price_column], errors="coerce")
        business_rows |= price.notna() & price.lt(0)
        if total_amount_column and total_amount_column in source.columns:
            total_amount = pd.to_numeric(source[total_amount_column], errors="coerce")
            comparable = quantity.notna() & price.notna() & total_amount.notna()
            business_rows |= comparable & ~np.isclose(
                total_amount,
                quantity * price,
                rtol=1e-6,
                atol=0.01,
            )

    report["business_rule_error_count"] = int(business_rows.sum())
    invalid_rows |= business_rows
    if business_rows.any():
        report["reasons"].append(
            f"Found {report['business_rule_error_count']} rows with business-rule violations"
        )

    valid_source = source.loc[~invalid_rows].copy()
    report["valid_rows"] = len(valid_source)
    report["rejected_rows"] = input_rows - report["valid_rows"]
    report["data_quality_status"] = "WARN" if report["rejected_rows"] else "PASS"

    mapped = pd.DataFrame(
        {
            target: valid_source[source_column].reset_index(drop=True)
            for target, source_column in field_mapping.items()
            if target in TARGET_FIELDS
        }
    )
    mapped = mapped.reindex(columns=TARGET_FIELDS)
    if not mapped.empty:
        mapped["customer_name"] = mapped["customer_name"].astype(str)
        mapped["product_name"] = mapped["product_name"].astype(str)
        mapped["quantity"] = pd.to_numeric(mapped["quantity"]).astype(int)
        mapped["amount"] = pd.to_numeric(mapped["amount"]).astype(float)
        mapped["sale_date"] = pd.to_datetime(mapped["sale_date"]).dt.date
    return mapped, report


def finalize_csv_quality(
    report: dict[str, Any],
    loaded_rows: int,
) -> tuple[dict[str, Any], str | None]:
    """Finalize load counts and report any validation/load reconciliation failure."""
    report["loaded_rows"] = loaded_rows
    report["rejected_rows"] += max(0, report["valid_rows"] - loaded_rows)
    errors = []
    if loaded_rows != report["valid_rows"]:
        errors.append(
            f"CSV load count mismatch: {report['valid_rows']} valid rows, {loaded_rows} loaded rows"
        )
    if report["input_rows"] != loaded_rows + report["rejected_rows"]:
        errors.append(
            "CSV row reconciliation failed: input_rows must equal loaded_rows + rejected_rows"
        )
    if errors:
        report["data_quality_status"] = "FAIL"
        report["reasons"].extend(errors)
        return report, "; ".join(errors)
    return report, None


def validate_schema_configuration(
    schema_config: dict[str, Any],
    available_columns: list[str] | None = None,
) -> tuple[bool, str | None]:
    """
    Validate that a schema configuration is well-formed:
      - All column names exist in available_columns (if provided)
      - Expected types are in SUPPORTED_DATA_TYPES
      - Regex patterns compile cleanly
      - Numeric min_value <= max_value
      - Unique key columns exist in available_columns
      - Severities are either ERROR or WARNING
    """
    import re

    if not isinstance(schema_config, dict):
        return False, "Schema configuration must be a dictionary."

    columns = schema_config.get("columns", {})
    if not isinstance(columns, dict) or not columns:
        return False, "Schema configuration must specify at least one column."

    available_set = set(available_columns) if available_columns is not None else None

    for col_name, col_rules in columns.items():
        if available_set is not None and col_name not in available_set:
            return False, f"Configured column '{col_name}' does not exist in the source CSV headers."

        if not isinstance(col_rules, dict):
            return False, f"Rules for column '{col_name}' must be an object."

        exp_type = str(col_rules.get("expected_type", "text")).lower()
        if exp_type not in SUPPORTED_DATA_TYPES:
            return (
                False,
                f"Unsupported expected type '{exp_type}' for column '{col_name}'. Must be one of: {', '.join(SUPPORTED_DATA_TYPES)}.",
            )

        severity = str(col_rules.get("severity", "ERROR")).upper()
        if severity not in {"ERROR", "WARNING"}:
            return False, f"Invalid severity '{severity}' for column '{col_name}'. Must be ERROR or WARNING."

        # Validate regex
        pattern = col_rules.get("regex_pattern")
        if pattern:
            try:
                re.compile(str(pattern))
            except re.error as exc:
                return False, f"Invalid regex pattern for column '{col_name}': {exc}"

        # Validate numeric min / max
        min_val = col_rules.get("min_value")
        max_val = col_rules.get("max_value")
        if min_val is not None and max_val is not None:
            try:
                if float(min_val) > float(max_val):
                    return (
                        False,
                        f"Minimum value ({min_val}) cannot be greater than maximum value ({max_val}) for column '{col_name}'.",
                    )
            except (ValueError, TypeError):
                return False, f"Invalid numeric min/max values for column '{col_name}'."

    # Validate composite unique keys
    composite_keys = schema_config.get("composite_unique_keys", [])
    if composite_keys:
        if not isinstance(composite_keys, list):
            return False, "composite_unique_keys must be a list."
        for key_def in composite_keys:
            key_cols = [key_def] if isinstance(key_def, str) else list(key_def)
            for k in key_cols:
                if available_set is not None and k not in available_set:
                    return False, f"Unique key column '{k}' does not exist in the source CSV headers."

    return True, None


def validate_generic_csv_data(
    frame: pd.DataFrame,
    schema_config: dict[str, Any],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    """
    Validate an arbitrary CSV DataFrame against a schema configuration:
      - Checks required vs missing columns (schema drift detection)
      - Detects unexpected/extra columns (recorded as WARNING by default)
      - Type validation (text, integer, decimal, boolean, date, datetime)
      - Null checks for required columns
      - Single and composite unique key checks
      - Min/max range checks, regex format checks, allowed-value lists
      - Tracks per-row rejection reasons with source row number and rule name
      - Honors rule severity: ERROR (rejects row) vs WARNING (logs warning, accepts row)
      - Enforces invariant: input_rows == valid_rows + rejected_rows
    """
    import re

    source = frame.copy().reset_index(drop=True)
    input_rows = len(source)
    columns_config = schema_config.get("columns", {})
    composite_unique_keys = schema_config.get("composite_unique_keys", [])

    # Check missing required columns
    missing_columns = [
        col for col, cfg in columns_config.items()
        if cfg.get("required", True) and col not in source.columns
    ]
    missing_column_count = len(missing_columns)
    missing_schema = bool(missing_column_count)

    # Detect unexpected extra columns
    extra_columns = [col for col in source.columns if col not in columns_config]
    warnings: list[str] = []
    if extra_columns:
        warnings.append(f"Unexpected extra columns found in CSV: {', '.join(sorted(extra_columns))}")

    report: dict[str, Any] = {
        "input_rows": input_rows,
        "valid_rows": 0,
        "rejected_rows": input_rows if missing_schema else 0,
        "loaded_rows": 0,
        "missing_column_count": missing_column_count,
        "null_count": 0,
        "duplicate_count": 0,
        "datatype_error_count": 0,
        "business_rule_error_count": 0,
        "data_quality_status": "FAIL" if missing_schema else "PASS",
        "reasons": [],
        "warnings": warnings,
    }

    if missing_schema:
        report["reasons"].append(f"Missing required columns: {', '.join(sorted(missing_columns))}")
        rejected_rows = [
            {
                "row_number": idx + 1,
                "raw_data": row.to_dict(),
                "rejection_reasons": [
                    {
                        "rule": "MISSING_REQUIRED_COLUMN",
                        "column": col,
                        "message": f"Missing required column: {col}",
                        "severity": "ERROR",
                    }
                    for col in missing_columns
                ],
            }
            for idx, row in source.iterrows()
        ]
        return [], rejected_rows, report

    # Compile regexes
    compiled_regexes = {}
    for col, cfg in columns_config.items():
        pattern = cfg.get("regex_pattern")
        if pattern:
            try:
                compiled_regexes[col] = re.compile(str(pattern))
            except Exception:
                pass

    # Duplicate detection masks
    duplicate_masks: dict[str, tuple[pd.Series, list[str]]] = {}
    for col, cfg in columns_config.items():
        if cfg.get("unique", False) and col in source.columns:
            duplicate_masks[f"unique_{col}"] = (source.duplicated(subset=[col], keep="first"), [col])
    for key_cols in composite_unique_keys:
        cols = [key_cols] if isinstance(key_cols, str) else list(key_cols)
        if all(c in source.columns for c in cols):
            duplicate_masks[f"composite_{'_'.join(cols)}"] = (source.duplicated(subset=cols, keep="first"), cols)

    valid_rows: list[dict[str, Any]] = []
    rejected_rows: list[dict[str, Any]] = []

    null_count = 0
    duplicate_count = 0
    datatype_error_count = 0
    business_rule_error_count = 0

    for idx, row in source.iterrows():
        row_num = idx + 1
        raw_dict = row.to_dict()
        errors: list[dict[str, Any]] = []
        row_warnings: list[dict[str, Any]] = []

        # Check unique key violations
        for mask_key, (dup_mask, dup_cols) in duplicate_masks.items():
            if dup_mask.iloc[idx]:
                duplicate_count += 1
                errors.append({
                    "rule": "DUPLICATE_KEY",
                    "column": ", ".join(dup_cols),
                    "message": f"Duplicate value for unique key ({', '.join(dup_cols)})",
                    "severity": "ERROR",
                })

        # Column-level validations
        parsed_row = dict(raw_dict)
        for col, cfg in columns_config.items():
            val = row.get(col)
            severity = str(cfg.get("severity", "ERROR")).upper()
            is_null = pd.isna(val) or val is None or str(val).strip() == ""

            # Required non-null check
            if cfg.get("required", False) and is_null:
                null_count += 1
                violation = {
                    "rule": "REQUIRED_NON_NULL",
                    "column": col,
                    "message": f"Column '{col}' is required but null or empty",
                    "severity": severity,
                }
                if severity == "ERROR":
                    errors.append(violation)
                else:
                    row_warnings.append(violation)
                continue

            if is_null:
                parsed_row[col] = None
                continue

            # Data type parsing
            exp_type = str(cfg.get("expected_type", "text")).lower()
            val_str = str(val).strip()
            type_err = False

            if exp_type in {"integer", "int"}:
                try:
                    num = float(val)
                    if num % 1 == 0 and "." not in val_str:
                        parsed_row[col] = int(num)
                    else:
                        type_err = True
                except (ValueError, TypeError):
                    type_err = True
            elif exp_type in {"decimal", "number", "float"}:
                try:
                    parsed_row[col] = float(val)
                except (ValueError, TypeError):
                    type_err = True
            elif exp_type in {"boolean", "bool"}:
                lowered = val_str.lower()
                if lowered in {"true", "t", "yes", "1"}:
                    parsed_row[col] = True
                elif lowered in {"false", "f", "no", "0"}:
                    parsed_row[col] = False
                else:
                    type_err = True
            elif exp_type in {"date", "datetime"}:
                date_fmt = cfg.get("date_format")
                try:
                    if date_fmt:
                        dt = pd.to_datetime(val_str, format=date_fmt)
                    else:
                        dt = pd.to_datetime(val_str, format="mixed")
                    parsed_row[col] = dt.strftime("%Y-%m-%d %H:%M:%S" if exp_type == "datetime" else "%Y-%m-%d")
                except Exception:
                    type_err = True
            else:
                parsed_row[col] = str(val)

            if type_err:
                datatype_error_count += 1
                violation = {
                    "rule": "INVALID_DATA_TYPE",
                    "column": col,
                    "message": f"Value '{val}' cannot be parsed as {exp_type}",
                    "severity": severity,
                }
                if severity == "ERROR":
                    errors.append(violation)
                else:
                    row_warnings.append(violation)
                continue

            # Regex pattern check
            if col in compiled_regexes:
                if not compiled_regexes[col].search(val_str):
                    business_rule_error_count += 1
                    violation = {
                        "rule": "REGEX_MISMATCH",
                        "column": col,
                        "message": f"Value '{val}' does not match pattern '{cfg['regex_pattern']}'",
                        "severity": severity,
                    }
                    if severity == "ERROR":
                        errors.append(violation)
                    else:
                        row_warnings.append(violation)

            # Min value check
            min_val = cfg.get("min_value")
            if min_val is not None:
                try:
                    cur_num = float(parsed_row.get(col, val))
                    if cur_num < float(min_val):
                        business_rule_error_count += 1
                        violation = {
                            "rule": "MIN_VALUE_EXCEEDED",
                            "column": col,
                            "message": f"Value {val} is below minimum {min_val}",
                            "severity": severity,
                        }
                        if severity == "ERROR":
                            errors.append(violation)
                        else:
                            row_warnings.append(violation)
                except Exception:
                    pass

            # Max value check
            max_val = cfg.get("max_value")
            if max_val is not None:
                try:
                    cur_num = float(parsed_row.get(col, val))
                    if cur_num > float(max_val):
                        business_rule_error_count += 1
                        violation = {
                            "rule": "MAX_VALUE_EXCEEDED",
                            "column": col,
                            "message": f"Value {val} is above maximum {max_val}",
                            "severity": severity,
                        }
                        if severity == "ERROR":
                            errors.append(violation)
                        else:
                            row_warnings.append(violation)
                except Exception:
                    pass

            # Allowed values check
            allowed_vals = cfg.get("allowed_values")
            if allowed_vals:
                allowed_set = {str(a).strip().lower() for a in allowed_vals}
                if val_str.lower() not in allowed_set:
                    business_rule_error_count += 1
                    violation = {
                        "rule": "ALLOWED_VALUES_VIOLATION",
                        "column": col,
                        "message": f"Value '{val}' is not in allowed list: {allowed_vals}",
                        "severity": severity,
                    }
                    if severity == "ERROR":
                        errors.append(violation)
                    else:
                        row_warnings.append(violation)

        if errors:
            rejected_rows.append({
                "row_number": row_num,
                "raw_data": raw_dict,
                "rejection_reasons": errors,
            })
        else:
            valid_rows.append(parsed_row)

    report["valid_rows"] = len(valid_rows)
    report["rejected_rows"] = len(rejected_rows)
    report["null_count"] = null_count
    report["duplicate_count"] = duplicate_count
    report["datatype_error_count"] = datatype_error_count
    report["business_rule_error_count"] = business_rule_error_count

    if report["rejected_rows"] > 0:
        report["data_quality_status"] = "WARN"
        summary_reasons: list[str] = []
        if null_count > 0:
            summary_reasons.append(f"Found {null_count} missing/null required values")
        if duplicate_count > 0:
            summary_reasons.append(f"Found {duplicate_count} duplicate key values")
        if datatype_error_count > 0:
            summary_reasons.append(f"Found {datatype_error_count} data type mismatches")
        if business_rule_error_count > 0:
            summary_reasons.append(f"Found {business_rule_error_count} business rule / constraint violations")
        report["reasons"].extend(summary_reasons)
    else:
        report["data_quality_status"] = "PASS"

    return valid_rows, rejected_rows, report