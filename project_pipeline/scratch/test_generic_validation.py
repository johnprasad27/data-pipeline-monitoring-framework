import json
import re
import pandas as pd
import numpy as np

SUPPORTED_DATA_TYPES = ("text", "integer", "decimal", "boolean", "date", "datetime")

def infer_column_type(series: pd.Series) -> str:
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

def validate_generic_csv_data(
    frame: pd.DataFrame,
    schema_config: dict,
):
    source = frame.copy().reset_index(drop=True)
    input_rows = len(source)
    columns_config = schema_config.get("columns", {})
    composite_unique_keys = schema_config.get("composite_unique_keys", [])

    # Missing columns
    missing_columns = [
        col for col, cfg in columns_config.items()
        if cfg.get("required", True) and col not in source.columns
    ]
    missing_column_count = len(missing_columns)
    missing_schema = bool(missing_column_count)

    # Extra columns
    extra_columns = [col for col in source.columns if col not in columns_config]
    warnings = []
    if extra_columns:
        warnings.append(f"Unexpected columns found in CSV: {', '.join(sorted(extra_columns))}")

    report = {
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
        report["reasons"].append(f"Missing required source columns: {', '.join(sorted(missing_columns))}")
        # Build rejected rows for all rows
        rejected_rows = [
            {
                "row_number": idx + 1,
                "raw_data": row.to_dict(),
                "rejection_reasons": [{"rule": "MISSING_REQUIRED_COLUMN", "message": f"Missing column: {col}", "column": col} for col in missing_columns]
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
    duplicate_masks = {}
    # Single column uniques
    for col, cfg in columns_config.items():
        if cfg.get("unique", False) and col in source.columns:
            duplicate_masks[f"unique_{col}"] = (source.duplicated(subset=[col], keep="first"), [col])
    # Composite keys
    for key_cols in composite_unique_keys:
        cols = [key_cols] if isinstance(key_cols, str) else list(key_cols)
        if all(c in source.columns for c in cols):
            duplicate_masks[f"composite_{'_'.join(cols)}"] = (source.duplicated(subset=cols, keep="first"), cols)

    valid_rows = []
    rejected_rows = []

    null_count = 0
    duplicate_count = 0
    datatype_error_count = 0
    business_rule_error_count = 0

    for idx, row in source.iterrows():
        row_num = idx + 1
        raw_dict = row.to_dict()
        errors = []
        row_warnings = []

        # Check duplicates
        for mask_key, (dup_mask, dup_cols) in duplicate_masks.items():
            if dup_mask.iloc[idx]:
                duplicate_count += 1
                errors.append({
                    "rule": "DUPLICATE_KEY",
                    "column": ", ".join(dup_cols),
                    "message": f"Duplicate value for unique key ({', '.join(dup_cols)})",
                    "severity": "ERROR",
                })

        # Column-by-column checks
        parsed_row = dict(raw_dict)
        for col, cfg in columns_config.items():
            val = row.get(col)
            severity = str(cfg.get("severity", "ERROR")).upper()
            is_null = pd.isna(val) or val is None or str(val).strip() == ""

            # Required / Null check
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
                continue

            # Type check
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

            # Format / Regex
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

            # Min / Max
            min_val = cfg.get("min_value")
            if min_val is not None:
                try:
                    if float(parsed_row.get(col, val)) < float(min_val):
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

            max_val = cfg.get("max_value")
            if max_val is not None:
                try:
                    if float(parsed_row.get(col, val)) > float(max_val):
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

            # Allowed values
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
        summary_reasons = []
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

# Run test on sample CSV
data = [
    [1, "Arun", "arun@example.com", 22, "Kochi"],
    [2, "Meera", "meera@example.com", 23, "Trivandrum"],
    [3, "Rahul", "rahul@example.com", "not-a-number", "Muvattupuzha"],
]
df = pd.DataFrame(data, columns=["customer_id", "name", "email", "age", "city"])
cfg = {
    "columns": {
        "customer_id": {"expected_type": "integer", "required": True, "unique": True},
        "name": {"expected_type": "text", "required": True},
        "email": {"expected_type": "text", "required": True},
        "age": {"expected_type": "integer", "required": True, "min_value": 0, "max_value": 120},
        "city": {"expected_type": "text", "required": False},
    }
}

val, rej, rep = validate_generic_csv_data(df, cfg)
print("Valid rows:", len(val))
print("Rejected rows:", len(rej))
print("Report:", rep)
print("Rejection details:", json.dumps(rej, indent=2))
assert rep["input_rows"] == rep["valid_rows"] + rep["rejected_rows"]
print("Reconciliation invariant PASS!")
