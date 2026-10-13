"""Configurable quality checks for CSV sources loaded into the sales table."""

from __future__ import annotations

from typing import Any

import numpy as np
import pandas as pd

TARGET_FIELDS = ("customer_name", "product_name", "quantity", "amount", "sale_date")
FIELD_ALIASES = {
    "customer_name": ("customer_name", "customer_id"),
    "product_name": ("product_name", "product"),
    "quantity": ("quantity",),
    "amount": ("amount", "total_amount"),
    "sale_date": ("sale_date", "transaction_date"),
}


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