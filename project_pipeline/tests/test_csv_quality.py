import pandas as pd

from pipelines.csv_quality import (
    default_csv_quality_config,
    finalize_csv_quality,
    validate_csv_data,
)


def test_csv_quality_maps_sales_columns_and_reconciles_rejected_rows():
    frame = pd.DataFrame(
        [
            ["C-1", "2026-10-01", "Keyboard", 2, 10.0, 20.0],
            ["C-1", "2026-10-01", "Keyboard", 2, 10.0, 20.0],
            ["C-2", "2026-10-02", None, 1, 5.0, 5.0],
            ["C-3", "2026-10-03", "Mouse", "bad", 5.0, 5.0],
            ["C-4", "2026-10-04", "Cable", 0, 5.0, 5.0],
        ],
        columns=["customer_id", "transaction_date", "product", "quantity", "price", "total_amount"],
    )
    quality_config = default_csv_quality_config(list(frame.columns))
    quality_config["duplicate_key_columns"] = ["customer_id", "transaction_date", "product"]

    loaded_frame, report = validate_csv_data(frame, quality_config)

    assert list(loaded_frame.columns) == [
        "customer_name", "product_name", "quantity", "amount", "sale_date"
    ]
    assert report["input_rows"] == 5
    assert report["valid_rows"] == 1
    assert report["rejected_rows"] == 4
    assert report["loaded_rows"] == 0
    assert report["input_rows"] == report["valid_rows"] + report["rejected_rows"]
    assert report["duplicate_count"] == 1
    assert report["null_count"] == 1
    assert report["datatype_error_count"] == 1
    assert report["business_rule_error_count"] == 1
    assert report["data_quality_status"] == "WARN"


def test_csv_quality_fails_all_rows_when_required_column_is_missing():
    frame = pd.DataFrame({"customer_name": ["A"], "quantity": [1]})
    config = default_csv_quality_config(list(frame.columns))
    config["required_columns"].append("sale_date")

    mapped, report = validate_csv_data(frame, config)

    assert mapped.empty
    assert report["missing_column_count"] == 1
    assert report["input_rows"] == report["rejected_rows"] == 1
    assert report["data_quality_status"] == "FAIL"


def test_csv_quality_rejects_invalid_dates_and_total_mismatch():
    frame = pd.DataFrame(
        [["C-1", "not-a-date", "Keyboard", 2, 10.0, 19.0]],
        columns=["customer_id", "transaction_date", "product", "quantity", "price", "total_amount"],
    )

    _, report = validate_csv_data(frame, default_csv_quality_config(list(frame.columns)))

    assert report["valid_rows"] == 0
    assert report["rejected_rows"] == 1
    assert report["datatype_error_count"] == 1
    assert report["business_rule_error_count"] == 1


def test_csv_quality_finalizer_reconciles_and_fails_on_load_count_mismatch():
    report = {
        "input_rows": 5,
        "valid_rows": 3,
        "rejected_rows": 2,
        "loaded_rows": 0,
        "data_quality_status": "WARN",
        "reasons": [],
    }

    complete, complete_error = finalize_csv_quality(report.copy(), 3)
    incomplete, incomplete_error = finalize_csv_quality(report.copy(), 2)

    assert complete_error is None
    assert complete["input_rows"] == complete["loaded_rows"] + complete["rejected_rows"]
    assert incomplete_error is not None
    assert incomplete["data_quality_status"] == "FAIL"
    assert incomplete["input_rows"] == incomplete["loaded_rows"] + incomplete["rejected_rows"]


def test_csv_quality_finalizer_fails_when_loaded_count_exceeds_input():
    report = {
        "input_rows": 2,
        "valid_rows": 2,
        "rejected_rows": 0,
        "loaded_rows": 0,
        "data_quality_status": "PASS",
        "reasons": [],
    }

    finalized, error = finalize_csv_quality(report, 3)

    assert error is not None
    assert finalized["data_quality_status"] == "FAIL"
    assert finalized["input_rows"] != finalized["loaded_rows"] + finalized["rejected_rows"]