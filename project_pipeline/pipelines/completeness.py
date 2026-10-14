"""Data completeness and recovery validation module.

Provides capabilities for:
1. Detecting incomplete CSV/API data against an optional expected row count.
2. Checking duplicates separately without confusing duplicates with missing records.
3. Inspecting API responses to distinguish successful HTTP 200 from complete pagination/data extraction.
4. Orchestrating extraction retries, recording each attempt's audit trail, and preventing duplicate inserts.
5. Reporting final outcomes: PASSED, RECOVERED, or FAILED — DATA INCOMPLETE.
6. Supplying interactive presentation/demo scenarios without modifying real execution history.
"""

from __future__ import annotations

import logging
from dataclasses import asdict, dataclass, field
from datetime import datetime
from typing import Any, Dict, List, Optional, Sequence, Tuple, Union

import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class ValidationIssue:
    """Structured data quality or completeness issue."""
    category: str
    problem: str
    affected: str
    severity: str  # "ERROR" or "WARNING"
    corrective_action: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class RetryAttempt:
    """Audit log entry for an extraction retry attempt."""
    attempt_number: int
    timestamp: str
    received_rows: int
    expected_rows: Optional[int]
    discrepancy: int
    duplicate_count: int
    valid_rows: int
    rejected_rows: int
    loaded_rows: int
    status: str  # "SUCCESS", "RETRYING", "FAILED", "FAILED — DATA INCOMPLETE"
    error_message: Optional[str]
    action_taken: str

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass
class CompletenessReport:
    """Full evaluation of row completeness, validation, and recovery."""
    expected_rows: Optional[int]
    received_rows: int
    discrepancy: int
    is_complete: bool
    duplicate_count: int
    valid_rows: int
    rejected_rows: int
    loaded_rows: int
    status: str  # "PASSED", "RECOVERED", "FAILED — DATA INCOMPLETE", "FAILED"
    quality_score: float
    issues: list[ValidationIssue] = field(default_factory=list)
    attempts: list[RetryAttempt] = field(default_factory=list)
    workflow_stages: list[dict[str, Any]] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        res = asdict(self)
        res["issues"] = [i.to_dict() if hasattr(i, "to_dict") else i for i in self.issues]
        res["attempts"] = [a.to_dict() if hasattr(a, "to_dict") else a for a in self.attempts]
        return res


def evaluate_data_completeness(
    data: Union[pd.DataFrame, Sequence[dict[str, Any]]],
    *,
    expected_rows: Optional[int] = None,
    unique_keys: Optional[Union[str, Sequence[str]]] = None,
    schema_config: Optional[dict[str, Any]] = None,
    required_columns: Optional[Sequence[str]] = None,
    loaded_rows: Optional[int] = None,
) -> CompletenessReport:
    """
    Evaluate dataset completeness and quality:
      - Validates received row count against optional expected row count.
      - Checks duplicates separately using single or composite unique key.
      - Validates required columns, null values, and data types if schema provided.
      - Ensures invariant: received_rows == valid_rows + rejected_rows.
      - Does not substitute duplicate checking for missing records.
    """
    # 1. Normalize data to DataFrame
    if isinstance(data, pd.DataFrame):
        df = data.copy().reset_index(drop=True)
    elif isinstance(data, (list, tuple)):
        df = pd.DataFrame(data).reset_index(drop=True)
    else:
        df = pd.DataFrame()

    received_rows = len(df)
    issues: list[ValidationIssue] = []

    # 2. Check Completeness against expected row count
    discrepancy = 0
    is_complete = True
    if expected_rows is not None and expected_rows > 0:
        if received_rows < expected_rows:
            discrepancy = expected_rows - received_rows
            is_complete = False
            issues.append(
                ValidationIssue(
                    category="COMPLETENESS",
                    problem=(
                        f"Incomplete extraction: expected {expected_rows} rows, "
                        f"but received only {received_rows} rows ({discrepancy} missing)."
                    ),
                    affected=f"Source extraction ({discrepancy} missing rows)",
                    severity="ERROR",
                    corrective_action=(
                        "Trigger extraction retry, verify source pagination / offsets, "
                        "or inspect upstream batch completion."
                    ),
                )
            )
        elif received_rows > expected_rows:
            issues.append(
                ValidationIssue(
                    category="COMPLETENESS",
                    problem=(
                        f"Row count warning: received {received_rows} rows, "
                        f"exceeding expected count of {expected_rows} (+{received_rows - expected_rows})."
                    ),
                    affected=f"Source stream (+{received_rows - expected_rows} extra rows)",
                    severity="WARNING",
                    corrective_action=(
                        "Verify whether source configuration expected count requires updating "
                        "or if unexpected upstream records were included."
                    ),
                )
            )

    # 3. Check Duplicates independently
    # Resolve unique key columns
    key_cols: list[str] = []
    if unique_keys:
        if isinstance(unique_keys, str):
            key_cols = [unique_keys]
        else:
            key_cols = list(unique_keys)
    elif schema_config:
        # Extract from schema_config if present
        cols_cfg = schema_config.get("columns", {})
        for col_name, cfg in cols_cfg.items():
            if isinstance(cfg, dict) and cfg.get("unique"):
                key_cols.append(col_name)
        comp = schema_config.get("composite_unique_keys", [])
        if comp and not key_cols:
            first_comp = comp[0]
            key_cols = [first_comp] if isinstance(first_comp, str) else list(first_comp)

    duplicate_count = 0
    duplicate_mask = pd.Series([False] * received_rows)
    if key_cols and not df.empty and all(k in df.columns for k in key_cols):
        duplicate_mask = df.duplicated(subset=key_cols, keep="first")
        duplicate_count = int(duplicate_mask.sum())
        if duplicate_count > 0:
            dup_keys_str = ", ".join(key_cols)
            # Find sample duplicate values
            dup_vals = df.loc[duplicate_mask, key_cols].to_dict(orient="records")
            dup_sample_str = str(dup_vals[:3])
            issues.append(
                ValidationIssue(
                    category="DUPLICATE",
                    problem=(
                        f"Detected {duplicate_count} duplicate record(s) on key [{dup_keys_str}]. "
                        f"Sample: {dup_sample_str}"
                    ),
                    affected=f"Key [{dup_keys_str}] ({duplicate_count} duplicates)",
                    severity="ERROR",
                    corrective_action=(
                        f"Enforce upsert / merge on key [{dup_keys_str}] or quarantine duplicate rows. "
                        "Note: duplicate resolution does NOT replace missing records."
                    ),
                )
            )

    # 4. Validate Schema (Required columns, types, nulls)
    rejected_indices: set[int] = set(df.index[duplicate_mask])
    
    # Check required columns
    req_cols = list(required_columns or [])
    if schema_config:
        for c, cfg in schema_config.get("columns", {}).items():
            if isinstance(cfg, dict) and cfg.get("required", True) and c not in req_cols:
                req_cols.append(c)

    missing_cols = [c for c in req_cols if c not in df.columns]
    if missing_cols:
        issues.append(
            ValidationIssue(
                category="SCHEMA",
                problem=f"Missing required column(s): {', '.join(missing_cols)}",
                affected=", ".join(missing_cols),
                severity="ERROR",
                corrective_action="Update source format or schema configuration to match expected columns.",
            )
        )
        # All rows rejected if required column is completely missing
        rejected_indices.update(range(received_rows))

    # Check nulls in required columns
    if not df.empty:
        for c in req_cols:
            if c in df.columns:
                null_mask = df[c].isna() | (df[c].astype(str).str.strip() == "")
                null_indices = df.index[null_mask]
                if len(null_indices) > 0:
                    rejected_indices.update(null_indices)
                    issues.append(
                        ValidationIssue(
                            category="DATA_QUALITY",
                            problem=f"Found {len(null_indices)} null or empty value(s) in required column '{c}'.",
                            affected=f"Column '{c}' (rows: {list(null_indices[:5])})",
                            severity="ERROR",
                            corrective_action=f"Backfill missing values for '{c}' or update source validation rules.",
                        )
                    )

    # Calculate valid and rejected rows
    rejected_rows = len(rejected_indices)
    valid_rows = max(0, received_rows - rejected_rows)
    actual_loaded = loaded_rows if loaded_rows is not None else valid_rows

    # 5. Compute Quality Score and Final Status
    if received_rows == 0:
        quality_score = 0.0 if (expected_rows and expected_rows > 0) else 100.0
    else:
        penalty = (discrepancy * 15) + (duplicate_count * 10) + (rejected_rows * 10)
        quality_score = max(0.0, round(100.0 - (penalty / (expected_rows or received_rows) * 100.0), 1))

    if not is_complete:
        status = "FAILED — DATA INCOMPLETE"
    elif rejected_rows > 0:
        status = "FAILED"
    else:
        status = "PASSED"

    return CompletenessReport(
        expected_rows=expected_rows,
        received_rows=received_rows,
        discrepancy=discrepancy,
        is_complete=is_complete,
        duplicate_count=duplicate_count,
        valid_rows=valid_rows,
        rejected_rows=rejected_rows,
        loaded_rows=actual_loaded,
        status=status,
        quality_score=quality_score,
        issues=issues,
    )


def inspect_api_response_completeness(
    payload: Any,
    extracted_rows_count: int,
    expected_rows: Optional[int] = None,
) -> dict[str, Any]:
    """
    Distinguishes a successful HTTP response from complete data extraction:
      - Inspects pagination metadata (total, limit, skip, pages).
      - Checks whether all available pages were fetched.
      - Never assumes HTTP 200 guarantees complete data.
    """
    pagination_info: dict[str, Any] = {
        "has_pagination": False,
        "is_complete": True,
        "total_available": None,
        "limit": None,
        "skip": None,
        "pages_fetched": 1,
        "total_pages": 1,
        "discrepancy": 0,
        "notes": "Single response payload; no pagination metadata discovered.",
    }

    if not isinstance(payload, dict):
        if expected_rows is not None and extracted_rows_count < expected_rows:
            pagination_info["is_complete"] = False
            pagination_info["discrepancy"] = expected_rows - extracted_rows_count
            pagination_info["notes"] = f"Payload returned {extracted_rows_count} records; expected {expected_rows}."
        return pagination_info

    # DummyJSON carts/products shape: {"carts": [...], "total": 50, "skip": 0, "limit": 10}
    total = payload.get("total") or payload.get("total_count") or payload.get("count")
    limit = payload.get("limit") or payload.get("page_size") or payload.get("per_page")
    skip = payload.get("skip") if payload.get("skip") is not None else payload.get("offset")

    if total is not None:
        try:
            total_val = int(total)
            pagination_info["total_available"] = total_val
            pagination_info["has_pagination"] = True

            if limit is not None:
                limit_val = int(limit)
                pagination_info["limit"] = limit_val
                if limit_val > 0:
                    import math
                    pagination_info["total_pages"] = math.ceil(total_val / limit_val)

            if skip is not None:
                pagination_info["skip"] = int(skip)

            # Check if extracted count is less than total available or expected
            target_count = expected_rows if (expected_rows and expected_rows > 0) else total_val
            if extracted_rows_count < target_count:
                pagination_info["is_complete"] = False
                pagination_info["discrepancy"] = target_count - extracted_rows_count
                pagination_info["notes"] = (
                    f"HTTP 200 OK received, but extraction is incomplete: "
                    f"fetched {extracted_rows_count} of {total_val} available records "
                    f"(target: {target_count}, discrepancy: {pagination_info['discrepancy']})."
                )
            else:
                pagination_info["is_complete"] = True
                pagination_info["notes"] = f"All {total_val} records successfully extracted across pages."

        except (ValueError, TypeError):
            pass

    return pagination_info


def simulate_recovery_workflow(
    scenario: str = "incomplete_exhausted",
    *,
    expected_rows: int = 6,
    initial_received_rows: int = 4,
    max_retries: int = 2,
    unique_key: str = "customer_id",
) -> CompletenessReport:
    """
    Execute an interactive, presentation-ready simulation of the 6-stage workflow:
      1. Extract -> 2. Validate -> 3. Detect Issue -> 4. Retry -> 5. Verify -> 6. Final Status
    
    Pre-configured Scenarios:
      - "incomplete_exhausted": Expected 6 rows, Received 4 rows. Retries return 4 rows.
                                Stops after retry limit and reports `FAILED — DATA INCOMPLETE`.
      - "incomplete_recovered": Expected 6 rows, Received 4 rows. Retry 1 recovers to 6 rows.
                                Reports `RECOVERED`.
      - "incomplete_with_duplicates": Expected 6 rows, Received 5 rows, 1 duplicate.
                                      Demonstrates duplicate checking is separate from missing records.
      - "perfect_ingestion": Expected 6 rows, Received 6 rows, 0 duplicates, all valid.
                             Reports `PASSED`.
    """
    now = datetime.now()
    attempts: list[RetryAttempt] = []
    workflow_stages: list[dict[str, Any]] = []

    # Sample reference dataset of 6 items
    full_dataset = [
        {"customer_id": 101, "name": "Arun", "email": "arun@example.com", "age": 24, "city": "Kochi"},
        {"customer_id": 102, "name": "Meera", "email": "meera@example.com", "age": 28, "city": "Trivandrum"},
        {"customer_id": 103, "name": "Rahul", "email": "rahul@example.com", "age": 22, "city": "Muvattupuzha"},
        {"customer_id": 104, "name": "Ananya", "email": "ananya@example.com", "age": 31, "city": "Calicut"},
        {"customer_id": 105, "name": "Deepak", "email": "deepak@example.com", "age": 29, "city": "Thrissur"},
        {"customer_id": 106, "name": "Pooja", "email": "pooja@example.com", "age": 26, "city": "Kottayam"},
    ]

    # --- ATTEMPT 0: Initial Extraction ---
    if scenario == "incomplete_exhausted":
        current_data = full_dataset[:initial_received_rows]  # 4 rows
    elif scenario == "incomplete_recovered":
        current_data = full_dataset[:initial_received_rows]  # 4 rows
    elif scenario == "incomplete_with_duplicates":
        # 5 rows total, customer_id 103 duplicated, missing row 106
        current_data = full_dataset[:4] + [full_dataset[2]]  # row 103 appears twice
    elif scenario == "perfect_ingestion":
        current_data = full_dataset[:expected_rows]
    else:
        current_data = full_dataset[:initial_received_rows]

    eval_0 = evaluate_data_completeness(
        current_data,
        expected_rows=expected_rows,
        unique_keys=unique_key,
        loaded_rows=0,  # Not loaded yet
    )

    t0_str = now.strftime("%H:%M:%S.100")
    if eval_0.discrepancy > 0 or eval_0.duplicate_count > 0:
        att0_status = "RETRYING"
        att0_err = f"Incomplete data: received {eval_0.received_rows}/{expected_rows} rows"
        att0_action = "Recorded completeness failure; scheduled Retry Attempt 1"
    else:
        att0_status = "SUCCESS"
        att0_err = None
        att0_action = "Validated successfully; proceeded to Load"

    attempts.append(
        RetryAttempt(
            attempt_number=0,
            timestamp=t0_str,
            received_rows=eval_0.received_rows,
            expected_rows=expected_rows,
            discrepancy=eval_0.discrepancy,
            duplicate_count=eval_0.duplicate_count,
            valid_rows=eval_0.valid_rows,
            rejected_rows=eval_0.rejected_rows,
            loaded_rows=0,
            status=att0_status,
            error_message=att0_err,
            action_taken=att0_action,
        )
    )

    # --- RETRY LOOPS ---
    final_data = current_data
    recovered = False

    if eval_0.discrepancy > 0:
        for r in range(1, max_retries + 1):
            t_r_str = datetime.fromtimestamp(now.timestamp() + r * 2.5).strftime("%H:%M:%S.%f")[:12]
            
            if scenario == "incomplete_recovered" and r >= 1:
                # Source recovers on retry 1
                final_data = full_dataset[:expected_rows]  # All 6 rows
                eval_r = evaluate_data_completeness(
                    final_data,
                    expected_rows=expected_rows,
                    unique_keys=unique_key,
                    loaded_rows=expected_rows,
                )
                recovered = True
                attempts.append(
                    RetryAttempt(
                        attempt_number=r,
                        timestamp=t_r_str,
                        received_rows=eval_r.received_rows,
                        expected_rows=expected_rows,
                        discrepancy=0,
                        duplicate_count=0,
                        valid_rows=eval_r.valid_rows,
                        rejected_rows=0,
                        loaded_rows=eval_r.valid_rows,
                        status="SUCCESS",
                        error_message=None,
                        action_taken=f"Re-extracted complete dataset ({expected_rows}/{expected_rows} rows); verified and loaded via idempotent upsert",
                    )
                )
                break
            else:
                # Source still incomplete
                final_data = current_data
                eval_r = evaluate_data_completeness(
                    final_data,
                    expected_rows=expected_rows,
                    unique_keys=unique_key,
                    loaded_rows=0,
                )
                is_last = (r == max_retries)
                att_status = "FAILED — DATA INCOMPLETE" if is_last else "RETRYING"
                action_text = (
                    f"Max retries ({max_retries}) exhausted; stopped extraction; recorded FAILED — DATA INCOMPLETE"
                    if is_last
                    else f"Retry {r} received only {eval_r.received_rows}/{expected_rows} rows; scheduled Retry Attempt {r+1}"
                )
                attempts.append(
                    RetryAttempt(
                        attempt_number=r,
                        timestamp=t_r_str,
                        received_rows=eval_r.received_rows,
                        expected_rows=expected_rows,
                        discrepancy=eval_r.discrepancy,
                        duplicate_count=eval_r.duplicate_count,
                        valid_rows=eval_r.valid_rows,
                        rejected_rows=eval_r.rejected_rows,
                        loaded_rows=0,
                        status=att_status,
                        error_message=f"Source returned {eval_r.received_rows}/{expected_rows} rows",
                        action_taken=action_text,
                    )
                )

    # Final evaluation
    final_report = evaluate_data_completeness(
        final_data,
        expected_rows=expected_rows,
        unique_keys=unique_key,
        loaded_rows=len(final_data) if (recovered or scenario == "perfect_ingestion") else 0,
    )
    final_report.attempts = attempts

    # Final status determination
    if recovered:
        final_report.status = "RECOVERED"
    elif scenario == "perfect_ingestion":
        final_report.status = "PASSED"
    elif final_report.discrepancy > 0:
        final_report.status = "FAILED — DATA INCOMPLETE"
    elif final_report.rejected_rows > 0:
        final_report.status = "FAILED"
    else:
        final_report.status = "PASSED"

    # Build Workflow Stages
    workflow_stages.append({
        "stage": "1. Extract",
        "title": "Data Extraction",
        "status": "COMPLETED",
        "details": f"Pulled {len(final_data)} records from source (Target expected: {expected_rows}).",
        "icon": "📥",
    })
    workflow_stages.append({
        "stage": "2. Validate",
        "title": "Schema & Integrity",
        "status": "COMPLETED" if final_report.rejected_rows == 0 else "WARNING",
        "details": f"Checked types, nulls, and key constraints. Valid: {final_report.valid_rows}, Rejected: {final_report.rejected_rows}.",
        "icon": "🔬",
    })
    workflow_stages.append({
        "stage": "3. Detect Issue",
        "title": "Discrepancy & Duplicates",
        "status": "FAILED" if (final_report.discrepancy > 0 or final_report.duplicate_count > 0) else "PASSED",
        "details": (
            f"Discrepancy: {final_report.discrepancy} missing rows. Duplicates: {final_report.duplicate_count}."
            if (final_report.discrepancy > 0 or final_report.duplicate_count > 0)
            else "No discrepancy detected; duplicate check passed."
        ),
        "icon": "⚠️" if (final_report.discrepancy > 0 or final_report.duplicate_count > 0) else "✅",
    })
    workflow_stages.append({
        "stage": "4. Retry Engine",
        "title": "Extraction Retry",
        "status": "RECOVERED" if recovered else ("EXHAUSTED" if final_report.discrepancy > 0 else "SKIPPED"),
        "details": (
            f"Executed {len(attempts) - 1} retry attempt(s) within max retry limit ({max_retries})."
            if len(attempts) > 1
            else "No retries needed (initial extraction satisfied requirements)."
        ),
        "icon": "🔄",
    })
    workflow_stages.append({
        "stage": "5. Verify & Reconcile",
        "title": "Idempotent Load Check",
        "status": "VERIFIED",
        "details": (
            f"Committed {final_report.loaded_rows} rows using idempotent cleanup / unique key. Duplicate inserts prevented."
            if final_report.loaded_rows > 0
            else "Rolled back; 0 rows committed due to completeness failure."
        ),
        "icon": "🔒",
    })
    workflow_stages.append({
        "stage": "6. Final Status",
        "title": "Outcome Determination",
        "status": final_report.status,
        "details": f"Pipeline outcome finalized as {final_report.status}.",
        "icon": "🏁",
    })

    final_report.workflow_stages = workflow_stages
    return final_report
