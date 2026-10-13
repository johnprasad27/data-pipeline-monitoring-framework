"""Classification helpers for pipeline failures and retry decisions."""

from __future__ import annotations

import socket
import re
from typing import Any

import psycopg2
import requests

from api.api_client import APIError
from database.db import DatabaseError
FAILURE_CATEGORIES = frozenset({
    "NETWORK_ERROR",
    "API_ERROR",
    "TIMEOUT",
    "DATABASE_ERROR",
    "FILE_ERROR",
    "DATA_QUALITY_ERROR",
    "UNKNOWN",
})

RETRYABLE_DATABASE_MESSAGES = (
    "could not connect",
    "connection refused",
    "connection reset",
    "server closed the connection",
    "deadlock detected",
    "serialization failure",
    "connection timed out",
)


def classify_failure(error: Any, *, default_retryable: bool = False) -> tuple[str, bool]:
    """Classify a pipeline failure, returning the category and retry policy."""
    original_error = error
    message = str(error or "").strip().lower()
    if message == "unknown_error":
        return "UNKNOWN", default_retryable
    if message in FAILURE_CATEGORIES:
        category = message.upper()
        retryable = (
            default_retryable
            if category == "UNKNOWN"
            else category in {"NETWORK_ERROR", "TIMEOUT"}
        )
        return category, retryable

    cause = error if isinstance(error, BaseException) else None
    while isinstance(cause, BaseException):
        if isinstance(cause, (requests.Timeout, TimeoutError)):
            return "TIMEOUT", True
        if isinstance(cause, (requests.ConnectionError, socket.gaierror)):
            return "NETWORK_ERROR", True
        if isinstance(cause, requests.HTTPError):
            status_code = (
                cause.response.status_code
                if cause.response is not None
                else _http_status_code(str(cause).lower())
            )
            return "API_ERROR", status_code == 429 or (
                status_code is not None and status_code >= 500
            )
        if isinstance(cause, FileNotFoundError):
            return "FILE_ERROR", False
        if isinstance(cause, (psycopg2.OperationalError, psycopg2.InterfaceError)):
            cause_message = str(cause).lower()
            retryable = any(token in cause_message for token in RETRYABLE_DATABASE_MESSAGES)
            return "DATABASE_ERROR", retryable
        if isinstance(cause, psycopg2.Error):
            if getattr(cause, "pgcode", None) in {"40001", "40P01"}:
                return "DATABASE_ERROR", True
            return "DATABASE_ERROR", False
        if isinstance(cause, DatabaseError):
            return "DATABASE_ERROR", False
        cause = cause.__cause__ or cause.__context__

    if not message:
        return "UNKNOWN", default_retryable

    text = message
    if isinstance(original_error, (requests.Timeout, TimeoutError)):
        return "TIMEOUT", True
    if isinstance(original_error, (requests.ConnectionError, socket.gaierror, ConnectionError)):
        return "NETWORK_ERROR", True
    if isinstance(original_error, FileNotFoundError):
        return "FILE_ERROR", False
    if isinstance(original_error, (psycopg2.Error, DatabaseError)):
        retryable = any(token in text for token in RETRYABLE_DATABASE_MESSAGES)
        return "DATABASE_ERROR", retryable

    if "http error" in text:
        status_code = _http_status_code(text)
        return "API_ERROR", status_code == 429 or (
            status_code is not None and status_code >= 500
        )

    if any(token in text for token in ("timeout", "timed out", "read timed out")):
        return "TIMEOUT", True
    if any(token in text for token in (
        "connection refused",
        "connection reset",
        "connection error",
        "host unreachable",
        "network",
        "dns",
        "name resolution",
        "service unavailable",
    )):
        return "NETWORK_ERROR", True
    if any(token in text for token in (
        "file not found",
        "csv file not found",
        "missing file",
        "failed to read csv",
        "mock file not found",
    )):
        return "FILE_ERROR", False
    if any(token in text for token in (
        "missing required columns",
        "required column",
        "required columns",
        "data quality",
        "schema validation",
        "duplicate rows",
        "duplicate record",
        "null values",
        "missing values",
        "invalid value",
        "invalid records",
        "validation failed",
        "invalid sale date",
    )):
        return "DATA_QUALITY_ERROR", False

    if any(token in text for token in ("database", "postgres", "psycopg", "sql error")):
        retryable = any(token in text for token in RETRYABLE_DATABASE_MESSAGES)
        return "DATABASE_ERROR", retryable

    if isinstance(original_error, APIError) or "http error" in text or "api error" in text:
        status_code = _http_status_code(text)
        return "API_ERROR", status_code == 429 or (status_code is not None and status_code >= 500)

    if text in FAILURE_CATEGORIES:
        category = text
        return category, category in {"NETWORK_ERROR", "TIMEOUT"}

    return "UNKNOWN", default_retryable


def _http_status_code(message: str) -> int | None:
    """Read a status code from the API client's normal HTTP error message."""
    match = re.search(r"\b([1-5]\d{2})\b", message)
    return int(match.group(1)) if match else None
