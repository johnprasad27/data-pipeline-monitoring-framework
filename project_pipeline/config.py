"""
Configuration module for the Data Pipeline Monitoring and Recovery Framework.

Loads database credentials from environment variables (.env file).
Defaults provided for local development; override in .env for production.
"""

from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# Base directory for relative path resolution
BASE_DIR = Path(__file__).resolve().parent

# =====================================================================
# Pipeline registration and monitoring configuration
# =====================================================================
PIPELINES_CONFIG_PATH = BASE_DIR / os.getenv("PIPELINES_CONFIG_PATH", "pipelines.yaml")
DATA_FRESHNESS_FRESH_SECONDS = max(30, int(os.getenv("DATA_FRESHNESS_FRESH_SECONDS", "120")))
DATA_FRESHNESS_WARNING_SECONDS = max(
    DATA_FRESHNESS_FRESH_SECONDS,
    int(os.getenv("DATA_FRESHNESS_WARNING_SECONDS", "300")),
)

# =====================================================================
# PostgreSQL Database Configuration
# =====================================================================
DB_HOST = os.getenv("DB_HOST", "localhost")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "pipeline_monitor")
DB_USER = os.getenv("DB_USER", "postgres")
DB_PASSWORD = os.getenv("DB_PASSWORD", "CHANGE_ME")

# =====================================================================
# API Configuration
# =====================================================================
API_BASE_URL = os.getenv("API_BASE_URL", "https://dummyjson.com/carts")

# =====================================================================
# Continuous execution and monitoring configuration
# =====================================================================
PIPELINE_INTERVAL_SECONDS = max(
	1, int(os.getenv("PIPELINE_INTERVAL_SECONDS", "60"))
)
MAX_RETRIES = max(0, int(os.getenv("MAX_RETRIES", "3")))
DASHBOARD_REFRESH_SECONDS = max(
	2, int(os.getenv("DASHBOARD_REFRESH_SECONDS", "3"))
)
PIPELINE_STATUS_EVENT_CHANNEL = os.getenv("PIPELINE_STATUS_EVENT_CHANNEL", "pipeline_status")
CSV_PIPELINE_PATH = BASE_DIR / os.getenv("CSV_PIPELINE_PATH", "data/csv/sales.csv")
LOG_FILE = BASE_DIR / os.getenv("LOG_FILE", "logs/pipeline.log")