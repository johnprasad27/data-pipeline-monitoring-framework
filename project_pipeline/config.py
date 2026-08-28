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