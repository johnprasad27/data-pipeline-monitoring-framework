# Data Pipeline Monitoring and Recovery Framework

Developed for academic demonstration (MCA Project, College of Engineering Trivandrum).

---

## 1. Overview & Purpose

The **Data Pipeline Monitoring and Recovery Framework** is a lightweight, configurable ETL orchestration, monitoring, and automated recovery system developed using Python, PostgreSQL, and Streamlit.

In modern data architectures, automated ETL workflows frequently encounter transient issues—such as intermittent network drops, temporary API rate limits, or momentary database deadlocks—as well as fatal anomalies like missing files or malformed schemas. When these failures go undetected or unhandled, downstream analytics and business reports become corrupted or stale.

Rather than introducing heavyweight distributed orchestration systems like Apache Airflow for small-to-medium pipelines, this project demonstrates how a robust monitoring layer can:
- Coordinate scheduled, multi-source ETL workflows (API and CSV).
- Intercept and classify failures into granular categories.
- Make intelligent, automated retry decisions to recover from transient faults without manual intervention.
- Track fine-grained data quality metrics and data freshness signals.
- Maintain persistent execution history and broadcast near-real-time state notifications in PostgreSQL.
- Present operational health, KPIs, and configuration controls via an intuitive Streamlit dashboard.

---

## 2. System Architecture

The framework is organized into decoupled layers: configuration and registry, execution scheduling, ETL data processing, failure and recovery management, database persistence and notification, and interactive presentation.

```text
┌────────────────────────────────────────────────────────────────────────┐
│                   Configuration & Pipeline Registry                    │
│    • pipelines.yaml (declarative YAML config)                          │
│    • PostgreSQL pipeline_registry table (UI dynamic registrations)     │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                          Continuous Scheduler                          │
│    • pipelines/scheduler.py (independent run intervals per pipeline)  │
│    • Dynamic registry polling (refreshes enabled state every 2s)       │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                      Recovery & Execution Wrapper                      │
│    • pipelines/recovery.py (run_with_recovery, retry loop orchestration│
│    • pipelines/failure_handling.py (classify_failure)                  │
│    • monitoring/execution.py (wraps unmanaged handlers, logs history)  │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                        ETL Pipeline Execution                          │
│    • API Pipeline (pipelines/api_pipeline.py -> DummyJSON API)         │
│    • CSV Pipeline (pipelines/csv_pipeline.py -> sales.csv / uploads)   │
│    • Validation & Quality Layer (pipelines/csv_quality.py, etl.py)     │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                      PostgreSQL Database Storage                       │
│    • sales_data: Clean, transformed business transactions              │
│    • pipeline_execution: Audit logs, metrics, errors, health scores   │
│    • pipeline_registry: Dynamic UI pipeline configurations             │
│    • pg_notify ('pipeline_status'): RUNNING / RETRYING / SUCCESS /     │
│      FAILED event broadcasts                                           │
└───────────────────────────────────┬────────────────────────────────────┘
                                    │
                                    ▼
┌────────────────────────────────────────────────────────────────────────┐
│                     Streamlit Monitoring Dashboard                     │
│    • Overview (pages/overview.py): Airflow-inspired system overview    │
│    • Pipeline Monitor (monitoring/dashboard.py): KPIs, charts, history │
│    • Add Pipeline (pages/add_pipeline.py): Dynamic UI registration     │
└────────────────────────────────────────────────────────────────────────┘
```

---

## 3. Core Components

### 3.1 Existing Pipelines

1. **API Pipeline (`pipelines/api_pipeline.py`)**:
   - **Source**: REST API endpoint (defaults to `https://dummyjson.com/carts` via `api.api_client.APIClient`).
   - **Process**: Extracts cart and product items, normalizes nested JSON into flat sales transaction records (`customer_name`, `product_name`, `quantity`, `amount`, `sale_date`), validates fields, transforms types, and loads records into the `sales_data` target table.
   - **Self-Logging**: Decorated with `@execution_history_managed` to start execution rows, record intermediate progress, evaluate quality scores, compute freshness/health, and log final `SUCCESS` or `FAILED` states.

2. **CSV Pipeline (`pipelines/csv_pipeline.py`)**:
   - **Source**: Structured local CSV files (`data/csv/sales.csv` or user-uploaded files stored under `data/storage/`).
   - **Process**: Reads source CSV, runs comprehensive schema and data quality validation via `pipelines/csv_quality.py`, transforms valid rows, and loads them into `sales_data` inside a database transaction.
   - **Reconciliation**: Reconciles input rows against loaded rows and rejected rows; updates detailed error counts (`null_count`, `duplicate_count`, `datatype_error_count`, `business_rule_error_count`).

### 3.2 Continuous Scheduler (`pipelines/scheduler.py`)

- **Loop Mechanism**: `run_forever()` continuously tracks the `next_run_at` timestamp for each active pipeline.
- **Dynamic Registry Refresh**: Every 2 seconds, the scheduler queries `get_enabled_pipeline_configs()`, refreshing definitions from `pipelines.yaml` merged with the database `pipeline_registry` table.
- **Per-Pipeline Interval & Retries**: Each pipeline runs on its own configured schedule (`interval_seconds`) and enforces its own retry ceiling (`max_retries`).
- **Failure Isolation**: An exception or failure in one pipeline does not block or terminate the scheduling of other pipelines.

### 3.3 Retry & Recovery System (`pipelines/recovery.py`, `pipelines/failure_handling.py`)

- When a pipeline encounters an error, `run_with_recovery()` passes the exception or error message to `classify_failure()`.
- If the failure is classified as **retryable** and the current attempt is below `max_retries`:
  1. The execution row in `pipeline_execution` is immediately updated to `status = 'RETRYING'`.
  2. A PostgreSQL notification is broadcast on the `pipeline_status` channel.
  3. The runner executes the next attempt.
- If the failure is **non-retryable** (e.g. malformed CSV, missing columns, HTTP 404), the retry loop aborts immediately without wasting system resources.
- If retries are exhausted without success, the status transitions to `FAILED` with failure classification and error details recorded.

### 3.4 PostgreSQL Execution History & Notification

- **Audit Logging**: Every execution attempt creates a `RUNNING` row in `pipeline_execution` upon start, capturing `start_time`, `pipeline_name`, and `source_type`. Upon completion, it commits `end_time`, `execution_duration_seconds`, `rows_processed`, `retry_count`, `failure_category`, `retryable`, `data_quality_score`, `freshness_status`, and `health_score`.
- **Event Broadcasting**: PostgreSQL `pg_notify` publishes JSON payloads on `pipeline_status` whenever a pipeline enters `RUNNING`, `RETRYING`, `SUCCESS`, or `FAILED` states.
- **Streamlit Consumption**: Streamlit views poll using `@st.fragment(run_every=3)` against the latest execution ID, keeping the UI near-real-time without requiring complex continuous socket listeners.

### 3.5 Streamlit Dashboard Interface

The Streamlit application (`app.py`) provides sidebar navigation across three purpose-built views:

1. **Overview (`pages/overview.py`)**:
   - Operational summary displaying high-level counts: Failed, Running, and Successful pipelines.
   - Filterable by history time window: *Last 1 Hour*, *Last 6 Hours*, *Last 24 Hours*, or *Last 7 Days*.
   - Pipeline Health status badges (`SUCCESS`, `FAILED`, `RUNNING`, `RETRYING`) with computed health scores and timestamps.

2. **Pipeline Monitor (`monitoring/dashboard.py`)**:
   - Single-page monitoring dashboard with aggregate KPI cards: *Total Pipelines*, *Successful Runs*, *Failed Runs*, *Rows Processed*, and *Avg Exec Duration*.
   - Global view or single-pipeline drill-down filter.
   - Pipeline status table featuring action buttons (`View`, `Logs`, `Retry`).
   - Detailed metric panels for selected pipelines: Health Score (/100), Freshness Status, Quality %, Failure Category, and Retryable flag.
   - Detailed data quality breakdown table for CSV pipelines (`Input rows`, `Valid rows`, `Rejected rows`, `Loaded rows`, `Missing columns`, `Null values`, `Duplicates`, `Datatype errors`, `Business rule errors`).
   - Plotly visualizations: Success vs. Failure donut chart and Execution Duration bar charts.
   - Tabular Recent Executions and Failed Executions audit log.

3. **Add Pipeline (`pages/add_pipeline.py`)**:
   - Form-based dynamic registration for new CSV and API pipelines.
   - Upload CSV files with automatic header inspection and configurable quality checks.
   - Configure custom API URLs, interval seconds, and retry limits.
   - Live registry management table to enable, disable, or delete pipelines. Deleting a disabled pipeline safely removes only its registry row while preserving all historical execution records in `pipeline_execution`.

---

## 4. Failure Classification & Retry Decision Rules

Failure classification is centralized in `pipelines/failure_handling.py`. It inspects exception types, HTTP response status codes, PostgreSQL diagnostic codes, and error string tokens:

| Failure Category | Description / Triggers | Retryable? | Retry Policy / Criteria |
| :--- | :--- | :---: | :--- |
| **`NETWORK_ERROR`** | Connectivity drops, connection refused/reset, host unreachable, DNS/name resolution errors | **Yes** | Retried up to configured `max_retries`. |
| **`TIMEOUT`** | HTTP request timeout, read timeout, socket timeout (`requests.Timeout`, `TimeoutError`) | **Yes** | Retried up to configured `max_retries`. |
| **`API_ERROR`** | HTTP response errors from external API endpoints | **Conditional** | **Retryable only for HTTP 429 (rate limited) and 5xx (server errors)**. Client errors (4xx like 400, 401, 404) are permanent and fail immediately. |
| **`DATABASE_ERROR`** | PostgreSQL query, connectivity, or engine failures | **Conditional** | **Retryable only for transient connection loss, deadlocks, or serialization failures** (`pgcode` in `40001`, `40P01` or connection dropped). Schema errors, syntax errors, or constraint violations fail immediately. |
| **`FILE_ERROR`** | Missing file (`FileNotFoundError`), unreadable file, corrupted CSV format | **No** | Permanent failure; aborts immediately without retry. |
| **`DATA_QUALITY_ERROR`** | Missing required columns, schema violations, null constraints, duplicate keys, business rule mismatches | **No** | Permanent data anomaly; aborts immediately without retry. |
| **`UNKNOWN`** | Unclassified exceptions or generic errors | **No** | Default to non-retryable unless a custom handler explicitly marks `retryable: true`. |

---

## 5. Data Quality, Freshness & Health Scoring

### 5.1 Data Quality Scoring

The framework provides two complementary validation layers:
1. **CSV Quality Layer (`pipelines/csv_quality.py`)**:
   - Inspects missing columns, nulls in required fields, unexpected data types, duplicate rows, and business-rule constraints (e.g. quantity must be positive, amount non-negative, amount reconciles with quantity × price).
   - Generates granular audit counts (`input_rows`, `valid_rows`, `rejected_rows`, `loaded_rows`, `missing_column_count`, `null_count`, `duplicate_count`, `datatype_error_count`, `business_rule_error_count`).
   - Formula:
     $$\text{Data Quality Score} = \begin{cases} 100.0 & \text{if } \text{input\_rows} = 0 \\ \operatorname{round}\left(\frac{\text{valid\_rows} \times 100}{\text{input\_rows}}, 2\right) & \text{if } \text{input\_rows} > 0 \end{cases}$$
2. **Generic Quality Helper (`pipelines/etl.py:evaluate_data_quality`)**:
   - Used by the API pipeline and generic datasets:
     $$\text{Score} = \max\left(0.0, \operatorname{round}\left(100 \times \left(1 - \frac{\text{missing\_values} + \text{duplicate\_rows} + \text{invalid\_rows}}{\text{total\_rows}}\right), 2\right)\right)$$

### 5.2 Data Freshness Monitoring (`monitoring/freshness.py`)

Freshness measures the time elapsed since the pipeline's **most recent successful execution**:
- **`FRESH`**: Age $< 120\text{ seconds}$ (`DATA_FRESHNESS_FRESH_SECONDS`).
- **`WARNING`**: $120\text{ seconds} \le \text{Age} \le 300\text{ seconds}$ (`DATA_FRESHNESS_WARNING_SECONDS`).
- **`STALE`**: $\text{Age} > 300\text{ seconds}$ or no recorded successful run.

Both threshold parameters can be customized via `.env` or `config.py`.

### 5.3 Pipeline Health Score (`monitoring/freshness.py:calculate_pipeline_health`)

The health score is a deterministic, weighted metric scaled between $0.0$ and $100.0$:

$$\text{Health Score} = \min\left(100.0, \max\left(0.0, W_{\text{status}} + W_{\text{freshness}} + W_{\text{quality}} - P_{\text{retry}}\right)\right)$$

Where:
- **Status Weight ($W_{\text{status}}$)**:
  - `SUCCESS`: $55\text{ pts}$
  - `RUNNING`: $35\text{ pts}$
  - `RETRYING`: $25\text{ pts}$
  - `FAILED`: $10\text{ pts}$
  - `UNKNOWN`: $0\text{ pts}$
- **Freshness Weight ($W_{\text{freshness}}$)**:
  - `FRESH`: $25\text{ pts}$
  - `WARNING`: $15\text{ pts}$
  - `STALE`: $5\text{ pts}$
- **Quality Weight ($W_{\text{quality}}$)**:
  - $\max\left(0.0, \text{data\_quality\_score} \times 0.2\right)$ (up to $20\text{ pts}$)
- **Retry Penalty ($P_{\text{retry}}$)**:
  - $\min\left(20, \max(0, \text{retry\_count}) \times 5\right)$ ($-5\text{ pts}$ per retry attempt, capped at $-20\text{ pts}$)

---

## 6. Configurable Pipeline Registration (`pipelines.yaml` & Dynamic UI)

The framework allows registering pipelines both statically via `pipelines.yaml` and dynamically via the Streamlit UI without altering scheduler or monitoring code.

### 6.1 Declarative YAML Registration (`pipelines.yaml`)

The file `pipelines.yaml` defines baseline pipelines loaded at startup:

```yaml
pipelines:
  - name: api_pipeline
    source: API
    handler: api_pipeline
    interval: 60
    max_retries: 3
    enabled: true

  - name: csv_pipeline
    source: CSV
    handler: csv_pipeline
    interval: 60
    max_retries: 3
    enabled: true
```

#### Supported YAML Schema Keys:
- `name` (required): Unique pipeline identifier (alphanumeric, underscores, hyphens).
- `source` (required): Must be `API` or `CSV` (aligned with the PostgreSQL check constraint).
- `handler` or `module` (required):
  - Conventional shorthand: `handler: my_pipeline` resolves to `pipelines.my_pipeline:run_my_pipeline`.
  - Explicit module and function: `module: pipelines.my_pipeline` with `handler: run_custom`.
  - Colon notation: `handler: my_pipeline:run_custom`.
- `interval` (optional, default `60`): Execution recurrence frequency in seconds.
- `max_retries` (optional, default `3`): Maximum recovery retry attempts.
- `enabled` (optional, default `true`): Boolean active flag.
- Source configuration keys (e.g. `url` for API sources; `file_path` for CSV sources).

### 6.2 Step-by-Step: Adding Another Pipeline via Code & YAML

1. **Create the handler module** under `pipelines/`, for example `pipelines/weather_pipeline.py`:
   ```python
   def run_weather_pipeline(*, pipeline_name: str, source_config: dict, retry_count: int = 0) -> dict:
       url = source_config.get("url")
       # Extraction, transformation, and database load logic here...
       return {
           "status": "SUCCESS",
           "rows_processed": 10,
           "error_message": None,
       }
   ```
2. **Register the entry in `pipelines.yaml`**:
   ```yaml
   - name: weather_pipeline
     source: API
     handler: weather_pipeline
     interval: 120
     max_retries: 2
     enabled: true
     url: https://api.weather.gov/gridpoints/TOP/31,80/forecast
   ```
3. **Execution**: The scheduler discovers the entry on its next 2-second refresh. Because the new handler is not marked with `@execution_history_managed`, the scheduler automatically wraps it with `monitoring.execution.monitor_pipeline_runner`, creating execution records, timing metrics, and failure classifications automatically.

### 6.3 Adding a Pipeline via the Streamlit UI

1. Open the **Add Pipeline** page in the Streamlit application.
2. Select pipeline type: **CSV** or **API**.
3. Fill in the fields:
   - For **CSV**: Enter a name, upload a `.csv` file (automatically placed in `data/storage/`), configure optional column mappings, set interval (seconds), and max retries.
   - For **API**: Enter a name, provide an HTTP(S) endpoint URL, set interval, and max retries.
4. Click **Register pipeline**.
5. The pipeline is written to the `pipeline_registry` table in PostgreSQL. The scheduler picks it up automatically on the next cycle without requiring an application restart.

---

## 7. Database Schema & Migration Guide

### 7.1 Database Tables

1. **`sales_data`**: Target data warehouse table for loaded transactions.
   - Columns: `id` (SERIAL PRIMARY KEY), `customer_name`, `product_name`, `quantity` ($> 0$), `amount` ($\ge 0$), `sale_date`, `created_at`.
2. **`pipeline_execution`**: Central execution tracking and audit table.
   - Columns: `execution_id` (SERIAL PRIMARY KEY), `pipeline_name`, `source_type` (`'CSV'`, `'API'`), `start_time`, `end_time`, `execution_duration_seconds`, `status` (`'SUCCESS'`, `'FAILED'`, `'RUNNING'`, `'RETRYING'`), `rows_processed`, `retry_count`, `error_message`, `execution_log`, `failure_type`, `failure_category`, `retryable`, `data_quality_score`, `freshness_status`, `health_score`, granular CSV quality metrics, `created_at`.
3. **`pipeline_registry`**: Dynamic database store for UI-registered pipelines.
   - Columns: `id`, `pipeline_name` (UNIQUE), `pipeline_type` (`'CSV'`, `'API'`), `source_config` (JSONB), `interval_seconds`, `max_retries`, `enabled`, `created_at`, `updated_at`.
4. **`pipeline_registry_seed_state`**: Tracks idempotency of configuration seeds (`seed_key = 'legacy_pipelines_v1'`).

### 7.2 Running Additive Migrations

All schema updates in `database/db.py:Database.ensure_monitoring_schema()` are non-destructive and idempotent. They use `ADD COLUMN IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS`, and safe constraint updates. Existing execution records are never deleted or modified destructively.

To manually trigger the database preparation and migration:

```powershell
.\venv\Scripts\python.exe -c "from pipelines.scheduler import prepare_database; prepare_database()"
```

> **Compatibility Note**: Running `prepare_database()` on a database with existing history maintains row counts exactly (verified with 1,209 historical execution records preserved).

---

## 8. Setup & Running the Project

### 8.1 Prerequisites
- Operating System: Windows 10/11 (or Linux/macOS with appropriate shell paths).
- Python 3.10+ (tested on Python 3.14).
- PostgreSQL 12+ running locally on port 5432.

### 8.2 Environment Variables (`.env`)

Configure the `.env` file in the project root:

```ini
# PostgreSQL Database Settings
DB_HOST=localhost
DB_PORT=5432
DB_NAME=pipeline_monitor
DB_USER=postgres
DB_PASSWORD=your_password_here

# API & Source Settings
API_BASE_URL=https://dummyjson.com/carts
CSV_PIPELINE_PATH=data/csv/sales.csv
PIPELINES_CONFIG_PATH=pipelines.yaml

# Scheduler & Monitoring Cadence
PIPELINE_INTERVAL_SECONDS=60
MAX_RETRIES=3
DASHBOARD_REFRESH_SECONDS=3
DATA_FRESHNESS_FRESH_SECONDS=120
DATA_FRESHNESS_WARNING_SECONDS=300
PIPELINE_STATUS_EVENT_CHANNEL=pipeline_status
LOG_FILE=logs/pipeline.log
```

### 8.3 Step-by-Step Launch Sequence

#### Step 1: Activate Virtual Environment
Open PowerShell:
```powershell
cd C:\Users\john27\MCA\mini_project\project_pipeline
.\venv\Scripts\Activate.ps1
```

#### Step 2: Verify Database Connectivity
```powershell
.\venv\Scripts\python.exe test_db.py
```
*Expected output: `Database Connected Successfully!`*

#### Step 3: Run Additive Migration & Seed Check
```powershell
.\venv\Scripts\python.exe -c "from pipelines.scheduler import prepare_database; prepare_database()"
```

#### Step 4: Start the Continuous Pipeline Scheduler (Terminal 1)
```powershell
.\venv\Scripts\python.exe main.py
```
*The scheduler runs continuously in the foreground. Press `Ctrl + C` to stop.*

#### Step 5: Start the Streamlit Dashboard (Terminal 2)
Open a second PowerShell terminal:
```powershell
cd C:\Users\john27\MCA\mini_project\project_pipeline
.\venv\Scripts\Activate.ps1
.\venv\Scripts\python.exe -m streamlit run app.py
```
*(Legacy direct entry: `.\venv\Scripts\python.exe -m streamlit run monitoring/dashboard.py`)*

The dashboard will open automatically in your browser at `http://localhost:8501`.

---

## 9. Verification & Testing

### 9.1 Running Regression Tests

The project includes unit and regression tests covering failure classification, retry policies, data quality metrics, freshness states, YAML parsing, dynamic registration, and event notification payloads:

```powershell
.\venv\Scripts\python.exe -m pytest
```

Output:
```text
============================= test session starts =============================
platform win32 -- Python 3.14.0, pytest-9.1.1, pluggy-1.6.0
rootdir: C:\Users\john27\MCA\mini_project\project_pipeline
collected 37 items

tests\test_csv_quality.py .....                                          [ 13%]
tests\test_monitoring_features.py .............................          [ 91%]
tests\test_overview_health.py ..                                         [ 97%]
tests\test_pipeline_events.py .                                          [100%]

============================= 37 passed in 0.97s ==============================
```

### 9.2 Python Syntax Compilation Check

To verify that all Python source files compile cleanly without syntax errors:

```powershell
.\venv\Scripts\python.exe -m py_compile (Get-ChildItem -Recurse -Filter *.py | Where-Object { $_.FullName -notmatch 'venv' } | ForEach-Object { $_.FullName })
```

### 9.3 Git Formatting Check

```powershell
git diff --check
```

### 9.4 Ad-Hoc Manual Pipeline Tests

To manually execute a single run of the built-in pipelines without waiting for the scheduler:

- **Manual API Pipeline Run**:
  ```powershell
  .\venv\Scripts\python.exe -c "from pipelines.api_pipeline import run_api_pipeline; print(run_api_pipeline())"
  ```
- **Manual CSV Pipeline Run**:
  ```powershell
  .\venv\Scripts\python.exe -c "from pipelines.csv_pipeline import run_csv_pipeline; print(run_csv_pipeline('data/csv/sales.csv'))"
  ```
- **Simulate Network Failure & Retry**:
  ```powershell
  $env:API_BASE_URL = "http://127.0.0.1:1"
  .\venv\Scripts\python.exe -c "from pipelines.api_pipeline import run_api_pipeline; print(run_api_pipeline())"
  $env:API_BASE_URL = "https://dummyjson.com/carts"
  ```

---

## 10. Verification Status & Known Limitations

### 10.1 Verified Repository State (Commit `0361db7` on `main`)
- **Git Commit**: Verified HEAD is commit `0361db7` on branch `main`.
- **Test Suite**: Verified all **37 tests pass** across the four test modules.
- **Compilation**: Verified Python syntax compilation passes cleanly for all workspace scripts.
- **Git Diff**: Verified `git diff --check` passes cleanly with no trailing whitespace or formatting conflicts.
- **Database History Preservation**: Verified applying `prepare_database()` preserves existing history intact without altering or truncating row counts (count verified at 1,209 records).
- **Execution Scope Note**: In accordance with regression testing best practices, the automated test suite uses test doubles, mocks, and isolated fixtures. The continuous production API and CSV ETL pipelines were **not** actively run against live endpoints during test verification.

### 10.2 Known Limitations
1. **Source Type Constraints**: The PostgreSQL schema enforces a check constraint (`CHECK (source_type IN ('CSV', 'API'))`). New custom pipelines must align with either `CSV` or `API` source classifications.
2. **Handler Package Boundary**: Custom handlers must be Python modules placed inside the `pipelines/` package.
3. **Near-Real-Time Streamlit Polling**: The Streamlit interface updates near-real-time by polling database changes via `@st.fragment(run_every=3)`. It does not maintain a continuous PostgreSQL `LISTEN` socket connection, which avoids persistent connection state issues with Streamlit session restarts.
4. **Target Schema Uniformity**: The built-in loader (`pipelines/etl.py:load_sales_data`) expects target records to conform to the `sales_data` schema (`customer_name`, `product_name`, `quantity`, `amount`, `sale_date`).

---

## 11. Demo Walkthrough for Academic Evaluation

Follow these steps to demonstrate the complete framework during project viva / evaluation:

1. **System Startup**:
   - Start PostgreSQL service.
   - Launch the scheduler in Terminal 1: `python main.py`.
   - Launch the Streamlit dashboard in Terminal 2: `streamlit run app.py`.
2. **Explore the Airflow-Inspired Overview**:
   - Navigate to `http://localhost:8501` and view **Overview**.
   - Show status cards for *Failed*, *Running*, and *Successful* pipelines.
   - Select different time windows (*Last 1 Hour*, *Last 24 Hours*) to showcase dynamic time-window filtering.
3. **Inspect the Pipeline Monitor**:
   - Open **Pipeline Monitor** from the sidebar.
   - Examine KPI cards: *Total Pipelines*, *Successful Runs*, *Failed Runs*, *Rows Processed*, and *Avg Exec Duration*.
   - Filter by `csv_pipeline` to show the granular CSV data quality breakdown table (`Missing columns`, `Null values`, `Duplicates`, `Datatype errors`, `Business rule errors`).
   - Highlight the *Health Score*, *Data Freshness*, and Plotly *Success vs Failure* charts.
4. **Demonstrate Dynamic Registration & Control**:
   - Navigate to **Add Pipeline**.
   - Register a new CSV pipeline (upload a valid sample CSV or point to `data/csv/sales.csv`) with interval `30` seconds and `2` retries.
   - Observe the confirmation and registry row. Switch to the Overview and Monitor pages to show the scheduler picking it up automatically.
   - Disable the pipeline from the UI to demonstrate pause functionality without deleting history.
5. **Demonstrate Failure Classification & Recovery**:
   - Run the network failure simulation script in a separate terminal:
     `$env:API_BASE_URL = "http://127.0.0.1:1"; python -c "from pipelines.api_pipeline import run_api_pipeline; print(run_api_pipeline())"`
   - Show that the dashboard immediately displays the `RETRYING` transition, classifies the error as `NETWORK_ERROR` (`retryable = YES`), attempts recovery up to max retries, and transitions to `FAILED` with failure category properly logged.
   - Restore `$env:API_BASE_URL = "https://dummyjson.com/carts"` and execute a clean run to demonstrate self-healing back to `SUCCESS`.

---

## 12. Author & Academic Information

- **Student Name**: John Prasad
- **Register Number**: TVE25MCA-2037
- **Course**: Master of Computer Applications (MCA)
- **Institution**: College of Engineering Trivandrum (CET)
- **Project**: Data Pipeline Monitoring and Recovery Framework (Mini Project)