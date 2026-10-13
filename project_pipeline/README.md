# Data Pipeline Monitoring and Recovery Framework

## Overview

This project is a lightweight, configurable data pipeline monitoring and recovery framework developed for academic demonstration. It is not intended to replace Apache Airflow; instead, it shows how a small monitoring system can coordinate scheduled ETL activities, classify failures, track data quality, and surface freshness and health signals using PostgreSQL and a Streamlit dashboard.

The system includes:

- API and CSV ETL pipelines
- PostgreSQL execution logging
- retry and recovery orchestration
- PostgreSQL event notification support
- configurable pipeline registration
- simple data-quality checks and scoring
- freshness monitoring based on last successful run
- a Streamlit dashboard and an Airflow-inspired overview page

---

## Architecture

The architecture stays intentionally simple and incremental:

```text
Configuration / Pipeline Registry
        ↓
Scheduler
        ↓
Pipeline Execution
        ↓
Validation + Data Quality Checks
        ↓
PostgreSQL storage and event notifications
        ↓
Monitoring + Dashboard + Recovery
```

This keeps the existing implementation intact while making the framework extensible. The scheduler reads enabled pipelines from a pipeline registry instead of hardcoding execution calls.

---

## Existing Pipelines

### API pipeline

The API pipeline reads data from the configured API client, validates it, transforms it, and loads it into PostgreSQL. It follows the same monitoring pattern as the CSV pipeline and records execution metadata, status, retry count, and error details.

### CSV pipeline

The CSV pipeline reads a local sales CSV file, validates required business rules, transforms the data, and loads it into PostgreSQL. This pipeline is used as the structured file-based workload in the same framework.

---

## Configurable Pipeline Registration

The scheduler reloads `pipelines.yaml` during each registry refresh and also merges in pipelines registered through the Streamlit **Add Pipeline** page. The YAML configuration controls handler, interval, retry limit, and enabled state for entries it defines; a pipeline disabled through the UI remains disabled. DB-registered entries not present in YAML continue to use the built-in API or CSV handlers.

Configured handlers must be Python modules inside `pipelines/`. Use `module: pipelines.weather_pipeline` and `handler: run_weather_pipeline`, or use `handler: weather_pipeline` to infer the conventional function name `run_weather_pipeline`. Handlers receive `pipeline_name`, `source_config`, and `retry_count` keyword arguments and return a result dictionary with `status` and, on failure, `error_message`. New handlers are wrapped by the monitoring layer, which records each attempt and publishes its state; handler code should focus on extraction, transformation, and loading. The existing API/CSV handlers retain their current self-recording behavior. Invalid source settings, missing modules, and missing callables are reported as configuration errors and are not executed.

```yaml
pipelines:
  - name: api_pipeline
    source: API
    handler: api_pipeline
    interval: 60
    max_retries: 3
    enabled: true
    url: https://dummyjson.com/carts

  - name: csv_pipeline
    source: CSV
    handler: csv_pipeline
    interval: 60
    max_retries: 3
    enabled: true
    file_path: data/csv/sales.csv
```

To add a pipeline without editing scheduler or monitoring code, create `pipelines/<your_module>.py` with a `run_<your_module>(pipeline_name, source_config, retry_count)` function, then add a YAML entry naming that module and handler. Keep source as `API` or `CSV`: the current PostgreSQL schema intentionally constrains both registry and execution source types to those values. The handler remains in its own module and can use the source configuration fields it needs.

---

## Failure Classification

Failures are classified centrally and the category and retry decision are written to each execution row. Categories and retry policy are:

| Failure Type | Meaning | Retryable |
| --- | --- | --- |
| NETWORK_ERROR | connectivity or connection refusal problems | Yes |
| API_ERROR | API HTTP response failures (429 and 5xx are transient) | 429/5xx only |
| TIMEOUT | request timeout | Yes |
| DATABASE_ERROR | PostgreSQL failures | Transient connection/deadlock/serialization errors only |
| FILE_ERROR | missing or unreadable file | No |
| DATA_QUALITY_ERROR | missing values, duplicates, required columns, invalid data | No |
| UNKNOWN | unclassified issue | No, unless a custom handler explicitly marks it retryable |

The classification is added to execution metadata alongside `error_message` and `retryable` so the dashboard can present the reason clearly.

---

## Retry Decision Logic

The scheduler keeps the existing retry model but makes it smarter:

- retryable issues are retried up to each configured pipeline's `max_retries` value (`MAX_RETRIES` remains the default for direct handler use),
- permanent problems stop immediately,
- the framework preserves the existing attempt flow and status transitions,
- retry attempts are still visible in `pipeline_execution` as `RETRYING` rows.

Example rule set:

```text
NETWORK_ERROR / TIMEOUT -> retry
DATABASE_ERROR -> retry only when transient
API_ERROR -> retry only for HTTP 429 or 5xx
FILE_ERROR / DATA_QUALITY_ERROR / UNKNOWN -> no retry by default
```

This prevents useless retry loops on permanent problems while still recovering from temporary infrastructure issues.

---

## Data Quality Monitoring

A small reusable quality layer measures basic data quality for incoming records.

Supported checks:

- missing values
- duplicate records
- required columns
- invalid numeric/date values

The quality helper returns:

- `total_rows`
- `missing_values`
- `duplicate_rows`
- `invalid_rows`
- `missing_columns`
- `issues`
- `score`

The score is intentionally simple and transparent:

```text
score = 100 * (1 - (missing_values + duplicate_rows + invalid_rows) / total_rows)
```

This makes the metric understandable and easy to explain academically. If the total row count is zero, the framework records a neutral score of 100.0.

If a required column is missing, the pipeline fails cleanly with a `DATA_QUALITY_ERROR`.

---

## Data Freshness Monitoring

Freshness is measured as the age of the most recent successful execution for a pipeline.

The framework uses thresholds:

```text
Fresh:       < 2 minutes
Warning:     2-5 minutes
Stale:       > 5 minutes
```

These thresholds are configurable through `config.py` using:

```text
DATA_FRESHNESS_FRESH_SECONDS=120
DATA_FRESHNESS_WARNING_SECONDS=300
```

The freshness status is kept lightweight and is computed from the latest success timestamp instead of the most recent run.

---

## Pipeline Health Score

A transparent weighted health score is used for the pipeline signals in the dashboard.

The score combines:

- latest status outcome,
- freshness status,
- data quality score,
- retry count.

The formula is deliberately simple:

```text
status contribution + freshness contribution + quality contribution - retry penalty
```

The result is bounded between 0 and 100. This keeps the implementation practical and easy to understand without building a complex health model.

---

## PostgreSQL Monitoring

The project keeps its PostgreSQL-backed execution history and event notification flow.

The existing monitoring model still works:

- `RUNNING` notifications are emitted when a pipeline starts,
- `SUCCESS` and `FAILED` states are published on completion,
- `RETRYING` shows the retry attempt before the next execution starts,
- execution rows remain available for dashboard polling.

Schema updates are additive and do not delete existing data. The monitoring schema adds fields such as:

- `failure_type`
- `failure_category` (backfilled from existing `failure_type` values where available)
- `retryable`
- `data_quality_score`
- `freshness_status`
- `health_score`

This preserves historical execution rows while extending the current monitoring model.

---

## Dashboard

The main dashboard remains the approved single-page monitoring interface. The existing design and layout are preserved. A small addition is included only where necessary to show:

- current pipeline status,
- freshness,
- data quality,
- health score,
- failure type,
- retryable status.

The Airflow-inspired overview page remains a lightweight operational summary page without replacing the original dashboard.

---

## Near-Real-Time Monitoring

The database emits PostgreSQL `NOTIFY` messages for RUNNING, RETRYING, SUCCESS, and FAILED transitions. The Streamlit views refresh on their configured fragment interval and poll the latest execution ID; they do not open a PostgreSQL `LISTEN` connection. This preserves lightweight near-real-time updates without changing the current dashboard architecture.

This keeps the solution realistic and faithful to the project scope.

---

## Add a Pipeline from the UI

Open **Add Pipeline** in the Streamlit sidebar and choose **CSV** or **API**. Enter a unique pipeline name, source, interval, retry limit, and enabled state, then select **Register pipeline**. The page confirms the registration and lists all configured pipelines. Use its Enable/Disable action to pause or resume a pipeline without removing execution history.

UI registrations are stored in PostgreSQL's `pipeline_registry` table. The scheduler reloads YAML definitions and database registrations every two seconds, applying the configured interval and retry limit. Entries defined in YAML retain their handler settings; database-only UI registrations continue to use the existing CSV and API modules. Schema initialization is additive and does not alter or duplicate execution history.

Uploaded CSV files are stored under `data/storage/` with generated filenames. CSV input must use the sales columns expected by the existing target table: `customer_name`, `product_name`, `quantity`, `amount`, and `sale_date`. Missing columns, invalid values, and duplicates are recorded as data-quality failures. API URLs must be valid HTTP(S) URLs without embedded credentials, and API responses must be compatible with the current sales-data normalization.

### Demo steps

1. Start PostgreSQL, then start the scheduler and Streamlit using the commands below.
2. Open **Add Pipeline**, select **CSV**, enter `customer_pipeline`, upload a compatible sales CSV, and choose its interval and retries.
3. Register it and confirm the success message and registry row.
4. Open **Pipeline Monitor** to see its status and execution history. The scheduler discovers it automatically and writes the same PostgreSQL execution and `NOTIFY` events used by the built-in pipelines.
5. Open **Overview** to see the new pipeline in Pipeline Health. Use **Disable** on Add Pipeline to pause it; enabling it again makes it eligible on the next scheduler refresh.

The same page can register API pipelines by supplying an HTTP(S) URL. Network and timeout failures are retryable; API responses are retried only for HTTP 429/5xx; missing/unreadable CSV files, validation failures, and permanent database errors are not.

---

## How to Run the Project

### 1. Create or activate the environment

```powershell
cd project_pipeline
.\venv\Scripts\Activate.ps1
```

### 2. Start PostgreSQL

Start the local PostgreSQL instance used by the project.

### 3. Apply the additive monitoring migration

```powershell
.\venv\Scripts\python.exe -c "from pipelines.scheduler import prepare_database; prepare_database()"
```

This adds/backfills `failure_category` and adds `retryable` without deleting execution rows. The scheduler also runs this idempotent migration during startup.

### 4. Start the scheduler

```powershell
.\venv\Scripts\python.exe main.py
```

### 5. Start the dashboard

```powershell
.\venv\Scripts\python.exe -m streamlit run app.py
```

or, if needed for the legacy direct dashboard entry:

```powershell
.\venv\Scripts\python.exe -m streamlit run monitoring/dashboard.py
```

---

## Testing

The project includes regression tests for the monitoring features, including:

- network failure classification,
- API, timeout, database, file, data-quality, and unknown failure policies,
- configured retry limits and custom handler dispatch,
- data quality scoring,
- freshness state thresholds,
- API and CSV discovery from `pipelines.yaml`.

Run tests with:

```powershell
.\venv\Scripts\python.exe -m pytest -q
```

---

## Academic Positioning

This framework is presented as a lightweight, configurable data pipeline monitoring and recovery framework developed for academic demonstration. It is intentionally bounded in scope, practical to implement, and compatible with the existing project architecture.

It is designed to show core monitoring concepts clearly and reliably without introducing unnecessary distributed infrastructure or enterprise-scale complexity.

---

## Summary

The project now includes:

- dynamic pipeline registration,
- failure classification,
- retry decision logic,
- data quality checks and scoring,
- freshness checks,
- health scoring,
- additive database upgrades,
- monitoring integration without redesigning the existing system.

This keeps the current framework stable while making it more practical and more demonstrably intelligent for academic evaluation.

---

## Author

John Prasad
TVE25MCA-2037
College of Engineering Trivandrum