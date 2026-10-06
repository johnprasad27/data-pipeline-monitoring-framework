# Data Pipeline Monitoring and Recovery Framework

## Overview

This project is an MCA Mini Project that demonstrates how modern data engineering pipelines can be monitored and recovered automatically when failures occur.

The framework monitors ETL (Extract, Transform, Load) pipelines, records execution metadata, detects failures, retries recoverable errors, and visualizes pipeline health through a Streamlit dashboard.

---

## Objectives

- Build multiple ETL pipelines
- Monitor pipeline execution
- Detect pipeline failures
- Automatically recover from temporary failures
- Maintain execution logs
- Measure data freshness
- Display pipeline status in a dashboard

---

## Technology Stack

### Programming

- Python

### Database

- PostgreSQL

### Libraries

- Pandas
- psycopg2
- Requests
- Streamlit
- python-dotenv

---

## Project Structure

```

project_pipeline/
│
├── data/
├── database/
├── pipelines/
├── monitoring/
├── dashboard/
├── logs/
├── config.py
├── main.py

```

---

## Architecture

```

CSV / API
↓

Extract
↓

Validate
↓

Transform
↓

Load
↓

PostgreSQL
↓

Monitoring Framework
↓

Recovery Engine
↓

Streamlit Dashboard

```

---

## Features

- CSV Pipeline
- API Pipeline
- Pipeline Monitoring
- Retry Mechanism
- Error Logging
- Data Freshness
- Dashboard Visualization

## Continuous Execution

The continuous runner executes both existing pipelines immediately, then
starts the next cycle after `PIPELINE_INTERVAL_SECONDS`. It is isolated from
the Streamlit process, so the dashboard can be started separately and poll
PostgreSQL for the latest execution rows.

Configuration is loaded from `.env` by `config.py`:

```text
PIPELINE_INTERVAL_SECONDS=60
MAX_RETRIES=3
DASHBOARD_REFRESH_SECONDS=10
```

Start the runner from the project directory:

```powershell
.\venv\Scripts\Activate.ps1
python main.py
```

Start the unchanged single-page dashboard in another terminal:

```powershell
.\venv\Scripts\Activate.ps1
streamlit run monitoring/dashboard.py
```

Stop either foreground process with `Ctrl+C`. The scheduler catches pipeline
failures, records them, marks the attempt `RETRYING`, and starts up to
`MAX_RETRIES` additional attempts. Each attempt remains a separate row in
`pipeline_execution`; the final attempt is `SUCCESS` or `FAILED`.

To test an API failure, temporarily set `API_BASE_URL` in `.env` to an
unreachable URL and restart `main.py`. The database should show failed
attempts with retry counts and error messages. Restore the URL afterward.
For a permanent failure, leave the unreachable URL in place until all retry
attempts are recorded, then restore it and restart the runner.

The scheduler applies only additive database changes at startup. Existing
execution history is preserved. The schema adds `execution_log`, allows the
`RETRYING` status, and stores execution durations with millisecond precision.

---

## Author

John Prasad

TVE25MCA-2037

College of Engineering Trivandrum