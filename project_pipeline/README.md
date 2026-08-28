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

---

## Author

John Prasad

TVE25MCA-2037

College of Engineering Trivandrum