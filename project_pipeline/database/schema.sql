-- =====================================================================
-- Data Pipeline Monitoring and Recovery Framework
-- Database Schema - Version 1.0
--
-- Purpose: Store ETL pipeline execution data and business data from 
--          CSV/API sources. Designed for extensibility to support 
--          monitoring, recovery, and analytics modules.
--
-- Created: 2026-08-26
-- PostgreSQL Version: 12+
-- =====================================================================


-- =====================================================================
-- Table: sales_data
--
-- Purpose: Store business data loaded from ETL pipelines (CSV/API sources).
-- This is the target table for the Extract-Transform-Load process.
--
-- Why this table exists:
--   - Serves as the data warehouse for sales transactions
--   - Provides source data for monitoring data freshness metrics
--   - Validates successful pipeline execution (row counts, data quality)
--   - Enables business analytics and reporting
--
-- =====================================================================
CREATE TABLE sales_data (
    id SERIAL PRIMARY KEY,
    customer_name VARCHAR(255) NOT NULL,
    product_name VARCHAR(255) NOT NULL,
    quantity INTEGER NOT NULL,
    amount DECIMAL(10, 2) NOT NULL,
    sale_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints for data integrity
    CONSTRAINT chk_quantity_positive CHECK (quantity > 0),
    CONSTRAINT chk_amount_non_negative CHECK (amount >= 0)
);

-- Performance indexes for common query patterns
CREATE INDEX idx_sales_data_sale_date ON sales_data(sale_date);
CREATE INDEX idx_sales_data_created_at ON sales_data(created_at);


-- =====================================================================
-- Table: pipeline_execution
--
-- Purpose: Record metadata for every ETL pipeline execution.
-- Central table for the monitoring and recovery framework.
--
-- Why this table exists:
--   - Tracks execution history and pipeline health
--   - Records failures and error messages for troubleshooting
--   - Enables retry/recovery logic via retry_count and error_message
--   - Provides metrics: execution duration, rows processed, status
--   - Feeds data to Streamlit dashboard for visualization
--   - Baseline for future monitoring_logs, recovery_history, alerts, 
--     pipeline_metrics, and data_freshness tables
--
-- =====================================================================
CREATE TABLE pipeline_execution (
    execution_id SERIAL PRIMARY KEY,
    pipeline_name VARCHAR(100) NOT NULL,
    source_type VARCHAR(10) NOT NULL,
    start_time TIMESTAMP NOT NULL,
    end_time TIMESTAMP,
    execution_duration_seconds INTEGER,
    status VARCHAR(20) NOT NULL,
    rows_processed INTEGER DEFAULT 0,
    retry_count INTEGER DEFAULT 0,
    error_message TEXT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    
    -- Constraints for data integrity
    CONSTRAINT chk_source_type CHECK (source_type IN ('CSV', 'API')),
    CONSTRAINT chk_status CHECK (status IN ('SUCCESS', 'FAILED', 'RUNNING')),
    CONSTRAINT chk_rows_processed_non_negative CHECK (rows_processed >= 0),
    CONSTRAINT chk_retry_count_non_negative CHECK (retry_count >= 0)
);

-- Performance indexes for dashboard and monitoring queries
CREATE INDEX idx_pipeline_execution_pipeline_name ON pipeline_execution(pipeline_name);
CREATE INDEX idx_pipeline_execution_status ON pipeline_execution(status);
CREATE INDEX idx_pipeline_execution_start_time ON pipeline_execution(start_time);
CREATE INDEX idx_pipeline_execution_created_at ON pipeline_execution(created_at);


-- =====================================================================
-- Future Table Design Notes (Do not create yet)
--
-- monitoring_logs
--   - Link via execution_id to pipeline_execution
--   - Store DEBUG, INFO, WARNING, ERROR log levels
--   - Enable detailed troubleshooting and audit trails
--
-- recovery_history
--   - Link via execution_id to pipeline_execution
--   - Track retry attempts, recovery actions, outcomes
--   - Enable recovery engine effectiveness analysis
--
-- alerts
--   - Track SLA violations and threshold breaches
--   - Notify teams of pipeline anomalies
--
-- pipeline_metrics
--   - Aggregated KPIs: success rate, avg duration, retry patterns
--   - Computed from pipeline_execution data
--   - Support KPI dashboards and trend analysis
--
-- data_freshness
--   - Link via execution_id to pipeline_execution
--   - Measure lag between source data and loaded data
--   - Enable SLA compliance reporting
--
-- =====================================================================

