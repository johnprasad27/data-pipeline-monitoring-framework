"""
database/db.py

Modular PostgreSQL database access layer for the
Data Pipeline Monitoring and Recovery Framework.

Responsibilities:
    - Manage a single PostgreSQL connection (pooling intentionally excluded).
    - Provide safe, reusable methods for executing queries.
    - Centralize exception handling and logging for all DB operations.

This module is designed to be imported by other packages
(pipelines/, monitoring/, dashboard/) without exposing raw
psycopg2 connection/cursor objects to the rest of the codebase.
"""

import json
import logging
from datetime import datetime

import psycopg2
import psycopg2.extras

import config  # Expects DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD

# Module-level logger. Configuration (handlers/formatters) is expected
# to be set up centrally (e.g. in main.py or a logging config module).
logger = logging.getLogger(__name__)


class DatabaseError(Exception):
    """Custom exception wrapper for all database-related failures."""
    pass


class Database:
    """
    Thin wrapper around a single psycopg2 connection.

    Usage:
        db = Database()
        db.connect()
        rows = db.fetch_all("SELECT * FROM pipeline_runs;")
        db.close()

    Or as a context manager:
        with Database() as db:
            db.execute("INSERT INTO ...", (values,))
    """

    def __init__(self):
        self.connection = None

    # ------------------------------------------------------------------
    # Connection lifecycle
    # ------------------------------------------------------------------
    def connect(self):
        """
        Establish a connection to the PostgreSQL database using
        credentials defined in config.py.

        Raises:
            DatabaseError: if the connection cannot be established.
        """
        try:
            self.connection = psycopg2.connect(
                host=config.DB_HOST,
                port=config.DB_PORT,
                dbname=config.DB_NAME,
                user=config.DB_USER,
                password=config.DB_PASSWORD,
            )
            # Autocommit is left False deliberately so callers control
            # transaction boundaries explicitly via commit()/rollback().
            self.connection.autocommit = False
            logger.info("Database connection established successfully.")

        except psycopg2.OperationalError as e:
            logger.error("Failed to connect to the database: %s", e)
            raise DatabaseError(f"Could not connect to database: {e}") from e

        except Exception as e:
            logger.exception("Unexpected error while connecting to database.")
            raise DatabaseError(f"Unexpected connection error: {e}") from e

    def ensure_monitoring_schema(self):
        """Apply additive monitoring changes without deleting execution history."""
        self._ensure_connection()
        try:
            self.execute(
                "ALTER TABLE pipeline_execution "
                "ADD COLUMN IF NOT EXISTS execution_log TEXT",
                commit=False,
            )
            self.execute(
                "ALTER TABLE pipeline_execution ADD COLUMN IF NOT EXISTS failure_type VARCHAR(64)",
                commit=False,
            )
            self.execute(
                "ALTER TABLE pipeline_execution ADD COLUMN IF NOT EXISTS failure_category VARCHAR(64)",
                commit=False,
            )
            self.execute(
                "ALTER TABLE pipeline_execution ADD COLUMN IF NOT EXISTS retryable BOOLEAN",
                commit=False,
            )
            self.execute(
                """
                UPDATE pipeline_execution
                SET failure_category = CASE
                    WHEN UPPER(failure_type) IN (
                        'NETWORK_ERROR', 'API_ERROR', 'DATABASE_ERROR',
                        'FILE_ERROR', 'DATA_QUALITY_ERROR', 'TIMEOUT'
                    ) THEN UPPER(failure_type)
                    ELSE 'UNKNOWN'
                END
                WHERE failure_category IS NULL AND failure_type IS NOT NULL
                """,
                commit=False,
            )
            self.execute(
                "ALTER TABLE pipeline_execution ADD COLUMN IF NOT EXISTS data_quality_score NUMERIC(5, 2)",
                commit=False,
            )
            self.execute(
                "ALTER TABLE pipeline_execution ADD COLUMN IF NOT EXISTS freshness_status VARCHAR(32)",
                commit=False,
            )
            self.execute(
                "ALTER TABLE pipeline_execution ADD COLUMN IF NOT EXISTS health_score NUMERIC(5, 2)",
                commit=False,
            )
            for column in (
                "input_rows INTEGER",
                "valid_rows INTEGER",
                "rejected_rows INTEGER",
                "loaded_rows INTEGER",
                "missing_column_count INTEGER",
                "null_count INTEGER",
                "duplicate_count INTEGER",
                "datatype_error_count INTEGER",
                "business_rule_error_count INTEGER",
                "data_quality_status VARCHAR(16)",
            ):
                self.execute(
                    f"ALTER TABLE pipeline_execution ADD COLUMN IF NOT EXISTS {column}",
                    commit=False,
                )
            self.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_registry (
                    id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    pipeline_name VARCHAR(100) NOT NULL UNIQUE,
                    pipeline_type VARCHAR(10) NOT NULL CHECK (pipeline_type IN ('CSV', 'API')),
                    source_config JSONB NOT NULL DEFAULT '{}'::jsonb,
                    interval_seconds INTEGER NOT NULL CHECK (interval_seconds > 0),
                    max_retries INTEGER NOT NULL CHECK (max_retries >= 0),
                    enabled BOOLEAN NOT NULL DEFAULT TRUE,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """,
                commit=False,
            )
            self.execute(
                "CREATE UNIQUE INDEX IF NOT EXISTS idx_pipeline_registry_name_ci "
                "ON pipeline_registry (LOWER(pipeline_name))",
                commit=False,
            )
            self.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_registry_seed_state (
                    seed_key VARCHAR(64) PRIMARY KEY,
                    seeded_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """,
                commit=False,
            )
            self.execute(
                "ALTER TABLE pipeline_execution DROP CONSTRAINT IF EXISTS chk_status",
                commit=False,
            )
            self.execute(
                "ALTER TABLE pipeline_execution ADD CONSTRAINT chk_status "
                "CHECK (status IN ('SUCCESS', 'FAILED', 'RUNNING', 'RETRYING', 'QUEUED'))",
                commit=False,
            )
            self.execute(
                "ALTER TABLE pipeline_execution ALTER COLUMN execution_duration_seconds "
                "TYPE NUMERIC(12, 3) USING execution_duration_seconds::NUMERIC",
                commit=False,
            )
            # --- Task-level execution tracking (additive) ---
            self.execute(
                """
                CREATE TABLE IF NOT EXISTS pipeline_task_execution (
                    task_id BIGINT GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
                    execution_id INTEGER NOT NULL
                        REFERENCES pipeline_execution(execution_id) ON DELETE CASCADE,
                    pipeline_name VARCHAR(100) NOT NULL,
                    task_name VARCHAR(64) NOT NULL,
                    status VARCHAR(20) NOT NULL
                        CHECK (status IN ('SUCCESS', 'FAILED', 'SKIPPED')),
                    started_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
                    ended_at TIMESTAMP,
                    error_message TEXT,
                    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
                )
                """,
                commit=False,
            )
            self.execute(
                "CREATE INDEX IF NOT EXISTS idx_task_exec_execution_id "
                "ON pipeline_task_execution (execution_id)",
                commit=False,
            )
            self.execute(
                "CREATE INDEX IF NOT EXISTS idx_task_exec_pipeline_name "
                "ON pipeline_task_execution (pipeline_name)",
                commit=False,
            )
            self.connection.commit()
            logger.info("Monitoring schema is ready.")
        except DatabaseError:
            self.connection.rollback()
            logger.exception("Database error while applying monitoring schema changes.")
            raise

    def log_task_execution(
        self,
        execution_id: int,
        pipeline_name: str,
        task_name: str,
        status: str,
        started_at: datetime | None = None,
        ended_at: datetime | None = None,
        error_message: str | None = None,
    ) -> None:
        """Insert a single task-stage row for a pipeline execution."""
        self.execute(
            """
            INSERT INTO pipeline_task_execution
                (execution_id, pipeline_name, task_name, status,
                 started_at, ended_at, error_message)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (
                execution_id,
                pipeline_name,
                task_name,
                status,
                started_at or datetime.now(),
                ended_at or datetime.now(),
                error_message,
            ),
            commit=True,
        )


    @staticmethod
    def build_status_notification(
        pipeline_name: str,
        status: str,
        execution_id: int | None = None,
        retry_count: int = 0,
    ) -> str:
        """Build a compact PostgreSQL notification payload for pipeline state changes."""
        payload = {
            "pipeline_name": pipeline_name,
            "status": status,
            "execution_id": execution_id,
            "retry_count": retry_count,
        }
        return json.dumps(payload, separators=(",", ":"))

    def notify_pipeline_status(
        self,
        pipeline_name: str,
        status: str,
        execution_id: int | None = None,
        retry_count: int = 0,
    ) -> None:
        """Send a lightweight notification whenever a pipeline execution changes state."""
        if self.connection is None or self.connection.closed:
            return

        payload = self.build_status_notification(
            pipeline_name=pipeline_name,
            status=status,
            execution_id=execution_id,
            retry_count=retry_count or 0,
        )

        try:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT pg_notify(%s, %s)",
                (config.PIPELINE_STATUS_EVENT_CHANNEL, payload),
            )
            self.connection.commit()
        except Exception as exc:
            logger.warning("Failed to publish pipeline status notification: %s", exc)
            try:
                self.connection.rollback()
            except Exception:
                pass

    def close(self):
        """Close the active database connection, if one exists."""
        if self.connection is not None:
            try:
                self.connection.close()
                logger.info("Database connection closed.")
            except Exception as e:
                logger.warning("Error while closing database connection: %s", e)
            finally:
                self.connection = None

    def _ensure_connection(self):
        """
        Internal helper to guarantee an active connection exists
        before any query is executed.
        """
        if self.connection is None or self.connection.closed:
            raise DatabaseError(
                "No active database connection. Call connect() first."
            )

    # ------------------------------------------------------------------
    # Core query execution methods
    # ------------------------------------------------------------------
    def execute(self, query, params=None, commit=True):
        """
        Execute an INSERT/UPDATE/DELETE/DDL statement.

        Args:
            query (str): SQL statement with %s placeholders.
            params (tuple | list | dict): Parameters to bind safely.
            commit (bool): Whether to commit immediately after execution.

        Returns:
            int: Number of rows affected.

        Raises:
            DatabaseError: on any failure, with the transaction rolled back.
        """
        self._ensure_connection()
        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.execute(query, params)
            if commit:
                self.connection.commit()
            return cursor.rowcount

        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error("Query execution failed: %s | Query: %s", e, query)
            raise DatabaseError(f"Execute failed: {e}") from e

        except Exception as e:
            self.connection.rollback()
            logger.exception("Unexpected error during query execution.")
            raise DatabaseError(f"Unexpected execute error: {e}") from e

        finally:
            if cursor is not None:
                cursor.close()

    def start_pipeline_execution(
        self,
        pipeline_name,
        source_type,
        start_time=None,
        retry_count=0,
    ):
        """Create a visible RUNNING execution and return its execution ID."""
        self._ensure_connection()
        cursor = None
        start_time = start_time or datetime.now()
        try:
            cursor = self.connection.cursor()
            cursor.execute(
                """
                INSERT INTO pipeline_execution
                    (pipeline_name, source_type, status, rows_processed,
                     start_time, execution_duration_seconds, retry_count,
                     execution_log)
                VALUES (%s, %s, 'RUNNING', 0, %s, 0, %s, %s)
                RETURNING execution_id
                """,
                (
                    pipeline_name,
                    source_type,
                    start_time,
                    retry_count,
                    "pipeline started",
                ),
            )
            execution_id = cursor.fetchone()[0]
            self.connection.commit()
            self.notify_pipeline_status(
                pipeline_name=pipeline_name,
                status="RUNNING",
                execution_id=execution_id,
                retry_count=retry_count,
            )
            return execution_id
        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error("Failed to start pipeline execution: %s", e)
            raise DatabaseError(f"Failed to start pipeline execution: {e}") from e
        finally:
            if cursor is not None:
                cursor.close()

    def update_pipeline_execution(
        self,
        execution_id,
        status,
        rows_processed,
        start_time,
        end_time=None,
        error_message=None,
        retry_count=None,
        execution_log=None,
        pipeline_name=None,
        failure_type=None,
        failure_category=None,
        retryable=None,
        data_quality_score=None,
        freshness_status=None,
        health_score=None,
        input_rows=None,
        valid_rows=None,
        rejected_rows=None,
        loaded_rows=None,
        missing_column_count=None,
        null_count=None,
        duplicate_count=None,
        datatype_error_count=None,
        business_rule_error_count=None,
        data_quality_status=None,
    ):
        """Update the existing execution row and commit it for dashboard polling."""
        duration_seconds = round(
            ((end_time or datetime.now()) - start_time).total_seconds(), 3
        )
        failure_type_clause = "%s" if status == "SUCCESS" else "COALESCE(%s, failure_type)"
        failure_category_clause = "%s" if status == "SUCCESS" else "COALESCE(%s, failure_category)"
        retryable_clause = "%s" if status == "SUCCESS" else "COALESCE(%s, retryable)"

        query = f"""
            UPDATE pipeline_execution
            SET status = %s,
                rows_processed = %s,
                execution_duration_seconds = %s,
                end_time = %s,
                error_message = %s,
                retry_count = COALESCE(%s, retry_count),
                execution_log = COALESCE(%s, execution_log),
                failure_type = {failure_type_clause},
                failure_category = {failure_category_clause},
                retryable = {retryable_clause},
                data_quality_score = COALESCE(%s, data_quality_score),
                freshness_status = COALESCE(%s, freshness_status),
                health_score = COALESCE(%s, health_score),
                input_rows = COALESCE(%s, input_rows),
                valid_rows = COALESCE(%s, valid_rows),
                rejected_rows = COALESCE(%s, rejected_rows),
                loaded_rows = COALESCE(%s, loaded_rows),
                missing_column_count = COALESCE(%s, missing_column_count),
                null_count = COALESCE(%s, null_count),
                duplicate_count = COALESCE(%s, duplicate_count),
                datatype_error_count = COALESCE(%s, datatype_error_count),
                business_rule_error_count = COALESCE(%s, business_rule_error_count),
                data_quality_status = COALESCE(%s, data_quality_status)
            WHERE execution_id = %s
        """
        params = (
            status,
            rows_processed,
            duration_seconds,
            end_time,
            error_message,
            retry_count,
            execution_log,
            failure_type,
            failure_category,
            retryable,
            data_quality_score,
            freshness_status,
            health_score,
            input_rows,
            valid_rows,
            rejected_rows,
            loaded_rows,
            missing_column_count,
            null_count,
            duplicate_count,
            datatype_error_count,
            business_rule_error_count,
            data_quality_status,
            execution_id,
        )
        self.execute(query, params, commit=True)

        if pipeline_name is not None:
            self.notify_pipeline_status(
                pipeline_name=pipeline_name,
                status=status,
                execution_id=execution_id,
                retry_count=retry_count or 0,
            )

    def append_execution_log(self, execution_id, message):
        """Append a non-sensitive lifecycle message to an execution row."""
        self.execute(
            """
            UPDATE pipeline_execution
            SET execution_log = CONCAT_WS(E'\\n', execution_log, %s)
            WHERE execution_id = %s
            """,
            (message, execution_id),
            commit=True,
        )

    def fetch_one(self, query, params=None):
        """
        Execute a SELECT query and return a single row as a dict.

        Returns:
            dict | None: Row data, or None if no result found.
        """
        self._ensure_connection()
        cursor = None
        try:
            cursor = self.connection.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            cursor.execute(query, params)
            result = cursor.fetchone()
            return dict(result) if result else None

        except psycopg2.Error as e:
            logger.error("fetch_one failed: %s | Query: %s", e, query)
            raise DatabaseError(f"fetch_one failed: {e}") from e

        except Exception as e:
            logger.exception("Unexpected error during fetch_one.")
            raise DatabaseError(f"Unexpected fetch_one error: {e}") from e

        finally:
            if cursor is not None:
                cursor.close()

    def fetch_all(self, query, params=None):
        """
        Execute a SELECT query and return all rows as a list of dicts.

        Returns:
            list[dict]: List of rows (empty list if none found).
        """
        self._ensure_connection()
        cursor = None
        try:
            cursor = self.connection.cursor(
                cursor_factory=psycopg2.extras.RealDictCursor
            )
            cursor.execute(query, params)
            results = cursor.fetchall()
            return [dict(row) for row in results]

        except psycopg2.Error as e:
            logger.error("fetch_all failed: %s | Query: %s", e, query)
            raise DatabaseError(f"fetch_all failed: {e}") from e

        except Exception as e:
            logger.exception("Unexpected error during fetch_all.")
            raise DatabaseError(f"Unexpected fetch_all error: {e}") from e

        finally:
            if cursor is not None:
                cursor.close()

    def execute_many(self, query, param_list, commit=True):
        """
        Execute the same statement against a batch of parameter sets.
        Useful for bulk inserts (e.g. metrics, log batches).

        Args:
            query (str): SQL statement with %s placeholders.
            param_list (list[tuple]): List of parameter tuples.
            commit (bool): Whether to commit immediately after execution.

        Returns:
            int: Number of rows affected.
        """
        self._ensure_connection()
        cursor = None
        try:
            cursor = self.connection.cursor()
            cursor.executemany(query, param_list)
            if commit:
                self.connection.commit()
            return cursor.rowcount

        except psycopg2.Error as e:
            self.connection.rollback()
            logger.error("execute_many failed: %s | Query: %s", e, query)
            raise DatabaseError(f"execute_many failed: {e}") from e

        except Exception as e:
            self.connection.rollback()
            logger.exception("Unexpected error during execute_many.")
            raise DatabaseError(f"Unexpected execute_many error: {e}") from e

        finally:
            if cursor is not None:
                cursor.close()

    # ------------------------------------------------------------------
    # Context manager support
    # ------------------------------------------------------------------
    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        # Roll back any uncommitted work if an exception occurred
        # inside the `with` block, then always close the connection.
        if exc_type is not None and self.connection is not None:
            try:
                self.connection.rollback()
            except Exception:
                logger.warning("Rollback on exit failed (connection may already be closed).")
        self.close()
        # Returning False re-raises any exception that occurred in the block.
        return False