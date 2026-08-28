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

import logging
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