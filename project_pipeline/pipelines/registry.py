"""Pipeline registry that loads enabled pipeline metadata from configuration."""

from __future__ import annotations

import importlib
import json
import logging
import re
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import config
from database.db import Database, DatabaseError

logger = logging.getLogger(__name__)


PIPELINE_NAME_PATTERN = re.compile(r"^[A-Za-z][A-Za-z0-9_-]{0,99}$")


def validate_pipeline_registration(
    name: str,
    pipeline_type: str,
    source_config: dict[str, Any],
    interval_seconds: int,
    max_retries: int,
) -> dict[str, Any]:
    """Validate and normalize configuration accepted by the predefined handlers."""
    normalized_name = name.strip() if isinstance(name, str) else ""
    if not PIPELINE_NAME_PATTERN.fullmatch(normalized_name):
        raise ValueError("Pipeline name must start with a letter and contain only letters, numbers, '_' or '-'.")

    normalized_type = pipeline_type.strip().upper()
    if normalized_type not in {"CSV", "API"}:
        raise ValueError("Pipeline type must be CSV or API.")
    if not isinstance(source_config, dict):
        raise ValueError("Pipeline source configuration is invalid.")

    if isinstance(interval_seconds, bool) or int(interval_seconds) < 1:
        raise ValueError("Execution interval must be a positive integer.")
    if isinstance(max_retries, bool) or int(max_retries) < 0:
        raise ValueError("Maximum retries must be zero or greater.")

    if normalized_type == "CSV":
        file_path = source_config.get("file_path")
        if not file_path:
            raise ValueError("CSV file is required.")
        resolved_path = (config.BASE_DIR / Path(file_path)).resolve()
        data_dir = (config.BASE_DIR / "data").resolve()
        if resolved_path.suffix.lower() != ".csv" or not resolved_path.is_relative_to(data_dir):
            raise ValueError("CSV source must be a .csv file inside the project's data directory.")
        if not resolved_path.is_file():
            raise ValueError("CSV file is required and must exist.")
        normalized_source = dict(source_config)
        normalized_source["file_path"] = str(resolved_path.relative_to(config.BASE_DIR))
        quality_config = source_config.get("quality_config")
        if quality_config is not None:
            if not isinstance(quality_config, dict):
                raise ValueError("CSV quality configuration must be an object.")
            normalized_source["quality_config"] = quality_config
        schema_config = source_config.get("schema_config")
        if schema_config is not None:
            if not isinstance(schema_config, dict):
                raise ValueError("CSV schema configuration must be an object.")
            normalized_source["schema_config"] = schema_config
        for key in ("content_hash", "original_filename", "file_size_bytes"):
            if key in source_config:
                normalized_source[key] = source_config[key]
    else:
        url = str(source_config.get("url", "")).strip()
        parsed_url = urlparse(url) if url else None
        if url and (
            parsed_url.scheme not in {"http", "https"}
            or not parsed_url.hostname
            or parsed_url.username is not None
            or parsed_url.password is not None
        ):
            raise ValueError("API URL is invalid. Enter an HTTP or HTTPS URL without embedded credentials.")
        normalized_source = dict(source_config)
        normalized_source["url"] = url

    if "expected_rows" in source_config:
        val = source_config.get("expected_rows")
        if val is not None and val != "":
            try:
                int_val = int(val)
                if int_val < 0:
                    raise ValueError("Expected row count must be zero or greater.")
                normalized_source["expected_rows"] = int_val
            except (ValueError, TypeError):
                raise ValueError("Expected row count must be a valid integer.")

    return {
        "name": normalized_name,
        "pipeline_type": normalized_type,
        "source_config": normalized_source,
        "interval_seconds": int(interval_seconds),
        "max_retries": int(max_retries),
    }


def _parse_simple_yaml_config(path: Path) -> dict[str, Any]:
    """Parse the flat pipeline mapping syntax used in pipelines.yaml."""
    if not path.is_file():
        raise FileNotFoundError(f"Pipeline configuration file does not exist: {path}")
    content = path.read_text(encoding="utf-8")
    if not content.strip():
        raise ValueError(f"Pipeline configuration file is empty: {path}")

    pipelines: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    active_section: str | None = None
    found_pipeline_list = False

    for line_number, raw_line in enumerate(content.splitlines(), start=1):
        line = raw_line.split(" #", 1)[0].rstrip()
        if not line.strip() or line.lstrip().startswith("#"):
            continue
        stripped = line.strip()
        if stripped == "pipelines:":
            if found_pipeline_list:
                raise ValueError(f"Duplicate 'pipelines' key on line {line_number}")
            found_pipeline_list = True
            continue
        if not found_pipeline_list:
            raise ValueError(f"Expected 'pipelines:' before line {line_number}")
        if line.lstrip().startswith("- "):
            if current is not None:
                pipelines.append(current)
            current = {}
            active_section = None
            remainder = stripped[2:].strip()
            if remainder:
                key, value = _split_yaml_pair(remainder, line_number)
                current[key] = _parse_scalar(value)
            continue
        if current is None or ":" not in stripped:
            raise ValueError(f"Invalid pipeline configuration on line {line_number}: {raw_line.strip()}")
        key, value = _split_yaml_pair(stripped, line_number)
        indent = len(raw_line) - len(raw_line.lstrip())
        if indent >= 6:
            if active_section is None or not isinstance(current.get(active_section), dict):
                raise ValueError(f"Unexpected nested value on line {line_number}")
            section = current[active_section]
            if key in section:
                raise ValueError(f"Duplicate key '{key}' on line {line_number}")
            section[key] = _parse_scalar(value)
        else:
            if key in current:
                raise ValueError(f"Duplicate key '{key}' on line {line_number}")
            if value:
                current[key] = _parse_scalar(value)
                active_section = None
            else:
                current[key] = {}
                active_section = key

    if not found_pipeline_list:
        raise ValueError(f"Missing 'pipelines:' list in {path}")
    if current is not None:
        pipelines.append(current)
    return {"pipelines": pipelines}


def _parse_scalar(value: str) -> Any:
    value = value.strip()
    if len(value) >= 2 and value[0] == value[-1] and value[0] in {"'", '"'}:
        return value[1:-1]
    if value.startswith("[") and value.endswith("]"):
        contents = value[1:-1].strip()
        return [_parse_scalar(item.strip()) for item in contents.split(",") if item.strip()]
    if value.lower() in {"true", "false"}:
        return value.lower() == "true"
    if value.isdigit() or (value.startswith("-") and value[1:].isdigit()):
        return int(value)
    try:
        return float(value)
    except ValueError:
        return value


def _split_yaml_pair(line: str, line_number: int) -> tuple[str, str]:
    if ":" not in line:
        raise ValueError(f"Expected a key/value pair on line {line_number}")
    key, value = line.split(":", 1)
    key = key.strip()
    if not key:
        raise ValueError(f"Empty key on line {line_number}")
    return key, value.strip()


def _resolve_handler_spec(pipeline: dict[str, Any]) -> tuple[str, str]:
    module_spec = pipeline.get("module")
    handler_spec = pipeline.get("handler")
    if module_spec:
        module_path = str(module_spec).strip()
        function_name = str(handler_spec).strip() if handler_spec else ""
    else:
        if not handler_spec:
            raise ValueError("Missing required 'handler' or 'module' setting.")
        handler_spec = str(handler_spec).strip()
        if ":" in handler_spec:
            module_path, function_name = handler_spec.split(":", 1)
        else:
            module_path, function_name = handler_spec, ""

    module_path = module_path.strip()
    if not module_path.startswith("pipelines."):
        module_path = f"pipelines.{module_path}"
    if not re.fullmatch(r"pipelines(?:\.[A-Za-z_][A-Za-z0-9_]*)+", module_path):
        raise ValueError(
            "Handler modules must be Python modules inside the project's pipelines package."
        )
    if function_name and not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", function_name):
        raise ValueError("Handler must be a simple function name.")
    if not function_name:
        function_name = f"run_{module_path.rsplit('.', 1)[-1]}"
    return module_path, function_name


def load_pipeline_registry(path: str | Path | None = None) -> dict[str, dict[str, Any]]:
    """Load and validate configured pipelines, keyed by their unique name."""
    config_path = Path(path) if path is not None else Path(config.PIPELINES_CONFIG_PATH)
    if not config_path.is_absolute():
        config_path = config.BASE_DIR / config_path
    data = _parse_simple_yaml_config(config_path)
    pipelines = data.get("pipelines", [])
    registry: dict[str, dict[str, Any]] = {}
    seen_names: set[str] = set()
    if not isinstance(pipelines, list) or not pipelines:
        raise ValueError(f"No pipeline entries found in {config_path}")

    for index, entry in enumerate(pipelines, start=1):
        if not isinstance(entry, dict):
            raise ValueError(f"Pipeline entry {index} must be a mapping.")
        name = entry.get("name")
        if not isinstance(name, str) or not PIPELINE_NAME_PATTERN.fullmatch(name.strip()):
            raise ValueError(f"Pipeline entry {index} has an invalid or missing name.")
        name = name.strip()
        if name.casefold() in seen_names:
            raise ValueError(f"Duplicate pipeline name '{name}' in {config_path}.")
        seen_names.add(name.casefold())
        source = entry.get("source")
        if not isinstance(source, str) or source.strip().upper() not in {"API", "CSV"}:
            raise ValueError(
                f"Pipeline '{name}' source must be API or CSV to match the existing PostgreSQL schema."
            )
        interval = entry.get("interval", config.PIPELINE_INTERVAL_SECONDS)
        max_retries = entry.get("max_retries", config.MAX_RETRIES)
        enabled = entry.get("enabled", True)
        if isinstance(interval, bool) or not isinstance(interval, int) or interval < 1:
            raise ValueError(f"Pipeline '{name}' interval must be a positive integer.")
        if isinstance(max_retries, bool) or not isinstance(max_retries, int) or max_retries < 0:
            raise ValueError(f"Pipeline '{name}' max_retries must be a non-negative integer.")
        if not isinstance(enabled, bool):
            raise ValueError(f"Pipeline '{name}' enabled must be true or false.")

        module_path, function_name = _resolve_handler_spec(entry)
        try:
            runner = resolve_pipeline_runner(module_path, function_name)
        except (ImportError, AttributeError, ValueError) as exc:
            raise ValueError(f"Pipeline '{name}' has an invalid handler: {exc}") from exc
        if not callable(runner):
            raise ValueError(f"Pipeline '{name}' handler is not callable.")

        source_config = entry.get("source_config", {})
        if not isinstance(source_config, dict):
            raise ValueError(f"Pipeline '{name}' source_config must be a mapping.")
        metadata_keys = {
            "name", "source", "handler", "module", "interval",
            "max_retries", "enabled", "source_config",
        }
        source_config = {
            **{key: value for key, value in entry.items() if key not in metadata_keys},
            **source_config,
        }
        pipeline_type = source.strip().upper()
        if pipeline_type == "CSV":
            source_config.setdefault(
                "file_path",
                str(config.CSV_PIPELINE_PATH.relative_to(config.BASE_DIR)),
            )
        else:
            source_config.setdefault("url", config.API_BASE_URL)
        registration = validate_pipeline_registration(
            name,
            pipeline_type,
            source_config,
            interval,
            max_retries,
        )
        registry[name] = {
            **registration,
            "source": pipeline_type,
            "interval": registration["interval_seconds"],
            "enabled": enabled,
            "module": module_path,
            "handler_function": function_name,
            "handler": f"{module_path}:{function_name}",
        }
    return registry


def seed_pipeline_registry(db: Database) -> None:
    """Migrate configured pipelines into PostgreSQL without replacing existing rows."""
    if db.fetch_one(
        "SELECT 1 FROM pipeline_registry_seed_state WHERE seed_key = %s",
        ("legacy_pipelines_v1",),
    ):
        return

    for pipeline in load_pipeline_registry(config.PIPELINES_CONFIG_PATH).values():
        pipeline_type = str(pipeline.get("source", "")).upper()
        if pipeline_type not in {"CSV", "API"}:
            continue

        source_config = pipeline.get("source_config") or {}
        if pipeline_type == "CSV":
            file_path = source_config.get("file_path") or pipeline.get("file_path")
            if not file_path and pipeline["name"] == "csv_pipeline":
                file_path = str(config.CSV_PIPELINE_PATH.relative_to(config.BASE_DIR))
            source_config = {"file_path": file_path} if file_path else {}
        else:
            url = source_config.get("url") or pipeline.get("url")
            source_config = {"url": url or config.API_BASE_URL}

        try:
            registration = validate_pipeline_registration(
                pipeline["name"],
                pipeline_type,
                source_config,
                pipeline.get("interval", config.PIPELINE_INTERVAL_SECONDS),
                pipeline.get("max_retries", config.MAX_RETRIES),
            )
        except (TypeError, ValueError) as exc:
            logger.warning("Skipping invalid configured pipeline %s: %s", pipeline.get("name"), exc)
            continue

        db.execute(
            """
            INSERT INTO pipeline_registry
                (pipeline_name, pipeline_type, source_config, interval_seconds,
                 max_retries, enabled)
            VALUES (%s, %s, %s::jsonb, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                registration["name"],
                registration["pipeline_type"],
                json.dumps(registration["source_config"]),
                registration["interval_seconds"],
                registration["max_retries"],
                bool(pipeline.get("enabled", True)),
            ),
            commit=False,
        )
    db.execute(
        "INSERT INTO pipeline_registry_seed_state (seed_key) VALUES (%s) ON CONFLICT DO NOTHING",
        ("legacy_pipelines_v1",),
        commit=False,
    )
    db.connection.commit()


def load_registered_pipeline_configs(db: Database, include_deleted: bool = False) -> list[dict[str, Any]]:
    """Read database-backed pipeline configuration for scheduler and UI consumers."""
    where_clause = "" if include_deleted else "WHERE deleted_at IS NULL"
    rows = db.fetch_all(
        f"""
        SELECT pipeline_name, pipeline_type, source_config, interval_seconds,
               max_retries, enabled, created_at, updated_at, deleted_at
        FROM pipeline_registry
        {where_clause}
        ORDER BY pipeline_name
        """
    )
    for row in rows:
        row["name"] = row["pipeline_name"]
        row["source"] = row["pipeline_type"]
        row["handler"] = f"{row['pipeline_type'].lower()}_pipeline"
        row["interval"] = row["interval_seconds"]
    return rows


def _sync_configured_pipeline_registry(
    db: Database, configured: dict[str, dict[str, Any]]
) -> None:
    """Make YAML registrations visible to monitoring without overwriting UI edits or resurrecting deleted pipelines."""
    rows = db.fetch_all("SELECT pipeline_name, deleted_at FROM pipeline_registry")
    existing_active = set()
    tombstoned = set()
    for row in rows:
        name_cf = row["pipeline_name"].casefold()
        if row.get("deleted_at") is not None:
            tombstoned.add(name_cf)
        else:
            existing_active.add(name_cf)

    inserted = False
    for pipeline in configured.values():
        name_cf = pipeline["name"].casefold()
        if name_cf in existing_active or name_cf in tombstoned:
            continue
        db.execute(
            """
            INSERT INTO pipeline_registry
                (pipeline_name, pipeline_type, source_config, interval_seconds,
                 max_retries, enabled)
            VALUES (%s, %s, %s::jsonb, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                pipeline["name"],
                pipeline["pipeline_type"],
                json.dumps(pipeline["source_config"]),
                pipeline["interval_seconds"],
                pipeline["max_retries"],
                pipeline["enabled"],
            ),
            commit=False,
        )
        inserted = True
    if inserted:
        db.connection.commit()


def initialize_pipeline_registry() -> list[dict[str, Any]]:
    """Apply additive schema updates and seed the old configured pipelines once."""
    db = Database()
    try:
        db.connect()
        db.ensure_monitoring_schema()
        seed_pipeline_registry(db)
        return load_registered_pipeline_configs(db)
    finally:
        db.close()


def get_enabled_pipeline_configs(path: str | Path | None = None) -> list[dict[str, Any]]:
    configured = load_pipeline_registry(path)
    if path is not None:
        return [entry for entry in configured.values() if entry.get("enabled", True)]
    db = Database()
    try:
        db.connect()
        _sync_configured_pipeline_registry(db, configured)
        tombstones = {
            row["pipeline_name"].casefold()
            for row in db.fetch_all("SELECT pipeline_name FROM pipeline_registry WHERE deleted_at IS NOT NULL")
        }
        # Remove tombstoned entries from static config mapping so scheduler never executes them
        configured = {
            k: v for k, v in configured.items()
            if k.casefold() not in tombstones
        }
        database_rows = load_registered_pipeline_configs(db)
        for row in database_rows:
            name = row["pipeline_name"]
            if name in configured:
                configured[name]["enabled"] = (
                    configured[name]["enabled"] and bool(row["enabled"])
                )
                continue
            pipeline_type = str(row["pipeline_type"]).upper()
            module_path = f"pipelines.{pipeline_type.lower()}_pipeline"
            configured[name] = {
                **row,
                "name": name,
                "source": pipeline_type,
                "pipeline_type": pipeline_type,
                "source_config": row.get("source_config") or {},
                "interval_seconds": row["interval_seconds"],
                "interval": row["interval_seconds"],
                "max_retries": row["max_retries"],
                "module": module_path,
                "handler_function": f"run_{pipeline_type.lower()}_pipeline",
                "handler": f"{module_path}:run_{pipeline_type.lower()}_pipeline",
            }
        return [
            entry for entry in configured.values() if entry.get("enabled", True)
        ]
    finally:
        db.close()


def register_pipeline(
    db: Database,
    name: str,
    pipeline_type: str,
    source_config: dict[str, Any],
    interval_seconds: int,
    max_retries: int,
    enabled: bool = True,
) -> dict[str, Any]:
    registration = validate_pipeline_registration(
        name, pipeline_type, source_config, interval_seconds, max_retries
    )
    existing = db.fetch_one(
        "SELECT pipeline_name, deleted_at FROM pipeline_registry WHERE LOWER(pipeline_name) = LOWER(%s)",
        (registration["name"],),
    )
    if existing and existing.get("deleted_at") is None:
        raise ValueError("Pipeline name already exists.")

    if existing and existing.get("deleted_at") is not None:
        # Reusing a deleted pipeline name: resurrect as a clean new pipeline with new created_at
        db.execute(
            """
            UPDATE pipeline_registry
            SET pipeline_name = %s,
                pipeline_type = %s,
                source_config = %s::jsonb,
                interval_seconds = %s,
                max_retries = %s,
                enabled = %s,
                created_at = CURRENT_TIMESTAMP,
                updated_at = CURRENT_TIMESTAMP,
                deleted_at = NULL
            WHERE LOWER(pipeline_name) = LOWER(%s)
            """,
            (
                registration["name"],
                registration["pipeline_type"],
                json.dumps(registration["source_config"]),
                registration["interval_seconds"],
                registration["max_retries"],
                bool(enabled),
                registration["name"],
            ),
        )
        return registration | {"enabled": bool(enabled)}

    try:
        db.execute(
            """
            INSERT INTO pipeline_registry
                (pipeline_name, pipeline_type, source_config, interval_seconds,
                 max_retries, enabled)
            VALUES (%s, %s, %s::jsonb, %s, %s, %s)
            """,
            (
                registration["name"],
                registration["pipeline_type"],
                json.dumps(registration["source_config"]),
                registration["interval_seconds"],
                registration["max_retries"],
                bool(enabled),
            ),
        )
    except DatabaseError as exc:
        if db.fetch_one(
            "SELECT 1 FROM pipeline_registry WHERE LOWER(pipeline_name) = LOWER(%s) AND deleted_at IS NULL",
            (registration["name"],),
        ):
            raise ValueError("Pipeline name already exists.") from exc
        raise

    return registration | {"enabled": bool(enabled)}


def set_pipeline_enabled(db: Database, name: str, enabled: bool) -> None:
    updated = db.execute(
        "UPDATE pipeline_registry SET enabled = %s, updated_at = CURRENT_TIMESTAMP WHERE pipeline_name = %s",
        (bool(enabled), name),
    )
    if not updated:
        raise ValueError("Pipeline was not found.")


def delete_disabled_pipeline(db: Database, name: str) -> bool:
    """Soft-delete one disabled registry row; execution history is never modified."""
    # Check if an execution is currently RUNNING
    try:
        running = db.fetch_one(
            "SELECT 1 FROM pipeline_execution WHERE pipeline_name = %s AND status = 'RUNNING'",
            (name,),
        )
        if running and isinstance(running, dict) and any(running.values()):
            raise ValueError(
                f"Cannot delete pipeline '{name}' while an execution is currently RUNNING. Please wait for the execution to finish."
            )
    except ValueError:
        raise
    except Exception:
        pass

    deleted = db.execute(
        "UPDATE pipeline_registry SET deleted_at = CURRENT_TIMESTAMP, enabled = FALSE "
        "WHERE pipeline_name = %s AND enabled = FALSE AND deleted_at IS NULL",
        (name,),
    )
    if not deleted:
        # Check if it was already deleted or does not exist (idempotent), or if it is currently enabled
        try:
            row = db.fetch_one(
                "SELECT enabled, deleted_at FROM pipeline_registry WHERE pipeline_name = %s",
                (name,),
            )
            if isinstance(row, dict):
                if row.get("deleted_at") is not None:
                    return False
                if not row:
                    return False
                if row.get("enabled"):
                    raise ValueError("Only a disabled registered pipeline can be deleted.")
            elif row is None:
                return False
        except ValueError:
            raise
        except Exception:
            return False
        raise ValueError("Only a disabled registered pipeline can be deleted.")
    return True


def resolve_pipeline_runner(handler_name: str, function_name: str | None = None):
    """Resolve a configured callable from inside the pipelines package only."""
    module_path = handler_name.strip()
    legacy_type = module_path.upper().removesuffix("_PIPELINE")
    if legacy_type in {"API", "CSV"}:
        module_path = f"pipelines.{legacy_type.lower()}_pipeline"
        if function_name is None:
            function_name = f"run_{legacy_type.lower()}_pipeline"
    if ":" in module_path and function_name is None:
        module_path, function_name = module_path.split(":", 1)
    if not module_path.startswith("pipelines."):
        module_path = f"pipelines.{module_path}"
    if not re.fullmatch(r"pipelines(?:\.[A-Za-z_][A-Za-z0-9_]*)+", module_path):
        raise ValueError(f"Unsupported pipeline handler module: {handler_name}")

    if function_name is None:
        module_stem = module_path.rsplit(".", 1)[-1]
        function_name = f"run_{module_stem}"
    try:
        module = importlib.import_module(module_path)
        if getattr(module, "__file__", None):
            try:
                module = importlib.reload(module)
            except Exception:
                pass
    except ModuleNotFoundError as exc:
        if exc.name == module_path or module_path.startswith(f"{exc.name}."):
            raise ValueError(
                f"Unsupported pipeline handler: module '{module_path}' was not found."
            ) from exc
        raise
    try:
        runner = getattr(module, function_name)
    except AttributeError as exc:
        raise ValueError(
            f"Pipeline handler '{function_name}' was not found in '{module_path}'."
        ) from exc
    if not callable(runner):
        raise ValueError(f"Pipeline handler '{module_path}:{function_name}' is not callable.")
    return runner
