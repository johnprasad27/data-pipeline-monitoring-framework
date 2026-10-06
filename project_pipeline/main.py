"""Entry point for the project pipeline application."""

from __future__ import annotations

import logging

import config
from pipelines.scheduler import run_forever


def configure_logging() -> None:
    config.LOG_FILE.parent.mkdir(parents=True, exist_ok=True)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(), logging.FileHandler(config.LOG_FILE, encoding="utf-8")],
    )

def main() -> None:
    configure_logging()
    run_forever()


if __name__ == "__main__":
    main()
