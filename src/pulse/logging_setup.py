"""One logging format for every Pulse process (producer, Spark jobs, Airflow tasks)."""

import logging
import os


def setup_logging(name: str = "pulse") -> logging.Logger:
    level = os.getenv("PULSE_LOG_LEVEL", "INFO").upper()
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S",
    )
    # Third-party libraries are noisy at INFO; keep our own logs readable.
    for noisy in ("py4j", "yfinance", "urllib3", "peewee"):
        logging.getLogger(noisy).setLevel(logging.WARNING)
    return logging.getLogger(name)
