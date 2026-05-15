"""
utils.py — Shared utilities for the VITAL-Flow pipeline.
Provides config loading, logging helpers, and a PySpark session factory.
"""

import yaml
import logging
import csv
import os
import uuid
from datetime import datetime, timezone
from pyspark.sql import SparkSession


def load_config(config_path: str = "config.yaml") -> dict:
    """Load the project config.yaml and return as a dict."""
    with open(config_path, "r") as f:
        return yaml.safe_load(f)


def get_spark(app_name: str = "VITALFlow") -> SparkSession:
    """Create or retrieve a SparkSession."""
    return (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def get_logger(name: str) -> logging.Logger:
    """Return a configured logger."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
        datefmt="%Y-%m-%dT%H:%M:%S"
    )
    return logging.getLogger(name)


def log_run(
    log_path: str,
    source: str,
    rows_ingested: int,
    null_rate_pct: float,
    status: str
) -> None:
    """
    Append a pipeline run record to the ingestion log CSV.
    Creates the file with headers if it does not exist.
    """
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    file_exists = os.path.isfile(log_path)
    with open(log_path, "a", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "run_id", "source", "timestamp",
            "rows_ingested", "null_rate_pct", "status"
        ])
        if not file_exists:
            writer.writeheader()
        writer.writerow({
            "run_id": str(uuid.uuid4()),
            "source": source,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "rows_ingested": rows_ingested,
            "null_rate_pct": round(null_rate_pct, 4),
            "status": status
        })
