"""Airflow callbacks and shared DAG helpers."""

from __future__ import annotations

import logging
from datetime import timedelta
from pathlib import Path

logger = logging.getLogger(__name__)

FAILURE_LOG_DIR = Path("/opt/airflow/project/logs/failures")
LOCAL_FAILURE_LOG_DIR = Path(__file__).resolve().parents[1] / "logs" / "failures"


def on_failure_callback(context: dict) -> None:
    task_instance = context["task_instance"]
    message = (
        f"Task {task_instance.task_id} failed in DAG {task_instance.dag_id} "
        f"(run_id={context.get('run_id')})"
    )
    logger.error(message)

    log_dir = FAILURE_LOG_DIR if FAILURE_LOG_DIR.parent.exists() else LOCAL_FAILURE_LOG_DIR
    log_dir.mkdir(parents=True, exist_ok=True)
    log_file = log_dir / f"{task_instance.dag_id}_{task_instance.task_id}.log"
    log_file.write_text(message + "\n", encoding="utf-8")


DEFAULT_ARGS = {
    "owner": "transit-pipeline",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "on_failure_callback": on_failure_callback,
}
