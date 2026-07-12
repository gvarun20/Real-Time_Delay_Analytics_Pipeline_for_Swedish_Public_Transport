"""Frequent GTFS-RT TripUpdates snapshot ingestion DAG."""

from __future__ import annotations

import sys
from datetime import datetime
from pathlib import Path

from airflow import DAG
from airflow.operators.python import PythonOperator

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from dags.common import DEFAULT_ARGS  # noqa: E402
from jobs.ingest.fetch_realtime_snapshot import run_fetch_realtime  # noqa: E402


def fetch_realtime_task(**context) -> str:
    ti = context["task_instance"]
    service_date = context["ds"]
    output_path, _ = run_fetch_realtime(
        service_date=service_date,
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=context.get("run_id"),
    )
    return output_path


with DAG(
    dag_id="gtfs_realtime_ingest",
    default_args=DEFAULT_ARGS,
    description="Fetch GTFS-RT TripUpdates snapshots every 15 minutes",
    schedule="*/15 * * * *",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gtfs", "ingest", "realtime"],
    doc_md="""
    ## gtfs_realtime_ingest

    Pulls **GTFS Regional Realtime** TripUpdates for SL every 15 minutes.
    Lands protobuf snapshots under `data/raw/realtime/{date}/{timestamp}/`.

    Requires `TRAFIKLAB_REALTIME_API_KEY` and `REALTIME_FEED=gtfs_regional`.

    See [week1-runbook.md](../docs/week1-runbook.md).
    """,
) as dag:
    fetch_realtime_snapshot = PythonOperator(
        task_id="fetch_realtime_snapshot",
        python_callable=fetch_realtime_task,
    )
