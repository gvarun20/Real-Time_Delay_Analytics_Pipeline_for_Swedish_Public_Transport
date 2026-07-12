"""Daily static GTFS ingestion DAG."""

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
from jobs.ingest.fetch_static_gtfs import run_fetch_static  # noqa: E402


def fetch_static_task(**context) -> str:
    ti = context["task_instance"]
    service_date = context["ds"]
    output = run_fetch_static(
        service_date=service_date,
        dag_id=ti.dag_id,
        task_id=ti.task_id,
        run_id=context.get("run_id"),
    )
    return str(output)


with DAG(
    dag_id="gtfs_static_ingest",
    default_args=DEFAULT_ARGS,
    description="Fetch daily static GTFS zip from Trafiklab",
    schedule="@daily",
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["gtfs", "ingest", "static"],
    doc_md="""
    ## gtfs_static_ingest

    Downloads the **GTFS Sweden 3** static zip daily and lands it under
    `data/raw/static/{YYYY-MM-DD}/gtfs.zip`.

    Requires `TRAFIKLAB_STATIC_API_KEY` with GTFS Sweden 3 Static subscription.

    See [week1-runbook.md](../docs/week1-runbook.md).
    """,
) as dag:
    fetch_static_gtfs = PythonOperator(
        task_id="fetch_static_gtfs",
        python_callable=fetch_static_task,
    )
