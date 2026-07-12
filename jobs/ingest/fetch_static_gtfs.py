"""Download and land static GTFS zip from Trafiklab."""

from __future__ import annotations

import argparse
import logging
import sys
import zipfile
from datetime import date
from pathlib import Path

import requests

from config.settings import (
    OPERATOR,
    OPERATOR_NAME,
    TRAFIKLAB_STATIC_API_KEY,
    static_gtfs_url,
)
from jobs.ingest.audit import record_ingest_run
from jobs.ingest.common import GTFS_HEADERS, static_landing_dir, write_metadata

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def run_fetch_static(
    service_date: str | date | None = None,
    operator: str | None = None,
    api_key: str | None = None,
    *,
    dag_id: str | None = None,
    task_id: str | None = None,
    run_id: str | None = None,
) -> Path:
    service_date = service_date or date.today().isoformat()
    operator = (operator or OPERATOR).lower()
    api_key = api_key or TRAFIKLAB_STATIC_API_KEY

    if not api_key or api_key in ("your_api_key_here", "your_static_key_here"):
        raise ValueError(
            "TRAFIKLAB_STATIC_API_KEY is not set. Edit .env in VS Code:\n"
            "  TRAFIKLAB_STATIC_API_KEY=<key from 'GTFS Sweden 3 Static data'>\n"
            "Then run: docker compose up -d --force-recreate airflow-scheduler airflow-webserver"
        )

    landing_dir = static_landing_dir(service_date)
    output_path = landing_dir / "gtfs.zip"
    url = static_gtfs_url(operator, api_key)

    logger.info("Fetching static GTFS for %s from Trafiklab", operator)
    response = requests.get(url, headers=GTFS_HEADERS, timeout=120)
    if response.status_code == 403:
        detail = ""
        try:
            detail = response.json().get("errorMessage", "")
        except Exception:
            pass
        raise ValueError(
            "Trafiklab returned 403 Forbidden"
            + (f": {detail}" if detail else "")
            + ". Use the key from 'GTFS Sweden 3 Static data' (or GTFS Regional Static "
            "with STATIC_FEED=gtfs_regional) at https://developer.trafiklab.se"
        )
    response.raise_for_status()

    output_path.write_bytes(response.content)
    with zipfile.ZipFile(output_path) as archive:
        required = {"stops.txt", "routes.txt", "trips.txt", "stop_times.txt"}
        names = set(archive.namelist())
        missing = sorted(required - names)
        if missing:
            raise ValueError(f"GTFS zip missing required files: {missing}")

    write_metadata(
        landing_dir,
        operator=OPERATOR_NAME,
        feed_type="static_gtfs",
        record_count=len(names),
        api_status=response.status_code,
        extra={"operator_code": operator, "service_date": str(service_date)},
    )
    logger.info("Saved static GTFS to %s (%d bytes)", output_path, output_path.stat().st_size)
    record_ingest_run(
        feed_type="static_gtfs",
        service_date=str(service_date),
        file_path=str(output_path),
        bytes_written=output_path.stat().st_size,
        record_count=len(names),
        api_status=response.status_code,
        dag_id=dag_id,
        task_id=task_id,
        run_id=run_id,
        extra={"operator_code": operator},
    )
    return output_path


def main() -> int:
    parser = argparse.ArgumentParser(description="Fetch static GTFS from Trafiklab")
    parser.add_argument("--service-date", default=date.today().isoformat())
    parser.add_argument("--operator", default=OPERATOR)
    args = parser.parse_args()

    try:
        run_fetch_static(service_date=args.service_date, operator=args.operator)
    except Exception:
        logger.exception("Static GTFS fetch failed")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
