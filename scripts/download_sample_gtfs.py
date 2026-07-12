#!/usr/bin/env python3
"""Standalone script to download static GTFS and a realtime snapshot."""

from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

from jobs.ingest.fetch_realtime_snapshot import run_fetch_realtime  # noqa: E402
from jobs.ingest.fetch_static_gtfs import run_fetch_static  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)


def main() -> int:
    parser = argparse.ArgumentParser(description="Download GTFS static + realtime snapshot")
    parser.add_argument("--operator", default="sl", help="Trafiklab operator code (e.g. sl, otraf)")
    parser.add_argument("--service-date", default=date.today().isoformat())
    parser.add_argument("--static-only", action="store_true")
    parser.add_argument("--realtime-only", action="store_true")
    args = parser.parse_args()

    try:
        if not args.realtime_only:
            run_fetch_static(service_date=args.service_date, operator=args.operator)
        if not args.static_only:
            run_fetch_realtime(service_date=args.service_date, operator=args.operator)
    except Exception:
        logger.exception("Download failed")
        return 1

    logger.info("Done. Check data/raw/ for landed files.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
