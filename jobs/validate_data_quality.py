"""Week 3: Data quality validation for the `fact_trip_delay` star schema.

Runs the check catalog from the project's Data Quality Framework against a
single service_date's fact rows. ERROR-severity checks raise `DataQualityError`
(and should fail the Airflow DAG); WARN-severity checks are logged but do not
fail the run.

Each check is split into a pure "evaluate_*" function (no I/O, easily unit
tested) and a DB query that feeds it, so the check catalog's *logic* can be
tested without a live Postgres connection.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import dataclass
from datetime import date, datetime, timezone

from config.settings import DATA_RAW_DIR
from jobs.transform.loaders import get_connection, update_dq_status
from jobs.transform.time_utils import date_to_date_key

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
logger = logging.getLogger(__name__)

ERROR = "ERROR"
WARN = "WARN"

DELAY_LOWER_BOUND_SEC = -3600
DELAY_UPPER_BOUND_SEC = 10800
MAX_NULL_DELAY_RATE = 0.8
MAX_STATIC_AGE_DAYS = 7


class DataQualityError(Exception):
    """Raised when one or more ERROR-severity data quality checks fail."""


@dataclass
class CheckResult:
    check_id: str
    description: str
    severity: str  # ERROR | WARN
    passed: bool
    detail: str

    def __str__(self) -> str:
        status = "PASS" if self.passed else f"FAIL ({self.severity})"
        return f"[{self.check_id}] {status} — {self.description}: {self.detail}"


# ---------------------------------------------------------------------------
# Pure logic — unit-testable without a database
# ---------------------------------------------------------------------------


def evaluate_row_count(count: int) -> CheckResult:
    return CheckResult(
        check_id="DQ-001",
        description="fact_trip_delay row count > 0 for service_date",
        severity=ERROR,
        passed=count > 0,
        detail=f"{count} fact rows found",
    )


def evaluate_null_trip_ids(null_count: int) -> CheckResult:
    return CheckResult(
        check_id="DQ-002",
        description="No NULL trip_id in facts",
        severity=ERROR,
        passed=null_count == 0,
        detail=f"{null_count} rows with NULL trip_id",
    )


def evaluate_delay_bounds(
    out_of_range: int,
    total: int,
    lower: int = DELAY_LOWER_BOUND_SEC,
    upper: int = DELAY_UPPER_BOUND_SEC,
) -> CheckResult:
    return CheckResult(
        check_id="DQ-003",
        description=f"delay_seconds within [{lower}, {upper}]s",
        severity=WARN,
        passed=out_of_range == 0,
        detail=f"{out_of_range}/{total} rows outside bounds",
    )


def evaluate_null_delay_rate(
    null_count: int,
    total: int,
    max_null_rate: float = MAX_NULL_DELAY_RATE,
) -> CheckResult:
    rate = (null_count / total) if total else 0.0
    return CheckResult(
        check_id="DQ-004",
        description=f"delay_seconds NULL rate < {max_null_rate:.0%}",
        severity=WARN,
        passed=rate < max_null_rate,
        detail=f"NULL rate {rate:.1%} ({null_count}/{total})",
    )


def evaluate_duplicate_grain(dup_count: int) -> CheckResult:
    return CheckResult(
        check_id="DQ-006",
        description="No duplicate (trip_id, stop_key, date_key, stop_sequence) rows",
        severity=ERROR,
        passed=dup_count == 0,
        detail=f"{dup_count} duplicate grain rows",
    )


def evaluate_static_freshness(
    age_days: float | None,
    max_age_days: int = MAX_STATIC_AGE_DAYS,
) -> CheckResult:
    description = f"Static GTFS file age < {max_age_days} days"
    if age_days is None:
        return CheckResult(
            check_id="DQ-007",
            description=description,
            severity=WARN,
            passed=False,
            detail="Static gtfs.zip / metadata.json not found — age unknown",
        )
    return CheckResult(
        check_id="DQ-007",
        description=description,
        severity=WARN,
        passed=age_days < max_age_days,
        detail=f"Static file is {age_days:.1f} days old",
    )


# ---------------------------------------------------------------------------
# DB-backed data gathering
# ---------------------------------------------------------------------------


def _fetch_scalar(cur, sql: str, params: tuple) -> int:
    cur.execute(sql, params)
    row = cur.fetchone()
    return int(row[0]) if row and row[0] is not None else 0


def _gather_fact_stats(conn, date_key: int) -> dict[str, int]:
    with conn.cursor() as cur:
        row_count = _fetch_scalar(
            cur, "SELECT COUNT(*) FROM fact_trip_delay WHERE date_key = %s", (date_key,)
        )
        null_trip_ids = _fetch_scalar(
            cur,
            "SELECT COUNT(*) FROM fact_trip_delay WHERE date_key = %s AND trip_id IS NULL",
            (date_key,),
        )
        out_of_range = _fetch_scalar(
            cur,
            """
            SELECT COUNT(*) FROM fact_trip_delay
            WHERE date_key = %s AND delay_seconds IS NOT NULL
              AND (delay_seconds < %s OR delay_seconds > %s)
            """,
            (date_key, DELAY_LOWER_BOUND_SEC, DELAY_UPPER_BOUND_SEC),
        )
        null_delays = _fetch_scalar(
            cur,
            "SELECT COUNT(*) FROM fact_trip_delay WHERE date_key = %s AND delay_seconds IS NULL",
            (date_key,),
        )
        dup_count = _fetch_scalar(
            cur,
            """
            SELECT COALESCE(SUM(cnt) - COUNT(*), 0) FROM (
                SELECT COUNT(*) AS cnt
                FROM fact_trip_delay
                WHERE date_key = %s
                GROUP BY trip_id, stop_key, date_key, stop_sequence
            ) grouped
            """,
            (date_key,),
        )
    return {
        "row_count": row_count,
        "null_trip_ids": null_trip_ids,
        "out_of_range": out_of_range,
        "null_delays": null_delays,
        "dup_count": dup_count,
    }


def _static_file_age_days(service_date: str, raw_base=None) -> float | None:
    base = raw_base or DATA_RAW_DIR
    metadata_path = base / "static" / service_date / "metadata.json"
    if not metadata_path.exists():
        return None
    try:
        payload = json.loads(metadata_path.read_text(encoding="utf-8"))
        pulled_at = datetime.fromisoformat(payload["pulled_at_utc"])
        if pulled_at.tzinfo is None:
            pulled_at = pulled_at.replace(tzinfo=timezone.utc)
    except (KeyError, ValueError, OSError):
        return None
    age = datetime.now(timezone.utc) - pulled_at
    return age.total_seconds() / 86400


# ---------------------------------------------------------------------------
# Orchestration
# ---------------------------------------------------------------------------


def run_checks(service_date: str | date, conn=None, raw_base=None) -> list[CheckResult]:
    """Run the full DQ check catalog for a service_date and return results."""
    if isinstance(service_date, date):
        service_date = service_date.isoformat()
    date_key = date_to_date_key(service_date)

    own_conn = conn is None
    conn = conn or get_connection()
    try:
        stats = _gather_fact_stats(conn, date_key)
    finally:
        if own_conn:
            conn.close()

    age_days = _static_file_age_days(service_date, raw_base)

    return [
        evaluate_row_count(stats["row_count"]),
        evaluate_null_trip_ids(stats["null_trip_ids"]),
        evaluate_delay_bounds(stats["out_of_range"], stats["row_count"]),
        evaluate_null_delay_rate(stats["null_delays"], stats["row_count"]),
        evaluate_duplicate_grain(stats["dup_count"]),
        evaluate_static_freshness(age_days),
    ]


def raise_on_failures(results: list[CheckResult]) -> None:
    """Raise DataQualityError if any ERROR-severity check failed."""
    failed_errors = [r for r in results if not r.passed and r.severity == ERROR]
    if failed_errors:
        summary = "; ".join(str(r) for r in failed_errors)
        raise DataQualityError(f"{len(failed_errors)} data quality check(s) failed: {summary}")


def validate(
    service_date: str | date,
    dag_run_id: str | None = None,
    conn=None,
    raw_base=None,
) -> list[CheckResult]:
    """Run all checks, log results, persist dq_status, and raise on ERROR failures."""
    if isinstance(service_date, date):
        service_date = service_date.isoformat()

    results = run_checks(service_date, conn=conn, raw_base=raw_base)
    for result in results:
        log = logger.info if result.passed else (
            logger.warning if result.severity == WARN else logger.error
        )
        log(str(result))

    any_error_failed = any(not r.passed and r.severity == ERROR for r in results)
    any_warn_failed = any(not r.passed and r.severity == WARN for r in results)
    if any_error_failed:
        dq_status = "failed"
    elif any_warn_failed:
        dq_status = "warn"
    else:
        dq_status = "passed"

    update_dq_status(service_date, dq_status, dag_run_id=dag_run_id)
    logger.info("Data quality status for %s: %s", service_date, dq_status)

    raise_on_failures(results)
    return results


def main() -> int:
    parser = argparse.ArgumentParser(description="Run data quality checks on fact_trip_delay")
    parser.add_argument("--service-date", required=True)
    parser.add_argument("--dag-run-id", default=None)
    args = parser.parse_args()

    try:
        validate(args.service_date, dag_run_id=args.dag_run_id)
    except DataQualityError as exc:
        logger.error("Data quality validation FAILED: %s", exc)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
