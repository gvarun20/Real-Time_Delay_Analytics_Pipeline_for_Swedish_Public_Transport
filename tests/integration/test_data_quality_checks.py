"""Integration tests for jobs/validate_data_quality.py against a real Postgres.

These are read-only: no test here writes new fact rows, so they're safe to
run against a shared development database. They prove the Week 3 phase gate
acceptance criterion: "data quality task fails intentionally on bad data."
"""

from __future__ import annotations

import pytest

from jobs.validate_data_quality import DataQualityError, raise_on_failures, run_checks, validate

# Inside the seeded dim_date range (2024-2027) but never loaded with facts
# in this project's timeline — a safe stand-in for "bad/missing data".
EMPTY_SERVICE_DATE = "2025-01-01"


def test_run_checks_flags_error_for_a_date_with_no_facts(pg_conn):
    results = run_checks(EMPTY_SERVICE_DATE, conn=pg_conn)
    row_count_check = next(r for r in results if r.check_id == "DQ-001")
    assert row_count_check.passed is False
    assert row_count_check.severity == "ERROR"


def test_raise_on_failures_raises_for_empty_date(pg_conn):
    results = run_checks(EMPTY_SERVICE_DATE, conn=pg_conn)
    with pytest.raises(DataQualityError):
        raise_on_failures(results)


def test_validate_raises_dataqualityerror_for_empty_date(pg_conn):
    with pytest.raises(DataQualityError):
        validate(EMPTY_SERVICE_DATE, conn=pg_conn)


def test_run_checks_on_real_loaded_date_has_no_error_failures(pg_conn):
    with pg_conn.cursor() as cur:
        cur.execute(
            """
            SELECT DISTINCT d.full_date
            FROM dim_date d
            JOIN fact_trip_delay f ON f.date_key = d.date_key
            LIMIT 1
            """
        )
        row = cur.fetchone()
    if row is None:
        pytest.skip("No fact_trip_delay data loaded yet; run the gtfs_transform DAG first")
    service_date = row[0].isoformat()

    results = run_checks(service_date, conn=pg_conn)
    error_failures = [r for r in results if not r.passed and r.severity == "ERROR"]
    assert error_failures == [], f"Unexpected ERROR-severity failures: {error_failures}"
