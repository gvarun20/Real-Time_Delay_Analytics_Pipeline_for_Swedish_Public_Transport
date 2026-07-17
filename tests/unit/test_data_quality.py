"""Unit tests for the pure-logic data quality check functions.

These test the ERROR/WARN decision logic in isolation, without touching
Postgres. DB-backed integration coverage lives in
`tests/integration/test_data_quality_checks.py`.
"""

from __future__ import annotations

import pytest

from jobs.validate_data_quality import (
    ERROR,
    WARN,
    DataQualityError,
    evaluate_delay_bounds,
    evaluate_duplicate_grain,
    evaluate_null_delay_rate,
    evaluate_null_trip_ids,
    evaluate_row_count,
    evaluate_static_freshness,
    raise_on_failures,
)


def test_row_count_passes_when_positive():
    result = evaluate_row_count(638)
    assert result.check_id == "DQ-001"
    assert result.severity == ERROR
    assert result.passed is True


def test_row_count_fails_when_zero():
    result = evaluate_row_count(0)
    assert result.passed is False
    assert result.severity == ERROR


def test_null_trip_ids_passes_when_zero():
    result = evaluate_null_trip_ids(0)
    assert result.passed is True


def test_null_trip_ids_fails_when_present():
    result = evaluate_null_trip_ids(3)
    assert result.passed is False
    assert result.severity == ERROR
    assert "3" in result.detail


def test_delay_bounds_passes_when_all_within_range():
    result = evaluate_delay_bounds(out_of_range=0, total=100)
    assert result.passed is True
    assert result.severity == WARN


def test_delay_bounds_warns_when_outliers_present():
    result = evaluate_delay_bounds(out_of_range=5, total=100)
    assert result.passed is False
    assert result.severity == WARN
    assert "5/100" in result.detail


def test_null_delay_rate_passes_below_threshold():
    result = evaluate_null_delay_rate(null_count=10, total=100, max_null_rate=0.8)
    assert result.passed is True


def test_null_delay_rate_fails_above_threshold():
    result = evaluate_null_delay_rate(null_count=90, total=100, max_null_rate=0.8)
    assert result.passed is False
    assert result.severity == WARN


def test_null_delay_rate_handles_zero_total():
    result = evaluate_null_delay_rate(null_count=0, total=0)
    assert result.passed is True


def test_duplicate_grain_passes_when_zero():
    result = evaluate_duplicate_grain(0)
    assert result.passed is True
    assert result.severity == ERROR


def test_duplicate_grain_fails_when_present():
    result = evaluate_duplicate_grain(2)
    assert result.passed is False
    assert result.severity == ERROR


def test_static_freshness_passes_when_recent():
    result = evaluate_static_freshness(age_days=1.0, max_age_days=7)
    assert result.passed is True
    assert result.severity == WARN


def test_static_freshness_warns_when_stale():
    result = evaluate_static_freshness(age_days=10.0, max_age_days=7)
    assert result.passed is False


def test_static_freshness_warns_when_unknown():
    result = evaluate_static_freshness(age_days=None)
    assert result.passed is False
    assert "unknown" in result.detail


def test_raise_on_failures_raises_for_error_severity():
    results = [evaluate_row_count(0)]
    with pytest.raises(DataQualityError):
        raise_on_failures(results)


def test_raise_on_failures_does_not_raise_for_warn_only():
    results = [evaluate_delay_bounds(out_of_range=5, total=100)]
    raise_on_failures(results)  # should not raise


def test_raise_on_failures_passes_when_all_pass():
    results = [evaluate_row_count(10), evaluate_null_trip_ids(0)]
    raise_on_failures(results)  # should not raise
