"""Integration tests confirming the star schema exists and is queryable.

Read-only — verifies table/column presence and basic referential integrity,
does not depend on any specific amount of data being loaded.
"""

from __future__ import annotations

EXPECTED_TABLES = {
    "dim_date",
    "dim_route",
    "dim_stop",
    "dim_vehicle_type",
    "fact_trip_delay",
    "pipeline_runs",
}


def test_all_star_schema_tables_exist(pg_cursor):
    pg_cursor.execute(
        """
        SELECT table_name FROM information_schema.tables
        WHERE table_schema = 'public'
        """
    )
    existing = {row[0] for row in pg_cursor.fetchall()}
    missing = EXPECTED_TABLES - existing
    assert not missing, f"Missing tables: {missing}"


def test_dim_date_has_rows(pg_cursor):
    pg_cursor.execute("SELECT COUNT(*) FROM dim_date")
    (count,) = pg_cursor.fetchone()
    assert count > 0, "dim_date should be pre-seeded (sql/seed_dim_date.sql)"


def test_dim_vehicle_type_is_seeded(pg_cursor):
    pg_cursor.execute("SELECT COUNT(*) FROM dim_vehicle_type")
    (count,) = pg_cursor.fetchone()
    assert count >= 8, "dim_vehicle_type should be seeded from GTFS route_type mapping"


def test_fact_trip_delay_has_no_orphaned_route_keys(pg_cursor):
    pg_cursor.execute(
        """
        SELECT COUNT(*) FROM fact_trip_delay f
        LEFT JOIN dim_route r ON f.route_key = r.route_key
        WHERE r.route_key IS NULL
        """
    )
    (orphans,) = pg_cursor.fetchone()
    assert orphans == 0


def test_fact_trip_delay_has_no_orphaned_stop_keys(pg_cursor):
    pg_cursor.execute(
        """
        SELECT COUNT(*) FROM fact_trip_delay f
        LEFT JOIN dim_stop s ON f.stop_key = s.stop_key
        WHERE s.stop_key IS NULL
        """
    )
    (orphans,) = pg_cursor.fetchone()
    assert orphans == 0


def test_fact_trip_delay_grain_has_no_duplicates(pg_cursor):
    pg_cursor.execute(
        """
        SELECT COALESCE(SUM(cnt) - COUNT(*), 0) FROM (
            SELECT COUNT(*) AS cnt
            FROM fact_trip_delay
            GROUP BY trip_id, stop_key, date_key, stop_sequence
        ) grouped
        """
    )
    (dup_count,) = pg_cursor.fetchone()
    assert dup_count == 0
