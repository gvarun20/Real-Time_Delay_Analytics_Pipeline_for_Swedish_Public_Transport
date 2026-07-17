"""Parameterized SQL queries for the Streamlit dashboard.

All queries go through SQLAlchemy `text()` with bound parameters (never
f-string interpolation of user-controlled values) to avoid SQL injection from
sidebar filter selections.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date

import pandas as pd
from sqlalchemy import bindparam, create_engine, text
from sqlalchemy.engine import Engine

from config.settings import postgres_url
from jobs.transform.time_utils import date_to_date_key

_engine: Engine | None = None


def get_engine() -> Engine:
    global _engine
    if _engine is None:
        _engine = create_engine(postgres_url(), pool_pre_ping=True)
    return _engine


@dataclass
class Filters:
    start_date: date
    end_date: date
    route_ids: list[str] = field(default_factory=list)
    vehicle_types: list[str] = field(default_factory=list)

    @property
    def start_date_key(self) -> int:
        return date_to_date_key(self.start_date)

    @property
    def end_date_key(self) -> int:
        return date_to_date_key(self.end_date)


def _where_clause(filters: Filters) -> tuple[str, dict]:
    clauses = ["f.date_key BETWEEN :start_date_key AND :end_date_key"]
    params: dict = {
        "start_date_key": filters.start_date_key,
        "end_date_key": filters.end_date_key,
    }
    if filters.route_ids:
        clauses.append("r.route_id IN :route_ids")
        params["route_ids"] = tuple(filters.route_ids)
    if filters.vehicle_types:
        clauses.append("vt.type_name IN :vehicle_types")
        params["vehicle_types"] = tuple(filters.vehicle_types)
    return " AND ".join(clauses), params


def _run(sql: str, params: dict) -> pd.DataFrame:
    stmt = text(sql)
    if isinstance(params.get("route_ids"), tuple):
        stmt = stmt.bindparams(bindparam("route_ids", expanding=True))
    if isinstance(params.get("vehicle_types"), tuple):
        stmt = stmt.bindparams(bindparam("vehicle_types", expanding=True))
    with get_engine().connect() as conn:
        return pd.read_sql(stmt, conn, params=params)


def get_available_date_range() -> tuple[date | None, date | None]:
    df = _run(
        """
        SELECT MIN(d.full_date) AS min_date, MAX(d.full_date) AS max_date
        FROM fact_trip_delay f
        JOIN dim_date d ON f.date_key = d.date_key
        """,
        {},
    )
    if df.empty or pd.isna(df.iloc[0]["min_date"]):
        return None, None
    return df.iloc[0]["min_date"], df.iloc[0]["max_date"]


def get_available_routes() -> pd.DataFrame:
    return _run(
        """
        SELECT DISTINCT r.route_id, r.route_short_name
        FROM dim_route r
        JOIN fact_trip_delay f ON f.route_key = r.route_key
        ORDER BY r.route_short_name
        """,
        {},
    )


def get_available_vehicle_types() -> pd.DataFrame:
    return _run(
        """
        SELECT DISTINCT vt.type_name
        FROM dim_vehicle_type vt
        JOIN fact_trip_delay f ON f.vehicle_type_key = vt.vehicle_type_key
        ORDER BY vt.type_name
        """,
        {},
    )


def get_kpis(filters: Filters) -> dict:
    where, params = _where_clause(filters)
    sql = f"""
        SELECT
            COUNT(*) AS total_facts,
            COUNT(DISTINCT f.trip_id) AS trips_observed,
            PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY f.delay_seconds) AS median_delay_sec,
            AVG(CASE WHEN f.delay_seconds IS NULL THEN NULL
                     WHEN f.delay_seconds <= 0 THEN 1.0
                     ELSE 0.0 END) AS on_time_rate,
            COUNT(f.delay_seconds) AS observed_delay_count
        FROM fact_trip_delay f
        JOIN dim_route r ON f.route_key = r.route_key
        JOIN dim_vehicle_type vt ON f.vehicle_type_key = vt.vehicle_type_key
        WHERE {where}
    """
    df = _run(sql, params)
    row = df.iloc[0]

    worst_route_sql = f"""
        SELECT r.route_short_name, AVG(f.delay_seconds) AS avg_delay
        FROM fact_trip_delay f
        JOIN dim_route r ON f.route_key = r.route_key
        JOIN dim_vehicle_type vt ON f.vehicle_type_key = vt.vehicle_type_key
        WHERE {where} AND f.delay_seconds IS NOT NULL
        GROUP BY r.route_short_name
        ORDER BY avg_delay DESC
        LIMIT 1
    """
    worst_df = _run(worst_route_sql, params)
    worst_route = (
        worst_df.iloc[0]["route_short_name"] if not worst_df.empty else "N/A"
    )

    return {
        "total_facts": int(row["total_facts"] or 0),
        "trips_observed": int(row["trips_observed"] or 0),
        "median_delay_sec": row["median_delay_sec"],
        "on_time_rate": row["on_time_rate"],
        "observed_delay_count": int(row["observed_delay_count"] or 0),
        "worst_route": worst_route,
    }


def get_avg_delay_by_route(filters: Filters, limit: int = 20) -> pd.DataFrame:
    where, params = _where_clause(filters)
    params["limit"] = limit
    sql = f"""
        SELECT
            r.route_short_name,
            AVG(f.delay_seconds) AS avg_delay_sec,
            COUNT(*) AS n_observations
        FROM fact_trip_delay f
        JOIN dim_route r ON f.route_key = r.route_key
        JOIN dim_vehicle_type vt ON f.vehicle_type_key = vt.vehicle_type_key
        WHERE {where} AND f.delay_seconds IS NOT NULL
        GROUP BY r.route_short_name
        ORDER BY avg_delay_sec DESC
        LIMIT :limit
    """
    return _run(sql, params)


def get_delay_heatmap(filters: Filters) -> pd.DataFrame:
    where, params = _where_clause(filters)
    sql = f"""
        SELECT
            d.day_name,
            d.day_of_week,
            EXTRACT(HOUR FROM f.scheduled_arrival)::int AS hour_of_day,
            AVG(f.delay_seconds) AS avg_delay_sec
        FROM fact_trip_delay f
        JOIN dim_route r ON f.route_key = r.route_key
        JOIN dim_vehicle_type vt ON f.vehicle_type_key = vt.vehicle_type_key
        JOIN dim_date d ON f.date_key = d.date_key
        WHERE {where} AND f.delay_seconds IS NOT NULL
        GROUP BY d.day_name, d.day_of_week, hour_of_day
        ORDER BY d.day_of_week, hour_of_day
    """
    return _run(sql, params)


def get_worst_stops(filters: Filters, limit: int = 10) -> pd.DataFrame:
    where, params = _where_clause(filters)
    params["limit"] = limit
    sql = f"""
        SELECT
            s.stop_name,
            s.stop_id,
            AVG(f.delay_seconds) AS avg_delay_sec,
            COUNT(*) AS n_observations
        FROM fact_trip_delay f
        JOIN dim_stop s ON f.stop_key = s.stop_key
        JOIN dim_route r ON f.route_key = r.route_key
        JOIN dim_vehicle_type vt ON f.vehicle_type_key = vt.vehicle_type_key
        WHERE {where} AND f.delay_seconds IS NOT NULL
        GROUP BY s.stop_name, s.stop_id
        HAVING COUNT(*) >= 3
        ORDER BY avg_delay_sec DESC
        LIMIT :limit
    """
    return _run(sql, params)


def get_stops_map_data(filters: Filters) -> pd.DataFrame:
    where, params = _where_clause(filters)
    sql = f"""
        SELECT
            s.stop_name,
            s.stop_lat,
            s.stop_lon,
            AVG(f.delay_seconds) AS avg_delay_sec,
            COUNT(*) AS n_observations
        FROM fact_trip_delay f
        JOIN dim_stop s ON f.stop_key = s.stop_key
        JOIN dim_route r ON f.route_key = r.route_key
        JOIN dim_vehicle_type vt ON f.vehicle_type_key = vt.vehicle_type_key
        WHERE {where} AND f.delay_seconds IS NOT NULL
          AND s.stop_lat IS NOT NULL AND s.stop_lon IS NOT NULL
        GROUP BY s.stop_name, s.stop_lat, s.stop_lon
    """
    return _run(sql, params)


def get_delay_distribution(filters: Filters, sample_limit: int = 20000) -> pd.DataFrame:
    where, params = _where_clause(filters)
    params["limit"] = sample_limit
    sql = f"""
        SELECT f.delay_seconds
        FROM fact_trip_delay f
        JOIN dim_route r ON f.route_key = r.route_key
        JOIN dim_vehicle_type vt ON f.vehicle_type_key = vt.vehicle_type_key
        WHERE {where} AND f.delay_seconds IS NOT NULL
        LIMIT :limit
    """
    return _run(sql, params)
