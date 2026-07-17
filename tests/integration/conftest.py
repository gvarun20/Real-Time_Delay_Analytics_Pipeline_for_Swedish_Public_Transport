"""Fixtures for integration tests that need a real Postgres connection.

Locally: point `.env` at the docker-compose `postgres-analytics` service
(default `localhost:5433`, per `.env.example`) and these tests run against it.
In CI: `.github/workflows/ci.yml` spins up a `postgres:15` service container
and loads `sql/schema.sql` before pytest runs.

If no Postgres is reachable, these tests are skipped rather than failed, so
`pytest` stays green on a machine with no Docker running.
"""

from __future__ import annotations

import psycopg2
import pytest

from config.settings import (
    POSTGRES_DB,
    POSTGRES_HOST,
    POSTGRES_PASSWORD,
    POSTGRES_PORT,
    POSTGRES_USER,
)


@pytest.fixture(scope="session")
def pg_conn():
    try:
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD,
            connect_timeout=3,
        )
    except psycopg2.OperationalError as exc:
        pytest.skip(f"Postgres not reachable at {POSTGRES_HOST}:{POSTGRES_PORT} ({exc})")
        return
    yield conn
    conn.close()


@pytest.fixture
def pg_cursor(pg_conn):
    with pg_conn.cursor() as cur:
        yield cur
    pg_conn.rollback()
