-- Kimball star schema for Swedish transit delay analytics

CREATE TABLE IF NOT EXISTS dim_date (
    date_key            INTEGER PRIMARY KEY,
    full_date           DATE NOT NULL UNIQUE,
    day_of_week         SMALLINT NOT NULL,
    day_name            VARCHAR(10) NOT NULL,
    is_weekend          BOOLEAN NOT NULL,
    is_swedish_holiday  BOOLEAN DEFAULT FALSE
);

CREATE TABLE IF NOT EXISTS dim_vehicle_type (
    vehicle_type_key    SERIAL PRIMARY KEY,
    gtfs_route_type     INTEGER NOT NULL UNIQUE,
    type_name           VARCHAR(20) NOT NULL
);

INSERT INTO dim_vehicle_type (gtfs_route_type, type_name) VALUES
    (0, 'Tram'),
    (1, 'Metro'),
    (2, 'Rail'),
    (3, 'Bus'),
    (4, 'Ferry'),
    (7, 'Funicular'),
    (11, 'Trolleybus'),
    (12, 'Monorail')
ON CONFLICT (gtfs_route_type) DO NOTHING;

CREATE TABLE IF NOT EXISTS dim_route (
    route_key           SERIAL PRIMARY KEY,
    route_id            VARCHAR(64) NOT NULL,
    route_short_name    VARCHAR(32),
    route_long_name     VARCHAR(255),
    operator            VARCHAR(64) NOT NULL,
    UNIQUE (route_id, operator)
);

CREATE TABLE IF NOT EXISTS dim_stop (
    stop_key            SERIAL PRIMARY KEY,
    stop_id             VARCHAR(64) NOT NULL,
    stop_name           VARCHAR(255),
    stop_lat            DOUBLE PRECISION,
    stop_lon            DOUBLE PRECISION,
    operator            VARCHAR(64) NOT NULL,
    UNIQUE (stop_id, operator)
);

CREATE TABLE IF NOT EXISTS fact_trip_delay (
    delay_key           BIGSERIAL PRIMARY KEY,
    date_key            INTEGER NOT NULL REFERENCES dim_date(date_key),
    route_key           INTEGER NOT NULL REFERENCES dim_route(route_key),
    stop_key            INTEGER NOT NULL REFERENCES dim_stop(stop_key),
    vehicle_type_key    INTEGER NOT NULL REFERENCES dim_vehicle_type(vehicle_type_key),
    trip_id             VARCHAR(64) NOT NULL,
    stop_sequence       INTEGER NOT NULL,
    scheduled_arrival   TIMESTAMP NOT NULL,
    actual_arrival      TIMESTAMP,
    delay_seconds       INTEGER,
    data_source         VARCHAR(20) DEFAULT 'gtfs_rt',
    UNIQUE (trip_id, stop_key, date_key, stop_sequence)
);

CREATE TABLE IF NOT EXISTS pipeline_runs (
    run_id          UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    dag_run_id      VARCHAR(64),
    service_date    DATE NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    finished_at     TIMESTAMPTZ,
    status          VARCHAR(20),
    rows_ingested   INTEGER,
    rows_fact       INTEGER,
    dq_status       VARCHAR(20)
);
