CREATE INDEX IF NOT EXISTS idx_fact_trip_delay_date_route
    ON fact_trip_delay (date_key, route_key);

CREATE INDEX IF NOT EXISTS idx_fact_trip_delay_stop
    ON fact_trip_delay (stop_key);

CREATE INDEX IF NOT EXISTS idx_fact_trip_delay_trip
    ON fact_trip_delay (trip_id);

CREATE INDEX IF NOT EXISTS idx_dim_route_operator
    ON dim_route (operator);

CREATE INDEX IF NOT EXISTS idx_dim_stop_operator
    ON dim_stop (operator);
