INSERT INTO dim_date (date_key, full_date, day_of_week, day_name, is_weekend, is_swedish_holiday)
SELECT
    TO_CHAR(d, 'YYYYMMDD')::INTEGER AS date_key,
    d::DATE AS full_date,
    EXTRACT(ISODOW FROM d)::SMALLINT AS day_of_week,
    TRIM(TO_CHAR(d, 'Dy')) AS day_name,
    EXTRACT(ISODOW FROM d) IN (6, 7) AS is_weekend,
    FALSE AS is_swedish_holiday
FROM generate_series('2024-01-01'::DATE, '2027-12-31'::DATE, INTERVAL '1 day') AS d
ON CONFLICT (date_key) DO NOTHING;
