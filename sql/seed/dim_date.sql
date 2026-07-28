WITH raw_date_series AS (
    SELECT generate_series(
        '2020-01-01'::DATE,
        '2030-12-31'::DATE,
        '1 day'::INTERVAL
    )::DATE AS datum
)

INSERT INTO mart.dim_date (
    date_key, full_date, year_number, quarter_number,
    month_number, month_name, week_of_year, day_of_week, day_name
)
SELECT
    to_char(datum, 'YYYYMMDD')::INT AS date_key,
    datum AS full_date,
    extract(YEAR FROM datum)::INT AS year_number,
    extract(QUARTER FROM datum)::INT AS quarter_number,
    extract(MONTH FROM datum)::INT AS month_number,
    to_char(datum, 'TMMonth') AS month_name,
    extract(WEEK FROM datum)::INT AS week_of_year,
    extract(ISODOW FROM datum)::INT AS day_of_week,
    to_char(datum, 'TMDay') AS day_name
FROM raw_date_series
ON CONFLICT (date_key) DO NOTHING;
