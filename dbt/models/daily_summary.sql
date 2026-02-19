-- daily_summary: aggregated weather metrics derived from raw_weather.
-- dbt manages this table; it is rebuilt on every dbt run.

SELECT
    city,
    date,
    ROUND(((temperature_max + temperature_min) / 2.0)::numeric, 2) AS avg_temperature,
    precipitation                                                    AS total_precipitation,
    windspeed_max                                                    AS max_windspeed,
    NOW()                                                            AS created_at
FROM {{ source('weather', 'raw_weather') }}
