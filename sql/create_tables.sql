-- Raw weather data ingested directly from Open-Meteo API
CREATE TABLE IF NOT EXISTS raw_weather (
    id               SERIAL PRIMARY KEY,
    city             VARCHAR(100),
    date             DATE,
    temperature_max  FLOAT,
    temperature_min  FLOAT,
    precipitation    FLOAT,
    windspeed_max    FLOAT,
    ingested_at      TIMESTAMP DEFAULT NOW(),
    CONSTRAINT raw_weather_city_date_unique UNIQUE (city, date)
);

-- daily_summary is managed by dbt (see dbt/models/daily_summary.sql)
