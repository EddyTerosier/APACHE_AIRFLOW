-- Recalcule les agrégats journaliers à partir de silver (source de vérité)
-- et les upserte. Idempotent grâce à ON CONFLICT (city, observation_date).
INSERT INTO gold.weather_daily_city
    (city, observation_date, avg_temperature, max_wind_speed,
     total_precipitation, run_id)
SELECT
    city,
    DATE(observed_at)                            AS observation_date,
    ROUND(AVG(temperature_celsius)::numeric, 2)  AS avg_temperature,
    MAX(wind_speed_kmh)                          AS max_wind_speed,
    SUM(precipitation_mm)                        AS total_precipitation,
    %s                                           AS run_id
FROM silver.weather_observations
WHERE DATE(observed_at) = CURRENT_DATE
GROUP BY city, DATE(observed_at)
ON CONFLICT (city, observation_date)
DO UPDATE SET
    avg_temperature     = EXCLUDED.avg_temperature,
    max_wind_speed      = EXCLUDED.max_wind_speed,
    total_precipitation = EXCLUDED.total_precipitation,
    run_id              = EXCLUDED.run_id,
    updated_at          = NOW();