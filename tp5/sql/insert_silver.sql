-- Upsert idempotent : si une ligne existe déjà pour (city, observed_at),
-- elle est mise à jour plutôt que dupliquée.
INSERT INTO silver.weather_observations
    (city, observed_at, temperature_celsius, humidity_pct,
     wind_speed_kmh, precipitation_mm, run_id)
VALUES (%s, %s, %s, %s, %s, %s, %s)
ON CONFLICT (city, observed_at)
DO UPDATE SET
    temperature_celsius = EXCLUDED.temperature_celsius,
    humidity_pct        = EXCLUDED.humidity_pct,
    wind_speed_kmh      = EXCLUDED.wind_speed_kmh,
    precipitation_mm    = EXCLUDED.precipitation_mm,
    run_id              = EXCLUDED.run_id,
    inserted_at         = NOW();