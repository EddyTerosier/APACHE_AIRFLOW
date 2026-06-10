INSERT INTO bronze.raw_weather_payloads
    (run_id, city, latitude, longitude, payload_json)
VALUES (%s, %s, %s, %s, %s);