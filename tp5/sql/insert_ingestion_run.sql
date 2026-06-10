INSERT INTO technical.ingestion_runs
    (run_id, source, data_interval_start, data_interval_end,
     started_at, ended_at, status, records_received, records_inserted)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);