INSERT INTO technical.data_quality_results
    (run_id, status, records_checked, anomaly_count, detail)
VALUES (%s, %s, %s, %s, %s);