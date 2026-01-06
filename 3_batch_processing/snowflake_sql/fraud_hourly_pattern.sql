CREATE OR REPLACE VIEW fraud_hourly_pattern AS
SELECT  
    DATE_PART(hour, ingest_ts) AS hour,
    COUNT_IF(Class = 1) AS fraud_count
FROM creditcard_cleaned
GROUP BY hour
ORDER BY hour;
