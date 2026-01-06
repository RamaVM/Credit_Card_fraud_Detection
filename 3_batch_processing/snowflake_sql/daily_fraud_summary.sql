CREATE OR REPLACE VIEW daily_fraud_summary AS
SELECT 
    DATE(ingest_ts) AS day,
    COUNT(*) AS total_tx,
    COUNT_IF(Class = 1) AS fraud_tx,
    ROUND((fraud_tx / total_tx) * 100, 2) AS fraud_percentage
FROM creditcard_cleaned
GROUP BY day
ORDER BY day;
