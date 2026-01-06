CREATE OR REPLACE VIEW top_fraud_transactions AS
SELECT  
    Amount,
    ingest_ts,
    Time
FROM creditcard_cleaned
WHERE Class = 1
ORDER BY Amount DESC
LIMIT 20;
