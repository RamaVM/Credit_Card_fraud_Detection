CREATE OR REPLACE VIEW fraud_percentage_report AS
SELECT  
    COUNT_IF(Class = 1) AS frauds,
    COUNT(*) AS total,
    ROUND(frauds / total * 100, 2) AS fraud_rate
FROM creditcard_cleaned;
