CREATE OR REPLACE VIEW analytics_high_risk_patterns AS
SELECT 
    V14, V17, V21,
    COUNT(*) AS frequency,
    SUM(CASE WHEN Class = 1 THEN 1 ELSE 0 END) AS fraud_count
FROM creditcard_cleaned
GROUP BY V14, V17, V21
ORDER BY fraud_count DESC
LIMIT 20;
