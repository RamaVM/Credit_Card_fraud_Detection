CREATE OR REPLACE VIEW fraud_amount_analysis AS
SELECT  
    Class,
    ROUND(AVG(Amount), 2) AS avg_amount,
    ROUND(MIN(Amount), 2) AS min_amount,
    ROUND(MAX(Amount), 2) AS max_amount,
    COUNT(*) AS tx_count
FROM creditcard_cleaned
GROUP BY Class
ORDER BY Class;
