-- Country breakdown
SELECT
    country,
    COUNT(*)                   AS total_events,
    COUNT(DISTINCT session_id) AS unique_sessions,
    ROUND(AVG(price), 2)       AS avg_price,
    ROUND(SUM(price), 2)       AS total_revenue
FROM events
GROUP BY country
ORDER BY total_events DESC;
